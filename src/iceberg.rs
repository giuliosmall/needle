//! Incremental Needle index from a local Iceberg table's current snapshot.
//!
//! Subset: `metadata/*.metadata.json` + Avro manifest-list / manifests. Object
//! stores other than `s3://` URIs stored as path strings are not fetched.
//!
//! Avro is a simplified Iceberg subset: we read `manifest_path` from the
//! manifest-list and `status` + nested `data_file.file_path` (also `file-path`)
//! from manifest entries. Writers in tests use those underscore names (valid
//! Avro identifiers). Spec Iceberg Avro uses the same underscore names; JSON
//! table metadata uses hyphens (`current-snapshot-id`, `manifest-list`).

use crate::index::{IndexBuilder, IndexFragmentMeta};
use anyhow::{Context, Result};
use apache_avro::types::Value as AvroValue;
use apache_avro::Reader;
use serde::Deserialize;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct IcebergIndexOpts {
    /// Table root (directory that contains `metadata/`).
    pub table: PathBuf,
    pub index: PathBuf,
    /// Default `["user_id"]` when empty.
    pub key_columns: Vec<String>,
    pub value_columns: Vec<String>,
    pub covering: bool,
    pub buckets: u32,
    /// Default `"iceberg"` when empty.
    pub fragment_prefix: String,
}

#[derive(Debug, Clone)]
pub struct IcebergIndexReport {
    pub snapshot_id: i64,
    pub fragment_id: String,
    pub files_indexed: usize,
    /// Already had a fragment for this snapshot.
    pub skipped: bool,
    pub table_location: String,
}

#[derive(Debug, Deserialize)]
struct TableMetadataJson {
    #[serde(rename = "current-snapshot-id", alias = "current_snapshot_id", default)]
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    location: Option<String>,
    #[serde(default)]
    snapshots: Vec<SnapshotJson>,
}

#[derive(Debug, Deserialize)]
struct SnapshotJson {
    #[serde(rename = "snapshot-id", alias = "snapshot_id")]
    snapshot_id: i64,
    #[serde(rename = "manifest-list", alias = "manifest_list", default)]
    manifest_list: Option<String>,
}

struct LoadedTable {
    metadata_path: PathBuf,
    current_snapshot_id: i64,
    location: String,
    manifest_list: PathBuf,
}

pub fn read_current_snapshot_id(table: &Path) -> Result<i64> {
    Ok(load_table(table)?.current_snapshot_id)
}

pub fn list_current_parquet_files(table: &Path) -> Result<Vec<PathBuf>> {
    let loaded = load_table(table)?;
    list_live_files(&loaded)
}

pub fn index_iceberg_table(opts: &IcebergIndexOpts) -> Result<IcebergIndexReport> {
    let loaded = load_table(&opts.table)?;
    let snapshot_id = loaded.current_snapshot_id;
    let table_location = loaded.location.clone();
    let prefix = if opts.fragment_prefix.is_empty() {
        "iceberg"
    } else {
        opts.fragment_prefix.as_str()
    };
    let fragment_id = format!("{prefix}-{snapshot_id}");

    let metas = load_fragment_metas(&opts.index)?;
    if let Some(existing) = metas.iter().find(|m| fragment_has_snapshot(m, snapshot_id)) {
        return Ok(IcebergIndexReport {
            snapshot_id,
            fragment_id: existing.fragment_id.clone(),
            files_indexed: 0,
            skipped: true,
            table_location,
        });
    }

    let live = list_live_files(&loaded)?;
    let already = previous_file_keys(&metas, &opts.index);
    let new_files: Vec<PathBuf> = live
        .iter()
        .filter(|p| {
            !file_match_keys(p, &opts.index)
                .iter()
                .any(|k| already.contains(k))
        })
        .cloned()
        .collect();

    // New snapshot with no added files (rewrites / same live set): index the
    // current live files into a new fragment rather than skipping.
    let files_to_index: Vec<PathBuf> = if new_files.is_empty() { live } else { new_files };
    if files_to_index.is_empty() {
        return Ok(IcebergIndexReport {
            snapshot_id,
            fragment_id,
            files_indexed: 0,
            skipped: false,
            table_location,
        });
    }

    let key_columns = if opts.key_columns.is_empty() {
        vec!["user_id".to_string()]
    } else {
        opts.key_columns.clone()
    };
    let note = format!("iceberg-snapshot:{snapshot_id}");
    IndexBuilder::new(&opts.index, opts.buckets)
        .with_covering(opts.covering)
        .with_key_columns(key_columns)
        .with_value_columns(opts.value_columns.clone())
        .build_fragment(&files_to_index, &fragment_id, Some(&note))?;

    Ok(IcebergIndexReport {
        snapshot_id,
        fragment_id,
        files_indexed: files_to_index.len(),
        skipped: false,
        table_location,
    })
}

fn load_table(table: &Path) -> Result<LoadedTable> {
    let metadata_path = find_current_metadata(table)?;
    let file = File::open(&metadata_path)
        .with_context(|| format!("open Iceberg metadata {}", metadata_path.display()))?;
    let meta: TableMetadataJson = serde_json::from_reader(BufReader::new(file))
        .with_context(|| format!("parse Iceberg metadata {}", metadata_path.display()))?;
    let current_snapshot_id = meta
        .current_snapshot_id
        .filter(|&id| id >= 0)
        .with_context(|| {
            format!(
                "no current snapshot in {}",
                metadata_path.display()
            )
        })?;
    let location = meta
        .location
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| table.to_string_lossy().to_string());
    let snap = meta
        .snapshots
        .iter()
        .find(|s| s.snapshot_id == current_snapshot_id)
        .with_context(|| {
            format!(
                "snapshot {current_snapshot_id} missing from {}",
                metadata_path.display()
            )
        })?;
    let ml = snap
        .manifest_list
        .as_deref()
        .filter(|s| !s.is_empty())
        .with_context(|| format!("snapshot {current_snapshot_id} has no manifest-list"))?;
    let manifest_list = resolve_iceberg_path(ml, table, &location);
    Ok(LoadedTable {
        metadata_path,
        current_snapshot_id,
        location,
        manifest_list,
    })
}

fn find_current_metadata(table: &Path) -> Result<PathBuf> {
    let meta_dir = table.join("metadata");
    anyhow::ensure!(
        meta_dir.is_dir(),
        "Iceberg table {} has no metadata/ directory",
        table.display()
    );
    let hint_path = meta_dir.join("version-hint.text");
    if hint_path.is_file() {
        let hint = fs::read_to_string(&hint_path)
            .with_context(|| format!("read {}", hint_path.display()))?;
        if let Some(p) = resolve_version_hint(&meta_dir, hint.trim()) {
            return Ok(p);
        }
    }
    let mut files = scan_metadata_files(&meta_dir)?;
    anyhow::ensure!(
        !files.is_empty(),
        "no *.metadata.json under {}",
        meta_dir.display()
    );
    files.sort_by_key(|(v, _)| *v);
    Ok(files.pop().unwrap().1)
}

fn resolve_version_hint(meta_dir: &Path, hint: &str) -> Option<PathBuf> {
    if hint.is_empty() {
        return None;
    }
    let direct = meta_dir.join(hint);
    if direct.is_file() {
        return Some(direct);
    }
    let as_json = meta_dir.join(format!("{hint}.metadata.json"));
    if as_json.is_file() {
        return Some(as_json);
    }
    let num = hint.strip_prefix('v').unwrap_or(hint);
    let v: i64 = num.parse().ok()?;
    for name in [
        format!("v{v}.metadata.json"),
        format!("v{v:05}.metadata.json"),
        format!("{v:05}.metadata.json"),
        format!("{v}.metadata.json"),
    ] {
        let p = meta_dir.join(&name);
        if p.is_file() {
            return Some(p);
        }
    }
    scan_metadata_files(meta_dir)
        .ok()?
        .into_iter()
        .find(|(ver, _)| *ver == v)
        .map(|(_, p)| p)
}

fn scan_metadata_files(meta_dir: &Path) -> Result<Vec<(i64, PathBuf)>> {
    let mut out = Vec::new();
    for ent in fs::read_dir(meta_dir)
        .with_context(|| format!("read_dir {}", meta_dir.display()))?
    {
        let ent = ent?;
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if !name.ends_with(".metadata.json") {
            continue;
        }
        if let Some(v) = parse_metadata_version(&name) {
            out.push((v, ent.path()));
        }
    }
    Ok(out)
}

fn parse_metadata_version(name: &str) -> Option<i64> {
    let stem = name.strip_suffix(".metadata.json")?;
    if let Some(rest) = stem.strip_prefix('v') {
        if let Ok(n) = rest.parse::<i64>() {
            return Some(n);
        }
    }
    let num_part = stem.split(['-', '_']).next()?;
    num_part.parse().ok()
}

fn list_live_files(loaded: &LoadedTable) -> Result<Vec<PathBuf>> {
    let table = loaded
        .metadata_path
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or_else(|| Path::new("."));
    let list_values = read_avro(&loaded.manifest_list).with_context(|| {
        format!(
            "read manifest-list {}",
            loaded.manifest_list.display()
        )
    })?;
    let mut files = Vec::new();
    let mut seen = HashSet::new();
    for rec in &list_values {
        let Some(mp) = avro_string_field(rec, &["manifest_path", "manifest-path"]) else {
            continue;
        };
        let manifest = resolve_iceberg_path(&mp, table, &loaded.location);
        let entries = read_avro(&manifest)
            .with_context(|| format!("read manifest {}", manifest.display()))?;
        for entry in &entries {
            if avro_status(entry) == 2 {
                continue;
            }
            let Some(data_file) = avro_field(entry, &["data_file", "data-file"]) else {
                continue;
            };
            let Some(fp) = avro_string_field(
                data_file,
                &["file_path", "file-path"],
            ) else {
                continue;
            };
            let path = resolve_iceberg_path(&fp, table, &loaded.location);
            let key = normalize_file_key(&path.to_string_lossy());
            if seen.insert(key) {
                files.push(path);
            }
        }
    }
    files.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));
    Ok(files)
}

fn read_avro(path: &Path) -> Result<Vec<AvroValue>> {
    let file = File::open(path).with_context(|| format!("open avro {}", path.display()))?;
    let reader = Reader::new(BufReader::new(file))
        .with_context(|| format!("avro reader {}", path.display()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("avro decode {}", path.display()))
}

fn unwrap_union(v: &AvroValue) -> &AvroValue {
    match v {
        AvroValue::Union(_, inner) => unwrap_union(inner),
        other => other,
    }
}

fn avro_name_eq(a: &str, b: &str) -> bool {
    let norm = |s: &str| s.replace('-', "_");
    norm(a).eq_ignore_ascii_case(&norm(b))
}

fn avro_field<'a>(v: &'a AvroValue, names: &[&str]) -> Option<&'a AvroValue> {
    let v = unwrap_union(v);
    match v {
        AvroValue::Record(fields) => {
            for (k, val) in fields {
                if names.iter().any(|n| avro_name_eq(k, n)) {
                    return Some(unwrap_union(val));
                }
            }
            None
        }
        AvroValue::Map(map) => {
            for n in names {
                if let Some(val) = map.get(*n) {
                    return Some(unwrap_union(val));
                }
                let alt_us = n.replace('-', "_");
                if let Some(val) = map.get(&alt_us) {
                    return Some(unwrap_union(val));
                }
                let alt_hy = n.replace('_', "-");
                if let Some(val) = map.get(&alt_hy) {
                    return Some(unwrap_union(val));
                }
            }
            None
        }
        _ => None,
    }
}

fn avro_string_field(v: &AvroValue, names: &[&str]) -> Option<String> {
    match unwrap_union(avro_field(v, names)?) {
        AvroValue::String(s) => Some(s.clone()),
        AvroValue::Bytes(b) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

fn avro_status(entry: &AvroValue) -> i32 {
    match avro_field(entry, &["status"]).map(unwrap_union) {
        Some(AvroValue::Int(i)) => *i,
        Some(AvroValue::Long(i)) => *i as i32,
        Some(AvroValue::Enum(i, _)) => *i as i32,
        _ => 0,
    }
}

/// Map Iceberg path strings onto local PathBufs / s3 URI PathBufs.
fn resolve_iceberg_path(raw: &str, table: &Path, table_location: &str) -> PathBuf {
    let raw = raw.trim();
    if raw.starts_with("s3://") || raw.starts_with("s3a://") || raw.starts_with("s3n://") {
        return PathBuf::from(raw);
    }
    if let Some(local) = strip_file_uri(raw) {
        return PathBuf::from(local);
    }
    let p = Path::new(raw);
    if p.is_absolute() {
        return p.to_path_buf();
    }
    if table_location.starts_with("s3://")
        || table_location.starts_with("s3a://")
        || table_location.starts_with("s3n://")
    {
        let loc = table_location.trim_end_matches('/');
        return PathBuf::from(format!("{loc}/{raw}"));
    }
    if let Some(loc) = strip_file_uri(table_location) {
        return PathBuf::from(loc).join(raw);
    }
    if Path::new(table_location).is_absolute() {
        return PathBuf::from(table_location).join(raw);
    }
    table.join(raw)
}

fn strip_file_uri(s: &str) -> Option<String> {
    if let Some(rest) = s.strip_prefix("file://") {
        let rest = rest.strip_prefix("localhost").unwrap_or(rest);
        return Some(rest.to_string());
    }
    if let Some(rest) = s.strip_prefix("file:") {
        return Some(rest.to_string());
    }
    None
}

fn normalize_file_key(s: &str) -> String {
    let s = s.trim();
    let s = strip_file_uri(s).unwrap_or_else(|| s.to_string());
    if let Ok(c) = Path::new(&s).canonicalize() {
        return c.to_string_lossy().to_string();
    }
    s
}

fn file_match_keys(path: &Path, index_root: &Path) -> Vec<String> {
    let mut keys = Vec::new();
    let mut push = |s: String| {
        if !s.is_empty() && !keys.contains(&s) {
            keys.push(s);
        }
    };
    let raw = path.to_string_lossy().to_string();
    push(normalize_file_key(&raw));
    push(raw);
    if let Ok(c) = path.canonicalize() {
        push(c.to_string_lossy().to_string());
    }
    if let Ok(rel) = path.strip_prefix(index_root.parent().unwrap_or(index_root)) {
        push(rel.to_string_lossy().to_string());
    }
    keys
}

fn previous_file_keys(metas: &[IndexFragmentMeta], index_root: &Path) -> HashSet<String> {
    let mut out = HashSet::new();
    for meta in metas {
        for stored in &meta.files {
            for k in stored_file_keys(stored, index_root) {
                out.insert(k);
            }
        }
    }
    out
}

fn stored_file_keys(stored: &str, index_root: &Path) -> Vec<String> {
    let mut keys = Vec::new();
    let mut push = |s: String| {
        if !s.is_empty() && !keys.contains(&s) {
            keys.push(s);
        }
    };
    push(stored.to_string());
    push(normalize_file_key(stored));
    let p = PathBuf::from(strip_file_uri(stored).unwrap_or_else(|| stored.to_string()));
    if p.is_absolute() {
        if let Ok(c) = p.canonicalize() {
            push(c.to_string_lossy().to_string());
        }
        if let Ok(rel) = p.strip_prefix(index_root.parent().unwrap_or(index_root)) {
            push(rel.to_string_lossy().to_string());
        }
    } else {
        for base in [
            index_root.parent().unwrap_or(index_root).to_path_buf(),
            index_root.to_path_buf(),
        ] {
            let joined = base.join(&p);
            push(joined.to_string_lossy().to_string());
            if let Ok(c) = joined.canonicalize() {
                push(c.to_string_lossy().to_string());
            }
        }
    }
    keys
}

fn fragment_has_snapshot(meta: &IndexFragmentMeta, snapshot_id: i64) -> bool {
    if meta.iceberg_snapshot_id == Some(snapshot_id) {
        return true;
    }
    if let Some(note) = &meta.note {
        if note_has_snapshot(note, snapshot_id) {
            return true;
        }
    }
    false
}

fn note_has_snapshot(note: &str, snapshot_id: i64) -> bool {
    let token = format!("iceberg-snapshot:{snapshot_id}");
    let note = note.trim();
    if note == token {
        return true;
    }
    if let Some(pos) = note.find(&token) {
        let after = pos + token.len();
        let next_is_digit = note
            .as_bytes()
            .get(after)
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false);
        return !next_is_digit;
    }
    false
}

fn load_fragment_metas(index_root: &Path) -> Result<Vec<IndexFragmentMeta>> {
    let registry_path = index_root.join("registry.json");
    if !registry_path.exists() {
        return Ok(Vec::new());
    }
    let registry: Vec<String> = serde_json::from_reader(File::open(&registry_path)?)
        .with_context(|| format!("read {}", registry_path.display()))?;
    let mut metas = Vec::new();
    for id in registry {
        let p = index_root.join("fragments").join(&id).join("manifest.json");
        if p.exists() {
            metas.push(
                serde_json::from_reader(File::open(&p)?)
                    .with_context(|| format!("read {}", p.display()))?,
            );
        }
    }
    Ok(metas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::load_index;
    use crate::writer::{WriteMode, WriterOptions, write_sample_dataset};
    use apache_avro::types::Record;
    use apache_avro::{Schema, Writer};
    use std::io::Write;

    /// Simplified Iceberg-like Avro (underscore names; valid Avro identifiers).
    /// Spec manifests also use `manifest_path` / `status` / `data_file.file_path`.
    const MANIFEST_LIST_SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "manifest_file",
      "fields": [
        {"name": "manifest_path", "type": "string"}
      ]
    }
    "#;

    const MANIFEST_SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "manifest_entry",
      "fields": [
        {"name": "status", "type": "int"},
        {"name": "data_file", "type": {
          "type": "record",
          "name": "data_file",
          "fields": [
            {"name": "file_path", "type": "string"}
          ]
        }}
      ]
    }
    "#;

    fn tiny_opts(dir: &Path) -> WriterOptions {
        WriterOptions {
            out_dir: dir.to_path_buf(),
            num_users: 8,
            listens_per_user: 4,
            num_files: 2,
            mode: WriteMode::Sorted,
            rows_per_row_group: 64,
            write_page_index: true,
            seed: 42,
            one_page_per_key: false,
        }
    }

    fn file_uri(p: &Path) -> String {
        let abs = p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
        format!("file://{}", abs.display())
    }

    fn write_avro_records(path: &Path, schema_json: &str, values: Vec<AvroValue>) -> Result<()> {
        let schema = Schema::parse_str(schema_json)?;
        let file = File::create(path)?;
        let mut writer = Writer::new(&schema, file);
        for v in values {
            writer.append(v)?;
        }
        writer.flush()?;
        let _ = writer.into_inner()?;
        Ok(())
    }

    fn write_manifest_list(path: &Path, manifest: &Path) -> Result<()> {
        let schema = Schema::parse_str(MANIFEST_LIST_SCHEMA)?;
        let mut rec = Record::new(&schema).expect("manifest_file record");
        rec.put("manifest_path", file_uri(manifest));
        write_avro_records(path, MANIFEST_LIST_SCHEMA, vec![rec.into()])
    }

    fn write_manifest(path: &Path, parquet: &[PathBuf], deleted: &[PathBuf]) -> Result<()> {
        let schema = Schema::parse_str(MANIFEST_SCHEMA)?;
        let mut values = Vec::new();
        for (status, files) in [(1i32, parquet), (2i32, deleted)] {
            for f in files {
                let mut rec = Record::new(&schema).expect("manifest_entry record");
                rec.put("status", status);
                rec.put(
                    "data_file",
                    AvroValue::Record(vec![(
                        "file_path".to_string(),
                        AvroValue::String(file_uri(f)),
                    )]),
                );
                values.push(rec.into());
            }
        }
        write_avro_records(path, MANIFEST_SCHEMA, values)
    }

    fn write_metadata(
        table: &Path,
        snapshot_id: i64,
        version: i64,
        manifest_list: &Path,
    ) -> Result<()> {
        let meta_dir = table.join("metadata");
        fs::create_dir_all(&meta_dir)?;
        let location = file_uri(table);
        let body = serde_json::json!({
            "format-version": 2,
            "location": location,
            "current-snapshot-id": snapshot_id,
            "snapshots": [{
                "snapshot-id": snapshot_id,
                "timestamp-ms": 1,
                "manifest-list": file_uri(manifest_list),
                "schema-id": 0,
                "summary": {"operation": "append"}
            }]
        });
        fs::write(
            meta_dir.join(format!("v{version}.metadata.json")),
            serde_json::to_vec_pretty(&body)?,
        )?;
        let mut hint = File::create(meta_dir.join("version-hint.text"))?;
        write!(hint, "{version}")?;
        Ok(())
    }

    fn write_iceberg_table(
        table: &Path,
        parquet: &[PathBuf],
        snapshot_id: i64,
        version: i64,
    ) -> Result<()> {
        let meta_dir = table.join("metadata");
        fs::create_dir_all(&meta_dir)?;
        let manifest = meta_dir.join(format!("manifest-{snapshot_id}.avro"));
        let list = meta_dir.join(format!("snap-{snapshot_id}-manifest-list.avro"));
        let deleted = vec![meta_dir.join("deleted-not-live.parquet")];
        write_manifest(&manifest, parquet, &deleted)?;
        write_manifest_list(&list, &manifest)?;
        write_metadata(table, snapshot_id, version, &list)?;
        Ok(())
    }

    fn index_opts(table: &Path, index: &Path) -> IcebergIndexOpts {
        IcebergIndexOpts {
            table: table.to_path_buf(),
            index: index.to_path_buf(),
            key_columns: vec!["user_id".to_string()],
            value_columns: Vec::new(),
            covering: true,
            buckets: 8,
            fragment_prefix: "iceberg".to_string(),
        }
    }

    #[test]
    fn read_current_snapshot_id_from_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let paths = write_sample_dataset(&tiny_opts(&data)).unwrap();
        write_iceberg_table(&table, &paths, 42, 1).unwrap();
        assert_eq!(read_current_snapshot_id(&table).unwrap(), 42);
    }

    #[test]
    fn list_current_parquet_files_from_avro_manifests() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let paths = write_sample_dataset(&tiny_opts(&data)).unwrap();
        write_iceberg_table(&table, &paths, 1, 1).unwrap();
        let listed = list_current_parquet_files(&table).unwrap();
        assert_eq!(listed.len(), paths.len(), "live parquet files: {listed:?}");
        let listed_canon: HashSet<_> = listed
            .iter()
            .map(|p| p.canonicalize().unwrap())
            .collect();
        for p in &paths {
            assert!(
                listed_canon.contains(&p.canonicalize().unwrap()),
                "missing {} in {listed:?}",
                p.display()
            );
        }
        assert!(
            listed.iter().all(|p| !p.to_string_lossy().contains("deleted-not-live")),
            "status=2 files must be skipped"
        );
    }

    #[test]
    fn index_iceberg_table_lookup_and_skip() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data)).unwrap();
        write_iceberg_table(&table, &paths, 7, 1).unwrap();

        let opts = index_opts(&table, &index);
        let report = index_iceberg_table(&opts).unwrap();
        assert!(!report.skipped);
        assert_eq!(report.snapshot_id, 7);
        assert_eq!(report.fragment_id, "iceberg-7");
        assert_eq!(report.files_indexed, paths.len());
        assert!(report.table_location.contains("table") || report.table_location.starts_with("file:"));

        let rap = load_index(&index).unwrap();
        let hits = rap.lookup("user_0000");
        assert!(!hits.is_empty(), "expected index hit for user_0000");
        assert_eq!(rap.fragments.len(), 1);
        assert_eq!(
            rap.fragments[0].note.as_deref(),
            Some("iceberg-snapshot:7")
        );
        assert_eq!(rap.fragments[0].iceberg_snapshot_id, Some(7));

        let again = index_iceberg_table(&opts).unwrap();
        assert!(again.skipped);
        assert_eq!(again.files_indexed, 0);
        assert_eq!(again.snapshot_id, 7);
        assert_eq!(again.fragment_id, "iceberg-7");
        let rap2 = load_index(&index).unwrap();
        assert_eq!(rap2.fragments.len(), 1);
    }

    #[test]
    fn index_iceberg_table_incremental_new_files() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data)).unwrap();
        write_iceberg_table(&table, &paths, 1, 1).unwrap();

        let opts = index_opts(&table, &index);
        let first = index_iceberg_table(&opts).unwrap();
        assert_eq!(first.files_indexed, paths.len());

        let extra = data.join("extra_append.parquet");
        fs::copy(&paths[0], &extra).unwrap();
        let mut all = paths.clone();
        all.push(extra);
        write_iceberg_table(&table, &all, 2, 2).unwrap();

        let second = index_iceberg_table(&opts).unwrap();
        assert!(!second.skipped);
        assert_eq!(second.snapshot_id, 2);
        assert_eq!(second.fragment_id, "iceberg-2");
        assert_eq!(second.files_indexed, 1, "only the newly added parquet file");

        let rap = load_index(&index).unwrap();
        assert_eq!(rap.fragments.len(), 2);
        assert!(!rap.lookup("user_0000").is_empty());
    }
}
