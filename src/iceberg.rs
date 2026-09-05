//! Incremental Needle index from a local or object-store Iceberg table's current snapshot.
//!
//! Subset: `metadata/*.metadata.json` + Avro manifest-list / manifests. Metadata
//! and Avro are fetched via `S3Client::get_object` for `s3://` / `https://`
//! (data-file URIs stay as `s3://…` in the index).
//!
//! Catalogs: Iceberg REST (`GET {rest-uri}/v1/namespaces/{ns}/tables/{table}` →
//! `metadata-location`) is the production discovery path. Hadoop warehouse /
//! table-root (`--table`) remains a fallback. REST nested namespaces use the
//! Iceberg unit separator (`a.b` → `a%1Fb`).
//!
//! Avro is a simplified Iceberg subset: we read `manifest_path` from the
//! manifest-list and `status` + nested `data_file.file_path` (also `file-path`)
//! from manifest entries. Writers in tests use those underscore names (valid
//! Avro identifiers). Spec Iceberg Avro uses the same underscore names; JSON
//! table metadata uses hyphens (`current-snapshot-id`, `manifest-list`).

use crate::index::{self, IndexBuilder, IndexFragmentMeta};
use crate::s3::S3Client;
use anyhow::{bail, Context, Result};
use apache_avro::types::Value as AvroValue;
use apache_avro::Reader;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

const UNSUPPORTED_DELETE_FILES: &str =
    "needle refuses Iceberg tables with unsupported delete files; apply deletes or compact first";

/// Iceberg catalog used to discover the current table metadata location.
///
/// REST is the production path; Hadoop warehouse/table-root is a fallback.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IcebergCatalog {
    #[default]
    Hadoop,
    Rest,
    /// AWS Glue Data Catalog — not implemented (hard error, no Hadoop fallback).
    Glue,
    /// Project Nessie — not implemented (hard error, no Hadoop fallback).
    Nessie,
}

impl IcebergCatalog {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hadoop => "hadoop",
            Self::Rest => "rest",
            Self::Glue => "glue",
            Self::Nessie => "nessie",
        }
    }
}

#[derive(Debug, Clone)]
pub struct IcebergIndexOpts {
    /// Hadoop table root (directory that contains `metadata/`). Unused for REST.
    pub table: PathBuf,
    pub index: PathBuf,
    /// Default `["user_id"]` when empty.
    pub key_columns: Vec<String>,
    pub value_columns: Vec<String>,
    pub covering: bool,
    pub buckets: u32,
    /// Default `"iceberg"` when empty.
    pub fragment_prefix: String,
    /// `Hadoop` (default) uses [`Self::table`]; `Rest` uses REST catalog fields.
    pub catalog: IcebergCatalog,
    /// REST catalog base URI, e.g. `http://host/iceberg`.
    pub rest_uri: Option<String>,
    /// Iceberg namespace (`db`, or `a.b` for nested).
    pub namespace: Option<String>,
    /// Table name in the REST catalog.
    pub table_name: Option<String>,
    /// Bearer token (`Authorization: Bearer`). CLI also reads `NEEDLE_ICEBERG_TOKEN`.
    pub rest_token: Option<String>,
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
struct RestLoadTable {
    #[serde(rename = "metadata-location", alias = "metadata_location", default)]
    metadata_location: Option<String>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
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

struct SnapshotFiles {
    data: Vec<PathBuf>,
    position_deletes: Vec<PathBuf>,
    equality_deletes: Vec<PathBuf>,
}

pub fn read_current_snapshot_id(table: &Path) -> Result<i64> {
    Ok(load_table(table)?.current_snapshot_id)
}

pub fn list_current_parquet_files(table: &Path) -> Result<Vec<PathBuf>> {
    let loaded = load_table(table)?;
    list_live_files(&loaded)
}

pub fn index_iceberg_table(opts: &IcebergIndexOpts) -> Result<IcebergIndexReport> {
    let loaded = load_table_for_index(opts)?;
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

    let snap = list_snapshot_files(&loaded)?;
    let (position_deletes, equality_deletes, delete_idents) =
        load_snapshot_deletes(&loaded, &snap)?;

    let live = snap.data;
    let already = fragment_live_file_keys(&metas, &opts.index, false);
    let iceberg_live = fragment_live_file_keys(&metas, &opts.index, true);
    let added: Vec<PathBuf> = live
        .iter()
        .filter(|p| {
            !file_match_keys(p, &opts.index)
                .iter()
                .any(|k| already.contains(k))
        })
        .cloned()
        .collect();
    let live_keys: HashSet<String> = live
        .iter()
        .flat_map(|p| file_match_keys(p, &opts.index))
        .collect();
    let removed: Vec<String> = iceberg_live
        .into_iter()
        .filter(|k| !live_keys.contains(k))
        .collect();

    let applied_deletes = applied_iceberg_delete_keys(&metas);
    let deletes_changed = applied_deletes != delete_idents;

    let mut to_scan = added;
    let mut affected_drop: Vec<String> = Vec::new();
    if deletes_changed {
        for p in &live {
            let already_ix = file_match_keys(p, &opts.index)
                .iter()
                .any(|k| already.contains(k));
            if !already_ix {
                continue;
            }
            let affected =
                !equality_deletes.is_empty() || position_deletes_mention(&position_deletes, p);
            if !affected {
                continue;
            }
            if !to_scan.iter().any(|q| q == p) {
                to_scan.push(p.clone());
            }
            for stored in stored_paths_matching(&metas, &opts.index, p) {
                if !affected_drop.contains(&stored) {
                    affected_drop.push(stored);
                }
            }
        }
    }

    let key_columns = if opts.key_columns.is_empty() {
        vec!["user_id".to_string()]
    } else {
        opts.key_columns.clone()
    };
    let note = format!("iceberg-snapshot:{snapshot_id}");
    let delete_ident_list: Vec<String> = {
        let mut v: Vec<String> = delete_idents.into_iter().collect();
        v.sort();
        v
    };

    // Same live set, new snapshot, delete files unchanged: record snapshot id.
    if to_scan.is_empty() && removed.is_empty() && affected_drop.is_empty() {
        IndexBuilder::new(&opts.index, opts.buckets)
            .with_covering(opts.covering)
            .with_key_columns(key_columns)
            .with_value_columns(opts.value_columns.clone())
            .build_fragment(&[], &fragment_id, Some(&note))?;
        patch_fragment_side_meta(&opts.index, &fragment_id, &[], &delete_ident_list)?;
        return Ok(IcebergIndexReport {
            snapshot_id,
            fragment_id,
            files_indexed: 0,
            skipped: true,
            table_location,
        });
    }

    IndexBuilder::new(&opts.index, opts.buckets)
        .with_covering(opts.covering)
        .with_key_columns(key_columns)
        .with_value_columns(opts.value_columns.clone())
        .with_position_deletes(position_deletes)
        .with_equality_deletes(equality_deletes)
        .build_fragment(&to_scan, &fragment_id, Some(&note))?;
    let mut dropped = removed;
    dropped.extend(affected_drop);
    patch_fragment_side_meta(&opts.index, &fragment_id, &dropped, &delete_ident_list)?;

    Ok(IcebergIndexReport {
        snapshot_id,
        fragment_id,
        files_indexed: to_scan.len(),
        skipped: false,
        table_location,
    })
}

fn fragment_is_iceberg(meta: &IndexFragmentMeta) -> bool {
    meta.iceberg_snapshot_id.is_some()
        || meta
            .note
            .as_deref()
            .is_some_and(|n| n.starts_with("iceberg-snapshot:"))
}

/// Live file identity keys after applying each fragment's `files` then `dropped_files`.
fn fragment_live_file_keys(
    metas: &[IndexFragmentMeta],
    index_root: &Path,
    iceberg_only: bool,
) -> HashSet<String> {
    let mut live = HashSet::new();
    for meta in metas {
        if iceberg_only && !fragment_is_iceberg(meta) {
            continue;
        }
        // Iceberg: dropped_files evict, then files re-add (delete re-scan).
        if fragment_is_iceberg(meta) {
            for d in &meta.dropped_files {
                live.remove(d);
                for k in stored_file_keys(d, index_root) {
                    live.remove(&k);
                }
            }
            for stored in &meta.files {
                for k in stored_file_keys(stored, index_root) {
                    live.insert(k);
                }
            }
        } else {
            for stored in &meta.files {
                for k in stored_file_keys(stored, index_root) {
                    live.insert(k);
                }
            }
            for d in &meta.dropped_files {
                live.remove(d);
                for k in stored_file_keys(d, index_root) {
                    live.remove(&k);
                }
            }
        }
    }
    live
}

fn patch_fragment_side_meta(
    index: &Path,
    fragment_id: &str,
    dropped: &[String],
    delete_files: &[String],
) -> Result<()> {
    if dropped.is_empty() && delete_files.is_empty() {
        return Ok(());
    }
    let man = index
        .join("fragments")
        .join(fragment_id)
        .join("manifest.json");
    let mut meta: IndexFragmentMeta = serde_json::from_reader(
        File::open(&man).with_context(|| format!("open {}", man.display()))?,
    )?;
    meta.dropped_files = dropped.to_vec();
    meta.iceberg_delete_files = delete_files.to_vec();
    let tmp = man.with_file_name(".manifest.json.tmp");
    serde_json::to_writer_pretty(File::create(&tmp)?, &meta)
        .with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, &man).with_context(|| format!("publish {}", man.display()))?;
    Ok(())
}

fn read_bytes(path: &Path) -> Result<Vec<u8>> {
    let uri = path.to_string_lossy();
    if S3Client::is_remote_uri(&uri) {
        let (bucket, key) =
            S3Client::parse_uri(&uri).with_context(|| format!("parse object uri {uri}"))?;
        S3Client::from_env()
            .get_object(&bucket, &key)
            .with_context(|| format!("s3 get {uri}"))
    } else {
        fs::read(path).with_context(|| format!("read {}", path.display()))
    }
}

fn load_table_for_index(opts: &IcebergIndexOpts) -> Result<LoadedTable> {
    match opts.catalog {
        IcebergCatalog::Hadoop => {
            if opts.table.as_os_str().is_empty() {
                return Err(catalog_error(
                    "Hadoop catalog requires --table (warehouse/table root); REST is the production catalog path",
                ));
            }
            load_table(&opts.table)
        }
        IcebergCatalog::Rest => load_table_from_rest(opts),
        IcebergCatalog::Glue | IcebergCatalog::Nessie => Err(catalog_error(format!(
            "unsupported catalog: {}; supported: rest, hadoop",
            opts.catalog.as_str()
        ))),
    }
}

fn load_table_from_rest(opts: &IcebergIndexOpts) -> Result<LoadedTable> {
    let resp = rest_load_table(opts)?;
    if let Some(loc) = resp
        .metadata_location
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return load_table(&path_from_iceberg_uri(loc));
    }
    let Some(meta_val) = resp.metadata else {
        return Err(catalog_error(
            "REST table response missing metadata-location",
        ));
    };
    let parsed: TableMetadataJson = serde_json::from_value(meta_val.clone()).map_err(|e| {
        catalog_error(format!(
            "REST table response has invalid inline metadata: {e}"
        ))
    })?;
    let hint = parsed
        .location
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(path_from_iceberg_uri)
        .ok_or_else(|| {
            catalog_error("REST inline metadata missing location and metadata-location")
        })?;
    let meta_path = hint.join("metadata").join("rest.metadata.json");
    let bytes = serde_json::to_vec(&meta_val)
        .map_err(|e| catalog_error(format!("serialize REST inline metadata: {e}")))?;
    load_table_from_metadata_bytes(&hint, meta_path, &bytes)
}

fn load_table(table: &Path) -> Result<LoadedTable> {
    let metadata_path = find_current_metadata(table)?;
    let bytes = read_bytes(&metadata_path)
        .with_context(|| format!("open Iceberg metadata {}", metadata_path.display()))?;
    // REST `metadata-location` is often the JSON file; Hadoop `--table` is the root.
    let root = if is_metadata_json_path(table.to_string_lossy().trim()) {
        metadata_path
            .parent()
            .and_then(|p| p.parent())
            .unwrap_or(table)
            .to_path_buf()
    } else {
        table.to_path_buf()
    };
    load_table_from_metadata_bytes(&root, metadata_path, &bytes)
}

fn load_table_from_metadata_bytes(
    table: &Path,
    metadata_path: PathBuf,
    bytes: &[u8],
) -> Result<LoadedTable> {
    let meta: TableMetadataJson = serde_json::from_slice(bytes)
        .with_context(|| format!("parse Iceberg metadata {}", metadata_path.display()))?;
    let current_snapshot_id = meta
        .current_snapshot_id
        .filter(|&id| id >= 0)
        .with_context(|| format!("no current snapshot in {}", metadata_path.display()))?;
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
    let table_uri = table.to_string_lossy();
    if let Some(local) = strip_file_uri(table_uri.trim()) {
        return find_current_metadata(Path::new(&local));
    }
    if is_metadata_json_path(table_uri.trim()) {
        return Ok(table.to_path_buf());
    }
    if S3Client::is_remote_uri(&table_uri) {
        return find_current_metadata_remote(&table_uri);
    }
    let meta_dir = table.join("metadata");
    anyhow::ensure!(
        meta_dir.is_dir(),
        "Iceberg table {} has no metadata/ directory",
        table.display()
    );
    let hint_path = meta_dir.join("version-hint.text");
    if hint_path.is_file() {
        let hint =
            read_bytes(&hint_path).with_context(|| format!("read {}", hint_path.display()))?;
        let hint = String::from_utf8_lossy(&hint);
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

fn find_current_metadata_remote(table_uri: &str) -> Result<PathBuf> {
    let (bucket, prefix) = S3Client::parse_uri(table_uri)
        .with_context(|| format!("parse Iceberg table uri {table_uri}"))?;
    let prefix = prefix.trim_end_matches('/');
    let meta_prefix = if prefix.is_empty() {
        "metadata".to_string()
    } else {
        format!("{prefix}/metadata")
    };
    let client = S3Client::from_env();
    let hint_key = format!("{meta_prefix}/version-hint.text");
    let listed = list_metadata_json_objects(&client, &bucket, &meta_prefix);
    if let Ok(bytes) = client.get_object(&bucket, &hint_key) {
        let hint = String::from_utf8_lossy(&bytes);
        let hint = hint.trim();
        if !hint.is_empty() {
            if let Ok(files) = &listed {
                if let Some(p) = pick_metadata_from_hint(files, hint) {
                    return Ok(p);
                }
            }
            for name in version_hint_filenames(hint) {
                let key = format!("{meta_prefix}/{name}");
                if client.head_object(&bucket, &key).is_ok()
                    || client.get_object(&bucket, &key).is_ok()
                {
                    return Ok(PathBuf::from(S3Client::s3_uri(&bucket, &key)));
                }
            }
        }
    }
    let mut files =
        listed.with_context(|| format!("list Iceberg metadata s3://{bucket}/{meta_prefix}/"))?;
    anyhow::ensure!(
        !files.is_empty(),
        "no *.metadata.json under s3://{bucket}/{meta_prefix}/"
    );
    files.sort_by_key(|(v, _)| *v);
    Ok(files.pop().unwrap().1)
}

fn version_hint_filenames(hint: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut push = |s: String| {
        if !s.is_empty() && !names.contains(&s) {
            names.push(s);
        }
    };
    push(hint.to_string());
    if !hint.ends_with(".metadata.json") {
        push(format!("{hint}.metadata.json"));
    }
    let num = hint.strip_prefix('v').unwrap_or(hint);
    if let Ok(v) = num.parse::<i64>() {
        push(format!("v{v}.metadata.json"));
        push(format!("v{v:05}.metadata.json"));
        push(format!("{v:05}.metadata.json"));
        push(format!("{v}.metadata.json"));
    }
    names
}

fn pick_metadata_from_hint(files: &[(i64, PathBuf)], hint: &str) -> Option<PathBuf> {
    if hint.is_empty() {
        return None;
    }
    for name in version_hint_filenames(hint) {
        for (_, p) in files {
            if p.file_name().map(|n| n.to_string_lossy()) == Some(name.as_str().into()) {
                return Some(p.clone());
            }
        }
    }
    let num = hint.strip_prefix('v').unwrap_or(hint);
    let v: i64 = num.parse().ok()?;
    files
        .iter()
        .find(|(ver, _)| *ver == v)
        .map(|(_, p)| p.clone())
}

fn list_metadata_json_objects(
    client: &S3Client,
    bucket: &str,
    meta_prefix: &str,
) -> Result<Vec<(i64, PathBuf)>> {
    let list_prefix = format!("{}/", meta_prefix.trim_end_matches('/'));
    let keys = client.list_objects(bucket, &list_prefix)?;
    let mut out = Vec::new();
    for key in keys {
        let name = key.rsplit('/').next().unwrap_or(key.as_str());
        if !name.ends_with(".metadata.json") {
            continue;
        }
        if let Some(v) = parse_metadata_version(name) {
            out.push((v, PathBuf::from(S3Client::s3_uri(bucket, &key))));
        }
    }
    Ok(out)
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
    for ent in fs::read_dir(meta_dir).with_context(|| format!("read_dir {}", meta_dir.display()))? {
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

fn is_metadata_json_path(s: &str) -> bool {
    s.trim_end_matches('/')
        .split('?')
        .next()
        .unwrap_or(s)
        .ends_with(".metadata.json")
}

fn path_from_iceberg_uri(s: &str) -> PathBuf {
    let s = s.trim();
    if let Some(local) = strip_file_uri(s) {
        PathBuf::from(local)
    } else {
        PathBuf::from(s)
    }
}

fn rest_load_table(opts: &IcebergIndexOpts) -> Result<RestLoadTable> {
    let uri = opts
        .rest_uri
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| catalog_error("REST catalog requires --rest-uri"))?;
    let ns = opts
        .namespace
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| catalog_error("REST catalog requires --namespace"))?;
    let name = opts
        .table_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| catalog_error("REST catalog requires --table-name"))?;
    if !(uri.starts_with("http://") || uri.starts_with("https://")) {
        return Err(catalog_error(format!(
            "REST URI must be http:// or https://: {uri}"
        )));
    }
    let url = rest_table_url(uri, ns, name);
    let token = opts
        .rest_token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let (status, body) = rest_http_get(&url, token)?;
    if status == 401 || status == 403 {
        return Err(catalog_error(format!(
            "REST catalog GET {url} returned {status} (unauthorized; set --rest-token or NEEDLE_ICEBERG_TOKEN)"
        )));
    }
    if !(200..300).contains(&status) {
        let preview = String::from_utf8_lossy(&body);
        let preview: String = preview.chars().take(200).collect();
        return Err(catalog_error(format!(
            "REST catalog GET {url} returned {status}: {preview}"
        )));
    }
    serde_json::from_slice(&body)
        .map_err(|e| catalog_error(format!("REST catalog GET {url} returned invalid JSON: {e}")))
}

fn rest_table_url(rest_uri: &str, namespace: &str, table: &str) -> String {
    let base = rest_uri.trim().trim_end_matches('/');
    format!(
        "{base}/v1/namespaces/{}/tables/{}",
        encode_rest_namespace(namespace),
        encode_path_segment(table.trim())
    )
}

/// Iceberg REST: nested namespaces are unit-separator joined, then percent-encoded.
/// `a.b` → `a%1Fb`.
fn encode_rest_namespace(ns: &str) -> String {
    let joined = ns
        .trim()
        .split('.')
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join("\u{1F}");
    encode_path_segment(&joined)
}

fn encode_path_segment(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn rest_http_get(url: &str, bearer: Option<&str>) -> Result<(u16, Vec<u8>)> {
    let mut last_err = None;
    for attempt in 0..20 {
        match rest_http_get_once(url, bearer) {
            Ok(v) => return Ok(v),
            Err(e) => {
                let msg = format!("{e:#}");
                let transient = msg.contains("connect")
                    || msg.contains("Connection refused")
                    || msg.contains("Connection reset");
                last_err = Some(e);
                if !transient || attempt == 19 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    Err(catalog_error(format!(
        "REST GET {url} failed: {:#}",
        last_err.unwrap_or_else(|| anyhow::anyhow!("unknown error"))
    )))
}

struct RestUrl {
    tls: bool,
    sni: String,
    connect_addr: String,
    host_header: String,
    path: String,
}

fn parse_rest_url(url: &str) -> Result<RestUrl> {
    let url = url.split('#').next().unwrap_or(url).trim();
    let (tls, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        bail!("REST URL must be http:// or https://");
    };
    let rest = rest.split_once('?').map(|(a, _)| a).unwrap_or(rest);
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    if authority.is_empty() {
        bail!("REST URL missing host");
    }
    let (host, port) = split_host_port(authority, if tls { 443 } else { 80 })?;
    let connect_addr = format!("{host}:{port}");
    let host_header = if (tls && port == 443) || (!tls && port == 80) {
        host.to_string()
    } else {
        format!("{host}:{port}")
    };
    Ok(RestUrl {
        tls,
        sni: host.to_string(),
        connect_addr,
        host_header,
        path,
    })
}

fn split_host_port(authority: &str, default_port: u16) -> Result<(&str, u16)> {
    if let Some(h) = authority.strip_prefix('[') {
        let end = h
            .find(']')
            .ok_or_else(|| anyhow::anyhow!("invalid IPv6 host"))?;
        let host = &h[..end];
        let rest = &h[end + 1..];
        if let Some(p) = rest.strip_prefix(':') {
            let port: u16 = p.parse().context("invalid port")?;
            return Ok((host, port));
        }
        return Ok((host, default_port));
    }
    if let Some((host, p)) = authority.rsplit_once(':') {
        if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) {
            let port: u16 = p.parse().context("invalid port")?;
            return Ok((host, port));
        }
    }
    Ok((authority, default_port))
}

fn rest_http_get_once(url: &str, bearer: Option<&str>) -> Result<(u16, Vec<u8>)> {
    let parsed = parse_rest_url(url)?;
    let auth = match bearer {
        Some(t) => format!("Authorization: Bearer {t}\r\n"),
        None => String::new(),
    };
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Accept: application/json\r\n\
         {auth}\
         Connection: close\r\n\
         \r\n",
        path = parsed.path,
        host = parsed.host_header,
    );
    let tcp = TcpStream::connect(&parsed.connect_addr)
        .with_context(|| format!("connect {}", parsed.connect_addr))?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(Duration::from_secs(30)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(30)))?;
    if parsed.tls {
        let connector = native_tls::TlsConnector::new().context("tls connector")?;
        let mut stream = connector
            .connect(&parsed.sni, tcp)
            .map_err(|e| anyhow::anyhow!("tls handshake {}: {e}", parsed.sni))?;
        stream.write_all(req.as_bytes())?;
        stream.flush()?;
        rest_read_response(&mut stream)
    } else {
        let mut stream = tcp;
        stream.write_all(req.as_bytes())?;
        stream.flush()?;
        rest_read_response(&mut stream)
    }
}

fn rest_read_response(stream: &mut impl Read) -> Result<(u16, Vec<u8>)> {
    let mut resp = Vec::with_capacity(1024);
    let mut tmp = [0u8; 8192];
    let sep = loop {
        let n = match stream.read(&mut tmp) {
            Ok(0) => bail!("eof before HTTP headers"),
            Ok(n) => n,
            Err(e) => return Err(e.into()),
        };
        resp.extend_from_slice(&tmp[..n]);
        if let Some(s) = resp.windows(4).position(|w| w == b"\r\n\r\n") {
            break s;
        }
        if resp.len() > 1024 * 1024 {
            bail!("HTTP headers too large");
        }
    };
    let headers = std::str::from_utf8(&resp[..sep]).unwrap_or("");
    let status = headers
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let header_end = sep + 4;
    let mut body = resp[header_end..].to_vec();
    if let Some(need) = rest_content_length(headers) {
        while body.len() < need {
            let n = stream.read(&mut tmp)?;
            if n == 0 {
                bail!("eof in HTTP body ({} of {} bytes)", body.len(), need);
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(need);
    } else {
        loop {
            match stream.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => body.extend_from_slice(&tmp[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok((status, body))
}

fn rest_content_length(headers: &str) -> Option<usize> {
    headers.lines().find_map(|l| {
        let l = l.to_ascii_lowercase();
        l.strip_prefix("content-length:")
            .and_then(|v| v.trim().parse().ok())
    })
}

fn list_live_files(loaded: &LoadedTable) -> Result<Vec<PathBuf>> {
    Ok(list_snapshot_files(loaded)?.data)
}

fn table_root(loaded: &LoadedTable) -> &Path {
    loaded
        .metadata_path
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or_else(|| Path::new("."))
}

fn unsupported_deletes(why: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!("{UNSUPPORTED_DELETE_FILES} ({why})")
}

fn catalog_error(msg: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!("catalog_error: {msg}")
}

fn list_snapshot_files(loaded: &LoadedTable) -> Result<SnapshotFiles> {
    let table = table_root(loaded);
    let list_values = read_avro(&loaded.manifest_list)
        .with_context(|| format!("read manifest-list {}", loaded.manifest_list.display()))?;
    let mut data = Vec::new();
    let mut position_deletes = Vec::new();
    let mut equality_deletes = Vec::new();
    let mut seen_data = HashSet::new();
    let mut seen_pos = HashSet::new();
    let mut seen_eq = HashSet::new();
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
            let Some(fp) = avro_string_field(data_file, &["file_path", "file-path"]) else {
                continue;
            };
            let path = resolve_iceberg_path(&fp, table, &loaded.location);
            let key = normalize_file_key(&path.to_string_lossy());
            // Iceberg v2: content 0/missing = data, 1 = position deletes, 2 = equality deletes.
            match avro_i32_field(data_file, &["content"]) {
                Some(1) => {
                    reject_non_parquet_delete(data_file, &path)?;
                    if seen_pos.insert(key) {
                        position_deletes.push(path);
                    }
                }
                Some(2) => {
                    reject_non_parquet_delete(data_file, &path)?;
                    if seen_eq.insert(key) {
                        equality_deletes.push(path);
                    }
                }
                Some(0) | None => {
                    if seen_data.insert(key) {
                        data.push(path);
                    }
                }
                Some(other) => {
                    return Err(unsupported_deletes(format!(
                        "unknown data_file content={other}"
                    )));
                }
            }
        }
    }
    data.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));
    position_deletes.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));
    equality_deletes.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));
    Ok(SnapshotFiles {
        data,
        position_deletes,
        equality_deletes,
    })
}

fn reject_non_parquet_delete(data_file: &AvroValue, path: &Path) -> Result<()> {
    if let Some(fmt) = avro_string_field(data_file, &["file_format", "file-format"]) {
        let fmt = fmt.trim();
        if !fmt.is_empty() && !fmt.eq_ignore_ascii_case("PARQUET") {
            return Err(unsupported_deletes(format!(
                "delete file_format={fmt} (v3 deletion vectors / non-Parquet)"
            )));
        }
    }
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    if name.ends_with(".puffin") {
        return Err(unsupported_deletes(
            "Puffin deletion vector (apply deletes or compact first)",
        ));
    }
    Ok(())
}

fn load_snapshot_deletes(
    loaded: &LoadedTable,
    snap: &SnapshotFiles,
) -> Result<(
    HashMap<String, HashSet<u64>>,
    Vec<HashMap<String, Option<String>>>,
    HashSet<String>,
)> {
    let mut pos: HashMap<String, HashSet<u64>> = HashMap::new();
    for p in &snap.position_deletes {
        let rows = read_position_deletes(p)
            .map_err(|e| unsupported_deletes(format!("position delete {}: {e}", p.display())))?;
        for (fp, row) in rows {
            for k in index::delete_path_keys(&fp) {
                pos.entry(k).or_default().insert(row);
            }
            let resolved = resolve_iceberg_path(&fp, table_root(loaded), &loaded.location);
            for k in index::delete_path_keys(&resolved.to_string_lossy()) {
                pos.entry(k).or_default().insert(row);
            }
        }
    }
    let mut eq = Vec::new();
    for p in &snap.equality_deletes {
        let rows = read_equality_deletes(p)
            .map_err(|e| unsupported_deletes(format!("equality delete {}: {e}", p.display())))?;
        eq.extend(rows);
    }
    let mut idents = HashSet::new();
    for p in snap.position_deletes.iter().chain(&snap.equality_deletes) {
        idents.insert(normalize_file_key(&p.to_string_lossy()));
    }
    Ok((pos, eq, idents))
}

fn applied_iceberg_delete_keys(metas: &[IndexFragmentMeta]) -> HashSet<String> {
    metas
        .iter()
        .filter(|m| fragment_is_iceberg(m))
        .flat_map(|m| m.iceberg_delete_files.iter().map(|d| normalize_file_key(d)))
        .collect()
}

fn position_deletes_mention(map: &HashMap<String, HashSet<u64>>, path: &Path) -> bool {
    index::delete_path_keys(&path.to_string_lossy())
        .iter()
        .any(|k| map.get(k).is_some_and(|s| !s.is_empty()))
}

fn stored_paths_matching(
    metas: &[IndexFragmentMeta],
    index_root: &Path,
    data: &Path,
) -> Vec<String> {
    let want: HashSet<String> = file_match_keys(data, index_root).into_iter().collect();
    let mut out = Vec::new();
    let mut push = |s: String| {
        if !s.is_empty() && !out.contains(&s) {
            out.push(s);
        }
    };
    for meta in metas {
        if !fragment_is_iceberg(meta) {
            continue;
        }
        for stored in &meta.files {
            if stored_file_keys(stored, index_root)
                .iter()
                .any(|k| want.contains(k))
            {
                push(stored.clone());
            }
        }
    }
    for k in file_match_keys(data, index_root) {
        push(k);
    }
    out
}

fn parquet_batches(path: &Path) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let uri = path.to_string_lossy();
    if S3Client::is_remote_uri(&uri) {
        let (bucket, key) =
            S3Client::parse_uri(&uri).with_context(|| format!("parse object uri {uri}"))?;
        let reader = crate::s3::S3ChunkReader::open(S3Client::from_env(), bucket, key)
            .with_context(|| format!("s3 chunk reader {uri}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader)
            .with_context(|| format!("parquet builder {uri}"))?;
        let reader = builder
            .build()
            .with_context(|| format!("parquet reader {uri}"))?;
        return reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("parquet decode {uri}"));
    }
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("parquet builder {}", path.display()))?;
    let reader = builder
        .build()
        .with_context(|| format!("parquet reader {}", path.display()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parquet decode {}", path.display()))
}

fn parquet_schema(path: &Path) -> Result<arrow::datatypes::SchemaRef> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let uri = path.to_string_lossy();
    if S3Client::is_remote_uri(&uri) {
        let (bucket, key) = S3Client::parse_uri(&uri)?;
        let reader = crate::s3::S3ChunkReader::open(S3Client::from_env(), bucket, key)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
        return Ok(builder.schema().clone());
    }
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder.schema().clone())
}

fn read_position_deletes(path: &Path) -> Result<Vec<(String, u64)>> {
    let schema = parquet_schema(path)?;
    let has_file = schema
        .fields()
        .iter()
        .any(|f| avro_name_eq(f.name(), "file_path") || avro_name_eq(f.name(), "file-path"));
    let has_pos = schema
        .fields()
        .iter()
        .any(|f| avro_name_eq(f.name(), "pos"));
    if !has_file || !has_pos {
        bail!("position delete file missing file_path/pos columns");
    }
    let batches = parquet_batches(path)?;
    let mut out = Vec::new();
    for batch in &batches {
        let file_col = batch
            .column_by_name("file_path")
            .or_else(|| batch.column_by_name("file-path"))
            .context("position delete missing file_path")?;
        let pos_col = batch
            .column_by_name("pos")
            .context("position delete missing pos")?;
        for i in 0..batch.num_rows() {
            if file_col.is_null(i) || pos_col.is_null(i) {
                bail!("position delete row {i} has null file_path/pos");
            }
            let fp = index::encode_array_value(file_col.as_ref(), i)?;
            let pos = pos_i64(pos_col.as_ref(), i)
                .with_context(|| format!("position delete pos at row {i}"))?;
            if pos < 0 {
                bail!("negative pos {pos} in position delete file");
            }
            out.push((fp, pos as u64));
        }
    }
    Ok(out)
}

fn pos_i64(arr: &dyn arrow::array::Array, i: usize) -> Option<i64> {
    use arrow::array::{Int32Array, Int64Array, UInt32Array, UInt64Array};
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(i));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Some(a.value(i) as i64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return Some(a.value(i) as i64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
        return Some(a.value(i) as i64);
    }
    None
}

fn read_equality_deletes(path: &Path) -> Result<Vec<HashMap<String, Option<String>>>> {
    let schema = parquet_schema(path)?;
    if schema.fields().is_empty() {
        bail!("equality delete file has no columns");
    }
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let batches = parquet_batches(path)?;
    let mut out = Vec::new();
    for batch in &batches {
        for i in 0..batch.num_rows() {
            let mut rec = HashMap::new();
            for name in &cols {
                let arr = batch
                    .column_by_name(name)
                    .with_context(|| format!("equality delete missing column `{name}`"))?;
                let val = if arr.is_null(i) {
                    None
                } else {
                    Some(index::encode_array_value(arr.as_ref(), i)?)
                };
                rec.insert(name.clone(), val);
            }
            if rec.is_empty() {
                bail!("equality delete record has no fields");
            }
            out.push(rec);
        }
    }
    Ok(out)
}

fn read_avro(path: &Path) -> Result<Vec<AvroValue>> {
    let bytes = read_bytes(path).with_context(|| format!("open avro {}", path.display()))?;
    let reader =
        Reader::new(&bytes[..]).with_context(|| format!("avro reader {}", path.display()))?;
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
    avro_i32_field(entry, &["status"]).unwrap_or(0)
}

fn avro_i32_field(v: &AvroValue, names: &[&str]) -> Option<i32> {
    match avro_field(v, names).map(unwrap_union) {
        Some(AvroValue::Int(i)) => Some(*i),
        Some(AvroValue::Long(i)) => Some(*i as i32),
        Some(AvroValue::Enum(i, _)) => Some(*i as i32),
        _ => None,
    }
}

fn is_object_store_uri(s: &str) -> bool {
    S3Client::is_remote_uri(s) || s.starts_with("s3a://") || s.starts_with("s3n://")
}

/// Map Iceberg path strings onto local PathBufs / s3 URI PathBufs.
fn resolve_iceberg_path(raw: &str, table: &Path, table_location: &str) -> PathBuf {
    let raw = raw.trim();
    if is_object_store_uri(raw) {
        return PathBuf::from(raw);
    }
    if let Some(local) = strip_file_uri(raw) {
        return PathBuf::from(local);
    }
    let p = Path::new(raw);
    if p.is_absolute() {
        return p.to_path_buf();
    }
    if is_object_store_uri(table_location) {
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
    let registry = crate::index::read_registry(index_root)?;
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
    use crate::query::RapQuerier;
    use crate::writer::{write_sample_dataset, WriteMode, WriterOptions};
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
            {"name": "file_path", "type": "string"},
            {"name": "content", "type": "int", "default": 0}
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

    fn write_manifest_classified(
        path: &Path,
        data: &[PathBuf],
        position_deletes: &[PathBuf],
        equality_deletes: &[PathBuf],
        deleted: &[PathBuf],
    ) -> Result<()> {
        let schema = Schema::parse_str(MANIFEST_SCHEMA)?;
        let mut values = Vec::new();
        let groups: [(i32, i32, &[PathBuf]); 4] = [
            (1, 0, data),
            (1, 1, position_deletes),
            (1, 2, equality_deletes),
            (2, 0, deleted),
        ];
        for (status, content, files) in groups {
            for f in files {
                let mut rec = Record::new(&schema).expect("manifest_entry record");
                rec.put("status", status);
                rec.put(
                    "data_file",
                    AvroValue::Record(vec![
                        ("file_path".to_string(), AvroValue::String(file_uri(f))),
                        ("content".to_string(), AvroValue::Int(content)),
                    ]),
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
        write_iceberg_table_with_deletes(table, parquet, &[], &[], snapshot_id, version)
    }

    fn write_iceberg_table_with_deletes(
        table: &Path,
        data: &[PathBuf],
        position_deletes: &[PathBuf],
        equality_deletes: &[PathBuf],
        snapshot_id: i64,
        version: i64,
    ) -> Result<()> {
        let meta_dir = table.join("metadata");
        fs::create_dir_all(&meta_dir)?;
        let manifest = meta_dir.join(format!("manifest-{snapshot_id}.avro"));
        let list = meta_dir.join(format!("snap-{snapshot_id}-manifest-list.avro"));
        let deleted = vec![meta_dir.join("deleted-not-live.parquet")];
        write_manifest_classified(
            &manifest,
            data,
            position_deletes,
            equality_deletes,
            &deleted,
        )?;
        write_manifest_list(&list, &manifest)?;
        write_metadata(table, snapshot_id, version, &list)?;
        Ok(())
    }

    fn write_flat_rows(path: &Path, users: &[&str]) -> Result<()> {
        use arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("track_uri", DataType::Utf8, false),
            Field::new("duration_ms", DataType::Int64, false),
        ]));
        let n = users.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    users.iter().map(|s| *s).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMillisecondArray::from(
                    (0..n)
                        .map(|i| 1_700_000_000_000 + i as i64 * 1_000)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|i| format!("spotify:track:{i:08}"))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![180_000i64; n])),
            ],
        )?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn write_position_delete_parquet(path: &Path, deletes: &[(&Path, i64)]) -> Result<()> {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));
        let files: Vec<String> = deletes.iter().map(|(p, _)| file_uri(p)).collect();
        let pos: Vec<i64> = deletes.iter().map(|(_, p)| *p).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(files)),
                Arc::new(Int64Array::from(pos)),
            ],
        )?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn write_equality_delete_parquet(path: &Path, users: &[&str]) -> Result<()> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(
                users.iter().map(|s| *s).collect::<Vec<_>>(),
            ))],
        )?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn write_parquet_cols(path: &Path, names: &[&str], values: &[Vec<String>]) -> Result<()> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let fields: Vec<Field> = names
            .iter()
            .map(|n| Field::new(*n, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let cols: Vec<arrow::array::ArrayRef> = values
            .iter()
            .map(|c| {
                Arc::new(StringArray::from(
                    c.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as arrow::array::ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), cols)?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
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
            catalog: IcebergCatalog::Hadoop,
            rest_uri: None,
            namespace: None,
            table_name: None,
            rest_token: None,
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
        let listed_canon: HashSet<_> = listed.iter().map(|p| p.canonicalize().unwrap()).collect();
        for p in &paths {
            assert!(
                listed_canon.contains(&p.canonicalize().unwrap()),
                "missing {} in {listed:?}",
                p.display()
            );
        }
        assert!(
            listed
                .iter()
                .all(|p| !p.to_string_lossy().contains("deleted-not-live")),
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
        assert!(
            report.table_location.contains("table") || report.table_location.starts_with("file:")
        );

        let rap = load_index(&index).unwrap();
        let hits = rap.lookup("user_0000");
        assert!(!hits.is_empty(), "expected index hit for user_0000");
        assert_eq!(rap.fragments.len(), 1);
        assert_eq!(rap.fragments[0].note.as_deref(), Some("iceberg-snapshot:7"));
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

    #[test]
    fn index_iceberg_table_drops_removed_files_and_survives_compact() {
        use crate::index::compact_index;

        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data)).unwrap();
        assert!(paths.len() >= 2, "need two parquet files to drop one");
        write_iceberg_table(&table, &paths, 1, 1).unwrap();

        let opts = index_opts(&table, &index);
        let first = index_iceberg_table(&opts).unwrap();
        assert_eq!(first.files_indexed, paths.len());
        let rap = load_index(&index).unwrap();
        let gone_key = key_only_in(&rap, &paths[0]);
        let kept_key = key_only_in(&rap, &paths[1]);

        let kept = vec![paths[1].clone()];
        write_iceberg_table(&table, &kept, 2, 2).unwrap();
        let second = index_iceberg_table(&opts).unwrap();
        assert!(!second.skipped);
        assert_eq!(second.snapshot_id, 2);
        assert_eq!(second.files_indexed, 0, "no new parquet files");
        let rap = load_index(&index).unwrap();
        assert!(
            rap.lookup(&gone_key).is_empty(),
            "dropped file must hide {gone_key}"
        );
        assert!(
            !rap.lookup(&kept_key).is_empty(),
            "kept file must still hit {kept_key}"
        );
        assert!(
            rap.fragments.iter().any(|m| !m.dropped_files.is_empty()),
            "snapshot 2 fragment should list dropped files"
        );

        let report = compact_index(&index, Some("compact-ice")).unwrap();
        assert_eq!(report.fragment_id, "compact-ice");
        let rap = load_index(&index).unwrap();
        assert!(
            rap.lookup(&gone_key).is_empty(),
            "compact must not resurrect dropped Iceberg files ({gone_key})"
        );
        assert!(!rap.lookup(&kept_key).is_empty());
        assert_eq!(rap.fragments.len(), 1);
        assert_eq!(rap.fragments[0].iceberg_snapshot_id, Some(2));

        // Re-adding file 0 in a later snapshot must reindex it.
        write_iceberg_table(&table, &paths, 3, 3).unwrap();
        let third = index_iceberg_table(&opts).unwrap();
        assert!(!third.skipped);
        assert_eq!(third.files_indexed, 1, "re-added file 0 only");
        let rap = load_index(&index).unwrap();
        assert!(!rap.lookup(&gone_key).is_empty());
        assert!(!rap.lookup(&kept_key).is_empty());
    }

    fn key_only_in(index: &crate::index::RapIndex, want: &Path) -> String {
        let want = want.canonicalize().unwrap_or_else(|_| want.to_path_buf());
        for (k, ents) in &index.entries_by_key {
            if ents.is_empty() {
                continue;
            }
            let only_here = ents.iter().all(|e| {
                index
                    .file_path(e.file)
                    .ok()
                    .map(|p| p.canonicalize().unwrap_or_else(|_| p.to_path_buf()) == want)
                    .unwrap_or(false)
            });
            if only_here {
                return k.clone();
            }
        }
        panic!("no key lives only in {}", want.display());
    }

    #[test]
    fn position_delete_removes_key_other_keys_hit() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0001", "user_0002"]).unwrap();
        let pos_del = data.join("pos-del.parquet");
        write_position_delete_parquet(&pos_del, &[(&parquet, 0)]).unwrap();
        write_iceberg_table_with_deletes(&table, &[parquet], &[pos_del], &[], 1, 1).unwrap();

        let opts = index_opts(&table, &index);
        let report = index_iceberg_table(&opts).unwrap();
        assert!(!report.skipped);
        assert_eq!(report.files_indexed, 1);

        let rap = load_index(&index).unwrap();
        assert!(
            rap.lookup("user_0000").is_empty(),
            "position delete must hide user_0000"
        );
        assert!(!rap.lookup("user_0001").is_empty());
        assert!(!rap.lookup("user_0002").is_empty());
    }

    #[test]
    fn equality_delete_on_user_id_empties_lookup() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0001", "user_0002"]).unwrap();
        let eq_del = data.join("eq-del.parquet");
        write_equality_delete_parquet(&eq_del, &["user_0000"]).unwrap();
        write_iceberg_table_with_deletes(&table, &[parquet], &[], &[eq_del], 1, 1).unwrap();

        let opts = index_opts(&table, &index);
        index_iceberg_table(&opts).unwrap();

        let rap = load_index(&index).unwrap();
        assert!(
            rap.lookup("user_0000").is_empty(),
            "equality delete on user_id must hide user_0000"
        );
        assert!(!rap.lookup("user_0001").is_empty());
        assert!(!rap.lookup("user_0002").is_empty());
    }

    #[test]
    fn position_delete_one_of_two_rows_same_key() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0000", "user_0001"]).unwrap();
        let pos_del = data.join("pos-del.parquet");
        write_position_delete_parquet(&pos_del, &[(&parquet, 0)]).unwrap();
        write_iceberg_table_with_deletes(&table, &[parquet], &[pos_del], &[], 1, 1).unwrap();

        let opts = index_opts(&table, &index);
        index_iceberg_table(&opts).unwrap();

        let rap = load_index(&index).unwrap();
        let hits = rap.lookup("user_0000");
        assert!(!hits.is_empty());
        let rows: Vec<u64> = hits
            .iter()
            .flat_map(|e| e.row_numbers.iter().copied())
            .collect();
        assert_eq!(rows, vec![1], "surviving parquet pos for user_0000");
        assert!(!rap.lookup("user_0001").is_empty());

        let querier = RapQuerier::new(rap);
        let result = querier.query("user_0000").unwrap();
        assert_eq!(
            result.rows.len(),
            1,
            "query must return the surviving row only"
        );
        assert_eq!(result.rows[0].user_id, "user_0000");
        let other = querier.query("user_0001").unwrap();
        assert_eq!(other.rows.len(), 1);
    }

    #[test]
    fn unsupported_delete_content_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0001"]).unwrap();
        let bogus = data.join("bogus-del.parquet");
        write_flat_rows(&bogus, &["user_0000"]).unwrap();

        let meta_dir = table.join("metadata");
        fs::create_dir_all(&meta_dir).unwrap();
        let manifest = meta_dir.join("manifest-1.avro");
        let schema = Schema::parse_str(MANIFEST_SCHEMA).unwrap();
        let mut rec = Record::new(&schema).unwrap();
        rec.put("status", 1i32);
        rec.put(
            "data_file",
            AvroValue::Record(vec![
                (
                    "file_path".to_string(),
                    AvroValue::String(file_uri(&parquet)),
                ),
                ("content".to_string(), AvroValue::Int(0)),
            ]),
        );
        let mut rec99 = Record::new(&schema).unwrap();
        rec99.put("status", 1i32);
        rec99.put(
            "data_file",
            AvroValue::Record(vec![
                ("file_path".to_string(), AvroValue::String(file_uri(&bogus))),
                ("content".to_string(), AvroValue::Int(99)),
            ]),
        );
        write_avro_records(&manifest, MANIFEST_SCHEMA, vec![rec.into(), rec99.into()]).unwrap();
        let list = meta_dir.join("snap-1-manifest-list.avro");
        write_manifest_list(&list, &manifest).unwrap();
        write_metadata(&table, 1, 1, &list).unwrap();

        let opts = index_opts(&table, &index);
        let err = index_iceberg_table(&opts).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.to_ascii_lowercase().contains("unsupported delete"),
            "got: {msg}"
        );
        assert!(
            !index.join("registry.json").exists(),
            "must not publish an index that ignored delete files"
        );
    }

    #[test]
    fn unsupported_position_delete_missing_columns_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0001"]).unwrap();
        let bad = data.join("bad-pos-del.parquet");
        write_parquet_cols(&bad, &["foo"], &[vec!["x".to_string()]]).unwrap();
        write_iceberg_table_with_deletes(&table, &[parquet], &[bad], &[], 1, 1).unwrap();

        let opts = index_opts(&table, &index);
        let err = index_iceberg_table(&opts).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.to_ascii_lowercase().contains("unsupported delete"),
            "got: {msg}"
        );
        assert!(
            !index.join("registry.json").exists(),
            "must not publish an index that ignored delete files"
        );
    }

    #[test]
    fn incremental_position_delete_on_already_indexed_file() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0001"]).unwrap();
        write_iceberg_table(&table, &[parquet.clone()], 1, 1).unwrap();

        let opts = index_opts(&table, &index);
        index_iceberg_table(&opts).unwrap();
        let rap = load_index(&index).unwrap();
        assert!(!rap.lookup("user_0000").is_empty());

        let pos_del = data.join("pos-del.parquet");
        write_position_delete_parquet(&pos_del, &[(&parquet, 0)]).unwrap();
        write_iceberg_table_with_deletes(&table, &[parquet], &[pos_del], &[], 2, 2).unwrap();
        let second = index_iceberg_table(&opts).unwrap();
        assert!(!second.skipped);
        assert_eq!(second.snapshot_id, 2);
        assert!(
            second.files_indexed >= 1,
            "already-indexed data file must be re-scanned"
        );

        let rap = load_index(&index).unwrap();
        assert!(
            rap.lookup("user_0000").is_empty(),
            "new position delete must hide previously indexed row"
        );
        assert!(!rap.lookup("user_0001").is_empty());
    }

    #[test]
    fn resolve_iceberg_path_keeps_object_store_uris() {
        let table = Path::new("/tmp/iceberg-table");
        assert_eq!(
            resolve_iceberg_path("s3://bkt/wh/f.parquet", table, "s3://bkt/wh").to_string_lossy(),
            "s3://bkt/wh/f.parquet"
        );
        assert_eq!(
            resolve_iceberg_path(
                "https://bkt.s3.amazonaws.com/wh/f.parquet",
                table,
                "https://bkt.s3.amazonaws.com/wh"
            )
            .to_string_lossy(),
            "https://bkt.s3.amazonaws.com/wh/f.parquet"
        );
        assert_eq!(
            resolve_iceberg_path("metadata/m.avro", table, "s3://bkt/wh").to_string_lossy(),
            "s3://bkt/wh/metadata/m.avro"
        );
        assert_eq!(
            resolve_iceberg_path("data/f.parquet", table, "https://host/bkt/wh").to_string_lossy(),
            "https://host/bkt/wh/data/f.parquet"
        );
    }

    #[test]
    fn read_bytes_local_file() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("x.txt");
        fs::write(&p, b"abc").unwrap();
        assert_eq!(read_bytes(&p).unwrap(), b"abc");
    }

    #[test]
    fn version_hint_filenames_cover_common_layouts() {
        let names = version_hint_filenames("1");
        assert!(names.contains(&"v1.metadata.json".to_string()));
        assert!(names.contains(&"00001.metadata.json".to_string()));
        assert!(names.contains(&"1.metadata.json".to_string()));
    }

    #[test]
    fn iceberg_metadata_from_minio_if_present() {
        let client = S3Client::from_env();
        if std::net::TcpStream::connect(&client.cfg.endpoint).is_err() {
            eprintln!("skip: minio not listening");
            return;
        }
        let prefix = ".rap-iceberg-test/table1";
        let body = br#"{"format-version":2,"location":"s3://rap-lake/.rap-iceberg-test/table1","current-snapshot-id":9,"snapshots":[{"snapshot-id":9,"manifest-list":"s3://rap-lake/.rap-iceberg-test/table1/metadata/snap.avro"}]}"#;
        if client
            .put_object(
                "rap-lake",
                &format!("{prefix}/metadata/v1.metadata.json"),
                body,
            )
            .is_err()
        {
            eprintln!("skip: minio put failed");
            return;
        }
        let _ = client.put_object(
            "rap-lake",
            &format!("{prefix}/metadata/version-hint.text"),
            b"1",
        );
        let table = PathBuf::from(format!("s3://rap-lake/{prefix}"));
        let meta = find_current_metadata(&table).expect("remote metadata");
        assert!(
            meta.to_string_lossy().ends_with("v1.metadata.json"),
            "got {}",
            meta.display()
        );
        let bytes = read_bytes(&meta).expect("get metadata json");
        assert!(bytes.starts_with(b"{"));
    }

    fn blockers_http_get(url: &str, bearer: Option<&str>) -> (u16, String) {
        use std::io::{Read, Write};
        use std::net::TcpStream;
        let bare = url.strip_prefix("http://").expect("http url");
        let (hostport, path) = bare
            .split_once('/')
            .map(|(h, p)| (h, format!("/{p}")))
            .unwrap();
        let auth = bearer
            .map(|t| format!("Authorization: Bearer {t}\r\n"))
            .unwrap_or_default();
        let req =
            format!("GET {path} HTTP/1.1\r\nHost: {hostport}\r\n{auth}Connection: close\r\n\r\n");
        let mut tcp = TcpStream::connect(hostport).unwrap();
        tcp.write_all(req.as_bytes()).unwrap();
        let mut buf = Vec::new();
        tcp.read_to_end(&mut buf).unwrap();
        let text = String::from_utf8_lossy(&buf);
        let status = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    #[test]
    fn blockers_position_delete_stale_identity_and_needled_token() {
        use crate::index::STALE_FILE_IDENTITY;
        use crate::query::QueryOptions;
        use crate::server::{start, DaemonOptions};

        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0001", "user_0002"]).unwrap();
        let pos_del = data.join("pos-del.parquet");
        write_position_delete_parquet(&pos_del, &[(&parquet, 0)]).unwrap();
        write_iceberg_table_with_deletes(&table, &[parquet.clone()], &[pos_del], &[], 1, 1)
            .unwrap();
        index_iceberg_table(&index_opts(&table, &index)).unwrap();

        let rap = load_index(&index).unwrap();
        assert!(
            rap.lookup("user_0000").is_empty(),
            "deleted row must not come back"
        );
        assert!(!rap.lookup("user_0001").is_empty());

        let token = "blocker-token";
        let handle = start(DaemonOptions {
            index: index.clone(),
            bind: "127.0.0.1:0".into(),
            token: Some(token.into()),
            ..Default::default()
        })
        .expect("start needled");
        let base = handle.base_url();
        let (st, body) = blockers_http_get(&format!("{base}/v1/query?key=user_0001"), None);
        assert_eq!(st, 401, "unauth must 401, body={body}");
        assert!(!body.contains(index.to_string_lossy().as_ref()));
        let (st, body) = blockers_http_get(&format!("{base}/v1/query?key=user_0001"), Some(token));
        assert_eq!(st, 200, "auth query, body={body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(!v["rows"].as_array().unwrap().is_empty());
        handle.stop();

        // Change mtime only so Parquet still decodes under --no-verify.
        let st = std::process::Command::new("touch")
            .args(["-d", "1970-01-02 00:00:00 UTC", parquet.to_str().unwrap()])
            .status()
            .expect("touch");
        assert!(st.success(), "touch mtime");

        let rap = load_index(&index).unwrap();
        let err = RapQuerier::new(rap)
            .query("user_0001")
            .expect_err("identity mismatch must fail closed");
        let msg = format!("{err:#}");
        assert!(
            msg.contains(STALE_FILE_IDENTITY),
            "expected stale_file_identity, got {msg}"
        );
        let ok = RapQuerier::new(load_index(&index).unwrap())
            .query_with(
                "user_0001",
                &QueryOptions {
                    verify: false,
                    ..QueryOptions::default()
                },
            )
            .unwrap();
        assert!(
            ok.batch.num_rows() > 0 || !ok.rows.is_empty(),
            " --no-verify must still decode"
        );
    }

    struct RestCatalogMock {
        addr: std::net::SocketAddr,
        handle: Option<std::thread::JoinHandle<()>>,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl RestCatalogMock {
        fn start(expected_path: &str, body: String, required_token: Option<String>) -> Self {
            let server = tiny_http::Server::http("127.0.0.1:0").expect("bind rest mock");
            let addr = server.server_addr().to_ip().expect("ip addr");
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let stop_c = std::sync::Arc::clone(&stop);
            let expected_path = expected_path.to_string();
            let handle = std::thread::spawn(move || {
                rest_mock_loop(server, expected_path, body, required_token, stop_c);
            });
            std::thread::sleep(std::time::Duration::from_millis(20));
            Self {
                addr,
                handle: Some(handle),
                stop,
            }
        }

        fn rest_uri(&self) -> String {
            format!("http://{}/iceberg", self.addr)
        }
    }

    impl Drop for RestCatalogMock {
        fn drop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = std::net::TcpStream::connect(self.addr);
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
        }
    }

    fn rest_mock_loop(
        server: tiny_http::Server,
        expected_path: String,
        body: String,
        required_token: Option<String>,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        loop {
            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            let request = match server.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(Some(r)) => r,
                Ok(None) => continue,
                Err(_) => break,
            };
            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = request.respond(tiny_http::Response::empty(503));
                break;
            }
            let url = request.url().to_string();
            let path = url.split('?').next().unwrap_or(&url);
            let path_ok = path == expected_path || path.ends_with(&expected_path);
            if request.method() != &tiny_http::Method::Get || !path_ok {
                let _ = request
                    .respond(tiny_http::Response::from_string("not found").with_status_code(404));
                continue;
            }
            if let Some(tok) = &required_token {
                let got = rest_mock_bearer(&request);
                if got.as_deref() != Some(tok.as_str()) {
                    let _ = request.respond(
                        tiny_http::Response::from_string(r#"{"error":"unauthorized"}"#)
                            .with_status_code(401),
                    );
                    continue;
                }
            }
            let resp = tiny_http::Response::from_string(body.clone())
                .with_status_code(200)
                .with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
            let _ = request.respond(resp);
        }
    }

    fn rest_mock_bearer(request: &tiny_http::Request) -> Option<String> {
        for h in request.headers() {
            if !h.field.equiv("Authorization") {
                continue;
            }
            let v = h.value.as_str().trim();
            if v.len() >= 7 && v[..7].eq_ignore_ascii_case("Bearer ") {
                return Some(v[7..].trim().to_string());
            }
        }
        None
    }

    fn rest_index_opts(rest_uri: &str, index: &Path, token: Option<&str>) -> IcebergIndexOpts {
        IcebergIndexOpts {
            table: PathBuf::new(),
            index: index.to_path_buf(),
            key_columns: vec!["user_id".to_string()],
            value_columns: Vec::new(),
            covering: true,
            buckets: 8,
            fragment_prefix: "iceberg".to_string(),
            catalog: IcebergCatalog::Rest,
            rest_uri: Some(rest_uri.to_string()),
            namespace: Some("db".to_string()),
            table_name: Some("tbl".to_string()),
            rest_token: token.map(|s| s.to_string()),
        }
    }

    #[test]
    fn rest_namespace_encodes_unit_separator() {
        assert_eq!(encode_rest_namespace("db"), "db");
        assert_eq!(encode_rest_namespace("a.b"), "a%1Fb");
        assert_eq!(encode_rest_namespace("a.b.c"), "a%1Fb%1Fc");
        assert_eq!(
            rest_table_url("http://host/iceberg/", "a.b", "tbl"),
            "http://host/iceberg/v1/namespaces/a%1Fb/tables/tbl"
        );
    }

    #[test]
    fn index_iceberg_table_via_rest_applies_position_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0001", "user_0002"]).unwrap();
        let pos_del = data.join("pos-del.parquet");
        write_position_delete_parquet(&pos_del, &[(&parquet, 0)]).unwrap();
        write_iceberg_table_with_deletes(&table, &[parquet], &[pos_del], &[], 1, 1).unwrap();

        let body = serde_json::json!({
            "metadata-location": file_uri(&table),
        })
        .to_string();
        let mock = RestCatalogMock::start("/iceberg/v1/namespaces/db/tables/tbl", body, None);
        let opts = rest_index_opts(&mock.rest_uri(), &index, None);
        let report = index_iceberg_table(&opts).unwrap();
        assert!(!report.skipped);
        assert_eq!(report.files_indexed, 1);

        let rap = load_index(&index).unwrap();
        assert!(
            rap.lookup("user_0000").is_empty(),
            "REST-discovered position delete must hide user_0000"
        );
        assert!(!rap.lookup("user_0001").is_empty());
        assert!(!rap.lookup("user_0002").is_empty());
    }

    #[test]
    fn rest_catalog_bearer_required() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0001"]).unwrap();
        write_iceberg_table(&table, &[parquet], 1, 1).unwrap();

        let meta_loc = table
            .join("metadata")
            .join("v1.metadata.json")
            .canonicalize()
            .unwrap();
        let body = serde_json::json!({
            "metadata-location": format!("file://{}", meta_loc.display()),
        })
        .to_string();
        let token = "rest-secret";
        let mock = RestCatalogMock::start(
            "/iceberg/v1/namespaces/db/tables/tbl",
            body,
            Some(token.to_string()),
        );

        let unauth = rest_index_opts(&mock.rest_uri(), &index, None);
        let err = index_iceberg_table(&unauth).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("catalog_error"),
            "401 must be catalog_error, got {msg}"
        );
        assert!(
            msg.contains("401"),
            "401 status must appear in catalog_error, got {msg}"
        );
        assert!(
            !index.join("registry.json").exists(),
            "must not publish an index after catalog 401"
        );

        let auth = rest_index_opts(&mock.rest_uri(), &index, Some(token));
        let report = index_iceberg_table(&auth).unwrap();
        assert!(!report.skipped);
        let rap = load_index(&index).unwrap();
        assert!(!rap.lookup("user_0000").is_empty());
        assert!(!rap.lookup("user_0001").is_empty());
    }

    #[test]
    fn glue_and_nessie_are_unsupported_no_hadoop_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let index = tmp.path().join("rap-index");
        for cat in [IcebergCatalog::Glue, IcebergCatalog::Nessie] {
            let opts = IcebergIndexOpts {
                table: tmp.path().join("would-be-hadoop"),
                index: index.clone(),
                key_columns: vec!["user_id".to_string()],
                value_columns: Vec::new(),
                covering: false,
                buckets: 8,
                fragment_prefix: "iceberg".to_string(),
                catalog: cat,
                rest_uri: None,
                namespace: None,
                table_name: None,
                rest_token: None,
            };
            let err = index_iceberg_table(&opts).expect_err("unsupported catalog must error");
            let msg = format!("{err:#}");
            assert!(msg.contains("unsupported catalog"), "got {msg}");
            assert!(
                msg.contains(cat.as_str()),
                "error must name {}: {msg}",
                cat.as_str()
            );
            assert!(
                !index.join("registry.json").exists(),
                "must not publish an index for unsupported catalog"
            );
        }
    }

    struct IntegratorS3Mock {
        endpoint: String,
        hits: std::sync::Arc<std::sync::atomic::AtomicU64>,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        replaced: std::sync::Arc<std::sync::atomic::AtomicBool>,
        join: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for IntegratorS3Mock {
        fn drop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = TcpStream::connect(&self.endpoint);
            if let Some(j) = self.join.take() {
                let _ = j.join();
            }
        }
    }

    fn start_integrator_s3_mock(object: Vec<u8>, required_token: &'static str) -> IntegratorS3Mock {
        use std::io::Read;
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind integrator s3");
        let endpoint = listener.local_addr().unwrap().to_string();
        let hits = std::sync::Arc::new(AtomicU64::new(0));
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let replaced = std::sync::Arc::new(AtomicBool::new(false));
        let hits_c = hits.clone();
        let stop_c = stop.clone();
        let replaced_c = replaced.clone();
        let join = std::thread::spawn(move || {
            for incoming in listener.incoming() {
                if stop_c.load(Ordering::SeqCst) {
                    break;
                }
                let mut stream = match incoming {
                    Ok(s) => s,
                    Err(_) => break,
                };
                if stop_c.load(Ordering::SeqCst) {
                    break;
                }
                let _ = stream.set_nodelay(true);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                let header_end = loop {
                    match stream.read(&mut tmp) {
                        Ok(0) => break None,
                        Ok(n) => {
                            buf.extend_from_slice(&tmp[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break buf.windows(4).position(|w| w == b"\r\n\r\n");
                            }
                            if buf.len() > 1024 * 1024 {
                                break None;
                            }
                        }
                        Err(_) => break None,
                    }
                };
                let Some(sep) = header_end else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&buf[..sep]).into_owned();
                let n = hits_c.fetch_add(1, Ordering::SeqCst) + 1;
                let (status, extra, body) = integrator_s3_handle(
                    n,
                    &headers,
                    &object,
                    required_token,
                    replaced_c.load(Ordering::SeqCst),
                );
                let reason = match status {
                    200 => "OK",
                    206 => "Partial Content",
                    403 => "Forbidden",
                    503 => "Service Unavailable",
                    _ => "Error",
                };
                let mut resp = format!("HTTP/1.1 {status} {reason}\r\nConnection: close\r\n");
                let has_cl = extra
                    .iter()
                    .any(|(k, _)| k.eq_ignore_ascii_case("Content-Length"));
                for (k, v) in extra {
                    resp.push_str(&format!("{k}: {v}\r\n"));
                }
                if !has_cl {
                    resp.push_str(&format!("Content-Length: {}\r\n", body.len()));
                }
                resp.push_str("\r\n");
                let _ = stream.write_all(resp.as_bytes());
                if !headers.starts_with("HEAD ") {
                    let _ = stream.write_all(&body);
                }
                let _ = stream.flush();
            }
        });
        IntegratorS3Mock {
            endpoint,
            hits,
            stop,
            replaced,
            join: Some(join),
        }
    }

    fn integrator_s3_handle(
        n: u64,
        headers: &str,
        object: &[u8],
        required_token: &str,
        replaced: bool,
    ) -> (u16, Vec<(String, String)>, Vec<u8>) {
        if n == 1 {
            return (
                503,
                vec![("Content-Type".into(), "application/xml".into())],
                b"<Error>slow down</Error>".to_vec(),
            );
        }
        let has_tok = headers.lines().any(|l| {
            let l = l.to_ascii_lowercase();
            l.starts_with("x-amz-security-token:")
                && l.contains(&required_token.to_ascii_lowercase())
        });
        if !has_tok {
            return (403, vec![], b"missing token".to_vec());
        }
        let method = headers
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().next())
            .unwrap_or("GET");
        let len = object.len() as u64;
        let (etag, size) = if replaced {
            ("etag-replaced", len + 1)
        } else {
            ("e2eetag", len)
        };
        if method.eq_ignore_ascii_case("HEAD") {
            return (
                200,
                vec![
                    ("Content-Length".into(), size.to_string()),
                    ("ETag".into(), format!("\"{etag}\"")),
                ],
                Vec::new(),
            );
        }
        if replaced {
            // Identity already fails on HEAD; keep GET from succeeding if a client skipped verify.
            return (
                200,
                vec![
                    ("Content-Length".into(), size.to_string()),
                    ("ETag".into(), format!("\"{etag}\"")),
                ],
                vec![0u8; size as usize],
            );
        }
        if let Some(range) = integrator_parse_range(headers, object.len()) {
            let (start, end_incl) = range;
            let slice = &object[start..=end_incl];
            return (
                206,
                vec![
                    (
                        "Content-Range".into(),
                        format!("bytes {start}-{end_incl}/{}", object.len()),
                    ),
                    ("Content-Length".into(), slice.len().to_string()),
                    ("ETag".into(), format!("\"{etag}\"")),
                ],
                slice.to_vec(),
            );
        }
        (
            200,
            vec![
                ("Content-Length".into(), object.len().to_string()),
                ("ETag".into(), format!("\"{etag}\"")),
            ],
            object.to_vec(),
        )
    }

    fn integrator_parse_range(headers: &str, len: usize) -> Option<(usize, usize)> {
        for line in headers.lines() {
            let Some(rest) = line.split_once(':') else {
                continue;
            };
            if !rest.0.trim().eq_ignore_ascii_case("range") {
                continue;
            }
            let v = rest.1.trim();
            let v = v.strip_prefix("bytes=")?;
            let (a, b) = v.split_once('-')?;
            let start: usize = a.parse().ok()?;
            let end_incl: usize = if b.is_empty() {
                len.saturating_sub(1)
            } else {
                b.parse().ok()?
            };
            if start < len && end_incl < len && start <= end_incl {
                return Some((start, end_incl));
            }
        }
        None
    }

    fn integrator_s3_client(endpoint: &str, session_token: Option<&str>) -> crate::s3::S3Client {
        crate::s3::S3Client::new(crate::s3::S3Config {
            endpoint: endpoint.to_string(),
            access_key: "AKIA_TEST".into(),
            secret_key: "testsecret".into(),
            session_token: session_token.map(|s| s.to_string()),
            region: "us-east-1".into(),
            anonymous_read: false,
            use_tls: false,
            path_style: true,
        })
    }

    fn integrator_http_get(url: &str, bearer: Option<&str>) -> (u16, String) {
        let (tls, bare) = if let Some(b) = url.strip_prefix("https://") {
            (true, b)
        } else if let Some(b) = url.strip_prefix("http://") {
            (false, b)
        } else {
            panic!("url must be http(s): {url}");
        };
        let (hostport, path) = match bare.split_once('/') {
            Some((h, p)) => (h, format!("/{p}")),
            None => (bare, "/".to_string()),
        };
        let auth = bearer
            .map(|t| format!("Authorization: Bearer {t}\r\n"))
            .unwrap_or_default();
        let req =
            format!("GET {path} HTTP/1.1\r\nHost: {hostport}\r\n{auth}Connection: close\r\n\r\n");
        let mut last = None;
        for _ in 0..25 {
            match integrator_http_once(hostport, &req, tls) {
                Ok(v) => return v,
                Err(e) => {
                    last = Some(e);
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
        panic!("http get {url} failed: {last:?}");
    }

    fn integrator_http_once(hostport: &str, req: &str, tls: bool) -> Result<(u16, String)> {
        let tcp = TcpStream::connect(hostport)?;
        tcp.set_read_timeout(Some(Duration::from_secs(10)))?;
        tcp.set_write_timeout(Some(Duration::from_secs(10)))?;
        if tls {
            let connector = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .context("tls connector")?;
            let stream = connector
                .connect("localhost", tcp)
                .map_err(|e| anyhow::anyhow!("tls handshake: {e}"))?;
            integrator_read_http(stream, req)
        } else {
            integrator_read_http(tcp, req)
        }
    }

    fn integrator_read_http(mut stream: impl Read + Write, req: &str) -> Result<(u16, String)> {
        stream.write_all(req.as_bytes())?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf)?;
        let text = String::from_utf8_lossy(&buf);
        let status = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        Ok((status, body))
    }

    fn openssl_ok() -> bool {
        std::process::Command::new("openssl")
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn gen_self_signed(dir: &Path) -> Result<(PathBuf, PathBuf)> {
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        let st = std::process::Command::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout"])
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .args(["-days", "1", "-subj", "/CN=localhost"])
            .status()
            .context("openssl req")?;
        if !st.success() {
            bail!("openssl req failed");
        }
        Ok((cert, key))
    }

    /// Integrator A: REST catalog + S3 STS mock + TLS needled + position delete + replaced object.
    #[test]
    fn integrator_a_rest_s3_sts_tls_position_delete_and_stale() {
        use crate::index::STALE_FILE_IDENTITY;
        use crate::query::QueryOptions;
        use crate::server::{start, DaemonOptions};

        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("table");
        let data = table.join("data");
        let index = tmp.path().join("rap-index");
        let parquet = data.join("rows.parquet");
        write_flat_rows(&parquet, &["user_0000", "user_0001", "user_0002"]).unwrap();
        let pos_del = data.join("pos-del.parquet");
        write_position_delete_parquet(&pos_del, &[(&parquet, 0)]).unwrap();
        write_iceberg_table_with_deletes(&table, &[parquet.clone()], &[pos_del], &[], 1, 1)
            .unwrap();

        let body = serde_json::json!({
            "metadata-location": file_uri(&table),
        })
        .to_string();
        let rest = RestCatalogMock::start("/iceberg/v1/namespaces/db/tables/tbl", body, None);
        let report = index_iceberg_table(&rest_index_opts(&rest.rest_uri(), &index, None)).unwrap();
        assert!(!report.skipped);
        assert_eq!(report.files_indexed, 1);

        let rap = load_index(&index).unwrap();
        assert!(
            rap.lookup("user_0000").is_empty(),
            "position delete must hide user_0000"
        );
        assert!(!rap.lookup("user_0001").is_empty());

        let token = "integrator-a-token";
        let tls_pair = if openssl_ok() {
            gen_self_signed(tmp.path()).ok()
        } else {
            None
        };
        let (tls_cert, tls_key) = match &tls_pair {
            Some((c, k)) => (Some(c.clone()), Some(k.clone())),
            None => (None, None),
        };
        let handle = start(DaemonOptions {
            index: index.clone(),
            bind: "127.0.0.1:0".into(),
            token: Some(token.into()),
            tls_cert,
            tls_key,
            ..Default::default()
        })
        .expect("start needled");
        let base = handle.base_url();
        if tls_pair.is_some() {
            assert!(
                base.starts_with("https://127.0.0.1:"),
                "expected TLS needled, got {base}"
            );
        }

        let (st, body) = integrator_http_get(&format!("{base}/v1/query?key=user_0001"), None);
        assert_eq!(st, 401, "unauth must 401, body={body}");
        let err: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(err["error"], "unauthenticated");
        assert!(!body.contains(index.to_string_lossy().as_ref()));

        let (st, body) =
            integrator_http_get(&format!("{base}/v1/query?key=user_0001"), Some(token));
        assert_eq!(st, 200, "auth live key, body={body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(!v["rows"].as_array().unwrap().is_empty());

        let (st, body) =
            integrator_http_get(&format!("{base}/v1/query?key=user_0000"), Some(token));
        assert_eq!(st, 200, "deleted key is empty rows not 401, body={body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(
            v["rows"].as_array().unwrap().is_empty(),
            "position-deleted key must not return rows: {body}"
        );

        let (st, body) = integrator_http_get(&format!("{base}/no-such"), Some(token));
        assert_eq!(st, 404, "unknown path, body={body}");
        handle.stop();

        let parquet_bytes = fs::read(&parquet).unwrap();
        let s3 = start_integrator_s3_mock(parquet_bytes.clone(), "sts-session-token");
        let mut remote = load_index(&index).unwrap();
        remote.files =
            crate::index::FileDict::from_paths(vec![PathBuf::from("s3://needle-e2e/rows.parquet")]);
        for entries in remote.entries_by_key.values_mut() {
            for e in entries {
                e.file_etag = Some("e2eetag".into());
                e.file_size = Some(parquet_bytes.len() as u64);
                e.file_mtime_ms = None;
            }
        }

        let denied = RapQuerier::new(clone_rap_index(&remote))
            .with_s3(integrator_s3_client(&s3.endpoint, None));
        let err = denied
            .query("user_0001")
            .expect_err("S3 without STS token must fail closed");
        let msg = format!("{err:#}");
        assert!(
            msg.contains(STALE_FILE_IDENTITY)
                || msg.contains("s3_identity_mismatch")
                || msg.contains("403"),
            "expected identity/auth fail closed, got {msg}"
        );

        let allowed = RapQuerier::new(clone_rap_index(&remote)).with_s3(integrator_s3_client(
            &s3.endpoint,
            Some("sts-session-token"),
        ));
        let ok = allowed
            .query("user_0001")
            .expect("STS + 503 retry must succeed");
        assert!(
            ok.batch.num_rows() > 0 || !ok.rows.is_empty(),
            "live key via S3 STS must decode"
        );
        assert!(
            s3.hits.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "503 must be retried"
        );

        s3.replaced.store(true, std::sync::atomic::Ordering::SeqCst);
        let stale = RapQuerier::new(clone_rap_index(&remote)).with_s3(integrator_s3_client(
            &s3.endpoint,
            Some("sts-session-token"),
        ));
        let err = stale
            .query("user_0001")
            .expect_err("replaced S3 object must fail closed");
        let msg = format!("{err:#}");
        assert!(
            msg.contains(STALE_FILE_IDENTITY) || msg.contains("s3_identity_mismatch"),
            "expected stale/s3 identity mismatch, got {msg}"
        );

        let st = std::process::Command::new("touch")
            .args(["-d", "1970-01-02 00:00:00 UTC", parquet.to_str().unwrap()])
            .status()
            .expect("touch");
        assert!(st.success(), "touch mtime");
        let err = RapQuerier::new(load_index(&index).unwrap())
            .query("user_0001")
            .expect_err("local mtime change must fail closed");
        let msg = format!("{err:#}");
        assert!(
            msg.contains(STALE_FILE_IDENTITY),
            "expected stale_file_identity, got {msg}"
        );
        let ok = RapQuerier::new(load_index(&index).unwrap())
            .query_with(
                "user_0001",
                &QueryOptions {
                    verify: false,
                    ..QueryOptions::default()
                },
            )
            .unwrap();
        assert!(
            ok.batch.num_rows() > 0 || !ok.rows.is_empty(),
            "--no-verify must still decode"
        );
    }

    fn clone_rap_index(src: &crate::index::RapIndex) -> crate::index::RapIndex {
        crate::index::RapIndex {
            files: src.files.clone(),
            entries_by_key: src.entries_by_key.clone(),
            fragments: src.fragments.clone(),
            root: src.root.clone(),
        }
    }
}
