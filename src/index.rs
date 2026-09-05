//! External RAP index: multimap key → {file ordinal, row numbers, value_count, …}.
//!
//! Article: "The index is a multimap - a single key can have entries across many
//! files and partitions. … Large indexes distribute naturally by hash bucketing.
//! The index grows by appending fragments, not modifying existing ones."
//!
//! Extended fields for write-time preparation:
//! - `page_locs` - one-page-per-key locations stored in the entry (no footer needed)
//! - `frame_locs` - ZSTD frame offset+size per column
//! - `contiguous` - interleaved multi-column span for one ranged read

use crate::prepared::{self, ByteSpan, FrameLoc, PreparedManifest};
use crate::s3::{S3ChunkReader, S3Client};
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

thread_local! {
    static BUCKET_LOADS: Cell<u64> = const { Cell::new(0) };
    static BUCKET_CAP: Cell<u64> = const { Cell::new(u64::MAX) };
}

/// Test hook: fail if this thread deserializes more hash buckets than `cap`.
pub fn test_set_bucket_load_cap(cap: u64) {
    BUCKET_LOADS.with(|c| c.set(0));
    BUCKET_CAP.with(|c| c.set(cap));
}

/// Restore the unlimited bucket-load cap for this thread.
pub fn test_clear_bucket_load_cap() {
    test_set_bucket_load_cap(u64::MAX);
}

fn account_bucket_load() -> Result<()> {
    let n = BUCKET_LOADS.with(|c| {
        let n = c.get().saturating_add(1);
        c.set(n);
        n
    });
    let max = BUCKET_CAP.with(|c| c.get());
    if n > max {
        bail!(
            "index working set exceeded bucket cap ({n} > {max}); \
             point lookup deserializes one mmapped hash bucket"
        );
    }
    Ok(())
}

/// On-disk `registry.json` major version written by this crate.
/// Format v1 is frozen; see `FORMAT.md`.
pub const INDEX_FORMAT_VERSION: u32 = 1;

/// Exclusive writer lock (`flock` on `<index>/.needle.lock`).
#[must_use]
#[derive(Debug)]
pub struct IndexWriteLock {
    _file: File,
}

/// `registry.json` object form (always written; legacy arrays are still read).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexRegistry {
    pub format_version: u32,
    pub fragments: Vec<String>,
}

/// Per-column page location stored directly in the index (article: one page per key).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PageLoc {
    pub column: String,
    pub offset: u64,
    pub size: u32,
}

/// One index entry: key lives in a specific file at specific rows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RapIndexEntry {
    pub key: String,
    pub file: u32,
    pub row_numbers: Vec<u64>,
    #[serde(default)]
    pub value_count: Option<u64>,
    #[serde(default)]
    pub covering: Option<CoveringValues>,
    /// Page locations when writer flushed one page per key (preferred over footer).
    #[serde(default)]
    pub page_locs: Option<Vec<PageLoc>>,
    /// ZSTD frame locations (prepared layout).
    #[serde(default)]
    pub frame_locs: Option<Vec<FrameLoc>>,
    /// Contiguous multi-column span (interleaved layout).
    #[serde(default)]
    pub contiguous: Option<ByteSpan>,
    /// Prepared data file relative path (legacy .rapz/.rapi). None when frames live in the Parquet file.
    #[serde(default)]
    pub prepared_file: Option<String>,
    /// Key's primary fetch is 4KB-aligned.
    #[serde(default)]
    pub aligned: Option<bool>,
    /// True = forget this key (drop prior entries for the key).
    #[serde(default)]
    pub tombstone: bool,
    #[serde(default)]
    pub file_etag: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
    #[serde(default)]
    pub file_mtime_ms: Option<i64>,
}

/// Identity of a data file referenced by a fragment (size / etag / mtime for staleness checks).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct FileIdent {
    pub path: String,
    #[serde(default)]
    pub etag: Option<String>,
    #[serde(default)]
    pub size: Option<u64>,
    /// Local filesystem mtime in unix milliseconds. Remote objects leave this unset.
    #[serde(default)]
    pub mtime_ms: Option<i64>,
}

impl FileIdent {
    /// True when at least one identity field was recorded.
    pub fn has_identity(&self) -> bool {
        self.etag.is_some() || self.size.is_some() || self.mtime_ms.is_some()
    }
}

/// Token embedded in query errors when a live object no longer matches the index.
pub const STALE_FILE_IDENTITY: &str = "stale_file_identity";

/// Compare stored vs live identity. Only fields present on **both** sides participate.
/// Size, ETag, and mtime mismatches are all stale.
pub fn file_ident_mismatch(stored: &FileIdent, live: &FileIdent) -> bool {
    let size_mismatch = match (stored.size, live.size) {
        (Some(want), Some(got)) => want != got,
        _ => false,
    };
    let etag_mismatch = match (&stored.etag, &live.etag) {
        (Some(want), Some(got)) => want != got,
        _ => false,
    };
    let mtime_mismatch = match (stored.mtime_ms, live.mtime_ms) {
        (Some(want), Some(got)) => want != got,
        _ => false,
    };
    size_mismatch || etag_mismatch || mtime_mismatch
}

/// Structured `stale_file_identity` error (JSON object in the message).
pub fn stale_file_identity_error(stored: &FileIdent, live: Option<&FileIdent>) -> anyhow::Error {
    let payload = serde_json::json!({
        "error": STALE_FILE_IDENTITY,
        "path": stored.path,
        "stored": {
            "etag": stored.etag,
            "size": stored.size,
            "mtime_ms": stored.mtime_ms,
        },
        "live": live.map(|l| serde_json::json!({
            "etag": l.etag,
            "size": l.size,
            "mtime_ms": l.mtime_ms,
        })),
    });
    anyhow!("{payload}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CoveringValues {
    pub listen_count: u64,
    pub total_duration_ms: u64,
    #[serde(default)]
    pub min_ts: Option<i64>,
    #[serde(default)]
    pub max_ts: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexFragmentMeta {
    pub fragment_id: String,
    pub created_at: String,
    pub files: Vec<String>,
    pub num_buckets: u32,
    pub note: Option<String>,
    #[serde(default)]
    pub key_columns: Vec<String>,
    #[serde(default)]
    pub value_columns: Vec<String>,
    /// Iceberg snapshot this fragment was built from, if any.
    #[serde(default)]
    pub iceberg_snapshot_id: Option<i64>,
    /// Optional size/etag identity for `files` (and extras). Empty on old fragments.
    #[serde(default)]
    pub file_idents: Vec<FileIdent>,
    /// Paths removed from the live set (Iceberg overwrite/expire). Applied in registry order.
    #[serde(default)]
    pub dropped_files: Vec<String>,
    /// Iceberg delete files applied when this fragment was built (`#[serde(default)]` for old manifests).
    #[serde(default)]
    pub iceberg_delete_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactReport {
    pub fragment_id: String,
    pub keys: usize,
    pub entries: usize,
    pub files: usize,
    pub dropped_tombstoned_keys: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyReport {
    pub checked: usize,
    pub stale: Vec<String>,
    pub skipped: usize,
}

fn parse_iceberg_snapshot_note(note: &str) -> Option<i64> {
    note.trim()
        .strip_prefix("iceberg-snapshot:")
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit() && c != '-').next())
        .and_then(|id| id.parse().ok())
}

#[derive(Debug, Default, Clone)]
pub struct RapIndex {
    /// Shared so stress waves can reuse the file dictionary without cloning 300k paths.
    pub files: std::sync::Arc<Vec<PathBuf>>,
    pub entries_by_key: HashMap<String, Vec<RapIndexEntry>>,
    pub fragments: Vec<IndexFragmentMeta>,
    pub root: PathBuf,
}

impl RapIndex {
    pub fn lookup(&self, key: &str) -> &[RapIndexEntry] {
        self.entries_by_key
            .get(key)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn file_path(&self, ordinal: u32) -> Result<&Path> {
        self.files
            .get(ordinal as usize)
            .map(|p| p.as_path())
            .with_context(|| format!("file ordinal {ordinal} out of range"))
    }

    pub fn num_keys(&self) -> usize {
        self.entries_by_key.len()
    }

    pub fn num_entries(&self) -> usize {
        self.entries_by_key.values().map(|v| v.len()).sum()
    }
}

pub fn key_bucket(key: &str, num_buckets: u32) -> u32 {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let n = u64::from_be_bytes(digest[0..8].try_into().unwrap());
    (n % num_buckets as u64) as u32
}

/// Unit separator between compound key parts.
pub const KEY_SEP: char = '\u{1f}';

pub fn encode_key(parts: &[&str]) -> String {
    parts.join(&KEY_SEP.to_string())
}

fn default_key_columns() -> Vec<String> {
    vec!["user_id".to_string()]
}

fn default_value_columns() -> Vec<String> {
    vec![
        "user_id".to_string(),
        "timestamp".to_string(),
        "track_uri".to_string(),
        "duration_ms".to_string(),
        "payload".to_string(),
        "payload_bytes".to_string(),
    ]
}

pub struct IndexBuilder {
    root: PathBuf,
    num_buckets: u32,
    covering: bool,
    /// Capture OffsetIndex page locs into entries (one-page-per-key).
    store_page_locs: bool,
    key_columns: Vec<String>,
    value_columns: Vec<String>,
    /// Iceberg position deletes: normalized data-file path → deleted parquet row ids (`pos`).
    position_deletes: HashMap<String, HashSet<u64>>,
    /// Iceberg equality-delete records (column → encoded value; `None` = null).
    equality_deletes: Vec<HashMap<String, Option<String>>>,
}

impl IndexBuilder {
    pub fn new(root: impl AsRef<Path>, num_buckets: u32) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            num_buckets: num_buckets.max(1),
            covering: false,
            store_page_locs: true,
            key_columns: default_key_columns(),
            value_columns: default_value_columns(),
            position_deletes: HashMap::new(),
            equality_deletes: Vec::new(),
        }
    }

    pub fn with_covering(mut self, covering: bool) -> Self {
        self.covering = covering;
        self
    }

    pub fn with_store_page_locs(mut self, v: bool) -> Self {
        self.store_page_locs = v;
        self
    }

    pub fn with_key_columns(mut self, cols: Vec<String>) -> Self {
        if !cols.is_empty() {
            self.key_columns = cols;
        }
        self
    }

    pub fn with_value_columns(mut self, cols: Vec<String>) -> Self {
        if !cols.is_empty() {
            self.value_columns = cols;
        }
        self
    }

    /// Drop indexed locations whose (data file path, parquet row id) is in a position-delete file.
    pub fn with_position_deletes(mut self, deletes: HashMap<String, HashSet<u64>>) -> Self {
        self.position_deletes = deletes;
        self
    }

    /// Drop rows whose equality-column values match an Iceberg equality-delete record.
    pub fn with_equality_deletes(mut self, deletes: Vec<HashMap<String, Option<String>>>) -> Self {
        self.equality_deletes = deletes;
        self
    }

    pub fn build_fragment(
        &self,
        parquet_files: &[PathBuf],
        fragment_id: &str,
        note: Option<&str>,
    ) -> Result<PathBuf> {
        let _lock = try_lock_index(&self.root)?;
        if self.covering && !parquet_files.is_empty() {
            ensure_listen_covering_schema(&parquet_files[0])?;
        }
        let frag_dir = self.root.join("fragments").join(fragment_id);
        fs::create_dir_all(frag_dir.join("buckets"))?;

        let mut file_dict: Vec<String> = Vec::new();
        let mut buckets: Vec<Vec<RapIndexEntry>> =
            (0..self.num_buckets).map(|_| Vec::new()).collect();

        // Map absolute parquet path → ordinal for merging prepared manifests.
        let mut path_to_ord: HashMap<PathBuf, u32> = HashMap::new();

        for (ordinal, path) in parquet_files.iter().enumerate() {
            let stored = if let Ok(r) = path.strip_prefix(self.root.parent().unwrap_or(&self.root))
            {
                r.to_string_lossy().to_string()
            } else {
                path.to_string_lossy().to_string()
            };
            file_dict.push(stored.clone());
            path_to_ord.insert(path.clone(), ordinal as u32);

            let pos_deletes = position_deletes_for_file(&self.position_deletes, path);
            let key_rows = scan_key_column(
                path,
                &self.key_columns,
                self.covering,
                &pos_deletes,
                &self.equality_deletes,
            )?;
            for (key, rows, covering) in key_rows {
                let page_locs = if self.store_page_locs {
                    capture_page_locs(path, &rows, &self.value_columns).ok()
                } else {
                    None
                };
                let b = key_bucket(&key, self.num_buckets) as usize;
                let vc = covering
                    .as_ref()
                    .map(|c| c.listen_count)
                    .unwrap_or(rows.len() as u64);
                buckets[b].push(RapIndexEntry {
                    key,
                    file: ordinal as u32,
                    value_count: Some(vc),
                    row_numbers: rows,
                    covering,
                    page_locs,
                    frame_locs: None,
                    contiguous: None,
                    prepared_file: None,
                    aligned: None,
                    tombstone: false,
                    file_etag: None,
                    file_size: None,
                    file_mtime_ms: None,
                });
            }
        }

        // Merge prepared manifests (zstd / aligned / interleaved) sitting next
        // to the Parquet files.
        if let Some(parent) = parquet_files.first().and_then(|p| p.parent()) {
            merge_prepared_into_buckets(
                parent,
                &mut buckets,
                &mut file_dict,
                &path_to_ord,
                self.num_buckets,
            )?;
        }

        let file_idents: Vec<FileIdent> = file_dict
            .iter()
            .enumerate()
            .map(|(i, stored)| {
                let open = parquet_files
                    .get(i)
                    .map(|p| p.as_path())
                    .unwrap_or_else(|| Path::new(stored));
                probe_file_ident(stored, open)
            })
            .collect();
        for bucket in &mut buckets {
            for e in bucket {
                if let Some(ident) = file_idents.get(e.file as usize) {
                    apply_file_ident(e, ident);
                }
            }
        }

        for (bi, entries) in buckets.iter().enumerate() {
            let jsonl_path = frag_dir
                .join("buckets")
                .join(format!("bucket_{bi:03}.jsonl"));
            let bin_path = frag_dir.join("buckets").join(format!("bucket_{bi:03}.bin"));
            write_jsonl(&jsonl_path, entries)?;
            write_bincode(&bin_path, entries)?;
        }

        let note_str = note.map(|s| s.to_string());
        let iceberg_snapshot_id = note_str.as_deref().and_then(parse_iceberg_snapshot_note);
        let meta = IndexFragmentMeta {
            fragment_id: fragment_id.to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            files: file_dict,
            num_buckets: self.num_buckets,
            note: note_str,
            key_columns: self.key_columns.clone(),
            value_columns: self.value_columns.clone(),
            iceberg_snapshot_id,
            file_idents,
            dropped_files: Vec::new(),
            iceberg_delete_files: Vec::new(),
        };
        serde_json::to_writer_pretty(File::create(frag_dir.join("manifest.json"))?, &meta)?;

        let mut registry: Vec<String> = if self.root.join("registry.json").exists() {
            read_registry(&self.root)?
        } else {
            Vec::new()
        };
        if !registry.iter().any(|id| id == fragment_id) {
            registry.push(fragment_id.to_string());
        }
        write_registry(&self.root, &registry)?;

        Ok(frag_dir)
    }
}

fn merge_prepared_into_buckets(
    data_dir: &Path,
    buckets: &mut [Vec<RapIndexEntry>],
    file_dict: &mut Vec<String>,
    path_to_ord: &HashMap<PathBuf, u32>,
    num_buckets: u32,
) -> Result<()> {
    for man_path in prepared::find_manifests(data_dir)? {
        let man: PreparedManifest = prepared::load_manifest(&man_path)?;
        let data_abs = data_dir.join(&man.data_file);
        let prepared_rel =
            if let Ok(r) = data_abs.strip_prefix(data_dir.parent().unwrap_or(data_dir)) {
                r.to_string_lossy().to_string()
            } else {
                data_abs.to_string_lossy().to_string()
            };

        // Frames live either inside the Parquet file itself (custom writer) or
        // in a legacy .rapz/.rapi sidecar. Prefer attaching to the parquet ordinal.
        let in_parquet = man.data_file.ends_with(".parquet");
        let companion = if in_parquet {
            data_dir.join(&man.data_file)
        } else {
            let stem = man
                .data_file
                .trim_end_matches(".rapz")
                .trim_end_matches(".rapi");
            data_dir.join(format!("{stem}.parquet"))
        };
        let parquet_ord = path_to_ord.get(&companion).copied();

        // Legacy sidecars still get their own dictionary entry.
        let prep_ord = if in_parquet {
            parquet_ord
        } else {
            let ord = file_dict.len() as u32;
            file_dict.push(prepared_rel.clone());
            Some(ord)
        };

        for k in &man.keys {
            let b = key_bucket(&k.key, num_buckets) as usize;
            let mut updated = false;
            if let Some(ord) = parquet_ord {
                if let Some(e) = buckets[b]
                    .iter_mut()
                    .find(|e| e.key == k.key && e.file == ord)
                {
                    e.frame_locs = if k.frames.is_empty() {
                        None
                    } else {
                        Some(k.frames.clone())
                    };
                    e.contiguous = k.contiguous.clone();
                    // Frames live in the parquet file - no sidecar needed.
                    e.prepared_file = if in_parquet {
                        None
                    } else {
                        Some(prepared_rel.clone())
                    };
                    e.aligned = Some(k.aligned);
                    updated = true;
                }
            }
            if !updated {
                buckets[b].push(RapIndexEntry {
                    key: k.key.clone(),
                    file: prep_ord.unwrap_or(0),
                    row_numbers: k.row_numbers.clone(),
                    value_count: Some(k.value_count),
                    covering: None,
                    page_locs: None,
                    frame_locs: if k.frames.is_empty() {
                        None
                    } else {
                        Some(k.frames.clone())
                    },
                    contiguous: k.contiguous.clone(),
                    prepared_file: if in_parquet {
                        None
                    } else {
                        Some(prepared_rel.clone())
                    },
                    aligned: Some(k.aligned),
                    tombstone: false,
                    file_etag: None,
                    file_size: None,
                    file_mtime_ms: None,
                });
            }
        }
    }
    Ok(())
}

/// Capture OffsetIndex page locations for the rows of one key (all value columns).
fn capture_page_locs(path: &Path, rows: &[u64], value_columns: &[String]) -> Result<Vec<PageLoc>> {
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use parquet::file::metadata::PageIndexPolicy;
    use std::collections::HashSet;

    let uri = path.to_string_lossy();
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let arrow_meta = if S3Client::is_remote_uri(&uri) {
        let (bucket, key) = S3Client::parse_uri(&uri)?;
        let reader = S3ChunkReader::open(S3Client::from_env(), bucket, key)?;
        ArrowReaderMetadata::load(&reader, options)?
    } else {
        let file = File::open(path)?;
        ArrowReaderMetadata::load(&file, options)?
    };
    let pq = arrow_meta.metadata();
    let offset_indexes = pq
        .offset_index()
        .context("no offset index - cannot capture page locs")?;

    // Build row_group starts.
    let mut starts = Vec::new();
    let mut running = 0i64;
    for i in 0..pq.num_row_groups() {
        starts.push(running);
        running += pq.row_group(i).num_rows();
    }

    let mut locs = Vec::new();
    let mut seen = HashSet::new();
    let cols_wanted: HashSet<&str> = value_columns.iter().map(|s| s.as_str()).collect();

    for &grow in rows {
        let r = grow as i64;
        let mut rg = 0usize;
        for (i, &s) in starts.iter().enumerate() {
            let c = pq.row_group(i).num_rows();
            if r >= s && r < s + c {
                rg = i;
                break;
            }
        }
        let local = r - starts[rg];
        let rg_meta = pq.row_group(rg);
        for col in 0..rg_meta.num_columns() {
            let col_meta = rg_meta.column(col);
            let name = col_meta.column_path().string();
            let leaf = name.rsplit('.').next().unwrap_or(&name).to_string();
            if !cols_wanted.contains(leaf.as_str()) && !cols_wanted.contains(name.as_str()) {
                continue;
            }
            let oi = offset_indexes
                .get(rg)
                .and_then(|c| c.get(col))
                .context("missing offset index col")?;
            let pages = oi.page_locations();
            // binary search page
            let mut lo = 0usize;
            let mut hi = pages.len();
            while lo + 1 < hi {
                let mid = (lo + hi) / 2;
                if pages[mid].first_row_index <= local {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            let page = &pages[lo];
            let key = (leaf.clone(), page.offset, page.compressed_page_size);
            if seen.insert(key.clone()) {
                locs.push(PageLoc {
                    column: leaf,
                    offset: page.offset as u64,
                    size: page.compressed_page_size as u32,
                });
            }
        }
    }
    Ok(locs)
}

/// Covering aggregates are listen-shaped (`duration_ms` / `timestamp` / nested `listens`).
/// Refuse `--covering` on generic schemas rather than emitting a fake JSON alias.
fn ensure_listen_covering_schema(path: &Path) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let uri = path.to_string_lossy();
    let schema = if S3Client::is_remote_uri(&uri) {
        let (bucket, key) = S3Client::parse_uri(&uri)?;
        let reader = S3ChunkReader::open(S3Client::from_env(), bucket, key)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
        builder.schema().clone()
    } else {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        builder.schema().clone()
    };
    let ok = schema.index_of("duration_ms").is_ok()
        || schema.index_of("timestamp").is_ok()
        || schema.index_of("timestamp_ms").is_ok()
        || schema.index_of("listens").is_ok()
        || schema.index_of("payload").is_ok();
    if !ok {
        bail!(
            "covering is listen-shaped (needs duration_ms, timestamp, or listens); \
             omit --covering for generic Parquet schemas"
        );
    }
    Ok(())
}

fn write_jsonl(path: &Path, entries: &[RapIndexEntry]) -> Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    for e in entries {
        serde_json::to_writer(&mut w, e)?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

fn write_bincode(path: &Path, entries: &[RapIndexEntry]) -> Result<()> {
    let bytes = bincode::serialize(entries).context("bincode serialize")?;
    fs::write(path, bytes)?;
    Ok(())
}

/// Writer stored a contiguous run as `[first_row]` + `value_count`. Expand so
/// decode / RowSelection still sees every row.
pub fn expand_compact_rows(e: &mut RapIndexEntry) {
    let Some(vc) = e.value_count else {
        return;
    };
    if vc <= 1 || e.row_numbers.len() != 1 {
        return;
    }
    let start = e.row_numbers[0];
    e.row_numbers = (start..start.saturating_add(vc)).collect();
}

fn read_bucket_entries(frag_dir: &Path, bi: u32) -> Result<Vec<RapIndexEntry>> {
    read_bucket_entries_filtered(frag_dir, bi, None)
}

fn read_bucket_entries_filtered(
    frag_dir: &Path,
    bi: u32,
    keep: Option<&HashSet<&str>>,
) -> Result<Vec<RapIndexEntry>> {
    let bin_path = frag_dir.join("buckets").join(format!("bucket_{bi:03}.bin"));
    let jsonl_path = frag_dir
        .join("buckets")
        .join(format!("bucket_{bi:03}.jsonl"));
    if let Some(keep) = keep {
        if jsonl_path.exists() {
            account_bucket_load()?;
            return read_jsonl_matching(&jsonl_path, keep);
        }
    }
    if bin_path.exists() {
        account_bucket_load()?;
        match mmap_deserialize_bin(&bin_path) {
            Ok(v) => {
                return Ok(filter_kept(v, keep));
            }
            Err(_) if jsonl_path.exists() => {}
            Err(e) => return Err(e).context("bincode deserialize"),
        }
    }
    if jsonl_path.exists() {
        account_bucket_load()?;
        let v = read_jsonl(&jsonl_path)?;
        Ok(filter_kept(v, keep))
    } else {
        Ok(Vec::new())
    }
}

fn filter_kept(mut v: Vec<RapIndexEntry>, keep: Option<&HashSet<&str>>) -> Vec<RapIndexEntry> {
    if let Some(keep) = keep {
        v.retain(|e| keep.contains(e.key.as_str()));
    }
    v
}

fn read_jsonl_matching(path: &Path, keep: &HashSet<&str>) -> Result<Vec<RapIndexEntry>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let e: RapIndexEntry = serde_json::from_str(&line)?;
        if keep.contains(e.key.as_str()) {
            out.push(e);
        }
    }
    Ok(out)
}

fn mmap_deserialize_bin(path: &Path) -> Result<Vec<RapIndexEntry>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    // SAFETY: bucket files are append-only and never rewritten in place.
    let mmap =
        unsafe { memmap2::Mmap::map(&file) }.with_context(|| format!("mmap {}", path.display()))?;
    Ok(bincode::deserialize(mmap.as_ref())?)
}

pub fn load_index(root: impl AsRef<Path>) -> Result<RapIndex> {
    load_index_inner(root.as_ref(), None)
}

/// Load file dictionary + only the hash buckets that contain `keys`.
/// Needed for lakes with millions of index entries (full HashMap would OOM).
pub fn load_index_for_keys(root: impl AsRef<Path>, keys: &[String]) -> Result<RapIndex> {
    load_index_inner(root.as_ref(), Some(keys))
}

fn load_index_inner(root: &Path, only_keys: Option<&[String]>) -> Result<RapIndex> {
    let root = root.to_path_buf();
    let registry = read_registry(&root)?;

    let mut files: Vec<PathBuf> = Vec::new();
    let mut entries_by_key: HashMap<String, Vec<RapIndexEntry>> = HashMap::new();
    let mut fragments: Vec<IndexFragmentMeta> = Vec::new();
    let mut dropped: HashSet<String> = HashSet::new();
    let forgotten = load_forgotten(&root);

    for frag_id in registry {
        let frag_dir = root.join("fragments").join(&frag_id);
        let meta: IndexFragmentMeta =
            serde_json::from_reader(File::open(frag_dir.join("manifest.json"))?)?;

        let mut local_to_global: Vec<u32> = Vec::with_capacity(meta.files.len());
        for rel in &meta.files {
            let abs = resolve_data_path(&root, rel);
            let global = files.len() as u32;
            files.push(abs);
            local_to_global.push(global);
        }
        apply_fragment_file_liveness(&mut dropped, &mut entries_by_key, &root, &meta, |ord| {
            files
                .get(ord as usize)
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default()
        });

        let wanted: Option<HashSet<u32>> =
            only_keys.map(|ks| ks.iter().map(|k| key_bucket(k, meta.num_buckets)).collect());
        let keep_keys: Option<HashSet<&str>> =
            only_keys.map(|ks| ks.iter().map(|s| s.as_str()).collect());

        for bi in 0..meta.num_buckets {
            if let Some(ref w) = wanted {
                if !w.contains(&bi) {
                    continue;
                }
            }
            let mut entries = read_bucket_entries_filtered(&frag_dir, bi, keep_keys.as_ref())?;
            if entries.is_empty() {
                continue;
            }

            for e in &mut entries {
                if let Some(ref keep) = keep_keys {
                    if !keep.contains(e.key.as_str()) {
                        continue;
                    }
                }
                let local = e.file as usize;
                if local < local_to_global.len() {
                    e.file = local_to_global[local];
                }
                // IndexBuilder stores full row lists and records key_columns.
                // Compact `[first]+value_count` encoding is lake-only (empty key_columns).
                if meta.key_columns.is_empty() {
                    expand_compact_rows(e);
                }
                if e.tombstone {
                    absorb_entry(&mut entries_by_key, e.clone());
                    continue;
                }
                let stored = files
                    .get(e.file as usize)
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let rel = meta.files.get(local).map(|s| s.as_str()).unwrap_or("");
                if file_is_dropped(&dropped, &root, rel)
                    || file_is_dropped(&dropped, &root, &stored)
                {
                    continue;
                }
                absorb_entry(&mut entries_by_key, e.clone());
            }
        }
        if is_iceberg_fragment(&meta) {
            supersede_entries_for_files(&mut entries_by_key, &local_to_global, |ord| {
                files
                    .get(ord as usize)
                    .map(|p| file_compare_id(&root, &p.to_string_lossy()))
                    .unwrap_or_default()
            });
        }
        fragments.push(meta);
    }
    retain_undropped_entries(&mut entries_by_key, &dropped, &root, |ord| {
        files
            .get(ord as usize)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    for k in &forgotten {
        entries_by_key.remove(k);
    }

    Ok(RapIndex {
        root,
        files: std::sync::Arc::new(files),
        entries_by_key,
        fragments,
    })
}

/// Load file dictionary once (heavy for 300k lakes). Reuse across stress waves.
pub fn load_index_file_dictionary(
    root: impl AsRef<Path>,
) -> Result<(
    std::sync::Arc<Vec<PathBuf>>,
    Vec<IndexFragmentMeta>,
    PathBuf,
)> {
    let root = root.as_ref().to_path_buf();
    let registry = read_registry(&root)?;
    let mut files: Vec<PathBuf> = Vec::new();
    let mut fragments = Vec::new();
    for frag_id in registry {
        let frag_dir = root.join("fragments").join(&frag_id);
        let meta: IndexFragmentMeta =
            serde_json::from_reader(File::open(frag_dir.join("manifest.json"))?)?;
        for rel in &meta.files {
            files.push(resolve_data_path(&root, rel));
        }
        fragments.push(meta);
    }
    Ok((std::sync::Arc::new(files), fragments, root))
}

/// Build a RapIndex for `keys` using a preloaded file dictionary (no manifest re-read).
pub fn load_index_entries_for_keys(
    root: &Path,
    files: std::sync::Arc<Vec<PathBuf>>,
    fragments: &[IndexFragmentMeta],
    keys: &[String],
) -> Result<RapIndex> {
    let mut entries_by_key: HashMap<String, Vec<RapIndexEntry>> = HashMap::new();
    let keep_keys: HashSet<&str> = keys.iter().map(|s| s.as_str()).collect();
    let forgotten = load_forgotten(root);
    let mut dropped: HashSet<String> = HashSet::new();
    let mut file_base = 0usize;
    for meta in fragments {
        let n_files = meta.files.len();
        apply_fragment_file_liveness(&mut dropped, &mut entries_by_key, root, meta, |ord| {
            files
                .get(ord as usize)
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default()
        });
        let frag_dir = root.join("fragments").join(&meta.fragment_id);
        let wanted: HashSet<u32> = keys
            .iter()
            .map(|k| key_bucket(k, meta.num_buckets))
            .collect();
        for bi in wanted {
            let mut entries = read_bucket_entries_filtered(&frag_dir, bi, Some(&keep_keys))?;
            for e in &mut entries {
                if !keep_keys.contains(e.key.as_str()) {
                    continue;
                }
                let local = e.file as usize;
                if local < n_files {
                    e.file = (file_base + local) as u32;
                }
                if meta.key_columns.is_empty() {
                    expand_compact_rows(e);
                }
                if e.tombstone {
                    absorb_entry(&mut entries_by_key, e.clone());
                    continue;
                }
                let stored = files
                    .get(e.file as usize)
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let rel = meta.files.get(local).map(|s| s.as_str()).unwrap_or("");
                if file_is_dropped(&dropped, root, rel) || file_is_dropped(&dropped, root, &stored)
                {
                    continue;
                }
                absorb_entry(&mut entries_by_key, e.clone());
            }
        }
        if is_iceberg_fragment(meta) {
            let new_ords: Vec<u32> = (file_base..file_base + n_files).map(|i| i as u32).collect();
            supersede_entries_for_files(&mut entries_by_key, &new_ords, |ord| {
                files
                    .get(ord as usize)
                    .map(|p| file_compare_id(root, &p.to_string_lossy()))
                    .unwrap_or_default()
            });
        }
        file_base += n_files;
    }
    retain_undropped_entries(&mut entries_by_key, &dropped, root, |ord| {
        files
            .get(ord as usize)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    for k in &forgotten {
        entries_by_key.remove(k);
    }
    Ok(RapIndex {
        root: root.to_path_buf(),
        files,
        entries_by_key,
        fragments: fragments.to_vec(),
    })
}

fn absorb_entry(entries_by_key: &mut HashMap<String, Vec<RapIndexEntry>>, e: RapIndexEntry) {
    if e.tombstone {
        entries_by_key.remove(&e.key);
    } else {
        entries_by_key.entry(e.key.clone()).or_default().push(e);
    }
}

fn is_remote_uri(s: &str) -> bool {
    S3Client::is_remote_uri(s)
}

fn probe_remote_ident_s3(uri: &str, s3: Option<&S3Client>) -> (Option<String>, Option<u64>) {
    let Ok((bucket, key)) = S3Client::parse_uri(uri) else {
        return (None, None);
    };
    let owned;
    let client = match s3 {
        Some(c) => c,
        None => {
            owned = S3Client::from_env();
            &owned
        }
    };
    match client.head_object_meta(&bucket, &key) {
        Ok(meta) => (meta.etag, Some(meta.size)),
        Err(_) => (None, None),
    }
}

fn metadata_mtime_ms(meta: &fs::Metadata) -> Option<i64> {
    let modified = meta.modified().ok()?;
    match modified.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_millis()).ok(),
        Err(e) => i64::try_from(e.duration().as_millis()).ok().map(|n| -n),
    }
}

fn apply_file_ident(e: &mut RapIndexEntry, ident: &FileIdent) {
    e.file_etag = ident.etag.clone();
    e.file_size = ident.size;
    e.file_mtime_ms = ident.mtime_ms;
}

/// Probe live size / ETag / mtime. Remote uses `S3Client::head_object_meta`.
pub fn probe_file_ident(stored: &str, open_path: &Path) -> FileIdent {
    probe_file_ident_s3(stored, open_path, None)
}

pub fn probe_file_ident_s3(stored: &str, open_path: &Path, s3: Option<&S3Client>) -> FileIdent {
    if is_remote_uri(stored) {
        let (etag, size) = probe_remote_ident_s3(stored, s3);
        return FileIdent {
            path: stored.to_string(),
            etag,
            size,
            mtime_ms: None,
        };
    }
    let meta = fs::metadata(open_path)
        .or_else(|_| fs::metadata(stored))
        .ok();
    FileIdent {
        path: stored.to_string(),
        etag: None,
        size: meta.as_ref().map(|m| m.len()),
        mtime_ms: meta.as_ref().and_then(metadata_mtime_ms),
    }
}

/// Stored identity for an index entry (entry fields, else fragment `file_idents`).
pub fn stored_ident_for_entry(index: &RapIndex, entry: &RapIndexEntry) -> FileIdent {
    let path = index
        .file_path(entry.file)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    let ident = FileIdent {
        path: path.clone(),
        etag: entry.file_etag.clone(),
        size: entry.file_size,
        mtime_ms: entry.file_mtime_ms,
    };
    if ident.has_identity() {
        return ident;
    }
    for frag in &index.fragments {
        for fi in &frag.file_idents {
            if ident_path_eq(index, &fi.path, &path) && fi.has_identity() {
                let mut found = fi.clone();
                found.path = path;
                return found;
            }
        }
    }
    ident
}

fn ident_path_eq(index: &RapIndex, a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    resolve_data_path(&index.root, a) == resolve_data_path(&index.root, b)
}

/// HEAD / `fs::metadata` every unique file referenced by `entries`. Fail the whole
/// key if any stored size/ETag/mtime no longer matches. Skip files with no stored identity.
pub fn ensure_entries_fresh(
    index: &RapIndex,
    entries: &[RapIndexEntry],
    s3: Option<&S3Client>,
) -> Result<()> {
    let mut seen = HashSet::new();
    for e in entries {
        let open = match index.file_path(e.file) {
            Ok(p) => p.to_path_buf(),
            Err(_) => continue,
        };
        let path_key = open.to_string_lossy().into_owned();
        if !seen.insert(path_key.clone()) {
            continue;
        }
        let stored = stored_ident_for_entry(index, e);
        if !stored.has_identity() {
            continue;
        }
        let live = probe_file_ident_s3(&path_key, &open, s3);
        if !live.has_identity() {
            return Err(stale_file_identity_error(&stored, None));
        }
        if file_ident_mismatch(&stored, &live) {
            return Err(stale_file_identity_error(&stored, Some(&live)));
        }
    }
    Ok(())
}

fn lock_path(root: &Path) -> PathBuf {
    root.join(".needle.lock")
}

/// Non-blocking exclusive `flock` on `<index>/.needle.lock`.
/// A second overlapping writer fails with an error containing `index lock`.
pub fn try_lock_index(root: &Path) -> Result<IndexWriteLock> {
    fs::create_dir_all(root)?;
    let path = lock_path(root);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("open index lock {}", path.display()))?;
    // SAFETY: `file` is a live fd we own; flock is released when it is dropped.
    let mut rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    // Brief retry: sequential writers in one process can see EAGAIN until the
    // previous fd is fully closed. Overlapping holders still fail.
    let mut spins = 0;
    while rc != 0 && spins < 40 {
        std::thread::sleep(std::time::Duration::from_millis(5));
        rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        spins += 1;
    }
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        bail!(
            "index lock: exclusive lock on {} is held by another writer ({err})",
            path.display()
        );
    }
    Ok(IndexWriteLock { _file: file })
}

fn fragment_ids_from_array(arr: &[serde_json::Value]) -> Result<Vec<String>> {
    arr.iter()
        .map(|v| {
            v.as_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow!("registry fragment id must be a string"))
        })
        .collect()
}

fn parse_registry_json(value: &serde_json::Value) -> Result<Vec<String>> {
    match value {
        serde_json::Value::Array(arr) => fragment_ids_from_array(arr),
        serde_json::Value::Object(map) => {
            let version = map
                .get("format_version")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow!("registry.json missing numeric format_version"))?;
            if version != u64::from(INDEX_FORMAT_VERSION) {
                bail!(
                    "unsupported index format_version {version} (supported: {INDEX_FORMAT_VERSION})"
                );
            }
            let Some(arr) = map.get("fragments").and_then(|v| v.as_array()) else {
                bail!("registry.json missing fragments array");
            };
            fragment_ids_from_array(arr)
        }
        _ => bail!("registry.json must be an object or a legacy JSON array of fragment ids"),
    }
}

/// Read fragment ids from `registry.json` (v1 object or legacy array).
pub fn read_registry(root: &Path) -> Result<Vec<String>> {
    let registry_path = root.join("registry.json");
    if !registry_path.exists() {
        bail!("no RAP index at {} (missing registry.json)", root.display());
    }
    let value: serde_json::Value = serde_json::from_reader(File::open(&registry_path)?)
        .with_context(|| format!("read {}", registry_path.display()))?;
    parse_registry_json(&value).with_context(|| format!("parse {}", registry_path.display()))
}

/// Publish `registry.json` as a v1 object via tmp+rename. Caller must hold the index lock.
///
/// When `root` is an `s3://` URI, uses a conditional PUT (`If-None-Match: *` on create,
/// `If-Match` on update) so two writers cannot clobber a valid v1 object.
pub fn write_registry(root: &Path, ids: &[String]) -> Result<()> {
    let uri = root.to_string_lossy();
    if S3Client::is_remote_uri(uri.trim()) {
        let (bucket, prefix) = S3Client::parse_uri(uri.trim())?;
        let key = registry_s3_key(&prefix);
        return publish_registry_s3(&S3Client::from_env(), &bucket, &key, ids);
    }
    fs::create_dir_all(root)?;
    let path = root.join("registry.json");
    let tmp = root.join(".registry.json.tmp");
    let doc = IndexRegistry {
        format_version: INDEX_FORMAT_VERSION,
        fragments: ids.to_vec(),
    };
    {
        let mut f = File::create(&tmp).context("create registry tmp")?;
        serde_json::to_writer_pretty(&mut f, &doc).context("write registry tmp")?;
        f.sync_all().ok();
    }
    fs::rename(&tmp, &path).context("publish registry.json")
}

fn registry_s3_key(prefix: &str) -> String {
    let p = prefix.trim().trim_end_matches('/');
    if p.is_empty() || p.ends_with("registry.json") {
        if p.is_empty() {
            "registry.json".into()
        } else {
            p.to_string()
        }
    } else {
        format!("{p}/registry.json")
    }
}

fn registry_v1_bytes(ids: &[String]) -> Result<Vec<u8>> {
    let doc = IndexRegistry {
        format_version: INDEX_FORMAT_VERSION,
        fragments: ids.to_vec(),
    };
    serde_json::to_vec_pretty(&doc).context("serialize registry")
}

/// Conditional S3 publish of a v1 `registry.json`. Create uses `If-None-Match: *`;
/// update uses `If-Match` on the live ETag. A lost race is `s3_precondition_failed`.
pub fn publish_registry_s3(
    client: &S3Client,
    bucket: &str,
    key: &str,
    ids: &[String],
) -> Result<()> {
    let body = registry_v1_bytes(ids)?;
    match client.head_object_meta(bucket, key) {
        Ok(meta) => {
            let etag = meta
                .etag
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    anyhow!("s3_precondition_failed: HEAD {bucket}/{key} missing ETag")
                })?;
            client
                .put_object_if_match(bucket, key, &body, etag)
                .with_context(|| format!("CAS PUT s3://{bucket}/{key}"))
        }
        Err(e) => {
            let msg = format!("{e:#}");
            if msg.contains("404") {
                client
                    .put_object_if_none_match(bucket, key, &body)
                    .with_context(|| format!("exclusive PUT s3://{bucket}/{key}"))
            } else {
                Err(e).context(format!("HEAD s3://{bucket}/{key}"))
            }
        }
    }
}

fn gc_unreferenced_fragments(root: &Path, live: &[String]) -> Result<()> {
    let frag_root = root.join("fragments");
    if !frag_root.is_dir() {
        return Ok(());
    }
    let live: HashSet<&str> = live.iter().map(String::as_str).collect();
    for ent in fs::read_dir(&frag_root).with_context(|| format!("read {}", frag_root.display()))? {
        let ent = ent?;
        if !ent.file_type()?.is_dir() {
            continue;
        }
        let name = ent.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if live.contains(name) {
            continue;
        }
        fs::remove_dir_all(ent.path())
            .with_context(|| format!("remove unreferenced fragment {}", ent.path().display()))?;
    }
    Ok(())
}

fn forgotten_path(root: &Path) -> PathBuf {
    root.join("forgotten.jsonl")
}

fn normalize_stored_key(s: &str) -> String {
    let s = s.trim();
    let s = s.strip_prefix("file://").unwrap_or(s);
    s.replace('\\', "/")
}

/// Stored path plus the resolved open path, so Iceberg `dropped_files` matches
/// both relative dictionary entries and absolute `files` vec paths.
fn path_identity_keys(root: &Path, stored: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut push = |s: &str| {
        let n = normalize_stored_key(s);
        if !n.is_empty() && !out.contains(&n) {
            out.push(n);
        }
    };
    push(stored);
    let resolved = resolve_data_path(root, stored);
    push(&resolved.to_string_lossy());
    out
}

fn mark_file_present(dropped: &mut HashSet<String>, root: &Path, stored: &str) {
    for k in path_identity_keys(root, stored) {
        dropped.remove(&k);
    }
}

fn mark_file_dropped(dropped: &mut HashSet<String>, root: &Path, stored: &str) {
    for k in path_identity_keys(root, stored) {
        dropped.insert(k);
    }
}

fn file_is_dropped(dropped: &HashSet<String>, root: &Path, stored: &str) -> bool {
    path_identity_keys(root, stored)
        .iter()
        .any(|k| dropped.contains(k))
}

fn is_iceberg_fragment(meta: &IndexFragmentMeta) -> bool {
    meta.iceberg_snapshot_id.is_some()
        || meta
            .note
            .as_deref()
            .is_some_and(|n| n.starts_with("iceberg-snapshot:"))
}

fn file_compare_id(root: &Path, stored: &str) -> String {
    if is_remote_uri(stored) {
        return normalize_stored_key(stored);
    }
    let p = resolve_data_path(root, stored);
    p.canonicalize()
        .unwrap_or(p)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Iceberg fragments: `dropped_files` first (evict prior entries), then `files` re-add.
/// Other fragments keep dropped-wins-over-files so existing drop tests stay valid.
fn apply_fragment_file_liveness(
    dropped: &mut HashSet<String>,
    entries_by_key: &mut HashMap<String, Vec<RapIndexEntry>>,
    root: &Path,
    meta: &IndexFragmentMeta,
    file_of: impl Fn(u32) -> String,
) {
    if is_iceberg_fragment(meta) {
        for d in &meta.dropped_files {
            mark_file_dropped(dropped, root, d);
        }
        if !meta.dropped_files.is_empty() {
            retain_undropped_entries(entries_by_key, dropped, root, &file_of);
        }
        for rel in &meta.files {
            mark_file_present(dropped, root, rel);
        }
    } else {
        for rel in &meta.files {
            mark_file_present(dropped, root, rel);
        }
        for d in &meta.dropped_files {
            mark_file_dropped(dropped, root, d);
        }
    }
}

/// Later Iceberg fragment entries for a file replace earlier ones (delete re-scan).
fn supersede_entries_for_files(
    entries_by_key: &mut HashMap<String, Vec<RapIndexEntry>>,
    new_ordinals: &[u32],
    file_id_of: impl Fn(u32) -> String,
) {
    if new_ordinals.is_empty() {
        return;
    }
    let new_ord: HashSet<u32> = new_ordinals.iter().copied().collect();
    let new_ids: HashSet<String> = new_ordinals
        .iter()
        .map(|&o| file_id_of(o))
        .filter(|s| !s.is_empty())
        .collect();
    if new_ids.is_empty() {
        return;
    }
    for ents in entries_by_key.values_mut() {
        ents.retain(|e| new_ord.contains(&e.file) || !new_ids.contains(&file_id_of(e.file)));
    }
    entries_by_key.retain(|_, ents| !ents.is_empty());
}

/// Path aliases used to match Iceberg position-delete `file_path` values to data files.
pub(crate) fn delete_path_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut push = |x: String| {
        if !x.is_empty() && !keys.contains(&x) {
            keys.push(x);
        }
    };
    let raw = s.trim();
    push(raw.to_string());
    let stripped = if let Some(rest) = raw.strip_prefix("file://") {
        rest.strip_prefix("localhost").unwrap_or(rest)
    } else if let Some(rest) = raw.strip_prefix("file:") {
        rest
    } else {
        raw
    };
    push(stripped.to_string());
    push(normalize_stored_key(raw));
    push(normalize_stored_key(stripped));
    let p = Path::new(stripped);
    if let Ok(c) = p.canonicalize() {
        push(c.to_string_lossy().to_string());
        push(normalize_stored_key(&c.to_string_lossy()));
    }
    keys
}

fn position_deletes_for_file(map: &HashMap<String, HashSet<u64>>, path: &Path) -> HashSet<u64> {
    let mut out = HashSet::new();
    if map.is_empty() {
        return out;
    }
    for k in delete_path_keys(&path.to_string_lossy()) {
        if let Some(s) = map.get(&k) {
            out.extend(s.iter().copied());
        }
    }
    out
}

/// Dropped files on a later fragment must evict entries already absorbed.
fn retain_undropped_entries(
    entries_by_key: &mut HashMap<String, Vec<RapIndexEntry>>,
    dropped: &HashSet<String>,
    root: &Path,
    file_of: impl Fn(u32) -> String,
) {
    if dropped.is_empty() {
        return;
    }
    for ents in entries_by_key.values_mut() {
        ents.retain(|e| {
            if e.tombstone {
                return true;
            }
            !file_is_dropped(dropped, root, &file_of(e.file))
        });
    }
    entries_by_key.retain(|_, ents| !ents.is_empty());
}

fn load_forgotten(root: &Path) -> HashSet<String> {
    let path = forgotten_path(root);
    let Ok(f) = File::open(&path) else {
        return HashSet::new();
    };
    let mut out = HashSet::new();
    for line in BufReader::new(f).lines().map_while(Result::ok) {
        let k = line.trim();
        if !k.is_empty() {
            out.insert(k.to_string());
        }
    }
    out
}

fn append_forgotten(root: &Path, keys: &[String]) -> Result<()> {
    fs::create_dir_all(root)?;
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(forgotten_path(root))
        .context("open forgotten.jsonl")?;
    for k in keys {
        if k.is_empty() {
            continue;
        }
        writeln!(f, "{k}")?;
    }
    Ok(())
}

fn last_fragment_meta(root: &Path) -> Result<Option<IndexFragmentMeta>> {
    if !root.join("registry.json").exists() {
        return Ok(None);
    }
    let registry = read_registry(root)?;
    let Some(id) = registry.last() else {
        return Ok(None);
    };
    let man = root.join("fragments").join(id).join("manifest.json");
    if !man.exists() {
        return Ok(None);
    }
    let meta = serde_json::from_reader(File::open(&man)?)?;
    Ok(Some(meta))
}

fn write_fragment_dir(
    root: &Path,
    meta: &IndexFragmentMeta,
    buckets: &[Vec<RapIndexEntry>],
) -> Result<PathBuf> {
    let frag_dir = root.join("fragments").join(&meta.fragment_id);
    fs::create_dir_all(frag_dir.join("buckets"))?;
    for (bi, entries) in buckets.iter().enumerate() {
        let jsonl_path = frag_dir
            .join("buckets")
            .join(format!("bucket_{bi:03}.jsonl"));
        let bin_path = frag_dir.join("buckets").join(format!("bucket_{bi:03}.bin"));
        write_jsonl(&jsonl_path, entries)?;
        write_bincode(&bin_path, entries)?;
    }
    serde_json::to_writer_pretty(File::create(frag_dir.join("manifest.json"))?, meta)?;
    Ok(frag_dir)
}

fn bucketize_entries(
    entries: impl IntoIterator<Item = RapIndexEntry>,
    num_buckets: u32,
) -> Vec<Vec<RapIndexEntry>> {
    let n = num_buckets.max(1);
    let mut buckets: Vec<Vec<RapIndexEntry>> = (0..n).map(|_| Vec::new()).collect();
    for e in entries {
        let b = key_bucket(&e.key, n) as usize;
        buckets[b].push(e);
    }
    buckets
}

/// Load all fragments, apply tombstones, last `(key, file)` wins, write one fragment,
/// rewrite `registry.json` to only that id, and delete unreferenced fragment directories.
pub fn compact_index(root: impl AsRef<Path>, fragment_id: Option<&str>) -> Result<CompactReport> {
    let root = root.as_ref();
    let _lock = try_lock_index(root)?;
    let registry = read_registry(root)?;
    let fragment_id = fragment_id
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("compact-{}", chrono::Utc::now().timestamp_millis()));

    let mut files: Vec<String> = Vec::new();
    let mut resolved: Vec<PathBuf> = Vec::new();
    let mut entries_by_key: HashMap<String, Vec<RapIndexEntry>> = HashMap::new();
    let mut ever_tombstoned: HashSet<String> = HashSet::new();
    let mut last_meta: Option<IndexFragmentMeta> = None;
    let mut dropped: HashSet<String> = HashSet::new();
    let mut iceberg_snapshot_id: Option<i64> = None;

    for frag_id in &registry {
        let frag_dir = root.join("fragments").join(frag_id);
        let meta: IndexFragmentMeta =
            serde_json::from_reader(File::open(frag_dir.join("manifest.json"))?)?;

        let mut local_to_global: Vec<u32> = Vec::with_capacity(meta.files.len());
        for rel in &meta.files {
            local_to_global.push(files.len() as u32);
            files.push(rel.clone());
            resolved.push(resolve_data_path(root, rel));
        }
        apply_fragment_file_liveness(&mut dropped, &mut entries_by_key, root, &meta, |ord| {
            files.get(ord as usize).cloned().unwrap_or_default()
        });
        if let Some(id) = meta
            .iceberg_snapshot_id
            .or_else(|| meta.note.as_deref().and_then(parse_iceberg_snapshot_note))
        {
            iceberg_snapshot_id = Some(id);
        }

        for bi in 0..meta.num_buckets {
            let mut entries = read_bucket_entries(&frag_dir, bi)?;
            for e in &mut entries {
                let local = e.file as usize;
                if local < local_to_global.len() {
                    e.file = local_to_global[local];
                }
                if meta.key_columns.is_empty() {
                    expand_compact_rows(e);
                }
                if e.tombstone {
                    ever_tombstoned.insert(e.key.clone());
                } else {
                    let rel = meta.files.get(local).map(|s| s.as_str()).unwrap_or("");
                    let stored = files.get(e.file as usize).cloned().unwrap_or_default();
                    if file_is_dropped(&dropped, root, rel)
                        || file_is_dropped(&dropped, root, &stored)
                    {
                        continue;
                    }
                }
                absorb_entry(&mut entries_by_key, e.clone());
            }
        }
        if is_iceberg_fragment(&meta) {
            supersede_entries_for_files(&mut entries_by_key, &local_to_global, |ord| {
                files
                    .get(ord as usize)
                    .map(|s| file_compare_id(root, s))
                    .unwrap_or_default()
            });
        }
        last_meta = Some(meta);
    }
    retain_undropped_entries(&mut entries_by_key, &dropped, root, |ord| {
        files.get(ord as usize).cloned().unwrap_or_default()
    });

    let dropped_tombstoned_keys = ever_tombstoned
        .iter()
        .filter(|k| !entries_by_key.contains_key(*k))
        .count();

    // Last (key, stored-path) wins; rebuild a unique file dictionary.
    let mut path_to_ord: HashMap<String, u32> = HashMap::new();
    let mut new_files: Vec<String> = Vec::new();
    let mut live: Vec<RapIndexEntry> = Vec::new();
    let mut keys_sorted: Vec<String> = entries_by_key.keys().cloned().collect();
    keys_sorted.sort();
    for key in keys_sorted {
        let ents = entries_by_key.remove(&key).unwrap_or_default();
        let mut by_path: HashMap<String, RapIndexEntry> = HashMap::new();
        let mut path_order: Vec<String> = Vec::new();
        for e in ents {
            let path = files.get(e.file as usize).cloned().unwrap_or_else(|| {
                resolved
                    .get(e.file as usize)
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default()
            });
            if !by_path.contains_key(&path) {
                path_order.push(path.clone());
            }
            by_path.insert(path, e);
        }
        for path in path_order {
            let mut e = by_path.remove(&path).unwrap();
            let ord = if let Some(&o) = path_to_ord.get(&path) {
                o
            } else {
                let o = new_files.len() as u32;
                path_to_ord.insert(path.clone(), o);
                new_files.push(path);
                o
            };
            e.file = ord;
            live.push(e);
        }
    }
    let forgotten = load_forgotten(root);
    live.retain(|e| !forgotten.contains(&e.key));

    let file_idents: Vec<FileIdent> = new_files
        .iter()
        .map(|stored| probe_file_ident(stored, &resolve_data_path(root, stored)))
        .collect();
    for e in &mut live {
        if let Some(ident) = file_idents.get(e.file as usize) {
            apply_file_ident(e, ident);
        }
    }

    let num_buckets = last_meta
        .as_ref()
        .map(|m| m.num_buckets.max(1))
        .unwrap_or(16);
    let key_columns = last_meta
        .as_ref()
        .map(|m| m.key_columns.clone())
        .unwrap_or_default();
    let value_columns = last_meta
        .as_ref()
        .map(|m| m.value_columns.clone())
        .unwrap_or_default();

    let keys = {
        let set: HashSet<&str> = live.iter().map(|e| e.key.as_str()).collect();
        set.len()
    };
    let entries = live.len();
    let n_files = new_files.len();
    let buckets = bucketize_entries(live, num_buckets);
    let meta = IndexFragmentMeta {
        fragment_id: fragment_id.clone(),
        created_at: chrono::Utc::now().to_rfc3339(),
        files: new_files,
        num_buckets,
        note: Some("compacted".to_string()),
        key_columns,
        value_columns,
        // Newest Iceberg snapshot across fragments (forget fragments have none).
        iceberg_snapshot_id,
        file_idents,
        dropped_files: Vec::new(),
        iceberg_delete_files: Vec::new(),
    };
    write_fragment_dir(root, &meta, &buckets)?;
    write_registry(root, std::slice::from_ref(&fragment_id))?;
    gc_unreferenced_fragments(root, std::slice::from_ref(&fragment_id))?;

    Ok(CompactReport {
        fragment_id,
        keys,
        entries,
        files: n_files,
        dropped_tombstoned_keys,
    })
}

/// Append a fragment of tombstone entries that forget `keys` on subsequent loads.
pub fn forget_keys(
    root: impl AsRef<Path>,
    keys: &[String],
    fragment_id: Option<&str>,
) -> Result<PathBuf> {
    let root = root.as_ref();
    let _lock = try_lock_index(root)?;
    fs::create_dir_all(root)?;
    let prev = last_fragment_meta(root)?;
    let num_buckets = prev.as_ref().map(|m| m.num_buckets.max(1)).unwrap_or(16);
    let key_columns = prev
        .as_ref()
        .map(|m| m.key_columns.clone())
        .unwrap_or_else(default_key_columns);
    let value_columns = prev
        .as_ref()
        .map(|m| m.value_columns.clone())
        .unwrap_or_else(default_value_columns);
    let fragment_id = fragment_id
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("forget-{}", chrono::Utc::now().timestamp_millis()));

    let tombstones: Vec<RapIndexEntry> = keys
        .iter()
        .map(|k| RapIndexEntry {
            key: k.clone(),
            file: 0,
            row_numbers: Vec::new(),
            tombstone: true,
            ..Default::default()
        })
        .collect();
    let buckets = bucketize_entries(tombstones, num_buckets);
    let meta = IndexFragmentMeta {
        fragment_id: fragment_id.clone(),
        created_at: chrono::Utc::now().to_rfc3339(),
        files: Vec::new(),
        num_buckets,
        note: Some(format!("forget {} key(s)", keys.len())),
        key_columns,
        value_columns,
        iceberg_snapshot_id: None,
        file_idents: Vec::new(),
        dropped_files: Vec::new(),
        iceberg_delete_files: Vec::new(),
    };
    let frag_dir = write_fragment_dir(root, &meta, &buckets)?;
    append_forgotten(root, keys)?;

    let mut registry: Vec<String> = if root.join("registry.json").exists() {
        read_registry(root)?
    } else {
        Vec::new()
    };
    if !registry.iter().any(|id| id == &fragment_id) {
        registry.push(fragment_id);
    }
    write_registry(root, &registry)?;
    Ok(frag_dir)
}

fn probe_live_ident(ident: &FileIdent, root: &Path) -> Option<FileIdent> {
    let open = if is_remote_uri(&ident.path) {
        PathBuf::from(&ident.path)
    } else {
        resolve_data_path(root, &ident.path)
    };
    let live = probe_file_ident(&ident.path, &open);
    if live.has_identity() {
        Some(live)
    } else {
        None
    }
}

/// Check stored file identity (size / etag / mtime) against the live objects.
/// Uses the same comparison as the query path (`file_ident_mismatch`).
pub fn verify_index_files(root: impl AsRef<Path>) -> Result<VerifyReport> {
    let root = root.as_ref();
    let registry = read_registry(root)?;
    let mut checked = 0usize;
    let mut skipped = 0usize;
    let mut stale = Vec::new();

    for frag_id in registry {
        let frag_dir = root.join("fragments").join(&frag_id);
        let meta: IndexFragmentMeta =
            serde_json::from_reader(File::open(frag_dir.join("manifest.json"))?)?;
        let idents: Vec<FileIdent> = if meta.file_idents.is_empty() {
            meta.files
                .iter()
                .map(|p| FileIdent {
                    path: p.clone(),
                    ..Default::default()
                })
                .collect()
        } else {
            meta.file_idents
        };

        for ident in idents {
            let live = probe_live_ident(&ident, root);
            match live {
                None => {
                    checked += 1;
                    stale.push(ident.path);
                }
                Some(live) => {
                    if !ident.has_identity() {
                        skipped += 1;
                        continue;
                    }
                    checked += 1;
                    if file_ident_mismatch(&ident, &live) {
                        stale.push(ident.path);
                    }
                }
            }
        }
    }

    Ok(VerifyReport {
        checked,
        stale,
        skipped,
    })
}

fn resolve_data_path(index_root: &Path, rel: &str) -> PathBuf {
    // Keep object-store URIs intact (s3://… / s3a://… / http(s)://…).
    if S3Client::is_remote_uri(rel) {
        return PathBuf::from(rel);
    }
    let p = PathBuf::from(rel);
    if p.is_absolute() {
        return p;
    }
    let candidates = [
        index_root.parent().unwrap_or(index_root).join(rel),
        index_root.join(rel),
        PathBuf::from(rel),
    ];
    for c in candidates {
        if c.exists() {
            return c;
        }
    }
    index_root.parent().unwrap_or(index_root).join(rel)
}

fn read_jsonl(path: &Path) -> Result<Vec<RapIndexEntry>> {
    let f = File::open(path)?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        out.push(serde_json::from_str(&line)?);
    }
    Ok(out)
}

fn fold_ts(min_ts: &mut Option<i64>, max_ts: &mut Option<i64>, ts: i64) {
    *min_ts = Some(match *min_ts {
        Some(m) => m.min(ts),
        None => ts,
    });
    *max_ts = Some(match *max_ts {
        Some(m) => m.max(ts),
        None => ts,
    });
}

fn format_timestamp_ms(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| ms.to_string())
}

fn format_date32(days: i32) -> String {
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| days.to_string())
}

fn timestamp_unit_to_ms(raw: i64, unit: arrow::datatypes::TimeUnit) -> i64 {
    match unit {
        arrow::datatypes::TimeUnit::Second => raw.saturating_mul(1_000),
        arrow::datatypes::TimeUnit::Millisecond => raw,
        arrow::datatypes::TimeUnit::Microsecond => raw / 1_000,
        arrow::datatypes::TimeUnit::Nanosecond => raw / 1_000_000,
    }
}

fn timestamp_raw_at(arr: &dyn arrow::array::Array, i: usize) -> Option<i64> {
    use arrow::array::{
        Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Some(a.value(i));
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Some(a.value(i));
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(a.value(i));
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        return Some(a.value(i));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(i));
    }
    None
}

fn timestamp_ms_at(col: &dyn arrow::array::Array, i: usize) -> Option<i64> {
    use arrow::array::{Date32Array, DictionaryArray, Int64Array};
    use arrow::datatypes::{
        ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    };
    if col.is_null(i) {
        return None;
    }
    match col.data_type() {
        DataType::Timestamp(unit, _) => {
            timestamp_raw_at(col, i).map(|raw| timestamp_unit_to_ms(raw, *unit))
        }
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(i)),
        DataType::Date32 => col
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| (a.value(i) as i64).saturating_mul(86_400_000)),
        DataType::Dictionary(_, _) => {
            macro_rules! dict {
                ($t:ty) => {
                    if let Some(d) = col.as_any().downcast_ref::<DictionaryArray<$t>>() {
                        let idx = d.keys().value(i).to_usize()?;
                        return timestamp_ms_at(d.values().as_ref(), idx);
                    }
                };
            }
            dict!(Int8Type);
            dict!(Int16Type);
            dict!(Int32Type);
            dict!(Int64Type);
            dict!(UInt8Type);
            dict!(UInt16Type);
            dict!(UInt32Type);
            dict!(UInt64Type);
            None
        }
        _ => None,
    }
}

const UNSUPPORTED_ICEBERG_DELETES: &str =
    "needle refuses Iceberg tables with unsupported delete files; apply deletes or compact first";

fn equality_row_deleted(
    batch: &arrow::record_batch::RecordBatch,
    row: usize,
    deletes: &[HashMap<String, Option<String>>],
) -> Result<bool> {
    if deletes.is_empty() {
        return Ok(false);
    }
    for rec in deletes {
        if equality_record_matches(batch, row, rec)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn equality_record_matches(
    batch: &arrow::record_batch::RecordBatch,
    row: usize,
    rec: &HashMap<String, Option<String>>,
) -> Result<bool> {
    for (col, want) in rec {
        let arr = batch.column_by_name(col).with_context(|| {
            format!("{UNSUPPORTED_ICEBERG_DELETES} (missing equality column `{col}`)")
        })?;
        let is_null = arr.is_null(row);
        match (want, is_null) {
            (None, true) => {}
            (None, false) | (Some(_), true) => return Ok(false),
            (Some(w), false) => {
                if encode_array_value(arr.as_ref(), row)? != *w {
                    return Ok(false);
                }
            }
        }
    }
    Ok(!rec.is_empty())
}

pub(crate) fn encode_array_value(arr: &dyn arrow::array::Array, i: usize) -> Result<String> {
    use arrow::array::{
        Date32Array, DictionaryArray, Int32Array, Int64Array, LargeStringArray, StringArray,
        UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{
        ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    };
    if arr.is_null(i) {
        return Ok(String::new());
    }
    match arr.data_type() {
        DataType::Utf8 => Ok(arr
            .as_any()
            .downcast_ref::<StringArray>()
            .context("Utf8 array")?
            .value(i)
            .to_string()),
        DataType::LargeUtf8 => Ok(arr
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .context("LargeUtf8 array")?
            .value(i)
            .to_string()),
        DataType::Int64 => Ok(arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Int64 array")?
            .value(i)
            .to_string()),
        DataType::Int32 => Ok(arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("Int32 array")?
            .value(i)
            .to_string()),
        DataType::UInt64 => Ok(arr
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("UInt64 array")?
            .value(i)
            .to_string()),
        DataType::UInt32 => Ok(arr
            .as_any()
            .downcast_ref::<UInt32Array>()
            .context("UInt32 array")?
            .value(i)
            .to_string()),
        DataType::Date32 => {
            let days = arr
                .as_any()
                .downcast_ref::<Date32Array>()
                .context("Date32 array")?
                .value(i);
            Ok(format_date32(days))
        }
        DataType::Timestamp(unit, _) => {
            let raw =
                timestamp_raw_at(arr, i).with_context(|| format!("timestamp array at row {i}"))?;
            Ok(format_timestamp_ms(timestamp_unit_to_ms(raw, *unit)))
        }
        DataType::Dictionary(_, _) => {
            macro_rules! dict {
                ($t:ty) => {
                    if let Some(d) = arr.as_any().downcast_ref::<DictionaryArray<$t>>() {
                        let idx = d.keys().value(i).to_usize().context("dictionary index")?;
                        return encode_array_value(d.values().as_ref(), idx);
                    }
                };
            }
            dict!(Int8Type);
            dict!(Int16Type);
            dict!(Int32Type);
            dict!(Int64Type);
            dict!(UInt8Type);
            dict!(UInt16Type);
            dict!(UInt32Type);
            dict!(UInt64Type);
            bail!("unsupported dictionary key type for key encoding")
        }
        _ => {
            // Utf8View and other displayable types.
            if let Ok(s) = arrow::util::display::array_value_to_string(arr, i) {
                return Ok(s);
            }
            bail!("unsupported key column type {}", arr.data_type())
        }
    }
}

fn encode_row_key(
    batch: &arrow::record_batch::RecordBatch,
    key_columns: &[String],
    row: usize,
) -> Result<Option<String>> {
    let mut parts = Vec::with_capacity(key_columns.len());
    for name in key_columns {
        let col = batch
            .column_by_name(name)
            .with_context(|| format!("missing key column `{name}`"))?;
        if col.is_null(row) {
            return Ok(None);
        }
        parts.push(encode_array_value(col.as_ref(), row)?);
    }
    let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
    Ok(Some(encode_key(&refs)))
}

fn struct_timestamp_col(st: &arrow::array::StructArray) -> Option<&dyn arrow::array::Array> {
    use arrow::datatypes::DataType;
    if let Some(c) = st.column_by_name("timestamp") {
        return Some(c.as_ref());
    }
    if let Some(c) = st.column_by_name("timestamp_ms") {
        return Some(c.as_ref());
    }
    for i in 0..st.num_columns() {
        let col = st.column(i);
        if matches!(col.data_type(), DataType::Timestamp(_, _)) {
            return Some(col.as_ref());
        }
    }
    None
}

/// Hoist listen_count + total_duration_ms (+ time range) from cogrouped `listens: LIST<STRUCT>`.
fn covering_from_nested_listens(
    listens: &arrow::array::ListArray,
    row: usize,
) -> Option<(u64, u64, Option<i64>, Option<i64>)> {
    use arrow::array::{Array, Int64Array, StructArray, UInt64Array};
    if listens.is_null(row) {
        return Some((0, 0, None, None));
    }
    let arr = listens.value(row);
    let st = arr.as_any().downcast_ref::<StructArray>()?;
    let count = st.len() as u64;
    let mut sum = 0u64;
    if let Some(col) = st.column_by_name("duration_ms") {
        if let Some(d) = col.as_any().downcast_ref::<Int64Array>() {
            for j in 0..st.len() {
                if !d.is_null(j) {
                    sum = sum.saturating_add(d.value(j).max(0) as u64);
                }
            }
        } else if let Some(d) = col.as_any().downcast_ref::<UInt64Array>() {
            for j in 0..st.len() {
                if !d.is_null(j) {
                    sum = sum.saturating_add(d.value(j));
                }
            }
        }
    }
    let mut min_ts = None;
    let mut max_ts = None;
    if let Some(ts_col) = struct_timestamp_col(st) {
        for j in 0..st.len() {
            if let Some(ts) = timestamp_ms_at(ts_col, j) {
                fold_ts(&mut min_ts, &mut max_ts, ts);
            }
        }
    }
    Some((count, sum, min_ts, max_ts))
}

#[derive(Default)]
struct KeyAgg {
    rows: Vec<u64>,
    listen_count: u64,
    total_duration_ms: u64,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
}

fn scan_key_column(
    path: &Path,
    key_columns: &[String],
    covering: bool,
    pos_deletes: &HashSet<u64>,
    equality_deletes: &[HashMap<String, Option<String>>],
) -> Result<Vec<(String, Vec<u64>, Option<CoveringValues>)>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let uri = path.to_string_lossy();
    if S3Client::is_remote_uri(&uri) {
        let (bucket, key) = S3Client::parse_uri(&uri)?;
        let reader = S3ChunkReader::open(S3Client::from_env(), bucket, key)
            .with_context(|| format!("s3 chunk reader {uri}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader)
            .with_context(|| format!("parquet builder {uri}"))?;
        return scan_key_column_builder(
            builder,
            key_columns,
            covering,
            pos_deletes,
            equality_deletes,
        );
    }
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("parquet builder {}", path.display()))?;
    scan_key_column_builder(
        builder,
        key_columns,
        covering,
        pos_deletes,
        equality_deletes,
    )
}

fn scan_key_column_builder<T: parquet::file::reader::ChunkReader + 'static>(
    builder: parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder<T>,
    key_columns: &[String],
    covering: bool,
    pos_deletes: &HashSet<u64>,
    equality_deletes: &[HashMap<String, Option<String>>],
) -> Result<Vec<(String, Vec<u64>, Option<CoveringValues>)>> {
    use arrow::array::{Array, Int64Array, ListArray, StringArray, UInt64Array};
    let schema = builder.schema();
    for col in key_columns {
        schema
            .index_of(col)
            .ok()
            .with_context(|| format!("missing key column `{col}`"))?;
    }
    let reader = builder.build()?;

    let mut map: HashMap<String, KeyAgg> = HashMap::new();
    let mut row_base: u64 = 0;

    for batch in reader {
        let batch = batch?;
        let n = batch.num_rows();

        let durations: Option<&Int64Array> = if covering {
            batch
                .column_by_name("duration_ms")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        } else {
            None
        };
        let durations_u64: Option<&UInt64Array> = if covering && durations.is_none() {
            batch
                .column_by_name("duration_ms")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        } else {
            None
        };

        // Blob mode: covering from JSON payload if no duration column.
        let payload_col: Option<&StringArray> =
            if covering && durations.is_none() && durations_u64.is_none() {
                batch
                    .column_by_name("payload")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            } else {
                None
            };

        // Cogrouped: nested LIST<STRUCT> of listens - hoist list length + duration sum.
        let listens_col: Option<&ListArray> = if covering {
            batch
                .column_by_name("listens")
                .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        } else {
            None
        };

        let timestamp_col: Option<&dyn Array> = if covering {
            batch
                .column_by_name("timestamp")
                .or_else(|| batch.column_by_name("timestamp_ms"))
                .map(|c| c.as_ref())
        } else {
            None
        };

        for i in 0..n {
            let row_id = row_base + i as u64;
            if pos_deletes.contains(&row_id) {
                continue;
            }
            if equality_row_deleted(&batch, i, equality_deletes)? {
                continue;
            }
            let Some(key) = encode_row_key(&batch, key_columns, i)? else {
                continue;
            };
            let entry = map.entry(key).or_default();
            entry.rows.push(row_base + i as u64);
            if !covering {
                continue;
            }
            if let Some(list) = listens_col {
                if let Some((count, dur, min_ts, max_ts)) = covering_from_nested_listens(list, i) {
                    entry.listen_count = entry.listen_count.saturating_add(count);
                    entry.total_duration_ms = entry.total_duration_ms.saturating_add(dur);
                    if let Some(t) = min_ts {
                        fold_ts(&mut entry.min_ts, &mut entry.max_ts, t);
                    }
                    if let Some(t) = max_ts {
                        fold_ts(&mut entry.min_ts, &mut entry.max_ts, t);
                    }
                } else {
                    entry.listen_count += 1;
                }
            } else if let Some(p) = payload_col {
                // Count listens inside JSON blob.
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(p.value(i)) {
                    if let Some(arr) = v.get("listens").and_then(|x| x.as_array()) {
                        entry.listen_count += arr.len() as u64;
                        for item in arr {
                            if let Some(d) = item.get("duration_ms").and_then(|x| x.as_i64()) {
                                entry.total_duration_ms =
                                    entry.total_duration_ms.saturating_add(d.max(0) as u64);
                            }
                            if let Some(ts) = item
                                .get("timestamp_ms")
                                .and_then(|x| x.as_i64())
                                .or_else(|| item.get("timestamp").and_then(|x| x.as_i64()))
                            {
                                fold_ts(&mut entry.min_ts, &mut entry.max_ts, ts);
                            }
                        }
                    } else {
                        entry.listen_count += 1;
                    }
                } else {
                    entry.listen_count += 1;
                }
            } else {
                entry.listen_count += 1;
                if let Some(d) = durations {
                    if !d.is_null(i) {
                        entry.total_duration_ms = entry
                            .total_duration_ms
                            .saturating_add(d.value(i).max(0) as u64);
                    }
                } else if let Some(d) = durations_u64 {
                    if !d.is_null(i) {
                        entry.total_duration_ms =
                            entry.total_duration_ms.saturating_add(d.value(i));
                    }
                }
                if let Some(ts_col) = timestamp_col {
                    if let Some(ts) = timestamp_ms_at(ts_col, i) {
                        fold_ts(&mut entry.min_ts, &mut entry.max_ts, ts);
                    }
                }
            }
        }
        row_base += n as u64;
    }

    let mut out: Vec<_> = map
        .into_iter()
        .map(|(k, agg)| {
            let cov = if covering {
                Some(CoveringValues {
                    listen_count: agg.listen_count,
                    total_duration_ms: agg.total_duration_ms,
                    min_ts: agg.min_ts,
                    max_ts: agg.max_ts,
                })
            } else {
                None
            };
            (k, agg.rows, cov)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{write_sample_dataset, WriteMode, WriterOptions};
    use std::collections::HashSet;
    use std::io::Write;
    use std::sync::Arc;

    fn tiny_opts(dir: &Path, mode: WriteMode, files: usize) -> WriterOptions {
        WriterOptions {
            out_dir: dir.to_path_buf(),
            num_users: 24,
            listens_per_user: 6,
            num_files: files,
            mode,
            rows_per_row_group: 64,
            write_page_index: true,
            seed: 42,
            one_page_per_key: false,
        }
    }

    #[test]
    fn bucket_stable() {
        let b = key_bucket("user_42", 16);
        assert_eq!(b, key_bucket("user_42", 16));
        assert!(b < 16);
    }

    #[test]
    fn bucket_distribution_spreads_keys() {
        let n = 16u32;
        let mut counts = vec![0usize; n as usize];
        for i in 0..256 {
            let b = key_bucket(&format!("user_{i:04}"), n) as usize;
            counts[b] += 1;
        }
        // Every bucket gets at least one key; no bucket gets everything.
        assert!(counts.iter().all(|&c| c > 0));
        assert!(counts.iter().all(|&c| c < 256));
        let min = *counts.iter().min().unwrap();
        let max = *counts.iter().max().unwrap();
        assert!(max - min < 40, "skew too high: {counts:?}");
    }

    #[test]
    fn append_only_fragments_grow_registry() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 2)).unwrap();

        let builder = IndexBuilder::new(&idx_root, 8).with_covering(true);
        builder
            .build_fragment(&paths, "frag-001", Some("first"))
            .unwrap();
        // Second fragment with same files (simulates append of another partition).
        builder
            .build_fragment(&paths, "frag-002", Some("second"))
            .unwrap();

        let registry: serde_json::Value =
            serde_json::from_reader(File::open(idx_root.join("registry.json")).unwrap()).unwrap();
        assert_eq!(registry["format_version"], 1);
        assert_eq!(
            registry["fragments"],
            serde_json::json!(["frag-001", "frag-002"])
        );

        let index = load_index(&idx_root).unwrap();
        assert_eq!(index.fragments.len(), 2);
        // Same key appears in both fragments → multiple entries.
        let entries = index.lookup("user_0000");
        assert!(
            entries.len() >= 2,
            "expected multi-fragment entries, got {}",
            entries.len()
        );
    }

    #[test]
    fn entry_fields_and_lookup_hit_miss() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 2)).unwrap();
        IndexBuilder::new(&idx_root, 8)
            .with_covering(true)
            .build_fragment(&paths, "frag-001", None)
            .unwrap();
        let index = load_index(&idx_root).unwrap();

        assert!(index.lookup("no_such_user").is_empty());
        let entries = index.lookup("user_0010");
        assert!(!entries.is_empty());
        for e in entries {
            assert_eq!(e.key, "user_0010");
            assert!(!e.row_numbers.is_empty());
            assert_eq!(e.value_count, Some(e.row_numbers.len() as u64));
            let cov = e.covering.as_ref().expect("covering");
            assert_eq!(cov.listen_count, e.row_numbers.len() as u64);
            assert!(cov.total_duration_ms > 0);
            // Sorted + page index → page_locs captured.
            let locs = e.page_locs.as_ref().expect("page_locs");
            assert!(locs.iter().any(|l| l.column == "user_id"));
            assert!(locs.iter().any(|l| l.column == "track_uri"));
            assert!(locs.iter().all(|l| l.size > 0));
        }
        assert!(index.num_keys() >= 20);
        assert!(index.num_entries() >= index.num_keys());
    }

    #[test]
    fn multi_entry_keys_across_files_unsorted() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Unsorted, 4)).unwrap();
        IndexBuilder::new(&idx_root, 8)
            .build_fragment(&paths, "frag-u", None)
            .unwrap();
        let index = load_index(&idx_root).unwrap();

        // Find a key that landed in >1 file (very likely with 24 users × 6 listens × 4 files).
        let multi = index.entries_by_key.iter().find(|(_, ents)| {
            let files: HashSet<_> = ents.iter().map(|e| e.file).collect();
            files.len() > 1
        });
        assert!(
            multi.is_some(),
            "expected at least one key spanning multiple files"
        );
    }

    #[test]
    fn load_prefers_bincode_and_jsonl_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 1)).unwrap();
        IndexBuilder::new(&idx_root, 4)
            .with_covering(true)
            .build_fragment(&paths, "frag-bin", None)
            .unwrap();

        // Both formats exist.
        let bucket = idx_root
            .join("fragments")
            .join("frag-bin")
            .join("buckets")
            .join("bucket_000.bin");
        let jsonl = bucket.with_extension("jsonl");
        assert!(bucket.exists());
        assert!(jsonl.exists());

        let via_bin = load_index(&idx_root).unwrap();
        assert!(!via_bin.lookup("user_0000").is_empty());

        // Delete bincode → loader falls back to JSONL.
        std::fs::remove_file(&bucket).unwrap();
        // Wipe other bins too so JSONL path is exercised for all buckets.
        for bi in 0..4u32 {
            let p = idx_root
                .join("fragments")
                .join("frag-bin")
                .join("buckets")
                .join(format!("bucket_{bi:03}.bin"));
            let _ = std::fs::remove_file(p);
        }
        let via_jsonl = load_index(&idx_root).unwrap();
        assert_eq!(via_bin.num_keys(), via_jsonl.num_keys());
        let a = via_bin.lookup("user_0005");
        let b = via_jsonl.lookup("user_0005");
        assert_eq!(a.len(), b.len());
        assert_eq!(a[0].row_numbers, b[0].row_numbers);
        assert_eq!(a[0].covering, b[0].covering);
    }

    #[test]
    fn one_page_per_key_stores_page_locs() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let mut opts = tiny_opts(&data, WriteMode::OnePagePerKey, 1);
        opts.num_users = 8;
        opts.listens_per_user = 4;
        let paths = write_sample_dataset(&opts).unwrap();
        IndexBuilder::new(&idx_root, 4)
            .build_fragment(&paths, "frag-opp", None)
            .unwrap();
        let index = load_index(&idx_root).unwrap();
        let e = &index.lookup("user_0003")[0];
        let locs = e.page_locs.as_ref().unwrap();
        let user_pages: Vec<_> = locs.iter().filter(|l| l.column == "user_id").collect();
        assert_eq!(user_pages.len(), 1, "one page per key for user_id");
    }

    #[test]
    fn cogrouped_covering_hoists_nested_list_aggregates() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let mut opts = tiny_opts(&data, WriteMode::Cogrouped, 2);
        opts.num_users = 12;
        opts.listens_per_user = 5;
        let paths = write_sample_dataset(&opts).unwrap();
        IndexBuilder::new(&idx_root, 4)
            .with_covering(true)
            .build_fragment(&paths, "frag-cg", None)
            .unwrap();
        let index = load_index(&idx_root).unwrap();

        for key in ["user_0000", "user_0006", "user_0011"] {
            let entries = index.lookup(key);
            assert!(!entries.is_empty(), "missing {key}");
            let parquet_rows: usize = entries.iter().map(|e| e.row_numbers.len()).sum();
            assert_eq!(parquet_rows, 1, "cogrouped is one parquet row per key");
            let cov_count: u64 = entries
                .iter()
                .map(|e| e.covering.as_ref().expect("covering").listen_count)
                .sum();
            let cov_dur: u64 = entries
                .iter()
                .map(|e| e.covering.as_ref().unwrap().total_duration_ms)
                .sum();
            assert_eq!(
                cov_count, 5,
                "{key}: covering listen_count must be nested list length, not parquet row count"
            );
            assert!(cov_dur > 0, "{key}: nested duration sum should be hoisted");
            assert_eq!(
                entries
                    .iter()
                    .map(|e| e.value_count.unwrap_or(0))
                    .sum::<u64>(),
                5
            );
        }
    }

    fn first_string_in_col(paths: &[PathBuf], col: &str) -> String {
        use arrow::array::{Array, StringArray};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        for path in paths {
            let file = File::open(path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let arr = batch
                    .column_by_name(col)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        return arr.value(i).to_string();
                    }
                }
            }
        }
        panic!("no values for column {col}");
    }

    fn first_track_for_user(paths: &[PathBuf], user: &str) -> String {
        use arrow::array::{Array, StringArray};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        for path in paths {
            let file = File::open(path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let users = batch
                    .column_by_name("user_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let tracks = batch
                    .column_by_name("track_uri")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    if users.value(i) == user {
                        return tracks.value(i).to_string();
                    }
                }
            }
        }
        panic!("user {user} not found");
    }

    #[test]
    fn default_index_finds_user_0000() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 2)).unwrap();
        IndexBuilder::new(&idx_root, 8)
            .build_fragment(&paths, "frag-default", None)
            .unwrap();
        let index = load_index(&idx_root).unwrap();
        assert!(!index.lookup("user_0000").is_empty());
    }

    #[test]
    fn index_by_track_uri_lookup_hits() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let mut opts = tiny_opts(&data, WriteMode::Sorted, 1);
        opts.num_users = 8;
        opts.listens_per_user = 3;
        let paths = write_sample_dataset(&opts).unwrap();
        let track = first_string_in_col(&paths, "track_uri");
        IndexBuilder::new(&idx_root, 4)
            .with_key_columns(vec!["track_uri".into()])
            .build_fragment(&paths, "frag-track", None)
            .unwrap();
        let index = load_index(&idx_root).unwrap();
        let entries = index.lookup(&track);
        assert!(!entries.is_empty(), "expected hits for track_uri {track}");
        let via_encode = index.lookup(&encode_key(&[track.as_str()]));
        assert_eq!(entries.len(), via_encode.len());
    }

    #[test]
    fn compound_user_and_track_key_lookup() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let mut opts = tiny_opts(&data, WriteMode::Sorted, 1);
        opts.num_users = 8;
        opts.listens_per_user = 3;
        let paths = write_sample_dataset(&opts).unwrap();
        let track = first_track_for_user(&paths, "user_0000");
        IndexBuilder::new(&idx_root, 4)
            .with_key_columns(vec!["user_id".into(), "track_uri".into()])
            .build_fragment(&paths, "frag-compound", None)
            .unwrap();
        let index = load_index(&idx_root).unwrap();
        let key = encode_key(&["user_0000", track.as_str()]);
        let entries = index.lookup(&key);
        assert!(
            !entries.is_empty(),
            "expected compound key hit for {key:?} track={track}"
        );
    }

    #[test]
    fn covering_min_max_ts_on_sorted() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 2)).unwrap();
        IndexBuilder::new(&idx_root, 8)
            .with_covering(true)
            .build_fragment(&paths, "frag-ts", None)
            .unwrap();
        let index = load_index(&idx_root).unwrap();
        let entries = index.lookup("user_0000");
        assert!(!entries.is_empty());
        for e in entries {
            let cov = e.covering.as_ref().expect("covering");
            let min_ts = cov.min_ts.expect("min_ts");
            let max_ts = cov.max_ts.expect("max_ts");
            assert!(min_ts <= max_ts, "min_ts={min_ts} max_ts={max_ts}");
        }
    }

    #[test]
    fn missing_key_column_errors_with_name() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let mut opts = tiny_opts(&data, WriteMode::Sorted, 1);
        opts.num_users = 4;
        opts.listens_per_user = 2;
        let paths = write_sample_dataset(&opts).unwrap();
        let err = IndexBuilder::new(&idx_root, 4)
            .with_key_columns(vec!["definitely_missing".into()])
            .build_fragment(&paths, "frag-miss", None)
            .expect_err("missing key column should error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("definitely_missing"),
            "error should mention the column name, got: {msg}"
        );
    }

    #[test]
    fn forget_key_hides_lookup() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 2)).unwrap();
        IndexBuilder::new(&idx_root, 8)
            .build_fragment(&paths, "frag-001", None)
            .unwrap();
        forget_keys(&idx_root, &[String::from("user_0000")], Some("forget-001")).unwrap();

        let index = load_index(&idx_root).unwrap();
        assert!(
            index.lookup("user_0000").is_empty(),
            "tombstone should hide user_0000"
        );
        assert!(
            !index.lookup("user_0001").is_empty(),
            "other keys must still hit"
        );

        let keyed = load_index_for_keys(&idx_root, &[String::from("user_0000")]).unwrap();
        assert!(keyed.lookup("user_0000").is_empty());
    }

    #[test]
    fn forget_is_sticky_across_new_data_fragment() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 2)).unwrap();
        let builder = IndexBuilder::new(&idx_root, 8);
        builder.build_fragment(&paths, "frag-001", None).unwrap();
        forget_keys(&idx_root, &[String::from("user_0000")], Some("forget-001")).unwrap();
        builder
            .build_fragment(&paths, "frag-002", Some("later"))
            .unwrap();
        let index = load_index(&idx_root).unwrap();
        assert!(
            index.lookup("user_0000").is_empty(),
            "forgotten keys must not resurrect after a later data fragment"
        );
        assert!(!index.lookup("user_0001").is_empty());
    }

    #[test]
    fn compact_drops_tombstones_and_keeps_data() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 2)).unwrap();
        let builder = IndexBuilder::new(&idx_root, 8);
        builder
            .build_fragment(&paths, "frag-001", Some("first"))
            .unwrap();
        builder
            .build_fragment(&paths, "frag-002", Some("second"))
            .unwrap();
        forget_keys(&idx_root, &[String::from("user_0000")], Some("forget-001")).unwrap();

        let report = compact_index(&idx_root, Some("compact-001")).unwrap();
        assert_eq!(report.fragment_id, "compact-001");
        assert!(report.dropped_tombstoned_keys >= 1);
        assert!(report.keys >= 1);
        assert!(report.entries >= 1);

        let registry: serde_json::Value =
            serde_json::from_reader(File::open(idx_root.join("registry.json")).unwrap()).unwrap();
        assert_eq!(registry["format_version"], 1);
        assert_eq!(registry["fragments"], serde_json::json!(["compact-001"]));
        assert!(!idx_root.join("fragments").join("frag-001").exists());
        assert!(!idx_root.join("fragments").join("frag-002").exists());
        assert!(!idx_root.join("fragments").join("forget-001").exists());
        assert!(idx_root.join("fragments").join("compact-001").exists());

        let index = load_index(&idx_root).unwrap();
        assert!(index.lookup("user_0000").is_empty());
        assert!(!index.lookup("user_0001").is_empty());
        assert_eq!(index.fragments.len(), 1);
    }

    #[test]
    fn compact_honors_dropped_files() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 2)).unwrap();
        IndexBuilder::new(&idx_root, 8)
            .build_fragment(&paths, "frag-001", None)
            .unwrap();
        let man = idx_root.join("fragments/frag-001/manifest.json");
        let mut meta: IndexFragmentMeta =
            serde_json::from_reader(File::open(&man).unwrap()).unwrap();
        assert!(meta.files.len() >= 2);
        let drop_path = meta.files[0].clone();
        let keep_path = meta.files[1].clone();
        let before = load_index(&idx_root).unwrap();
        let gone = key_only_in_stored(&before, &drop_path);
        let kept = key_only_in_stored(&before, &keep_path);
        meta.dropped_files = vec![drop_path.clone()];
        serde_json::to_writer_pretty(File::create(&man).unwrap(), &meta).unwrap();

        let full = load_index(&idx_root).unwrap();
        let keyed = load_index_for_keys(&idx_root, &[gone.clone()]).unwrap();
        assert!(
            full.lookup(&gone).is_empty(),
            "{gone} lives in dropped file"
        );
        assert!(keyed.lookup(&gone).is_empty());
        assert!(!full.lookup(&kept).is_empty());

        compact_index(&idx_root, Some("compact-drop")).unwrap();
        let after = load_index(&idx_root).unwrap();
        assert!(after.lookup(&gone).is_empty());
        assert!(!after.lookup(&kept).is_empty());
        assert!(
            after.fragments[0]
                .files
                .iter()
                .all(|f| normalize_stored_key(f) != normalize_stored_key(&drop_path)),
            "compacted dict should omit dropped path {drop_path}"
        );
    }

    fn key_only_in_stored(index: &RapIndex, stored: &str) -> String {
        let want = resolve_data_path(&index.root, stored);
        let want = want.canonicalize().unwrap_or(want);
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
        panic!("no key lives only in {stored}");
    }

    #[test]
    fn verify_detects_resized_parquet() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 1)).unwrap();
        IndexBuilder::new(&idx_root, 4)
            .build_fragment(&paths, "frag-v", None)
            .unwrap();
        let ok = verify_index_files(&idx_root).unwrap();
        assert!(ok.stale.is_empty(), "fresh index should be clean: {ok:?}");
        assert!(ok.checked >= 1);

        let mut f = fs::OpenOptions::new().append(true).open(&paths[0]).unwrap();
        f.write_all(b"x").unwrap();
        drop(f);

        let bad = verify_index_files(&idx_root).unwrap();
        assert!(
            !bad.stale.is_empty(),
            "resized parquet must be reported stale: {bad:?}"
        );

        let ident = &load_index(&idx_root).unwrap().fragments[0].file_idents[0];
        let live = probe_file_ident(ident.path.as_str(), &paths[0]);
        assert!(
            file_ident_mismatch(ident, &live),
            "verify uses file_ident_mismatch; stored={ident:?} live={live:?}"
        );
    }

    #[test]
    fn file_ident_mismatch_etag_abc_vs_xyz() {
        let stored = FileIdent {
            path: "s3://b/k".into(),
            etag: Some("abc".into()),
            size: Some(4),
            mtime_ms: None,
        };
        let live = FileIdent {
            etag: Some("xyz".into()),
            ..stored.clone()
        };
        assert!(file_ident_mismatch(&stored, &live));
        assert!(!file_ident_mismatch(&stored, &stored));
    }

    #[test]
    fn local_index_persists_size_and_mtime() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 1)).unwrap();
        IndexBuilder::new(&idx_root, 4)
            .build_fragment(&paths, "frag-mt", None)
            .unwrap();
        let man = idx_root
            .join("fragments")
            .join("frag-mt")
            .join("manifest.json");
        let meta: IndexFragmentMeta = serde_json::from_reader(File::open(&man).unwrap()).unwrap();
        assert!(
            !meta.file_idents.is_empty(),
            "fragment should store file_idents"
        );
        assert!(meta.file_idents[0].size.is_some());
        assert!(
            meta.file_idents[0].mtime_ms.is_some(),
            "local ident must persist mtime_ms: {:?}",
            meta.file_idents[0]
        );
        let index = load_index(&idx_root).unwrap();
        let e = &index.lookup("user_0000")[0];
        assert!(e.file_size.is_some());
        assert!(e.file_mtime_ms.is_some());
    }

    #[test]
    fn load_legacy_array_registry_still_works() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 1)).unwrap();
        IndexBuilder::new(&idx_root, 4)
            .build_fragment(&paths, "frag-001", None)
            .unwrap();
        serde_json::to_writer_pretty(
            File::create(idx_root.join("registry.json")).unwrap(),
            &vec!["frag-001".to_string()],
        )
        .unwrap();
        let index = load_index(&idx_root).unwrap();
        assert_eq!(index.fragments.len(), 1);
        assert!(!index.lookup("user_0000").is_empty());
    }

    #[test]
    fn load_unsupported_format_version_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 1)).unwrap();
        IndexBuilder::new(&idx_root, 4)
            .build_fragment(&paths, "frag-001", None)
            .unwrap();
        serde_json::to_writer_pretty(
            File::create(idx_root.join("registry.json")).unwrap(),
            &serde_json::json!({"format_version": 99, "fragments": ["frag-001"]}),
        )
        .unwrap();
        let err = load_index(&idx_root).expect_err("format_version 99 must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unsupported index format_version"),
            "got: {msg}"
        );
    }

    fn committed_v1_index_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v1-index")
    }

    #[test]
    fn load_committed_v1_index_fixture() {
        let root = committed_v1_index_dir();
        assert!(
            root.join("registry.json").is_file(),
            "committed v1 fixture missing at {}",
            root.display()
        );
        let index = load_index(&root).expect("load committed v1 fixture");
        let hits = index.lookup("user_0000");
        assert!(
            !hits.is_empty(),
            "fixture lookup user_0000 must be nonempty"
        );
        assert_eq!(hits[0].key, "user_0000");
        assert_eq!(hits[0].file, 0);
        assert!(!hits[0].row_numbers.is_empty());
        let q = crate::query::RapQuerier::new(index);
        let res = q
            .query("user_0000")
            .expect("query committed v1 fixture parquet");
        assert!(
            res.batch.num_rows() >= 1 || !res.rows.is_empty(),
            "frozen v1 fixture must be queryable"
        );
    }

    #[test]
    fn write_registry_is_format_version_1_object() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 1)).unwrap();
        IndexBuilder::new(&idx_root, 4)
            .build_fragment(&paths, "frag-001", None)
            .unwrap();
        let raw: serde_json::Value =
            serde_json::from_reader(File::open(idx_root.join("registry.json")).unwrap()).unwrap();
        assert!(
            raw.is_object(),
            "registry.json must be an object, got {raw}"
        );
        assert_eq!(raw["format_version"], 1);
        assert_eq!(raw["fragments"], serde_json::json!(["frag-001"]));
    }

    #[test]
    fn try_lock_index_is_exclusive() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rap-index");
        let _held = try_lock_index(&root).unwrap();
        let err = try_lock_index(&root).expect_err("second lock must fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("index lock"), "got: {msg}");
    }

    fn write_generic_id_amount(path: &Path) -> Result<()> {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::arrow_writer::ArrowWriter;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )?;
        let file = File::create(path)?;
        let mut w = ArrowWriter::try_new(file, schema, None)?;
        w.write(&batch)?;
        w.close()?;
        Ok(())
    }

    #[test]
    fn covering_refused_on_generic_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let parquet = tmp.path().join("generic.parquet");
        write_generic_id_amount(&parquet).unwrap();
        let idx = tmp.path().join("idx");
        let err = IndexBuilder::new(&idx, 4)
            .with_key_columns(vec!["id".into()])
            .with_covering(true)
            .build_fragment(&[parquet.clone()], "frag-g", None)
            .expect_err("covering on generic schema must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("covering is listen-shaped") || msg.contains("omit --covering"),
            "got {msg}"
        );
    }

    #[test]
    fn generic_id_amount_point_lookup_without_covering() {
        let tmp = tempfile::tempdir().unwrap();
        let parquet = tmp.path().join("generic.parquet");
        write_generic_id_amount(&parquet).unwrap();
        let idx = tmp.path().join("idx");
        IndexBuilder::new(&idx, 4)
            .with_key_columns(vec!["id".into()])
            .build_fragment(&[parquet], "frag-g", None)
            .unwrap();
        let index = load_index(&idx).unwrap();
        assert!(!index.lookup("a").is_empty());
        assert!(!index.lookup("b").is_empty());
        let q = crate::query::RapQuerier::new(index);
        let res = q.query("a").unwrap();
        assert!(res.batch.num_rows() >= 1);
    }

    #[test]
    fn lazy_lookup_stays_under_bucket_cap() {
        struct Guard;
        impl Drop for Guard {
            fn drop(&mut self) {
                test_clear_bucket_load_cap();
            }
        }
        let _g = Guard;
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 1)).unwrap();
        IndexBuilder::new(&idx_root, 8)
            .build_fragment(&paths, "frag-mmap", None)
            .unwrap();

        test_set_bucket_load_cap(1);
        let keyed = load_index_for_keys(&idx_root, &[String::from("user_0000")]).unwrap();
        assert!(
            !keyed.lookup("user_0000").is_empty(),
            "point lookup must succeed with one-bucket cap"
        );

        test_set_bucket_load_cap(1);
        let err = load_index(&idx_root).expect_err("full load exceeds one-bucket cap");
        let msg = format!("{err:#}");
        assert!(msg.contains("working set exceeded bucket cap"), "got {msg}");
    }

    #[test]
    fn overlapping_build_fragment_second_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 1)).unwrap();
        let idx_root = std::sync::Arc::new(idx_root);
        let paths = std::sync::Arc::new(paths);

        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder_root = idx_root.clone();
        let holder = std::thread::spawn(move || {
            let _lock = try_lock_index(&holder_root).unwrap();
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        ready_rx.recv().unwrap();
        let err = IndexBuilder::new(idx_root.as_path(), 4)
            .build_fragment(&paths, "frag-a", None)
            .expect_err("overlapping build_fragment must fail");
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        let msg = format!("{err:#}");
        assert!(msg.contains("index lock"), "got: {msg}");
    }

    #[test]
    fn compact_gc_leaves_only_compact_fragment() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx_root = tmp.path().join("rap-index");
        let paths = write_sample_dataset(&tiny_opts(&data, WriteMode::Sorted, 2)).unwrap();
        let builder = IndexBuilder::new(&idx_root, 8);
        builder
            .build_fragment(&paths, "frag-001", Some("first"))
            .unwrap();
        builder
            .build_fragment(&paths, "frag-002", Some("second"))
            .unwrap();
        forget_keys(&idx_root, &[String::from("user_0000")], Some("forget-001")).unwrap();

        compact_index(&idx_root, Some("compact-only")).unwrap();
        let mut names: Vec<String> = fs::read_dir(idx_root.join("fragments"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, vec!["compact-only".to_string()]);
        assert!(idx_root.join("forgotten.jsonl").exists());
        let index = load_index(&idx_root).unwrap();
        assert!(index.lookup("user_0000").is_empty());
        assert!(!index.lookup("user_0001").is_empty());
    }
}
