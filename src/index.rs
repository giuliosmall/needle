//! External RAP index: multimap key → {file ordinal, row numbers, value_count, …}.
//!
//! Article: "The index is a multimap — a single key can have entries across many
//! files and partitions. … Large indexes distribute naturally by hash bucketing.
//! The index grows by appending fragments, not modifying existing ones."
//!
//! Extended fields for write-time preparation:
//! - `page_locs` — one-page-per-key locations stored in the entry (no footer needed)
//! - `frame_locs` — ZSTD frame offset+size per column
//! - `contiguous` — interleaved multi-column span for one ranged read

use crate::prepared::{self, ByteSpan, FrameLoc, PreparedManifest};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Per-column page location stored directly in the index (article: one page per key).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PageLoc {
    pub column: String,
    pub offset: u64,
    pub size: u32,
}

/// One index entry: key lives in a specific file at specific rows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoveringValues {
    pub listen_count: u64,
    pub total_duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexFragmentMeta {
    pub fragment_id: String,
    pub created_at: String,
    pub files: Vec<String>,
    pub num_buckets: u32,
    pub note: Option<String>,
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

pub struct IndexBuilder {
    root: PathBuf,
    num_buckets: u32,
    covering: bool,
    /// Capture OffsetIndex page locs into entries (one-page-per-key).
    store_page_locs: bool,
}

impl IndexBuilder {
    pub fn new(root: impl AsRef<Path>, num_buckets: u32) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            num_buckets: num_buckets.max(1),
            covering: false,
            store_page_locs: true,
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

    pub fn build_fragment(
        &self,
        parquet_files: &[PathBuf],
        fragment_id: &str,
        note: Option<&str>,
    ) -> Result<PathBuf> {
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

            let key_rows = scan_key_column(path, self.covering)?;
            for (key, rows, covering) in key_rows {
                let page_locs = if self.store_page_locs {
                    capture_page_locs(path, &rows).ok()
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

        for (bi, entries) in buckets.iter().enumerate() {
            let jsonl_path = frag_dir
                .join("buckets")
                .join(format!("bucket_{bi:03}.jsonl"));
            let bin_path = frag_dir.join("buckets").join(format!("bucket_{bi:03}.bin"));
            write_jsonl(&jsonl_path, entries)?;
            write_bincode(&bin_path, entries)?;
        }

        let meta = IndexFragmentMeta {
            fragment_id: fragment_id.to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            files: file_dict,
            num_buckets: self.num_buckets,
            note: note.map(|s| s.to_string()),
        };
        serde_json::to_writer_pretty(File::create(frag_dir.join("manifest.json"))?, &meta)?;

        let registry_path = self.root.join("registry.json");
        let mut registry: Vec<String> = if registry_path.exists() {
            serde_json::from_reader(File::open(&registry_path)?)?
        } else {
            Vec::new()
        };
        if !registry.iter().any(|id| id == fragment_id) {
            registry.push(fragment_id.to_string());
        }
        serde_json::to_writer_pretty(File::create(&registry_path)?, &registry)?;

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
        let prepared_rel = if let Ok(r) =
            data_abs.strip_prefix(data_dir.parent().unwrap_or(data_dir))
        {
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
                    // Frames live in the parquet file — no sidecar needed.
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
                });
            }
        }
    }
    Ok(())
}

/// Capture OffsetIndex page locations for the rows of one key (all value columns).
fn capture_page_locs(path: &Path, rows: &[u64]) -> Result<Vec<PageLoc>> {
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use parquet::file::metadata::PageIndexPolicy;
    use std::collections::HashSet;

    let file = File::open(path)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let arrow_meta = ArrowReaderMetadata::load(&file, options)?;
    let pq = arrow_meta.metadata();
    let offset_indexes = pq
        .offset_index()
        .context("no offset index — cannot capture page locs")?;

    // Build row_group starts.
    let mut starts = Vec::new();
    let mut running = 0i64;
    for i in 0..pq.num_row_groups() {
        starts.push(running);
        running += pq.row_group(i).num_rows();
    }

    let mut locs = Vec::new();
    let mut seen = HashSet::new();
    let cols_wanted = ["user_id", "timestamp", "track_uri", "duration_ms", "payload", "payload_bytes"];

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
            if !cols_wanted.iter().any(|c| *c == leaf) {
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
    let bin_path = frag_dir
        .join("buckets")
        .join(format!("bucket_{bi:03}.bin"));
    let jsonl_path = frag_dir
        .join("buckets")
        .join(format!("bucket_{bi:03}.jsonl"));
    if bin_path.exists() {
        let bytes = fs::read(&bin_path)?;
        bincode::deserialize(&bytes).context("bincode deserialize")
    } else if jsonl_path.exists() {
        read_jsonl(&jsonl_path)
    } else {
        Ok(Vec::new())
    }
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
    let registry_path = root.join("registry.json");
    if !registry_path.exists() {
        bail!("no RAP index at {} (missing registry.json)", root.display());
    }
    let registry: Vec<String> = serde_json::from_reader(File::open(&registry_path)?)?;

    let mut files: Vec<PathBuf> = Vec::new();
    let mut entries_by_key: HashMap<String, Vec<RapIndexEntry>> = HashMap::new();
    let mut fragments: Vec<IndexFragmentMeta> = Vec::new();

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

        let wanted: Option<HashSet<u32>> = only_keys.map(|ks| {
            ks.iter().map(|k| key_bucket(k, meta.num_buckets)).collect()
        });
        let keep_keys: Option<HashSet<&str>> =
            only_keys.map(|ks| ks.iter().map(|s| s.as_str()).collect());

        for bi in 0..meta.num_buckets {
            if let Some(ref w) = wanted {
                if !w.contains(&bi) {
                    continue;
                }
            }
            let mut entries = read_bucket_entries(&frag_dir, bi)?;
            if entries.is_empty() {
                continue;
            }

            for e in &mut entries {
                if let Some(ref keep) = keep_keys {
                    if !keep.contains(e.key.as_str()) {
                        continue;
                    }
                }
                if (e.file as usize) < local_to_global.len() {
                    e.file = local_to_global[e.file as usize];
                }
                expand_compact_rows(e);
                entries_by_key
                    .entry(e.key.clone())
                    .or_default()
                    .push(e.clone());
            }
        }
        fragments.push(meta);
    }

    Ok(RapIndex {
        root,
        files: std::sync::Arc::new(files),
        entries_by_key,
        fragments,
    })
}

/// Load file dictionary once (heavy for 300k lakes). Reuse across stress waves.
pub fn load_index_file_dictionary(root: impl AsRef<Path>) -> Result<(std::sync::Arc<Vec<PathBuf>>, Vec<IndexFragmentMeta>, PathBuf)> {
    let root = root.as_ref().to_path_buf();
    let registry_path = root.join("registry.json");
    if !registry_path.exists() {
        bail!("no RAP index at {} (missing registry.json)", root.display());
    }
    let registry: Vec<String> = serde_json::from_reader(File::open(&registry_path)?)?;
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
    let mut file_base = 0usize;
    for meta in fragments {
        let frag_dir = root.join("fragments").join(&meta.fragment_id);
        let wanted: HashSet<u32> = keys
            .iter()
            .map(|k| key_bucket(k, meta.num_buckets))
            .collect();
        let n_files = meta.files.len();
        for bi in wanted {
            let mut entries = read_bucket_entries(&frag_dir, bi)?;
            for e in &mut entries {
                if !keep_keys.contains(e.key.as_str()) {
                    continue;
                }
                let local = e.file as usize;
                if local < n_files {
                    e.file = (file_base + local) as u32;
                }
                expand_compact_rows(e);
                entries_by_key
                    .entry(e.key.clone())
                    .or_default()
                    .push(e.clone());
            }
        }
        file_base += n_files;
    }
    Ok(RapIndex {
        root: root.to_path_buf(),
        files,
        entries_by_key,
        fragments: fragments.to_vec(),
    })
}

fn resolve_data_path(index_root: &Path, rel: &str) -> PathBuf {
    // Keep object-store URIs intact (s3://… or http://host/bucket/key).
    if rel.starts_with("s3://") || rel.starts_with("http://") || rel.starts_with("https://") {
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

/// Hoist listen_count + total_duration_ms from cogrouped `listens: LIST<STRUCT>`.
fn covering_from_nested_listens(
    listens: &arrow::array::ListArray,
    row: usize,
) -> Option<(u64, u64)> {
    use arrow::array::{Array, Int64Array, StructArray, UInt64Array};
    if listens.is_null(row) {
        return Some((0, 0));
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
    Some((count, sum))
}

fn scan_key_column(
    path: &Path,
    covering: bool,
) -> Result<Vec<(String, Vec<u64>, Option<CoveringValues>)>> {
    use arrow::array::{Array, Int64Array, ListArray, StringArray, UInt64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut map: HashMap<String, (Vec<u64>, u64, u64)> = HashMap::new();
    let mut row_base: u64 = 0;

    for batch in reader {
        let batch = batch?;
        let n = batch.num_rows();
        let user_col = batch
            .column_by_name("user_id")
            .context("missing user_id column")?;
        let users = user_col
            .as_any()
            .downcast_ref::<StringArray>()
            .context("user_id must be Utf8")?;

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
        let payload_col: Option<&StringArray> = if covering && durations.is_none() && durations_u64.is_none()
        {
            batch
                .column_by_name("payload")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        } else {
            None
        };

        // Cogrouped: nested LIST<STRUCT> of listens — hoist list length + duration sum.
        let listens_col: Option<&ListArray> = if covering {
            batch
                .column_by_name("listens")
                .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        } else {
            None
        };

        for i in 0..n {
            let key = users.value(i).to_string();
            let entry = map.entry(key).or_default();
            entry.0.push(row_base + i as u64);
            if let Some(list) = listens_col {
                if let Some((count, dur)) = covering_from_nested_listens(list, i) {
                    entry.1 = entry.1.saturating_add(count);
                    entry.2 = entry.2.saturating_add(dur);
                } else {
                    entry.1 += 1;
                }
            } else if let Some(d) = durations {
                entry.1 += 1;
                if !d.is_null(i) {
                    entry.2 = entry.2.saturating_add(d.value(i).max(0) as u64);
                }
            } else if let Some(d) = durations_u64 {
                entry.1 += 1;
                if !d.is_null(i) {
                    entry.2 = entry.2.saturating_add(d.value(i));
                }
            } else if let Some(p) = payload_col {
                // Count listens inside JSON blob.
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(p.value(i)) {
                    if let Some(arr) = v.get("listens").and_then(|x| x.as_array()) {
                        entry.1 += arr.len() as u64;
                        for item in arr {
                            if let Some(d) = item.get("duration_ms").and_then(|x| x.as_i64()) {
                                entry.2 = entry.2.saturating_add(d.max(0) as u64);
                            }
                        }
                    } else {
                        entry.1 += 1;
                    }
                } else {
                    entry.1 += 1;
                }
            } else {
                entry.1 += 1;
            }
        }
        row_base += n as u64;
    }

    let mut out: Vec<_> = map
        .into_iter()
        .map(|(k, (rows, count, dur))| {
            let cov = if covering {
                Some(CoveringValues {
                    listen_count: count,
                    total_duration_ms: dur,
                })
            } else {
                None
            };
            (k, rows, cov)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{WriteMode, WriterOptions, write_sample_dataset};
    use std::collections::HashSet;

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

        let registry: Vec<String> =
            serde_json::from_reader(File::open(idx_root.join("registry.json")).unwrap()).unwrap();
        assert_eq!(registry, vec!["frag-001".to_string(), "frag-002".to_string()]);

        let index = load_index(&idx_root).unwrap();
        assert_eq!(index.fragments.len(), 2);
        // Same key appears in both fragments → multiple entries.
        let entries = index.lookup("user_0000");
        assert!(entries.len() >= 2, "expected multi-fragment entries, got {}", entries.len());
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
        let multi = index
            .entries_by_key
            .iter()
            .find(|(_, ents)| {
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
            assert!(
                cov_dur > 0,
                "{key}: nested duration sum should be hoisted"
            );
            assert_eq!(
                entries.iter().map(|e| e.value_count.unwrap_or(0)).sum::<u64>(),
                5
            );
        }
    }
}
