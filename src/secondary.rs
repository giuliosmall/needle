//! Secondary indexes (article § Secondary Indexes).
//!
//! "RAP supports this by building multiple access structures over the same index
//! entries, one per dimension. Hash tables give O(1) exact lookups; sorted indexes
//! enable range queries. Adding or removing a secondary index is a serving-layer
//! decision - no pipeline changes, no data rewriting."
//!
//! Layout:
//!   fragments/<id>/secondary/<dim>/
//!     manifest.json
//!     hash/buckets/bucket_NNN.bin|.jsonl   - O(1) exact
//!     sorted/entries.bin|.jsonl            - range scans (keys sorted)

use crate::index::{key_bucket, RapIndexEntry};
use anyhow::{Context, Result, bail};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecondaryRef {
    pub dimension: String,
    pub key: String,
    pub fragment_id: String,
    pub primary_key: String,
    pub file: u32,
    pub row_numbers: Vec<u64>,
    #[serde(default)]
    pub value_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecondaryManifest {
    pub dimension: String,
    pub fragment_id: String,
    pub num_buckets: u32,
    pub num_keys: usize,
    pub num_entries: usize,
    pub created_at: String,
    /// Dictionary copied from primary fragment (ordinal → path).
    pub files: Vec<String>,
}

#[derive(Debug, Default)]
pub struct SecondaryIndex {
    pub dimension: String,
    pub files: Vec<PathBuf>,
    /// Exact O(1) map.
    pub by_key: HashMap<String, Vec<SecondaryRef>>,
    /// Sorted keys for range queries.
    pub sorted_keys: Vec<String>,
    pub root: PathBuf,
}

impl SecondaryIndex {
    pub fn lookup_exact(&self, key: &str) -> &[SecondaryRef] {
        self.by_key
            .get(key)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Inclusive range over secondary keys (lexicographic).
    pub fn lookup_range(&self, start: &str, end: &str) -> Vec<&SecondaryRef> {
        let lo = self.sorted_keys.binary_search_by(|k| k.as_str().cmp(start)).unwrap_or_else(|i| i);
        let mut out = Vec::new();
        for k in &self.sorted_keys[lo..] {
            if k.as_str() > end {
                break;
            }
            if let Some(refs) = self.by_key.get(k) {
                out.extend(refs.iter());
            }
        }
        out
    }

    pub fn num_keys(&self) -> usize {
        self.by_key.len()
    }
}

pub fn secondary_index_root(
    index_root: &Path,
    fragment_id: &str,
    dimension: &str,
) -> PathBuf {
    index_root
        .join("fragments")
        .join(fragment_id)
        .join("secondary")
        .join(dimension)
}

/// Build a secondary index over `dimension` by scanning the same Parquet files
/// already registered in the primary fragment - **no Parquet rewrite**.
pub fn build_secondary(
    index_root: &Path,
    fragment_id: &str,
    dimension: &str,
    num_buckets: u32,
) -> Result<PathBuf> {
    let frag_dir = index_root.join("fragments").join(fragment_id);
    let primary_manifest: crate::index::IndexFragmentMeta =
        serde_json::from_reader(File::open(frag_dir.join("manifest.json"))?)?;

    let out = secondary_index_root(index_root, fragment_id, dimension);
    fs::create_dir_all(out.join("hash").join("buckets"))?;
    fs::create_dir_all(out.join("sorted"))?;

    let mut buckets: Vec<Vec<SecondaryRef>> =
        (0..num_buckets.max(1)).map(|_| Vec::new()).collect();
    let mut all_refs: Vec<SecondaryRef> = Vec::new();

    for (ordinal, rel) in primary_manifest.files.iter().enumerate() {
        let abs = resolve_data_path(index_root, rel);
        if abs
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s != "parquet")
            .unwrap_or(true)
        {
            // Skip prepared sidecars in the dictionary.
            continue;
        }
        let scanned = scan_dimension(&abs, dimension)?;
        for (sec_key, primary_key, rows) in scanned {
            let r = SecondaryRef {
                dimension: dimension.to_string(),
                key: sec_key.clone(),
                fragment_id: fragment_id.to_string(),
                primary_key,
                file: ordinal as u32,
                value_count: Some(rows.len() as u64),
                row_numbers: rows,
            };
            let b = key_bucket(&sec_key, num_buckets.max(1)) as usize;
            buckets[b].push(r.clone());
            all_refs.push(r);
        }
    }

    for (bi, entries) in buckets.iter().enumerate() {
        let jsonl = out
            .join("hash")
            .join("buckets")
            .join(format!("bucket_{bi:03}.jsonl"));
        let bin = out
            .join("hash")
            .join("buckets")
            .join(format!("bucket_{bi:03}.bin"));
        write_jsonl(&jsonl, entries)?;
        fs::write(&bin, bincode::serialize(entries)?)?;
    }

    // Sorted index: sort by secondary key.
    all_refs.sort_by(|a, b| a.key.cmp(&b.key).then(a.primary_key.cmp(&b.primary_key)));
    let sorted_jsonl = out.join("sorted").join("entries.jsonl");
    let sorted_bin = out.join("sorted").join("entries.bin");
    write_jsonl(&sorted_jsonl, &all_refs)?;
    fs::write(&sorted_bin, bincode::serialize(&all_refs)?)?;

    let num_keys = {
        let mut s = std::collections::HashSet::new();
        for r in &all_refs {
            s.insert(r.key.clone());
        }
        s.len()
    };
    let meta = SecondaryManifest {
        dimension: dimension.to_string(),
        fragment_id: fragment_id.to_string(),
        num_buckets: num_buckets.max(1),
        num_keys,
        num_entries: all_refs.len(),
        created_at: chrono::Utc::now().to_rfc3339(),
        files: primary_manifest.files.clone(),
    };
    serde_json::to_writer_pretty(File::create(out.join("manifest.json"))?, &meta)?;
    Ok(out)
}

pub fn load_secondary(
    index_root: &Path,
    fragment_id: &str,
    dimension: &str,
) -> Result<SecondaryIndex> {
    let root = secondary_index_root(index_root, fragment_id, dimension);
    if !root.join("manifest.json").exists() {
        bail!(
            "no secondary index for dim={dimension} fragment={fragment_id} under {}",
            index_root.display()
        );
    }
    let meta: SecondaryManifest =
        serde_json::from_reader(File::open(root.join("manifest.json"))?)?;

    let mut idx = SecondaryIndex {
        dimension: meta.dimension.clone(),
        root: root.clone(),
        ..Default::default()
    };
    for rel in &meta.files {
        idx.files.push(resolve_data_path(index_root, rel));
    }

    // Prefer sorted.bin (complete); also builds hash map.
    let sorted_bin = root.join("sorted").join("entries.bin");
    let refs: Vec<SecondaryRef> = if sorted_bin.exists() {
        bincode::deserialize(&fs::read(&sorted_bin)?)?
    } else {
        read_jsonl(&root.join("sorted").join("entries.jsonl"))?
    };

    let mut sorted_keys = Vec::new();
    for r in refs {
        if sorted_keys.last().map(|k| k != &r.key).unwrap_or(true) {
            if sorted_keys.last().map(|k: &String| k.as_str() < r.key.as_str()).unwrap_or(true)
                || sorted_keys.is_empty()
            {
                // keep unique sorted keys
            }
            if !sorted_keys.iter().any(|k| k == &r.key) {
                sorted_keys.push(r.key.clone());
            }
        }
        idx.by_key.entry(r.key.clone()).or_default().push(r);
    }
    sorted_keys.sort();
    sorted_keys.dedup();
    idx.sorted_keys = sorted_keys;
    Ok(idx)
}

/// Auto-discover fragment ids that have a secondary for `dimension`.
pub fn load_secondary_any(index_root: &Path, dimension: &str) -> Result<SecondaryIndex> {
    let registry: Vec<String> =
        serde_json::from_reader(File::open(index_root.join("registry.json"))?)?;
    for frag in registry.iter().rev() {
        let p = secondary_index_root(index_root, frag, dimension);
        if p.join("manifest.json").exists() {
            return load_secondary(index_root, frag, dimension);
        }
    }
    bail!("no secondary index for dimension '{dimension}' in any fragment");
}

fn scan_dimension(
    path: &Path,
    dimension: &str,
) -> Result<Vec<(String /*sec*/, String /*primary*/, Vec<u64>)>> {
    use arrow::array::{Array, StringArray};

    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

    // sec_key → (primary_key → rows) - for track_uri, primary is user_id.
    let mut map: HashMap<String, HashMap<String, Vec<u64>>> = HashMap::new();
    let mut row_base = 0u64;

    for batch in reader {
        let batch = batch?;
        let n = batch.num_rows();
        let users = batch
            .column_by_name("user_id")
            .context("user_id")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("user_id utf8")?;

        match dimension {
            "track_uri" => {
                if let Some(col) = batch.column_by_name("track_uri") {
                    let tracks = col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .context("track_uri")?;
                    for i in 0..n {
                        let sec = tracks.value(i).to_string();
                        let primary = users.value(i).to_string();
                        map.entry(sec)
                            .or_default()
                            .entry(primary)
                            .or_default()
                            .push(row_base + i as u64);
                    }
                } else if let Some(payload) = batch.column_by_name("payload") {
                    // Blob mode: expand JSON.
                    let payloads = payload
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .context("payload")?;
                    for i in 0..n {
                        let primary = users.value(i).to_string();
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(payloads.value(i))
                        {
                            if let Some(arr) = v.get("listens").and_then(|x| x.as_array()) {
                                for item in arr {
                                    if let Some(t) =
                                        item.get("track_uri").and_then(|x| x.as_str())
                                    {
                                        map.entry(t.to_string())
                                            .or_default()
                                            .entry(primary.clone())
                                            .or_default()
                                            .push(row_base + i as u64);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    bail!("no track_uri or payload column in {}", path.display());
                }
            }
            other => bail!("unsupported secondary dimension: {other}"),
        }
        row_base += n as u64;
    }

    let mut out = Vec::new();
    for (sec, primaries) in map {
        for (primary, rows) in primaries {
            out.push((sec.clone(), primary, rows));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    Ok(out)
}

fn write_jsonl(path: &Path, entries: &[SecondaryRef]) -> Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    for e in entries {
        serde_json::to_writer(&mut w, e)?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

fn read_jsonl(path: &Path) -> Result<Vec<SecondaryRef>> {
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

fn resolve_data_path(index_root: &Path, rel: &str) -> PathBuf {
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

/// Convert secondary refs into synthetic primary-style entries for the querier.
pub fn refs_to_primary_entries(refs: &[SecondaryRef]) -> Vec<RapIndexEntry> {
    refs.iter()
        .map(|r| RapIndexEntry {
            key: r.primary_key.clone(),
            file: r.file,
            row_numbers: r.row_numbers.clone(),
            value_count: r.value_count,
            covering: None,
            page_locs: None,
            frame_locs: None,
            contiguous: None,
            prepared_file: None,
            aligned: None,
        })
        .collect()
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::IndexBuilder;
    use crate::writer::{WriteMode, WriterOptions, write_sample_dataset};

    fn build_with_secondary() -> (tempfile::TempDir, SecondaryIndex, String) {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx = tmp.path().join("rap-index");
        let opts = WriterOptions {
            out_dir: data,
            num_users: 16,
            listens_per_user: 5,
            num_files: 2,
            mode: WriteMode::Sorted,
            rows_per_row_group: 32,
            write_page_index: true,
            seed: 55,
            one_page_per_key: false,
        };
        let paths = write_sample_dataset(&opts).unwrap();
        IndexBuilder::new(&idx, 8)
            .build_fragment(&paths, "frag-s", None)
            .unwrap();
        build_secondary(&idx, "frag-s", "track_uri", 8).unwrap();
        let sec = load_secondary(&idx, "frag-s", "track_uri").unwrap();
        // Pick a key that exists.
        let sample_key = sec.sorted_keys[sec.sorted_keys.len() / 2].clone();
        (tmp, sec, sample_key)
    }

    #[test]
    fn exact_hash_lookup() {
        let (_tmp, sec, key) = build_with_secondary();
        let hits = sec.lookup_exact(&key);
        assert!(!hits.is_empty());
        assert!(hits.iter().all(|r| r.key == key));
        assert!(hits.iter().all(|r| !r.row_numbers.is_empty()));
        assert!(sec.lookup_exact("spotify:track:nope").is_empty());
    }

    #[test]
    fn sorted_range_inclusive_bounds() {
        let (_tmp, sec, _) = build_with_secondary();
        assert!(sec.sorted_keys.len() >= 3);
        let start = sec.sorted_keys[1].clone();
        let end = sec.sorted_keys[3.min(sec.sorted_keys.len() - 1)].clone();
        let range = sec.lookup_range(&start, &end);
        assert!(!range.is_empty());
        for r in &range {
            assert!(r.key.as_str() >= start.as_str());
            assert!(r.key.as_str() <= end.as_str());
        }
        // Inclusive: start and end keys themselves appear if present.
        assert!(range.iter().any(|r| r.key == start));
        assert!(range.iter().any(|r| r.key == end));
    }

    #[test]
    fn empty_range_and_missing_key() {
        let (_tmp, sec, _) = build_with_secondary();
        assert!(sec.lookup_exact("missing-track").is_empty());
        // start > end → empty
        let empty = sec.lookup_range("zzz", "aaa");
        assert!(empty.is_empty());
    }
}
