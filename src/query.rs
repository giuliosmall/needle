//! RAP point-query path.
//!
//! Article:
//! 1. O(1) index lookup for key
//! 2. For each (file, rows): cached metadata → row numbers → byte ranges
//!    (or use page_locs / frame_locs / contiguous stored in the index)
//! 3. Precise ranged reads (local seek or HTTP Range)
//! 4. Decode only those pages; extract matching rows
//! 5. Parallel across columns/files (rayon)
//! 6. Pagination via value_count + offset/limit over index row lists

use crate::index::{RapIndex, RapIndexEntry};
use crate::metadata::{MetaCache, ranged_read};
use crate::prepared::{self, FrameLoc};
use crate::s3::{S3ChunkReader, S3Client, S3RangeReader};
use crate::storage::{HttpRange, LocalFile, RangeReader};
use anyhow::{Context, Result};
use arrow::array::{Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use rayon::prelude::*;
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ListenRow {
    pub user_id: String,
    pub timestamp_ms: i64,
    pub track_uri: String,
    pub duration_ms: i64,
    pub source_file: String,
    pub row_number: u64,
}

#[derive(Debug)]
pub struct QueryTimings {
    pub index_lookup: Duration,
    pub metadata_resolve: Duration,
    pub ranged_read_demo: Duration,
    pub decode_extract: Duration,
    pub total: Duration,
    pub pages_touched: usize,
    pub bytes_ranged: u64,
    pub files_touched: usize,
    /// True when we used index-stored page_locs instead of footer OffsetIndex.
    pub used_index_page_locs: bool,
    /// True when we used prepared ZSTD frames / contiguous span.
    pub used_prepared_layout: bool,
}

#[derive(Debug)]
pub struct QueryResult {
    pub key: String,
    pub rows: Vec<ListenRow>,
    pub timings: QueryTimings,
    pub covering_hits: Vec<String>,
    pub page_descriptions: Vec<String>,
    /// Total values available before pagination (from value_count / row lists).
    pub total_value_count: u64,
    pub offset: usize,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Skip this many values across the flattened row list (pagination).
    pub offset: usize,
    /// Max values to return (None = all after offset).
    pub limit: Option<usize>,
    /// Optional HTTP base URL - when set, demo ranged reads go through HttpRange.
    pub http_base: Option<String>,
}

pub struct RapQuerier {
    pub index: RapIndex,
    pub cache: MetaCache,
    /// Optional MinIO/S3 client for remote file dictionary URIs.
    pub s3: Option<S3Client>,
}

impl RapQuerier {
    pub fn new(index: RapIndex) -> Self {
        Self {
            index,
            cache: MetaCache::new(),
            s3: None,
        }
    }

    pub fn with_s3(mut self, client: S3Client) -> Self {
        self.s3 = Some(client);
        self
    }

    fn s3_or_env(&self) -> S3Client {
        self.s3.clone().unwrap_or_else(S3Client::from_env)
    }

    pub fn query(&self, key: &str) -> Result<QueryResult> {
        self.query_with(key, &QueryOptions::default())
    }

    pub fn query_with(&self, key: &str, opts: &QueryOptions) -> Result<QueryResult> {
        let t0 = Instant::now();

        let t_idx = Instant::now();
        let entries: Vec<RapIndexEntry> = self.index.lookup(key).to_vec();
        let index_lookup = t_idx.elapsed();

        let total_value_count: u64 = entries
            .iter()
            .map(|e| {
                e.value_count
                    .unwrap_or(e.row_numbers.len() as u64)
            })
            .sum();

        // Pagination: slice row_numbers across entries using offset/limit.
        let page_entries = paginate_entries(&entries, opts.offset, opts.limit);

        let mut covering_hits = Vec::new();
        for e in &entries {
            if let Some(c) = &e.covering {
                let path = self.index.file_path(e.file)?.display().to_string();
                covering_hits.push(format!(
                    "file={path} listen_count={} total_duration_ms={}",
                    c.listen_count, c.total_duration_ms
                ));
            }
        }

        let t_meta = Instant::now();
        let mut page_descriptions = Vec::new();
        let mut bytes_ranged = 0u64;
        let mut pages_touched = 0usize;
        let mut used_index_page_locs = false;
        let mut used_prepared_layout = false;
        let columns = ["user_id", "timestamp", "track_uri", "duration_ms", "payload"];

        let mut units: Vec<WorkUnit> = Vec::new();
        for e in &page_entries {
            let path = self.index.file_path(e.file)?.to_path_buf();

            if let Some(span) = &e.contiguous {
                used_prepared_layout = true;
                pages_touched += 1;
                bytes_ranged += span.size;
                page_descriptions.push(format!(
                    "interleaved contiguous offset={} size={} aligned={:?}",
                    span.offset, span.size, e.aligned
                ));
            } else if let Some(frames) = &e.frame_locs {
                used_prepared_layout = true;
                for f in frames {
                    pages_touched += 1;
                    bytes_ranged += f.size as u64;
                    page_descriptions.push(format!(
                        "zstd_frame {} offset={} size={}",
                        f.column, f.offset, f.size
                    ));
                }
            } else if let Some(locs) = &e.page_locs {
                used_index_page_locs = true;
                for loc in locs {
                    pages_touched += 1;
                    bytes_ranged += loc.size as u64;
                    page_descriptions.push(format!(
                        "index_page_loc {} offset={} size={}",
                        loc.column, loc.offset, loc.size
                    ));
                }
            } else {
                // Fallback: footer OffsetIndex via MetaCache.
                let meta = self.cache.get_or_load(&path)?;
                for col in &columns {
                    match meta.describe_pages(col, &e.row_numbers) {
                        Ok(descs) => {
                            for d in descs {
                                page_descriptions.push(format!("{col}: {d}"));
                            }
                        }
                        Err(_) => {}
                    }
                    if let Ok(ranges) = meta.page_ranges_for_rows(col, &e.row_numbers) {
                        pages_touched += ranges.len();
                        for (_, r) in &ranges {
                            bytes_ranged += r.end - r.start;
                        }
                    }
                }
            }

            units.push(WorkUnit {
                entry: e.clone(),
                path,
            });
        }
        let metadata_resolve = t_meta.elapsed();
        let files_touched = units.len();

        // Precise ranged reads (local or HTTP).
        let t_io = Instant::now();
        let s3 = self.s3_or_env();
        let _demo_bytes: u64 = units
            .par_iter()
            .map(|u| demo_ranged_read(u, opts.http_base.as_deref(), &self.cache, &s3))
            .sum();
        let ranged_read_demo = t_io.elapsed();

        // Decode.
        let t_dec = Instant::now();
        let s3_dec = self.s3_or_env();
        let decoded: Result<Vec<Vec<ListenRow>>> = units
            .par_iter()
            .map(|u| decode_unit(u, key, &s3_dec))
            .collect();
        let mut rows: Vec<ListenRow> = decoded?.into_iter().flatten().collect();
        rows.sort_by(|a, b| {
            a.timestamp_ms
                .cmp(&b.timestamp_ms)
                .then(a.row_number.cmp(&b.row_number))
        });
        // Defensive re-apply limit (blob expansion may inflate counts).
        if let Some(lim) = opts.limit {
            if rows.len() > lim {
                rows.truncate(lim);
            }
        }
        let decode_extract = t_dec.elapsed();

        Ok(QueryResult {
            key: key.to_string(),
            rows,
            timings: QueryTimings {
                index_lookup,
                metadata_resolve,
                ranged_read_demo,
                decode_extract,
                total: t0.elapsed(),
                pages_touched,
                bytes_ranged,
                files_touched,
                used_index_page_locs,
                used_prepared_layout,
            },
            covering_hits,
            page_descriptions,
            total_value_count,
            offset: opts.offset,
            limit: opts.limit,
        })
    }
}

struct WorkUnit {
    entry: RapIndexEntry,
    path: PathBuf,
}

fn paginate_entries(
    entries: &[RapIndexEntry],
    offset: usize,
    limit: Option<usize>,
) -> Vec<RapIndexEntry> {
    if offset == 0 && limit.is_none() {
        return entries.to_vec();
    }
    let mut skip = offset;
    let mut remaining = limit.unwrap_or(usize::MAX);
    let mut out = Vec::new();
    for e in entries {
        if remaining == 0 {
            break;
        }
        let n = e.row_numbers.len();
        if skip >= n {
            skip -= n;
            continue;
        }
        let end = (skip + remaining).min(n);
        let slice = e.row_numbers[skip..end].to_vec();
        let took = slice.len();
        let mut ne = e.clone();
        ne.row_numbers = slice;
        ne.value_count = Some(took as u64);
        out.push(ne);
        remaining -= took;
        skip = 0;
    }
    out
}

fn demo_ranged_read(
    u: &WorkUnit,
    http_base: Option<&str>,
    cache: &MetaCache,
    s3: &S3Client,
) -> u64 {
    let uri = u.path.to_string_lossy();
    let reader: Box<dyn RangeReader> = if let Some(base) = http_base {
        let name = u
            .path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("file");
        // Prefer prepared file for frame reads.
        let name = u
            .entry
            .prepared_file
            .as_ref()
            .and_then(|p| Path::new(p).file_name())
            .and_then(|s| s.to_str())
            .unwrap_or(name);
        Box::new(HttpRange::new(format!(
            "{}/{}",
            base.trim_end_matches('/'),
            name
        )))
    } else if S3Client::is_remote_uri(&uri) {
        match S3RangeReader::from_uri(s3.clone(), &uri) {
            Ok(r) => Box::new(r),
            Err(_) => return 0,
        }
    } else if let Some(pf) = &u.entry.prepared_file {
        let p = resolve_prepared(&u.path, pf);
        Box::new(LocalFile::new(p))
    } else {
        Box::new(LocalFile::new(&u.path))
    };

    let mut n = 0u64;
    if let Some(span) = &u.entry.contiguous {
        let r = span.offset..span.offset + span.size;
        if let Ok(buf) = reader.read_range(&r) {
            n += buf.len() as u64;
        }
        return n;
    }
    if let Some(frames) = &u.entry.frame_locs {
        for f in frames {
            let r = f.offset..f.offset + f.size as u64;
            if let Ok(buf) = reader.read_range(&r) {
                n += buf.len() as u64;
            }
        }
        return n;
    }
    if let Some(locs) = &u.entry.page_locs {
        for loc in locs {
            let r = loc.offset..loc.offset + loc.size as u64;
            // Page locs point at the Parquet file, not prepared.
            if let Ok(buf) = reader.read_range(&r) {
                n += buf.len() as u64;
            }
        }
        return n;
    }
    // Footer-based page ranges.
    if let Ok(meta) = cache.get_or_load(&u.path) {
        for col in ["track_uri", "duration_ms", "payload"] {
            if let Ok(ranges) = meta.page_ranges_for_rows(col, &u.entry.row_numbers) {
                for (_, r) in ranges {
                    if let Ok(buf) = ranged_read(&u.path, &r) {
                        n += buf.len() as u64;
                    }
                }
            }
        }
    }
    n
}

fn resolve_prepared(parquet_path: &Path, prepared_rel: &str) -> PathBuf {
    let p = PathBuf::from(prepared_rel);
    if p.is_absolute() && p.exists() {
        return p;
    }
    if let Some(parent) = parquet_path.parent() {
        let name = Path::new(prepared_rel)
            .file_name()
            .map(PathBuf::from)
            .unwrap_or(p.clone());
        let c = parent.join(name);
        if c.exists() {
            return c;
        }
        let c2 = parent.join(prepared_rel);
        if c2.exists() {
            return c2;
        }
    }
    p
}

fn decode_unit(u: &WorkUnit, key: &str, s3: &S3Client) -> Result<Vec<ListenRow>> {
    // Prepared ZSTD / interleaved path.
    if u.entry.frame_locs.is_some() || u.entry.contiguous.is_some() {
        return decode_prepared(u, key);
    }
    // Parquet path (flat / blob / cogrouped / one-page-per-key).
    decode_rows_for_key(&u.path, key, &u.entry.row_numbers, u.entry.file, s3)
}

fn decode_prepared(u: &WorkUnit, key: &str) -> Result<Vec<ListenRow>> {
    let prep_path = u
        .entry
        .prepared_file
        .as_ref()
        .map(|p| resolve_prepared(&u.path, p))
        .unwrap_or_else(|| u.path.clone());
    let data = std::fs::read(&prep_path)
        .with_context(|| format!("read prepared {}", prep_path.display()))?;

    let frames: &[FrameLoc] = u
        .entry
        .frame_locs
        .as_deref()
        .context("prepared entry missing frame_locs")?;

    let ts = frames
        .iter()
        .find(|f| f.column == "timestamp")
        .map(|f| prepared::read_frame_i64(&data, f))
        .transpose()?
        .unwrap_or_default();
    let tracks = frames
        .iter()
        .find(|f| f.column == "track_uri")
        .map(|f| prepared::read_frame_str(&data, f))
        .transpose()?
        .unwrap_or_default();
    let durs = frames
        .iter()
        .find(|f| f.column == "duration_ms")
        .map(|f| prepared::read_frame_i64(&data, f))
        .transpose()?
        .unwrap_or_default();

    let n = ts.len().max(tracks.len()).max(durs.len());
    let file_name = prep_path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(ListenRow {
            user_id: key.to_string(),
            timestamp_ms: ts.get(i).copied().unwrap_or(0),
            track_uri: tracks.get(i).cloned().unwrap_or_default(),
            duration_ms: durs.get(i).copied().unwrap_or(0),
            source_file: file_name.clone(),
            row_number: u.entry.row_numbers.get(i).copied().unwrap_or(i as u64),
        });
    }
    Ok(out)
}

fn decode_rows_for_key(
    path: &PathBuf,
    key: &str,
    row_numbers: &[u64],
    _file_ord: u32,
    s3: &S3Client,
) -> Result<Vec<ListenRow>> {
    let uri = path.to_string_lossy();
    let options = parquet::arrow::arrow_reader::ArrowReaderOptions::new()
        .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Optional);
    let builder = if S3Client::is_remote_uri(&uri) {
        let (bucket, object_key) = S3Client::parse_uri(&uri)?;
        // Fat files: Range-GET footer + OffsetIndex + needed pages via ChunkReader.
        // Tiny files: same path (object is smaller than one window).
        let reader = S3ChunkReader::open(s3.clone(), bucket, object_key)
            .with_context(|| format!("s3 chunk reader {uri}"))?;
        ParquetRecordBatchReaderBuilder::try_new_with_options(reader, options)?
    } else {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        // Keep local File path (existing behaviour).
        return decode_rows_for_key_local(path, key, row_numbers, file, options);
    };
    finish_decode_rows(builder, path, key, row_numbers)
}

fn decode_rows_for_key_local(
    path: &PathBuf,
    key: &str,
    row_numbers: &[u64],
    file: File,
    options: parquet::arrow::arrow_reader::ArrowReaderOptions,
) -> Result<Vec<ListenRow>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    finish_decode_rows(builder, path, key, row_numbers)
}

fn finish_decode_rows<T: parquet::file::reader::ChunkReader + 'static>(
    builder: ParquetRecordBatchReaderBuilder<T>,
    path: &PathBuf,
    key: &str,
    row_numbers: &[u64],
) -> Result<Vec<ListenRow>> {
    let selection =
        row_numbers_to_selection(row_numbers, builder.metadata().file_metadata().num_rows() as u64)?;
    let reader = builder.with_row_selection(selection).build()?;

    let mut out = Vec::new();
    let mut sorted_rows = row_numbers.to_vec();
    sorted_rows.sort_unstable();
    let mut cursor = 0usize;

    for batch in reader {
        let batch = batch?;
        let extracted = extract_listens(&batch, path)?;
        for row in extracted {
            if row.user_id == key {
                let mut row = row;
                if cursor < sorted_rows.len() {
                    row.row_number = sorted_rows[cursor];
                    cursor += 1;
                }
                out.push(row);
            }
        }
    }
    Ok(out)
}

fn row_numbers_to_selection(rows: &[u64], total_rows: u64) -> Result<RowSelection> {
    let mut sorted = rows.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut pos: u64 = 0;
    for &r in &sorted {
        if r >= total_rows {
            continue;
        }
        if r > pos {
            selectors.push(RowSelector::skip((r - pos) as usize));
        }
        selectors.push(RowSelector::select(1));
        pos = r + 1;
    }
    if pos < total_rows {
        selectors.push(RowSelector::skip((total_rows - pos) as usize));
    }
    Ok(RowSelection::from(selectors))
}

fn extract_listens(batch: &RecordBatch, path: &PathBuf) -> Result<Vec<ListenRow>> {
    let n = batch.num_rows();
    let file_name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    // Blob schema: user_id + payload JSON.
    if batch.column_by_name("payload").is_some() && batch.column_by_name("track_uri").is_none() {
        let users = batch
            .column_by_name("user_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("user_id")?;
        let payloads = batch
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("payload")?;
        let mut out = Vec::new();
        for i in 0..n {
            let user = users.value(i).to_string();
            let v: serde_json::Value = serde_json::from_str(payloads.value(i))?;
            if let Some(arr) = v.get("listens").and_then(|x| x.as_array()) {
                for item in arr {
                    out.push(ListenRow {
                        user_id: user.clone(),
                        timestamp_ms: item
                            .get("timestamp_ms")
                            .and_then(|x| x.as_i64())
                            .unwrap_or(0),
                        track_uri: item
                            .get("track_uri")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        duration_ms: item
                            .get("duration_ms")
                            .and_then(|x| x.as_i64())
                            .unwrap_or(0),
                        source_file: file_name.clone(),
                        row_number: 0,
                    });
                }
            }
        }
        return Ok(out);
    }

    if batch.column_by_name("track_uri").is_some() {
        let users = batch
            .column_by_name("user_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("user_id")?;
        let ts = batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context("timestamp")?;
        let tracks = batch
            .column_by_name("track_uri")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("track_uri")?;
        let durs = batch
            .column_by_name("duration_ms")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("duration_ms")?;

        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(ListenRow {
                user_id: users.value(i).to_string(),
                timestamp_ms: ts.value(i),
                track_uri: tracks.value(i).to_string(),
                duration_ms: durs.value(i),
                source_file: file_name.clone(),
                row_number: 0,
            });
        }
        return Ok(out);
    }

    // Cogrouped.
    use arrow::array::{ListArray, StructArray};
    let users = batch
        .column_by_name("user_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .context("user_id")?;
    let listens = batch
        .column_by_name("listens")
        .context("listens")?
        .as_any()
        .downcast_ref::<ListArray>()
        .context("listens list")?;

    let mut out = Vec::new();
    for i in 0..n {
        let user = users.value(i).to_string();
        if listens.is_null(i) {
            continue;
        }
        let arr = listens.value(i);
        let st = arr
            .as_any()
            .downcast_ref::<StructArray>()
            .context("listen struct")?;
        let ts = st
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let tracks = st
            .column_by_name("track_uri")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let durs = st
            .column_by_name("duration_ms")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for j in 0..st.len() {
            out.push(ListenRow {
                user_id: user.clone(),
                timestamp_ms: ts.value(j),
                track_uri: tracks.value(j).to_string(),
                duration_ms: durs.value(j),
                source_file: file_name.clone(),
                row_number: 0,
            });
        }
    }
    Ok(out)
}

/// Naive full scan of all Parquet files - baseline for `rap bench`.
pub fn naive_scan(files: &[PathBuf], key: &str) -> Result<(Vec<ListenRow>, Duration)> {
    let t0 = Instant::now();
    let mut rows = Vec::new();
    for path in files {
        let file = File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        for batch in reader {
            let batch = batch?;
            for row in extract_listens(&batch, path)? {
                if row.user_id == key {
                    rows.push(row);
                }
            }
        }
    }
    rows.sort_by_key(|r| r.timestamp_ms);
    Ok((rows, t0.elapsed()))
}

/// Collect byte ranges that a RAP query would fetch (for HTTP proof demos).
pub fn collect_demo_ranges(
    querier: &RapQuerier,
    key: &str,
) -> Result<Vec<(PathBuf, Range<u64>)>> {
    let mut out = Vec::new();
    for e in querier.index.lookup(key) {
        let path = querier.index.file_path(e.file)?.to_path_buf();
        if let Some(span) = &e.contiguous {
            let prep = e
                .prepared_file
                .as_ref()
                .map(|p| resolve_prepared(&path, p))
                .unwrap_or(path.clone());
            out.push((prep, span.offset..span.offset + span.size));
        } else if let Some(frames) = &e.frame_locs {
            let prep = e
                .prepared_file
                .as_ref()
                .map(|p| resolve_prepared(&path, p))
                .unwrap_or(path.clone());
            for f in frames {
                out.push((prep.clone(), f.offset..f.offset + f.size as u64));
            }
        } else if let Some(locs) = &e.page_locs {
            for loc in locs {
                out.push((path.clone(), loc.offset..loc.offset + loc.size as u64));
            }
        } else {
            let meta = querier.cache.get_or_load(&path)?;
            if let Ok(ranges) = meta.page_ranges_for_rows("track_uri", &e.row_numbers) {
                for (_, r) in ranges {
                    out.push((path.clone(), r));
                }
            }
        }
    }
    Ok(out)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{IndexBuilder, load_index};
    use crate::writer::{WriteMode, WriterOptions, write_sample_dataset};

    fn setup(mode: WriteMode, covering: bool) -> (tempfile::TempDir, RapQuerier, Vec<PathBuf>) {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx = tmp.path().join("rap-index");
        let opts = WriterOptions {
            out_dir: data,
            num_users: 20,
            listens_per_user: 8,
            num_files: 2,
            mode,
            rows_per_row_group: 32,
            write_page_index: true,
            seed: 123,
            one_page_per_key: false,
        };
        let paths = write_sample_dataset(&opts).unwrap();
        IndexBuilder::new(&idx, 8)
            .with_covering(covering)
            .build_fragment(&paths, "frag-q", None)
            .unwrap();
        let querier = RapQuerier::new(load_index(&idx).unwrap());
        (tmp, querier, paths)
    }

    fn row_sig(rows: &[ListenRow]) -> Vec<(i64, String, i64)> {
        let mut v: Vec<_> = rows
            .iter()
            .map(|r| (r.timestamp_ms, r.track_uri.clone(), r.duration_ms))
            .collect();
        v.sort();
        v
    }

    #[test]
    fn rap_matches_naive_for_edge_keys() {
        let (_tmp, querier, paths) = setup(WriteMode::Sorted, true);
        for key in ["user_0000", "user_0010", "user_0019"] {
            let rap = querier.query(key).unwrap();
            let (naive, _) = naive_scan(&paths, key).unwrap();
            assert_eq!(
                row_sig(&rap.rows),
                row_sig(&naive),
                "mismatch for {key}"
            );
            assert_eq!(rap.rows.len(), 8);
        }
    }

    #[test]
    fn covering_listen_count_without_all_rows() {
        let (_tmp, querier, _paths) = setup(WriteMode::Sorted, true);
        let res = querier.query("user_0005").unwrap();
        assert!(!res.covering_hits.is_empty());
        assert!(res.covering_hits[0].contains("listen_count=8"));
        // Covering is available even before looking at decoded rows.
        let entries = querier.index.lookup("user_0005");
        let cov = entries[0].covering.as_ref().unwrap();
        assert_eq!(cov.listen_count, 8);
        assert!(cov.total_duration_ms > 0);
    }

    #[test]
    fn pagination_offset_limit_bounds() {
        let (_tmp, querier, _paths) = setup(WriteMode::Sorted, false);
        let full = querier.query("user_0002").unwrap();
        assert_eq!(full.rows.len(), 8);

        let page = querier
            .query_with(
                "user_0002",
                &QueryOptions {
                    offset: 2,
                    limit: Some(3),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(page.rows.len(), 3);
        assert_eq!(page.offset, 2);
        assert_eq!(page.limit, Some(3));
        assert_eq!(page.total_value_count, 8);
        // Pagination slices index row_numbers; decode then sorts by timestamp -
        // so compare membership against the full result, not a positional slice.
        let full_sig = row_sig(&full.rows);
        for r in &page.rows {
            let sig = (r.timestamp_ms, r.track_uri.clone(), r.duration_ms);
            assert!(full_sig.contains(&sig), "paginated row missing from full");
        }
        // Distinct rows within the page.
        assert_eq!(row_sig(&page.rows).len(), 3);

        // Offset past end → empty.
        let empty = querier
            .query_with(
                "user_0002",
                &QueryOptions {
                    offset: 100,
                    limit: Some(5),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(empty.rows.is_empty());
    }

    #[test]
    fn page_locs_path_used_for_sorted() {
        let (_tmp, querier, _) = setup(WriteMode::Sorted, false);
        let res = querier.query("user_0001").unwrap();
        assert!(
            res.timings.used_index_page_locs || res.timings.used_prepared_layout,
            "expected index page_locs path"
        );
    }

    #[test]
    fn frame_locs_path_for_zstd_frames() {
        let (_tmp, querier, paths) = setup(WriteMode::ZstdFrames, false);
        let res = querier.query("user_0000").unwrap();
        assert!(res.timings.used_prepared_layout);
        let (naive, _) = naive_scan(&paths, "user_0000").unwrap();
        assert_eq!(row_sig(&res.rows), row_sig(&naive));
    }

    #[test]
    fn blob_query_expands_listens() {
        let (_tmp, querier, paths) = setup(WriteMode::Blob, true);
        let res = querier.query("user_0007").unwrap();
        assert_eq!(res.rows.len(), 8);
        let (naive, _) = naive_scan(&paths, "user_0007").unwrap();
        assert_eq!(row_sig(&res.rows), row_sig(&naive));
        assert!(!res.covering_hits.is_empty());
    }

    #[test]
    fn cogrouped_covering_matches_recomputed_nested_sums() {
        let (_tmp, querier, paths) = setup(WriteMode::Cogrouped, true);
        for key in ["user_0000", "user_0010", "user_0019"] {
            let res = querier.query(key).unwrap();
            let (naive, _) = naive_scan(&paths, key).unwrap();
            assert_eq!(row_sig(&res.rows), row_sig(&naive));
            assert_eq!(res.rows.len(), 8);
            let entries = querier.index.lookup(key);
            let parquet_rows: usize = entries.iter().map(|e| e.row_numbers.len()).sum();
            assert_eq!(parquet_rows, 1);
            let cov = entries[0].covering.as_ref().expect("covering");
            let sum_dur: u64 = res.rows.iter().map(|r| r.duration_ms.max(0) as u64).sum();
            assert_eq!(cov.listen_count, 8);
            assert_eq!(cov.total_duration_ms, sum_dur);
            assert!(res.covering_hits.iter().any(|h| h.contains("listen_count=8")));
        }
    }
}
