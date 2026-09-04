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
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Columns ranged-read when `QueryOptions::columns` is None.
pub const DEFAULT_QUERY_COLUMNS: [&str; 5] = [
    "user_id",
    "timestamp",
    "track_uri",
    "duration_ms",
    "payload",
];

/// Column names that participate in page/frame IO for this query.
pub fn projected_columns(opts: &QueryOptions) -> Vec<String> {
    match &opts.columns {
        Some(cols) if !cols.is_empty() => cols.clone(),
        _ => DEFAULT_QUERY_COLUMNS
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
    }
}

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
    /// Decoded projection as Arrow. Always present (0 rows when covering_only / no hits).
    pub batch: RecordBatch,
    pub timings: QueryTimings,
    pub covering_hits: Vec<String>,
    pub page_descriptions: Vec<String>,
    /// Total values available before pagination (from value_count / row lists).
    pub total_value_count: u64,
    pub offset: usize,
    pub limit: Option<usize>,
    /// Entries dropped by covering/time predicates before IO.
    pub skipped_by_predicate: usize,
}

impl QueryResult {
    /// Decoded projection as Arrow. Empty batch (0 rows) when covering_only or no hits.
    pub fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// One JSON object per row from the batch (not ListenRow-only).
    pub fn json_rows(&self) -> Vec<serde_json::Value> {
        batch_to_json_rows(&self.batch)
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Skip this many values across the flattened row list (pagination).
    pub offset: usize,
    /// Max values to return (None = all after offset).
    pub limit: Option<usize>,
    /// Optional HTTP base URL - when set, demo ranged reads go through HttpRange.
    pub http_base: Option<String>,
    /// If Some, only these Parquet/index columns are ranged-read.
    pub columns: Option<Vec<String>>,
    /// Inclusive lower bound on row timestamp_ms (and covering min/max overlap).
    pub since_ms: Option<i64>,
    /// Inclusive upper bound on row timestamp_ms.
    pub until_ms: Option<i64>,
    /// If true: do not Range-GET or decode. Return covering + empty rows.
    pub covering_only: bool,
    /// Drop index entries whose covering.listen_count is Some and < this.
    pub min_listen_count: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ExplainResult {
    pub key: String,
    pub bucket: Option<u32>,
    pub num_entries: usize,
    pub num_entries_after_predicates: usize,
    pub files: Vec<String>,
    pub covering: Vec<String>,
    pub page_descriptions: Vec<String>,
    pub estimated_bytes: u64,
    pub estimated_range_gets: usize,
    pub covering_only: bool,
    pub columns: Vec<String>,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    pub skipped_by_predicate: usize,
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

        let (kept, skipped_by_predicate) = apply_predicates(&entries, opts);
        let total_value_count: u64 = kept
            .iter()
            .map(|e| e.value_count.unwrap_or(e.row_numbers.len() as u64))
            .sum();

        let covering_hits = covering_lines(&self.index, &kept)?;

        if opts.covering_only {
            return Ok(QueryResult {
                key: key.to_string(),
                rows: Vec::new(),
                batch: empty_record_batch(),
                timings: QueryTimings {
                    index_lookup,
                    metadata_resolve: Duration::ZERO,
                    ranged_read_demo: Duration::ZERO,
                    decode_extract: Duration::ZERO,
                    total: t0.elapsed(),
                    pages_touched: 0,
                    bytes_ranged: 0,
                    files_touched: 0,
                    used_index_page_locs: false,
                    used_prepared_layout: false,
                },
                covering_hits,
                page_descriptions: Vec::new(),
                total_value_count,
                offset: opts.offset,
                limit: opts.limit,
                skipped_by_predicate,
            });
        }

        // Pagination: slice row_numbers across remaining entries using offset/limit.
        let mut page_entries = paginate_entries(&kept, opts.offset, opts.limit);
        let io_cols = io_column_filter(opts);
        for e in &mut page_entries {
            project_entry_locs(e, io_cols.as_deref());
        }

        let t_meta = Instant::now();
        let mut page_descriptions = Vec::new();
        let mut bytes_ranged = 0u64;
        let mut pages_touched = 0usize;
        let mut used_index_page_locs = false;
        let mut used_prepared_layout = false;
        let footer_cols: Vec<String> = match &io_cols {
            Some(c) => c.clone(),
            None => projected_columns(opts),
        };

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
                for col in &footer_cols {
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
                io_cols: io_cols.clone(),
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
        let decoded: Result<Vec<UnitDecode>> = units
            .par_iter()
            .map(|u| decode_unit(u, key, &s3_dec))
            .collect();
        let decoded = decoded?;
        let mut rows: Vec<ListenRow> = Vec::new();
        let mut parquet_batches: Vec<RecordBatch> = Vec::new();
        for part in decoded {
            rows.extend(part.rows);
            parquet_batches.extend(part.parquet_batches);
        }
        rows.sort_by(|a, b| {
            a.timestamp_ms
                .cmp(&b.timestamp_ms)
                .then(a.row_number.cmp(&b.row_number))
        });
        if opts.since_ms.is_some() || opts.until_ms.is_some() {
            rows.retain(|r| row_in_time_window(r, opts));
        }
        // Defensive re-apply limit (blob expansion / time filter may change counts).
        if let Some(lim) = opts.limit {
            if rows.len() > lim {
                rows.truncate(lim);
            }
        }
        let batch = if parquet_batches.is_empty() {
            if rows.is_empty() {
                empty_record_batch()
            } else {
                listen_rows_to_batch(&rows)
            }
        } else {
            assemble_parquet_batch(&parquet_batches, key, opts)
        };
        let decode_extract = t_dec.elapsed();

        Ok(QueryResult {
            key: key.to_string(),
            rows,
            batch,
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
            skipped_by_predicate,
        })
    }

    pub fn explain(&self, key: &str, opts: &QueryOptions) -> Result<ExplainResult> {
        let entries = self.index.lookup(key);
        let num_entries = entries.len();
        let (kept, skipped_by_predicate) = apply_predicates(entries, opts);
        let io_cols = io_column_filter(opts);

        let mut files = Vec::new();
        let mut seen = HashSet::new();
        let mut page_descriptions = Vec::new();
        let mut estimated_bytes = 0u64;
        let mut estimated_range_gets = 0usize;
        for e in &kept {
            if let Ok(p) = self.index.file_path(e.file) {
                let s = p.display().to_string();
                if seen.insert(s.clone()) {
                    files.push(s);
                }
            }
            let (b, g, descs) = estimate_entry_io(e, io_cols.as_deref());
            estimated_bytes += b;
            estimated_range_gets += g;
            page_descriptions.extend(descs);
        }

        let bucket = self
            .index
            .fragments
            .first()
            .map(|f| crate::index::key_bucket(key, f.num_buckets));

        Ok(ExplainResult {
            key: key.to_string(),
            bucket,
            num_entries,
            num_entries_after_predicates: kept.len(),
            files,
            covering: covering_lines(&self.index, &kept)?,
            page_descriptions,
            estimated_bytes,
            estimated_range_gets,
            covering_only: opts.covering_only,
            columns: projected_columns(opts),
            since_ms: opts.since_ms,
            until_ms: opts.until_ms,
            skipped_by_predicate,
        })
    }
}

struct WorkUnit {
    entry: RapIndexEntry,
    path: PathBuf,
    io_cols: Option<Vec<String>>,
}

struct UnitDecode {
    rows: Vec<ListenRow>,
    /// Projected parquet batches (row-selected). Empty for prepared / blob expansion.
    parquet_batches: Vec<RecordBatch>,
}

fn columns_eq(a: &str, b: &str) -> bool {
    a == b
        || matches!(
            (a, b),
            ("timestamp", "timestamp_ms") | ("timestamp_ms", "timestamp")
        )
}

fn column_selected(name: &str, filter: Option<&[String]>) -> bool {
    if name == "*" {
        return true;
    }
    let Some(cols) = filter else {
        return true;
    };
    cols.iter().any(|c| columns_eq(c, name))
}

/// None = every column (legacy behaviour). Some = projected names, plus
/// `timestamp` when a time window must be applied after decode.
fn io_column_filter(opts: &QueryOptions) -> Option<Vec<String>> {
    let mut cols = opts.columns.clone()?;
    if opts.since_ms.is_some() || opts.until_ms.is_some() {
        if !cols
            .iter()
            .any(|c| columns_eq(c, "timestamp") || columns_eq(c, "timestamp_ms"))
        {
            cols.push("timestamp".to_string());
        }
    }
    Some(cols)
}

fn project_entry_locs(e: &mut RapIndexEntry, filter: Option<&[String]>) {
    if filter.is_none() {
        return;
    }
    if let Some(locs) = e.page_locs.as_mut() {
        locs.retain(|l| column_selected(&l.column, filter));
    }
    if let Some(frames) = e.frame_locs.as_mut() {
        frames.retain(|f| column_selected(&f.column, filter));
    }
}

fn entry_passes_predicates(e: &RapIndexEntry, opts: &QueryOptions) -> bool {
    if let Some(min_lc) = opts.min_listen_count {
        if let Some(c) = &e.covering {
            if c.listen_count < min_lc {
                return false;
            }
        }
    }
    if opts.since_ms.is_none() && opts.until_ms.is_none() {
        return true;
    }
    let Some(c) = e.covering.as_ref() else {
        return true;
    };
    let (Some(min_ts), Some(max_ts)) = (c.min_ts, c.max_ts) else {
        return true;
    };
    if let Some(since) = opts.since_ms {
        if max_ts < since {
            return false;
        }
    }
    if let Some(until) = opts.until_ms {
        if min_ts > until {
            return false;
        }
    }
    true
}

fn apply_predicates(entries: &[RapIndexEntry], opts: &QueryOptions) -> (Vec<RapIndexEntry>, usize) {
    let mut kept = Vec::new();
    let mut skipped = 0usize;
    for e in entries {
        if entry_passes_predicates(e, opts) {
            kept.push(e.clone());
        } else {
            skipped += 1;
        }
    }
    (kept, skipped)
}

fn covering_lines(index: &RapIndex, entries: &[RapIndexEntry]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for e in entries {
        if let Some(c) = &e.covering {
            let path = index.file_path(e.file)?.display().to_string();
            out.push(format!(
                "file={path} listen_count={} total_duration_ms={}",
                c.listen_count, c.total_duration_ms
            ));
        }
    }
    Ok(out)
}

fn row_in_time_window(r: &ListenRow, opts: &QueryOptions) -> bool {
    if let Some(since) = opts.since_ms {
        if r.timestamp_ms < since {
            return false;
        }
    }
    if let Some(until) = opts.until_ms {
        if r.timestamp_ms > until {
            return false;
        }
    }
    true
}

fn estimate_entry_io(e: &RapIndexEntry, filter: Option<&[String]>) -> (u64, usize, Vec<String>) {
    let mut bytes = 0u64;
    let mut gets = 0usize;
    let mut descs = Vec::new();
    if let Some(span) = &e.contiguous {
        bytes += span.size;
        gets += 1;
        descs.push(format!(
            "interleaved contiguous offset={} size={} aligned={:?}",
            span.offset, span.size, e.aligned
        ));
    } else if let Some(frames) = &e.frame_locs {
        for f in frames {
            if column_selected(&f.column, filter) {
                bytes += f.size as u64;
                gets += 1;
                descs.push(format!(
                    "zstd_frame {} offset={} size={}",
                    f.column, f.offset, f.size
                ));
            }
        }
    } else if let Some(locs) = &e.page_locs {
        for loc in locs {
            if column_selected(&loc.column, filter) {
                bytes += loc.size as u64;
                gets += 1;
                descs.push(format!(
                    "index_page_loc {} offset={} size={}",
                    loc.column, loc.offset, loc.size
                ));
            }
        }
    } else {
        descs.push("footer OffsetIndex (bytes unknown until fetch)".to_string());
    }
    (bytes, gets, descs)
}

fn decode_projection(io_cols: Option<&[String]>) -> Option<Vec<String>> {
    let mut cols = io_cols?.to_vec();
    if !cols.iter().any(|c| c == "user_id") {
        cols.push("user_id".to_string());
    }
    Some(cols)
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
        let footer_cols: Vec<&str> = match &u.io_cols {
            Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
            None => vec!["track_uri", "duration_ms", "payload"],
        };
        for col in footer_cols {
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

fn decode_unit(u: &WorkUnit, key: &str, s3: &S3Client) -> Result<UnitDecode> {
    // Prepared ZSTD / interleaved path.
    if u.entry.frame_locs.is_some() || u.entry.contiguous.is_some() {
        return decode_prepared(u, key);
    }
    // Parquet path (flat / blob / cogrouped / one-page-per-key).
    decode_rows_for_key(
        &u.path,
        key,
        &u.entry.row_numbers,
        u.entry.file,
        s3,
        u.io_cols.as_deref(),
    )
}

fn decode_prepared(u: &WorkUnit, key: &str) -> Result<UnitDecode> {
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
    Ok(UnitDecode {
        rows: out,
        parquet_batches: Vec::new(),
    })
}

fn decode_rows_for_key(
    path: &PathBuf,
    key: &str,
    row_numbers: &[u64],
    _file_ord: u32,
    s3: &S3Client,
    io_cols: Option<&[String]>,
) -> Result<UnitDecode> {
    let uri = path.to_string_lossy();
    let options = parquet::arrow::arrow_reader::ArrowReaderOptions::new()
        .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Optional);
    let proj = decode_projection(io_cols);
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
        return decode_rows_for_key_local(path, key, row_numbers, file, options, proj.as_deref());
    };
    finish_decode_rows(builder, path, key, row_numbers, proj.as_deref())
}

fn decode_rows_for_key_local(
    path: &PathBuf,
    key: &str,
    row_numbers: &[u64],
    file: File,
    options: parquet::arrow::arrow_reader::ArrowReaderOptions,
    proj: Option<&[String]>,
) -> Result<UnitDecode> {
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    finish_decode_rows(builder, path, key, row_numbers, proj)
}

fn apply_projection<T: parquet::file::reader::ChunkReader + 'static>(
    builder: ParquetRecordBatchReaderBuilder<T>,
    proj: Option<&[String]>,
) -> ParquetRecordBatchReaderBuilder<T> {
    let Some(cols) = proj else {
        return builder;
    };
    if cols.is_empty() {
        return builder;
    }
    let names: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
    let mask = ProjectionMask::columns(builder.parquet_schema(), names);
    builder.with_projection(mask)
}

fn finish_decode_rows<T: parquet::file::reader::ChunkReader + 'static>(
    builder: ParquetRecordBatchReaderBuilder<T>,
    path: &PathBuf,
    key: &str,
    row_numbers: &[u64],
    proj: Option<&[String]>,
) -> Result<UnitDecode> {
    let selection =
        row_numbers_to_selection(row_numbers, builder.metadata().file_metadata().num_rows() as u64)?;
    let builder = apply_projection(builder, proj);
    let reader = builder.with_row_selection(selection).build()?;

    let mut out = Vec::new();
    let mut parquet_batches = Vec::new();
    let mut sorted_rows = row_numbers.to_vec();
    sorted_rows.sort_unstable();
    let mut cursor = 0usize;

    for batch in reader {
        let batch = batch?;
        let is_blob = is_blob_schema(&batch);
        let extracted = extract_listens(&batch, path, key)?;
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
        // Blob / payload expansion: ListenRows are the source of truth; batch is
        // rebuilt from those rows after sort/time/limit.
        if !is_blob {
            parquet_batches.push(batch);
        }
    }
    Ok(UnitDecode {
        rows: out,
        parquet_batches,
    })
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

fn utf8_at(col: Option<&dyn Array>, i: usize, fallback: &str) -> String {
    let Some(col) = col else {
        return fallback.to_string();
    };
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        if a.is_null(i) {
            return String::new();
        }
        return a.value(i).to_string();
    }
    fallback.to_string()
}

fn i64_at(col: Option<&dyn Array>, i: usize) -> i64 {
    let Some(col) = col else {
        return 0;
    };
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return if a.is_null(i) { 0 } else { a.value(i) };
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return if a.is_null(i) { 0 } else { a.value(i) };
    }
    0
}

fn extract_listens(batch: &RecordBatch, path: &PathBuf, fallback_user: &str) -> Result<Vec<ListenRow>> {
    let n = batch.num_rows();
    let file_name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let user_col = batch.column_by_name("user_id").map(|c| c as &dyn Array);

    // Blob schema: user_id + payload JSON.
    if batch.column_by_name("payload").is_some() && batch.column_by_name("track_uri").is_none() {
        let payloads = batch
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .context("payload")?;
        let mut out = Vec::new();
        for i in 0..n {
            let user = utf8_at(user_col, i, fallback_user);
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

    let has_flat = batch.column_by_name("track_uri").is_some()
        || batch.column_by_name("timestamp").is_some()
        || batch.column_by_name("duration_ms").is_some();
    if has_flat {
        let ts_col = batch.column_by_name("timestamp").map(|c| c as &dyn Array);
        let track_col = batch.column_by_name("track_uri").map(|c| c as &dyn Array);
        let dur_col = batch.column_by_name("duration_ms").map(|c| c as &dyn Array);
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(ListenRow {
                user_id: utf8_at(user_col, i, fallback_user),
                timestamp_ms: i64_at(ts_col, i),
                track_uri: utf8_at(track_col, i, ""),
                duration_ms: i64_at(dur_col, i),
                source_file: file_name.clone(),
                row_number: 0,
            });
        }
        return Ok(out);
    }

    // Cogrouped.
    use arrow::array::{ListArray, StructArray};
    if let Some(listens) = batch
        .column_by_name("listens")
        .and_then(|c| c.as_any().downcast_ref::<ListArray>())
    {
        let mut out = Vec::new();
        for i in 0..n {
            let user = utf8_at(user_col, i, fallback_user);
            if listens.is_null(i) {
                continue;
            }
            let arr = listens.value(i);
            let st = arr
                .as_any()
                .downcast_ref::<StructArray>()
                .context("listen struct")?;
            let ts_col = st.column_by_name("timestamp").map(|c| c as &dyn Array);
            let track_col = st.column_by_name("track_uri").map(|c| c as &dyn Array);
            let dur_col = st.column_by_name("duration_ms").map(|c| c as &dyn Array);
            for j in 0..st.len() {
                out.push(ListenRow {
                    user_id: user.clone(),
                    timestamp_ms: i64_at(ts_col, j),
                    track_uri: utf8_at(track_col, j, ""),
                    duration_ms: i64_at(dur_col, j),
                    source_file: file_name.clone(),
                    row_number: 0,
                });
            }
        }
        return Ok(out);
    }

    // Projected leftover: user_id only (or empty schema).
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(ListenRow {
            user_id: utf8_at(user_col, i, fallback_user),
            timestamp_ms: 0,
            track_uri: String::new(),
            duration_ms: 0,
            source_file: file_name.clone(),
            row_number: 0,
        });
    }
    Ok(out)
}

fn is_blob_schema(batch: &RecordBatch) -> bool {
    batch.column_by_name("payload").is_some() && batch.column_by_name("track_uri").is_none()
}

fn empty_record_batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(Schema::empty()))
}

fn listen_row_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("track_uri", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Int64, false),
    ]))
}

fn listen_rows_to_batch(rows: &[ListenRow]) -> RecordBatch {
    let schema = listen_row_schema();
    let user_id: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.user_id.as_str()),
    ));
    let timestamp_ms: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.timestamp_ms).collect::<Vec<_>>(),
    ));
    let track_uri: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.track_uri.as_str()),
    ));
    let duration_ms: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.duration_ms).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(
        schema,
        vec![user_id, timestamp_ms, track_uri, duration_ms],
    )
    .unwrap_or_else(|_| empty_record_batch())
}

fn assemble_parquet_batch(batches: &[RecordBatch], key: &str, opts: &QueryOptions) -> RecordBatch {
    let schema = batches[0].schema();
    let mut batch = match arrow::compute::concat_batches(&schema, batches) {
        Ok(b) => b,
        Err(_) => batches[0].clone(),
    };
    batch = filter_batch_by_key(&batch, key);
    batch = filter_batch_by_time(&batch, opts);
    if let Some(lim) = opts.limit {
        if batch.num_rows() > lim {
            batch = batch.slice(0, lim);
        }
    }
    batch
}

fn is_stringy(dt: &DataType) -> bool {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, value) => is_stringy(value.as_ref()),
        _ => false,
    }
}

fn key_column_index(batch: &RecordBatch) -> Option<usize> {
    if let Some((i, _)) = batch.schema().column_with_name("user_id") {
        return Some(i);
    }
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| is_stringy(f.data_type()))
}

fn string_value_at(col: &dyn Array, i: usize) -> Option<String> {
    use arrow::array::{DictionaryArray, LargeStringArray, StringViewArray};
    use arrow::datatypes::{
        ArrowNativeType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
        UInt64Type, UInt8Type,
    };
    if col.is_null(i) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Some(a.value(i).to_string());
    }
    if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
        return Some(a.value(i).to_string());
    }
    if let Some(a) = col.as_any().downcast_ref::<StringViewArray>() {
        return Some(a.value(i).to_string());
    }
    macro_rules! dict {
        ($t:ty) => {
            if let Some(d) = col.as_any().downcast_ref::<DictionaryArray<$t>>() {
                let idx = d.keys().value(i).to_usize()?;
                return string_value_at(d.values().as_ref(), idx);
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
    arrow::util::display::array_value_to_string(col, i).ok()
}

fn filter_batch_by_key(batch: &RecordBatch, key: &str) -> RecordBatch {
    if batch.num_rows() == 0 {
        return batch.clone();
    }
    let Some(idx) = key_column_index(batch) else {
        return batch.clone();
    };
    let col = batch.column(idx);
    let mask: Vec<bool> = (0..batch.num_rows())
        .map(|i| string_value_at(col.as_ref(), i).map(|s| s == key).unwrap_or(false))
        .collect();
    let pred = BooleanArray::from(mask);
    arrow::compute::filter_record_batch(batch, &pred).unwrap_or_else(|_| batch.clone())
}

fn timestamp_col(batch: &RecordBatch) -> Option<&dyn Array> {
    batch
        .column_by_name("timestamp")
        .or_else(|| batch.column_by_name("timestamp_ms"))
        .map(|c| c.as_ref())
}

fn ts_ms_at(col: &dyn Array, i: usize) -> Option<i64> {
    use arrow::array::{
        TimestampMicrosecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
    };
    use arrow::datatypes::TimeUnit;
    if col.is_null(i) {
        return Some(0);
    }
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(i)),
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(i) as i64),
        DataType::Timestamp(unit, _) => {
            let raw = if let Some(a) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                Some(a.value(i))
            } else if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                Some(a.value(i))
            } else if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
                Some(a.value(i))
            } else if let Some(a) = col.as_any().downcast_ref::<TimestampSecondArray>() {
                Some(a.value(i))
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                Some(a.value(i))
            } else {
                None
            }?;
            Some(match unit {
                TimeUnit::Second => raw.saturating_mul(1_000),
                TimeUnit::Millisecond => raw,
                TimeUnit::Microsecond => raw / 1_000,
                TimeUnit::Nanosecond => raw / 1_000_000,
            })
        }
        _ => None,
    }
}

fn filter_batch_by_time(batch: &RecordBatch, opts: &QueryOptions) -> RecordBatch {
    if batch.num_rows() == 0 || (opts.since_ms.is_none() && opts.until_ms.is_none()) {
        return batch.clone();
    }
    let Some(col) = timestamp_col(batch) else {
        return batch.clone();
    };
    let mask: Vec<bool> = (0..batch.num_rows())
        .map(|i| {
            let Some(ts) = ts_ms_at(col, i) else {
                return true;
            };
            if let Some(since) = opts.since_ms {
                if ts < since {
                    return false;
                }
            }
            if let Some(until) = opts.until_ms {
                if ts > until {
                    return false;
                }
            }
            true
        })
        .collect();
    let pred = BooleanArray::from(mask);
    arrow::compute::filter_record_batch(batch, &pred).unwrap_or_else(|_| batch.clone())
}

/// Convert each batch row to a JSON object. Unknown / nested types become
/// structured JSON when possible, otherwise `null` (never panics).
pub fn batch_to_json_rows(batch: &RecordBatch) -> Vec<serde_json::Value> {
    let n = batch.num_rows();
    let names: Vec<String> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let mut map = serde_json::Map::new();
        for (j, name) in names.iter().enumerate() {
            map.insert(name.clone(), array_value_to_json(batch.column(j).as_ref(), i));
        }
        out.push(serde_json::Value::Object(map));
    }
    out
}

fn array_value_to_json(col: &dyn Array, i: usize) -> serde_json::Value {
    use arrow::array::{
        BinaryArray, BooleanArray as ArrowBooleanArray, DictionaryArray, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int32Array, LargeBinaryArray, LargeListArray, LargeStringArray,
        ListArray, StringViewArray, StructArray, TimestampMicrosecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{
        ArrowNativeType, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    };
    if col.is_null(i) {
        return serde_json::Value::Null;
    }
    match col.data_type() {
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| serde_json::Value::String(a.value(i).to_string()))
            .unwrap_or(serde_json::Value::Null),
        DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| serde_json::Value::String(a.value(i).to_string()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Utf8View => col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|a| serde_json::Value::String(a.value(i).to_string()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| serde_json::json!(a.value(i)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| serde_json::json!(a.value(i)))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| serde_json::json!(a.value(i)))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| serde_json::json!(a.value(i)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(i)))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        DataType::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(i) as f64))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<ArrowBooleanArray>()
            .map(|a| serde_json::Value::Bool(a.value(i)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Timestamp(unit, _) => {
            let raw = if let Some(a) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                Some(a.value(i))
            } else if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                Some(a.value(i))
            } else if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
                Some(a.value(i))
            } else if let Some(a) = col.as_any().downcast_ref::<TimestampSecondArray>() {
                Some(a.value(i))
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                Some(a.value(i))
            } else {
                None
            };
            match raw {
                Some(v) => {
                    let ms = match unit {
                        TimeUnit::Second => v.saturating_mul(1_000),
                        TimeUnit::Millisecond => v,
                        TimeUnit::Microsecond => v / 1_000,
                        TimeUnit::Nanosecond => v / 1_000_000,
                    };
                    serde_json::json!(ms)
                }
                None => serde_json::Value::Null,
            }
        }
        DataType::Binary => col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|a| serde_json::Value::String(hex::encode(a.value(i))))
            .unwrap_or(serde_json::Value::Null),
        DataType::LargeBinary => col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|a| serde_json::Value::String(hex::encode(a.value(i))))
            .unwrap_or(serde_json::Value::Null),
        DataType::FixedSizeBinary(_) => col
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .map(|a| serde_json::Value::String(hex::encode(a.value(i))))
            .unwrap_or(serde_json::Value::Null),
        DataType::List(_) => col
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|a| list_values_to_json(a.value(i).as_ref()))
            .unwrap_or(serde_json::Value::Null),
        DataType::LargeList(_) => col
            .as_any()
            .downcast_ref::<LargeListArray>()
            .map(|a| list_values_to_json(a.value(i).as_ref()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Struct(_) => col
            .as_any()
            .downcast_ref::<StructArray>()
            .map(|st| struct_row_to_json(st, i))
            .unwrap_or(serde_json::Value::Null),
        DataType::Dictionary(_, _) => {
            macro_rules! dict {
                ($t:ty) => {
                    if let Some(d) = col.as_any().downcast_ref::<DictionaryArray<$t>>() {
                        let Some(idx) = d.keys().value(i).to_usize() else {
                            return serde_json::Value::Null;
                        };
                        return array_value_to_json(d.values().as_ref(), idx);
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
            serde_json::Value::Null
        }
        _ => {
            if let Ok(s) = arrow::util::display::array_value_to_string(col, i) {
                serde_json::Value::String(s)
            } else {
                serde_json::Value::Null
            }
        }
    }
}

fn list_values_to_json(inner: &dyn Array) -> serde_json::Value {
    let mut arr = Vec::with_capacity(inner.len());
    for j in 0..inner.len() {
        arr.push(array_value_to_json(inner, j));
    }
    serde_json::Value::Array(arr)
}

fn struct_row_to_json(st: &arrow::array::StructArray, i: usize) -> serde_json::Value {
    if st.is_null(i) {
        return serde_json::Value::Null;
    }
    let mut map = serde_json::Map::new();
    for (j, field) in st.fields().iter().enumerate() {
        map.insert(
            field.name().clone(),
            array_value_to_json(st.column(j).as_ref(), i),
        );
    }
    serde_json::Value::Object(map)
}

/// Naive full scan of all Parquet files - baseline for `needle bench`.
pub fn naive_scan(files: &[PathBuf], key: &str) -> Result<(Vec<ListenRow>, Duration)> {
    let t0 = Instant::now();
    let mut rows = Vec::new();
    for path in files {
        let file = File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        for batch in reader {
            let batch = batch?;
            for row in extract_listens(&batch, path, key)? {
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

    #[test]
    fn covering_only_skips_rows_but_has_covering() {
        let (_tmp, querier, _) = setup(WriteMode::Sorted, true);
        let res = querier
            .query_with(
                "user_0005",
                &QueryOptions {
                    covering_only: true,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(res.rows.is_empty());
        assert!(!res.covering_hits.is_empty());
        assert_eq!(res.timings.bytes_ranged, 0);
        assert_eq!(res.timings.pages_touched, 0);
    }

    #[test]
    fn min_listen_count_drops_entries() {
        let (_tmp, querier, _) = setup(WriteMode::Sorted, true);
        let res = querier
            .query_with(
                "user_0005",
                &QueryOptions {
                    min_listen_count: Some(u64::MAX),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(res.rows.is_empty());
        assert!(
            res.skipped_by_predicate >= 1,
            "expected skipped_by_predicate >= 1, got {}",
            res.skipped_by_predicate
        );
    }

    #[test]
    fn since_until_filters_decoded_rows() {
        let (_tmp, querier, _) = setup(WriteMode::Sorted, true);
        let none = querier
            .query_with(
                "user_0000",
                &QueryOptions {
                    since_ms: Some(i64::MAX),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(none.rows.is_empty());

        let full = querier.query("user_0000").unwrap();
        assert!(!full.rows.is_empty());
        let ts = full.rows[0].timestamp_ms;
        let keep = querier
            .query_with(
                "user_0000",
                &QueryOptions {
                    since_ms: Some(ts),
                    until_ms: Some(ts + 86_400_000),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            !keep.rows.is_empty(),
            "expected at least one row in time window around {ts}"
        );
        for r in &keep.rows {
            assert!(r.timestamp_ms >= ts);
            assert!(r.timestamp_ms <= ts + 86_400_000);
        }
    }

    #[test]
    fn columns_projection_reduces_page_descriptions() {
        let (_tmp, querier, _) = setup(WriteMode::Sorted, false);
        let res = querier
            .query_with(
                "user_0001",
                &QueryOptions {
                    columns: Some(vec!["track_uri".into()]),
                    ..Default::default()
                },
            )
            .unwrap();
        if res.timings.used_index_page_locs {
            for d in &res.page_descriptions {
                assert!(
                    !d.contains("duration_ms"),
                    "projected page_descriptions still mention duration_ms: {d}"
                );
            }
            assert!(
                res.page_descriptions.iter().any(|d| d.contains("track_uri")),
                "expected track_uri in page_descriptions: {:?}",
                res.page_descriptions
            );
        }
    }

    #[test]
    fn explain_does_not_need_http() {
        let (_tmp, querier, _) = setup(WriteMode::Sorted, false);
        let expl = querier
            .explain("user_0001", &QueryOptions::default())
            .unwrap();
        assert!(expl.num_entries > 0);
        assert!(!expl.files.is_empty());
        assert!(
            expl.estimated_bytes > 0 || !expl.page_descriptions.is_empty(),
            "explain should estimate bytes or describe pages"
        );
    }

    #[test]
    fn json_rows_contains_listen_fields() {
        let (_tmp, querier, _) = setup(WriteMode::Sorted, false);
        let res = querier.query("user_0000").unwrap();
        let json = res.json_rows();
        assert!(!json.is_empty(), "expected json rows for user_0000");
        let row = &json[0];
        assert!(
            row.get("track_uri").is_some() || row.get("timestamp").is_some(),
            "expected track_uri or timestamp in json row: {row}"
        );
    }

    #[test]
    fn record_batch_row_count_matches_rows() {
        let (_tmp, querier, _) = setup(WriteMode::Sorted, false);
        let res = querier.query("user_0000").unwrap();
        assert_eq!(res.record_batch().num_rows(), res.rows.len());
        assert!(!res.rows.is_empty());
    }

    #[test]
    fn covering_only_empty_batch() {
        let (_tmp, querier, _) = setup(WriteMode::Sorted, true);
        let res = querier
            .query_with(
                "user_0005",
                &QueryOptions {
                    covering_only: true,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(res.record_batch().num_rows(), 0);
        assert!(res.rows.is_empty());
    }
}
