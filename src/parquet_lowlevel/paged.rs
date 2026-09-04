//! Multi-page PLAIN / UNCOMPRESSED Parquet for fat lake files.
//!
//! Unlike `tiny.rs` (one data page per column, no OffsetIndex), this writer
//! flushes a data page every `page_rows` rows and emits a real OffsetIndex so
//! RAP can Range-GET **pages**, not whole objects.

use super::thrift::{
    self, ColumnChunkInfo, Kv, PageLoc, RowGroupInfo, SchemaField, CODEC_UNCOMPRESSED,
    CONV_TIMESTAMP_MILLIS, CONV_UTF8, ENC_PLAIN, ENC_RLE, TYPE_BYTE_ARRAY, TYPE_INT64,
};
use super::tiny::TinyRow;
use anyhow::{bail, Result};

const MAGIC: &[u8; 4] = b"PAR1";
const CREATED_BY: &str = "rap-rust parquet_lowlevel paged-plain 0.1.0";

/// One data page in a column chunk (OffsetIndex row).
#[derive(Debug, Clone)]
pub struct PageLocInfo {
    pub column: String,
    pub offset: u64,
    pub compressed_size: u32,
    pub first_row_index: i64,
}

/// Result of writing a multi-page file: per-column page locations.
#[derive(Debug, Clone)]
pub struct PagedWriteMeta {
    pub num_rows: usize,
    pub page_rows: usize,
    pub pages: Vec<PageLocInfo>,
    pub bytes: usize,
}

fn schema() -> [SchemaField; 4] {
    [
        SchemaField {
            name: "user_id".into(),
            physical: TYPE_BYTE_ARRAY,
            converted: Some(CONV_UTF8),
            logical: Some("utf8"),
        },
        SchemaField {
            name: "timestamp".into(),
            physical: TYPE_INT64,
            converted: Some(CONV_TIMESTAMP_MILLIS),
            logical: Some("ts_millis"),
        },
        SchemaField {
            name: "track_uri".into(),
            physical: TYPE_BYTE_ARRAY,
            converted: Some(CONV_UTF8),
            logical: Some("utf8"),
        },
        SchemaField {
            name: "duration_ms".into(),
            physical: TYPE_INT64,
            converted: None,
            logical: None,
        },
    ]
}

/// Append a spec-valid multi-page PLAIN Parquet file with OffsetIndex.
///
/// `page_rows` is the max rows per data page (must be ≥ 1). Sorted input is
/// not required for validity, but RAP point queries need it so one key's rows
/// occupy few pages.
pub fn write_paged_plain_into<R: TinyRow>(
    buf: &mut Vec<u8>,
    rows: &[R],
    page_rows: usize,
) -> Result<PagedWriteMeta> {
    if rows.is_empty() {
        bail!("paged parquet: no rows");
    }
    let page_rows = page_rows.max(1);
    let n = rows.len() as i64;
    buf.reserve(rows.len().saturating_mul(64) + 4096);
    buf.extend_from_slice(MAGIC);

    let mut chunks: Vec<ColumnChunkInfo> = Vec::with_capacity(4);
    let mut all_pages: Vec<PageLocInfo> = Vec::new();
    let mut per_col_thrift: Vec<Vec<PageLoc>> = Vec::with_capacity(4);

    // user_id
    let (c, pages) = write_byte_array_pages(buf, "user_id", rows, page_rows, |r| r.user_id());
    per_col_thrift.push(to_thrift(&pages));
    all_pages.extend(pages);
    chunks.push(c);
    // timestamp
    let (c, pages) = write_int64_pages(buf, "timestamp", rows, page_rows, |r| r.timestamp_ms());
    per_col_thrift.push(to_thrift(&pages));
    all_pages.extend(pages);
    chunks.push(c);
    // track_uri
    let (c, pages) = write_byte_array_pages(buf, "track_uri", rows, page_rows, |r| r.track_uri());
    per_col_thrift.push(to_thrift(&pages));
    all_pages.extend(pages);
    chunks.push(c);
    // duration_ms
    let (c, pages) = write_int64_pages(buf, "duration_ms", rows, page_rows, |r| r.duration_ms());
    per_col_thrift.push(to_thrift(&pages));
    all_pages.extend(pages);
    chunks.push(c);

    // OffsetIndex after all column chunks.
    for (i, locs) in per_col_thrift.iter().enumerate() {
        let blob = thrift::offset_index(locs);
        let off = buf.len() as i64;
        buf.extend_from_slice(&blob);
        chunks[i].offset_index_offset = Some(off);
        chunks[i].offset_index_length = Some(blob.len() as i32);
    }

    let rg = RowGroupInfo {
        num_rows: n,
        total_byte_size: chunks.iter().map(|c| c.uncompressed_size).sum(),
        file_offset: chunks[0].data_page_offset,
        total_compressed_size: chunks.iter().map(|c| c.compressed_size).sum(),
        ordinal: 0,
        columns: chunks,
    };
    let kv = [
        Kv {
            key: "rap.layout".into(),
            value: "paged_plain".into(),
        },
        Kv {
            key: "rap.page_rows".into(),
            value: page_rows.to_string(),
        },
    ];
    let schema = schema();
    let meta = thrift::file_metadata(&schema, n, &[rg], CREATED_BY, &kv);
    let meta_len = meta.len() as u32;
    buf.extend_from_slice(&meta);
    buf.extend_from_slice(&meta_len.to_le_bytes());
    buf.extend_from_slice(MAGIC);

    Ok(PagedWriteMeta {
        num_rows: rows.len(),
        page_rows,
        pages: all_pages,
        bytes: buf.len(),
    })
}

fn to_thrift(pages: &[PageLocInfo]) -> Vec<PageLoc> {
    pages
        .iter()
        .map(|p| PageLoc {
            offset: p.offset as i64,
            compressed_page_size: p.compressed_size as i32,
            first_row_index: p.first_row_index,
        })
        .collect()
}

fn write_byte_array_pages<R, F>(
    buf: &mut Vec<u8>,
    path: &str,
    rows: &[R],
    page_rows: usize,
    get: F,
) -> (ColumnChunkInfo, Vec<PageLocInfo>)
where
    R: TinyRow,
    F: Fn(&R) -> &str,
{
    let n = rows.len() as i64;
    let chunk_start = buf.len() as i64;
    let mut pages = Vec::new();
    let mut uncompressed = 0i64;
    let mut first = 0usize;
    while first < rows.len() {
        let last = (first + page_rows).min(rows.len());
        let slice = &rows[first..last];
        let payload_len: usize = slice.iter().map(|r| 4 + get(r).len()).sum();
        let header =
            thrift::data_page_v1_header(payload_len as i32, payload_len as i32, slice.len() as i32);
        let page_offset = buf.len() as u64;
        buf.extend_from_slice(&header);
        for r in slice {
            let s = get(r).as_bytes();
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s);
        }
        let total = (header.len() + payload_len) as u32;
        uncompressed += total as i64;
        pages.push(PageLocInfo {
            column: path.to_string(),
            offset: page_offset,
            compressed_size: total,
            first_row_index: first as i64,
        });
        first = last;
    }
    let total = (buf.len() as i64) - chunk_start;
    let chunk = ColumnChunkInfo {
        physical: TYPE_BYTE_ARRAY,
        path: path.to_string(),
        num_values: n,
        uncompressed_size: uncompressed,
        compressed_size: total,
        data_page_offset: chunk_start,
        encodings: vec![ENC_PLAIN, ENC_RLE],
        codec: CODEC_UNCOMPRESSED,
        offset_index_offset: None,
        offset_index_length: None,
        data_page_count: pages.len() as i32,
    };
    (chunk, pages)
}

fn write_int64_pages<R, F>(
    buf: &mut Vec<u8>,
    path: &str,
    rows: &[R],
    page_rows: usize,
    get: F,
) -> (ColumnChunkInfo, Vec<PageLocInfo>)
where
    R: TinyRow,
    F: Fn(&R) -> i64,
{
    let n = rows.len() as i64;
    let chunk_start = buf.len() as i64;
    let mut pages = Vec::new();
    let mut uncompressed = 0i64;
    let mut first = 0usize;
    while first < rows.len() {
        let last = (first + page_rows).min(rows.len());
        let slice = &rows[first..last];
        let payload_len = slice.len() * 8;
        let header =
            thrift::data_page_v1_header(payload_len as i32, payload_len as i32, slice.len() as i32);
        let page_offset = buf.len() as u64;
        buf.extend_from_slice(&header);
        for r in slice {
            buf.extend_from_slice(&get(r).to_le_bytes());
        }
        let total = (header.len() + payload_len) as u32;
        uncompressed += total as i64;
        pages.push(PageLocInfo {
            column: path.to_string(),
            offset: page_offset,
            compressed_size: total,
            first_row_index: first as i64,
        });
        first = last;
    }
    let total = (buf.len() as i64) - chunk_start;
    let chunk = ColumnChunkInfo {
        physical: TYPE_INT64,
        path: path.to_string(),
        num_values: n,
        uncompressed_size: uncompressed,
        compressed_size: total,
        data_page_offset: chunk_start,
        encodings: vec![ENC_PLAIN, ENC_RLE],
        codec: CODEC_UNCOMPRESSED,
        offset_index_offset: None,
        offset_index_length: None,
        data_page_count: pages.len() as i32,
    };
    (chunk, pages)
}

/// Map row numbers to the unique pages they occupy (all columns).
pub fn pages_for_rows(pages: &[PageLocInfo], rows: &[u64], total_rows: u64) -> Vec<PageLocInfo> {
    use std::collections::HashSet;
    let mut by_col: std::collections::BTreeMap<&str, Vec<&PageLocInfo>> =
        std::collections::BTreeMap::new();
    for p in pages {
        by_col.entry(&p.column).or_default().push(p);
    }
    for v in by_col.values_mut() {
        v.sort_by_key(|p| p.first_row_index);
    }
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for &row in rows {
        let r = row as i64;
        for col_pages in by_col.values() {
            if col_pages.is_empty() {
                continue;
            }
            let mut lo = 0usize;
            let mut hi = col_pages.len();
            while lo + 1 < hi {
                let mid = (lo + hi) / 2;
                if col_pages[mid].first_row_index <= r {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            let p = col_pages[lo];
            let next_first = col_pages
                .get(lo + 1)
                .map(|x| x.first_row_index)
                .unwrap_or(total_rows as i64);
            if r < p.first_row_index || r >= next_first {
                continue;
            }
            let key = (p.column.as_str(), p.offset, p.compressed_size);
            if seen.insert(key) {
                out.push(p.clone());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_lowlevel::tiny::TinyRow;
    use arrow::array::{Int64Array, StringArray};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    };
    use parquet::file::metadata::PageIndexPolicy;

    struct Row {
        user_id: String,
        timestamp_ms: i64,
        track_uri: String,
        duration_ms: i64,
    }
    impl TinyRow for Row {
        fn user_id(&self) -> &str {
            &self.user_id
        }
        fn timestamp_ms(&self) -> i64 {
            self.timestamp_ms
        }
        fn track_uri(&self) -> &str {
            &self.track_uri
        }
        fn duration_ms(&self) -> i64 {
            self.duration_ms
        }
    }

    fn many_rows(n: usize, listens_per_user: usize) -> Vec<Row> {
        let mut out = Vec::with_capacity(n);
        let mut i = 0usize;
        let mut u = 0usize;
        while i < n {
            let k = listens_per_user.min(n - i);
            for r in 0..k {
                out.push(Row {
                    user_id: format!("user_{u}"),
                    timestamp_ms: 1_700_000_000_000 + (r as i64) * 180_000,
                    track_uri: format!("spotify:track:{:08}", (u * 17 + r) % 50_000),
                    duration_ms: 60_000 + (r as i64) * 1_000,
                });
            }
            i += k;
            u += 1;
        }
        out
    }

    #[test]
    fn paged_plain_offset_index_many_pages_and_arrow() {
        let rows = many_rows(2_000, 20);
        let mut buf = Vec::new();
        let meta = write_paged_plain_into(&mut buf, &rows, 32).unwrap();
        assert_eq!(&buf[..4], b"PAR1");
        assert_eq!(&buf[buf.len() - 4..], b"PAR1");
        // 2000 / 32 = 62.5 → 63 pages per column × 4 columns
        assert!(
            meta.pages.len() >= 4 * 60,
            "expected many pages, got {}",
            meta.pages.len()
        );
        let max_page = meta
            .pages
            .iter()
            .map(|p| p.compressed_size as u64)
            .max()
            .unwrap();
        assert!(
            (max_page as u64) * 4 < buf.len() as u64 / 4,
            "pages should be ≪ file (max_page={max_page} file={})",
            buf.len()
        );

        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let arrow_meta = ArrowReaderMetadata::load(&Bytes::from(buf.clone()), options)
            .expect("load with OffsetIndex");
        let oi = arrow_meta
            .metadata()
            .offset_index()
            .expect("OffsetIndex present");
        let n_pages_col0 = oi[0][0].page_locations().len();
        assert!(n_pages_col0 > 10, "col0 pages={n_pages_col0}");

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buf)).unwrap();
        let reader = builder.build().unwrap();
        let mut n = 0usize;
        let mut users = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            n += batch.num_rows();
            let ua = batch
                .column_by_name("user_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                users.push(ua.value(i).to_string());
            }
            let _ = batch
                .column_by_name("duration_ms")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
        }
        assert_eq!(n, 2_000);
        assert_eq!(users[0], "user_0");
        assert_eq!(users[19], "user_0");
        assert_eq!(users[20], "user_1");
    }

    #[test]
    fn pages_for_one_key_are_a_tiny_slice() {
        let rows = many_rows(4_000, 16);
        let mut buf = Vec::new();
        let meta = write_paged_plain_into(&mut buf, &rows, 32).unwrap();
        // user_0 occupies rows 0..16 → one page per column (page_rows=32)
        let row_nums: Vec<u64> = (0..16).collect();
        let locs = pages_for_rows(&meta.pages, &row_nums, 4_000);
        assert_eq!(locs.len(), 4, "one page per column for a 16-row key");
        let ranged: u64 = locs.iter().map(|p| p.compressed_size as u64).sum();
        let ratio = ranged as f64 / buf.len() as f64;
        assert!(
            ratio < 0.05,
            "bytes_ranged/file={ratio:.4} ranged={ranged} file={}",
            buf.len()
        );
    }
}
