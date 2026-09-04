//! Fast PLAIN / UNCOMPRESSED Parquet builder for lake-scale tiny objects.
//!
//! Writes straight into a reusable `Vec<u8>` (no ArrowWriter, no compression,
//! no OffsetIndex). Schema matches the lake listen columns so Arrow / RAP
//! decode the objects as ordinary Parquet.
//!
//! Layout: `PAR1` + 4 column chunks (one data page each) + FileMetaData +
//! footer length + `PAR1`. Page payload is PLAIN values only (REQUIRED cols
//! have no definition/repetition levels).

use super::thrift::{
    self, ColumnChunkInfo, Kv, RowGroupInfo, SchemaField, CODEC_UNCOMPRESSED,
    CONV_TIMESTAMP_MILLIS, CONV_UTF8, ENC_PLAIN, ENC_RLE, TYPE_BYTE_ARRAY, TYPE_INT64,
};
use anyhow::{bail, Result};

const MAGIC: &[u8; 4] = b"PAR1";
const CREATED_BY: &str = "rap-rust parquet_lowlevel tiny-plain 0.1.0";

/// Row view used by the tiny lake writer.
pub trait TinyRow {
    fn user_id(&self) -> &str;
    fn timestamp_ms(&self) -> i64;
    fn track_uri(&self) -> &str;
    fn duration_ms(&self) -> i64;
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

/// Append a spec-valid PLAIN/UNCOMPRESSED Parquet file for `rows` into `buf`.
/// Caller should `buf.clear()` (or reuse a grown buffer) before calling.
pub fn write_tiny_plain_into<R: TinyRow>(buf: &mut Vec<u8>, rows: &[R]) -> Result<()> {
    if rows.is_empty() {
        bail!("tiny parquet: no rows");
    }
    let n = rows.len() as i64;
    let n_i32 = rows.len() as i32;
    buf.reserve(1024);
    buf.extend_from_slice(MAGIC);

    let mut chunks: Vec<ColumnChunkInfo> = Vec::with_capacity(4);

    // user_id BYTE_ARRAY PLAIN
    chunks.push(write_byte_array_page(
        buf,
        "user_id",
        n,
        n_i32,
        rows.iter().map(|r| r.user_id()),
    ));
    // timestamp INT64 PLAIN
    chunks.push(write_int64_page(
        buf,
        "timestamp",
        n,
        n_i32,
        rows.iter().map(|r| r.timestamp_ms()),
    ));
    // track_uri BYTE_ARRAY PLAIN
    chunks.push(write_byte_array_page(
        buf,
        "track_uri",
        n,
        n_i32,
        rows.iter().map(|r| r.track_uri()),
    ));
    // duration_ms INT64 PLAIN
    chunks.push(write_int64_page(
        buf,
        "duration_ms",
        n,
        n_i32,
        rows.iter().map(|r| r.duration_ms()),
    ));

    let rg = RowGroupInfo {
        num_rows: n,
        total_byte_size: chunks.iter().map(|c| c.uncompressed_size).sum(),
        file_offset: chunks[0].data_page_offset,
        total_compressed_size: chunks.iter().map(|c| c.compressed_size).sum(),
        ordinal: 0,
        columns: chunks,
    };
    let kv = [Kv {
        key: "rap.layout".into(),
        value: "tiny_plain".into(),
    }];
    let schema = schema();
    let meta = thrift::file_metadata(&schema, n, &[rg], CREATED_BY, &kv);
    let meta_len = meta.len() as u32;
    buf.extend_from_slice(&meta);
    buf.extend_from_slice(&meta_len.to_le_bytes());
    buf.extend_from_slice(MAGIC);
    Ok(())
}

fn write_byte_array_page<'a>(
    buf: &mut Vec<u8>,
    path: &str,
    num_rows: i64,
    num_values: i32,
    values: impl Iterator<Item = &'a str> + Clone,
) -> ColumnChunkInfo {
    let payload_len: usize = values.clone().map(|s| 4 + s.len()).sum();
    write_page(
        buf,
        path,
        TYPE_BYTE_ARRAY,
        num_rows,
        num_values,
        payload_len,
        |out| {
            for s in values {
                let b = s.as_bytes();
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
        },
    )
}

fn write_int64_page(
    buf: &mut Vec<u8>,
    path: &str,
    num_rows: i64,
    num_values: i32,
    values: impl Iterator<Item = i64>,
) -> ColumnChunkInfo {
    let payload_len = num_rows as usize * 8;
    write_page(
        buf,
        path,
        TYPE_INT64,
        num_rows,
        num_values,
        payload_len,
        |out| {
            for v in values {
                out.extend_from_slice(&v.to_le_bytes());
            }
        },
    )
}

fn write_page(
    buf: &mut Vec<u8>,
    path: &str,
    physical: i32,
    num_rows: i64,
    num_values: i32,
    payload_len: usize,
    write_payload: impl FnOnce(&mut Vec<u8>),
) -> ColumnChunkInfo {
    let header = thrift::data_page_v1_header(payload_len as i32, payload_len as i32, num_values);
    let page_offset = buf.len() as i64;
    buf.extend_from_slice(&header);
    let payload_start = buf.len();
    write_payload(buf);
    debug_assert_eq!(buf.len() - payload_start, payload_len);
    let total = (header.len() + payload_len) as i64;
    ColumnChunkInfo {
        physical,
        path: path.to_string(),
        num_values: num_rows,
        uncompressed_size: total,
        compressed_size: total,
        data_page_offset: page_offset,
        encodings: vec![ENC_PLAIN, ENC_RLE],
        codec: CODEC_UNCOMPRESSED,
        offset_index_offset: None,
        offset_index_length: None,
        data_page_count: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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

    fn sample() -> Vec<Row> {
        vec![
            Row {
                user_id: "user_0".into(),
                timestamp_ms: 1_700_000_000_000,
                track_uri: "spotify:track:00000001".into(),
                duration_ms: 180_000,
            },
            Row {
                user_id: "user_42".into(),
                timestamp_ms: 1_700_000_180_000,
                track_uri: "spotify:track:00000002".into(),
                duration_ms: 90_000,
            },
        ]
    }

    #[test]
    fn tiny_plain_par1_and_arrow_roundtrip() {
        let rows = sample();
        let mut buf = Vec::new();
        write_tiny_plain_into(&mut buf, &rows).unwrap();
        assert_eq!(&buf[..4], b"PAR1");
        assert_eq!(&buf[buf.len() - 4..], b"PAR1");
        assert!(buf.len() > 64);

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buf)).unwrap();
        let reader = builder.build().unwrap();
        let mut got_users = Vec::new();
        let mut got_ts = Vec::new();
        let mut got_tracks = Vec::new();
        let mut got_durs = Vec::new();
        let mut n = 0usize;
        for batch in reader {
            let batch = batch.unwrap();
            n += batch.num_rows();
            let users = batch
                .column_by_name("user_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ts = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .unwrap();
            let tracks = batch
                .column_by_name("track_uri")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let durs = batch
                .column_by_name("duration_ms")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                got_users.push(users.value(i).to_string());
                got_ts.push(ts.value(i));
                got_tracks.push(tracks.value(i).to_string());
                got_durs.push(durs.value(i));
            }
        }
        assert_eq!(n, 2);
        assert_eq!(got_users, vec!["user_0", "user_42"]);
        assert_eq!(got_ts, vec![1_700_000_000_000, 1_700_000_180_000]);
        assert_eq!(
            got_tracks,
            vec!["spotify:track:00000001", "spotify:track:00000002"]
        );
        assert_eq!(got_durs, vec![180_000, 90_000]);
    }

    #[test]
    fn tiny_plain_reuses_buffer() {
        let rows = sample();
        let mut buf = Vec::with_capacity(64);
        write_tiny_plain_into(&mut buf, &rows).unwrap();
        let first_len = buf.len();
        buf.clear();
        write_tiny_plain_into(&mut buf, &rows).unwrap();
        assert_eq!(buf.len(), first_len);
        assert!(buf.capacity() >= first_len);
    }

    #[test]
    fn tiny_plain_encode_throughput() {
        use std::time::Instant;
        let rows = sample();
        let mut buf = Vec::with_capacity(2048);
        write_tiny_plain_into(&mut buf, &rows).unwrap();
        let n = 20_000usize;
        let t0 = Instant::now();
        for _ in 0..n {
            buf.clear();
            write_tiny_plain_into(&mut buf, &rows).unwrap();
        }
        let dt = t0.elapsed().as_secs_f64();
        eprintln!(
            "tiny encode {n} files in {dt:.3}s ({:.0}/s, {} bytes each)",
            n as f64 / dt,
            buf.len()
        );
    }
}
