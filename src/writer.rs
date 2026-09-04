//! Sample Parquet / prepared-layout writer for listening-history style data.
//!
//! Article optimisations:
//! - Sorting by key → contiguous rows per key → fewer pages
//! - Co-grouping → one row per key with nested/list values
//! - One page per key → flush a data page at each key boundary; page locs in index
//! - Blobs / Variants → pack point-query fields into one JSON column
//! - ZSTD frame resets / storage alignment / interleaving → custom Parquet pages
//!   (see `parquet_lowlevel/` + NOTES.md)

use crate::parquet_lowlevel::{self, Layout as RapLayout};
use crate::prepared::ListenRec;
use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BinaryArray, Int64Array, ListBuilder, StringArray, StringBuilder,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum WriteMode {
    /// Flat rows, unsorted (baseline — keys scattered across pages).
    Unsorted,
    /// Flat rows sorted by user_id (article: concentrating key data).
    Sorted,
    /// One row per user with ARRAY of listen structs (article: co-grouping).
    Cogrouped,
    /// Flush a Parquet data page at each key boundary (article: one page per key).
    #[value(name = "one-page-per-key")]
    OnePagePerKey,
    /// Pack listens into a single JSON blob column (article: blobs / variants).
    Blob,
    /// Custom Parquet: ZSTD frame per key inside each column data page.
    #[value(name = "zstd-frames")]
    ZstdFrames,
    /// Custom Parquet: ZSTD frames + 4KB skippable-frame alignment.
    Aligned,
    /// Custom Parquet: interleaved multi-column spans via skippable bridges.
    Interleaved,
}

#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub out_dir: PathBuf,
    pub num_users: usize,
    pub listens_per_user: usize,
    pub num_files: usize,
    pub mode: WriteMode,
    /// Target max rows per row group (sorted/unsorted).
    pub rows_per_row_group: usize,
    /// Enable writing OffsetIndex / ColumnIndex (needed for precise page mapping).
    pub write_page_index: bool,
    pub seed: u64,
    /// Force one-page-per-key behaviour even when mode is Sorted.
    pub one_page_per_key: bool,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            out_dir: PathBuf::from("data/parquet"),
            num_users: 200,
            listens_per_user: 50,
            num_files: 4,
            mode: WriteMode::Sorted,
            rows_per_row_group: 1024,
            write_page_index: true,
            seed: 42,
            one_page_per_key: false,
        }
    }
}

#[derive(Debug, Clone)]
struct Listen {
    user_id: String,
    timestamp_ms: i64,
    track_uri: String,
    duration_ms: i64,
}

impl Listen {
    fn to_rec(&self) -> ListenRec {
        ListenRec {
            user_id: self.user_id.clone(),
            timestamp_ms: self.timestamp_ms,
            track_uri: self.track_uri.clone(),
            duration_ms: self.duration_ms,
        }
    }
}

/// Write a sample listening-history dataset; returns paths of primary Parquet files.
pub fn write_sample_dataset(opts: &WriterOptions) -> Result<Vec<PathBuf>> {
    fs::create_dir_all(&opts.out_dir)?;
    let mut rng = StdRng::seed_from_u64(opts.seed);

    let mut all: Vec<Listen> = Vec::with_capacity(opts.num_users * opts.listens_per_user);
    let base_ts = 1_700_000_000_000i64;
    for u in 0..opts.num_users {
        let user_id = format!("user_{u:04}");
        for listen_i in 0..opts.listens_per_user {
            let track = rng.gen_range(0..5_000);
            all.push(Listen {
                user_id: user_id.clone(),
                timestamp_ms: base_ts
                    + (u as i64 * 86_400_000)
                    + (listen_i as i64 * 180_000)
                    + rng.gen_range(0..60_000),
                track_uri: format!("spotify:track:{track:08}"),
                duration_ms: rng.gen_range(60_000..360_000),
            });
        }
    }

    let mut buckets: Vec<Vec<Listen>> = (0..opts.num_files).map(|_| Vec::new()).collect();
    for listen in all {
        let bucket = if matches!(opts.mode, WriteMode::Unsorted) {
            rng.gen_range(0..opts.num_files)
        } else {
            let n: u32 = listen
                .user_id
                .bytes()
                .fold(0u32, |a, b| a.wrapping_mul(31).wrapping_add(b as u32));
            (n as usize) % opts.num_files
        };
        buckets[bucket].push(listen);
    }

    let want_opp = opts.one_page_per_key || matches!(opts.mode, WriteMode::OnePagePerKey);
    let mut paths = Vec::new();
    let mut prepared_notes = Vec::new();

    for (i, mut bucket) in buckets.into_iter().enumerate() {
        match opts.mode {
            WriteMode::Unsorted => {
                use rand::seq::SliceRandom;
                bucket.shuffle(&mut rng);
            }
            _ => {
                bucket.sort_by(|a, b| {
                    a.user_id
                        .cmp(&b.user_id)
                        .then(a.timestamp_ms.cmp(&b.timestamp_ms))
                });
            }
        }

        let mode_name = mode_slug(opts.mode, want_opp);
        let name = format!("listens_{mode_name}_{i:02}.parquet");
        let path = opts.out_dir.join(&name);

        match opts.mode {
            WriteMode::Cogrouped => write_cogrouped(&path, &bucket, opts)?,
            WriteMode::Blob => write_blob(&path, &bucket, opts)?,
            WriteMode::OnePagePerKey => write_one_page_per_key(&path, &bucket, opts)?,
            WriteMode::ZstdFrames | WriteMode::Aligned | WriteMode::Interleaved => {
                let recs: Vec<ListenRec> = bucket.iter().map(|l| l.to_rec()).collect();
                let layout = match opts.mode {
                    WriteMode::ZstdFrames => RapLayout::ZstdFrames { align: false },
                    WriteMode::Aligned => RapLayout::Aligned { align: true },
                    WriteMode::Interleaved => RapLayout::Interleaved { align: true },
                    _ => unreachable!(),
                };
                let man = parquet_lowlevel::write_listens_parquet(&path, &recs, layout)?;
                let proof = parquet_lowlevel::verify_parquet_file(&path)?;
                prepared_notes.push(format!(
                    "{} ({}; aligned keys={}/{}; {})",
                    path.display(),
                    man.kind,
                    man.keys.iter().filter(|k| k.aligned).count(),
                    man.keys.len(),
                    proof
                ));
            }
            WriteMode::Sorted | WriteMode::Unsorted => {
                if want_opp {
                    write_one_page_per_key(&path, &bucket, opts)?;
                } else {
                    write_flat(&path, &bucket, opts)?;
                }
            }
        }
        paths.push(path);
    }

    let meta = serde_json::json!({
        "mode": format!("{:?}", opts.mode),
        "one_page_per_key": want_opp,
        "num_users": opts.num_users,
        "listens_per_user": opts.listens_per_user,
        "num_files": opts.num_files,
        "rows_per_row_group": opts.rows_per_row_group,
        "write_page_index": opts.write_page_index,
        "files": paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
        "prepared": prepared_notes,
    });
    fs::write(
        opts.out_dir.join("dataset_meta.json"),
        serde_json::to_string_pretty(&meta)?,
    )?;

    Ok(paths)
}

fn mode_slug(mode: WriteMode, opp: bool) -> &'static str {
    match mode {
        WriteMode::Unsorted => "unsorted",
        WriteMode::Sorted if opp => "one_page_per_key",
        WriteMode::Sorted => "sorted",
        WriteMode::Cogrouped => "cogrouped",
        WriteMode::OnePagePerKey => "one_page_per_key",
        WriteMode::Blob => "blob",
        WriteMode::ZstdFrames => "zstd_frames",
        WriteMode::Aligned => "aligned",
        WriteMode::Interleaved => "interleaved",
    }
}

fn writer_props(opts: &WriterOptions) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_write_batch_size(32)
        .set_data_page_row_count_limit(32)
        .set_data_page_size_limit(256)
        .set_max_row_group_row_count(Some(opts.rows_per_row_group))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_offset_index_disabled(!opts.write_page_index)
        .build()
}

/// One-page-per-key: one RecordBatch (and effectively one page / RG) per key.
/// Uses tiny page limits + one batch per key so OffsetIndex yields one page
/// location per key per column — stored into the RAP index at build time.
fn one_page_props(rows_in_key: usize, write_page_index: bool) -> WriterProperties {
    let n = rows_in_key.max(1);
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_write_batch_size(n)
        // One page holds the entire key: row-count limit = key size, size limit large.
        .set_data_page_row_count_limit(n)
        .set_data_page_size_limit(1024 * 1024)
        .set_max_row_group_row_count(Some(n))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_offset_index_disabled(!write_page_index)
        .build()
}

fn write_flat(path: &Path, listens: &[Listen], opts: &WriterOptions) -> Result<()> {
    let schema = flat_schema();
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let props = writer_props(opts);
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    let chunk = 32usize.min(opts.rows_per_row_group.max(1));
    for slice in listens.chunks(chunk) {
        writer.write(&flat_batch(&schema, slice)?)?;
    }
    writer.close()?;
    Ok(())
}

/// True one-page-per-key writer: each key is its own RecordBatch with page/RG
/// limits equal to the key's row count, forcing a page boundary at every key.
fn write_one_page_per_key(path: &Path, listens: &[Listen], opts: &WriterOptions) -> Result<()> {
    let schema = flat_schema();
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    // Open writer with a permissive max RG; we recreate props per key via
    // flush by writing one key then relying on max_row_group_row_count.
    // arrow-rs applies properties at writer construction — so we use the
    // max key size as the page/RG limit and feed exactly one key per write,
    // calling `flush` between keys when available.
    let max_key = listens
        .iter()
        .fold(std::collections::HashMap::<&str, usize>::new(), |mut m, l| {
            *m.entry(&l.user_id).or_default() += 1;
            m
        })
        .into_values()
        .max()
        .unwrap_or(1);
    let props = one_page_props(max_key, opts.write_page_index);
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut i = 0;
    while i < listens.len() {
        let key = &listens[i].user_id;
        let mut j = i + 1;
        while j < listens.len() && &listens[j].user_id == key {
            j += 1;
        }
        let slice = &listens[i..j];
        writer.write(&flat_batch(&schema, slice)?)?;
        // Flush buffered row group so the next key starts a new RG/page.
        writer.flush()?;
        i = j;
    }
    writer.close()?;
    Ok(())
}

/// Blob / Variant mode: one row per key, `payload` = JSON document of listens.
fn write_blob(path: &Path, listens: &[Listen], opts: &WriterOptions) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
        // Keep a binary twin for "Variant-like" opaque bytes demos.
        Field::new("payload_bytes", DataType::Binary, false),
    ]));

    let mut users = Vec::new();
    let mut payloads = Vec::new();
    let mut payloads_bin: Vec<Vec<u8>> = Vec::new();

    let mut i = 0;
    while i < listens.len() {
        let key = &listens[i].user_id;
        let mut j = i + 1;
        while j < listens.len() && &listens[j].user_id == key {
            j += 1;
        }
        let slice = &listens[i..j];
        let arr: Vec<serde_json::Value> = slice
            .iter()
            .map(|l| {
                serde_json::json!({
                    "timestamp_ms": l.timestamp_ms,
                    "track_uri": l.track_uri,
                    "duration_ms": l.duration_ms,
                })
            })
            .collect();
        let doc = serde_json::json!({ "listens": arr }).to_string();
        payloads_bin.push(doc.as_bytes().to_vec());
        users.push(key.clone());
        payloads.push(doc);
        i = j;
    }

    let user_id: ArrayRef = Arc::new(StringArray::from(users));
    let payload: ArrayRef = Arc::new(StringArray::from(payloads));
    let payload_bytes: ArrayRef = Arc::new(BinaryArray::from(
        payloads_bin
            .iter()
            .map(|b| Some(b.as_slice()))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![user_id, payload, payload_bytes])?;

    let file = File::create(path)?;
    // One row per key → one page per key naturally with small limits.
    let props = one_page_props(1, opts.write_page_index);
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    // Write one row at a time and flush so each key is its own page.
    for row in 0..batch.num_rows() {
        let slice = batch.slice(row, 1);
        writer.write(&slice)?;
        writer.flush()?;
    }
    writer.close()?;
    Ok(())
}

fn flat_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("track_uri", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Int64, false),
    ]))
}

fn flat_batch(schema: &Arc<Schema>, slice: &[Listen]) -> Result<RecordBatch> {
    let user_id: ArrayRef = Arc::new(StringArray::from_iter_values(
        slice.iter().map(|l| l.user_id.as_str()),
    ));
    let timestamp: ArrayRef = Arc::new(TimestampMillisecondArray::from(
        slice.iter().map(|l| l.timestamp_ms).collect::<Vec<_>>(),
    ));
    let track_uri: ArrayRef = Arc::new(StringArray::from_iter_values(
        slice.iter().map(|l| l.track_uri.as_str()),
    ));
    let duration_ms: ArrayRef = Arc::new(Int64Array::from(
        slice.iter().map(|l| l.duration_ms).collect::<Vec<_>>(),
    ));
    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![user_id, timestamp, track_uri, duration_ms],
    )?)
}

fn write_cogrouped(path: &Path, listens: &[Listen], opts: &WriterOptions) -> Result<()> {
    let listen_fields = Fields::from(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("track_uri", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Int64, false),
    ]);
    let list_field = Field::new(
        "listens",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(listen_fields.clone()),
            true, // StructBuilder yields nullable items
        ))),
        false,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        list_field,
    ]));

    let mut users: Vec<String> = Vec::new();
    let mut groups: Vec<Vec<&Listen>> = Vec::new();
    for l in listens {
        if users.last().map(|u| u == &l.user_id).unwrap_or(false) {
            groups.last_mut().unwrap().push(l);
        } else {
            users.push(l.user_id.clone());
            groups.push(vec![l]);
        }
    }

    let mut list_builder = ListBuilder::new({
        let ts = TimestampMillisecondArray::builder(64);
        let track = StringBuilder::new();
        let dur = arrow::array::Int64Builder::new();
        arrow::array::StructBuilder::new(
            listen_fields,
            vec![Box::new(ts), Box::new(track), Box::new(dur)],
        )
    });

    for group in &groups {
        let values = list_builder.values();
        for l in group {
            values
                .field_builder::<arrow::array::TimestampMillisecondBuilder>(0)
                .unwrap()
                .append_value(l.timestamp_ms);
            values
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(&l.track_uri);
            values
                .field_builder::<arrow::array::Int64Builder>(2)
                .unwrap()
                .append_value(l.duration_ms);
            values.append(true);
        }
        list_builder.append(true);
    }

    let user_arr: ArrayRef = Arc::new(StringArray::from(users));
    let list_arr: ArrayRef = Arc::new(list_builder.finish());
    let batch = RecordBatch::try_new(schema.clone(), vec![user_arr, list_arr])?;

    let file = File::create(path)?;
    let props = writer_props(opts);
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    fn count_rows(path: &Path) -> usize {
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
        reader.map(|b| b.unwrap().num_rows()).sum()
    }

    fn schema_names(path: &Path) -> Vec<String> {
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn base(dir: &Path, mode: WriteMode) -> WriterOptions {
        WriterOptions {
            out_dir: dir.to_path_buf(),
            num_users: 12,
            listens_per_user: 5,
            num_files: 2,
            mode,
            rows_per_row_group: 32,
            write_page_index: true,
            seed: 99,
            one_page_per_key: false,
        }
    }

    #[test]
    fn sorted_unsorted_schemas_and_counts() {
        for mode in [WriteMode::Sorted, WriteMode::Unsorted] {
            let tmp = tempfile::tempdir().unwrap();
            let paths = write_sample_dataset(&base(tmp.path(), mode)).unwrap();
            let total: usize = paths.iter().map(|p| count_rows(p)).sum();
            assert_eq!(total, 12 * 5);
            for p in &paths {
                assert_eq!(
                    schema_names(p),
                    vec!["user_id", "timestamp", "track_uri", "duration_ms"]
                );
            }
        }
    }

    #[test]
    fn cogrouped_one_row_per_user() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = write_sample_dataset(&base(tmp.path(), WriteMode::Cogrouped)).unwrap();
        let total: usize = paths.iter().map(|p| count_rows(p)).sum();
        assert_eq!(total, 12, "cogrouped = one row per user");
        for p in &paths {
            let names = schema_names(p);
            assert!(names.contains(&"user_id".to_string()));
            assert!(names.contains(&"listens".to_string()));
        }
    }

    #[test]
    fn blob_schema_and_one_row_per_user() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = write_sample_dataset(&base(tmp.path(), WriteMode::Blob)).unwrap();
        let total: usize = paths.iter().map(|p| count_rows(p)).sum();
        assert_eq!(total, 12);
        for p in &paths {
            assert_eq!(
                schema_names(p),
                vec!["user_id", "payload", "payload_bytes"]
            );
        }
    }

    #[test]
    fn one_page_per_key_row_count() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = write_sample_dataset(&base(tmp.path(), WriteMode::OnePagePerKey)).unwrap();
        let total: usize = paths.iter().map(|p| count_rows(p)).sum();
        assert_eq!(total, 12 * 5);
        assert_eq!(
            schema_names(&paths[0]),
            vec!["user_id", "timestamp", "track_uri", "duration_ms"]
        );
    }
}
