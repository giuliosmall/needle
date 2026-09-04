//! Local MinIO data-lake scale demo (no Suitsupply / GCS / gcloud).
//!
//! Layout mirrors the Spotify article: `date=YYYY-MM-DD/bucket=NNN/part-*.parquet`
//! with a huge number of tiny Parquet objects. RAP indexes keys → S3 URIs + page
//! locs, then point queries issue HTTP Range / S3 GetObject Range against MinIO.

use crate::index::{
    CoveringValues, IndexFragmentMeta, PageLoc, RapIndexEntry, key_bucket, load_index_for_keys,
    load_index_file_dictionary, load_index_entries_for_keys,
};
use crate::parquet_lowlevel::{pages_for_rows, write_paged_plain_into, write_tiny_plain_into, PageLocInfo, TinyRow};
use crate::query::{QueryOptions, QueryResult, RapQuerier};
use crate::s3::{S3Client, S3RangeReader, S3StatsSnap};
use crate::storage::RangeReader;
use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use arrow::array::{Int64Array, StringArray};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::PageIndexPolicy;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::{self, File};
use bytes::Bytes;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

thread_local! {
    /// Reused PLAIN/UNCOMPRESSED Parquet buffer (avoids per-object realloc).
    static PQ_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(2048));
    /// Fat-file buffer (~100–500 KiB objects).
    static FAT_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(512 * 1024));
}

pub const DEFAULT_BUCKET: &str = "rap-lake";
pub const DEFAULT_INDEX: &str = "data/rap-lake-index";
pub const TOOLS_MINIO: &str = "tools/minio";
pub const TOOLS_MC: &str = "tools/mc";
pub const MINIO_DATA: &str = "/workspace/rap-minio-data";

#[derive(Debug, Clone)]
pub struct LakeGenerateOpts {
    pub files: usize,
    pub bucket: String,
    pub days: usize,
    pub hash_buckets: usize,
    pub rows_per_file: usize,
    pub seed: u64,
    pub parallelism: usize,
    pub index_dir: PathBuf,
    pub fragment_id: String,
    pub index_buckets: u32,
    /// Prefix under the bucket (empty = root). Fat lake defaults to `fat`.
    pub prefix: String,
    /// Skip re-PUT if continuing; still rebuilds index from fresh puts in this run.
    pub clear_index: bool,
    /// Fat lake: few large multi-page files instead of 1M tiny objects.
    pub fat: bool,
    /// Rows per data page (fat writer). Small values → many OffsetIndex pages.
    pub page_rows: usize,
    /// Listens (rows) per user in fat files. Keep ≤ page_rows so one key ≈ one page/col.
    pub listens_per_user: usize,
}

impl Default for LakeGenerateOpts {
    fn default() -> Self {
        Self {
            files: 100_000,
            bucket: DEFAULT_BUCKET.into(),
            days: 100,
            hash_buckets: 100,
            rows_per_file: 4,
            seed: 42,
            parallelism: 96,
            index_dir: PathBuf::from(DEFAULT_INDEX),
            fragment_id: "lake-frag".into(),
            index_buckets: 256,
            prefix: String::new(),
            clear_index: true,
            fat: false,
            page_rows: 64,
            listens_per_user: 32,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LakeManifest {
    pub bucket: String,
    pub endpoint: String,
    pub objects: u64,
    pub bytes_uploaded: u64,
    pub index_dir: String,
    pub fragment_id: String,
    pub days: usize,
    pub hash_buckets: usize,
    pub rows_per_file: usize,
    pub generated_at: String,
    pub sample_uris: Vec<String>,
    pub sample_keys: Vec<String>,
    #[serde(default)]
    pub fat: bool,
    #[serde(default)]
    pub page_rows: usize,
    #[serde(default)]
    pub file_sizes: Vec<u64>,
    #[serde(default)]
    pub pages_per_file: usize,
}

#[derive(Debug, Clone)]
struct TinyListen {
    user_id: String,
    timestamp_ms: i64,
    track_uri: String,
    duration_ms: i64,
}

impl TinyRow for TinyListen {
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

fn lake_object_key(prefix: &str, date: &str, hash_bucket: usize, part: usize) -> String {
    if prefix.is_empty() {
        format!("date={date}/bucket={hash_bucket:03}/part-{part:06}.parquet")
    } else {
        format!(
            "{}/date={date}/bucket={hash_bucket:03}/part-{part:06}.parquet",
            prefix.trim_end_matches('/')
        )
    }
}

/// Ensure MinIO is reachable; create bucket + anonymous download.
pub fn ensure_minio(bucket: &str) -> Result<S3Client> {
    let client = S3Client::from_env();
    // Cheap health check: list a prefix that should be empty (do NOT list the
    // whole lake — that is 100k–1M keys and dominated startup).
    match client.list_objects(bucket, ".rap-healthcheck") {
        Ok(_) => Ok(client),
        Err(_) => {
            // Try creating via mc if available.
            let mc = PathBuf::from(TOOLS_MC);
            if mc.exists() {
                let _ = std::process::Command::new(&mc)
                    .args([
                        "alias",
                        "set",
                        "local",
                        &format!("http://{}", client.cfg.endpoint),
                        &client.cfg.access_key,
                        &client.cfg.secret_key,
                    ])
                    .status();
                let _ = std::process::Command::new(&mc)
                    .args(["mb", "-p", &format!("local/{bucket}")])
                    .status();
                let _ = std::process::Command::new(&mc)
                    .args(["anonymous", "set", "download", &format!("local/{bucket}")])
                    .status();
            }
            // Retry list (empty ok).
            let _ = client.list_objects(bucket, "");
            Ok(client)
        }
    }
}

/// Start MinIO server if not already listening (uses tools/minio binary).
pub fn minio_up() -> Result<()> {
    // Already up?
    if TcpOk::check("127.0.0.1:9000") {
        println!("MinIO already listening on 127.0.0.1:9000");
        let client = ensure_minio(DEFAULT_BUCKET)?;
        println!(
            "Bucket `{DEFAULT_BUCKET}` ready (endpoint={}, anon_read={})",
            client.cfg.endpoint, client.cfg.anonymous_read
        );
        println!("Credentials: {} / {} (local-only)", client.cfg.access_key, client.cfg.secret_key);
        return Ok(());
    }
    let minio = PathBuf::from(TOOLS_MINIO);
    if !minio.exists() {
        bail!(
            "MinIO binary not found at {} — download linux-amd64 minio + mc into tools/",
            minio.display()
        );
    }
    fs::create_dir_all(MINIO_DATA)?;
    let log = PathBuf::from(MINIO_DATA).join("minio.log");
    let log_file = File::create(&log)?;
    let mut cmd = std::process::Command::new(&minio);
    cmd.args([
        "server",
        MINIO_DATA,
        "--address",
        "127.0.0.1:9000",
        "--console-address",
        "127.0.0.1:9001",
    ])
    .env("MINIO_ROOT_USER", "minioadmin")
    .env("MINIO_ROOT_PASSWORD", "minioadmin")
    .stdout(log_file.try_clone()?)
    .stderr(log_file)
    .spawn()
    .context("spawn minio")?;
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if TcpOk::check("127.0.0.1:9000") {
            break;
        }
    }
    if !TcpOk::check("127.0.0.1:9000") {
        bail!("MinIO failed to start — see {}", log.display());
    }
    let client = ensure_minio(DEFAULT_BUCKET)?;
    println!("MinIO started at http://127.0.0.1:9000 (console :9001)");
    println!("Data dir: {MINIO_DATA}");
    println!("Bucket: {DEFAULT_BUCKET}");
    println!("Credentials: minioadmin / minioadmin (local-only, NOT cloud)");
    let _ = client;
    Ok(())
}

struct TcpOk;
impl TcpOk {
    fn check(addr: &str) -> bool {
        std::net::TcpStream::connect(addr).is_ok()
    }
}

/// Generate lake-shaped tiny Parquet objects into MinIO and stream a RAP index.
pub fn lake_generate(opts: &LakeGenerateOpts) -> Result<LakeManifest> {
    if opts.fat {
        return lake_generate_fat(opts);
    }
    let client = ensure_minio(&opts.bucket)?;
    let t0 = Instant::now();

    if opts.clear_index && opts.index_dir.exists() {
        fs::remove_dir_all(&opts.index_dir)?;
    }
    fs::create_dir_all(opts.index_dir.join("fragments").join(&opts.fragment_id).join("buckets"))?;

    let num_buckets = opts.index_buckets.max(1);
    let writers: Vec<Mutex<BufWriter<File>>> = (0..num_buckets)
        .map(|bi| {
            let p = opts
                .index_dir
                .join("fragments")
                .join(&opts.fragment_id)
                .join("buckets")
                .join(format!("bucket_{bi:03}.jsonl"));
            let f = File::create(p).expect("create bucket jsonl");
            Mutex::new(BufWriter::with_capacity(64 * 1024, f))
        })
        .collect();
    let uploaded = AtomicU64::new(0);
    let bytes_up = AtomicU64::new(0);
    let encode_ns = AtomicU64::new(0);
    let put_ns = AtomicU64::new(0);
    let index_ns = AtomicU64::new(0);
    let sample_uris: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let sample_keys: Mutex<Vec<String>> = Mutex::new(Vec::new());

    // Precompute date strings.
    let dates: Vec<String> = (0..opts.days.max(1))
        .map(|d| {
            let day = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(d as i64))
                .unwrap();
            day.format("%Y-%m-%d").to_string()
        })
        .collect();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(opts.parallelism.max(1))
        .build()
        .context("rayon pool")?;

    let client = Arc::new(client);
    let opts_c = opts.clone();
    let dates = Arc::new(dates);

    pool.install(|| {
        (0..opts_c.files).into_par_iter().try_for_each(|i| -> Result<()> {
            let mut rng = StdRng::seed_from_u64(opts_c.seed.wrapping_add(i as u64));
            let day_i = i % dates.len();
            let hb = (i / dates.len()) % opts_c.hash_buckets.max(1);
            let key_path = lake_object_key(&opts_c.prefix, &dates[day_i], hb, i);

            // Deterministic user ids so point queries hit known keys.
            // Spread users across files: ~files distinct primary keys when rows_per_file small.
            let base_user = i; // one primary user per file
            let mut listens = Vec::with_capacity(opts_c.rows_per_file);
            for r in 0..opts_c.rows_per_file.max(1) {
                let uid = if r == 0 {
                    base_user
                } else {
                    // occasional second key in same file
                    base_user.wrapping_add(r * 9973)
                };
                listens.push(TinyListen {
                    user_id: format!("user_{uid}"),
                    timestamp_ms: 1_700_000_000_000i64
                        + (day_i as i64) * 86_400_000
                        + (r as i64) * 180_000
                        + rng.gen_range(0..10_000),
                    track_uri: format!("spotify:track:{:08}", rng.gen_range(0..50_000)),
                    duration_ms: rng.gen_range(60_000..300_000),
                });
            }
            listens.sort_by(|a, b| {
                a.user_id
                    .cmp(&b.user_id)
                    .then(a.timestamp_ms.cmp(&b.timestamp_ms))
            });

            // Encode into a thread-local buffer (PLAIN UNCOMPRESSED, no ArrowWriter).
            let nbytes = PQ_BUF.with(|slot| -> Result<usize> {
                let mut buf = slot.borrow_mut();
                buf.clear();
                let t_enc = Instant::now();
                write_tiny_plain_into(&mut buf, &listens)?;
                encode_ns.fetch_add(t_enc.elapsed().as_nanos() as u64, Ordering::Relaxed);
                let t_put = Instant::now();
                client.put_object(&opts_c.bucket, &key_path, &buf)?;
                put_ns.fetch_add(t_put.elapsed().as_nanos() as u64, Ordering::Relaxed);
                Ok(buf.len())
            })?;
            // Fast path: index from in-memory listens; contiguous span = whole object
            // (tiny files — one ranged read). Skips footer parse at generate-time.
            // File ordinal = generation index (deterministic; no global dict lock).
            let t_idx = Instant::now();
            let entries = index_entries_fast(&listens, nbytes as u64, i as u32)?;

            for e in entries {
                let b = key_bucket(&e.key, num_buckets) as usize;
                let line = serde_json::to_string(&e)?;
                let mut w = writers[b].lock().unwrap();
                writeln!(w, "{line}")?;
            }
            index_ns.fetch_add(t_idx.elapsed().as_nanos() as u64, Ordering::Relaxed);

            uploaded.fetch_add(1, Ordering::Relaxed);
            bytes_up.fetch_add(nbytes as u64, Ordering::Relaxed);

            if i < 8 {
                sample_uris
                    .lock()
                    .unwrap()
                    .push(S3Client::s3_uri(&opts_c.bucket, &key_path));
                sample_keys
                    .lock()
                    .unwrap()
                    .push(format!("user_{base_user}"));
            }

            let done = uploaded.load(Ordering::Relaxed);
            if done > 0 && done % 10_000 == 0 {
                let secs = t0.elapsed().as_secs_f64();
                let rate = done as f64 / secs.max(1e-6);
                eprintln!(
                    "  lake-generate progress: ~{done}/{} ({:.1}s, {:.0} obj/s)",
                    opts_c.files, secs, rate
                );
            }
            Ok(())
        })
    })?;

    // Flush writers + write bincode twins.
    for (bi, w) in writers.into_iter().enumerate() {
        let mut w = w.into_inner().unwrap();
        w.flush()?;
        drop(w);
        let jsonl = opts
            .index_dir
            .join("fragments")
            .join(&opts.fragment_id)
            .join("buckets")
            .join(format!("bucket_{bi:03}.jsonl"));
        let entries = read_jsonl_entries(&jsonl)?;
        let bin = opts
            .index_dir
            .join("fragments")
            .join(&opts.fragment_id)
            .join("buckets")
            .join(format!("bucket_{bi:03}.bin"));
        let mut f = BufWriter::new(File::create(bin)?);
        bincode::serialize_into(&mut f, &entries)?;
        f.flush()?;
    }

    // Reconstruct URI dictionary in ordinal order (i == file ordinal).
    let files: Vec<String> = (0..opts.files)
        .map(|i| {
            let day_i = i % dates.len();
            let hb = (i / dates.len()) % opts.hash_buckets.max(1);
            let key = lake_object_key(&opts.prefix, &dates[day_i], hb, i);
            S3Client::s3_uri(&opts.bucket, &key)
        })
        .collect();
    let meta = IndexFragmentMeta {
        fragment_id: opts.fragment_id.clone(),
        created_at: chrono::Utc::now().to_rfc3339(),
        files: files.clone(),
        num_buckets,
        note: Some(format!(
            "MinIO lake generate files={} days={} hash_buckets={}",
            opts.files, opts.days, opts.hash_buckets
        )),
    };
    let frag_dir = opts.index_dir.join("fragments").join(&opts.fragment_id);
    serde_json::to_writer_pretty(File::create(frag_dir.join("manifest.json"))?, &meta)?;
    serde_json::to_writer_pretty(
        File::create(opts.index_dir.join("registry.json"))?,
        &vec![opts.fragment_id.clone()],
    )?;

    let n = uploaded.load(Ordering::Relaxed);
    let b = bytes_up.load(Ordering::Relaxed);
    let manifest = LakeManifest {
        bucket: opts.bucket.clone(),
        endpoint: client.cfg.endpoint.clone(),
        objects: n,
        bytes_uploaded: b,
        index_dir: opts.index_dir.display().to_string(),
        fragment_id: opts.fragment_id.clone(),
        days: opts.days,
        hash_buckets: opts.hash_buckets,
        rows_per_file: opts.rows_per_file,
        generated_at: chrono::Utc::now().to_rfc3339(),
        sample_uris: sample_uris.into_inner().unwrap(),
        sample_keys: sample_keys.into_inner().unwrap(),
        fat: false,
        page_rows: 0,
        file_sizes: Vec::new(),
        pages_per_file: 0,
    };
    fs::write(
        opts.index_dir.join("lake_manifest.json"),
        serde_json::to_string_pretty(&manifest)?,
    )?;

    let snap = client.stats.snapshot();
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "Lake generate done: {} objects, {} bytes in {:.3}s ({:.0} obj/s) → index {}",
        n,
        b,
        secs,
        n as f64 / secs.max(1e-9),
        opts.index_dir.display()
    );
    println!(
        "  s3 puts={} mc_fallbacks={} bytes_written={}",
        snap.puts, snap.mc_fallbacks, snap.bytes_written
    );
    let nthreads = opts.parallelism.max(1) as f64;
    let enc = encode_ns.load(Ordering::Relaxed) as f64 / 1e9;
    let put = put_ns.load(Ordering::Relaxed) as f64 / 1e9;
    let idx = index_ns.load(Ordering::Relaxed) as f64 / 1e9;
    println!(
        "  cpu-sum encode={enc:.3}s put={put:.3}s index={idx:.3}s (÷{nthreads:.0} threads ≈ wall encode={:.3}s put={:.3}s index={:.3}s)",
        enc / nthreads,
        put / nthreads,
        idx / nthreads
    );
    Ok(manifest)
}

fn read_jsonl_entries(path: &Path) -> Result<Vec<RapIndexEntry>> {
    let text = fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        out.push(serde_json::from_str(line)?);
    }
    Ok(out)
}


fn index_entries_fast(
    listens: &[TinyListen],
    object_size: u64,
    file_ord: u32,
) -> Result<Vec<RapIndexEntry>> {
    use std::collections::BTreeMap;
    let mut key_rows: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut covering: BTreeMap<String, CoveringValues> = BTreeMap::new();
    for (i, l) in listens.iter().enumerate() {
        key_rows
            .entry(l.user_id.clone())
            .or_default()
            .push(i as u64);
        let c = covering.entry(l.user_id.clone()).or_insert(CoveringValues {
            listen_count: 0,
            total_duration_ms: 0,
        });
        c.listen_count += 1;
        c.total_duration_ms += l.duration_ms as u64;
    }
    let mut entries = Vec::new();
    for (key, rows) in key_rows {
        let cov = covering.get(&key).cloned();
        let vc = cov
            .as_ref()
            .map(|c| c.listen_count)
            .unwrap_or(rows.len() as u64);
        // One Range GET covering the tiny object (article: precise ranged reads).
        entries.push(RapIndexEntry {
            key,
            file: file_ord,
            row_numbers: rows,
            value_count: Some(vc),
            covering: cov,
            page_locs: Some(vec![
                PageLoc {
                    column: "*".into(),
                    offset: 0,
                    size: object_size as u32,
                },
            ]),
            frame_locs: None,
            contiguous: None,
            prepared_file: None,
            aligned: None,
        });
    }
    Ok(entries)
}


fn index_entries_from_bytes(bytes: &[u8], file_ord: u32) -> Result<(Vec<RapIndexEntry>, Vec<String>)> {
    let owned = Bytes::copy_from_slice(bytes);
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let arrow_meta = ArrowReaderMetadata::load(&owned, options)?;
    let pq = arrow_meta.metadata();

    // Scan user_id column via arrow reader over in-memory bytes.
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(owned.clone())?;
    let reader = builder.build()?;
    let mut key_rows: std::collections::BTreeMap<String, Vec<u64>> =
        std::collections::BTreeMap::new();
    let mut covering: std::collections::BTreeMap<String, CoveringValues> =
        std::collections::BTreeMap::new();
    let mut global = 0u64;
    for batch in reader {
        let batch = batch?;
        let users = batch
            .column_by_name("user_id")
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
            let k = users.value(i).to_string();
            key_rows.entry(k.clone()).or_default().push(global);
            let c = covering.entry(k).or_insert(CoveringValues {
                listen_count: 0,
                total_duration_ms: 0,
            });
            c.listen_count += 1;
            c.total_duration_ms += durs.value(i) as u64;
            global += 1;
        }
    }

    let offset_indexes = pq.offset_index();
    let mut starts = Vec::new();
    let mut running = 0i64;
    for i in 0..pq.num_row_groups() {
        starts.push(running);
        running += pq.row_group(i).num_rows();
    }

    let mut entries = Vec::new();
    let mut keys = Vec::new();
    for (key, rows) in key_rows {
        let page_locs = if let Some(ois) = offset_indexes {
            capture_page_locs_mem(pq, &starts, ois, &rows).ok()
        } else {
            None
        };
        let cov = covering.get(&key).cloned();
        let vc = cov.as_ref().map(|c| c.listen_count).unwrap_or(rows.len() as u64);
        keys.push(key.clone());
        entries.push(RapIndexEntry {
            key,
            file: file_ord,
            row_numbers: rows,
            value_count: Some(vc),
            covering: cov,
            page_locs,
            frame_locs: None,
            contiguous: None,
            prepared_file: None,
            aligned: None,
        });
    }
    Ok((entries, keys))
}



fn capture_page_locs_mem(
    pq: &parquet::file::metadata::ParquetMetaData,
    starts: &[i64],
    offset_indexes: &[Vec<parquet::file::page_index::offset_index::OffsetIndexMetaData>],
    rows: &[u64],
) -> Result<Vec<PageLoc>> {
    use std::collections::HashSet;
    let cols_wanted = ["user_id", "timestamp", "track_uri", "duration_ms"];
    let mut locs = Vec::new();
    let mut seen = HashSet::new();
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
                .context("missing offset index")?;
            let pages = oi.page_locations();
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
            if seen.insert(key) {
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

/// Rebuild RAP index by listing MinIO and scanning each object (footer + key column).
/// Heavier than generate-time indexing; useful to verify streaming list+footer path.
pub fn lake_index_from_bucket(
    bucket: &str,
    prefix: &str,
    index_dir: &Path,
    fragment_id: &str,
    index_buckets: u32,
    max_objects: Option<usize>,
) -> Result<()> {
    let client = ensure_minio(bucket)?;
    let t0 = Instant::now();
    let mut keys = client.list_objects(bucket, prefix)?;
    keys.retain(|k| k.ends_with(".parquet"));
    keys.sort();
    if let Some(m) = max_objects {
        keys.truncate(m);
    }
    if keys.is_empty() {
        bail!("no parquet objects under s3://{bucket}/{prefix}");
    }
    println!("Indexing {} objects from s3://{bucket}/{prefix} …", keys.len());

    if index_dir.exists() {
        fs::remove_dir_all(index_dir)?;
    }
    fs::create_dir_all(index_dir.join("fragments").join(fragment_id).join("buckets"))?;
    let num_buckets = index_buckets.max(1);
    let writers: Vec<Mutex<BufWriter<File>>> = (0..num_buckets)
        .map(|bi| {
            let p = index_dir
                .join("fragments")
                .join(fragment_id)
                .join("buckets")
                .join(format!("bucket_{bi:03}.jsonl"));
            Mutex::new(BufWriter::new(File::create(p).unwrap()))
        })
        .collect();
    let file_dict: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let client = Arc::new(client);

    keys.par_iter().enumerate().try_for_each(|(i, key)| -> Result<()> {
        let bytes = client.get_object(bucket, key)?;
        let uri = S3Client::s3_uri(bucket, key);
        let ordinal = {
            let mut d = file_dict.lock().unwrap();
            let ord = d.len() as u32;
            d.push(uri);
            ord
        };
        let (entries, _) = index_entries_from_bytes(&bytes, ordinal)?;
        for e in entries {
            let b = key_bucket(&e.key, num_buckets) as usize;
            let line = serde_json::to_string(&e)?;
            writeln!(writers[b].lock().unwrap(), "{line}")?;
        }
        if i > 0 && i % 5_000 == 0 {
            eprintln!("  lake-index progress: {i}/{}", keys.len());
        }
        Ok(())
    })?;

    let mut files = file_dict.into_inner().unwrap();
    // Preserve order from parallel is nondeterministic — re-sort by URI and rewrite ordinals?
    // For correctness of entries.file we must keep dict order as assigned. OK.
    let _ = &mut files;

    for (bi, w) in writers.into_iter().enumerate() {
        let mut w = w.into_inner().unwrap();
        w.flush()?;
        drop(w);
        let jsonl = index_dir
            .join("fragments")
            .join(fragment_id)
            .join("buckets")
            .join(format!("bucket_{bi:03}.jsonl"));
        let entries = read_jsonl_entries(&jsonl)?;
        let bin = index_dir
            .join("fragments")
            .join(fragment_id)
            .join("buckets")
            .join(format!("bucket_{bi:03}.bin"));
        bincode::serialize_into(BufWriter::new(File::create(bin)?), &entries)?;
    }

    let meta = IndexFragmentMeta {
        fragment_id: fragment_id.to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        files,
        num_buckets,
        note: Some("lake-index from MinIO list+GET".into()),
    };
    serde_json::to_writer_pretty(
        File::create(index_dir.join("fragments").join(fragment_id).join("manifest.json"))?,
        &meta,
    )?;
    serde_json::to_writer_pretty(
        File::create(index_dir.join("registry.json"))?,
        &vec![fragment_id.to_string()],
    )?;
    println!(
        "lake-index done: {} files in {:?}",
        meta.files.len(),
        t0.elapsed()
    );
    Ok(())
}

/// Fat lake: many multi-page Parquet objects (`fat/date=…/bucket=NNN/part-*.parquet`).
///
/// Sorted by `user_id`, hash-bucketed so one user lands in one file. Writer emits
/// many data pages + OffsetIndex; RAP index stores **per-column page locs**
/// (not whole-object spans). High file counts use compact index entries
/// (`row_numbers = [first]` + `value_count`) and skip JSONL.
pub fn lake_generate_fat(opts: &LakeGenerateOpts) -> Result<LakeManifest> {
    let mut opts = opts.clone();
    if opts.prefix.is_empty() {
        opts.prefix = "fat".into();
    }
    if opts.page_rows == 0 {
        opts.page_rows = 64;
    }
    if opts.listens_per_user == 0 {
        opts.listens_per_user = 32;
    }
    if opts.rows_per_file < opts.listens_per_user {
        opts.rows_per_file = opts.listens_per_user.max(1024);
    }
    // Layout: small demos keep 1 file per hash bucket; large runs spread across
    // date= / bucket= prefixes so we do not create 200k unique bucket dirs.
    if opts.files <= 64 {
        if opts.hash_buckets == 0 || opts.hash_buckets > opts.files {
            opts.hash_buckets = opts.files.max(1);
        }
        opts.days = opts.days.max(1);
        if opts.index_buckets < 16 {
            opts.index_buckets = 64;
        }
    } else {
        if opts.hash_buckets == 0 || opts.hash_buckets >= opts.files {
            opts.hash_buckets = 100;
        }
        if opts.days <= 1 {
            opts.days = 200;
        }
        if opts.index_buckets <= 256 {
            opts.index_buckets = 2048;
        }
    }

    let users_per_file = (opts.rows_per_file / opts.listens_per_user.max(1)).max(1);
    let n_rows = users_per_file * opts.listens_per_user;
    let est_obj = (n_rows as u64).saturating_mul(70).saturating_add(16_384);
    let est_total = est_obj.saturating_mul(opts.files as u64);
    const DISK_BUDGET: u64 = 115_000_000_000; // ~10G headroom on ~126G box after cleanup
    if est_total > DISK_BUDGET {
        bail!(
            "fat generate estimate {:.1} GiB ({} files × ~{} rows) exceeds {} GiB disk budget; lower --files or --rows-per-file",
            est_total as f64 / (1024.0 * 1024.0 * 1024.0),
            opts.files,
            n_rows,
            DISK_BUDGET / (1024 * 1024 * 1024)
        );
    }

    let client = ensure_minio(&opts.bucket)?;
    let t0 = Instant::now();

    if opts.clear_index && opts.index_dir.exists() {
        fs::remove_dir_all(&opts.index_dir)?;
    }
    fs::create_dir_all(
        opts.index_dir
            .join("fragments")
            .join(&opts.fragment_id)
            .join("buckets"),
    )?;

    let num_buckets = opts.index_buckets.max(1);
    let scale = opts.files > 128;
    let writers: Vec<Mutex<BufWriter<File>>> = (0..num_buckets)
        .map(|bi| {
            let name = if scale {
                format!("bucket_{bi:03}.binpart")
            } else {
                format!("bucket_{bi:03}.jsonl")
            };
            let p = opts
                .index_dir
                .join("fragments")
                .join(&opts.fragment_id)
                .join("buckets")
                .join(name);
            let f = File::create(p).expect("create bucket staging");
            Mutex::new(BufWriter::with_capacity(1024 * 1024, f))
        })
        .collect();
    let uploaded = AtomicU64::new(0);
    let bytes_up = AtomicU64::new(0);
    let file_sizes: Mutex<Vec<(usize, u64)>> = Mutex::new(Vec::new());
    let sample_uris: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let sample_keys: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let pages_sample: Mutex<usize> = Mutex::new(0);

    let dates: Vec<String> = (0..opts.days.max(1))
        .map(|d| {
            let day = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(d as i64))
                .unwrap();
            day.format("%Y-%m-%d").to_string()
        })
        .collect();

    let nthreads = opts.parallelism.max(1).min(opts.files.max(1));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(nthreads)
        .build()
        .context("rayon pool")?;

    let client = Arc::new(client);
    let opts_c = opts.clone();
    let dates = Arc::new(dates);
    let page_rows = opts.page_rows;

    println!(
        "Fat lake generate: files={} rows/file≈{} users/file={} listens/user={} page_rows={} prefix={} days={} hash_buckets={} index_buckets={} threads={} scale={} est≈{:.1} GiB",
        opts.files,
        n_rows,
        users_per_file,
        opts.listens_per_user,
        page_rows,
        opts.prefix,
        opts.days,
        opts.hash_buckets,
        num_buckets,
        nthreads,
        scale,
        est_total as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    pool.install(|| {
        (0..opts_c.files)
            .into_par_iter()
            .try_for_each(|i| -> Result<()> {
                let mut rng = StdRng::seed_from_u64(opts_c.seed.wrapping_add(i as u64));
                let day_i = i % dates.len();
                let hb = i % opts_c.hash_buckets.max(1);
                let key_path = lake_object_key(&opts_c.prefix, &dates[day_i], hb, i);

                let mut listens = Vec::with_capacity(n_rows);
                for local in 0..users_per_file {
                    // user_0 → file 0, user_1 → file 1, … so sample keys are easy.
                    let uid = local * opts_c.files + i;
                    let user = format!("user_{uid}");
                    for r in 0..opts_c.listens_per_user {
                        listens.push(TinyListen {
                            user_id: user.clone(),
                            timestamp_ms: 1_700_000_000_000i64
                                + (day_i as i64) * 86_400_000
                                + (r as i64) * 180_000
                                + rng.gen_range(0..1_000),
                            track_uri: format!(
                                "spotify:track:{:08}",
                                rng.gen_range(0..50_000)
                            ),
                            duration_ms: rng.gen_range(60_000..300_000),
                        });
                    }
                }
                listens.sort_by(|a, b| {
                    a.user_id
                        .cmp(&b.user_id)
                        .then(a.timestamp_ms.cmp(&b.timestamp_ms))
                });

                let t_enc = Instant::now();
                let (nbytes, npages) = FAT_BUF.with(|cell| -> Result<(u64, usize)> {
                    let mut buf = cell.borrow_mut();
                    buf.clear();
                    let written = write_paged_plain_into(&mut buf, &listens, page_rows)?;
                    let npages = written.pages.len();
                    let t_put = Instant::now();
                    client.put_object(&opts_c.bucket, &key_path, &buf)?;
                    let put_ms = t_put.elapsed();
                    let enc_ms = t_enc.elapsed();

                    let entries = index_entries_from_pages(&listens, i as u32, &written.pages)?;
                    for e in entries {
                        let b = key_bucket(&e.key, num_buckets) as usize;
                        let mut w = writers[b].lock().unwrap();
                        if scale {
                            append_staged_entry(&mut w, &e)?;
                        } else {
                            let line = serde_json::to_string(&e)?;
                            writeln!(w, "{line}")?;
                        }
                    }

                    let nbytes = buf.len() as u64;
                    uploaded.fetch_add(1, Ordering::Relaxed);
                    bytes_up.fetch_add(nbytes, Ordering::Relaxed);
                    file_sizes.lock().unwrap().push((i, nbytes));

                    if i < 8 {
                        sample_uris
                            .lock()
                            .unwrap()
                            .push(S3Client::s3_uri(&opts_c.bucket, &key_path));
                        sample_keys.lock().unwrap().push(format!("user_{i}"));
                    }
                    if i == 0 {
                        *pages_sample.lock().unwrap() = npages;
                    }

                    let done = uploaded.load(Ordering::Relaxed);
                    if done <= 8
                        || done == opts_c.files as u64
                        || done % 500 == 0
                    {
                        let elapsed = t0.elapsed().as_secs_f64();
                        let rate = done as f64 / elapsed.max(1e-9);
                        let remain = (opts_c.files as u64).saturating_sub(done);
                        let eta = remain as f64 / rate.max(1e-9);
                        eprintln!(
                            "  fat-generate {done}/{} file {i}: {} rows, {} pages, {} bytes, wall_enc+put={:?} put={:?} {:.0} obj/s eta={:.0}s",
                            opts_c.files,
                            listens.len(),
                            npages,
                            nbytes,
                            enc_ms,
                            put_ms,
                            rate,
                            eta
                        );
                    }
                    Ok((nbytes, npages))
                })?;
                let _ = (nbytes, npages);
                Ok(())
            })
    })?;

    eprintln!(
        "  fat-generate PUTs done in {:.1}s, finalizing index ({} buckets, scale={})…",
        t0.elapsed().as_secs_f64(),
        num_buckets,
        scale
    );

    for (bi, w) in writers.into_iter().enumerate() {
        let mut w = w.into_inner().unwrap();
        w.flush()?;
        drop(w);
        let buckets_dir = opts
            .index_dir
            .join("fragments")
            .join(&opts.fragment_id)
            .join("buckets");
        let entries = if scale {
            let part = buckets_dir.join(format!("bucket_{bi:03}.binpart"));
            let ents = read_staged_entries(&part)?;
            let _ = fs::remove_file(&part);
            ents
        } else {
            let jsonl = buckets_dir.join(format!("bucket_{bi:03}.jsonl"));
            read_jsonl_entries(&jsonl)?
        };
        let bin = buckets_dir.join(format!("bucket_{bi:03}.bin"));
        let mut f = BufWriter::with_capacity(1024 * 1024, File::create(bin)?);
        bincode::serialize_into(&mut f, &entries)?;
        f.flush()?;
        if scale && bi % 256 == 0 {
            eprintln!("  index bucket {bi}/{num_buckets} entries={}", entries.len());
        }
    }

    let files: Vec<String> = (0..opts.files)
        .map(|i| {
            let day_i = i % dates.len();
            let hb = i % opts.hash_buckets.max(1);
            let key = lake_object_key(&opts.prefix, &dates[day_i], hb, i);
            S3Client::s3_uri(&opts.bucket, &key)
        })
        .collect();
    let meta = IndexFragmentMeta {
        fragment_id: opts.fragment_id.clone(),
        created_at: chrono::Utc::now().to_rfc3339(),
        files: files.clone(),
        num_buckets,
        note: Some(format!(
            "MinIO FAT lake generate files={} rows_per_file={} page_rows={} prefix={} compact_rows={}",
            opts.files, n_rows, opts.page_rows, opts.prefix, scale || true
        )),
    };
    let frag_dir = opts.index_dir.join("fragments").join(&opts.fragment_id);
    let mf = File::create(frag_dir.join("manifest.json"))?;
    if opts.files > 256 {
        serde_json::to_writer(mf, &meta)?;
    } else {
        serde_json::to_writer_pretty(mf, &meta)?;
    }
    serde_json::to_writer_pretty(
        File::create(opts.index_dir.join("registry.json"))?,
        &vec![opts.fragment_id.clone()],
    )?;

    let mut sizes = file_sizes.into_inner().unwrap();
    sizes.sort_by_key(|(i, _)| *i);
    let file_size_vec: Vec<u64> = {
        let mut v = vec![0u64; opts.files];
        for (i, sz) in sizes {
            if i < v.len() {
                v[i] = sz;
            }
        }
        v
    };

    let n = uploaded.load(Ordering::Relaxed);
    let b = bytes_up.load(Ordering::Relaxed);
    let pages_per_file = *pages_sample.lock().unwrap();
    let manifest = LakeManifest {
        bucket: opts.bucket.clone(),
        endpoint: client.cfg.endpoint.clone(),
        objects: n,
        bytes_uploaded: b,
        index_dir: opts.index_dir.display().to_string(),
        fragment_id: opts.fragment_id.clone(),
        days: opts.days,
        hash_buckets: opts.hash_buckets,
        rows_per_file: n_rows,
        generated_at: chrono::Utc::now().to_rfc3339(),
        sample_uris: sample_uris.into_inner().unwrap(),
        sample_keys: sample_keys.into_inner().unwrap(),
        fat: true,
        page_rows: opts.page_rows,
        file_sizes: file_size_vec,
        pages_per_file,
    };
    let man_path = opts.index_dir.join("lake_manifest.json");
    if opts.files > 256 {
        fs::write(&man_path, serde_json::to_string(&manifest)?)?;
    } else {
        fs::write(&man_path, serde_json::to_string_pretty(&manifest)?)?;
    }

    let snap = client.stats.snapshot();
    let secs = t0.elapsed().as_secs_f64();
    let avg = if n == 0 { 0.0 } else { b as f64 / n as f64 };
    println!(
        "Fat lake generate done: {} objects, {} bytes ({:.1} MiB, avg {:.0} B) in {:.3}s ({:.0} obj/s) pages/file={} → index {}",
        n,
        b,
        b as f64 / (1024.0 * 1024.0),
        avg,
        secs,
        n as f64 / secs.max(1e-9),
        pages_per_file,
        opts.index_dir.display()
    );
    println!(
        "  s3 puts={} mc_fallbacks={} bytes_written={}",
        snap.puts, snap.mc_fallbacks, snap.bytes_written
    );
    Ok(manifest)
}

fn append_staged_entry(w: &mut BufWriter<File>, e: &RapIndexEntry) -> Result<()> {
    let bytes = bincode::serialize(e)?;
    let len = bytes.len() as u32;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&bytes)?;
    Ok(())
}

fn read_staged_entries(path: &Path) -> Result<Vec<RapIndexEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut f = BufReader::with_capacity(1024 * 1024, File::open(path)?);
    let mut out = Vec::new();
    loop {
        let mut lenb = [0u8; 4];
        match f.read_exact(&mut lenb) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(lenb) as usize;
        if len == 0 || len > 16 * 1024 * 1024 {
            bail!("staged index entry has implausible length {len}");
        }
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)?;
        out.push(bincode::deserialize(&buf)?);
    }
    Ok(out)
}


fn index_entries_from_pages(
    listens: &[TinyListen],
    file_ord: u32,
    pages: &[PageLocInfo],
) -> Result<Vec<RapIndexEntry>> {
    use std::collections::BTreeMap;
    let mut key_rows: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut covering: BTreeMap<String, CoveringValues> = BTreeMap::new();
    for (i, l) in listens.iter().enumerate() {
        key_rows
            .entry(l.user_id.clone())
            .or_default()
            .push(i as u64);
        let c = covering.entry(l.user_id.clone()).or_insert(CoveringValues {
            listen_count: 0,
            total_duration_ms: 0,
        });
        c.listen_count += 1;
        c.total_duration_ms += l.duration_ms as u64;
    }
    let mut entries = Vec::new();
    for (key, rows) in key_rows {
        let cov = covering.get(&key).cloned();
        let vc = cov
            .as_ref()
            .map(|c| c.listen_count)
            .unwrap_or(rows.len() as u64);
        let page_hits = pages_for_rows(pages, &rows, listens.len() as u64);
        let page_locs: Vec<PageLoc> = page_hits
            .into_iter()
            .map(|p| PageLoc {
                column: p.column,
                offset: p.offset,
                size: p.compressed_size,
            })
            .collect();
        // Compact contiguous runs to [first_row] + value_count (expanded on load).
        let compact_rows = if rows.len() > 1 && rows.windows(2).all(|w| w[1] == w[0] + 1) {
            vec![rows[0]]
        } else {
            rows
        };
        entries.push(RapIndexEntry {
            key,
            file: file_ord,
            row_numbers: compact_rows,
            value_count: Some(vc),
            covering: cov,
            page_locs: Some(page_locs),
            frame_locs: None,
            contiguous: None,
            prepared_file: None,
            aligned: None,
        });
    }
    Ok(entries)
}

fn coalesce_ranges(mut rs: Vec<std::ops::Range<u64>>, gap: u64) -> Vec<std::ops::Range<u64>> {
    if rs.is_empty() {
        return rs;
    }
    rs.sort_by_key(|r| r.start);
    let mut out = vec![rs[0].clone()];
    for r in rs.into_iter().skip(1) {
        let last = out.last_mut().unwrap();
        if r.start <= last.end.saturating_add(gap) {
            last.end = last.end.max(r.end);
        } else {
            out.push(r);
        }
    }
    out
}

fn object_size(client: &S3Client, uri: &str, cached: Option<u64>) -> Result<u64> {
    if let Some(n) = cached {
        if n > 0 {
            return Ok(n);
        }
    }
    let (bucket, key) = S3Client::parse_uri(uri)?;
    match client.head_object(&bucket, &key) {
        Ok(n) => Ok(n),
        Err(_) => {
            let mut signed = client.clone();
            signed.cfg.anonymous_read = false;
            signed.head_object(&bucket, &key)
        }
    }
}


#[derive(Debug)]
pub struct LakeQueryReport {
    pub result: QueryResult,
    pub s3_stats: S3StatsSnap,
    pub range_requests_demo: u64,
    pub decode_bytes_fetched: u64,
    pub file_size: u64,
    pub bytes_ranged: u64,
    pub bytes_ratio: f64,
    pub coalesced_range_requests: u64,
    pub coalesced_span: u64,
    pub page_locs_count: u64,
    pub naive_full_get_bytes: u64,
    pub naive_full_get_ms: f64,
}

fn load_cached_file_sizes(index_dir: &Path) -> Vec<u64> {
    let man_path = index_dir.join("lake_manifest.json");
    if !man_path.exists() {
        return Vec::new();
    }
    let Ok(text) = fs::read_to_string(man_path) else {
        return Vec::new();
    };
    let Ok(v) = serde_json::from_str::<LakeManifest>(&text) else {
        return Vec::new();
    };
    v.file_sizes
}

/// Point query via RAP index with S3 Range GETs for page locs + ranged decode.
pub fn lake_query(index_dir: &Path, key: &str, limit: usize) -> Result<LakeQueryReport> {
    let idx = load_index_for_keys(index_dir, &[key.to_string()])?;
    let cached_sizes = load_cached_file_sizes(index_dir);
    let client = S3Client::from_env();
    client.stats.reset();

    let entries = idx.lookup(key).to_vec();
    let mut range_reqs = 0u64;
    let mut ranged_bytes = 0u64;
    let mut raw_ranges: Vec<std::ops::Range<u64>> = Vec::new();
    let mut file_size = 0u64;
    let mut page_locs_count = 0u64;

    for e in &entries {
        let path = idx.file_path(e.file)?;
        let uri = path.to_string_lossy();
        if !S3Client::is_remote_uri(&uri) {
            continue;
        }
        let cached = cached_sizes.get(e.file as usize).copied();
        file_size = file_size.saturating_add(object_size(&client, &uri, cached)?);
        let reader = S3RangeReader::from_uri(client.clone(), &uri)?;
        if let Some(locs) = &e.page_locs {
            page_locs_count += locs.len() as u64;
            for loc in locs {
                let r = loc.offset..loc.offset + loc.size as u64;
                let buf = reader.read_range(&r)?;
                range_reqs += 1;
                ranged_bytes += buf.len() as u64;
                raw_ranges.push(r);
            }
        }
    }

    let coalesced = coalesce_ranges(raw_ranges, 0);
    let coalesced_span: u64 = coalesced.iter().map(|r| r.end.saturating_sub(r.start)).sum();

    let naive_uris: Vec<String> = entries
        .iter()
        .filter_map(|e| {
            idx.file_path(e.file).ok().map(|p| p.to_string_lossy().into_owned())
        })
        .filter(|uri| S3Client::is_remote_uri(uri))
        .collect();

    let querier = RapQuerier::new(idx).with_s3(client.clone());
    let result = querier.query_with(
        key,
        &QueryOptions {
            offset: 0,
            limit: Some(limit),
            http_base: None,
        },
    )?;

    // Naive: full GET of each candidate object (not used for RAP bytes).
    let naive_t0 = Instant::now();
    let mut naive_bytes = 0u64;
    for uri in &naive_uris {
        let (bucket, object_key) = S3Client::parse_uri(uri)?;
        let body = client.get_object(&bucket, &object_key)?;
        naive_bytes += body.len() as u64;
    }
    let naive_ms = naive_t0.elapsed().as_secs_f64() * 1000.0;

    let snap = client.stats.snapshot();
    let ratio = if file_size == 0 {
        0.0
    } else {
        ranged_bytes as f64 / file_size as f64
    };
    Ok(LakeQueryReport {
        result,
        s3_stats: snap,
        range_requests_demo: range_reqs,
        decode_bytes_fetched: snap.bytes_read,
        file_size,
        bytes_ranged: ranged_bytes,
        bytes_ratio: ratio,
        coalesced_range_requests: coalesced.len() as u64,
        coalesced_span,
        page_locs_count,
        naive_full_get_bytes: naive_bytes,
        naive_full_get_ms: naive_ms,
    })
}

#[derive(Debug, Serialize)]
pub struct LakeBenchReport {
    pub objects: usize,
    pub index_size_bytes: u64,
    pub keys_queried: Vec<String>,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub avg_ms: f64,
    pub avg_range_requests: f64,
    pub avg_bytes_ranged: f64,
    pub avg_rows: f64,
    pub candidate_files_rap: f64,
    pub naive_prefix_list_cost_objects: usize,
    pub note: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_file_size: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_bytes_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_bytes_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_coalesced_range_requests: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_coalesced_span: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_naive_full_get_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_naive_full_get_bytes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fat: Option<bool>,
}

pub fn lake_bench(
    index_dir: &Path,
    keys: &[String],
    rounds: usize,
) -> Result<LakeBenchReport> {
    let idx = load_index_for_keys(index_dir, keys)?;
    let objects = {
        // File dictionary lives on the fragment even when we only loaded a few buckets.
        idx.files.len()
    };
    let index_size = dir_size(index_dir)?;
    let cached_sizes = load_cached_file_sizes(index_dir);
    let fat = index_dir.join("lake_manifest.json").exists() && {
        fs::read_to_string(index_dir.join("lake_manifest.json"))
            .ok()
            .and_then(|s| serde_json::from_str::<LakeManifest>(&s).ok())
            .map(|m| m.fat)
            .unwrap_or(false)
    };
    let client = S3Client::from_env();

    let mut latencies = Vec::new();
    let mut ranges = Vec::new();
    let mut bytes = Vec::new();
    let mut rows = Vec::new();
    let mut cand = Vec::new();
    let mut file_sizes = Vec::new();
    let mut ratios = Vec::new();
    let mut coal_reqs = Vec::new();
    let mut coal_spans = Vec::new();
    let mut naive_ms_v = Vec::new();
    let mut naive_bytes_v = Vec::new();

    let querier = RapQuerier::new(load_index_for_keys(index_dir, keys)?).with_s3(client.clone());
    if let Some(k) = keys.first() {
        let _ = querier.query(k);
    }

    for key in keys {
        for _ in 0..rounds.max(1) {
            client.stats.reset();
            let t0 = Instant::now();
            let entries = idx.lookup(key).to_vec();
            cand.push(entries.len() as f64);
            let mut range_reqs = 0u64;
            let mut ranged_bytes = 0u64;
            let mut raw_ranges: Vec<std::ops::Range<u64>> = Vec::new();
            let mut fsz = 0u64;
            for e in &entries {
                let path = idx.file_path(e.file)?;
                let uri = path.to_string_lossy();
                if S3Client::is_remote_uri(&uri) {
                    let cached = cached_sizes.get(e.file as usize).copied();
                    fsz = fsz.saturating_add(object_size(&client, &uri, cached).unwrap_or(0));
                    let reader = S3RangeReader::from_uri(client.clone(), &uri)?;
                    if let Some(locs) = &e.page_locs {
                        for loc in locs {
                            let r = loc.offset..loc.offset + loc.size as u64;
                            let buf = reader.read_range(&r)?;
                            range_reqs += 1;
                            ranged_bytes += buf.len() as u64;
                            raw_ranges.push(r);
                        }
                    }
                }
            }
            let coalesced = coalesce_ranges(raw_ranges, 0);
            let coal_span: u64 = coalesced.iter().map(|r| r.end.saturating_sub(r.start)).sum();
            let result = querier.query_with(
                key,
                &QueryOptions {
                    offset: 0,
                    limit: Some(20),
                    http_base: None,
                },
            )?;
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            latencies.push(ms);
            ranges.push(range_reqs as f64);
            bytes.push(ranged_bytes as f64);
            rows.push(result.rows.len() as f64);
            file_sizes.push(fsz as f64);
            let ratio = if fsz == 0 {
                0.0
            } else {
                ranged_bytes as f64 / fsz as f64
            };
            ratios.push(ratio);
            coal_reqs.push(coalesced.len() as f64);
            coal_spans.push(coal_span as f64);

            let nt0 = Instant::now();
            let mut nb = 0u64;
            for e in &entries {
                let path = idx.file_path(e.file)?;
                let uri = path.to_string_lossy();
                if S3Client::is_remote_uri(&uri) {
                    let (bucket, object_key) = S3Client::parse_uri(&uri)?;
                    let body = client.get_object(&bucket, &object_key)?;
                    nb += body.len() as u64;
                }
            }
            naive_ms_v.push(nt0.elapsed().as_secs_f64() * 1000.0);
            naive_bytes_v.push(nb as f64);
        }
    }

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = percentile(&latencies, 0.50);
    let p99 = percentile(&latencies, 0.99);
    let avg = latencies.iter().sum::<f64>() / latencies.len().max(1) as f64;
    let min_ratio = ratios
        .iter()
        .cloned()
        .fold(f64::INFINITY, f64::min);
    let min_ratio = if min_ratio.is_finite() { min_ratio } else { 0.0 };

    let note = if fat {
        "FAT lake: RAP Range-GETs OffsetIndex pages; naive = full object GET of the candidate file. bytes_ranged / file_size should be ≪ 1%.".into()
    } else {
        "RAP uses index→few files + Range GETs; naive baseline is full object listing cost (not opening 1M files)".into()
    };

    Ok(LakeBenchReport {
        objects,
        index_size_bytes: index_size,
        keys_queried: keys.to_vec(),
        p50_ms: p50,
        p99_ms: p99,
        avg_ms: avg,
        avg_range_requests: mean(&ranges),
        avg_bytes_ranged: mean(&bytes),
        avg_rows: mean(&rows),
        candidate_files_rap: mean(&cand),
        naive_prefix_list_cost_objects: objects,
        note,
        avg_file_size: Some(mean(&file_sizes)),
        avg_bytes_ratio: Some(mean(&ratios)),
        min_bytes_ratio: Some(min_ratio),
        avg_coalesced_range_requests: Some(mean(&coal_reqs)),
        avg_coalesced_span: Some(mean(&coal_spans)),
        avg_naive_full_get_ms: Some(mean(&naive_ms_v)),
        avg_naive_full_get_bytes: Some(mean(&naive_bytes_v)),
        fat: Some(fat),
    })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    if path.is_file() {
        return Ok(fs::metadata(path)?.len());
    }
    for e in fs::read_dir(path)? {
        let e = e?;
        let p = e.path();
        if p.is_dir() {
            total += dir_size(&p)?;
        } else {
            total += e.metadata()?.len();
        }
    }
    Ok(total)
}


#[derive(Debug, Clone)]
pub struct LakeStressOpts {
    pub index_dir: PathBuf,
    pub queries: usize,
    pub concurrency: usize,
    pub seed: u64,
    pub warmup: usize,
    pub verify_every: usize,
    /// If >0, after the fixed query batch run open-loop for this many seconds.
    pub duration_secs: u64,
    pub key_space: Option<u64>,
    pub buckets_per_wave: usize,
    pub out_json: Option<PathBuf>,
    pub limit: usize,
}

#[derive(Debug, Serialize)]
pub struct LakeStressReport {
    pub index: String,
    pub objects: usize,
    pub key_space: u64,
    pub seed: u64,
    pub warmup: usize,
    pub queries: usize,
    pub concurrency: usize,
    pub duration_secs: u64,
    pub ok: u64,
    pub err: u64,
    pub rows_returned: u64,
    pub bytes_ranged: u64,
    pub range_requests: u64,
    pub verify_ok: u64,
    pub verify_fail: u64,
    pub wall_secs: f64,
    pub qps: f64,
    pub avg_bytes_ranged: f64,
    pub avg_range_requests: f64,
    pub avg_rows: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
    pub open_loop: Option<LakeStressOpenLoop>,
    pub note: String,
}

#[derive(Debug, Serialize)]
pub struct LakeStressOpenLoop {
    pub duration_secs: f64,
    pub ok: u64,
    pub err: u64,
    pub qps: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
    pub avg_bytes_ranged: f64,
}

fn sample_stress_keys(n: usize, seed: u64, key_space: u64) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut keys = Vec::with_capacity(n);
    // Hot early keys
    let hot = [0u64, 1, 2, 3, 4, 5, 6, 7, 42, 100, 1000];
    for &h in &hot {
        if (h as u64) < key_space {
            keys.push(format!("user_{h}"));
        }
    }
    // Deep / boundary keys
    for d in [299_999u64, key_space.saturating_sub(1), 300_000, 7, key_space / 2] {
        if d < key_space {
            keys.push(format!("user_{d}"));
        }
    }
    while keys.len() < n {
        let uid = rng.gen_range(0..key_space);
        keys.push(format!("user_{uid}"));
    }
    keys.truncate(n);
    keys
}

fn infer_key_space(index_dir: &Path) -> Result<u64> {
    let man_path = index_dir.join("lake_manifest.json");
    let text = fs::read_to_string(&man_path)
        .with_context(|| format!("read {}", man_path.display()))?;
    let v: serde_json::Value = serde_json::from_str(&text)?;
    let objects = v.get("objects").and_then(|x| x.as_u64()).unwrap_or(0);
    let rows = v.get("rows_per_file").and_then(|x| x.as_u64()).unwrap_or(0);
    // Fat lake: 32 listens/user default → users_per_file = rows/32.
    let listens = 32u64;
    let users_per_file = if rows > 0 { (rows / listens).max(1) } else { 160 };
    Ok(objects.saturating_mul(users_per_file).max(1))
}

struct StressAcc {
    ok: AtomicU64,
    err: AtomicU64,
    rows: AtomicU64,
    bytes: AtomicU64,
    ranges: AtomicU64,
    verify_ok: AtomicU64,
    verify_fail: AtomicU64,
    lat_ms: Mutex<Vec<f64>>,
}

impl StressAcc {
    fn new(cap: usize) -> Self {
        Self {
            ok: AtomicU64::new(0),
            err: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            ranges: AtomicU64::new(0),
            verify_ok: AtomicU64::new(0),
            verify_fail: AtomicU64::new(0),
            lat_ms: Mutex::new(Vec::with_capacity(cap)),
        }
    }
}

/// Concurrent point-query stress against MinIO RAP lake (read-only).
pub fn lake_stress(opts: &LakeStressOpts) -> Result<LakeStressReport> {
    let key_space = match opts.key_space {
        Some(k) if k > 0 => k,
        _ => infer_key_space(&opts.index_dir)?,
    };
    let n = opts.queries.max(1);
    let keys = sample_stress_keys(n, opts.seed, key_space);
    let warmup_n = opts.warmup.min(keys.len());

    eprintln!(
        "lake-stress: loading file dictionary from {} (key_space={key_space}, queries={n}, concurrency={}, seed={})…",
        opts.index_dir.display(),
        opts.concurrency,
        opts.seed
    );
    let t_load0 = Instant::now();
    let (files, fragments, root) = load_index_file_dictionary(&opts.index_dir)?;
    let objects = files.len();
    eprintln!(
        "  files={} fragments={} load_dict={:.2}s",
        objects,
        fragments.len(),
        t_load0.elapsed().as_secs_f64()
    );

    let client = S3Client::from_env();
    let num_buckets = fragments.first().map(|f| f.num_buckets).unwrap_or(1);
    let wave_buckets = opts.buckets_per_wave.max(1);

    // Group key indices by hash bucket (first fragment).
    let mut by_bucket: HashMap<u32, Vec<usize>> = HashMap::new();
    for (i, k) in keys.iter().enumerate() {
        let b = key_bucket(k, num_buckets);
        by_bucket.entry(b).or_default().push(i);
    }
    let mut bucket_ids: Vec<u32> = by_bucket.keys().copied().collect();
    bucket_ids.sort_unstable();

    let acc = StressAcc::new(n + warmup_n);
    let wall0 = Instant::now();

    // Warm-up (sequential, first warmup_n keys) — still via waves for memory.
    if warmup_n > 0 {
        eprintln!("  warmup {warmup_n} queries…");
        let warm_keys: Vec<String> = keys[..warmup_n].to_vec();
        let warm_idx = load_index_entries_for_keys(&root, Arc::clone(&files), &fragments, &warm_keys)?;
        let querier = RapQuerier::new(warm_idx).with_s3(client.clone());
        for k in &warm_keys {
            let _ = querier.query_with(
                k,
                &QueryOptions {
                    offset: 0,
                    limit: Some(opts.limit),
                    http_base: None,
                },
            );
        }
        drop(querier);
    }

    eprintln!(
        "  main: {} queries across {} buckets in waves of ≤{}…",
        n,
        bucket_ids.len(),
        wave_buckets
    );

    for wave in bucket_ids.chunks(wave_buckets) {
        let mut wave_keys = Vec::new();
        let mut wave_positions = Vec::new(); // original index in keys for verify stride
        for &b in wave {
            for &ki in &by_bucket[&b] {
                wave_keys.push(keys[ki].clone());
                wave_positions.push(ki);
            }
        }
        if wave_keys.is_empty() {
            continue;
        }
        let idx = load_index_entries_for_keys(&root, Arc::clone(&files), &fragments, &wave_keys)?;
        let querier = RapQuerier::new(idx).with_s3(client.clone());
        // Run with verify based on original positions: pass keys and check inside using enumerate offset.
        // Simpler: run_query_batch with query_offset=0 and verify_every on local i — approximate.
        // Better: verify using original index.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(opts.concurrency.max(1))
            .build()?;
        let limit = opts.limit;
        let verify_every = opts.verify_every;
        pool.install(|| {
            wave_keys
                .par_iter()
                .zip(wave_positions.par_iter())
                .for_each(|(key, &orig_i)| {
                    let t0 = Instant::now();
                    let res = querier.query_with(
                        key,
                        &QueryOptions {
                            offset: 0,
                            limit: Some(limit),
                            http_base: None,
                        },
                    );
                    let ms = t0.elapsed().as_secs_f64() * 1000.0;
                    match res {
                        Ok(r) => {
                            acc.ok.fetch_add(1, Ordering::Relaxed);
                            acc.rows.fetch_add(r.rows.len() as u64, Ordering::Relaxed);
                            acc.bytes
                                .fetch_add(r.timings.bytes_ranged, Ordering::Relaxed);
                            acc.ranges
                                .fetch_add(r.timings.pages_touched as u64, Ordering::Relaxed);
                            if verify_every > 0 && orig_i % verify_every == 0 {
                                let nonempty = !r.rows.is_empty() || r.total_value_count > 0;
                                let cover_ok = r
                                    .covering_hits
                                    .iter()
                                    .any(|h| h.contains("listen_count="));
                                if nonempty && (r.covering_hits.is_empty() || cover_ok) {
                                    acc.verify_ok.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    acc.verify_fail.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(_) => {
                            acc.err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    acc.lat_ms.lock().unwrap().push(ms);
                });
        });
        drop(querier);
    }

    let wall_secs = wall0.elapsed().as_secs_f64();
    let ok = acc.ok.load(Ordering::Relaxed);
    let err = acc.err.load(Ordering::Relaxed);
    let rows = acc.rows.load(Ordering::Relaxed);
    let bytes = acc.bytes.load(Ordering::Relaxed);
    let ranges = acc.ranges.load(Ordering::Relaxed);
    let mut lats = acc.lat_ms.lock().unwrap().clone();
    lats.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let total_q = (ok + err).max(1) as f64;
    let qps = (ok as f64) / wall_secs.max(1e-9);

    let mut open_loop = None;
    if opts.duration_secs > 0 {
        eprintln!("  open-loop {}s max QPS…", opts.duration_secs);
        // Preload a working set: first min(4096, n) keys' buckets (few waves).
        let set_n = keys.len().min(4096).max(64);
        let set_keys: Vec<String> = keys[..set_n].to_vec();
        let idx = load_index_entries_for_keys(&root, Arc::clone(&files), &fragments, &set_keys)?;
        let querier = Arc::new(RapQuerier::new(idx).with_s3(client.clone()));
        let ol_acc = StressAcc::new(100_000);
        let stop_at = Instant::now() + Duration::from_secs(opts.duration_secs);
        let counter = AtomicUsize::new(0);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(opts.concurrency.max(1))
            .build()?;
        let set_keys = Arc::new(set_keys);
        let limit = opts.limit;
        pool.install(|| {
            (0..opts.concurrency.max(1)).into_par_iter().for_each(|_| {
                while Instant::now() < stop_at {
                    let i = counter.fetch_add(1, Ordering::Relaxed);
                    let key = &set_keys[i % set_keys.len()];
                    let t0 = Instant::now();
                    let res = querier.query_with(
                        key,
                        &QueryOptions {
                            offset: 0,
                            limit: Some(limit),
                            http_base: None,
                        },
                    );
                    let ms = t0.elapsed().as_secs_f64() * 1000.0;
                    match res {
                        Ok(r) => {
                            ol_acc.ok.fetch_add(1, Ordering::Relaxed);
                            ol_acc
                                .bytes
                                .fetch_add(r.timings.bytes_ranged, Ordering::Relaxed);
                        }
                        Err(_) => {
                            ol_acc.err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    ol_acc.lat_ms.lock().unwrap().push(ms);
                }
            });
        });
        let ol_ok = ol_acc.ok.load(Ordering::Relaxed);
        let ol_err = ol_acc.err.load(Ordering::Relaxed);
        let mut ol_lats = ol_acc.lat_ms.lock().unwrap().clone();
        ol_lats.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let ol_bytes = ol_acc.bytes.load(Ordering::Relaxed);
        let ol_dur = opts.duration_secs as f64;
        open_loop = Some(LakeStressOpenLoop {
            duration_secs: ol_dur,
            ok: ol_ok,
            err: ol_err,
            qps: ol_ok as f64 / ol_dur.max(1e-9),
            p50_ms: percentile(&ol_lats, 0.50),
            p90_ms: percentile(&ol_lats, 0.90),
            p99_ms: percentile(&ol_lats, 0.99),
            p999_ms: percentile(&ol_lats, 0.999),
            max_ms: ol_lats.last().copied().unwrap_or(0.0),
            avg_bytes_ranged: if ol_ok > 0 {
                ol_bytes as f64 / ol_ok as f64
            } else {
                0.0
            },
        });
    }

    let report = LakeStressReport {
        index: opts.index_dir.display().to_string(),
        objects,
        key_space,
        seed: opts.seed,
        warmup: warmup_n,
        queries: n,
        concurrency: opts.concurrency,
        duration_secs: opts.duration_secs,
        ok,
        err,
        rows_returned: rows,
        bytes_ranged: bytes,
        range_requests: ranges,
        verify_ok: acc.verify_ok.load(Ordering::Relaxed),
        verify_fail: acc.verify_fail.load(Ordering::Relaxed),
        wall_secs,
        qps,
        avg_bytes_ranged: if ok > 0 { bytes as f64 / ok as f64 } else { 0.0 },
        avg_range_requests: if ok > 0 {
            ranges as f64 / ok as f64
        } else {
            0.0
        },
        avg_rows: if ok > 0 { rows as f64 / ok as f64 } else { 0.0 },
        p50_ms: percentile(&lats, 0.50),
        p90_ms: percentile(&lats, 0.90),
        p99_ms: percentile(&lats, 0.99),
        p999_ms: percentile(&lats, 0.999),
        max_ms: lats.last().copied().unwrap_or(0.0),
        open_loop,
        note: format!(
            "READ-ONLY stress; waves≤{wave_buckets} buckets; no naive full GET; total_q={total_q}"
        ),
    };

    if let Some(ref out) = opts.out_json {
        if let Some(parent) = out.parent() {
            let _ = fs::create_dir_all(parent);
        }
        fs::write(out, serde_json::to_string_pretty(&report)?)?;
        eprintln!("wrote {}", out.display());
    }

    Ok(report)
}

/// Small E2E suitable for CI: N objects in MinIO, index, query.
pub fn lake_e2e_small(files: usize, index_dir: &Path) -> Result<()> {
    minio_up()?;
    let opts = LakeGenerateOpts {
        files,
        index_dir: index_dir.to_path_buf(),
        fragment_id: "e2e-lake".into(),
        clear_index: true,
        days: 5,
        hash_buckets: 4,
        rows_per_file: 3,
        parallelism: 8,
        ..Default::default()
    };
    let man = lake_generate(&opts)?;
    assert!(man.objects as usize == files);
    let key = man.sample_keys.first().cloned().unwrap_or_else(|| "user_0".into());
    let report = lake_query(index_dir, &key, 10)?;
    if report.result.rows.is_empty() {
        bail!("lake e2e: no rows for {key}");
    }
    if report.range_requests_demo == 0 {
        bail!("lake e2e: expected S3 Range GETs for page_locs");
    }
    println!(
        "lake e2e OK: key={key} rows={} range_reqs={} bytes={}",
        report.result.rows.len(),
        report.range_requests_demo,
        report.s3_stats.bytes_read
    );
    Ok(())
}

/// Fat-file E2E: a few multi-MB files, many pages, Range-GET ≪ object.
pub fn lake_e2e_fat_small(index_dir: &Path) -> Result<()> {
    minio_up()?;
    let opts = LakeGenerateOpts {
        files: 2,
        rows_per_file: 40_000,
        page_rows: 32,
        listens_per_user: 16,
        index_dir: index_dir.to_path_buf(),
        fragment_id: "e2e-fat".into(),
        clear_index: true,
        days: 1,
        hash_buckets: 2,
        parallelism: 2,
        fat: true,
        prefix: "fat-e2e".into(),
        bucket: DEFAULT_BUCKET.into(),
        ..Default::default()
    };
    let man = lake_generate(&opts)?;
    if man.objects != 2 {
        bail!("fat e2e: expected 2 objects, got {}", man.objects);
    }
    let min_sz = man.file_sizes.iter().copied().min().unwrap_or(0);
    if min_sz < 500_000 {
        bail!("fat e2e: file too small ({min_sz} bytes) — need multi-MB-ish for the proof");
    }
    let key = man.sample_keys.first().cloned().unwrap_or_else(|| "user_0".into());
    let report = lake_query(index_dir, &key, 32)?;
    if report.result.rows.is_empty() {
        bail!("fat e2e: no rows for {key}");
    }
    if report.result.rows.len() as u64 != opts.listens_per_user as u64
        && report.result.total_value_count != opts.listens_per_user as u64
    {
        // limit may truncate displayed rows; value_count is the source of truth
        if report.result.total_value_count != opts.listens_per_user as u64 {
            bail!(
                "fat e2e: expected {} values for {key}, got rows={} value_count={}",
                opts.listens_per_user,
                report.result.rows.len(),
                report.result.total_value_count
            );
        }
    }
    if report.page_locs_count < 4 {
        bail!(
            "fat e2e: expected ≥4 page_locs (one per column), got {}",
            report.page_locs_count
        );
    }
    // Whole-object loc is NOT success.
    if report.bytes_ranged >= report.file_size {
        bail!(
            "fat e2e: bytes_ranged {} ≥ file_size {} (whole-object Range is not success)",
            report.bytes_ranged,
            report.file_size
        );
    }
    if report.bytes_ratio >= 0.05 {
        bail!(
            "fat e2e: bytes_ranged/file_size = {:.4} (want ≪ 5%, ideally < 1%)",
            report.bytes_ratio
        );
    }
    if report.naive_full_get_bytes < report.file_size {
        // naive should pull the whole object
        eprintln!(
            "fat e2e note: naive GET {} vs file_size {}",
            report.naive_full_get_bytes, report.file_size
        );
    }
    println!(
        "fat e2e OK: key={key} rows={} value_count={} file_size={} bytes_ranged={} ratio={:.4}% ranges={} coalesced={} naive_ms={:.2}",
        report.result.rows.len(),
        report.result.total_value_count,
        report.file_size,
        report.bytes_ranged,
        report.bytes_ratio * 100.0,
        report.range_requests_demo,
        report.coalesced_range_requests,
        report.naive_full_get_ms
    );
    Ok(())
}

/// One-shot demo: minio-up → generate → query → bench.
pub fn lake_demo(files: usize, index_dir: &Path) -> Result<()> {
    minio_up()?;
    let opts = LakeGenerateOpts {
        files,
        index_dir: index_dir.to_path_buf(),
        clear_index: true,
        ..Default::default()
    };
    let man = lake_generate(&opts)?;
    let keys = if man.sample_keys.is_empty() {
        vec!["user_0".into(), "user_1".into(), "user_42".into()]
    } else {
        man.sample_keys.clone()
    };
    println!("\n=== lake query ===");
    for k in keys.iter().take(3) {
        let r = lake_query(index_dir, k, 5)?;
        println!(
            "  {k}: rows={} range_reqs={} s3_bytes={} file_size={} ratio={:.4} total={:?}",
            r.result.rows.len(),
            r.range_requests_demo,
            r.s3_stats.bytes_read,
            r.file_size,
            r.bytes_ratio,
            r.result.timings.total
        );
    }
    println!("\n=== lake bench ===");
    let bench = lake_bench(index_dir, &keys[..keys.len().min(5)], 3)?;
    println!("{}", serde_json::to_string_pretty(&bench)?);
    Ok(())
}

/// One-shot fat demo: minio-up → fat generate → query → bench vs full GET.
pub fn lake_demo_fat(files: usize, rows_per_file: usize, index_dir: &Path) -> Result<()> {
    minio_up()?;
    let opts = LakeGenerateOpts {
        files,
        rows_per_file,
        page_rows: 64,
        listens_per_user: 32,
        index_dir: index_dir.to_path_buf(),
        fragment_id: "lake-fat".into(),
        clear_index: true,
        fat: true,
        prefix: "fat".into(),
        days: 1,
        parallelism: 4,
        ..Default::default()
    };
    let man = lake_generate(&opts)?;
    let keys = if man.sample_keys.is_empty() {
        vec!["user_0".into()]
    } else {
        man.sample_keys.clone()
    };
    println!("\n=== fat lake query (page Range-GET vs full GET) ===");
    for k in keys.iter().take(3) {
        let r = lake_query(index_dir, k, 8)?;
        println!(
            "  {k}: rows={} value_count={} file_size={} bytes_ranged={} ratio={:.4}% ranges={} coalesced={} coalesced_span={} naive_full_get={} bytes in {:.2}ms",
            r.result.rows.len(),
            r.result.total_value_count,
            r.file_size,
            r.bytes_ranged,
            r.bytes_ratio * 100.0,
            r.range_requests_demo,
            r.coalesced_range_requests,
            r.coalesced_span,
            r.naive_full_get_bytes,
            r.naive_full_get_ms
        );
        if r.bytes_ratio >= 0.01 {
            println!("  (ratio not yet < 1% — still ≪ full object if ratio < 5%)");
        }
    }
    println!("\n=== fat lake bench ===");
    let bench = lake_bench(index_dir, &keys[..keys.len().min(4)], 3)?;
    println!("{}", serde_json::to_string_pretty(&bench)?);
    Ok(())
}
