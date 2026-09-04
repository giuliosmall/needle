//! CLI for Needle / Random Access Parquet (RAP).
//!
//! Commands map to the article workflow:
//!   rap generate  - write prepared Parquet / sidecars
//!   rap index     - build external append-only index (+ optional secondary)
//!   rap query     - point lookup via index + ranged reads (+ filters / JSON)
//!   rap explain   - plan a lookup (files, pages, estimated Range GETs)
//!   rap stats     - fragment / manifest summary without loading buckets
//!   rap bench     - compare naive scan vs RAP
//!   rap serve     - tiny HTTP Range server for object-store demo
//!   rap demo / demo-full - end-to-end demos

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate};
use clap::{Parser, Subcommand, ValueEnum};
use rap::index::{IndexBuilder, load_index, load_index_for_keys};
use rap::query::{QueryOptions, RapQuerier, collect_demo_ranges, naive_scan};
use rap::secondary::{self, refs_to_primary_entries};
use rap::lake::{self, LakeGenerateOpts};
use rap::storage::{RangeHttpServer, prove_http_matches_local};
use rap::parquet_lowlevel;
use rap::writer::{WriteMode, WriterOptions, write_sample_dataset};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(
    name = "rap",
    about = "Needle - point queries on a Parquet data lake (Random Access Parquet)",
    version
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    #[default]
    Table,
    Json,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Write sample listening-history Parquet / prepared files.
    Generate {
        #[arg(long, default_value = "data/parquet")]
        out: PathBuf,
        #[arg(long, default_value_t = 500)]
        users: usize,
        #[arg(long, default_value_t = 80)]
        listens_per_user: usize,
        #[arg(long, default_value_t = 4)]
        files: usize,
        #[arg(long, value_enum, default_value_t = WriteMode::Sorted)]
        mode: WriteMode,
        /// Force one-page-per-key flush (also available as --mode one-page-per-key).
        #[arg(long, default_value_t = false)]
        one_page_per_key: bool,
        #[arg(long, default_value_t = 1024)]
        rows_per_row_group: usize,
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
    /// Build (append) an external Needle / RAP index fragment over Parquet files.
    Index {
        #[arg(long, default_value = "data/parquet")]
        data: PathBuf,
        #[arg(long, default_value = "data/rap-index")]
        index: PathBuf,
        #[arg(long, default_value = "frag-001")]
        fragment: String,
        #[arg(long, default_value_t = 16)]
        buckets: u32,
        #[arg(long, default_value_t = false)]
        covering: bool,
        /// Build a secondary index on this dimension (e.g. track_uri). No Parquet rewrite.
        #[arg(long)]
        secondary: Option<String>,
        #[arg(long)]
        file: Vec<PathBuf>,
        /// Key column to index (repeatable). Default: user_id.
        #[arg(long, value_name = "NAME")]
        key_column: Vec<String>,
        /// Value column to store alongside the key (repeatable). Empty = builder default.
        #[arg(long, value_name = "NAME")]
        value_column: Vec<String>,
    },
    /// Point-query a key through the Needle / RAP index (filters, JSON, secondary range).
    ///
    /// Compound keys: join parts with `||` (encoded as U+001F), or pass an already-encoded
    /// string. Secondary range queries use `--dimension` with `--range-start` / `--range-end`
    /// and do not need a positional key.
    Query {
        /// Lookup key (optional when using `--dimension --range-start --range-end`).
        #[arg(value_name = "KEY")]
        key: Option<String>,
        #[arg(long, default_value = "data/rap-index")]
        index: PathBuf,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Pagination offset into the index row list (article: value_count).
        #[arg(long, default_value_t = 0)]
        offset: usize,
        #[arg(long, default_value_t = false)]
        verbose: bool,
        /// Query via secondary dimension (e.g. --dimension track_uri).
        #[arg(long)]
        dimension: Option<String>,
        /// Inclusive secondary range start (requires `--dimension` and `--range-end`).
        #[arg(long, value_name = "S")]
        range_start: Option<String>,
        /// Inclusive secondary range end (requires `--dimension` and `--range-start`).
        #[arg(long, value_name = "E")]
        range_end: Option<String>,
        /// Optional HTTP base URL for ranged reads (start with `rap serve`).
        #[arg(long)]
        http: Option<String>,
        /// Data dir for HTTP proof / secondary file resolution.
        #[arg(long, default_value = "data/parquet")]
        data: PathBuf,
        /// Project these columns (comma-separated or repeat the flag).
        #[arg(long, value_name = "NAME")]
        columns: Vec<String>,
        /// Inclusive start time (RFC3339, YYYY-MM-DD as UTC midnight, or integer ms).
        #[arg(long, value_name = "TIME")]
        since: Option<String>,
        /// Inclusive end time (RFC3339, YYYY-MM-DD as UTC end of day 23:59:59.999, or integer ms).
        #[arg(long, value_name = "TIME")]
        until: Option<String>,
        /// Skip page reads; return covering aggregates only.
        #[arg(long, default_value_t = false)]
        covering_only: bool,
        /// Keep entries whose covering listen_count is at least N.
        #[arg(long, value_name = "N")]
        min_listens: Option<u64>,
        /// Output format (`table` or `json`).
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
        /// Shorthand for `--format json` (stdout is a single JSON object).
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Explain a lookup: files, pages, and estimated Range GETs (no HTTP).
    Explain {
        /// Lookup key (compound: join parts with `||`).
        #[arg(value_name = "KEY")]
        key: String,
        #[arg(long, default_value = "data/rap-index")]
        index: PathBuf,
        /// Project these columns (comma-separated or repeat the flag).
        #[arg(long, value_name = "NAME")]
        columns: Vec<String>,
        /// Inclusive start time (RFC3339, YYYY-MM-DD as UTC midnight, or integer ms).
        #[arg(long, value_name = "TIME")]
        since: Option<String>,
        /// Inclusive end time (RFC3339, YYYY-MM-DD as UTC end of day 23:59:59.999, or integer ms).
        #[arg(long, value_name = "TIME")]
        until: Option<String>,
        /// Skip page reads; covering aggregates only.
        #[arg(long, default_value_t = false)]
        covering_only: bool,
        /// Keep entries whose covering listen_count is at least N.
        #[arg(long, value_name = "N")]
        min_listens: Option<u64>,
        /// Output format (`table` or `json`).
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
        /// Shorthand for `--format json`.
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Print index fragment stats (registry + manifests only; does not load buckets).
    Stats {
        #[arg(long, default_value = "data/rap-index")]
        index: PathBuf,
        /// Output format (`table` or `json`).
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
        /// Shorthand for `--format json`.
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Compare naive full scan vs RAP for a key.
    Bench {
        key: String,
        #[arg(long, default_value = "data/parquet")]
        data: PathBuf,
        #[arg(long, default_value = "data/rap-index")]
        index: PathBuf,
        #[arg(long, default_value_t = 5)]
        rounds: usize,
    },
    /// Serve Parquet/prepared files over HTTP with Range support.
    Serve {
        #[arg(long, default_value = "data/parquet")]
        root: PathBuf,
        /// Hold the server this many seconds (0 = until Ctrl-C / process end via demo).
        #[arg(long, default_value_t = 30)]
        seconds: u64,
    },
    /// End-to-end demo: generate → index → query → bench.
    Demo {
        /// Data root (parquet + rap-index). Alias: --data-dir. Env: RAP_DATA_DIR.
        #[arg(long, default_value = "data", visible_alias = "data-dir", env = "RAP_DATA_DIR")]
        root: PathBuf,
        #[arg(long, default_value = "user_0042")]
        key: String,
        #[arg(long, default_value_t = 500)]
        users: usize,
        #[arg(long, default_value_t = 80)]
        listens_per_user: usize,
        #[arg(long, default_value_t = 4)]
        files: usize,
    },
    /// Full article-coverage demo (all write modes + secondary + HTTP Range).
    #[command(name = "demo-full")]
    DemoFull {
        /// Data root (per-mode subdirs). Alias: --data-dir. Env: RAP_DATA_DIR.
        #[arg(long, default_value = "data", visible_alias = "data-dir", env = "RAP_DATA_DIR")]
        root: PathBuf,
        #[arg(long, default_value = "user_0042")]
        key: String,
        #[arg(long, default_value_t = 64)]
        users: usize,
        #[arg(long, default_value_t = 20)]
        listens_per_user: usize,
        #[arg(long, default_value_t = 2)]
        files: usize,
    },
    /// Start local MinIO (tools/minio) and ensure `rap-lake` bucket exists.
    #[command(name = "minio-up")]
    MinioUp,
    /// Bulk-generate lake-shaped Parquet into MinIO + stream RAP index (local only).
    #[command(name = "lake-generate")]
    LakeGenerate {
        #[arg(long, default_value_t = 100_000)]
        files: usize,
        #[arg(long, default_value = "rap-lake")]
        bucket: String,
        #[arg(long, default_value_t = 100)]
        days: usize,
        #[arg(long, default_value_t = 100)]
        hash_buckets: usize,
        #[arg(long, default_value_t = 4)]
        rows_per_file: usize,
        #[arg(long, default_value_t = 96)]
        parallelism: usize,
        #[arg(long, default_value = "data/rap-lake-index")]
        index: PathBuf,
        #[arg(long, default_value_t = 256)]
        index_buckets: u32,
        #[arg(long, default_value = "lake-frag")]
        fragment: String,
        /// Fat lake: few large multi-page files (prefix `fat/`, real OffsetIndex pages).
        #[arg(long, default_value_t = false)]
        fat: bool,
        #[arg(long, default_value_t = 64)]
        page_rows: usize,
        #[arg(long, default_value_t = 32)]
        listens_per_user: usize,
        #[arg(long, default_value = "")]
        prefix: String,
    },
    /// Fat lake: multi-page files with OffsetIndex (MinIO only). High --files for 100k–200k scale.
    #[command(name = "lake-generate-fat")]
    LakeGenerateFat {
        #[arg(long, default_value_t = 16)]
        files: usize,
        #[arg(long, default_value_t = 250_000)]
        rows_per_file: usize,
        #[arg(long, default_value_t = 64)]
        page_rows: usize,
        #[arg(long, default_value_t = 32)]
        listens_per_user: usize,
        #[arg(long, default_value = "rap-lake")]
        bucket: String,
        #[arg(long, default_value = "data/rap-lake-index-fat")]
        index: PathBuf,
        #[arg(long, default_value_t = 16)]
        parallelism: usize,
        #[arg(long, default_value = "lake-fat")]
        fragment: String,
        #[arg(long, default_value = "fat")]
        prefix: String,
        /// 0 = auto (1 for small, 200 for large).
        #[arg(long, default_value_t = 0)]
        days: usize,
        /// 0 = auto (files for small, 100 for large).
        #[arg(long, default_value_t = 0)]
        hash_buckets: usize,
        /// 0 = auto (64 for small, 2048 for large).
        #[arg(long, default_value_t = 0)]
        index_buckets: u32,
    },
    /// Re-index by listing MinIO + reading objects (footer/key column).
    #[command(name = "lake-index")]
    LakeIndex {
        #[arg(long, default_value = "rap-lake")]
        bucket: String,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value = "data/rap-lake-index")]
        index: PathBuf,
        #[arg(long, default_value = "lake-frag")]
        fragment: String,
        #[arg(long, default_value_t = 256)]
        index_buckets: u32,
        #[arg(long)]
        max_objects: Option<usize>,
    },
    /// Point query a key via RAP index + MinIO S3 Range GETs.
    #[command(name = "lake-query")]
    LakeQuery {
        key: String,
        #[arg(long, default_value = "data/rap-lake-index")]
        index: PathBuf,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// Bench RAP point queries over MinIO (latency / Range GETs / bytes).
    #[command(name = "lake-bench")]
    LakeBench {
        #[arg(long, default_value = "data/rap-lake-index")]
        index: PathBuf,
        #[arg(long)]
        key: Vec<String>,
        #[arg(long, default_value_t = 5)]
        rounds: usize,
    },
    /// Concurrent point-query stress against MinIO RAP lake (read-only).
    #[command(name = "lake-stress")]
    LakeStress {
        #[arg(long, default_value = "data/rap-lake-index")]
        index: PathBuf,
        #[arg(long, default_value_t = 50_000)]
        queries: usize,
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        #[arg(long, default_value_t = 100)]
        warmup: usize,
        /// Verify every Nth query (covering listen_count / non-empty). 0 = off.
        #[arg(long, default_value_t = 100)]
        verify_every: usize,
        /// Optional open-loop sustained seconds after the fixed batch (0 = skip).
        #[arg(long, default_value_t = 0)]
        duration_secs: u64,
        #[arg(long)]
        key_space: Option<u64>,
        #[arg(long, default_value_t = 4)]
        buckets_per_wave: usize,
        #[arg(long, default_value = "data/fat-lake-query-stress.json")]
        out: PathBuf,
        #[arg(long, default_value_t = 32)]
        limit: usize,
    },
    /// One-shot local lake demo: minio-up → generate → query → bench.
    #[command(name = "lake-demo")]
    LakeDemo {
        #[arg(long, default_value_t = 1_000)]
        files: usize,
        #[arg(long, default_value = "data/rap-lake-index")]
        index: PathBuf,
    },
    /// Fat lake one-shot: minio-up → generate fat files → query → bench vs full GET.
    #[command(name = "lake-demo-fat")]
    LakeDemoFat {
        #[arg(long, default_value_t = 8)]
        files: usize,
        #[arg(long, default_value_t = 200_000)]
        rows_per_file: usize,
        #[arg(long, default_value = "data/rap-lake-index-fat")]
        index: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Generate {
            out,
            users,
            listens_per_user,
            files,
            mode,
            one_page_per_key,
            rows_per_row_group,
            seed,
        } => {
            let opts = WriterOptions {
                out_dir: out.clone(),
                num_users: users,
                listens_per_user,
                num_files: files,
                mode,
                rows_per_row_group,
                write_page_index: true,
                seed,
                one_page_per_key,
            };
            let t0 = Instant::now();
            let paths = write_sample_dataset(&opts)?;
            println!(
                "Wrote {} file(s) in {:?} (mode={:?}, one_page_per_key={}, users={}, listens/user={})",
                paths.len(),
                t0.elapsed(),
                mode,
                one_page_per_key || matches!(mode, WriteMode::OnePagePerKey),
                users,
                listens_per_user
            );
            for p in &paths {
                let sz = std::fs::metadata(p)?.len();
                println!("  {} ({} bytes)", p.display(), sz);
                if matches!(
                    mode,
                    WriteMode::ZstdFrames | WriteMode::Aligned | WriteMode::Interleaved
                ) {
                    println!("    {}", parquet_lowlevel::verify_parquet_file(p)?);
                }
            }
        }
        Cmd::Index {
            data,
            index,
            fragment,
            buckets,
            covering,
            secondary,
            file,
            key_column,
            value_column,
        } => {
            let files = if file.is_empty() {
                collect_parquet(&data)?
            } else {
                file
            };
            if files.is_empty() {
                bail!("no Parquet files found under {}", data.display());
            }
            std::fs::create_dir_all(&index)?;
            let key_column = if key_column.is_empty() {
                vec!["user_id".to_string()]
            } else {
                key_column
            };
            let builder = IndexBuilder::new(&index, buckets)
                .with_covering(covering)
                .with_key_columns(key_column.clone())
                .with_value_columns(value_column);
            let t0 = Instant::now();
            let frag = builder.build_fragment(
                &files,
                &fragment,
                Some("RAP external index fragment"),
            )?;
            println!(
                "Indexed {} file(s) → {} in {:?} (buckets={}, covering={}, key_columns={:?})",
                files.len(),
                frag.display(),
                t0.elapsed(),
                buckets,
                covering,
                key_column
            );
            if let Some(dim) = secondary {
                let t1 = Instant::now();
                let sec = secondary::build_secondary(&index, &fragment, &dim, buckets)?;
                println!(
                    "Secondary index dim={dim} → {} in {:?}",
                    sec.display(),
                    t1.elapsed()
                );
            }
            let loaded = load_index(&index)?;
            println!(
                "Index now: {} keys, {} entries, {} files",
                loaded.num_keys(),
                loaded.num_entries(),
                loaded.files.len()
            );
        }
        Cmd::Query {
            key,
            index,
            limit,
            offset,
            verbose,
            dimension,
            range_start,
            range_end,
            http,
            data,
            columns,
            since,
            until,
            covering_only,
            min_listens,
            format,
            json,
        } => {
            let out = resolve_format(format, json);
            run_query_cmd(
                key,
                index,
                limit,
                offset,
                verbose,
                dimension,
                range_start,
                range_end,
                http,
                data,
                columns,
                since,
                until,
                covering_only,
                min_listens,
                out,
            )?;
        }
        Cmd::Explain {
            key,
            index,
            columns,
            since,
            until,
            covering_only,
            min_listens,
            format,
            json,
        } => {
            let out = resolve_format(format, json);
            let key = encode_cli_key(&key);
            let idx = load_index_for_keys(&index, &[key.to_string()])?;
            let querier = RapQuerier::new(idx);
            let qopts = build_query_options(
                0,
                None,
                None,
                &columns,
                since.as_deref(),
                until.as_deref(),
                covering_only,
                min_listens,
            )?;
            let expl = querier.explain(&key, &qopts)?;
            print_explain(&expl, out)?;
        }
        Cmd::Stats {
            index,
            format,
            json,
        } => {
            run_stats(&index, resolve_format(format, json))?;
        }
        Cmd::Bench {
            key,
            data,
            index,
            rounds,
        } => {
            let files = collect_parquet(&data)?;
            let idx = load_index(&index)?;
            let querier = RapQuerier::new(idx);

            let _ = querier.query(&key)?;
            let _ = naive_scan(&files, &key)?;

            let mut rap_times = Vec::new();
            let mut naive_times = Vec::new();
            let mut rap_count = 0usize;
            let mut naive_count = 0usize;

            for _ in 0..rounds {
                let r = querier.query(&key)?;
                rap_count = r.rows.len();
                rap_times.push(r.timings.total);

                let (rows, d) = naive_scan(&files, &key)?;
                naive_count = rows.len();
                naive_times.push(d);
            }

            let rap_avg = avg_duration(&rap_times);
            let naive_avg = avg_duration(&naive_times);
            println!("Bench key={key} rounds={rounds}");
            println!(
                "  RAP:   avg {:?}  (rows={rap_count}, pages≈{}, bytes≈{})",
                rap_avg,
                querier.query(&key)?.timings.pages_touched,
                querier.query(&key)?.timings.bytes_ranged
            );
            println!("  Naive: avg {:?}  (rows={naive_count})", naive_avg);
            if rap_avg.as_nanos() > 0 {
                let speedup = naive_avg.as_secs_f64() / rap_avg.as_secs_f64();
                println!("  Speedup: {speedup:.2}x (naive / RAP)");
            }
            if rap_count != naive_count {
                println!(
                    "  WARNING: row count mismatch RAP={rap_count} naive={naive_count}"
                );
            }
        }
        Cmd::Serve { root, seconds } => {
            let server = RangeHttpServer::start(&root)?;
            println!(
                "HTTP Range server at {} serving {} for {seconds}s",
                server.base_url(),
                root.display()
            );
            std::thread::sleep(std::time::Duration::from_secs(seconds));
            println!("Stopping server.");
            server.stop();
        }
        Cmd::Demo {
            root,
            key,
            users,
            listens_per_user,
            files,
        } => {
            run_demo(&root, &key, users, listens_per_user, files)?;
        }
        Cmd::DemoFull {
            root,
            key,
            users,
            listens_per_user,
            files,
        } => {
            run_demo_full(&root, &key, users, listens_per_user, files)?;
        }
        Cmd::MinioUp => {
            lake::minio_up()?;
        }
        Cmd::LakeGenerate {
            files,
            bucket,
            days,
            hash_buckets,
            rows_per_file,
            parallelism,
            index,
            index_buckets,
            fragment,
            fat,
            page_rows,
            listens_per_user,
            prefix,
        } => {
            let mut index = index;
            let mut prefix = prefix;
            let mut files = files;
            let mut rows_per_file = rows_per_file;
            let mut parallelism = parallelism;
            let mut fragment = fragment;
            if fat {
                if index == PathBuf::from("data/rap-lake-index") {
                    index = PathBuf::from("data/rap-lake-index-fat");
                }
                if prefix.is_empty() {
                    prefix = "fat".into();
                }
                if files == 100_000 {
                    files = 16;
                }
                if rows_per_file == 4 {
                    rows_per_file = 250_000;
                }
                if parallelism == 96 {
                    parallelism = 4;
                }
                if fragment == "lake-frag" {
                    fragment = "lake-fat".into();
                }
            }
            let opts = LakeGenerateOpts {
                files,
                bucket,
                days,
                hash_buckets,
                rows_per_file,
                parallelism,
                index_dir: index,
                fragment_id: fragment,
                index_buckets,
                fat,
                page_rows,
                listens_per_user,
                prefix,
                ..LakeGenerateOpts::default()
            };
            let man = lake::lake_generate(&opts)?;
            println!(
                "objects={} bytes={} fat={} page_rows={} index={}",
                man.objects, man.bytes_uploaded, man.fat, man.page_rows, man.index_dir
            );
            println!("sample_keys={:?}", man.sample_keys);
            if !man.file_sizes.is_empty() {
                println!("file_sizes={:?}", man.file_sizes);
            }
        }
        Cmd::LakeGenerateFat {
            files,
            rows_per_file,
            page_rows,
            listens_per_user,
            bucket,
            index,
            parallelism,
            fragment,
            prefix,
            days,
            hash_buckets,
            index_buckets,
        } => {
            let opts = LakeGenerateOpts {
                files,
                bucket,
                days,
                hash_buckets,
                rows_per_file,
                parallelism,
                index_dir: index,
                fragment_id: fragment,
                index_buckets,
                fat: true,
                page_rows,
                listens_per_user,
                prefix,
                ..LakeGenerateOpts::default()
            };
            let man = lake::lake_generate(&opts)?;
            println!(
                "FAT objects={} bytes={} page_rows={} pages/file={} index={}",
                man.objects, man.bytes_uploaded, man.page_rows, man.pages_per_file, man.index_dir
            );
            println!("sample_keys={:?}", man.sample_keys);
            if man.file_sizes.len() <= 16 {
                println!("file_sizes={:?}", man.file_sizes);
            } else if !man.file_sizes.is_empty() {
                let min_sz = *man.file_sizes.iter().min().unwrap();
                let max_sz = *man.file_sizes.iter().max().unwrap();
                let avg = man.bytes_uploaded as f64 / man.objects.max(1) as f64;
                println!(
                    "file_size min/avg/max={}/{:.0}/{} (n={})",
                    min_sz, avg, max_sz, man.file_sizes.len()
                );
            }
        }
        Cmd::LakeIndex {
            bucket,
            prefix,
            index,
            fragment,
            index_buckets,
            max_objects,
        } => {
            lake::lake_index_from_bucket(
                &bucket,
                &prefix,
                &index,
                &fragment,
                index_buckets,
                max_objects,
            )?;
        }
        Cmd::LakeQuery { key, index, limit } => {
            let report = lake::lake_query(&index, &key, limit)?;
            print_query_result(&report.result, limit, true, std::time::Duration::ZERO);
            println!(
                "  S3 Range GETs (page_locs): {}  bytes_ranged={}  file_size={}  ratio={:.4}%  coalesced={} span={}  naive_full_get={} in {:.2}ms  full_gets={}",
                report.range_requests_demo,
                report.bytes_ranged,
                report.file_size,
                report.bytes_ratio * 100.0,
                report.coalesced_range_requests,
                report.coalesced_span,
                report.naive_full_get_bytes,
                report.naive_full_get_ms,
                report.s3_stats.full_gets
            );
        }
        Cmd::LakeBench { index, key, rounds } => {
            let keys = if key.is_empty() {
                // Prefer sample keys from lake_manifest.json
                let man_path = index.join("lake_manifest.json");
                if man_path.exists() {
                    let v: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(man_path)?)?;
                    v.get("sample_keys")
                        .and_then(|x| x.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_else(|| vec!["user_0".into(), "user_42".into()])
                } else {
                    vec!["user_0".into(), "user_42".into()]
                }
            } else {
                key
            };
            let report = lake::lake_bench(&index, &keys, rounds)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Cmd::LakeStress {
            index,
            queries,
            concurrency,
            seed,
            warmup,
            verify_every,
            duration_secs,
            key_space,
            buckets_per_wave,
            out,
            limit,
        } => {
            let report = lake::lake_stress(&lake::LakeStressOpts {
                index_dir: index,
                queries,
                concurrency,
                seed,
                warmup,
                verify_every,
                duration_secs,
                key_space,
                buckets_per_wave,
                out_json: Some(out),
                limit,
            })?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Cmd::LakeDemo { files, index } => {
            lake::lake_demo(files, &index)?;
        }
        Cmd::LakeDemoFat {
            files,
            rows_per_file,
            index,
        } => {
            lake::lake_demo_fat(files, rows_per_file, &index)?;
        }
    }
    Ok(())
}

fn run_query_cmd(
    key: Option<String>,
    index: PathBuf,
    limit: usize,
    offset: usize,
    verbose: bool,
    dimension: Option<String>,
    range_start: Option<String>,
    range_end: Option<String>,
    http: Option<String>,
    data: PathBuf,
    columns: Vec<String>,
    since: Option<String>,
    until: Option<String>,
    covering_only: bool,
    min_listens: Option<u64>,
    out: OutputFormat,
) -> Result<()> {
    let key = key.filter(|s| !s.is_empty());
    let range_query = range_start.is_some() || range_end.is_some();
    if range_query {
        if dimension.is_none() {
            bail!("--range-start/--range-end require --dimension");
        }
        if range_start.is_none() || range_end.is_none() {
            bail!("secondary range query requires both --range-start and --range-end");
        }
    }
    if let Some(dim) = dimension {
        if !range_query && key.is_none() {
            bail!("query requires a KEY, or --dimension with --range-start and --range-end");
        }
        return run_secondary_query(
            &index,
            &dim,
            key.as_deref(),
            offset,
            limit,
            verbose,
            range_start.as_deref(),
            range_end.as_deref(),
            out,
        );
    }
    let Some(raw_key) = key else {
        bail!("query requires a KEY, or --dimension with --range-start and --range-end");
    };
    let key = encode_cli_key(&raw_key);
    let t_load = Instant::now();
    let idx = load_index_for_keys(&index, &[key.to_string()])?;
    let load_ms = t_load.elapsed();
    let querier = RapQuerier::new(idx);
    let qopts = build_query_options(
        offset,
        Some(limit),
        http.clone(),
        &columns,
        since.as_deref(),
        until.as_deref(),
        covering_only,
        min_listens,
    )?;
    let result = querier.query_with(&key, &qopts)?;
    match out {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string(&query_result_json(&result, load_ms))?
            );
        }
        OutputFormat::Table => {
            print_query_result(&result, limit, verbose, load_ms);
        }
    }

    if let Some(base) = http {
        let ranges = collect_demo_ranges(&querier, &key)?;
        let mut by_file: HashMap<PathBuf, Vec<std::ops::Range<u64>>> = HashMap::new();
        for (p, r) in ranges {
            by_file.entry(p).or_default().push(r);
        }
        let _ = data;
        let emit = |line: String| {
            if out == OutputFormat::Json {
                eprintln!("{line}");
            } else {
                println!("{line}");
            }
        };
        emit(format!("  HTTP Range proof (base={base}):"));
        for (path, rs) in by_file {
            match prove_http_matches_local(&path, &base, &rs) {
                Ok(pr) => emit(format!(
                    "    OK {} - {} ranges, {} bytes match ({})",
                    path.file_name().unwrap_or_default().to_string_lossy(),
                    pr.ranges,
                    pr.bytes_compared,
                    pr.url
                )),
                Err(e) => emit(format!("    FAIL {}: {e:#}", path.display())),
            }
        }
    }
    Ok(())
}

fn run_secondary_query(
    index: &PathBuf,
    dim: &str,
    key: Option<&str>,
    offset: usize,
    limit: usize,
    verbose: bool,
    range_start: Option<&str>,
    range_end: Option<&str>,
    out: OutputFormat,
) -> Result<()> {
    let t0 = Instant::now();
    let sec = secondary::load_secondary_any(index, dim)?;
    if let (Some(start), Some(end)) = (range_start, range_end) {
        let refs = sec.lookup_range(start, end);
        match out {
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "dimension": dim,
                        "range_start": start,
                        "range_end": end,
                        "count": refs.len(),
                        "refs": refs,
                    }))?
                );
            }
            OutputFormat::Table => {
                println!(
                    "Secondary range dim={dim} [{start} .. {end}] → {} ref(s) ({} keys in index) in {:?}",
                    refs.len(),
                    sec.num_keys(),
                    t0.elapsed()
                );
                for r in refs.iter().skip(offset).take(limit) {
                    println!(
                        "    sec={} primary={} file={} rows={}",
                        r.key,
                        r.primary_key,
                        r.file,
                        r.row_numbers.len()
                    );
                }
            }
        }
        return Ok(());
    }

    let key = key.unwrap_or("");
    let refs = sec.lookup_exact(key);
    if out == OutputFormat::Json {
        let mut body = serde_json::json!({
            "dimension": dim,
            "key": key,
            "count": refs.len(),
            "refs": refs,
        });
        if refs.is_empty() {
            let end = format!("{key}\u{ffff}");
            let range_refs = sec.lookup_range(key, &end);
            body["range_fallback"] = serde_json::json!({
                "start": key,
                "count": range_refs.len(),
                "refs": range_refs,
            });
        } else {
            let rows = decode_secondary_rows(&sec, refs, key, offset, limit)?;
            body["rows"] = serde_json::Value::Array(
                rows.into_iter().map(serde_json::Value::String).collect(),
            );
        }
        println!("{}", serde_json::to_string(&body)?);
        return Ok(());
    }
    println!(
        "Secondary query dim={dim} key={key} → {} ref(s) ({} keys in index) in {:?}",
        refs.len(),
        sec.num_keys(),
        t0.elapsed()
    );
    if refs.is_empty() {
        // Try a prefix range demo when exact miss.
        let end = format!("{key}\u{ffff}");
        let range_refs = sec.lookup_range(key, &end);
        println!(
            "  (exact miss - range [{key} .. ] → {} refs)",
            range_refs.len()
        );
        for r in range_refs.iter().skip(offset).take(limit) {
            println!(
                "    sec={} primary={} file={} rows={}",
                r.key,
                r.primary_key,
                r.file,
                r.row_numbers.len()
            );
        }
        return Ok(());
    }

    let rows = decode_secondary_rows(&sec, refs, key, offset, limit)?;
    println!("  decoded {} matching row(s) (showing up to {limit}):", rows.len());
    for line in rows.iter().take(limit) {
        println!("    {line}");
    }
    if verbose {
        println!("  secondary refs:");
        for r in refs.iter().take(8) {
            println!(
                "    primary={} file={} rows={:?}…",
                r.primary_key,
                r.file,
                &r.row_numbers[..r.row_numbers.len().min(4)]
            );
        }
    }
    Ok(())
}

fn decode_secondary_rows(
    sec: &secondary::SecondaryIndex,
    refs: &[secondary::SecondaryRef],
    key: &str,
    offset: usize,
    limit: usize,
) -> Result<Vec<String>> {
    // Decode via primary Parquet using row numbers from secondary.
    let _entries = refs_to_primary_entries(refs);
    let mut rows = Vec::new();
    for r in refs.iter().skip(offset).take(limit.max(1).saturating_mul(4)) {
        let path = sec
            .files
            .get(r.file as usize)
            .with_context(|| format!("sec file ordinal {}", r.file))?;
        if !path.exists() {
            continue;
        }
        let file = std::fs::File::open(path)?;
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let total = builder.metadata().file_metadata().num_rows() as u64;
        let mut sorted = r.row_numbers.clone();
        sorted.sort_unstable();
        let mut selectors = Vec::new();
        let mut pos = 0u64;
        for &row in &sorted {
            if row > pos {
                selectors.push(parquet::arrow::arrow_reader::RowSelector::skip(
                    (row - pos) as usize,
                ));
            }
            selectors.push(parquet::arrow::arrow_reader::RowSelector::select(1));
            pos = row + 1;
        }
        if pos < total {
            selectors.push(parquet::arrow::arrow_reader::RowSelector::skip(
                (total - pos) as usize,
            ));
        }
        let selection = parquet::arrow::arrow_reader::RowSelection::from(selectors);
        let reader = builder.with_row_selection(selection).build()?;
        for batch in reader {
            let batch = batch?;
            if let Some(tracks) = batch.column_by_name("track_uri") {
                let tracks = tracks
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                let users = batch
                    .column_by_name("user_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                let durs = batch
                    .column_by_name("duration_ms")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    if tracks.value(i) == key {
                        rows.push(format!(
                            "user={} track={} dur={}",
                            users.value(i),
                            tracks.value(i),
                            durs.value(i)
                        ));
                    }
                }
            }
        }
    }
    Ok(rows)
}

fn run_demo(
    root: &PathBuf,
    key: &str,
    users: usize,
    listens_per_user: usize,
    files: usize,
) -> Result<()> {
    let parquet_dir = root.join("parquet");
    let index_dir = root.join("rap-index");

    println!("=== RAP demo ===");
    println!("1) Generate sorted listening-history Parquet…");
    if parquet_dir.exists() {
        std::fs::remove_dir_all(&parquet_dir)?;
    }
    std::fs::create_dir_all(&parquet_dir)?;
    let opts = WriterOptions {
        out_dir: parquet_dir.clone(),
        num_users: users,
        listens_per_user,
        num_files: files.max(1),
        mode: WriteMode::Sorted,
        rows_per_row_group: 1024,
        write_page_index: true,
        seed: 42,
        one_page_per_key: false,
    };
    let paths = write_sample_dataset(&opts)?;
    println!("   {} files under {}", paths.len(), parquet_dir.display());

    println!("2) Build external RAP index (hash-bucketed, covering)…");
    if index_dir.exists() {
        std::fs::remove_dir_all(&index_dir)?;
    }
    std::fs::create_dir_all(&index_dir)?;
    let builder = IndexBuilder::new(&index_dir, 16).with_covering(true);
    builder.build_fragment(&paths, "frag-001", Some("demo fragment"))?;
    let idx = load_index(&index_dir)?;
    println!(
        "   keys={} entries={} files={}",
        idx.num_keys(),
        idx.num_entries(),
        idx.files.len()
    );

    println!("3) Point query key={key}…");
    let querier = RapQuerier::new(idx);
    let result = querier.query(key)?;
    print_query_result(&result, 5, true, std::time::Duration::ZERO);

    println!("4) Bench RAP vs naive scan…");
    let _ = querier.query(key)?;
    let _ = naive_scan(&paths, key)?;
    let mut rap = Vec::new();
    let mut naive = Vec::new();
    for _ in 0..5 {
        rap.push(querier.query(key)?.timings.total);
        naive.push(naive_scan(&paths, key)?.1);
    }
    println!(
        "   RAP avg {:?} | Naive avg {:?} | speedup {:.2}x",
        avg_duration(&rap),
        avg_duration(&naive),
        avg_duration(&naive).as_secs_f64() / avg_duration(&rap).as_secs_f64().max(1e-12)
    );
    println!("=== done ===");
    Ok(())
}

fn run_demo_full(
    root: &PathBuf,
    key: &str,
    users: usize,
    listens_per_user: usize,
    files: usize,
) -> Result<()> {
    println!("=== RAP demo-full (all article techniques) ===");
    let modes = [
        WriteMode::Sorted,
        WriteMode::OnePagePerKey,
        WriteMode::Blob,
        WriteMode::ZstdFrames,
        WriteMode::Aligned,
        WriteMode::Interleaved,
        WriteMode::Cogrouped,
    ];

    for mode in modes {
        let dir = root.join(format!("full_{:?}", mode).to_lowercase());
        let pq = dir.join("parquet");
        let ix = dir.join("rap-index");
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&pq)?;
        println!("\n--- mode={:?} ---", mode);
        let opts = WriterOptions {
            out_dir: pq.clone(),
            num_users: users,
            listens_per_user,
            num_files: files.max(1),
            mode,
            rows_per_row_group: 256,
            write_page_index: true,
            seed: 42,
            one_page_per_key: false,
        };
        let paths = write_sample_dataset(&opts)?;
        println!("  wrote {} parquet file(s)", paths.len());
        if matches!(
            mode,
            WriteMode::ZstdFrames | WriteMode::Aligned | WriteMode::Interleaved
        ) {
            for p in &paths {
                let proof = parquet_lowlevel::verify_parquet_file(p)?;
                let n = parquet_lowlevel::try_arrow_read(p)?;
                println!("  custom writer {} - {proof}; arrow rows={n}", p.file_name().unwrap().to_string_lossy());
                // Must not require sidecar data files.
                for ext in ["rapz", "rapi"] {
                    let side = p.with_extension(ext);
                    if side.exists() {
                        println!("  WARNING: leftover sidecar {}", side.display());
                    }
                }
            }
        }

        let builder = IndexBuilder::new(&ix, 8).with_covering(true);
        builder.build_fragment(&paths, "frag-001", Some(&format!("{mode:?}")))?;

        if matches!(
            mode,
            WriteMode::Sorted | WriteMode::OnePagePerKey | WriteMode::ZstdFrames
        ) {
            secondary::build_secondary(&ix, "frag-001", "track_uri", 8)?;
            println!("  secondary track_uri built");
        }

        let idx = load_index(&ix)?;
        let querier = RapQuerier::new(idx);
        let res = querier.query_with(
            key,
            &QueryOptions {
                offset: 0,
                limit: Some(5),
                http_base: None,
                ..Default::default()
            },
        )?;
        println!(
            "  query {key}: rows={} values={} pages={} bytes={} index_page_locs={} prepared={} aligned_hint={}",
            res.rows.len(),
            res.total_value_count,
            res.timings.pages_touched,
            res.timings.bytes_ranged,
            res.timings.used_index_page_locs,
            res.timings.used_prepared_layout,
            querier
                .index
                .lookup(key)
                .first()
                .and_then(|e| e.aligned)
                .unwrap_or(false)
        );

        // Pagination sample.
        let page = querier.query_with(
            key,
            &QueryOptions {
                offset: 5,
                limit: Some(5),
                http_base: None,
                ..Default::default()
            },
        )?;
        println!(
            "  pagination offset=5 limit=5 → {} rows (total_value_count={})",
            page.rows.len(),
            page.total_value_count
        );
    }

    // HTTP Range proof on one-page-per-key dataset.
    println!("\n--- HTTP Range proof ---");
    let opp = root.join("full_onepageperkey");
    let pq = opp.join("parquet");
    let ix = opp.join("rap-index");
    let idx = load_index(&ix)?;
    let querier = RapQuerier::new(idx);
    let server = RangeHttpServer::start(&pq)?;
    let base = server.base_url();
    println!("  server {}", base);
    let ranges = collect_demo_ranges(&querier, key)?;
    let mut by_file: HashMap<PathBuf, Vec<std::ops::Range<u64>>> = HashMap::new();
    for (p, r) in ranges {
        by_file.entry(p).or_default().push(r);
    }
    for (path, rs) in &by_file {
        match prove_http_matches_local(path, &base, rs) {
            Ok(pr) => println!(
                "  PROVED {} - {} ranges / {} bytes identical over HTTP Range",
                path.file_name().unwrap_or_default().to_string_lossy(),
                pr.ranges,
                pr.bytes_compared
            ),
            Err(e) => println!("  FAIL {}: {e:#}", path.display()),
        }
    }
    // Query via HTTP.
    let res = querier.query_with(
        key,
        &QueryOptions {
            offset: 0,
            limit: Some(3),
            http_base: Some(base.clone()),
            ..Default::default()
        },
    )?;
    println!(
        "  HTTP query rows={} ranged_read {:?}",
        res.rows.len(),
        res.timings.ranged_read_demo
    );
    server.stop();

    // Secondary exact lookup sample.
    println!("\n--- Secondary index (track_uri) ---");
    let sec_root = root.join("full_sorted").join("rap-index");
    if let Ok(sec) = secondary::load_secondary_any(&sec_root, "track_uri") {
        // Pick a track from a known user via primary query.
        let idx = load_index(&sec_root)?;
        let q = RapQuerier::new(idx);
        let sample = q.query(key)?;
        if let Some(track) = sample.rows.first().map(|r| r.track_uri.clone()) {
            let refs = sec.lookup_exact(&track);
            println!(
                "  exact lookup {track} → {} ref(s) across {} secondary keys",
                refs.len(),
                sec.num_keys()
            );
            // Range: prefix spotify:track:000
            let range = sec.lookup_range("spotify:track:00000000", "spotify:track:00000099");
            println!(
                "  range spotify:track:00000000..=00000099 → {} refs",
                range.len()
            );
        }
    }

    println!("\n=== demo-full done ===");
    Ok(())
}

fn print_query_result(
    result: &rap::query::QueryResult,
    limit: usize,
    verbose: bool,
    index_load: std::time::Duration,
) {
    let t = &result.timings;
    println!(
        "Query key={} → {} row(s) (value_count={} offset={} limit={:?})",
        result.key,
        result.rows.len(),
        result.total_value_count,
        result.offset,
        result.limit
    );
    if index_load.as_nanos() > 0 {
        println!("  index load:          {:?}", index_load);
    }
    println!("  index lookup:        {:?}", t.index_lookup);
    println!("  metadata resolve:    {:?}", t.metadata_resolve);
    println!(
        "  ranged-read (demo):  {:?}  ({} bytes across {} page ranges, {} files)",
        t.ranged_read_demo, t.bytes_ranged, t.pages_touched, t.files_touched
    );
    println!("  decode+extract:      {:?}", t.decode_extract);
    println!("  total (RAP path):    {:?}", t.total);
    if t.used_index_page_locs {
        println!("  used index-stored page_locs: yes");
    }
    if t.used_prepared_layout {
        println!("  used prepared ZSTD/interleaved layout: yes");
    }
    if result.skipped_by_predicate > 0 {
        println!("  skipped_by_predicate: {}", result.skipped_by_predicate);
    }
    if !result.covering_hits.is_empty() {
        println!("  covering index:");
        for c in &result.covering_hits {
            println!("    {c}");
        }
    }
    if verbose {
        println!("  page map (sample):");
        for d in result.page_descriptions.iter().take(12) {
            println!("    {d}");
        }
        if result.page_descriptions.len() > 12 {
            println!("    … {} more", result.page_descriptions.len() - 12);
        }
    }
    println!("  rows (showing up to {limit}):");
    for row in result.rows.iter().take(limit) {
        println!(
            "    row={} file={} ts={} track={} dur_ms={}",
            row.row_number, row.source_file, row.timestamp_ms, row.track_uri, row.duration_ms
        );
    }
    if result.rows.len() > limit {
        println!("    … {} more rows", result.rows.len() - limit);
    }
}

fn collect_parquet(dir: &PathBuf) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("read_dir {}", dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("parquet"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    Ok(files)
}

fn avg_duration(v: &[std::time::Duration]) -> std::time::Duration {
    if v.is_empty() {
        return std::time::Duration::ZERO;
    }
    let sum: std::time::Duration = v.iter().copied().sum();
    sum / (v.len() as u32)
}

fn resolve_format(format: OutputFormat, json: bool) -> OutputFormat {
    if json {
        OutputFormat::Json
    } else {
        format
    }
}

/// Compound keys: `a||b` is encoded with U+001F via `encode_key`.
fn encode_cli_key(raw: &str) -> String {
    if raw.contains("||") {
        let parts: Vec<&str> = raw.split("||").collect();
        rap::index::encode_key(&parts)
    } else {
        raw.to_string()
    }
}

fn parse_columns(raw: &[String]) -> Vec<String> {
    raw.iter()
        .flat_map(|s| s.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// RFC3339, `YYYY-MM-DD` (UTC), or integer milliseconds.
fn parse_time_ms(s: &str, end_of_day: bool) -> Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty time; expected RFC3339, YYYY-MM-DD, or integer milliseconds");
    }
    if let Ok(ms) = s.parse::<i64>() {
        return Ok(ms);
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let naive = if end_of_day {
            d.and_hms_milli_opt(23, 59, 59, 999)
        } else {
            d.and_hms_milli_opt(0, 0, 0, 0)
        }
        .with_context(|| format!("invalid clock for date {s}"))?;
        return Ok(naive.and_utc().timestamp_millis());
    }
    match DateTime::parse_from_rfc3339(s) {
        Ok(dt) => Ok(dt.timestamp_millis()),
        Err(e) => bail!(
            "invalid time {s:?}: expected RFC3339, YYYY-MM-DD, or integer milliseconds ({e})"
        ),
    }
}

fn build_query_options(
    offset: usize,
    limit: Option<usize>,
    http_base: Option<String>,
    columns: &[String],
    since: Option<&str>,
    until: Option<&str>,
    covering_only: bool,
    min_listens: Option<u64>,
) -> Result<QueryOptions> {
    let cols = parse_columns(columns);
    Ok(QueryOptions {
        offset,
        limit,
        http_base,
        columns: if cols.is_empty() { None } else { Some(cols) },
        since_ms: match since {
            Some(s) => Some(parse_time_ms(s, false)?),
            None => None,
        },
        until_ms: match until {
            Some(s) => Some(parse_time_ms(s, true)?),
            None => None,
        },
        covering_only,
        min_listen_count: min_listens,
        ..Default::default()
    })
}

fn query_result_json(
    result: &rap::query::QueryResult,
    index_load: std::time::Duration,
) -> serde_json::Value {
    let t = &result.timings;
    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "user_id": r.user_id,
                "timestamp_ms": r.timestamp_ms,
                "track_uri": r.track_uri,
                "duration_ms": r.duration_ms,
                "source_file": r.source_file,
                "row_number": r.row_number,
            })
        })
        .collect();
    serde_json::json!({
        "key": result.key,
        "rows": rows,
        "covering": result.covering_hits,
        "timings": {
            "index_load_ms": index_load.as_millis() as u64,
            "index_lookup_ms": t.index_lookup.as_millis() as u64,
            "metadata_resolve_ms": t.metadata_resolve.as_millis() as u64,
            "ranged_read_ms": t.ranged_read_demo.as_millis() as u64,
            "decode_extract_ms": t.decode_extract.as_millis() as u64,
            "total_ms": t.total.as_millis() as u64,
        },
        "totals": {
            "rows": result.rows.len(),
            "value_count": result.total_value_count,
            "bytes_ranged": t.bytes_ranged,
            "pages_touched": t.pages_touched,
            "files_touched": t.files_touched,
            "skipped_by_predicate": result.skipped_by_predicate,
            "offset": result.offset,
            "limit": result.limit,
        },
    })
}

fn print_explain(expl: &rap::query::ExplainResult, out: OutputFormat) -> Result<()> {
    match out {
        OutputFormat::Json => {
            let v = serde_json::json!({
                "key": expl.key,
                "bucket": expl.bucket,
                "num_entries": expl.num_entries,
                "num_entries_after_predicates": expl.num_entries_after_predicates,
                "files": expl.files,
                "covering": expl.covering,
                "page_descriptions": expl.page_descriptions,
                "estimated_bytes": expl.estimated_bytes,
                "estimated_range_gets": expl.estimated_range_gets,
                "covering_only": expl.covering_only,
                "columns": expl.columns,
                "since_ms": expl.since_ms,
                "until_ms": expl.until_ms,
                "skipped_by_predicate": expl.skipped_by_predicate,
            });
            println!("{}", serde_json::to_string(&v)?);
        }
        OutputFormat::Table => {
            println!(
                "explain key={} bucket={}",
                expl.key,
                expl.bucket
                    .map(|b| b.to_string())
                    .unwrap_or_else(|| "-".into())
            );
            println!(
                "  entries={} after_predicates={} skipped={}",
                expl.num_entries,
                expl.num_entries_after_predicates,
                expl.skipped_by_predicate
            );
            println!("  files: {:?}", expl.files);
            println!("  covering: {:?}", expl.covering);
            println!(
                "  estimated: {} bytes / {} range GETs",
                expl.estimated_bytes, expl.estimated_range_gets
            );
            println!("  pages:");
            for p in &expl.page_descriptions {
                println!("    {p}");
            }
            if expl.covering_only {
                println!("  covering_only: true");
            }
        }
    }
    Ok(())
}

fn run_stats(index: &Path, out: OutputFormat) -> Result<()> {
    let registry_path = index.join("registry.json");
    if !registry_path.exists() {
        bail!("no RAP index at {} (missing registry.json)", index.display());
    }
    let registry: Vec<String> = serde_json::from_reader(std::fs::File::open(&registry_path)?)
        .with_context(|| format!("read {}", registry_path.display()))?;

    let mut fragments = Vec::new();
    for frag_id in &registry {
        let man_path = index
            .join("fragments")
            .join(frag_id)
            .join("manifest.json");
        let raw = std::fs::read_to_string(&man_path)
            .with_context(|| format!("read {}", man_path.display()))?;
        let mut man: serde_json::Value = serde_json::from_str(&raw)
            .with_context(|| format!("parse {}", man_path.display()))?;
        if man.get("fragment_id").is_none() {
            man["fragment_id"] = serde_json::Value::String(frag_id.clone());
        }
        fragments.push(man);
    }

    match out {
        OutputFormat::Json => {
            let v = serde_json::json!({
                "index": index.display().to_string(),
                "fragments": fragments,
            });
            println!("{}", serde_json::to_string(&v)?);
        }
        OutputFormat::Table => {
            println!("index: {}", index.display());
            println!("fragments: {}", fragments.len());
            for man in &fragments {
                let id = man
                    .get("fragment_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let n_files = man
                    .get("files")
                    .and_then(|v| v.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                let buckets = man
                    .get("num_buckets")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let created = man
                    .get("created_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                print!("  {id}  files={n_files}  buckets={buckets}  created_at={created}");
                if let Some(kc) = man.get("key_columns") {
                    if !kc.is_null() {
                        print!("  key_columns={kc}");
                    }
                }
                println!();
            }
        }
    }
    Ok(())
}
