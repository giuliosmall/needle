//! Library-level end-to-end tests: generate → index → query for each write mode,
//! plus secondary index, HTTP Range, pagination, and covering aggregates.

use rap::index::{IndexBuilder, load_index};
use rap::query::{QueryOptions, RapQuerier, naive_scan};
use rap::secondary::{self, build_secondary, load_secondary};
use rap::storage::{RangeHttpServer, prove_http_matches_local};
use rap::writer::{WriteMode, WriterOptions, write_sample_dataset};
use std::path::{Path, PathBuf};

fn opts(out: &Path, mode: WriteMode) -> WriterOptions {
    WriterOptions {
        out_dir: out.to_path_buf(),
        num_users: 24,
        listens_per_user: 6,
        num_files: 2,
        mode,
        rows_per_row_group: 32,
        write_page_index: true,
        seed: 42,
        one_page_per_key: false,
    }
}

fn row_sig(rows: &[rap::query::ListenRow]) -> Vec<(i64, String, i64)> {
    let mut v: Vec<_> = rows
        .iter()
        .map(|r| (r.timestamp_ms, r.track_uri.clone(), r.duration_ms))
        .collect();
    v.sort();
    v
}

fn edge_keys(num_users: usize) -> [&'static str; 3] {
    // first / mid / last — fixed for num_users=24
    let _ = num_users;
    ["user_0000", "user_0012", "user_0023"]
}

fn generate_index_query(mode: WriteMode) -> (tempfile::TempDir, RapQuerier, Vec<PathBuf>) {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("parquet");
    let idx = tmp.path().join("rap-index");
    let paths = write_sample_dataset(&opts(&data, mode)).unwrap();
    IndexBuilder::new(&idx, 8)
        .with_covering(true)
        .build_fragment(&paths, "frag-e2e", Some(&format!("{mode:?}")))
        .unwrap();
    let querier = RapQuerier::new(load_index(&idx).unwrap());
    (tmp, querier, paths)
}

fn assert_rap_matches_naive(querier: &RapQuerier, paths: &[PathBuf], keys: &[&str]) {
    for key in keys {
        let rap = querier.query(key).unwrap();
        let (naive, _) = naive_scan(paths, key).unwrap();
        assert_eq!(
            row_sig(&rap.rows),
            row_sig(&naive),
            "RAP != naive for {key} (rap={}, naive={})",
            rap.rows.len(),
            naive.len()
        );
        assert!(!rap.rows.is_empty(), "expected rows for {key}");
    }
}

#[test]
fn e2e_sorted() {
    let (_t, q, paths) = generate_index_query(WriteMode::Sorted);
    assert_rap_matches_naive(&q, &paths, &edge_keys(24));
    let res = q.query("user_0012").unwrap();
    assert!(res.timings.used_index_page_locs || res.timings.pages_touched > 0);
}

#[test]
fn e2e_unsorted() {
    let (_t, q, paths) = generate_index_query(WriteMode::Unsorted);
    assert_rap_matches_naive(&q, &paths, &edge_keys(24));
}

#[test]
fn e2e_cogrouped() {
    let (_t, q, paths) = generate_index_query(WriteMode::Cogrouped);
    assert_rap_matches_naive(&q, &paths, &edge_keys(24));
    assert_eq!(q.query("user_0000").unwrap().rows.len(), 6);
}

#[test]
fn e2e_one_page_per_key() {
    let (_t, q, paths) = generate_index_query(WriteMode::OnePagePerKey);
    assert_rap_matches_naive(&q, &paths, &edge_keys(24));
    let e = &q.index.lookup("user_0000")[0];
    let locs = e.page_locs.as_ref().expect("page_locs");
    let user_pages = locs.iter().filter(|l| l.column == "user_id").count();
    assert_eq!(user_pages, 1);
    let res = q.query("user_0000").unwrap();
    assert!(res.timings.used_index_page_locs);
}

#[test]
fn e2e_blob() {
    let (_t, q, paths) = generate_index_query(WriteMode::Blob);
    assert_rap_matches_naive(&q, &paths, &edge_keys(24));
    assert_eq!(q.query("user_0023").unwrap().rows.len(), 6);
}

#[test]
fn e2e_zstd_frames() {
    let (_t, q, paths) = generate_index_query(WriteMode::ZstdFrames);
    assert_rap_matches_naive(&q, &paths, &edge_keys(24));
    let res = q.query("user_0012").unwrap();
    assert!(res.timings.used_prepared_layout);
    assert!(q.index.lookup("user_0012")[0].frame_locs.is_some());
}

#[test]
fn e2e_aligned() {
    let (_t, q, paths) = generate_index_query(WriteMode::Aligned);
    assert_rap_matches_naive(&q, &paths, &edge_keys(24));
    let e = &q.index.lookup("user_0000")[0];
    assert_eq!(e.aligned, Some(true));
    for f in e.frame_locs.as_ref().unwrap() {
        assert_eq!(f.offset % 4096, 0, "{} not aligned", f.column);
    }
}

#[test]
fn e2e_interleaved() {
    let (_t, q, paths) = generate_index_query(WriteMode::Interleaved);
    assert_rap_matches_naive(&q, &paths, &edge_keys(24));
    let e = &q.index.lookup("user_0012")[0];
    assert!(e.contiguous.is_some());
    let res = q.query("user_0012").unwrap();
    assert!(res.timings.used_prepared_layout);
}

#[test]
fn e2e_secondary_exact_and_range() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("parquet");
    let idx = tmp.path().join("rap-index");
    let paths = write_sample_dataset(&opts(&data, WriteMode::Sorted)).unwrap();
    IndexBuilder::new(&idx, 8)
        .build_fragment(&paths, "frag-sec", None)
        .unwrap();
    build_secondary(&idx, "frag-sec", "track_uri", 8).unwrap();
    let sec = load_secondary(&idx, "frag-sec", "track_uri").unwrap();
    assert!(sec.num_keys() > 0);

    let key = sec.sorted_keys[0].clone();
    let exact = sec.lookup_exact(&key);
    assert!(!exact.is_empty());

    let start = sec.sorted_keys[0].clone();
    let end = sec.sorted_keys[(sec.sorted_keys.len() - 1).min(5)].clone();
    let range = sec.lookup_range(&start, &end);
    assert!(!range.is_empty());
    assert!(range.iter().all(|r| r.key.as_str() >= start.as_str()
        && r.key.as_str() <= end.as_str()));

    // load_secondary_any discovers the fragment.
    let any = secondary::load_secondary_any(&idx, "track_uri").unwrap();
    assert_eq!(any.num_keys(), sec.num_keys());
}

#[test]
fn e2e_http_range_query() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("parquet");
    let idx = tmp.path().join("rap-index");
    let paths = write_sample_dataset(&opts(&data, WriteMode::Sorted)).unwrap();
    IndexBuilder::new(&idx, 8)
        .with_covering(true)
        .build_fragment(&paths, "frag-http", None)
        .unwrap();
    let querier = RapQuerier::new(load_index(&idx).unwrap());

    let server = RangeHttpServer::start(&data).unwrap();
    let base = server.base_url();

    let local = querier.query("user_0005").unwrap();
    let via_http = querier
        .query_with(
            "user_0005",
            &QueryOptions {
                http_base: Some(base.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(row_sig(&local.rows), row_sig(&via_http.rows));

    // Prove byte-identical ranged reads for index page locs.
    let e = &querier.index.lookup("user_0005")[0];
    let path = querier.index.file_path(e.file).unwrap();
    let ranges: Vec<_> = e
        .page_locs
        .as_ref()
        .unwrap()
        .iter()
        .map(|l| l.offset..l.offset + l.size as u64)
        .collect();
    let proof = prove_http_matches_local(path, &base, &ranges).unwrap();
    assert!(proof.bytes_compared > 0);
    server.stop();
}

#[test]
fn e2e_pagination_slices_match() {
    let (_t, q, _) = generate_index_query(WriteMode::Sorted);
    let full = q.query("user_0010").unwrap();
    assert_eq!(full.rows.len(), 6);

    let page = q
        .query_with(
            "user_0010",
            &QueryOptions {
                offset: 1,
                limit: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(page.rows.len(), 2);
    let full_sig = row_sig(&full.rows);
    for r in &page.rows {
        assert!(full_sig.contains(&(r.timestamp_ms, r.track_uri.clone(), r.duration_ms)));
    }

    let rest = q
        .query_with(
            "user_0010",
            &QueryOptions {
                offset: 4,
                limit: Some(10),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(rest.rows.len(), 2);
}

#[test]
fn e2e_covering_aggregates_match_recomputed() {
    let (_t, q, _) = generate_index_query(WriteMode::Sorted);
    for key in edge_keys(24) {
        let res = q.query(key).unwrap();
        let sum_dur: u64 = res.rows.iter().map(|r| r.duration_ms.max(0) as u64).sum();
        let entries = q.index.lookup(key);
        let cov_count: u64 = entries
            .iter()
            .map(|e| e.covering.as_ref().unwrap().listen_count)
            .sum();
        let cov_dur: u64 = entries
            .iter()
            .map(|e| e.covering.as_ref().unwrap().total_duration_ms)
            .sum();
        assert_eq!(cov_count, res.rows.len() as u64);
        assert_eq!(cov_dur, sum_dur);
        assert!(!res.covering_hits.is_empty());
    }
}

#[test]
fn e2e_cogrouped_covering_matches_nested_sums() {
    let (_t, q, paths) = generate_index_query(WriteMode::Cogrouped);
    for key in edge_keys(24) {
        let res = q.query(key).unwrap();
        let (naive, _) = rap::query::naive_scan(&paths, key).unwrap();
        assert_eq!(row_sig(&res.rows), row_sig(&naive));
        assert_eq!(res.rows.len(), 6, "{key} nested listen rows");

        let entries = q.index.lookup(key);
        let parquet_rows: usize = entries.iter().map(|e| e.row_numbers.len()).sum();
        assert_eq!(parquet_rows, 1, "{key} one cogrouped parquet row");

        let cov_count: u64 = entries
            .iter()
            .map(|e| e.covering.as_ref().expect("covering").listen_count)
            .sum();
        let cov_dur: u64 = entries
            .iter()
            .map(|e| e.covering.as_ref().unwrap().total_duration_ms)
            .sum();
        let sum_dur: u64 = res.rows.iter().map(|r| r.duration_ms.max(0) as u64).sum();
        assert_eq!(cov_count, res.rows.len() as u64);
        assert_eq!(cov_dur, sum_dur);
        assert!(
            cov_count > parquet_rows as u64,
            "covering must hoist nested list length, not parquet row count"
        );
        assert!(!res.covering_hits.is_empty());
        assert!(res.covering_hits.iter().any(|h| h.contains("listen_count=6")));
    }
}

#[test]
fn e2e_demo_library_path_sorted_covering() {
    // Exercises the same library path as `rap demo` without shelling out.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let data = root.join("parquet");
    let idx = root.join("rap-index");
    let mut o = opts(&data, WriteMode::Sorted);
    o.num_users = 30;
    o.listens_per_user = 5;
    let paths = write_sample_dataset(&o).unwrap();
    IndexBuilder::new(&idx, 16)
        .with_covering(true)
        .build_fragment(&paths, "frag-demo", Some("demo"))
        .unwrap();
    let q = RapQuerier::new(load_index(&idx).unwrap());
    let key = "user_0015";
    let rap = q.query(key).unwrap();
    let (naive, _) = naive_scan(&paths, key).unwrap();
    assert_eq!(row_sig(&rap.rows), row_sig(&naive));
    assert_eq!(rap.rows.len(), 5);
}

/// Slightly larger optional stress test (ignored in default CI runs).
#[test]
#[ignore]
fn e2e_stress_larger_dataset() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("parquet");
    let idx = tmp.path().join("rap-index");
    let o = WriterOptions {
        out_dir: data,
        num_users: 200,
        listens_per_user: 40,
        num_files: 4,
        mode: WriteMode::Sorted,
        rows_per_row_group: 512,
        write_page_index: true,
        seed: 1,
        one_page_per_key: false,
    };
    let paths = write_sample_dataset(&o).unwrap();
    IndexBuilder::new(&idx, 32)
        .with_covering(true)
        .build_fragment(&paths, "frag-stress", None)
        .unwrap();
    let q = RapQuerier::new(load_index(&idx).unwrap());
    for key in ["user_0000", "user_0100", "user_0199"] {
        let rap = q.query(key).unwrap();
        let (naive, _) = naive_scan(&paths, key).unwrap();
        assert_eq!(row_sig(&rap.rows), row_sig(&naive));
        assert_eq!(rap.rows.len(), 40);
    }
}
