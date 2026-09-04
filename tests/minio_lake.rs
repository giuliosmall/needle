//! Small MinIO lake E2E (local only — no cloud). Skips if tools/minio missing.
//! Large scale: `rap lake-generate --files 1000000` (see README).

use std::path::PathBuf;

fn tools_ok() -> bool {
    PathBuf::from("tools/minio").exists() && PathBuf::from("tools/mc").exists()
}

#[test]
fn minio_lake_e2e_small() {
    if !tools_ok() {
        eprintln!("skip: tools/minio or tools/mc missing");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let index = tmp.path().join("rap-lake-index");
    rap::lake::lake_e2e_small(80, &index).expect("minio lake e2e");
}

#[test]
#[ignore = "scale: run with --ignored when MinIO available; prefer CLI lake-generate for 100k–1M"]
fn minio_lake_scale_1k() {
    if !tools_ok() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let index = tmp.path().join("rap-lake-index");
    let opts = rap::lake::LakeGenerateOpts {
        files: 1_000,
        index_dir: index.clone(),
        parallelism: 32,
        days: 10,
        hash_buckets: 10,
        rows_per_file: 2,
        ..Default::default()
    };
    rap::lake::minio_up().unwrap();
    let man = rap::lake::lake_generate(&opts).unwrap();
    assert_eq!(man.objects, 1_000);
    let key = man.sample_keys.first().cloned().unwrap();
    let report = rap::lake::lake_query(&index, &key, 5).unwrap();
    assert!(!report.result.rows.is_empty());
    assert!(report.range_requests_demo > 0);
}


#[test]
fn minio_lake_fat_e2e_small() {
    if !tools_ok() {
        eprintln!("skip: tools/minio or tools/mc missing");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let index = tmp.path().join("rap-lake-index-fat");
    rap::lake::lake_e2e_fat_small(&index).expect("minio fat lake e2e");
}
