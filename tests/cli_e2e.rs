//! Deep CLI E2E: actually run the `rap` binary (demo + demo-full) on tiny
//! deterministic datasets in a tempdir so we never clobber `data/`.

use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;

fn rap() -> Command {
    Command::cargo_bin("rap").unwrap()
}

#[test]
fn cli_demo_tiny_dataset_exit_zero_and_markers() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    rap()
        .args([
            "demo",
            "--key",
            "user_0003",
            "--root",
            root.to_str().unwrap(),
            "--users",
            "8",
            "--listens-per-user",
            "3",
            "--files",
            "1",
        ])
        .timeout(Duration::from_secs(120))
        .assert()
        .success()
        .stdout(predicate::str::contains("=== RAP demo ==="))
        .stdout(predicate::str::contains("files under"))
        .stdout(predicate::str::contains(root.to_str().unwrap()))
        .stdout(predicate::str::contains("keys="))
        .stdout(predicate::str::contains("Query key=user_0003"))
        .stdout(predicate::str::contains("row(s)"))
        .stdout(predicate::str::contains("speedup").or(predicate::str::contains("=== done ===")))
        .stdout(predicate::str::contains("=== done ==="));
}

#[test]
fn cli_demo_data_dir_alias_and_env() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    // --data-dir is a visible alias for --root.
    rap()
        .args([
            "demo",
            "--key",
            "user_0001",
            "--data-dir",
            root.to_str().unwrap(),
            "--users",
            "6",
            "--listens-per-user",
            "2",
            "--files",
            "1",
        ])
        .timeout(Duration::from_secs(120))
        .assert()
        .success()
        .stdout(predicate::str::contains("=== RAP demo ==="))
        .stdout(predicate::str::contains("=== done ==="));

    // RAP_DATA_DIR env is honored when --root/--data-dir is omitted.
    let tmp2 = tempfile::tempdir().unwrap();
    rap()
        .env("RAP_DATA_DIR", tmp2.path())
        .args([
            "demo",
            "--key",
            "user_0000",
            "--users",
            "4",
            "--listens-per-user",
            "2",
            "--files",
            "1",
        ])
        .timeout(Duration::from_secs(120))
        .assert()
        .success()
        .stdout(predicate::str::contains(tmp2.path().to_str().unwrap()));
}

#[test]
fn cli_demo_full_tiny_all_mode_sections() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    rap()
        .args([
            "demo-full",
            "--key",
            "user_0002",
            "--root",
            root.to_str().unwrap(),
            "--users",
            "8",
            "--listens-per-user",
            "3",
            "--files",
            "1",
        ])
        .timeout(Duration::from_secs(180))
        .assert()
        .success()
        .stdout(predicate::str::contains("=== RAP demo-full"))
        .stdout(predicate::str::contains("--- mode=Sorted ---"))
        .stdout(predicate::str::contains("--- mode=OnePagePerKey ---"))
        .stdout(predicate::str::contains("--- mode=Blob ---"))
        .stdout(predicate::str::contains("--- mode=ZstdFrames ---"))
        .stdout(predicate::str::contains("--- mode=Aligned ---"))
        .stdout(predicate::str::contains("--- mode=Interleaved ---"))
        .stdout(predicate::str::contains("--- mode=Cogrouped ---"))
        .stdout(predicate::str::contains("custom writer"))
        .stdout(predicate::str::contains("PAR1 ok"))
        .stdout(predicate::str::contains("--- HTTP Range proof ---"))
        .stdout(predicate::str::contains("HTTP"))
        .stdout(predicate::str::contains("--- Secondary index (track_uri) ---"))
        .stdout(predicate::str::contains("=== demo-full done ==="));
}

fn generate_and_index(root: &std::path::Path, covering: bool) -> std::path::PathBuf {
    let data = root.join("parquet");
    let idx = root.join("rap-index");
    rap()
        .args([
            "generate",
            "--out",
            data.to_str().unwrap(),
            "--users",
            "8",
            "--listens-per-user",
            "3",
            "--files",
            "1",
            "--mode",
            "sorted",
            "--seed",
            "1",
        ])
        .timeout(Duration::from_secs(120))
        .assert()
        .success();

    let mut cmd = rap();
    cmd.args([
        "index",
        "--data",
        data.to_str().unwrap(),
        "--index",
        idx.to_str().unwrap(),
        "--buckets",
        "4",
        "--fragment",
        "frag-cli",
    ]);
    if covering {
        cmd.arg("--covering");
    }
    cmd.timeout(Duration::from_secs(120)).assert().success();
    idx
}

fn json_from_stdout(stdout: &str) -> serde_json::Value {
    let t = stdout.trim();
    if let Ok(v) = serde_json::from_str(t) {
        return v;
    }
    if let (Some(i), Some(j)) = (t.find('{'), t.rfind('}')) {
        if j > i {
            if let Ok(v) = serde_json::from_str(&t[i..=j]) {
                return v;
            }
        }
    }
    panic!("stdout is not JSON: {stdout}");
}

#[test]
fn cli_query_json_format() {
    let tmp = tempfile::tempdir().unwrap();
    let idx = generate_and_index(tmp.path(), false);

    let out = rap()
        .args([
            "query",
            "user_0003",
            "--index",
            idx.to_str().unwrap(),
            "--format",
            "json",
        ])
        .timeout(Duration::from_secs(120))
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let stdout = String::from_utf8_lossy(&out);
    let v = json_from_stdout(&stdout);
    let dumped = v.to_string();
    assert!(
        dumped.contains("user_0003") || v.get("rows").is_some(),
        "JSON should contain the key or a rows field: {dumped}"
    );
}

#[test]
fn cli_covering_only() {
    let tmp = tempfile::tempdir().unwrap();
    let idx = generate_and_index(tmp.path(), true);

    let out = rap()
        .args([
            "query",
            "user_0003",
            "--index",
            idx.to_str().unwrap(),
            "--covering-only",
            "--format",
            "json",
        ])
        .timeout(Duration::from_secs(120))
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let stdout = String::from_utf8_lossy(&out);
    let v = json_from_stdout(&stdout);
    if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
        let covering_present = v.get("covering_hits").is_some()
            || v.get("covering").is_some()
            || stdout.to_lowercase().contains("covering")
            || stdout.contains("listen_count");
        assert!(
            rows.is_empty() || covering_present,
            "covering-only JSON rows should be empty or covering fields present: {stdout}"
        );
    } else {
        assert!(
            stdout.to_lowercase().contains("covering")
                || v.get("covering_hits").is_some()
                || stdout.contains("listen_count"),
            "covering-only JSON should include covering fields: {stdout}"
        );
    }
}

#[test]
fn cli_explain_command() {
    let tmp = tempfile::tempdir().unwrap();
    let idx = generate_and_index(tmp.path(), false);

    rap()
        .args(["explain", "user_0003", "--index", idx.to_str().unwrap()])
        .timeout(Duration::from_secs(120))
        .assert()
        .success()
        .stdout(
            predicate::str::contains("explain")
                .or(predicate::str::contains("bucket"))
                .or(predicate::str::contains("estimated")),
        );
}

#[test]
fn cli_stats_command() {
    let tmp = tempfile::tempdir().unwrap();
    let idx = generate_and_index(tmp.path(), false);

    rap()
        .args(["stats", "--index", idx.to_str().unwrap()])
        .timeout(Duration::from_secs(120))
        .assert()
        .success()
        .stdout(
            predicate::str::contains("frag")
                .or(predicate::str::contains("bucket"))
                .or(predicate::str::contains("frag-cli")),
        );
}

#[test]
fn cli_query_help_lists_new_flags() {
    rap()
        .args(["query", "--help"])
        .timeout(Duration::from_secs(120))
        .assert()
        .success()
        .stdout(predicate::str::contains("covering-only"))
        .stdout(predicate::str::contains("since"))
        .stdout(predicate::str::contains("format"));
}

#[test]
fn cli_index_key_column_flag() {
    rap()
        .args(["index", "--help"])
        .timeout(Duration::from_secs(120))
        .assert()
        .success()
        .stdout(predicate::str::contains("key-column"));
}
