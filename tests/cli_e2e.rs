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
