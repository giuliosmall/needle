//! Light CLI smoke tests (library E2E is preferred for correctness).

use assert_cmd::Command;
use predicates::prelude::*;
use std::process::Command as StdCommand;

#[test]
fn cli_help_lists_commands() {
    Command::cargo_bin("rap")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("generate"))
        .stdout(predicate::str::contains("index"))
        .stdout(predicate::str::contains("query"))
        .stdout(predicate::str::contains("demo"));
}

#[test]
fn cli_generate_index_query_tiny() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("parquet");
    let idx = tmp.path().join("rap-index");

    Command::cargo_bin("rap")
        .unwrap()
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
        .assert()
        .success();

    Command::cargo_bin("rap")
        .unwrap()
        .args([
            "index",
            "--data",
            data.to_str().unwrap(),
            "--index",
            idx.to_str().unwrap(),
            "--covering",
            "--buckets",
            "4",
            "--fragment",
            "frag-cli",
        ])
        .assert()
        .success();

    Command::cargo_bin("rap")
        .unwrap()
        .args([
            "query",
            "user_0003",
            "--index",
            idx.to_str().unwrap(),
            "--limit",
            "10",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("user_0003").or(predicate::str::contains("rows")));
}

#[test]
fn cli_demo_full_flag_exists() {
    // --help on demo-full should work (don't run the heavy demo).
    let out = StdCommand::new(env!("CARGO_BIN_EXE_rap"))
        .args(["demo-full", "--help"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("demo-full") || stdout.contains("key"));
}
