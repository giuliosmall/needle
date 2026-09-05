//! Light CLI smoke tests (library E2E is preferred for correctness).

use assert_cmd::Command;
use predicates::prelude::*;
use std::process::Command as StdCommand;

#[test]
fn cli_help_lists_commands() {
    Command::cargo_bin("needle")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("generate"))
        .stdout(predicate::str::contains("index"))
        .stdout(predicate::str::contains("query"))
        .stdout(predicate::str::contains("demo"))
        .stdout(predicate::str::contains("explain"))
        .stdout(predicate::str::contains("stats"))
        .stdout(predicate::str::contains("daemon"))
        .stdout(predicate::str::contains("sql"))
        .stdout(predicate::str::contains("iceberg-index"))
        .stdout(predicate::str::contains("compact"))
        .stdout(predicate::str::contains("forget"))
        .stdout(predicate::str::contains("verify"));
}

#[test]
fn cli_new_subcommand_help() {
    Command::cargo_bin("needle")
        .unwrap()
        .args(["daemon", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("bind"))
        .stdout(predicate::str::contains("lazy-buckets"))
        .stdout(predicate::str::contains("full-index"))
        .stdout(predicate::str::contains("tls-cert"))
        .stdout(predicate::str::contains("token"))
        .stdout(predicate::str::contains("insecure"));

    Command::cargo_bin("needle")
        .unwrap()
        .args(["sql", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--sql"))
        .stdout(predicate::str::contains("--index"))
        .stdout(predicate::str::contains("no-verify"))
        .stdout(predicate::str::contains("one lookup key"))
        .stdout(predicate::str::contains("Not lake SQL"));

    Command::cargo_bin("needle")
        .unwrap()
        .args(["iceberg-index", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("key-column"))
        .stdout(predicate::str::contains("--table"))
        .stdout(predicate::str::contains("--catalog"))
        .stdout(predicate::str::contains("--rest-uri"))
        .stdout(predicate::str::contains("--namespace"))
        .stdout(predicate::str::contains("--table-name"))
        .stdout(predicate::str::contains("--rest-token"))
        .stdout(predicate::str::contains("NEEDLE_ICEBERG_TOKEN"))
        .stdout(predicate::str::contains("production catalog path"))
        .stdout(predicate::str::contains("fallback"))
        .stdout(predicate::str::contains("glue"))
        .stdout(predicate::str::contains("nessie"));

    Command::cargo_bin("needle")
        .unwrap()
        .args([
            "iceberg-index",
            "--catalog",
            "glue",
            "--index",
            "/tmp/needle-no-such-index",
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("unsupported catalog").and(predicate::str::contains("glue")),
        );

    Command::cargo_bin("needle")
        .unwrap()
        .args([
            "iceberg-index",
            "--catalog",
            "nessie",
            "--index",
            "/tmp/needle-no-such-index",
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("unsupported catalog").and(predicate::str::contains("nessie")),
        );

    Command::cargo_bin("needle")
        .unwrap()
        .args(["compact", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--index"))
        .stdout(predicate::str::contains("fragment"));

    Command::cargo_bin("needle")
        .unwrap()
        .args(["forget", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--key"))
        .stdout(predicate::str::contains("--index"))
        .stdout(predicate::str::contains("--check"))
        .stdout(predicate::str::contains("not a GDPR delete"));

    Command::cargo_bin("needle")
        .unwrap()
        .args(["verify", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--index"));
}

#[test]
fn cli_generate_index_query_tiny() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("parquet");
    let idx = tmp.path().join("rap-index");

    Command::cargo_bin("needle")
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

    Command::cargo_bin("needle")
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

    Command::cargo_bin("needle")
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
    let out = StdCommand::new(env!("CARGO_BIN_EXE_needle"))
        .args(["demo-full", "--help"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("demo-full") || stdout.contains("key"));
}
