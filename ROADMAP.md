# Needle roadmap

Needle is an **external key → file + page index** over Parquet already in the lake. Look up a key, issue Range GETs, decode those pages. Same files Spark / DuckDB / BigQuery scan.

This is **not** Spotify’s RAP. This is **not** a general lake query engine. Retention today is **hide-only** (`forget` does not rewrite Parquet; there is no `needle delete`).

Crate version is `0.5.0` while the product label is 1.0. **1.1** should tag `v1.0.0` so semver and README agree.

**North star:** a stranger can run `needled` against Iceberg REST + AWS S3, serve point lookups at roughly storage RTT, and trust file identity plus Iceberg v2 deletes **on read**.

If a proposal needs lake-wide SQL, a UI, or a second copy of the data, it is a different product.

---

## Principles

1. Fail closed (stale identity, unsupported Iceberg deletes, unknown index major).
2. Additive frozen v1 until a real break (`format_version` 2 / `/v2/`).
3. The lake is the system of record. Needle must not silently rewrite Parquet.
4. One access path: point lookup (optional SQL over **that key’s** rows).
5. Ship the smallest complete slice with tests and docs, or don’t ship it.

---

## 1.1 — Ops, not features

Strangers lose on jobs and serving, not on missing RAP write modes.

| Work | Done when |
|------|-----------|
| Tag crate `1.0.0` + changelog | Semver matches the README 1.0 sentence |
| `needled` metrics / structured logs (lookup ms, Range GETs, bytes, stale, 401) | Operators can graph a lookup |
| `iceberg-index` as an idempotent job (exit codes; same snapshot is a no-op) | Cron / Job example in docs |
| Index root `s3://…` for **fragments and** `registry.json` (CAS on publish) | Two writers still fail cleanly; readers load from object storage |
| Gated live AWS + REST catalog test | Nightly / `--ignored`; not required for `cargo test` |
| Runbook: compact, forget, verify, reload | README or this file; `forget --check` already exists |

**Not in 1.1:** Glue, Parquet Variant, generic covering, Iceberg delete commits.

---

## 1.2 — The index follows the lake

The RAP article assumes each pipeline run **appends index fragments**.

| Work | Done when |
|------|-----------|
| Iceberg snapshot id is the clock for incremental index | Safe next to Spark/Flink commits |
| **Either** `forget --commit-iceberg` (REST position-delete + snapshot bump) **or** keep hide-only forever | One story only; Hadoop/read-only fail closed if we commit |
| Glue catalog: implement with mocks **or** remain `unsupported catalog` | No silent Hadoop fallback |
| Generic covering: `count` / `sum` / `min` / `max` on named columns, or keep the listen-shaped refuse | Two-column table; no fake JSON alias |

Do not add a CLI flag named `delete` unless it commits a real Iceberg delete.

---

## 1.3 — Prep in the pipeline

Local `--mode sorted|cogrouped|one-page-per-key|blob|zstd-frames|aligned|interleaved` is a **fidelity demo**. Production wins when **writers** emit those layouts.

| Work | Done when |
|------|-----------|
| One documented Spark / Flink / Iceberg recipe: sort or bucket, then `iceberg-index` | Nobody needs `needle generate` on a real lake |
| True skippable-frame interleave (optional) | Spec-valid Parquet; RAP one span; analytics I/O documented |
| Parquet Variant **or** drop “Variant” language | JSON blob ≠ Variant; say so |

Until 1.3, the honest line is: **index any existing Parquet; prep modes are optional and local.**

---

## Later / maybe never

Only after 1.1–1.2 are boring:

- Nessie (if users have it)
- Swap hand-rolled SigV4 for `aws-sdk-s3` / `object_store` if IMDS/proxy pain is real
- Multi-replica `needled` (shared mmap, readers only)
- HTTP pagination cursors (offset/limit is enough until someone pages huge value lists)
- Z-order / Hilbert (article: complementary file layout, not RAP)

---

## Never (or this is a different product)

- Lake-wide SQL / “Trino but faster”
- A UI
- Copying the lake into a KV store
- Silent Parquet rewrite on `forget`
- Claiming this is Spotify RAP
- Bloom filters / partition pruning as RAP features (those shrink **scans**; RAP replaces the within-file discovery chain)

---

## Article fidelity (not a release gate)

Covered: external index, hash-bucketed fragments, parallel Range GETs, sort / cogroup / one-page-per-key / ZSTD frames / 4KB align / JSON blob / covering (listen-shaped) / secondary hash+sorted.

Asterisks vs the [Spotify RAP write-up](https://engineering.atspotify.com/2026/7/indexing-the-data-lake-for-online-point-queries): JSON blob is not Parquet Variant; covering is not generic; interleave keeps official sibling columns so Arrow still reads the file.

See [`NOTES.md`](./NOTES.md).
