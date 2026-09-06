# Needle

<p align="center">
  <img src="docs/logo.jpg" alt="Needle" width="280">
</p>

**Point queries on your data lake - without copying it into a KV store.**

Why *Needle*? A data lake is a haystack, one key is a needle, and the usual plan is to hire a combine harvester. Scan engines find the needle by eating the haystack (then billing you for the straw). We keep a map to the needle so you can just pick it up.

Needle is an external **key → file + page** index over ordinary Parquet. Look up a key, issue a handful of precise object-storage Range GETs, decode only those pages. Same files BigQuery / Spark / DuckDB already scan; no second copy of the data.

Inspired by Spotify’s [Random Access Parquet (RAP)](https://engineering.atspotify.com/2026/7/indexing-the-data-lake-for-online-point-queries) write-up. This repo is a faithful Rust recreation + local MinIO lake stress harness.

```bash
cargo build --release
./target/release/needle demo --key user_0042
# optional: install onto PATH
install -m 0755 target/release/needle target/release/needled ~/.local/bin/
```

---

## Why it exists

Online services and agents need **per-key** reads (one user, one entity) at interactive latency. Lakes are cheap and huge; distributed SQL is built for scans. The expensive part is rarely the disk - it’s the **dependent discovery chain** inside each Parquet file:

```
Point query → List / prune files → Fetch footer → Parse row groups
           → Scan key column → Page index → Fetch value pages
```

Each arrow is another round-trip. Needle collapses that to: **index lookup → ranged reads**.

```
Point query → Needle index O(1) → Cached page locations
           → Parallel Range GETs → Decode matching rows
```

---

## How it works

### External index

A multimap (hash-bucketed, append-only fragments):

| Field | Meaning |
|-------|---------|
| `key` | Lookup key (e.g. `user_id`) |
| `file` | Dictionary-encoded ordinal → Parquet path / `s3://…` URI |
| `row_numbers` | Rows for that key in the file |
| `value_count` | Optional - pagination without reading data |
| `page_locs` / `frame_locs` | Optional - byte ranges stored in the index |
| `covering` | Optional hoisted aggregates (counts, sums) |

```
key = user_42
        │
        ▼
   bucket_001  ──file=7, rows, page_locs──►  part-001.parquet
                                                    │
                                                    │  Range GET ~2 KiB
                                                    ▼
                                           rows for user_42
```

### Reader path

1. Hash the key → load the one index bucket (lazy).
2. Resolve rows → page byte ranges (OffsetIndex and/or index-stored `page_locs`).
3. Issue **parallel** Range GETs (local file, MinIO, or AWS S3).
4. Decode only those pages; return rows (optional offset/limit) plus covering aggregates.

### Prep modes (still valid Parquet where possible)

Sorting / co-grouping / one-page-per-key / blobs / covering indexes / secondary indexes / custom low-level writer for ZSTD frame-per-key, 4KB alignment, and interleaved layouts. See [`NOTES.md`](./NOTES.md) for the article fidelity checklist.

---

## Quick start

```bash
# Unit + E2E (no MinIO required for most tests)
cargo test

# Local demo: generate → index → query → bench
cargo run --release -- demo --key user_0042

# Article-wide demo (all prep modes + HTTP Range proof)
cargo run --release -- demo-full --key user_0042
```

## Usable CLI

Index any key column, filter point queries, explain a lookup, and inspect fragment
stats without loading every bucket (which OOMs large lakes). Compound keys can be
passed as an encoded string (U+001F-joined) or as `part||part`.

```bash
needle index --data data/parquet --index data/rap-index --key-column user_id --covering
needle query user_0042 --columns timestamp,track_uri --since 2024-01-01 --until 2024-12-31
needle query user_0042 --covering-only --format json
needle explain user_0042
needle query --dimension track_uri --range-start spotify:track:000 --range-end spotify:track:fff
needle stats --index data/rap-index
needle forget --index data/rap-index --key user_0000
needle compact --index data/rap-index
needle verify --index data/rap-index
```

`--format json` (or `--json`) prints one JSON object to stdout. Point-query JSON
includes `rows` (Arrow projection, not listen-schema-only) and `covering_values`:

```json
{
  "key": "user_0042",
  "rows": [{"user_id": "user_0042", "track_uri": "spotify:track:…"}],
  "covering": ["file=… listen_count=80 …"],
  "covering_values": [
    {"file": "…", "value_count": 80, "listen_count": 80, "total_duration_ms": 123, "min_ts": 1, "max_ts": 2}
  ]
}
```

`value_count` is the generic alias of `listen_count`. Point queries load only the
index buckets for that key. `needle query --help` / `needle index --help` list
the filter and column flags.

### Compact, forget, verify

The index is append-only fragments (`registry.json` + `fragments/`). These commands
maintain it without rewriting lake Parquet:

| Command | What it does |
|---------|----------------|
| `needle forget --key K` | Hide `K` from Needle lookups only (sticky `forgotten.jsonl`). Hide-only — lake Parquet is unchanged. `--check` warns if the key still has source files. There is no `needle delete`. |
| `needle compact` | Rewrite to one fragment: last `(key, file)` wins, apply forget + Iceberg drops. Unreferenced fragment dirs are removed. `registry.json` (`format_version` 1) points at the new id (`compact-<unix-ms>` unless `--fragment` is set). |
| `needle verify` | Compare stored size / ETag / mtime (`file_idents`) to live local files or S3 HEAD. Exits non-zero if any file is `stale`. |

Queries **fail closed** on identity mismatch (`stale_file_identity`) unless you pass `--no-verify` (unsafe). `registry.json` is published with tmp+rename under an exclusive `.needle.lock` locally, or a conditional S3 PUT (`If-None-Match` / `If-Match`) when the index root is `s3://`. `needled` reloads when that file’s mtime changes, so forget/compact show up without a restart. Default is lazy buckets: only the hashed bucket is mmapped and deserialized (`--full-index` loads every bucket into RAM). The file dictionary is mmapped `files.bin`; a point lookup decodes only the files that key names. See [`FORMAT.md`](./FORMAT.md).

### HTTP daemon

Serve point queries over HTTP (optional `--lazy-buckets`):

```bash
needled --index data/rap-index --bind 127.0.0.1:7780
# production-ish: TLS + bearer (required on non-loopback unless --insecure)
needled --bind 0.0.0.0:7780 --tls-cert cert.pem --tls-key key.pem --token "$NEEDLED_TOKEN"
# or
needle daemon --index data/rap-index
```

Loopback (`127.0.0.1`) may stay plaintext with no token for demos. Non-loopback binds require `--tls-cert`/`--tls-key` and `--token` (or `--insecure`, which prints an `INSECURE` warning).

| Method | Path | Notes |
|--------|------|--------|
| GET | `/health` | `{"ok": true}` — unauthenticated |
| GET | `/v1/query?key=user_0042` | Same JSON as `needle query --format json`. Requires `Authorization: Bearer` when a token is set. Optional: `offset`, `limit`, `columns`, `since_ms`, `until_ms`, `covering_only`, `min_listens`, `verify=0` (unsafe). |
| GET | `/v1/explain?key=user_0042` | Plan: files, pages, estimated Range GETs. Auth like query. |
| GET | `/v1/stats` | Fragment summary (no bucket load). Auth like query. |

Env: `NEEDLE_INDEX`, `NEEDLE_BIND`, `NEEDLED_TOKEN`.

### Iceberg

Production discovery is an **Iceberg REST catalog**. Hadoop `metadata/*.json` remains a fallback. `--catalog glue` and `--catalog nessie` exit with `unsupported catalog` (no silent Hadoop fallback).

```bash
# REST catalog (production)
needle iceberg-index --catalog rest --rest-uri http://catalog:8181/iceberg \
  --namespace db --table-name events --index data/rap-index --key-column user_id
# Bearer: --rest-token or NEEDLE_ICEBERG_TOKEN

# Hadoop warehouse fallback
needle iceberg-index --catalog hadoop --table /path/to/iceberg/table --index data/rap-index --key-column user_id
needle iceberg-index --table s3://bucket/warehouse/db/tbl --index data/rap-index --key-column user_id
```

Each run indexes the **current snapshot** incrementally: only newly added data files
are scanned; files dropped by overwrite/expire are recorded on the fragment
(`dropped_files`) and evicted on load/compact. Iceberg v2 **position** and
**equality** delete files are applied (deleted rows never appear in lookups).
Unsupported delete encodings fail closed:

`needle refuses Iceberg tables with unsupported delete files; apply deletes or compact first`

Re-running the same snapshot is a no-op. New delete files that hit already-indexed
data files re-scan those files. A later snapshot that re-adds a previously expired
file reindexes it.

### SQL

SQL over **rows for one lookup key** (`hits` / `needle_lookup`). This is not lake-wide SQL:

```bash
needle sql --index data/rap-index --key user_0042 --sql "SELECT track_uri, count(*) AS n FROM hits GROUP BY track_uri ORDER BY n DESC"
```

### Object store (MinIO and AWS)

Range GET, full GET, PUT, HEAD, ListObjectsV2. SigV4 over raw TCP; TLS via `native-tls`.
Retries with jitter on 429/5xx; STS session tokens; response checksums verified when S3 sends them.

Credential chain: `NEEDLE_S3_*` / `RAP_S3_*`, then `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` / `AWS_REGION`, then `~/.aws/credentials` + `~/.aws/config` (`AWS_PROFILE`).

| | MinIO (default) | AWS |
|--|-----------------|-----|
| Endpoint | `127.0.0.1:9000` | `s3.amazonaws.com` / `s3.<region>.amazonaws.com` |
| Addressing | path-style `/{bucket}/{key}` | virtual-hosted `/{key}` on `{bucket}.{host}` |
| TLS | off | on |
| Anonymous GET | on (if the bucket allows it) | off (signed) |

```bash
export NEEDLE_S3_ENDPOINT=s3.us-east-1.amazonaws.com   # or https://…
export NEEDLE_S3_REGION=us-east-1
export NEEDLE_S3_ACCESS_KEY=…          # or AWS_ACCESS_KEY_ID
export NEEDLE_S3_SECRET_KEY=…          # or AWS_SECRET_ACCESS_KEY
export NEEDLE_S3_SESSION_TOKEN=…       # optional STS; or AWS_SESSION_TOKEN
# NEEDLE_S3_TLS=1|0  NEEDLE_S3_PATH_STYLE=1|0  NEEDLE_S3_ANON_READ=1|0
```

Local MinIO stress (no cloud account):

```bash
# downloads tools/minio + tools/mc on first run if missing
cargo run --release -- minio-up
cargo run --release -- lake-generate-fat --files 16 --rows-per-file 250000
cargo run --release -- lake-query user_0 --index data/rap-lake-index-fat
cargo run --release -- lake-stress --index data/rap-lake-index-fat-300k --queries 10000 --concurrency 32
```

We routinely stress **hundreds of thousands** of multi-page objects on one box; expect tens of GiB of disk if you go there.

---

## Architecture sketch

```
Client  → Needle:  query user_42
Needle  → Index:   lookup index bucket
Index   → Needle:  file, rows, page_locs
Needle  → Store:   Range GET page A, B, C  (parallel)
Store   → Needle:  small page payloads
Needle  → Client:  rows and covering aggregates
```

**Two stress axes this repo exercises:**

1. **Many files** - index picks 1 of N objects (N → 10⁵–10⁶).
2. **Fat files** - Range GET pages so `bytes_ranged / file_size` ≪ 1%.

---

## Project layout

```
src/
  main.rs              CLI (`needle`)
  needled.rs           HTTP query daemon binary
  index.rs             External Needle / RAP index (fragments, compact, forget, verify, format v1)
  iceberg.rs           Incremental Iceberg snapshot → Needle fragments
  query.rs             Point query + Arrow/JSON rows + covering
  server.rs            needled JSON HTTP (`/v1/query`, reload on registry mtime)
  sql.rs               DataFusion SQL over one-key `hits`
  metadata.rs          Footer + OffsetIndex → page ranges
  writer.rs            Sample lake writers (sorted / cogrouped / …)
  storage.rs           RangeReader trait (local + HTTP)
  s3.rs                S3 SigV4 + Range GET (MinIO path-style HTTP, AWS TLS virtual-host)
  lake.rs              Lake generate / index / query / stress
  secondary.rs         Hash + sorted secondary indexes
  parquet_lowlevel/    Custom page writer (frames, align, interleaved, paged)
tests/                 Unit-adjacent E2E + MinIO smoke
FORMAT.md              On-disk index format (frozen v1, lock, compact GC)
HTTP.md                needled HTTP JSON (frozen v1)
NOTES.md               Article mapping & fidelity notes
docs/logo.jpg          Project mark (needle + thread)
```

---

## Status / honesty

**1.0 — point lookups on Iceberg REST + AWS S3 with mmapped buckets and on-demand file dictionary, frozen v1 index and HTTP; retention is hide-only (Needle never rewrites lake Parquet). Not Spotify RAP. Not lake SQL.**

**Stability policy.** Index `format_version` 1 and needled HTTP JSON v1 are **frozen**: current fields stay, additive optional keys are allowed, breaking changes require a new major (`format_version` 2 or `/v2/`). See [`FORMAT.md`](./FORMAT.md) and [`HTTP.md`](./HTTP.md).

- **Implemented:** external index, page-accurate ranged reads, covering + secondary indexes, HTTP/S3 Range (TLS + virtual-host, MinIO and AWS, STS session tokens, retries, checksums), Iceberg REST catalog + Hadoop fallback (`--catalog glue|nessie` hard-error), incremental index (add/drop live-set, v2 position/equality deletes applied on read, fail-closed on unsupported encodings), compact/forget/verify, **strict** file identity on query (`--no-verify` unsafe), `needled` TLS + bearer token, frozen `registry.json` v1 + writer lock + compact GC, mmapped hash buckets and on-demand `files.bin` dictionary, S3-conditional registry publish, CI, MinIO lake harness, broad unit/E2E suite.
- **Still out of scope:** Glue/Nessie (explicit unsupported), multi-writer beyond flock + S3 If-Match, lake-wide SQL, catalog-committed Iceberg position files from Needle. `--covering` is listen-shaped only (refused on generic schemas). `forget` only hides keys in Needle. There is no `needle delete`.
- **Residual RAM on point lookup:** registry fragment-id list, fragment manifests (without `files[]` when `files.bin` exists), mmap handles for `files.bin` + one bucket, decoded dict records for files that key names, forgotten-key set, that key’s postings. Not the whole file dictionary.
- **Custom Parquet prep** (ZSTD multi-frame pages, skippable alignment, interleaving) uses a low-level writer so layouts live **inside** `.parquet` files readable by Arrow.

Apache-2.0.

---

## References

- Will Edwards, Spotify Engineering - [*Indexing the Data Lake for Online Point Queries*](https://engineering.atspotify.com/2026/7/indexing-the-data-lake-for-online-point-queries) (2026)
