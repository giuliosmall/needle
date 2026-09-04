# Needle

**Point queries on your data lake - without copying it into a KV store.**

Needle is an external **key → file + page** index over ordinary Parquet. Look up a key, issue a handful of precise object-storage Range GETs, decode only those pages. Same files BigQuery / Spark / DuckDB already scan; no second copy of the data.

Inspired by Spotify’s [Random Access Parquet (RAP)](https://engineering.atspotify.com/2026/7/indexing-the-data-lake-for-online-point-queries) write-up. This repo is a faithful Rust recreation + local MinIO lake stress harness.

```bash
cargo build --release
./target/release/needle demo --key user_0042
```

---

## Why it exists

Online services and agents need **per-key** reads (one user, one entity) at interactive latency. Lakes are cheap and huge; distributed SQL is built for scans. The expensive part is rarely the disk - it’s the **dependent discovery chain** inside each Parquet file:

```mermaid
flowchart LR
  Q["Point query"] --> L["List / prune files"]
  L --> F["Fetch footer"]
  F --> RG["Parse row groups"]
  RG --> K["Scan key column"]
  K --> P["Page index"]
  P --> V["Fetch value pages"]
```

Each arrow is another round-trip. Needle collapses that to: **index lookup → ranged reads**.

```mermaid
flowchart LR
  Q["Point query"] --> I["Needle index O(1)"]
  I --> M["Cached page locations"]
  M --> R["Parallel Range GETs"]
  R --> D["Decode matching rows"]
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

```mermaid
flowchart TB
  subgraph Lake["Object store / MinIO"]
    P1["Parquet part-000.parquet"]
    P2["Parquet part-001.parquet"]
    Pn["more parts"]
  end
  subgraph NeedleIdx["Needle index"]
    B0["bucket_000"]
    B1["bucket_001"]
    Bn["more buckets"]
  end
  user_42["key = user_42"] --> B1
  B1 -->|"file=7 rows page_locs"| P2
  P2 -->|"Range GET about 2 KiB"| Out["Rows for user_42"]
```

### Reader path

1. Hash the key → load the one index bucket (lazy).
2. Resolve rows → page byte ranges (OffsetIndex and/or index-stored `page_locs`).
3. Issue **parallel** Range GETs (local file or S3/MinIO).
4. Decode only those pages; return rows (optional offset/limit).

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
needle compact --index data/rap-index --fragment compact-001
needle verify --index data/rap-index
```

`forget` suppresses keys from Needle lookups (sticky across later index fragments). It does not rewrite Parquet. Stop or let `needled` reload after compact/forget (it watches `registry.json`).

`--format json` (or `--json`) prints one JSON object to stdout. Point queries load
only the index buckets for that key. `needle query --help` / `needle index --help` list
the filter and column flags.

### HTTP daemon

Serve point queries over HTTP (optional `--lazy-buckets`):

```bash
needled --index data/rap-index --bind 127.0.0.1:7780
# or
needle daemon --index data/rap-index
```

`GET http://127.0.0.1:7780/v1/query?key=user_0042`

### Iceberg

Index an Apache Iceberg table into Needle fragments:

```bash
needle iceberg-index --table /path/to/iceberg/table --index data/rap-index --key-column user_id --covering
```

### SQL

Run SQL over the hits for a single key (`hits` table):

```bash
needle sql --index data/rap-index --key user_0042 --sql "SELECT track_uri, count(*) AS n FROM hits GROUP BY track_uri ORDER BY n DESC"
```

### MinIO lake (optional stress)

Needle can speak S3 Range GET against a **local** MinIO (no cloud account):

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

```mermaid
sequenceDiagram
  participant Client
  participant Needle
  participant Index as Index buckets
  participant Store as Object store
  Client->>Needle: query user_42
  Needle->>Index: lookup index bucket
  Index-->>Needle: file rows page_locs
  par column pages
    Needle->>Store: Range GET page A
    Needle->>Store: Range GET page B
    Needle->>Store: Range GET page C
  end
  Store-->>Needle: small page payloads
  Needle-->>Client: rows and covering aggregates
```

**Two stress axes this repo exercises:**

1. **Many files** - index picks 1 of N objects (N → 10⁵–10⁶).
2. **Fat files** - Range GET pages so `bytes_ranged / file_size` ≪ 1%.

---

## Project layout

```
src/
  main.rs              CLI
  index.rs             External Needle / RAP index
  metadata.rs          Footer + OffsetIndex → page ranges
  query.rs             Point query + naive baseline
  writer.rs            Sample lake writers (sorted / cogrouped / …)
  storage.rs           RangeReader trait (local + HTTP)
  s3.rs                S3 SigV4 + Range GET (MinIO path-style HTTP, AWS TLS virtual-host)
  lake.rs              Lake generate / index / query / stress
  secondary.rs         Hash + sorted secondary indexes
  parquet_lowlevel/    Custom page writer (frames, align, interleaved, paged)
tests/                 Unit-adjacent E2E + MinIO smoke
NOTES.md               Article mapping & fidelity notes
```

---

## Status / honesty

- **Implemented:** external index, page-accurate ranged reads, covering + secondary indexes, HTTP/S3 Range (TLS + virtual-host), Iceberg incremental index, compact/forget/verify, `needled` + SQL over key hits, MinIO lake harness, broad unit/E2E suite.
- **Custom Parquet prep** (ZSTD multi-frame pages, skippable alignment, interleaving) uses a low-level writer so layouts live **inside** `.parquet` files readable by Arrow.
- This is an R&D recreation, not Spotify’s production RAP.

Apache-2.0.

---

## References

- Will Edwards, Spotify Engineering - [*Indexing the Data Lake for Online Point Queries*](https://engineering.atspotify.com/2026/7/indexing-the-data-lake-for-online-point-queries) (2026)
