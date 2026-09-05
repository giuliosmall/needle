# NOTES - Article details & implementation fidelity

Source article:
https://engineering.atspotify.com/2026/7/indexing-the-data-lake-for-online-point-queries
(Will Edwards, Spotify Engineering, July 2026)

## Problem statement (article)

- Exabytes in the GCS data lake vs petabytes in Bigtable for online use-cases.
- Interactive / AI-agent point queries need per-key lookup + pagination at low latency.
- Distributed SQL (Trino, BigQuery) adds seconds of scheduling/planning even for one row.
- Object storage itself is fast (GCS 30–100ms; Rapid / S3 Express single-digit ms) -
  the bottleneck is the **dependent read chain** inside Parquet discovery.

## How engines find a needle today (article)

1. Partition pruning / key bucketing from filenames → reduce candidate files.
2. Bloom filters on key column → discard empty files.
3. Still must, per remaining file: fetch footer → parse row groups → scan key column →
   use column/page indexes → fetch value pages. Multiple round-trips per file/column.

## RAP approach (article) - what we recreated

> "Instead of scanning, RAP looks up. An external index maps each key directly to each
> file and row number… resolve the row number to page locations using cached file
> metadata, and issue ranged reads… These reads can be issued in parallel."

### External index fields (implemented)

| Field | Notes |
|-------|-------|
| key | `user_id` string; compound keys can be encoded later |
| file | dictionary-encoded ordinal into fragment file list |
| row numbers | absolute 0-based rows within that file |
| value count | enables pagination without reading data |
| covering (extra) | hoisted `listen_count` + `total_duration_ms` (flat rows, blob JSON, **cogrouped nested list length + duration sum**) |
| page_locs | one-page-per-key locations stored **in the entry** |
| frame_locs | ZSTD frame offset+size per column (prepared layout) |
| contiguous | interleaved multi-column span for one ranged read |
| aligned | key fetch starts on a 4KB boundary |

Index is **append-only fragments**; large indexes **hash-bucket** by key.
We write both **JSONL** (inspectable) and **bincode** (compact) per bucket.

### Reader path (implemented)

1. O(1) in-memory HashMap lookup after loading bucketed fragments
2. Prefer **index-stored** `page_locs` / `frame_locs` / `contiguous` when present;
   else `MetaCache` footer + OffsetIndex (`PageLocation`)
3. `page_ranges_for_rows` → coalesced byte ranges; `RangeReader` does seek or HTTP `Range:`
4. Decode via arrow-rs `RowSelection` (Parquet) or ZSTD-frame PLAIN decode (prepared);
   parallelised with rayon
5. Pagination: `value_count` + `--offset` / `--limit` over index row lists
6. Demo timings separate index / metadata / ranged-read / decode phases

### Optimisations

| Optimisation | Article benefit | Our status |
|--------------|-----------------|------------|
| Sorting by key | Fewer pages per key | **Implemented** (`--mode sorted`) |
| Co-grouping | One row per key | **Implemented** (`--mode cogrouped`) |
| Hash bucketing files | One file per key per partition | **Implemented** in sorted/cogrouped writer |
| Coarser partitioning | Fewer files across time | N/A in sample (single "day") |
| One page per key | Entire page = result; locs in index | **Implemented** (`--mode one-page-per-key` / `--one-page-per-key`) |
| ZSTD frame resets | O(1) within conventional pages | **Implemented inside Parquet** (`--mode zstd-frames`) |
| Storage alignment | Avoid block straddles | **Implemented inside Parquet** (`--mode aligned`, ZSTD skippable pad to 4KB) |
| Blobs / Variants | Single column read | **Implemented** (`--mode blob`, JSON + binary payload) |
| Interleaving columns | Single contiguous multi-col read | **Implemented inside Parquet** (`--mode interleaved`) |
| Covering index | Zero storage reads for hoisted fields | **Implemented** (`--covering`; cogrouped hoists nested list aggregates) |
| Secondary indexes | Multi-dimension access paths | **Implemented** (`needle index --secondary track_uri`; hash + sorted) |
| HTTP Range | Object-store ranged reads | **Implemented** (`LocalFile` + `HttpRange`, `needle serve`, prove path) |
| MinIO / S3 Range | Path-style GetObject Range | **Implemented** (`s3.rs` SigV4 + STS + retries + checksums; MinIO path-style and AWS virtual-host TLS) |
| Fat multi-page lake | Range-GET pages ≪ fat object | **Implemented** (`paged.rs`, `lake-generate-fat`, `S3ChunkReader`) |
| Pagination | value_count + offset/limit | **Implemented** (`needle query --offset --limit`) |

## One page per key

Writer: one `RecordBatch` per key, `flush()` between keys, page/RG row-count limit =
key cardinality, large `data_page_size_limit` so the key stays in **one** page per
column. Index builder copies OffsetIndex locations into `page_locs` on each entry.
Query prefers those locs and skips footer page-index resolution.

Valid standard Parquet - analytics readers unchanged.

## ZSTD frame resets / alignment / interleaving (inside Parquet)

`src/parquet_lowlevel/` writes real `.parquet` files (PAR1, Thrift-compact page
headers, PLAIN values, ZSTD page codec, column chunks, single row group,
OffsetIndex, FileMetaData footer). arrow-rs `SerializedFileReader` opens them;
Arrow can decode the official column chunks.

| Article idea | Byte layout inside the file |
|--------------|-----------------------------|
| Separate ZSTD frame per key, PLAIN | One data page per column. Compressed payload = concatenated ZSTD frames (one per key). Index `frame_locs` = absolute file offset+size of each frame. |
| Skippable-frame pad to 4KB | Between keys, a ZSTD skippable frame (`0x184D2A50` + u32 size + zeros) so the next key's frame starts on `ALIGN_BLOCK=4096`. Conventional page decompress skips pads. |
| Interleave cols + skippable bridges | Official `track_uri` / `duration_ms` chunks still exist (spec: a column chunk is contiguous). Article intent is **inside** the `timestamp` page: for each key `[zstd(ts)][skippable zstd(track)][skippable zstd(dur)]`. RAP `contiguous` is that bundle. Sibling chunks duplicate PLAIN/ZSTD so analytics readers see a normal file. |

RAP reads frames/ranges from the `.parquet` itself (no `.rapz` / `.rapi`). A
`.frames.json` sidecar is only an index-build helper listing those offsets.

## Blobs / Variants

`--mode blob`: one row per key; `payload` (JSON Utf8) + `payload_bytes` (Binary).
Point query → one ranged read of the payload page; JSON expanded to listen rows.
`value_count` reflects listen count inside the document when covering is on.

## Secondary indexes

```text
fragments/<id>/secondary/<dim>/
  manifest.json
  hash/buckets/bucket_NNN.bin|.jsonl   # O(1) exact
  sorted/entries.bin|.jsonl            # range scans
```

`needle index --secondary track_uri` scans existing Parquet (no rewrite).
`needle query <track> --dimension track_uri` does exact lookup; sorted tree supports
`lookup_range`.

## HTTP Range

`storage::RangeReader` trait with `LocalFile` and `HttpRange` (raw TCP `Range:`).
`needle serve --root data/parquet` - tiny_http with 206 + Content-Range.
`needle query … --http http://127.0.0.1:PORT` issues ranged reads over HTTP and can
prove bytes match local (`prove_http_matches_local`).

## Remaining non-goals (not a general lake query engine)

- mmap / multi-TB lakes: file dictionary + working set stay in RAM; lazy buckets are the default (`--full-index` is opt-in)
- Glue / Nessie catalogs (Iceberg REST is the production path; Hadoop `metadata/*.json` is fallback)
- Multi-writer index beyond exclusive `flock`
- Lake-wide SQL (SQL is over rows for one lookup key)
- Physical Parquet / Iceberg deletes (`forget` only hides keys in Needle)
- Covering aggregates remain listen-shaped; `--covering` is refused on generic schemas
- Interleaving stays spec-valid by duplicating sibling columns in official
  chunks; RAP's one-read span is the skippable-bridged bundle in the host page
- No Bloom / partition pruning layer (out of scope - RAP replaces within-file scan)
- Cogrouped nested decode expands lists; page-range demo focuses on flat / blob / prepared
- S3 is SigV4 over raw TCP (STS session tokens, retries, checksums, default cred chain) rather than aws-sdk-s3

## Demo expectations

On a laptop/box with tens of thousands of rows, RAP vs naive speedup is large on
warm cache because RAP touches kilobytes. The architectural win (collapse dependent
round-trips; bytes read ≪ file size) is what matters for cloud object storage.

```bash
cargo run --release -- demo --key user_0042
cargo run --release -- demo-full --key user_0042
```

## Page size tuning (sorted baseline)

For the sorted RAP demo we intentionally:

- `set_write_batch_size(32)`
- `set_data_page_row_count_limit(32)`
- `set_data_page_size_limit(256)`
- disable dictionary / use PLAIN for predictable splits
- feed the writer 32-row RecordBatches

One-page-per-key overrides these with per-key limits + `flush()` at key boundaries.


## Lake generate fast path

`src/lake.rs` `lake-generate` used to call arrow-rs `ArrowWriter` once per tiny
object. At ~4 rows that is dominated by writer setup (schema, properties, page
index machinery) - measured ~100 obj/s vs ~700/s for a bare MinIO PUT.

Fast path (`src/parquet_lowlevel/tiny.rs`):

1. PLAIN + UNCOMPRESSED data pages written directly into a pooled `Vec<u8>`
   (`write_tiny_plain_into`). REQUIRED columns → payload is PLAIN values only.
2. Same listen schema (`user_id`, `timestamp`, `track_uri`, `duration_ms`).
   Footer via the existing Thrift-compact `file_metadata` helper with
   `CODEC_UNCOMPRESSED`. No OffsetIndex (index stores a whole-object `page_loc`).
3. Thread-local buffer + rayon PUTs. S3 client keeps the TCP connection
   (`Connection: keep-alive`) and uses `UNSIGNED-PAYLOAD` on PUT.
4. RAP file ordinal = generation index `i` (URI dictionary rebuilt after the
   parallel loop). Layout still `date=YYYY-MM-DD/bucket=NNN/part-XXXXXX.parquet`.

Objects remain valid Parquet (Arrow roundtrip in `tiny.rs` tests).

The first fast-path 10k run stayed at ~100 obj/s because **every** PUT of a
`date=…/bucket=…` key returned `403 SignatureDoesNotMatch` and fell back to
`mc cp`. The HTTP request-line encoded `=` as `%3D` while the SigV4 canonical
URI used the raw path. Signing `encoded_uri` (same as the request line) fixes
it: 10k in 4.8s (**2080 obj/s**, 0 mc fallbacks).


## Fat lake (MinIO pages vs objects)

Tiny lake (`lake-generate --files 1e6`): one RAP Range GET is often the **whole ~1 KiB object**.
That proves index → 1 file vs listing 1M keys. It does **not** prove page-granular Range.

Fat lake (`lake-generate-fat` / `--fat`):

- `write_paged_plain_into` flushes a PLAIN data page every `page_rows` (default 64) and
  writes OffsetIndex (`ColumnChunk.offset_index_offset`).
- Index entries store **per-column** `page_locs` (offset+size inside the file), never
  `column="*"` / whole-object spans.
- Keys sorted + `user_id % files` so one user is contiguous in **one** fat file.
- Query/bench report `file_size`, `bytes_ranged`, `ratio`, `#range requests`, rows,
  and a naive **full GET** of the candidate object.
- Decode path: `S3ChunkReader` implements parquet `ChunkReader` with Range GET
  (`get_bytes` / windowed `get_read`) so Arrow+RowSelection does not pull the object.
- High file counts: compact `[first_row]+value_count` rows, skip JSONL, rayon PUT,
  `load_index_for_keys` (one hash bucket) so 25.6M keys do not need a full HashMap.

Published (2026-09-03): **200,000 × ~250 KiB**, 512 pages/file, point query **1740 / 255,576 = 0.6808%**,
p50 **14.05 ms**, generate **238 s / 840 obj/s**. Index `data/rap-lake-index-fat-200k`.
See `data/FAT_LAKE_RESULTS.md`.
