# Needle index format (v1)

On-disk layout of an index root (`--index`):

```
<index-root>/
  registry.json
  .needle.lock              # exclusive flock while a writer publishes
  forgotten.jsonl           # sticky forgotten keys (not removed by compact)
  fragments/
    <id>/
      manifest.json
      buckets/
        bucket_000.jsonl
        bucket_000.bin
        …
```

## `registry.json`

Writers always emit an object:

```json
{
  "format_version": 1,
  "fragments": ["frag-001", "frag-002"]
}
```

| Field | Meaning |
|-------|---------|
| `format_version` | Major version. **FROZEN** at `1`. |
| `fragments` | Fragment ids, oldest first (apply order). |

A legacy JSON array of fragment-id strings is still accepted as v1. The next write migrates to the object form.

Readers reject any other major with `unsupported index format_version`.

Writers take a non-blocking exclusive `flock` on `.needle.lock` for the whole fragment publish plus `registry.json` tmp+rename. A second overlapping writer fails with `index lock`.

When the index root is `s3://`, `registry.json` is published with a conditional PUT: `If-None-Match: *` on create and `If-Match: <etag>` on update. A lost race is `s3_precondition_failed` (the object is left as valid v1 JSON).

Point lookup mmaps `bucket_{NNN}.bin` (or scans `bucket_{NNN}.jsonl`) for the hashed bucket only. `--full-index` deserializes every bucket into RAM. The file dictionary stays in RAM.

## Fragment directory

`fragments/<id>/manifest.json` describes one append-only fragment:

| Field | Meaning |
|-------|---------|
| `fragment_id` | Same as the directory name |
| `created_at` | RFC3339 timestamp |
| `files` | Data-file dictionary (entry `file` ordinals) |
| `num_buckets` | Hash buckets under `buckets/` |
| `note` | Optional (compact / forget / Iceberg snapshot) |
| `key_columns` / `value_columns` | Indexed columns |
| `iceberg_snapshot_id` | Iceberg snapshot, if any |
| `file_idents` | `{path, etag, size, mtime_ms}` for `needle verify` and STRICT query |
| `dropped_files` | Paths removed from the live set (Iceberg overwrite/expire) |
| `iceberg_delete_files` | Position/equality delete files applied when this fragment was built |

Buckets are `bucket_{NNN}.jsonl` (inspectable) and `bucket_{NNN}.bin` (bincode; preferred on load).

## Compatibility policy (frozen v1)

`format_version` 1 is **FROZEN**.

- Additive optional fields on manifests and entries with `serde` default are allowed without a major bump.
- Removing or renaming fields, changing the bucket hash, changing the registry object shape, or changing ordinal meaning requires a major bump (`format_version` 2).
- Readers reject unknown majors (`unsupported index format_version`).
- A legacy JSON array of fragment-id strings is still accepted as v1. The next write migrates to the object form.

Compact rewrites to one fragment, points `registry.json` at that id, and deletes unreferenced directories under `fragments/`. `forgotten.jsonl` is kept.
