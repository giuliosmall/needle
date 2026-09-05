# needled HTTP JSON (frozen v1)

JSON contract for the `needled` daemon (`needle daemon`). Current success and error fields are **frozen**. Additive keys are allowed; removing or renaming fields, or changing meaning, requires a new `/v2/` prefix.

Auth, TLS, and bind policy are unchanged: loopback may stay plaintext with no token; non-loopback requires `--tls-cert`/`--tls-key` and `--token` (or `--insecure`).

## Endpoints

All listed routes are `GET`. Other methods return `405` with `{"error":"method_not_allowed","message":"method not allowed"}`. Unknown paths return `404`.

| Method | Path | Auth |
|--------|------|------|
| GET | `/health` | none |
| GET | `/v1/query` | Bearer when a token is configured |
| GET | `/v1/explain` | Bearer when a token is configured |
| GET | `/v1/stats` | Bearer when a token is configured |

## Auth

When `--token` / `NEEDLED_TOKEN` is set, `/v1/query`, `/v1/explain`, and `/v1/stats` require:

```
Authorization: Bearer <token>
```

`/health` is always unauthenticated.

Failed auth is HTTP `401`:

```json
{"error":"unauthenticated","message":"unauthorized"}
```

`message` is `"unauthorized"` when the header is missing and `"invalid token"` when the bearer does not match. Bodies do not include index paths, keys, fragment ids, or the configured token.

## Query parameters (`/v1/query`, `/v1/explain`)

| Param | Meaning |
|-------|---------|
| `key` | Lookup key (required). Missing/empty → `400` `bad_request`. |
| `offset` | Skip this many values (default `0`). |
| `limit` | Max values to return. |
| `columns` | Comma-separated projection. |
| `since_ms` | Inclusive lower bound on row timestamp (unix ms). |
| `until_ms` | Inclusive upper bound on row timestamp (unix ms). |
| `covering_only` | `1`/`true`/`yes`/`on` — index covering only, no Range-GET. |
| `min_listens` | Drop entries whose covering listen count is below this. |
| `verify` | File-identity check before IO. Default `true`. `verify=0` is unsafe. |

`/v1/stats` takes no query parameters.

## Success JSON (frozen fields)

Additive keys are allowed on every object below.

### `GET /health` — `200`

```json
{"ok": true}
```

### `GET /v1/query` — `200`

```json
{
  "key": "user_0000",
  "rows": [{"user_id": "user_0000"}],
  "covering": ["…"],
  "covering_values": [
    {
      "file": "…",
      "value_count": 1,
      "listen_count": 1,
      "total_duration_ms": 0,
      "min_ts": null,
      "max_ts": null
    }
  ],
  "timings": {
    "index_lookup_ms": 0,
    "metadata_resolve_ms": 0,
    "ranged_read_ms": 0,
    "decode_extract_ms": 0,
    "total_ms": 0
  },
  "totals": {
    "rows": 1,
    "value_count": 1,
    "bytes_ranged": 0,
    "pages_touched": 0,
    "files_touched": 1,
    "skipped_by_predicate": 0,
    "offset": 0,
    "limit": null
  }
}
```

`rows` is the Arrow projection as JSON objects (not listen-schema-only). `value_count` on covering objects is the generic alias of `listen_count`.

### `GET /v1/explain` — `200`

Frozen keys: `key`, `bucket`, `num_entries`, `num_entries_after_predicates`, `files`, `covering`, `page_descriptions`, `estimated_bytes`, `estimated_range_gets`, `covering_only`, `columns`, `since_ms`, `until_ms`, `skipped_by_predicate`.

### `GET /v1/stats` — `200`

Frozen keys: `index`, `lazy_buckets`, `num_files`, `num_fragments`, `fragments`.

Each `fragments[]` object: `fragment_id`, `num_buckets`, `num_files`, `created_at`, `key_columns`, `value_columns`.

When the full index is resident, also `num_keys`, `num_entries`, `fragment_id`, `num_buckets`. Under `--lazy-buckets`, `fragment_id` / `num_buckets` come from the first fragment when present.

## Errors

Every error body is:

```json
{"error": "<code>", "message": "<human>"}
```

plus optional extra fields (for example `path`, `stored`, `live` on identity failures). `error` is a stable machine code; `message` is human-readable and may change.

| HTTP | `error` code | When |
|------|----------------|------|
| 401 | `unauthenticated` | Missing or invalid bearer |
| 400 | `bad_request` | Missing `key`, invalid query param |
| 404 | `not_found` | Unknown path |
| 405 | `method_not_allowed` | Not `GET` |
| 500 | `stale_file_identity` | Indexed local file size/ETag/mtime no longer matches |
| 500 | `s3_identity_mismatch` | Same check on a remote (`s3://` / HTTP) object |
| 500 | `s3_checksum_mismatch` | Remote checksum/ETag payload mismatch (string-matched when present) |
| 500 | `iceberg_unsupported_deletes` | Iceberg delete encoding Needle cannot apply |
| 500 | `catalog_error` | Iceberg REST or Hadoop catalog/metadata/snapshot/manifest load failed |
| 500 | `internal` | Any other server error |

`stale_file_identity` extra fields match the query-layer JSON: `path`, `stored` (`etag`, `size`, `mtime_ms`), `live` (same shape, or `null`).

These codes are frozen. Needled maps them by structured JSON (query identity errors) or by string match / downcast of the underlying error.