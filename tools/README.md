# Local MinIO binaries (linux-amd64)

```bash
curl -fsSL -o minio https://dl.min.io/server/minio/release/linux-amd64/minio
curl -fsSL -o mc    https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x minio mc
```

Then: `needle minio-up` (data dir `/workspace/rap-minio-data`, API `127.0.0.1:9000`).
Credentials: `minioadmin` / `minioadmin` (local-only).
