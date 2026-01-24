# minio-unfuck

A minimal, read-only S3 server that reads directly from MinIO's erasure-coded disk format. No MinIO dependency.

## Why?

You used MinIO. Now you regret it. Your data is locked in their proprietary erasure format across 16+ disks. You just want to read your files and migrate somewhere sane. You could used MinIO but you are fed up with its slowness.

This tool:
- Reads MinIO's `xl.meta` format and erasure-coded shards directly
- Serves a read-only S3 API (LIST, GET, HEAD)
- Caches metadata in SQLite for fast listing
- Has zero MinIO code - just ~2000 lines of Go

## Requirements

- Go 1.22+
- Access to your MinIO disk directories
- CGO enabled (for SQLite)

## Usage

```bash
# Build
go build -o minio-unfuck .

# Run (point to directory containing your MinIO disks)
./minio-unfuck -root /path/to/disks -db metadata.db

# Access via S3
aws --endpoint-url http://localhost:9000 s3 ls
aws --endpoint-url http://localhost:9000 s3 cp s3://mybucket/myfile.txt ./
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-root` | `.disks` | Directory containing MinIO disk folders |
| `-db` | `metadata.db` | SQLite database path for metadata cache |
| `-addr` | `:9000` | Listen address |
| `-sync` | `true` | Sync metadata from disk on startup |

## How it works

1. **Startup**: Reads `format.json` from each disk to discover cluster configuration (multiple pools and erasure sets)
2. **Sync**: Walks one disk per erasure set per pool, parses `xl.meta` files, caches metadata in SQLite
3. **LIST/HEAD**: Served from SQLite (fast)
4. **GET**: Reads 11 data shards in parallel, reconstructs if needed using Reed-Solomon

Supports expanded MinIO installations with multiple pools (added after initial setup).

## Disk layout expected

```
/path/to/disks/
├── storage1/
│   ├── .minio.sys/format.json
│   └── mybucket/myobject/xl.meta
├── storage2/
│   └── ...
└── storage16/
    └── ...
```

The disk directory names don't matter - ordering is determined from `format.json`.

## Limitations

- **Read-only**: No PUT, DELETE, or bucket creation
- **No versioning**: Only reads latest version
- **No multipart upload**: But reads existing multipart objects fine
- **No encryption**: Assumes unencrypted data

## Migrating off MinIO

```bash
# Start the server
./minio-unfuck -root /mnt/minio-disks -db metadata.db &

# Sync to a new location using aws cli, rclone, etc.
aws --endpoint-url http://localhost:9000 s3 sync s3://mybucket /new/storage/

# Or use rclone for better performance
rclone sync :s3,endpoint=http://localhost:9000:mybucket /new/storage/
```

## Building from source

```bash
git clone https://github.com/wokalski/minio-unfuck
cd minio-unfuck
go build -o minio-unfuck .
```

## License

MIT
