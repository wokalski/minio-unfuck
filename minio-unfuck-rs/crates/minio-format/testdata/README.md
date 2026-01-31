# Test Fixtures

Test data files in this directory are sourced from the MinIO project
(https://github.com/minio/minio) for compatibility testing purposes.

MinIO is licensed under AGPL v3. These binary test data files (xl.meta,
shard files) are used solely for testing parser compatibility and are
not source code subject to AGPL copyleft requirements.

## Source Locations

### xlmeta/

xl.meta fixtures for testing the xlmeta parser:
- Source: https://github.com/minio/minio/tree/master/cmd/testdata

### cicd-corpus/

Complete erasure-coded test corpus with shard files:
- Source: https://github.com/minio/minio/tree/master/buildscripts/cicd-corpus

## Downloading Fixtures

To download the fixtures from MinIO's repository:

```bash
# From the minio-format crate directory
cd testdata

# xl.meta fixtures
curl -LO --output-dir xlmeta https://raw.githubusercontent.com/minio/minio/master/cmd/testdata/xl.meta
curl -LO --output-dir xlmeta https://raw.githubusercontent.com/minio/minio/master/cmd/testdata/xl-many-parts.meta

# cicd-corpus (complete erasure corpus)
for disk in disk1 disk2 disk3 disk4 disk5; do
  mkdir -p cicd-corpus/$disk/bucket/testobj/2b4f7e41-df82-4a5e-a3c1-8df87f83332f
  curl -L "https://raw.githubusercontent.com/minio/minio/master/buildscripts/cicd-corpus/$disk/bucket/testobj/2b4f7e41-df82-4a5e-a3c1-8df87f83332f/part.1" \
    -o "cicd-corpus/$disk/bucket/testobj/2b4f7e41-df82-4a5e-a3c1-8df87f83332f/part.1" 2>/dev/null || true
done

for disk in disk2 disk3 disk4 disk5; do
  curl -L "https://raw.githubusercontent.com/minio/minio/master/buildscripts/cicd-corpus/$disk/bucket/testobj/xl.meta" \
    -o "cicd-corpus/$disk/bucket/testobj/xl.meta" 2>/dev/null || true
done
```
