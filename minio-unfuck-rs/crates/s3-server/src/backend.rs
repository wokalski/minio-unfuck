//! S3 backend implementation using s3s traits
//!
//! Read-only: list_buckets, list_objects_v2, get_object, head_object.
//! All write operations return NotImplemented (default).

use std::sync::Mutex;

use async_trait::async_trait;
use s3s::dto::*;
use s3s::s3_error;
use s3s::{S3Request, S3Response, S3Result, S3};

use mfu_core::db::{MetadataDb, StoredObject};
use mfu_core::erasure;
use mfu_core::raw_io::{DeviceReader, PreadDeviceReader};
use mfu_core::types::ObjectMeta;

pub struct MfuS3Backend {
    db: Mutex<MetadataDb>,
    reader: PreadDeviceReader,
}

impl MfuS3Backend {
    pub fn new(db: MetadataDb, reader: PreadDeviceReader) -> Self {
        Self {
            db: Mutex::new(db),
            reader,
        }
    }
}

#[async_trait]
impl S3 for MfuS3Backend {
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let db = self.db.lock().unwrap();
        let buckets = db.list_buckets().map_err(|e| {
            tracing::error!("list_buckets: {}", e);
            s3_error!(InternalError)
        })?;

        let bucket_list: Vec<Bucket> = buckets
            .into_iter()
            .map(|name| Bucket {
                name: Some(name),
                creation_date: Some(Timestamp::from(time::OffsetDateTime::UNIX_EPOCH)),
                bucket_region: None,
            })
            .collect();

        let output = ListBucketsOutput {
            buckets: Some(bucket_list),
            owner: None,
            continuation_token: None,
            prefix: None,
        };
        Ok(S3Response::new(output))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = input.bucket.as_str();
        let prefix = input.prefix.as_deref().unwrap_or("");
        let marker = input.continuation_token.as_deref().unwrap_or("");
        let delimiter = input.delimiter.as_deref().unwrap_or("");
        let max_keys = input.max_keys.unwrap_or(1000);

        let db = self.db.lock().unwrap();
        let objects = db
            .list_objects(bucket, prefix, marker, delimiter, max_keys)
            .map_err(|e| {
                tracing::error!("list_objects: {}", e);
                s3_error!(InternalError)
            })?;

        let contents: Vec<Object> = objects
            .iter()
            .map(|obj| Object {
                key: Some(obj.key.clone()),
                size: Some(obj.size),
                e_tag: Some(obj.etag.clone()),
                last_modified: Some(Timestamp::from(time::OffsetDateTime::UNIX_EPOCH)),
                ..Default::default()
            })
            .collect();

        let is_truncated = contents.len() as i32 >= max_keys;
        let next_token = if is_truncated {
            contents.last().map(|o| o.key.clone().unwrap_or_default())
        } else {
            None
        };

        let output = ListObjectsV2Output {
            contents: Some(contents),
            is_truncated: Some(is_truncated),
            next_continuation_token: next_token,
            name: Some(bucket.to_string()),
            prefix: Some(prefix.to_string()),
            max_keys: Some(max_keys),
            key_count: Some(objects.len() as i32),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let bucket = input.bucket.as_str();
        let key = input.key.as_str();

        let db = self.db.lock().unwrap();
        let obj = db.get_object(bucket, key).map_err(|e| {
            tracing::error!("head_object: {}", e);
            s3_error!(InternalError)
        })?;

        let obj = obj.ok_or_else(|| s3_error!(NoSuchKey))?;

        let content_type: Option<mime::Mime> = obj
            .content_type
            .as_deref()
            .and_then(|s| s.parse().ok());

        let output = HeadObjectOutput {
            content_length: Some(obj.size),
            content_type,
            e_tag: Some(obj.etag.clone()),
            last_modified: Some(Timestamp::from(time::OffsetDateTime::UNIX_EPOCH)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let bucket = input.bucket.as_str();
        let key = input.key.as_str();

        let stored = {
            let db = self.db.lock().unwrap();
            db.get_object(bucket, key).map_err(|e| {
                tracing::error!("get_object db: {}", e);
                s3_error!(InternalError)
            })?
        };
        let stored = stored.ok_or_else(|| s3_error!(NoSuchKey))?;

        // Convert stored object to ObjectMeta for erasure decoding
        let meta = stored_to_meta(&stored);

        // Create shard reader backed by raw device reader + db extent lookups
        let db = self.db.lock().unwrap();
        let db_shard_reader = DbShardReader {
            db: &*db,
            reader: &self.reader,
        };

        // Decode the object
        let data =
            erasure::decode_object(&db_shard_reader, &meta, &[]).map_err(|e| {
                tracing::error!("decode_object: {}", e);
                s3_error!(InternalError)
            })?;

        let content_type: Option<mime::Mime> = stored
            .content_type
            .as_deref()
            .and_then(|s| s.parse().ok());

        let body = s3s::Body::from(data.clone());
        let output = GetObjectOutput {
            body: Some(StreamingBlob::new(body)),
            content_length: Some(data.len() as i64),
            content_type,
            e_tag: Some(stored.etag.clone()),
            last_modified: Some(Timestamp::from(time::OffsetDateTime::UNIX_EPOCH)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}

/// Shard reader that uses DuckDB extent lookups + raw device reads
struct DbShardReader<'a> {
    db: &'a MetadataDb,
    reader: &'a PreadDeviceReader,
}

impl<'a> erasure::ShardReader for DbShardReader<'a> {
    fn read_shard(
        &self,
        disk_index: usize,
        bucket: &str,
        key: &str,
        data_dir: &str,
        part_number: i32,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        // Look up device_id for this disk_index from cluster_disks
        let device_id = {
            let mut stmt = self.db.conn().prepare(
                "SELECT device_id FROM cluster_disks WHERE disk_index = ? AND device_id IS NOT NULL LIMIT 1",
            )?;
            match stmt.query_row(duckdb::params![disk_index as i32], |row| {
                row.get::<_, Option<i32>>(0)
            }) {
                Ok(Some(id)) => id,
                _ => return Ok(None),
            }
        };

        // Resolve shard path to inode
        let shard_rel_path = mfu_core::shard::shard_path(bucket, key, data_dir, part_number);
        let ino = match self.db.resolve_path(device_id, &format!("/{}", shard_rel_path))? {
            Some(ino) => ino,
            None => return Ok(None),
        };

        // Get extents
        let extents = self.db.get_extents(device_id, ino)?;
        if extents.is_empty() {
            return Ok(None);
        }

        let size = self
            .db
            .get_inode_size(device_id, ino)?
            .unwrap_or(0);
        if size == 0 {
            return Ok(None);
        }

        let data = self
            .reader
            .read_file(device_id as usize, &extents, size as u64)?;
        Ok(Some(data))
    }
}

fn stored_to_meta(stored: &StoredObject) -> ObjectMeta {
    let mut meta = ObjectMeta::default();
    meta.bucket = stored.bucket.clone();
    meta.key = stored.key.clone();
    meta.size = stored.size;
    meta.mod_time = stored.mod_time;
    meta.etag = stored.etag.clone();
    meta.content_type = stored.content_type.clone().unwrap_or_default();
    meta.data_blocks = stored.data_blocks as usize;
    meta.parity_blocks = stored.parity_blocks as usize;
    meta.block_size = stored.block_size;
    meta.distribution = stored.distribution.iter().map(|&v| v as u8).collect();
    meta.pool_index = stored.pool_index;
    meta.set_index = stored.set_index;

    // Parse data_dir UUID
    if let Ok(uuid_bytes) = parse_uuid_string(&stored.data_dir) {
        meta.data_dir = mfu_core::types::Uuid16(uuid_bytes);
    }

    // Parse parts JSON
    if let Some(ref parts_json) = stored.parts {
        if let Ok(parts) = serde_json::from_str::<Vec<serde_json::Value>>(parts_json) {
            meta.parts = parts
                .iter()
                .map(|p| mfu_core::types::PartMeta {
                    number: p["number"].as_i64().unwrap_or(1) as i32,
                    size: p["size"].as_i64().unwrap_or(0),
                    actual_size: p["actual_size"].as_i64().unwrap_or(0),
                })
                .collect();
        }
    }

    meta
}

fn parse_uuid_string(s: &str) -> Result<[u8; 16], ()> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return Err(());
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| ())?;
    }
    Ok(bytes)
}
