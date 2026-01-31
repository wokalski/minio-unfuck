//! S3 backend implementation using s3s traits
//!
//! Read-only: list_buckets, list_objects_v2, get_object, head_object.
//! All write operations return NotImplemented (default).

use std::fs::File;
use std::sync::Arc;

use async_trait::async_trait;
use s3s::dto::*;
use s3s::s3_error;
use s3s::{S3Request, S3Response, S3Result, S3};
use tracing::info;

use crate::batch::{BatchError, PendingRequest, RequestBatcher};
use crate::cluster::ClusterConfig;
use crate::db;

#[allow(dead_code)]
pub struct MfuS3Backend {
    client: Arc<clickhouse::Client>,
    cluster: Arc<ClusterConfig>,
    device_fds: Vec<File>,
    batcher: Arc<RequestBatcher>,
}

impl MfuS3Backend {
    pub fn new(
        client: Arc<clickhouse::Client>,
        cluster: Arc<ClusterConfig>,
        device_fds: Vec<File>,
        batcher: Arc<RequestBatcher>,
    ) -> Self {
        Self {
            client,
            cluster,
            device_fds,
            batcher,
        }
    }
}

#[async_trait]
impl S3 for MfuS3Backend {
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = db::list_buckets(&self.client).await.map_err(|e| {
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

        let objects = db::list_objects(&self.client, bucket, prefix, marker, delimiter, max_keys)
            .await
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

        let obj = db::get_object(&self.client, bucket, key)
            .await
            .map_err(|e| {
                tracing::error!("head_object: {}", e);
                s3_error!(InternalError)
            })?;

        let obj = obj.ok_or_else(|| s3_error!(NoSuchKey))?;

        let content_type: Option<mime::Mime> = if obj.content_type.is_empty() {
            None
        } else {
            obj.content_type.parse().ok()
        };

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
        let bucket = input.bucket.clone();
        let key = input.key.clone();

        info!("GetObject: {}/{}", bucket, key);

        // Get object metadata for response headers
        let stored = db::get_object(&self.client, &bucket, &key)
            .await
            .map_err(|e| {
                tracing::error!("get_object db: {}", e);
                s3_error!(InternalError)
            })?;
        let stored = stored.ok_or_else(|| s3_error!(NoSuchKey))?;

        // Submit to batcher and wait for result
        let (tx, rx) = tokio::sync::oneshot::channel();
        let pending = PendingRequest {
            bucket,
            key,
            response_tx: tx,
            received_at: std::time::Instant::now(),
        };

        self.batcher.submit(pending).await.map_err(|e| {
            tracing::error!("batcher submit failed: {}", e);
            s3_error!(InternalError)
        })?;

        let data = rx.await.map_err(|_| {
            tracing::error!("batcher response channel closed");
            s3_error!(InternalError)
        })?.map_err(|e| {
            match e {
                BatchError::NoSuchKey => s3_error!(NoSuchKey),
                BatchError::Internal(msg) => {
                    tracing::error!("batch error: {}", msg);
                    s3_error!(InternalError)
                }
            }
        })?;

        let content_type: Option<mime::Mime> = if stored.content_type.is_empty() {
            None
        } else {
            stored.content_type.parse().ok()
        };

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
