//! Types for request batching

use std::time::Instant;

use mfu_core::types::Extent;
use tokio::sync::oneshot;

use crate::db::StoredObject;

/// A pending GetObject request waiting to be batched
pub struct PendingRequest {
    pub bucket: String,
    pub key: String,
    pub response_tx: oneshot::Sender<Result<Vec<u8>, BatchError>>,
    pub received_at: Instant,
}

/// Error type for batch operations
#[derive(Debug, Clone)]
pub enum BatchError {
    NoSuchKey,
    Internal(String),
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchError::NoSuchKey => write!(f, "NoSuchKey"),
            BatchError::Internal(msg) => write!(f, "Internal: {}", msg),
        }
    }
}

impl std::error::Error for BatchError {}

/// Plan for reading a single shard from disk
#[derive(Debug, Clone)]
pub struct ShardReadPlan {
    /// Links back to the requesting object
    pub request_id: u64,
    /// Part number (1-based)
    pub part_number: i32,
    /// Disk index within the erasure set (0-based, 0..15 for 16-disk set)
    /// This is the position in the distribution array, NOT the erasure shard index
    pub disk_index: usize,
    /// Device to read from
    pub device_id: usize,
    /// Extents to read
    pub extents: Vec<Extent>,
    /// File size
    pub file_size: u64,
    /// First physical offset (for sorting reads)
    pub first_phys_offset: i64,
}

/// Result of reading a shard
#[derive(Debug, Clone)]
pub struct ShardReadResult {
    pub request_id: u64,
    pub part_number: i32,
    /// Disk index within the erasure set (0-based, 0..15 for 16-disk set)
    pub disk_index: usize,
    pub data: Option<Vec<u8>>,
}

/// Complete plan for reading an object
pub struct ObjectReadPlan {
    pub request_id: u64,
    pub meta: StoredObject,
    pub shard_plans: Vec<ShardReadPlan>,
    pub response_tx: oneshot::Sender<Result<Vec<u8>, BatchError>>,
}
