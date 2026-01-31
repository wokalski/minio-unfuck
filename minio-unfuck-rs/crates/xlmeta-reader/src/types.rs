//! Shared types for xlmeta-reader

use mfu_core::types::Extent;
use serde::{Deserialize, Serialize};

/// ClickHouse row for the objects table
#[derive(clickhouse::Row, Serialize, Debug, Clone)]
pub struct ChObject {
    pub bucket: String,
    pub key: String,
    pub size: i64,
    pub mod_time: i64,
    pub etag: String,
    pub content_type: String,
    pub compression: String,
    pub data_blocks: i32,
    pub parity_blocks: i32,
    pub block_size: i64,
    pub data_dir: String,
    pub distribution: Vec<i32>,
    pub parts_json: String,
    pub xlmeta_device_id: i32,
    pub xlmeta_ino: i64,
}

/// Row returned from the extents query
#[derive(clickhouse::Row, Deserialize, Debug)]
pub struct ExtentRow {
    pub bucket: String,
    pub key: String,
    pub xlmeta_ino: i64,
    #[allow(dead_code)]
    pub data_dir_ino: i64,
    pub logical_offset: i64,
    pub physical_offset: i64,
    pub length: i64,
    pub extent_count: u64,
}

/// A complete xlmeta ready to be read from disk
#[derive(Debug)]
pub struct ReadyXlmeta {
    pub bucket: String,
    pub key: String,
    pub device_id: i32,
    pub ino: i64,
    pub extents: Vec<Extent>,
    pub file_size: u64,
}

/// Accumulated state for a single xl.meta file during extent collection
#[derive(Debug)]
pub struct PendingXlmeta {
    pub bucket: String,
    pub key: String,
    pub extent_count: u64,
    pub extents: Vec<Extent>,
}

/// Result from the reader thread
#[derive(Debug)]
pub enum ReaderResult {
    Parsed(ChObject),
    Error {
        bucket: String,
        key: String,
        error: String,
    },
}
