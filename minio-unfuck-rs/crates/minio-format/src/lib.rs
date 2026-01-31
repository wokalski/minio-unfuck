//! minio-format: Parser for MinIO internal formats
//!
//! This library provides parsers and decoders for MinIO's internal data formats:
//! - `xl.meta`: Object metadata (msgpack binary format)
//! - `format.json`: Cluster topology and disk configuration
//! - Shard files: Erasure-coded data with HighwayHash256 bitrot verification
//! - Reed-Solomon erasure decoding
//!
//! # Example
//!
//! ```ignore
//! use minio_format::{parse_xlmeta, parse_format, build_cluster_config};
//! use minio_format::{decode_object, FsShardReader};
//!
//! // Parse xl.meta
//! let meta = parse_xlmeta(&xlmeta_bytes)?;
//!
//! // Parse cluster topology
//! let fmt = parse_format(&format_json_bytes)?;
//!
//! // Decode erasure-coded objects
//! let reader = FsShardReader { disk_paths: vec![...] };
//! let data = decode_object(&reader, &meta, &[])?;
//! ```

pub mod types;
pub mod xlmeta;
pub mod format;
pub mod shard;
pub mod erasure;

// Re-exports for convenient access
pub use types::{
    ceil_div, ClusterConfig, DiskInfo, ObjectMeta, PartMeta, PoolConfig, Uuid16, VersionType,
};
pub use xlmeta::parse as parse_xlmeta;
pub use format::{build_cluster_config, parse_format, DiskFormat, XLFormat};
pub use shard::{read_shard_all_blocks, read_shard_block, shard_path, HASH_SIZE};
pub use erasure::{decode_object, FsShardReader, ShardReader};
