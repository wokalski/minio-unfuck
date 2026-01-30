use std::collections::HashMap;
use std::fmt;

/// 16-byte UUID as used by MinIO (raw bytes, not standard UUID format)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Uuid16(pub [u8; 16]);

impl Uuid16 {
    /// Format as MinIO-style UUID string: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    pub fn to_uuid_string(&self) -> String {
        let b = &self.0;
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3],
            b[4], b[5],
            b[6], b[7],
            b[8], b[9],
            b[10], b[11], b[12], b[13], b[14], b[15],
        )
    }

    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 16]
    }
}

impl fmt::Debug for Uuid16 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Uuid16({})", self.to_uuid_string())
    }
}

impl fmt::Display for Uuid16 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_uuid_string())
    }
}

/// Metadata for a single part of a multipart object
#[derive(Debug, Clone)]
pub struct PartMeta {
    pub number: i32,
    pub size: i64,
    pub actual_size: i64,
}

/// Version type from xl.meta
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VersionType {
    #[default]
    Unknown = 0,
    Object = 1,
    DeleteMarker = 2,
    Legacy = 3,
}

impl VersionType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => VersionType::Object,
            2 => VersionType::DeleteMarker,
            3 => VersionType::Legacy,
            _ => VersionType::Unknown,
        }
    }

    pub fn is_object(&self) -> bool {
        matches!(self, VersionType::Object)
    }

    pub fn is_delete_marker(&self) -> bool {
        matches!(self, VersionType::DeleteMarker)
    }
}

/// Complete object metadata parsed from xl.meta
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    // Object identification
    pub bucket: String,
    pub key: String,

    // Version type (object, delete marker, legacy)
    pub version_type: VersionType,

    // Version info
    pub version_id: Uuid16,
    pub data_dir: Uuid16,

    // Erasure configuration
    pub data_blocks: usize,
    pub parity_blocks: usize,
    pub block_size: i64,
    pub erasure_index: usize, // 1-based
    pub distribution: Vec<u8>,

    // Parts
    pub parts: Vec<PartMeta>,

    // Object metadata
    pub size: i64,
    pub mod_time: i64, // nanos since epoch
    pub etag: String,
    pub content_type: String,
    pub user_meta: HashMap<String, String>,

    // Pool/set placement (filled in after cluster discovery)
    pub pool_index: i32,
    pub set_index: i32,
}

impl ObjectMeta {
    /// Data directory as UUID string
    pub fn data_dir_string(&self) -> String {
        self.data_dir.to_uuid_string()
    }

    /// Size of each shard for a given block: ceil(block_size / data_blocks)
    pub fn shard_size(&self) -> i64 {
        ceil_div(self.block_size, self.data_blocks as i64)
    }

    /// Total number of shards (data + parity)
    pub fn total_shards(&self) -> usize {
        self.data_blocks + self.parity_blocks
    }
}

impl Default for ObjectMeta {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            key: String::new(),
            version_type: VersionType::default(),
            version_id: Uuid16::default(),
            data_dir: Uuid16::default(),
            data_blocks: 0,
            parity_blocks: 0,
            block_size: 0,
            erasure_index: 0,
            distribution: Vec::new(),
            parts: Vec::new(),
            size: 0,
            mod_time: 0,
            etag: String::new(),
            content_type: String::new(),
            user_meta: HashMap::new(),
            pool_index: 0,
            set_index: 0,
        }
    }
}

/// A physical extent on a device (for extent-based raw I/O)
#[derive(Debug, Clone, Copy)]
pub struct Extent {
    pub logical_offset: i64,  // byte offset within the file
    pub physical_offset: i64, // byte offset on the device
    pub length: i64,          // extent length in bytes
}

/// Cluster topology: all pools, each with erasure sets of disks
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub pools: Vec<PoolConfig>,
}

impl ClusterConfig {
    pub fn total_sets(&self) -> usize {
        self.pools.iter().map(|p| p.sets.len()).sum()
    }
}

/// A single pool within a cluster
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub pool_id: String,
    pub pool_index: usize,
    pub sets: Vec<Vec<DiskInfo>>,
}

/// Information about a single disk in the cluster
#[derive(Debug, Clone)]
pub struct DiskInfo {
    pub uuid: String,
    pub pool_index: usize,
    pub set_index: usize,
    pub disk_index: usize,
    pub pool_id: String,
    pub device_id: Option<usize>, // index into the device list, set after mapping
}

/// Integer ceiling division
pub fn ceil_div(a: i64, b: i64) -> i64 {
    (a + b - 1) / b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid16_string() {
        let uuid = Uuid16([
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ]);
        assert_eq!(uuid.to_uuid_string(), "12345678-9abc-def0-1122-334455667788");
    }

    #[test]
    fn test_ceil_div() {
        assert_eq!(ceil_div(1048576, 11), 95326);
        assert_eq!(ceil_div(10, 3), 4);
        assert_eq!(ceil_div(9, 3), 3);
    }

    #[test]
    fn test_shard_size() {
        let meta = ObjectMeta {
            block_size: 1048576,
            data_blocks: 11,
            ..Default::default()
        };
        assert_eq!(meta.shard_size(), 95326);
    }
}
