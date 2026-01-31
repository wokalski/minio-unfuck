//! Cluster topology discovery and configuration
//!
//! Reads format.json files from each device to build a mapping between
//! device IDs (from our scanned partitions) and MinIO's disk ordering.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use anyhow::{Context, Result};
use mfu_core::types::Extent;
use serde::Deserialize;
use tracing::info;

use crate::db;

/// MinIO format.json structure
#[derive(Debug, Clone, Deserialize)]
pub struct DiskFormat {
    pub version: String,
    pub format: String,
    pub id: String, // Pool ID
    pub xl: XLFormat,
}

#[derive(Debug, Clone, Deserialize)]
pub struct XLFormat {
    pub version: String,
    #[serde(rename = "this")]
    pub this_disk: String, // This disk's UUID
    pub sets: Vec<Vec<String>>, // Disk UUIDs for each erasure set
    #[serde(rename = "distributionAlgo")]
    pub distribution_algo: String,
}

/// Pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub pool_id: String,
    pub pool_index: usize,
    pub sets: Vec<Vec<DiskInfo>>,
}

/// Information about a single disk
#[derive(Debug, Clone)]
pub struct DiskInfo {
    pub uuid: String,
    pub pool_index: usize,
    pub set_index: usize,
    pub disk_index: usize,
    pub device_id: Option<usize>,
}

/// Cluster configuration with bidirectional mappings
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub pools: Vec<PoolConfig>,
    /// device_id → (pool_idx, set_idx, disk_idx)
    device_to_disk: HashMap<usize, (usize, usize, usize)>,
    /// (pool_idx, set_idx, disk_idx) → device_id
    disk_to_device: HashMap<(usize, usize, usize), usize>,
}

impl ClusterConfig {
    /// Get disk position for a device
    pub fn device_position(&self, device_id: usize) -> Option<(usize, usize, usize)> {
        self.device_to_disk.get(&device_id).copied()
    }

    /// Get device_id for a disk position
    pub fn device_id(&self, pool_idx: usize, set_idx: usize, disk_idx: usize) -> Option<usize> {
        self.disk_to_device.get(&(pool_idx, set_idx, disk_idx)).copied()
    }

    /// Get device_id for a disk index within a specific pool/set
    /// distribution[disk_idx] = 1-based shard number assigned to that disk
    /// We need to map disk_idx -> device_id
    pub fn disk_index_to_device(
        &self,
        pool_idx: usize,
        set_idx: usize,
        disk_idx: usize,
    ) -> Option<usize> {
        self.disk_to_device.get(&(pool_idx, set_idx, disk_idx)).copied()
    }

    /// Get total number of disks in a set
    pub fn disks_in_set(&self, pool_idx: usize, set_idx: usize) -> usize {
        self.pools
            .get(pool_idx)
            .and_then(|p| p.sets.get(set_idx))
            .map(|s| s.len())
            .unwrap_or(0)
    }
}

/// Discover cluster topology from format.json files via ClickHouse
pub async fn discover_cluster(
    client: &clickhouse::Client,
    devices: &[String],
) -> Result<ClusterConfig> {
    info!("Discovering cluster topology from format.json files...");

    // Find format.json inodes from ClickHouse
    let format_locations = db::find_format_json_inodes(client).await?;
    info!("Found {} format.json files", format_locations.len());

    let mut formats: Vec<(usize, DiskFormat)> = Vec::new();

    for loc in format_locations {
        let device_id = loc.device_id as usize;
        if device_id >= devices.len() {
            tracing::warn!(
                "format.json on device {} but only {} devices provided",
                device_id,
                devices.len()
            );
            continue;
        }

        // Get extents for format.json
        let extents = db::get_file_extents(client, loc.device_id, loc.format_ino).await?;
        let size = db::get_inode_size(client, loc.device_id, loc.format_ino)
            .await?
            .unwrap_or(0);

        if extents.is_empty() || size == 0 {
            tracing::warn!("format.json on device {} has no extents or zero size", device_id);
            continue;
        }

        // Read format.json from disk
        let data = read_file_sync(&devices[device_id], &extents, size as u64)?;
        let fmt: DiskFormat = serde_json::from_slice(&data)
            .with_context(|| format!("parse format.json from device {}", device_id))?;

        info!(
            "Device {} has format.json: pool={}, this={}",
            device_id, fmt.id, fmt.xl.this_disk
        );
        formats.push((device_id, fmt));
    }

    build_cluster_config(&formats)
}

/// Build cluster config from parsed format.json files
fn build_cluster_config(formats: &[(usize, DiskFormat)]) -> Result<ClusterConfig> {
    // Group by pool ID
    let mut pools_map: HashMap<String, Vec<(usize, &DiskFormat)>> = HashMap::new();
    for (device_id, fmt) in formats {
        pools_map
            .entry(fmt.id.clone())
            .or_default()
            .push((*device_id, fmt));
    }

    // Build UUID to device_id mapping for each pool
    let mut pools: Vec<PoolConfig> = Vec::new();
    let mut device_to_disk: HashMap<usize, (usize, usize, usize)> = HashMap::new();
    let mut disk_to_device: HashMap<(usize, usize, usize), usize> = HashMap::new();

    for (pool_idx, (pool_id, disks_in_pool)) in pools_map.into_iter().enumerate() {
        // Build UUID -> device_id mapping for this pool
        let mut uuid_to_device: HashMap<String, usize> = HashMap::new();
        for (device_id, fmt) in &disks_in_pool {
            uuid_to_device.insert(fmt.xl.this_disk.clone(), *device_id);
        }

        // Get sets configuration from any disk (they should all be the same)
        let sets_config = &disks_in_pool[0].1.xl.sets;

        let mut sets: Vec<Vec<DiskInfo>> = Vec::new();
        for (set_idx, set_uuids) in sets_config.iter().enumerate() {
            let mut disk_infos: Vec<DiskInfo> = Vec::new();
            for (disk_idx, uuid) in set_uuids.iter().enumerate() {
                let device_id = uuid_to_device.get(uuid).copied();

                if let Some(dev_id) = device_id {
                    device_to_disk.insert(dev_id, (pool_idx, set_idx, disk_idx));
                    disk_to_device.insert((pool_idx, set_idx, disk_idx), dev_id);
                }

                disk_infos.push(DiskInfo {
                    uuid: uuid.clone(),
                    pool_index: pool_idx,
                    set_index: set_idx,
                    disk_index: disk_idx,
                    device_id,
                });
            }
            sets.push(disk_infos);
        }

        pools.push(PoolConfig {
            pool_id,
            pool_index: pool_idx,
            sets,
        });
    }

    info!(
        "Built cluster config: {} pools, {} device mappings",
        pools.len(),
        device_to_disk.len()
    );

    Ok(ClusterConfig {
        pools,
        device_to_disk,
        disk_to_device,
    })
}

/// Read a file from disk given its extents (synchronous)
fn read_file_sync(device_path: &str, extents: &[Extent], size: u64) -> Result<Vec<u8>> {
    let mut file = File::open(device_path)
        .with_context(|| format!("open device {}", device_path))?;

    let mut buffer = vec![0u8; size as usize];

    for extent in extents {
        file.seek(SeekFrom::Start(extent.physical_offset as u64))
            .with_context(|| format!("seek to offset {}", extent.physical_offset))?;

        let buf_start = extent.logical_offset as usize;
        let buf_end = buf_start + extent.length as usize;
        file.read_exact(&mut buffer[buf_start..buf_end])
            .with_context(|| format!("read extent at offset {}", extent.physical_offset))?;
    }

    Ok(buffer)
}
