//! format.json types and cluster discovery
//!
//! Port of erasure/format.go. Parses MinIO's format.json to discover cluster topology.

use std::collections::HashMap;

use anyhow::{bail, Result};
use serde::Deserialize;

use crate::types::{ClusterConfig, DiskInfo, PoolConfig};

/// Raw format.json structure from MinIO
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
    pub this: String, // This disk's UUID
    pub sets: Vec<Vec<String>>,
    #[serde(rename = "distributionAlgo")]
    pub distribution_algo: String,
}

/// Parse a format.json from bytes
pub fn parse_format(data: &[u8]) -> Result<DiskFormat> {
    let format: DiskFormat = serde_json::from_slice(data)?;
    Ok(format)
}

/// Build cluster config from multiple format.json files.
///
/// `formats` is a list of (device_id, DiskFormat) pairs.
pub fn build_cluster_config(formats: &[(usize, DiskFormat)]) -> Result<ClusterConfig> {
    if formats.is_empty() {
        bail!("no format.json files provided");
    }

    // Group by pool ID, preserving discovery order
    let mut pool_disks: HashMap<String, Vec<(usize, &DiskFormat)>> = HashMap::new();
    let mut pool_order: Vec<String> = Vec::new();

    for (device_id, fmt) in formats {
        let pool_id = &fmt.id;
        if !pool_disks.contains_key(pool_id) {
            pool_order.push(pool_id.clone());
        }
        pool_disks
            .entry(pool_id.clone())
            .or_default()
            .push((*device_id, fmt));
    }

    // Build cluster config
    let mut pools = Vec::with_capacity(pool_order.len());

    for (pool_idx, pool_id) in pool_order.iter().enumerate() {
        let disks_in_pool = &pool_disks[pool_id];

        // Get sets configuration from first disk in pool
        let sets_config = &disks_in_pool[0].1.xl.sets;
        if sets_config.is_empty() {
            bail!("pool {} has no erasure sets", pool_id);
        }

        // Build UUID to device_id mapping
        let mut uuid_to_device: HashMap<String, usize> = HashMap::new();
        for (device_id, fmt) in disks_in_pool {
            uuid_to_device.insert(fmt.xl.this.clone(), *device_id);
        }

        // Build pool config
        let mut sets = Vec::with_capacity(sets_config.len());

        for (set_idx, set) in sets_config.iter().enumerate() {
            let mut disk_infos = Vec::with_capacity(set.len());
            for (disk_idx, uuid) in set.iter().enumerate() {
                let device_id = uuid_to_device.get(uuid).copied();
                disk_infos.push(DiskInfo {
                    uuid: uuid.clone(),
                    pool_index: pool_idx,
                    set_index: set_idx,
                    disk_index: disk_idx,
                    pool_id: pool_id.clone(),
                    device_id,
                });
            }
            sets.push(disk_infos);
        }

        pools.push(PoolConfig {
            pool_id: pool_id.clone(),
            pool_index: pool_idx,
            sets,
        });
    }

    Ok(ClusterConfig { pools })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture_path(name: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.pop();
        path.push(".disks");
        path.push(name);
        path
    }

    #[test]
    fn test_parse_format_json() {
        let path = fixture_path("storage1/.minio.sys/format.json");
        if !path.exists() {
            eprintln!("skipping test: fixture not found");
            return;
        }
        let data = std::fs::read(&path).unwrap();
        let fmt = parse_format(&data).unwrap();

        assert_eq!(fmt.version, "1");
        assert_eq!(fmt.format, "xl");
        assert!(!fmt.id.is_empty());
        assert!(!fmt.xl.this.is_empty());
        assert!(!fmt.xl.sets.is_empty());
        assert_eq!(fmt.xl.sets[0].len(), 16);
    }

    #[test]
    fn test_build_cluster_config() {
        let mut formats = Vec::new();
        for i in 1..=16 {
            let path = fixture_path(&format!("storage{}/.minio.sys/format.json", i));
            if !path.exists() {
                eprintln!("skipping test: fixtures not found");
                return;
            }
            let data = std::fs::read(&path).unwrap();
            let fmt = parse_format(&data).unwrap();
            formats.push((i - 1, fmt));
        }

        let cluster = build_cluster_config(&formats).unwrap();
        assert_eq!(cluster.pools.len(), 1);
        assert_eq!(cluster.pools[0].sets.len(), 1);
        assert_eq!(cluster.pools[0].sets[0].len(), 16);

        // Verify all 16 disks have device_id set
        for disk in &cluster.pools[0].sets[0] {
            assert!(
                disk.device_id.is_some(),
                "disk {} should have device_id",
                disk.uuid
            );
        }
    }
}
