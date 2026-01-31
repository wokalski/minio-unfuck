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

    #[test]
    fn test_parse_format_valid_json() {
        let json = r#"{
            "version": "1",
            "format": "xl",
            "id": "pool-123",
            "xl": {
                "version": "3",
                "this": "disk-uuid-1",
                "sets": [["disk1", "disk2", "disk3"]],
                "distributionAlgo": "SIPMOD+PARITY"
            }
        }"#;

        let fmt = parse_format(json.as_bytes()).unwrap();
        assert_eq!(fmt.version, "1");
        assert_eq!(fmt.format, "xl");
        assert_eq!(fmt.id, "pool-123");
        assert_eq!(fmt.xl.sets.len(), 1);
        assert_eq!(fmt.xl.sets[0].len(), 3);
    }

    #[test]
    fn test_parse_format_rejects_invalid_json() {
        let result = parse_format(b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_cluster_config_empty_formats() {
        let result = build_cluster_config(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_cluster_config_single_pool() {
        let json = r#"{
            "version": "1",
            "format": "xl",
            "id": "pool-1",
            "xl": {
                "version": "3",
                "this": "disk-a",
                "sets": [["disk-a", "disk-b", "disk-c", "disk-d"]],
                "distributionAlgo": "SIPMOD+PARITY"
            }
        }"#;

        let fmt_a = parse_format(json.as_bytes()).unwrap();
        let json_b = json.replace("disk-a", "disk-b");
        let fmt_b = parse_format(json_b.as_bytes()).unwrap();

        let cluster = build_cluster_config(&[(0, fmt_a), (1, fmt_b)]).unwrap();
        assert_eq!(cluster.pools.len(), 1);
        assert_eq!(cluster.pools[0].sets.len(), 1);
        assert_eq!(cluster.pools[0].sets[0].len(), 4);

        // Verify device_id is set for the disks we provided
        let disk_a = cluster.pools[0].sets[0].iter().find(|d| d.uuid == "disk-a").unwrap();
        assert_eq!(disk_a.device_id, Some(0));

        let disk_b = cluster.pools[0].sets[0].iter().find(|d| d.uuid == "disk-b").unwrap();
        assert_eq!(disk_b.device_id, Some(1));

        // disk-c and disk-d were not provided, so device_id should be None
        let disk_c = cluster.pools[0].sets[0].iter().find(|d| d.uuid == "disk-c").unwrap();
        assert_eq!(disk_c.device_id, None);
    }
}
