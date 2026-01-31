//! Batch executor - plans and executes batched reads

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use tracing::{debug, error, info};

use mfu_core::erasure;
use mfu_core::{ObjectMeta, PartMeta, Uuid16};

use crate::cluster::ClusterConfig;
use crate::db::{self, StoredObject};

use super::types::{BatchError, ObjectReadPlan, PendingRequest, ShardReadPlan, ShardReadResult};

/// Executes batches of GetObject requests
pub struct BatchExecutor {
    client: Arc<clickhouse::Client>,
    cluster: Arc<ClusterConfig>,
    device_fds: Vec<File>,
}

impl BatchExecutor {
    pub fn new(
        client: Arc<clickhouse::Client>,
        cluster: Arc<ClusterConfig>,
        device_fds: Vec<File>,
    ) -> Self {
        Self {
            client,
            cluster,
            device_fds,
        }
    }

    /// Execute a batch of requests
    pub async fn execute(&self, requests: Vec<PendingRequest>) {
        info!("Executing batch of {} requests", requests.len());

        // 1. Batch lookup objects from ClickHouse
        let keys: Vec<(&str, &str)> = requests
            .iter()
            .map(|r| (r.bucket.as_str(), r.key.as_str()))
            .collect();

        let objects = match db::batch_get_objects(&self.client, &keys).await {
            Ok(objs) => objs,
            Err(e) => {
                error!("Batch object lookup failed: {}", e);
                // Send errors to all requests
                for req in requests {
                    let _ = req
                        .response_tx
                        .send(Err(BatchError::Internal(e.to_string())));
                }
                return;
            }
        };

        // 2. Build shard read plans for all objects
        let mut all_shard_plans: Vec<ShardReadPlan> = Vec::new();
        let mut object_plans: HashMap<u64, ObjectReadPlan> = HashMap::new();

        for (idx, (req, obj_opt)) in requests.into_iter().zip(objects).enumerate() {
            let request_id = idx as u64;
            match obj_opt {
                Some(obj) => {
                    match self.plan_shard_reads(request_id, &obj).await {
                        Ok(shard_plans) => {
                            all_shard_plans.extend(shard_plans.clone());
                            object_plans.insert(
                                request_id,
                                ObjectReadPlan {
                                    request_id,
                                    meta: obj,
                                    shard_plans,
                                    response_tx: req.response_tx,
                                },
                            );
                        }
                        Err(e) => {
                            error!("Failed to plan shard reads for {}/{}: {}", req.bucket, req.key, e);
                            let _ = req
                                .response_tx
                                .send(Err(BatchError::Internal(e.to_string())));
                        }
                    }
                }
                None => {
                    let _ = req.response_tx.send(Err(BatchError::NoSuchKey));
                }
            }
        }

        if all_shard_plans.is_empty() {
            debug!("No shard plans found, sending errors to {} pending requests", object_plans.len());
            for (_, plan) in object_plans {
                let _ = plan.response_tx.send(Err(BatchError::Internal(
                    "No shards could be read".to_string(),
                )));
            }
            return;
        }

        // 3. Sort by (device_id, physical_offset) for HDD sequential access
        all_shard_plans.sort_by_key(|p| (p.device_id, p.first_phys_offset));

        debug!(
            "Executing {} shard reads across {} devices",
            all_shard_plans.len(),
            all_shard_plans.iter().map(|p| p.device_id).collect::<std::collections::HashSet<_>>().len()
        );

        // 4. Execute batched reads
        let shard_results = self.execute_reads(&all_shard_plans);

        // 5. Group results by request_id and decode
        let mut by_request: HashMap<u64, Vec<ShardReadResult>> = HashMap::new();
        for result in shard_results {
            by_request
                .entry(result.request_id)
                .or_default()
                .push(result);
        }

        // 6. Decode each object and send responses
        for (request_id, shards) in by_request {
            if let Some(plan) = object_plans.remove(&request_id) {
                let result = self.decode_object(&plan.meta, shards);
                let _ = plan.response_tx.send(result);
            }
        }

        // Send errors for any remaining plans (shouldn't happen)
        for (_, plan) in object_plans {
            let _ = plan
                .response_tx
                .send(Err(BatchError::Internal("Missing shard results".to_string())));
        }
    }

    /// Plan shard reads for an object
    async fn plan_shard_reads(
        &self,
        request_id: u64,
        obj: &StoredObject,
    ) -> anyhow::Result<Vec<ShardReadPlan>> {
        let mut plans = Vec::new();

        // Parse data_dir UUID
        let data_dir = &obj.data_dir;

        // Determine pool/set from xlmeta_device_id
        let (pool_idx, set_idx, _) = self
            .cluster
            .device_position(obj.xlmeta_device_id as usize)
            .unwrap_or((0, 0, 0));

        info!(
            "Planning shard reads for {}/{}, data_dir={}, distribution={:?}, xlmeta_device={}, pool={}, set={}",
            obj.bucket, obj.key, data_dir, obj.distribution, obj.xlmeta_device_id, pool_idx, set_idx
        );

        // Parse parts JSON to get part numbers
        let parts: Vec<i32> = if obj.parts_json.is_empty() {
            vec![1] // Single-part object
        } else {
            let parts_val: Vec<serde_json::Value> = serde_json::from_str(&obj.parts_json)?;
            parts_val
                .iter()
                .map(|p| p["number"].as_i64().unwrap_or(1) as i32)
                .collect()
        };

        info!("Parts to read: {:?}", parts);

        // For each part, query fs table once to get all device->inode mappings
        for part_number in parts {
            // Build full path: bucket/key/data_dir/part.N
            let full_path = format!(
                "{}/{}/{}/part.{}",
                obj.bucket, obj.key, data_dir, part_number
            );

            info!("Looking up full path: {}", full_path);

            // Single query to get all devices with this part
            let locations = db::lookup_part_by_path(&self.client, &full_path).await?;

            info!("Found {} locations for {}", locations.len(), full_path);

            for loc in locations {
                let device_id = loc.device_id as usize;
                let ino = loc.child_ino;

                if device_id >= self.device_fds.len() {
                    continue;
                }

                // Get shard_index from distribution based on device_id
                // Find which disk_idx this device maps to
                let disk_idx = match self.cluster.device_position(device_id) {
                    Some((_, _, idx)) => idx,
                    None => continue,
                };

                // Verify this disk has a valid distribution entry
                let shard_num = obj.distribution.get(disk_idx).copied().unwrap_or(0);
                if shard_num == 0 {
                    continue;
                }

                // Get extents
                let extents = db::get_file_extents(&self.client, loc.device_id, ino).await?;
                if extents.is_empty() {
                    continue;
                }

                // Get file size
                let size = db::get_inode_size(&self.client, loc.device_id, ino)
                    .await?
                    .unwrap_or(0);

                if size == 0 {
                    continue;
                }

                let first_phys_offset = extents.first().map(|e| e.physical_offset).unwrap_or(0);

                plans.push(ShardReadPlan {
                    request_id,
                    part_number,
                    disk_index: disk_idx, // Position in distribution array (0..15)
                    device_id,
                    extents,
                    file_size: size as u64,
                    first_phys_offset,
                });
            }
        }

        Ok(plans)
    }

    /// Execute all shard reads (sorted by device/offset)
    fn execute_reads(&self, plans: &[ShardReadPlan]) -> Vec<ShardReadResult> {
        let mut results = Vec::with_capacity(plans.len());

        for plan in plans {
            let data = self.read_shard(plan);
            results.push(ShardReadResult {
                request_id: plan.request_id,
                part_number: plan.part_number,
                disk_index: plan.disk_index,
                data,
            });
        }

        results
    }

    /// Read a single shard from disk using pread
    fn read_shard(&self, plan: &ShardReadPlan) -> Option<Vec<u8>> {
        use std::os::unix::io::AsRawFd;

        let file = &self.device_fds[plan.device_id];
        let fd = file.as_raw_fd();

        // Calculate buffer size as max of inode size and extent coverage
        let extent_coverage = plan.extents
            .iter()
            .map(|e| (e.logical_offset + e.length) as u64)
            .max()
            .unwrap_or(0);
        let buffer_size = std::cmp::max(plan.file_size, extent_coverage) as usize;
        let mut buffer = vec![0u8; buffer_size];

        for extent in &plan.extents {
            let buf_start = extent.logical_offset as usize;
            let buf_end = buf_start + extent.length as usize;

            let n = unsafe {
                libc::pread(
                    fd,
                    buffer[buf_start..buf_end].as_mut_ptr() as *mut libc::c_void,
                    extent.length as usize,
                    extent.physical_offset as i64,
                )
            };

            if n < 0 {
                error!(
                    "pread failed for device {}: {}",
                    plan.device_id,
                    std::io::Error::last_os_error()
                );
                return None;
            }
        }

        // Truncate to actual file size
        buffer.truncate(plan.file_size as usize);
        Some(buffer)
    }

    /// Decode an object from its shards
    fn decode_object(
        &self,
        stored: &StoredObject,
        shard_results: Vec<ShardReadResult>,
    ) -> Result<Vec<u8>, BatchError> {
        // Convert stored object to ObjectMeta
        let meta = stored_to_meta(stored);

        // Build a shard reader from our results
        let reader = ResultShardReader {
            results: &shard_results,
        };

        // Decode the object
        erasure::decode_object(&reader, &meta, &[])
            .map_err(|e| BatchError::Internal(format!("decode failed: {}", e)))
    }
}

/// Shard reader that uses pre-read results
struct ResultShardReader<'a> {
    results: &'a [ShardReadResult],
}

impl<'a> erasure::ShardReader for ResultShardReader<'a> {
    fn read_shard(
        &self,
        disk_index: usize,
        _bucket: &str,
        _key: &str,
        _data_dir: &str,
        part_number: i32,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        // Find the matching result by disk_index (position in distribution array)
        for result in self.results {
            if result.disk_index == disk_index && result.part_number == part_number {
                return Ok(result.data.clone());
            }
        }
        Ok(None)
    }
}

fn stored_to_meta(stored: &StoredObject) -> ObjectMeta {
    let mut meta = ObjectMeta::default();
    meta.bucket = stored.bucket.clone();
    meta.key = stored.key.clone();
    meta.size = stored.size;
    meta.mod_time = stored.mod_time;
    meta.etag = stored.etag.clone();
    meta.content_type = stored.content_type.clone();
    meta.data_blocks = stored.data_blocks as usize;
    meta.parity_blocks = stored.parity_blocks as usize;
    meta.block_size = stored.block_size;
    meta.distribution = stored.distribution.iter().map(|&v| v as u8).collect();

    // Parse data_dir UUID
    if let Ok(uuid_bytes) = parse_uuid_string(&stored.data_dir) {
        meta.data_dir = Uuid16(uuid_bytes);
    }

    // Parse parts JSON
    if !stored.parts_json.is_empty() {
        if let Ok(parts) = serde_json::from_str::<Vec<serde_json::Value>>(&stored.parts_json) {
            meta.parts = parts
                .iter()
                .map(|p| PartMeta {
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
