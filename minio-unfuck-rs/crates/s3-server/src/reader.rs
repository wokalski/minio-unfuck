//! Batched device reader
//!
//! Reads shards from block devices using io_uring on Linux or pread on other platforms.
//! Reads are assumed to be pre-sorted by (device_id, physical_offset) for HDD optimization.

use std::fs::File;

use tracing::{debug, error};

use crate::batch::types::{ShardReadPlan, ShardReadResult};

/// Batched reader for device I/O
pub struct BatchedReader {
    device_fds: Vec<File>,
    #[allow(dead_code)]
    queue_depth: u32,
}

impl BatchedReader {
    pub fn new(device_fds: Vec<File>, queue_depth: u32) -> Self {
        Self {
            device_fds,
            queue_depth,
        }
    }

    /// Execute all shard reads (assumed to be pre-sorted by device_id, physical_offset)
    pub fn batch_read(&self, plans: &[ShardReadPlan]) -> Vec<ShardReadResult> {
        // For now, use the simple pread-based implementation
        // io_uring can be added later for Linux
        self.read_pread(plans)
    }

    /// Simple pread-based implementation (works on all platforms)
    fn read_pread(&self, plans: &[ShardReadPlan]) -> Vec<ShardReadResult> {
        use std::os::unix::io::AsRawFd;

        let mut results = Vec::with_capacity(plans.len());

        for plan in plans {
            if plan.device_id >= self.device_fds.len() {
                debug!(
                    "Device {} out of range (have {})",
                    plan.device_id,
                    self.device_fds.len()
                );
                results.push(ShardReadResult {
                    request_id: plan.request_id,
                    part_number: plan.part_number,
                    disk_index: plan.disk_index,
                    data: None,
                });
                continue;
            }

            let file = &self.device_fds[plan.device_id];
            let fd = file.as_raw_fd();
            let mut buffer = vec![0u8; plan.file_size as usize];

            let mut read_ok = true;
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
                    read_ok = false;
                    break;
                }
            }

            results.push(ShardReadResult {
                request_id: plan.request_id,
                part_number: plan.part_number,
                disk_index: plan.disk_index,
                data: if read_ok { Some(buffer) } else { None },
            });
        }

        results
    }
}

/// io_uring-based reader for Linux
#[cfg(target_os = "linux")]
pub mod uring {
    use super::*;
    use anyhow::Result;
    use io_uring::{opcode, types, IoUring};
    use std::collections::HashMap;
    use std::os::unix::io::AsRawFd;

    /// Execute reads using io_uring
    pub fn batch_read_uring(
        device_fds: &[File],
        plans: &[ShardReadPlan],
        queue_depth: u32,
    ) -> Result<Vec<ShardReadResult>> {
        if plans.is_empty() {
            return Ok(vec![]);
        }

        // Group plans by device
        let mut by_device: HashMap<usize, Vec<&ShardReadPlan>> = HashMap::new();
        for plan in plans {
            by_device.entry(plan.device_id).or_default().push(plan);
        }

        let mut all_results = Vec::with_capacity(plans.len());

        // Process each device
        for (device_id, device_plans) in by_device {
            if device_id >= device_fds.len() {
                for plan in device_plans {
                    all_results.push(ShardReadResult {
                        request_id: plan.request_id,
                        part_number: plan.part_number,
                        disk_index: plan.disk_index,
                        data: None,
                    });
                }
                continue;
            }

            let fd = device_fds[device_id].as_raw_fd();
            let results = read_device_uring(fd, &device_plans, queue_depth)?;
            all_results.extend(results);
        }

        Ok(all_results)
    }

    fn read_device_uring(
        fd: i32,
        plans: &[&ShardReadPlan],
        queue_depth: u32,
    ) -> Result<Vec<ShardReadResult>> {
        let mut ring = IoUring::new(queue_depth)?;

        struct InFlight {
            plan_idx: usize,
            buffer: Vec<u8>,
            extents_done: usize,
            total_extents: usize,
        }

        let mut in_flight: HashMap<u64, InFlight> = HashMap::new();
        let mut results: Vec<Option<ShardReadResult>> = vec![None; plans.len()];
        let mut next_idx: u64 = 0;
        let mut pending_submissions = 0u32;
        let mut plan_queue: std::collections::VecDeque<usize> =
            (0..plans.len()).collect();

        while !plan_queue.is_empty() || !in_flight.is_empty() {
            // Submit as many plans as we can
            while let Some(&plan_idx) = plan_queue.front() {
                let plan = plans[plan_idx];
                let extents_needed = plan.extents.len() as u32;

                if extents_needed > queue_depth {
                    // Skip plans with too many extents
                    plan_queue.pop_front();
                    results[plan_idx] = Some(ShardReadResult {
                        request_id: plan.request_id,
                        part_number: plan.part_number,
                        disk_index: plan.disk_index,
                        data: None,
                    });
                    continue;
                }

                if pending_submissions + extents_needed > queue_depth {
                    break; // No room
                }

                plan_queue.pop_front();
                let idx = next_idx;
                next_idx += 1;

                let buffer = vec![0u8; plan.file_size as usize];

                for (extent_idx, extent) in plan.extents.iter().enumerate() {
                    let user_data = (idx << 32) | (extent_idx as u64);
                    let buf_offset = extent.logical_offset as usize;
                    let buf_ptr = unsafe { buffer.as_ptr().add(buf_offset) as *mut u8 };

                    let read_op = opcode::Read::new(
                        types::Fd(fd),
                        buf_ptr,
                        extent.length as u32,
                    )
                    .offset(extent.physical_offset as u64)
                    .build()
                    .user_data(user_data);

                    unsafe {
                        ring.submission()
                            .push(&read_op)
                            .expect("SQ should have room");
                    }
                    pending_submissions += 1;
                }

                in_flight.insert(
                    idx,
                    InFlight {
                        plan_idx,
                        buffer,
                        extents_done: 0,
                        total_extents: plan.extents.len(),
                    },
                );
            }

            if in_flight.is_empty() {
                break;
            }

            ring.submit_and_wait(1)?;

            while let Some(cqe) = ring.completion().next() {
                pending_submissions -= 1;
                let user_data = cqe.user_data();
                let idx = user_data >> 32;

                let bytes_read = cqe.result();
                if bytes_read < 0 {
                    if let Some(inf) = in_flight.remove(&idx) {
                        let plan = plans[inf.plan_idx];
                        results[inf.plan_idx] = Some(ShardReadResult {
                            request_id: plan.request_id,
                            part_number: plan.part_number,
                            disk_index: plan.disk_index,
                            data: None,
                        });
                    }
                    continue;
                }

                if let Some(inf) = in_flight.get_mut(&idx) {
                    inf.extents_done += 1;

                    if inf.extents_done == inf.total_extents {
                        let inf = in_flight.remove(&idx).unwrap();
                        let plan = plans[inf.plan_idx];
                        results[inf.plan_idx] = Some(ShardReadResult {
                            request_id: plan.request_id,
                            part_number: plan.part_number,
                            disk_index: plan.disk_index,
                            data: Some(inf.buffer),
                        });
                    }
                }
            }
        }

        // Convert to final results
        Ok(results
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                r.unwrap_or_else(|| {
                    let plan = plans[i];
                    ShardReadResult {
                        request_id: plan.request_id,
                        part_number: plan.part_number,
                        disk_index: plan.disk_index,
                        data: None,
                    }
                })
            })
            .collect())
    }
}
