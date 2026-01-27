//! Extent-based raw device I/O
//!
//! All file reads go through physical extents stored in DuckDB.
//! Primary: io_uring on Linux. Fallback: pread().

use std::fs::File;

use anyhow::{Context, Result};

use crate::types::Extent;

/// Trait abstracting device reads via physical extents
pub trait DeviceReader: Send + Sync {
    /// Read a file's contents given its extents and size.
    fn read_file(&self, device_id: usize, extents: &[Extent], size: u64) -> Result<Vec<u8>>;
}

/// pread()-based device reader (works on all Unix systems)
pub struct PreadDeviceReader {
    device_fds: Vec<File>,
}

impl PreadDeviceReader {
    /// Open device files for reading.
    /// `device_paths` is a list of block device or image file paths.
    pub fn open(device_paths: &[String]) -> Result<Self> {
        let mut fds = Vec::with_capacity(device_paths.len());
        for path in device_paths {
            let f = File::open(path)
                .with_context(|| format!("open device {}", path))?;
            fds.push(f);
        }
        Ok(Self { device_fds: fds })
    }

    /// Number of devices
    pub fn device_count(&self) -> usize {
        self.device_fds.len()
    }
}

impl DeviceReader for PreadDeviceReader {
    fn read_file(&self, device_id: usize, extents: &[Extent], size: u64) -> Result<Vec<u8>> {
        if device_id >= self.device_fds.len() {
            anyhow::bail!("device_id {} out of range (have {})", device_id, self.device_fds.len());
        }

        let fd = &self.device_fds[device_id];
        let mut data = vec![0u8; size as usize];
        let mut bytes_read: u64 = 0;

        for ext in extents {
            let to_read = std::cmp::min(ext.length as u64, size - bytes_read);
            if to_read == 0 {
                break;
            }

            let dst_start = ext.logical_offset as usize;
            let dst_end = dst_start + to_read as usize;

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                fd.read_at(&mut data[dst_start..dst_end], ext.physical_offset as u64)
                    .with_context(|| {
                        format!(
                            "pread device {} at offset {}",
                            device_id, ext.physical_offset
                        )
                    })?;
            }

            bytes_read += to_read;
        }

        Ok(data)
    }
}

/// Batch read request for meta-reader: read many files sorted by physical offset
#[derive(Debug)]
pub struct BatchReadRequest {
    pub device_id: usize,
    pub ino: u64,
    pub extents: Vec<Extent>,
    pub size: u64,
}

/// Result of a batch read
#[derive(Debug)]
pub struct BatchReadResult {
    pub ino: u64,
    pub data: Vec<u8>,
}

/// Read many files in physical-offset order, one forward sweep per device.
///
/// Groups requests by device_id, sorts each group by first physical_offset,
/// then reads sequentially for HDD-friendly access patterns.
pub fn batch_read(
    reader: &dyn DeviceReader,
    mut requests: Vec<BatchReadRequest>,
) -> Vec<BatchReadResult> {
    // Sort by (device_id, first_physical_offset) for sequential access
    requests.sort_by(|a, b| {
        let a_phys = a.extents.first().map(|e| e.physical_offset).unwrap_or(0);
        let b_phys = b.extents.first().map(|e| e.physical_offset).unwrap_or(0);
        (a.device_id, a_phys).cmp(&(b.device_id, b_phys))
    });

    let mut results = Vec::with_capacity(requests.len());

    for req in &requests {
        match reader.read_file(req.device_id, &req.extents, req.size) {
            Ok(data) => {
                results.push(BatchReadResult {
                    ino: req.ino,
                    data,
                });
            }
            Err(e) => {
                tracing::warn!(
                    "batch_read: failed to read ino {} on device {}: {}",
                    req.ino,
                    req.device_id,
                    e
                );
            }
        }
    }

    results
}

// --- io_uring implementation (Linux only) ---

#[cfg(target_os = "linux")]
pub mod uring {
    use super::*;
    use io_uring::{opcode, types, IoUring};
    use std::os::unix::io::AsRawFd;

    /// io_uring-based device reader for high-throughput sequential reads
    pub struct UringDeviceReader {
        device_fds: Vec<File>,
        queue_depth: u32,
    }

    impl UringDeviceReader {
        pub fn open(device_paths: &[String], queue_depth: u32) -> Result<Self> {
            let mut fds = Vec::with_capacity(device_paths.len());
            for path in device_paths {
                let f = File::open(path)
                    .with_context(|| format!("open device {}", path))?;
                fds.push(f);
            }
            Ok(Self {
                device_fds: fds,
                queue_depth,
            })
        }
    }

    impl DeviceReader for UringDeviceReader {
        fn read_file(&self, device_id: usize, extents: &[Extent], size: u64) -> Result<Vec<u8>> {
            if device_id >= self.device_fds.len() {
                anyhow::bail!("device_id {} out of range", device_id);
            }

            let fd = &self.device_fds[device_id];
            let mut ring = IoUring::new(self.queue_depth)?;
            let mut data = vec![0u8; size as usize];

            // Submit read SQEs for each extent
            for (i, ext) in extents.iter().enumerate() {
                let to_read = std::cmp::min(ext.length as u64, size.saturating_sub(ext.logical_offset as u64));
                if to_read == 0 {
                    continue;
                }

                let dst_start = ext.logical_offset as usize;
                let dst_end = dst_start + to_read as usize;

                let read_e = opcode::Read::new(
                    types::Fd(fd.as_raw_fd()),
                    data[dst_start..dst_end].as_mut_ptr(),
                    to_read as u32,
                )
                .offset(ext.physical_offset as u64)
                .build()
                .user_data(i as u64);

                unsafe {
                    ring.submission()
                        .push(&read_e)
                        .map_err(|_| anyhow::anyhow!("SQ full"))?;
                }
            }

            ring.submit_and_wait(extents.len())?;

            // Reap completions
            for cqe in ring.completion() {
                if cqe.result() < 0 {
                    anyhow::bail!(
                        "io_uring read failed for extent {}: errno {}",
                        cqe.user_data(),
                        -cqe.result()
                    );
                }
            }

            Ok(data)
        }
    }
}
