//! Filesystem reader implementations
//!
//! Provides batched async I/O for reading xl.meta files from block devices.
//! Uses io_uring on Linux for high performance, falls back to pread on other platforms.

use std::sync::mpsc;

use anyhow::{Context, Result};
use tokio::sync::mpsc as tokio_mpsc;

use crate::parser::parse_xlmeta;
use crate::types::{ReadyXlmeta, ReaderResult};

/// Spawns a reader thread that consumes ReadyXlmeta from ready_rx,
/// reads the files from disk, parses them, and sends results to result_tx.
///
/// Returns a handle to the reader thread.
pub fn spawn_reader_thread(
    device_path: String,
    ready_rx: mpsc::Receiver<ReadyXlmeta>,
    result_tx: tokio_mpsc::Sender<ReaderResult>,
    queue_depth: u32,
) -> std::thread::JoinHandle<Result<()>> {
    std::thread::spawn(move || reader_thread(device_path, ready_rx, result_tx, queue_depth))
}

// ═══════════════════════════════════════════════════════════════════════
// Linux io_uring reader
// ═══════════════════════════════════════════════════════════════════════

#[cfg(target_os = "linux")]
fn reader_thread(
    device_path: String,
    ready_rx: mpsc::Receiver<ReadyXlmeta>,
    result_tx: tokio_mpsc::Sender<ReaderResult>,
    queue_depth: u32,
) -> Result<()> {
    use io_uring::{opcode, types, IoUring};
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;
    use std::time::Instant;
    use tracing::info;

    let file = OpenOptions::new()
        .read(true)
        .open(&device_path)
        .with_context(|| format!("open device {}", device_path))?;
    let fd = file.as_raw_fd();

    let mut ring = IoUring::new(queue_depth).context("create io_uring")?;

    struct InFlight {
        xlmeta: ReadyXlmeta,
        buffer: Vec<u8>,
        extents_done: usize,
    }

    let mut in_flight: HashMap<u64, InFlight> = HashMap::new();
    let mut next_xlmeta_idx: u64 = 0;
    let mut pending_submissions = 0u32;
    let mut deferred_xlmeta: Option<ReadyXlmeta> = None; // xlmeta waiting for queue space

    // I/O stats
    let start_time = Instant::now();
    let mut last_log_time = Instant::now();
    let mut total_bytes_read: u64 = 0;
    let mut total_read_ops: u64 = 0;
    let mut total_files_completed: u64 = 0;
    let mut bytes_since_last_log: u64 = 0;
    let mut ops_since_last_log: u64 = 0;
    let mut files_since_last_log: u64 = 0;

    info!("io_uring reader started for {}", device_path);

    loop {
        // Receive xlmetas and submit reads
        loop {
            // First, try to process any deferred xlmeta
            let xlmeta = if let Some(deferred) = deferred_xlmeta.take() {
                deferred
            } else if in_flight.is_empty() && deferred_xlmeta.is_none() {
                match ready_rx.recv() {
                    Ok(x) => x,
                    Err(_) => break, // Channel closed
                }
            } else {
                match ready_rx.try_recv() {
                    Ok(x) => x,
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            };

            // Check if we have room for ALL extents of this xlmeta
            let extents_needed = xlmeta.extents.len() as u32;
            if extents_needed > queue_depth {
                // File has more extents than queue can ever hold - skip it
                tracing::warn!(
                    "xlmeta {}:{} has {} extents, exceeds queue depth {} - skipping",
                    xlmeta.bucket, xlmeta.key, extents_needed, queue_depth
                );
                continue;
            }

            if pending_submissions + extents_needed > queue_depth {
                // Not enough room right now - defer this xlmeta for later
                deferred_xlmeta = Some(xlmeta);
                break;
            }

            let xlmeta_idx = next_xlmeta_idx;
            next_xlmeta_idx += 1;

            let buffer = vec![0u8; xlmeta.file_size as usize];

            for (extent_idx, extent) in xlmeta.extents.iter().enumerate() {
                let user_data = (xlmeta_idx << 32) | (extent_idx as u64);
                let buf_offset = extent.logical_offset as usize;
                let buf_ptr = unsafe { buffer.as_ptr().add(buf_offset) as *mut u8 };

                let read_op = opcode::Read::new(types::Fd(fd), buf_ptr, extent.length as u32)
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
                xlmeta_idx,
                InFlight {
                    xlmeta,
                    buffer,
                    extents_done: 0,
                },
            );
        }

        if in_flight.is_empty() && deferred_xlmeta.is_none() && ready_rx.try_recv().is_err() {
            break;
        }

        ring.submit_and_wait(1)
            .context("io_uring submit_and_wait")?;

        while let Some(cqe) = ring.completion().next() {
            pending_submissions -= 1;
            let user_data = cqe.user_data();
            let xlmeta_idx = user_data >> 32;

            let bytes_read = cqe.result();
            if bytes_read < 0 {
                if let Some(inf) = in_flight.remove(&xlmeta_idx) {
                    let _ = result_tx.blocking_send(ReaderResult::Error {
                        bucket: inf.xlmeta.bucket,
                        key: inf.xlmeta.key,
                        error: format!("read error: {}", bytes_read),
                    });
                }
                continue;
            }

            // Track I/O stats
            total_bytes_read += bytes_read as u64;
            bytes_since_last_log += bytes_read as u64;
            total_read_ops += 1;
            ops_since_last_log += 1;

            if let Some(inf) = in_flight.get_mut(&xlmeta_idx) {
                inf.extents_done += 1;

                if inf.extents_done == inf.xlmeta.extents.len() {
                    let inf = in_flight.remove(&xlmeta_idx).unwrap();
                    let result = parse_xlmeta(&inf.buffer, &inf.xlmeta);
                    if result_tx.blocking_send(result).is_err() {
                        return Ok(()); // Consumer gone
                    }
                    total_files_completed += 1;
                    files_since_last_log += 1;
                }
            }
        }

        // Log stats every 5 seconds
        if last_log_time.elapsed().as_secs() >= 5 {
            let elapsed = last_log_time.elapsed().as_secs_f64();
            let mb_per_sec = (bytes_since_last_log as f64 / 1024.0 / 1024.0) / elapsed;
            let iops = ops_since_last_log as f64 / elapsed;
            let files_per_sec = files_since_last_log as f64 / elapsed;

            info!(
                "I/O stats: {:.1} MB/s, {:.0} IOPS, {:.0} files/s | in_flight: {}, pending_sq: {}",
                mb_per_sec, iops, files_per_sec, in_flight.len(), pending_submissions
            );

            last_log_time = Instant::now();
            bytes_since_last_log = 0;
            ops_since_last_log = 0;
            files_since_last_log = 0;
        }
    }

    // Final stats
    let total_elapsed = start_time.elapsed().as_secs_f64();
    let total_mb = total_bytes_read as f64 / 1024.0 / 1024.0;
    info!(
        "io_uring reader finished: {:.1} MB in {:.1}s ({:.1} MB/s), {} files, {} read ops",
        total_mb, total_elapsed, total_mb / total_elapsed, total_files_completed, total_read_ops
    );

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// Non-Linux fallback reader (pread-based)
// ═══════════════════════════════════════════════════════════════════════

#[cfg(not(target_os = "linux"))]
fn reader_thread(
    device_path: String,
    ready_rx: mpsc::Receiver<ReadyXlmeta>,
    result_tx: tokio_mpsc::Sender<ReaderResult>,
    _queue_depth: u32,
) -> Result<()> {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};
    use std::time::Instant;
    use tracing::info;

    let mut file =
        File::open(&device_path).with_context(|| format!("open device {}", device_path))?;

    // I/O stats
    let start_time = Instant::now();
    let mut last_log_time = Instant::now();
    let mut total_bytes_read: u64 = 0;
    let mut total_read_ops: u64 = 0;
    let mut total_files_completed: u64 = 0;
    let mut bytes_since_last_log: u64 = 0;
    let mut ops_since_last_log: u64 = 0;
    let mut files_since_last_log: u64 = 0;

    info!("pread reader started for {}", device_path);

    for xlmeta in ready_rx {
        let mut buffer = vec![0u8; xlmeta.file_size as usize];

        let mut read_error = None;
        for extent in &xlmeta.extents {
            if let Err(e) = file.seek(SeekFrom::Start(extent.physical_offset as u64)) {
                read_error = Some(format!("seek error: {}", e));
                break;
            }

            let buf_offset = extent.logical_offset as usize;
            let buf_end = buf_offset + extent.length as usize;
            if let Err(e) = file.read_exact(&mut buffer[buf_offset..buf_end]) {
                read_error = Some(format!("read error: {}", e));
                break;
            }

            // Track I/O stats
            let bytes = extent.length as u64;
            total_bytes_read += bytes;
            bytes_since_last_log += bytes;
            total_read_ops += 1;
            ops_since_last_log += 1;
        }

        let result = if let Some(err) = read_error {
            ReaderResult::Error {
                bucket: xlmeta.bucket,
                key: xlmeta.key,
                error: err,
            }
        } else {
            parse_xlmeta(&buffer, &xlmeta)
        };

        if result_tx.blocking_send(result).is_err() {
            break; // Consumer gone
        }

        total_files_completed += 1;
        files_since_last_log += 1;

        // Log stats every 5 seconds
        if last_log_time.elapsed().as_secs() >= 5 {
            let elapsed = last_log_time.elapsed().as_secs_f64();
            let mb_per_sec = (bytes_since_last_log as f64 / 1024.0 / 1024.0) / elapsed;
            let iops = ops_since_last_log as f64 / elapsed;
            let files_per_sec = files_since_last_log as f64 / elapsed;

            info!(
                "I/O stats: {:.1} MB/s, {:.0} IOPS, {:.0} files/s",
                mb_per_sec, iops, files_per_sec
            );

            last_log_time = Instant::now();
            bytes_since_last_log = 0;
            ops_since_last_log = 0;
            files_since_last_log = 0;
        }
    }

    // Final stats
    let total_elapsed = start_time.elapsed().as_secs_f64();
    let total_mb = total_bytes_read as f64 / 1024.0 / 1024.0;
    info!(
        "pread reader finished: {:.1} MB in {:.1}s ({:.1} MB/s), {} files, {} read ops",
        total_mb, total_elapsed, total_mb / total_elapsed, total_files_completed, total_read_ops
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mfu_core::types::Extent;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_reader_with_temp_file() {
        // Create a temp file with some data
        let mut temp_file = NamedTempFile::new().unwrap();
        let test_data = b"XL2 test data here";
        temp_file.write_all(test_data).unwrap();
        temp_file.flush().unwrap();

        let (ready_tx, ready_rx) = mpsc::sync_channel(10);
        let (result_tx, mut result_rx) = tokio_mpsc::channel(10);

        let path = temp_file.path().to_string_lossy().to_string();
        let handle = spawn_reader_thread(path, ready_rx, result_tx, 16);

        // Send a read request
        let xlmeta = ReadyXlmeta {
            bucket: "test".to_string(),
            key: "key".to_string(),
            device_id: 0,
            ino: 1,
            extents: vec![Extent {
                logical_offset: 0,
                physical_offset: 0,
                length: test_data.len() as i64,
            }],
            file_size: test_data.len() as u64,
        };

        ready_tx.send(xlmeta).unwrap();
        drop(ready_tx); // Signal end

        // Get result (will be an error since test data isn't valid xl.meta)
        let result = result_rx.blocking_recv().unwrap();
        match result {
            ReaderResult::Error { bucket, key, .. } => {
                assert_eq!(bucket, "test");
                assert_eq!(key, "key");
            }
            ReaderResult::Parsed(_) => {
                // Also acceptable if the parser somehow accepts it
            }
        }

        handle.join().unwrap().unwrap();
    }
}
