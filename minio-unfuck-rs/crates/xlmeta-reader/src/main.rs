//! xlmeta-reader: Parse xl.meta files and populate ClickHouse objects table
//!
//! Streams xl.meta file extents per device (sorted by physical offset for HDD),
//! uses io_uring for batched async reads (Linux) or pread (other platforms),
//! parses them, and inserts into `objects` table.

use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use tracing::{info, warn, Level};

use mfu_core::types::Extent;
use mfu_core::xlmeta;

#[derive(Parser, Debug)]
#[command(
    name = "xlmeta-reader",
    about = "Parse xl.meta files and populate ClickHouse with object metadata"
)]
struct Args {
    /// Raw block device or XFS partition paths (in device_id order)
    #[arg(long = "root", required = true)]
    roots: Vec<String>,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://localhost:8123")]
    clickhouse_url: String,

    /// ClickHouse database name
    #[arg(long, default_value = "minio")]
    clickhouse_db: String,

    /// Batch size for ClickHouse inserts
    #[arg(long, default_value = "10000")]
    batch_size: usize,

    /// io_uring queue depth (Linux only)
    #[arg(long, default_value = "256")]
    queue_depth: u32,

    /// Dry run: parse but don't write to objects table
    #[arg(long)]
    dry_run: bool,

    /// Limit processing to N objects per device (for testing)
    #[arg(long)]
    limit: Option<usize>,
}

// ── ClickHouse row types ──────────────────────────────────────────────

#[derive(clickhouse::Row, Serialize, Debug, Clone)]
struct ChObject {
    bucket: String,
    key: String,
    size: i64,
    mod_time: i64,
    etag: String,
    content_type: String,
    compression: String,
    data_blocks: i32,
    parity_blocks: i32,
    block_size: i64,
    data_dir: String,
    distribution: Vec<i32>,
    parts_json: String,
    xlmeta_device_id: i32,
    xlmeta_ino: i64,
}

// ── Query result types ────────────────────────────────────────────────

#[derive(clickhouse::Row, Deserialize, Debug)]
struct ExtentRow {
    bucket: String,
    key: String,
    xlmeta_ino: i64,
    #[allow(dead_code)]
    data_dir_ino: i64,
    logical_offset: i64,
    physical_offset: i64,
    length: i64,
    extent_count: u64,
}

/// A complete xlmeta ready to be read from disk
struct ReadyXlmeta {
    #[allow(dead_code)]
    id: u64,
    bucket: String,
    key: String,
    device_id: i32,
    ino: i64,
    extents: Vec<Extent>,
    file_size: u64,
}

/// Result from the reader thread
enum ReaderResult {
    Parsed(ChObject),
    Error { bucket: String, key: String, error: String },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    let args = Args::parse();

    info!("xlmeta-reader starting");
    info!("roots: {:?}", args.roots);
    info!(
        "clickhouse: {}/{}",
        args.clickhouse_url, args.clickhouse_db
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    let client = clickhouse::Client::default()
        .with_url(&args.clickhouse_url)
        .with_database(&args.clickhouse_db);

    // Resolve roots: expand whole-disk devices to their partitions
    let devices = resolve_devices(&args.roots)?;
    info!("Resolved {} device(s): {:?}", devices.len(), devices);

    // Ensure objects table exists
    if !args.dry_run {
        create_objects_table(&client).await?;
    }

    let total_start = Instant::now();
    let mut total_objects = 0u64;
    let mut total_errors = 0u64;

    // Process each device
    for device_id in 0..devices.len() as i32 {
        info!("Processing device {}...", device_id);
        let dev_start = Instant::now();

        let (objects, errors) = process_device(
            &client,
            &devices,
            device_id,
            args.batch_size,
            args.queue_depth,
            args.dry_run,
            args.limit,
        )
        .await
        .with_context(|| format!("process device {}", device_id))?;

        total_objects += objects;
        total_errors += errors;

        info!(
            "Device {} done: {} objects, {} errors in {:.1}s",
            device_id,
            objects,
            errors,
            dev_start.elapsed().as_secs_f64()
        );
    }

    info!(
        "All devices done: {} total objects, {} errors in {:.1}s",
        total_objects,
        total_errors,
        total_start.elapsed().as_secs_f64()
    );

    Ok(())
}

async fn create_objects_table(client: &clickhouse::Client) -> Result<()> {
    let ddl = r#"
        CREATE TABLE IF NOT EXISTS objects (
            bucket String,
            key String,
            size Int64,
            mod_time Int64,
            etag String,
            content_type String,
            compression String,
            data_blocks Int32,
            parity_blocks Int32,
            block_size Int64,
            data_dir String,
            distribution Array(Int32),
            parts_json String,
            xlmeta_device_id Int32,
            xlmeta_ino Int64
        ) ENGINE = MergeTree()
        ORDER BY (bucket, key)
    "#;

    client
        .query(ddl)
        .execute()
        .await
        .context("create objects table")?;
    Ok(())
}

async fn process_device(
    client: &clickhouse::Client,
    devices: &[String],
    device_id: i32,
    batch_size: usize,
    queue_depth: u32,
    dry_run: bool,
    limit: Option<usize>,
) -> Result<(u64, u64)> {
    // Step 1: Create temp table with xlmeta locations for this device
    let limit_clause = limit.map(|l| format!("LIMIT {}", l)).unwrap_or_default();

    client
        .query("DROP TABLE IF EXISTS tmp_xlmetas")
        .execute()
        .await
        .context("drop tmp_xlmetas")?;

    let create_tmp = format!(
        r#"
        CREATE TABLE tmp_xlmetas ENGINE = Memory AS
        SELECT DISTINCT ON (bucket, key)
            toValidUTF8(bucket) as bucket,
            toValidUTF8(key) as key,
            device_id,
            xlmeta_ino,
            data_dir_ino
        FROM s3_xlmeta_locations s
        WHERE s.device_id = {}
        AND NOT EXISTS (
            SELECT 1 FROM objects o
            WHERE o.bucket = toValidUTF8(s.bucket)
            AND o.key = toValidUTF8(s.key)
        )
        {}
        "#,
        device_id, limit_clause
    );

    client
        .query(&create_tmp)
        .execute()
        .await
        .context("create tmp_xlmetas")?;

    // Check how many objects to process
    let count: u64 = client
        .query("SELECT count() FROM tmp_xlmetas")
        .fetch_one()
        .await
        .context("count tmp_xlmetas")?;

    if count == 0 {
        info!("  No new objects to process for device {}", device_id);
        client
            .query("DROP TABLE IF EXISTS tmp_xlmetas")
            .execute()
            .await?;
        return Ok((0, 0));
    }

    info!("  {} objects to process for device {}", count, device_id);

    // Set up channels for producer-consumer pattern
    let (ready_tx, ready_rx) = mpsc::sync_channel::<ReadyXlmeta>(queue_depth as usize * 2);
    let (result_tx, result_rx) = mpsc::sync_channel::<ReaderResult>(batch_size);

    // Spawn reader thread
    let device_path = devices[device_id as usize].clone();
    let reader_queue_depth = queue_depth;
    let reader_handle = thread::spawn(move || {
        reader_thread(device_path, ready_rx, result_tx, reader_queue_depth)
    });

    // Step 2: Query extents and stream to reader
    let query = format!(
        r#"
        SELECT
            t.bucket,
            t.key,
            t.xlmeta_ino,
            t.data_dir_ino,
            fe.logical_offset,
            fe.physical_offset,
            fe.length,
            COUNT(*) OVER (PARTITION BY fe.ino) as extent_count
        FROM tmp_xlmetas t
        JOIN file_extents AS fe
            ON fe.ino = t.xlmeta_ino
            AND fe.device_id = t.device_id
        WHERE fe.device_id = {}
        ORDER BY fe.physical_offset
        "#,
        device_id
    );

    let mut cursor = client
        .query(&query)
        .fetch::<ExtentRow>()
        .context("fetch extents")?;

    let pb = ProgressBar::new(count);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}) {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Accumulate extents and send complete xlmetas to reader
    let mut pending: HashMap<i64, PendingXlmeta> = HashMap::new();
    let mut xlmeta_id = 0u64;

    let producer_result: Result<()> = async {
        while let Some(row) = cursor.next().await? {
            let ino = row.xlmeta_ino;

            let entry = pending.entry(ino).or_insert_with(|| PendingXlmeta {
                bucket: row.bucket.clone(),
                key: row.key.clone(),
                extent_count: row.extent_count,
                extents: Vec::new(),
            });

            entry.extents.push(Extent {
                logical_offset: row.logical_offset,
                physical_offset: row.physical_offset,
                length: row.length,
            });

            // Check if we have all extents for this inode
            if entry.extents.len() as u64 == entry.extent_count {
                let mut entry = pending.remove(&ino).unwrap();
                entry.extents.sort_by_key(|e| e.logical_offset);
                let file_size: u64 = entry.extents.iter().map(|e| e.length as u64).sum();

                let ready = ReadyXlmeta {
                    id: xlmeta_id,
                    bucket: entry.bucket,
                    key: entry.key,
                    device_id,
                    ino,
                    extents: entry.extents,
                    file_size,
                };
                xlmeta_id += 1;

                if ready_tx.send(ready).is_err() {
                    break;
                }
            }
        }
        Ok(())
    }
    .await;

    // Signal reader to finish
    drop(ready_tx);

    // Collect results and insert
    let mut objects_batch: Vec<ChObject> = Vec::with_capacity(batch_size);
    let mut objects_count = 0u64;
    let mut errors = 0u64;

    for result in result_rx {
        match result {
            ReaderResult::Parsed(obj) => {
                objects_batch.push(obj);
                objects_count += 1;
                pb.inc(1);

                if !dry_run && objects_batch.len() >= batch_size {
                    insert_objects(client, &mut objects_batch).await?;
                }
            }
            ReaderResult::Error { bucket, key, error } => {
                warn!("Failed to parse xl.meta {}:{}: {}", bucket, key, error);
                errors += 1;
                pb.inc(1);
            }
        }
    }

    // Insert remaining
    if !dry_run && !objects_batch.is_empty() {
        insert_objects(client, &mut objects_batch).await?;
    }

    pb.finish_with_message("done");

    // Wait for reader thread
    reader_handle
        .join()
        .map_err(|_| anyhow::anyhow!("reader thread panicked"))??;

    producer_result?;

    // Cleanup
    client
        .query("DROP TABLE IF EXISTS tmp_xlmetas")
        .execute()
        .await
        .context("drop tmp_xlmetas")?;

    Ok((objects_count, errors))
}

/// Accumulated state for a single xl.meta file
struct PendingXlmeta {
    bucket: String,
    key: String,
    extent_count: u64,
    extents: Vec<Extent>,
}

// ═══════════════════════════════════════════════════════════════════════
// Linux io_uring reader
// ═══════════════════════════════════════════════════════════════════════

#[cfg(target_os = "linux")]
fn reader_thread(
    device_path: String,
    ready_rx: mpsc::Receiver<ReadyXlmeta>,
    result_tx: mpsc::SyncSender<ReaderResult>,
    queue_depth: u32,
) -> Result<()> {
    use io_uring::{opcode, types, IoUring};
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;

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

    loop {
        // Receive xlmetas and submit reads
        while pending_submissions < queue_depth {
            let xlmeta = if in_flight.is_empty() {
                match ready_rx.recv() {
                    Ok(x) => x,
                    Err(_) => break,
                }
            } else {
                match ready_rx.try_recv() {
                    Ok(x) => x,
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            };

            let xlmeta_idx = next_xlmeta_idx;
            next_xlmeta_idx += 1;

            let buffer = vec![0u8; xlmeta.file_size as usize];

            for (extent_idx, extent) in xlmeta.extents.iter().enumerate() {
                let user_data = (xlmeta_idx << 32) | (extent_idx as u64);
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
                        .map_err(|_| anyhow::anyhow!("SQ full"))?;
                }
                pending_submissions += 1;
            }

            in_flight.insert(xlmeta_idx, InFlight { xlmeta, buffer, extents_done: 0 });
        }

        if in_flight.is_empty() && ready_rx.try_recv().is_err() {
            break;
        }

        ring.submit_and_wait(1).context("io_uring submit_and_wait")?;

        while let Some(cqe) = ring.completion().next() {
            pending_submissions -= 1;
            let user_data = cqe.user_data();
            let xlmeta_idx = user_data >> 32;

            if cqe.result() < 0 {
                if let Some(inf) = in_flight.remove(&xlmeta_idx) {
                    let _ = result_tx.send(ReaderResult::Error {
                        bucket: inf.xlmeta.bucket,
                        key: inf.xlmeta.key,
                        error: format!("read error: {}", cqe.result()),
                    });
                }
                continue;
            }

            if let Some(inf) = in_flight.get_mut(&xlmeta_idx) {
                inf.extents_done += 1;

                if inf.extents_done == inf.xlmeta.extents.len() {
                    let inf = in_flight.remove(&xlmeta_idx).unwrap();
                    let obj = parse_xlmeta_to_object(&inf.buffer, &inf.xlmeta);
                    let _ = result_tx.send(obj);
                }
            }
        }
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// Non-Linux fallback reader (pread-based)
// ═══════════════════════════════════════════════════════════════════════

#[cfg(not(target_os = "linux"))]
fn reader_thread(
    device_path: String,
    ready_rx: mpsc::Receiver<ReadyXlmeta>,
    result_tx: mpsc::SyncSender<ReaderResult>,
    _queue_depth: u32,
) -> Result<()> {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    let mut file = File::open(&device_path)
        .with_context(|| format!("open device {}", device_path))?;

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
        }

        let result = if let Some(err) = read_error {
            ReaderResult::Error {
                bucket: xlmeta.bucket,
                key: xlmeta.key,
                error: err,
            }
        } else {
            parse_xlmeta_to_object(&buffer, &xlmeta)
        };

        if result_tx.send(result).is_err() {
            break;
        }
    }

    Ok(())
}

/// Parse xl.meta buffer into ChObject or error
fn parse_xlmeta_to_object(buffer: &[u8], xlmeta: &ReadyXlmeta) -> ReaderResult {
    match xlmeta::parse(buffer) {
        Ok(meta) => {
            let compression = meta
                .user_meta
                .get("x-minio-internal-compression")
                .cloned()
                .unwrap_or_default();

            let parts_json = serde_json::to_string(
                &meta
                    .parts
                    .iter()
                    .map(|p| {
                        serde_json::json!({
                            "number": p.number,
                            "size": p.size,
                            "actual_size": p.actual_size
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_else(|_| "[]".to_string());

            let data_dir = meta.data_dir_string();
            let distribution: Vec<i32> = meta.distribution.iter().map(|&d| d as i32).collect();

            ReaderResult::Parsed(ChObject {
                bucket: xlmeta.bucket.clone(),
                key: xlmeta.key.clone(),
                size: meta.size,
                mod_time: meta.mod_time,
                etag: meta.etag,
                content_type: meta.content_type,
                compression,
                data_blocks: meta.data_blocks as i32,
                parity_blocks: meta.parity_blocks as i32,
                block_size: meta.block_size,
                data_dir,
                distribution,
                parts_json,
                xlmeta_device_id: xlmeta.device_id,
                xlmeta_ino: xlmeta.ino,
            })
        }
        Err(e) => ReaderResult::Error {
            bucket: xlmeta.bucket.clone(),
            key: xlmeta.key.clone(),
            error: format!("parse error: {}", e),
        },
    }
}

async fn insert_objects(client: &clickhouse::Client, objects: &mut Vec<ChObject>) -> Result<()> {
    if objects.is_empty() {
        return Ok(());
    }

    let mut inserter = client.insert::<ChObject>("objects").await?;
    for obj in objects.drain(..) {
        inserter.write(&obj).await.context("write object")?;
    }
    inserter.end().await.context("end insert")?;

    Ok(())
}

/// Resolve `--root` paths to actual device paths.
fn resolve_devices(roots: &[String]) -> Result<Vec<String>> {
    use std::path::Path;

    let mut devices = Vec::new();
    for root in roots {
        let path = Path::new(root);

        if let Some(dev_name) = path.file_name().and_then(|n| n.to_str()) {
            let sysfs_dir = Path::new("/sys/block").join(dev_name);
            if sysfs_dir.is_dir() {
                let mut parts: Vec<String> = std::fs::read_dir(&sysfs_dir)?
                    .filter_map(|e| e.ok())
                    .filter_map(|e| {
                        let name = e.file_name().to_string_lossy().into_owned();
                        if name.starts_with(dev_name)
                            && name.ends_with(|c: char| c.is_ascii_digit())
                        {
                            Some(format!("/dev/{}", name))
                        } else {
                            None
                        }
                    })
                    .collect();

                if !parts.is_empty() {
                    parts.sort_by(|a, b| {
                        let num_a = a
                            .trim_start_matches(&format!("/dev/{}", dev_name))
                            .parse::<u32>()
                            .unwrap_or(0);
                        let num_b = b
                            .trim_start_matches(&format!("/dev/{}", dev_name))
                            .parse::<u32>()
                            .unwrap_or(0);
                        num_a.cmp(&num_b)
                    });
                    info!(
                        "{} is a whole disk with {} partition(s), expanding",
                        root,
                        parts.len()
                    );
                    devices.extend(parts);
                    continue;
                }
            }
        }

        devices.push(root.clone());
    }
    Ok(devices)
}
