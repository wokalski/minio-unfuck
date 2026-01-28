//! meta-reader: one-shot scan of XFS devices → ClickHouse
//!
//! Scans raw block devices (or loop images) using fxfsp, populates ClickHouse with
//! inodes, directory entries, and file extents.

use std::ops::ControlFlow;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use fxfsp::{FsEvent, IoEngine, MaybeInstrumented};
use serde::Serialize;
use tracing::{info, Level};

const BATCH_SIZE: usize = 100_000;

#[derive(Parser, Debug)]
#[command(
    name = "meta-reader",
    about = "Scan XFS devices and populate ClickHouse with metadata"
)]
struct Args {
    /// Raw block device or XFS image file paths
    #[arg(long = "root", required = true)]
    roots: Vec<String>,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://localhost:8123")]
    clickhouse_url: String,

    /// ClickHouse database name
    #[arg(long, default_value = "minio")]
    clickhouse_db: String,

    /// Stop scanning after the first N allocation groups per device (0 = all)
    #[arg(long, default_value = "0")]
    max_ags: u32,
}

// ── ClickHouse row types ──────────────────────────────────────────────

#[derive(clickhouse::Row, Serialize)]
struct ChInode {
    device_id: i32,
    ino: i64,
    mode: i32,
    size: i64,
    nlink: i32,
    uid: i32,
    gid: i32,
    mtime_sec: i64,
    nblocks: i64,
    ag_number: i32,
}

#[derive(clickhouse::Row, Serialize)]
struct ChDir {
    device_id: i32,
    parent_ino: i64,
    child_ino: i64,
    name: String,
    file_type: i32,
}

#[derive(clickhouse::Row, Serialize)]
struct ChExtent {
    device_id: i32,
    ino: i64,
    logical_offset: i64,
    physical_offset: i64,
    length: i64,
}

// ── Channel event (same as before) ───────────────────────────────────

enum ScanEvent {
    Inode {
        device_id: i32,
        ino: i64,
        mode: i32,
        size: i64,
        nlink: i32,
        uid: i32,
        gid: i32,
        mtime_sec: i64,
        nblocks: i64,
        ag_number: i32,
    },
    Extent {
        device_id: i32,
        ino: i64,
        logical_offset: i64,
        physical_offset: i64,
        length: i64,
    },
    Dir {
        device_id: i32,
        parent_ino: i64,
        child_ino: i64,
        name: String,
        file_type: i32,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();
    let args = Args::parse();

    info!("meta-reader starting");
    info!("roots: {:?}", args.roots);
    info!("clickhouse: {}/{}", args.clickhouse_url, args.clickhouse_db);

    // Resolve roots: expand whole-disk devices to their partitions
    let devices = resolve_devices(&args.roots)?;
    info!("Resolved {} device(s): {:?}", devices.len(), devices);

    // Phase 1: Scan each device with fxfsp → ClickHouse
    let scan_devices = if args.max_ags > 0 {
        info!(
            "Phase 1: XFS scan of 1 device (--max-ags {} limits to first partition)",
            args.max_ags
        );
        &devices[..1]
    } else {
        info!("Phase 1: XFS scan of {} device(s)", devices.len());
        &devices[..]
    };

    let total_start = Instant::now();
    for (device_id, device_path) in scan_devices.iter().enumerate() {
        info!("Scanning device {} ({})", device_id, device_path);
        let dev_start = Instant::now();
        scan_device(
            &args.clickhouse_url,
            &args.clickhouse_db,
            device_id as i32,
            device_path,
            args.max_ags,
        )
        .with_context(|| format!("scan device {}", device_path))?;
        info!(
            "Device {} done in {:.1}s",
            device_id,
            dev_start.elapsed().as_secs_f64()
        );
    }

    info!(
        "All scans done in {:.1}s",
        total_start.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Scan a single XFS device using fxfsp, ingesting into ClickHouse.
///
/// Architecture:
/// - Scan thread: fxfsp callback pushes owned ScanEvent into unbounded channel
/// - Writer thread: drains channel, batches rows, inserts into ClickHouse via HTTP
fn scan_device(
    ch_url: &str,
    ch_db: &str,
    device_id: i32,
    device_path: &str,
    max_ags: u32,
) -> Result<()> {
    let (tx, rx) = mpsc::channel::<ScanEvent>();

    // Writer thread: runs a tokio runtime, batches events, inserts into ClickHouse
    let url = ch_url.to_string();
    let db = ch_db.to_string();
    let writer = thread::spawn(move || -> Result<(u64, u64, u64)> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build tokio runtime")?;

        rt.block_on(clickhouse_writer(rx, &url, &db))
    });

    // Scan thread: fxfsp callback pushes into channel
    let engine = IoEngine::open(device_path, 256 * 1024, 2 * 1024 * 1024)
        .map_err(|e| anyhow::anyhow!("open device: {:?}", e))?;
    let mut fxfsp_reader = MaybeInstrumented::from_env(engine)
        .map_err(|e| anyhow::anyhow!("instrument reader: {:?}", e))?;

    let mut block_size: u32 = 0;

    fxfsp::scan_reader(&mut fxfsp_reader, |event| {
        match event {
            FsEvent::Superblock {
                block_size: bs, ..
            } => {
                block_size = *bs;
            }
            FsEvent::InodeFound {
                ag_number,
                ino,
                mode,
                size,
                uid,
                gid,
                nlink,
                mtime_sec,
                nblocks,
                extents,
                ..
            } => {
                if max_ags > 0 && *ag_number >= max_ags {
                    return ControlFlow::Break(());
                }

                let _ = tx.send(ScanEvent::Inode {
                    device_id,
                    ino: *ino as i64,
                    mode: *mode as i32,
                    size: *size as i64,
                    nlink: *nlink as i32,
                    uid: *uid as i32,
                    gid: *gid as i32,
                    mtime_sec: *mtime_sec as i64,
                    nblocks: *nblocks as i64,
                    ag_number: *ag_number as i32,
                });

                if let Some(exts) = extents {
                    for ext in exts {
                        let phys_offset = ext.start_block * block_size as u64;
                        let logical_offset = ext.logical_offset * block_size as u64;
                        let length = ext.block_count * block_size as u64;
                        let _ = tx.send(ScanEvent::Extent {
                            device_id,
                            ino: *ino as i64,
                            logical_offset: logical_offset as i64,
                            physical_offset: phys_offset as i64,
                            length: length as i64,
                        });
                    }
                }
            }
            FsEvent::FileExtents { ino, extents } => {
                for ext in extents {
                    let phys_offset = ext.start_block * block_size as u64;
                    let logical_offset = ext.logical_offset * block_size as u64;
                    let length = ext.block_count * block_size as u64;
                    let _ = tx.send(ScanEvent::Extent {
                        device_id,
                        ino: *ino as i64,
                        logical_offset: logical_offset as i64,
                        physical_offset: phys_offset as i64,
                        length: length as i64,
                    });
                }
            }
            FsEvent::DirEntry {
                parent_ino,
                child_ino,
                name,
                file_type,
            } => {
                if *name != b"." && *name != b".." {
                    let _ = tx.send(ScanEvent::Dir {
                        device_id,
                        parent_ino: *parent_ino as i64,
                        child_ino: *child_ino as i64,
                        name: String::from_utf8_lossy(name).into_owned(),
                        file_type: *file_type as i32,
                    });
                }
            }
        }
        ControlFlow::Continue(())
    })
    .map_err(|e| anyhow::anyhow!("fxfsp scan: {:?}", e))?;

    // Signal writer to finish
    drop(tx);

    // Wait for writer and get stats
    let (inodes, dirs, extents) = writer
        .join()
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
    info!(
        "Device {}: {} inodes, {} dirs, {} extents",
        device_id, inodes, dirs, extents
    );

    Ok(())
}

/// ClickHouse writer: drains the channel, batches rows, and inserts via HTTP.
///
/// Accumulates rows into per-table Vec buffers. When any buffer hits BATCH_SIZE,
/// it's flushed as a single INSERT. ClickHouse async_insert is enabled server-side
/// for additional server-side batching.
async fn clickhouse_writer(
    rx: mpsc::Receiver<ScanEvent>,
    url: &str,
    db: &str,
) -> Result<(u64, u64, u64)> {
    let client = clickhouse::Client::default()
        .with_url(url)
        .with_database(db);

    let mut inode_batch: Vec<ChInode> = Vec::with_capacity(BATCH_SIZE);
    let mut dir_batch: Vec<ChDir> = Vec::with_capacity(BATCH_SIZE);
    let mut extent_batch: Vec<ChExtent> = Vec::with_capacity(BATCH_SIZE);

    let mut inode_count = 0u64;
    let mut dir_count = 0u64;
    let mut extent_count = 0u64;

    for event in &rx {
        match event {
            ScanEvent::Inode {
                device_id,
                ino,
                mode,
                size,
                nlink,
                uid,
                gid,
                mtime_sec,
                nblocks,
                ag_number,
            } => {
                inode_batch.push(ChInode {
                    device_id,
                    ino,
                    mode,
                    size,
                    nlink,
                    uid,
                    gid,
                    mtime_sec,
                    nblocks,
                    ag_number,
                });
                inode_count += 1;
                if inode_batch.len() >= BATCH_SIZE {
                    flush_inodes(&client, &mut inode_batch).await?;
                }
                if inode_count % 1_000_000 == 0 {
                    info!(
                        "  progress: {} inodes, {} dirs, {} extents",
                        inode_count, dir_count, extent_count
                    );
                }
            }
            ScanEvent::Dir {
                device_id,
                parent_ino,
                child_ino,
                name,
                file_type,
            } => {
                dir_batch.push(ChDir {
                    device_id,
                    parent_ino,
                    child_ino,
                    name,
                    file_type,
                });
                dir_count += 1;
                if dir_batch.len() >= BATCH_SIZE {
                    flush_dirs(&client, &mut dir_batch).await?;
                }
            }
            ScanEvent::Extent {
                device_id,
                ino,
                logical_offset,
                physical_offset,
                length,
            } => {
                extent_batch.push(ChExtent {
                    device_id,
                    ino,
                    logical_offset,
                    physical_offset,
                    length,
                });
                extent_count += 1;
                if extent_batch.len() >= BATCH_SIZE {
                    flush_extents(&client, &mut extent_batch).await?;
                }
            }
        }
    }

    // Flush remaining
    if !inode_batch.is_empty() {
        flush_inodes(&client, &mut inode_batch).await?;
    }
    if !dir_batch.is_empty() {
        flush_dirs(&client, &mut dir_batch).await?;
    }
    if !extent_batch.is_empty() {
        flush_extents(&client, &mut extent_batch).await?;
    }

    Ok((inode_count, dir_count, extent_count))
}

async fn flush_inodes(client: &clickhouse::Client, batch: &mut Vec<ChInode>) -> Result<()> {
    let mut insert = client.insert("inodes")?;
    for row in batch.drain(..) {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

async fn flush_dirs(client: &clickhouse::Client, batch: &mut Vec<ChDir>) -> Result<()> {
    let mut insert = client.insert("dirs")?;
    for row in batch.drain(..) {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

async fn flush_extents(client: &clickhouse::Client, batch: &mut Vec<ChExtent>) -> Result<()> {
    let mut insert = client.insert("file_extents")?;
    for row in batch.drain(..) {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

/// Resolve `--root` paths to actual device paths.
///
/// If a path points to a whole-disk block device (e.g. `/dev/sde`) that has
/// partitions, expand it to the sorted list of partition devices
/// (`/dev/sde1`, `/dev/sde2`, …) via sysfs.  Otherwise return the path as-is
/// (it's already a partition device or an image file).
fn resolve_devices(roots: &[String]) -> Result<Vec<String>> {
    let mut devices = Vec::new();
    for root in roots {
        let path = Path::new(root);

        // Try sysfs partition discovery for block devices
        if let Some(dev_name) = path.file_name().and_then(|n| n.to_str()) {
            let sysfs_dir = Path::new("/sys/block").join(dev_name);
            if sysfs_dir.is_dir() {
                // It's a whole-disk block device — look for partition subdirs
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
