//! meta-reader: one-shot scan of XFS devices → ClickHouse
//!
//! Scans raw block devices (or loop images) using fxfsp, populates ClickHouse with
//! inodes, directory entries, and file extents.

use std::ops::ControlFlow;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use fxfsp::{
    parse_superblock, DirEntryInfo, FileExtentsInfo, FsContext, InodeInfo, IoEngine,
    MaybeInstrumented,
};
use serde::Serialize;
use tracing::{info, Level};

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
struct ChXlmeta {
    device_id: i32,
    parent_ino: i64,
    child_ino: i64,
}

#[derive(clickhouse::Row, Serialize)]
struct ChExtent {
    device_id: i32,
    ino: i64,
    logical_offset: i64,
    physical_offset: i64,
    length: i64,
}

// ── Channel events ───────────────────────────────────────────────────

enum ScanEvent {
    Inode {
        device_id: i32,
        ino: i64,
        mode: i32,
        size: i64,
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
    Xlmeta {
        device_id: i32,
        parent_ino: i64,
        child_ino: i64,
    },
    /// Marker indicating the XFS scan phase is complete.
    /// Inode/Extent/Xlmeta inserts can be finalized; only Dir events remain.
    ScanDone,
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

/// Helper to send extent events for a list of extents
fn send_extents(
    tx: &mpsc::Sender<ScanEvent>,
    device_id: i32,
    ino: u64,
    extents: &[fxfsp::Extent],
    ctx: &FsContext,
) {
    let block_size = ctx.block_size as u64;
    for ext in extents {
        let phys_offset = ext.start_byte(ctx);
        let logical_offset = ext.logical_offset * block_size;
        let length = ext.block_count * block_size;
        let _ = tx.send(ScanEvent::Extent {
            device_id,
            ino: ino as i64,
            logical_offset: logical_offset as i64,
            physical_offset: phys_offset as i64,
            length: length as i64,
        });
    }
}

/// Scan a single XFS device using fxfsp, ingesting into ClickHouse.
///
/// Architecture:
/// - Scan thread: fxfsp phased API pushes owned ScanEvent into unbounded channel
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
    let writer = thread::spawn(move || -> Result<(u64, u64, u64, u64)> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build tokio runtime")?;

        rt.block_on(clickhouse_writer(rx, &url, &db))
    });

    // Scan using fxfsp phased API
    let engine = IoEngine::open(device_path, 256 * 1024, 2 * 1024 * 1024)
        .map_err(|e| anyhow::anyhow!("open device: {:?}", e))?;
    let fxfsp_reader = MaybeInstrumented::from_env(engine)
        .map_err(|e| anyhow::anyhow!("instrument reader: {:?}", e))?;

    let (sb, mut scanner) = parse_superblock(fxfsp_reader)
        .map_err(|e| anyhow::anyhow!("parse superblock: {:?}", e))?;

    info!(
        "Superblock: block_size={} ag_count={} ag_blocks={} inode_size={} root_ino={}",
        sb.block_size, sb.ag_count, sb.ag_blocks, sb.inode_size, sb.root_ino
    );

    let ctx = scanner.context().clone();

    // Process each AG
    while let Some(ag_result) = scanner.next_ag() {
        let ag = ag_result.map_err(|e| anyhow::anyhow!("get AG: {:?}", e))?;
        let ag_number = ag.ag_number();

        if max_ags > 0 && ag_number >= max_ags {
            break;
        }

        // Phase 1: Scan inodes
        let phase2 = ag
            .scan_inodes(|inode: &InodeInfo| {
                let _ = tx.send(ScanEvent::Inode {
                    device_id,
                    ino: inode.ino as i64,
                    mode: inode.mode as i32,
                    size: inode.size as i64,
                    mtime_sec: inode.mtime_sec as i64,
                    nblocks: inode.nblocks as i64,
                    ag_number: inode.ag_number as i32,
                });

                // Send inline extents if present
                if let Some(ref exts) = inode.extents {
                    send_extents(&tx, device_id, inode.ino, exts, &ctx);
                }

                ControlFlow::Continue(())
            })
            .map_err(|e| anyhow::anyhow!("scan inodes: {:?}", e))?;

        // Phase 1.5: File extents (btree-format files)
        let phase3 = phase2
            .scan_file_extents(|fe: &FileExtentsInfo| {
                send_extents(&tx, device_id, fe.ino, &fe.extents, &ctx);
                ControlFlow::Continue(())
            })
            .map_err(|e| anyhow::anyhow!("scan file extents: {:?}", e))?;

        // Phase 2: Directory entries
        phase3
            .scan_dir_entries(|de: &DirEntryInfo| {
                if de.name == b"." || de.name == b".." {
                    // skip
                } else if de.name == b"xl.meta" {
                    let _ = tx.send(ScanEvent::Xlmeta {
                        device_id,
                        parent_ino: de.parent_ino as i64,
                        child_ino: de.child_ino as i64,
                    });
                } else {
                    let _ = tx.send(ScanEvent::Dir {
                        device_id,
                        parent_ino: de.parent_ino as i64,
                        child_ino: de.child_ino as i64,
                        name: String::from_utf8_lossy(de.name).into_owned(),
                        file_type: de.file_type as i32,
                    });
                }
                ControlFlow::Continue(())
            })
            .map_err(|e| anyhow::anyhow!("scan dir entries: {:?}", e))?;
    }

    // Signal that XFS scan is done - writer can finalize inode/xlmeta/extent inserts
    let _ = tx.send(ScanEvent::ScanDone);

    // Signal writer to finish
    drop(tx);

    // Wait for writer and get stats
    let (inodes, dirs, extents, xlmetas) = writer
        .join()
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
    info!(
        "Device {}: {} inodes, {} dirs, {} xlmeta, {} extents",
        device_id, inodes, dirs, xlmetas, extents
    );

    Ok(())
}

/// ClickHouse writer using Inserter API for automatic batching.
async fn clickhouse_writer(
    rx: mpsc::Receiver<ScanEvent>,
    url: &str,
    db: &str,
) -> Result<(u64, u64, u64, u64)> {
    let client = clickhouse::Client::default()
        .with_url(url)
        .with_database(db);

    let mut inode_count = 0u64;
    let mut dir_count = 0u64;
    let mut extent_count = 0u64;
    let mut xlmeta_count = 0u64;
    let mut total_events = 0u64;

    info!("writer: creating inserters");
    let mut inode_ins = client
        .inserter::<ChInode>("inodes")
        .with_max_rows(100_000)
        .with_period(Some(Duration::from_secs(10)));
    let mut dir_ins = client
        .inserter::<ChDir>("dirs")
        .with_max_rows(100_000)
        .with_period(Some(Duration::from_secs(10)));
    let mut extent_ins = client
        .inserter::<ChExtent>("file_extents")
        .with_max_rows(100_000)
        .with_period(Some(Duration::from_secs(10)));
    let mut xlmeta_ins = client
        .inserter::<ChXlmeta>("xlmeta_files")
        .with_max_rows(100_000)
        .with_period(Some(Duration::from_secs(10)));
    info!("writer: inserters ready, starting drain loop");

    while let Ok(event) = rx.recv() {
        total_events += 1;

        match event {
            ScanEvent::Inode {
                device_id,
                ino,
                mode,
                size,
                mtime_sec,
                nblocks,
                ag_number,
            } => {
                inode_ins
                    .write(&ChInode {
                        device_id,
                        ino,
                        mode,
                        size,
                        mtime_sec,
                        nblocks,
                        ag_number,
                    })
                    .await
                    .context("write inode")?;
                inode_ins.commit().await.context("commit inode")?;
                inode_count += 1;
            }
            ScanEvent::Dir {
                device_id,
                parent_ino,
                child_ino,
                name,
                file_type,
            } => {
                dir_ins
                    .write(&ChDir {
                        device_id,
                        parent_ino,
                        child_ino,
                        name,
                        file_type,
                    })
                    .await
                    .context("write dir")?;
                dir_ins.commit().await.context("commit dir")?;
                dir_count += 1;
            }
            ScanEvent::Xlmeta {
                device_id,
                parent_ino,
                child_ino,
            } => {
                xlmeta_ins
                    .write(&ChXlmeta {
                        device_id,
                        parent_ino,
                        child_ino,
                    })
                    .await
                    .context("write xlmeta")?;
                xlmeta_ins.commit().await.context("commit xlmeta")?;
                xlmeta_count += 1;
            }
            ScanEvent::Extent {
                device_id,
                ino,
                logical_offset,
                physical_offset,
                length,
            } => {
                extent_ins
                    .write(&ChExtent {
                        device_id,
                        ino,
                        logical_offset,
                        physical_offset,
                        length,
                    })
                    .await
                    .context("write extent")?;
                extent_ins.commit().await.context("commit extent")?;
                extent_count += 1;
            }
            ScanEvent::ScanDone => {
                // Ignored - inserters handle their own lifecycle
            }
        }

        if total_events % 100_000 == 0 {
            info!(
                "  {}k events | {} inodes, {} dirs, {} xlmeta, {} extents",
                total_events / 1_000,
                inode_count,
                dir_count,
                xlmeta_count,
                extent_count
            );
        }
    }

    info!(
        "writer: channel drained, finalizing inserters ({} inodes, {} dirs, {} xlmeta, {} extents)",
        inode_count, dir_count, xlmeta_count, extent_count
    );
    inode_ins.end().await.context("end inode inserter")?;
    dir_ins.end().await.context("end dir inserter")?;
    xlmeta_ins.end().await.context("end xlmeta inserter")?;
    extent_ins.end().await.context("end extent inserter")?;
    info!("writer: all inserters finalized");

    Ok((inode_count, dir_count, extent_count, xlmeta_count))
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
