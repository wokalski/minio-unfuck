//! meta-reader: one-shot scan of XFS devices → DuckDB
//!
//! Scans raw block devices (or loop images) using fxfsp, populates DuckDB with
//! inodes, directory entries, file extents, cluster topology, and parsed xl.meta objects.

use std::ops::ControlFlow;
use std::path::Path;
use std::sync::mpsc;
use std::thread;

use anyhow::{Context, Result};
use clap::Parser;
use fxfsp::{FsEvent, IoEngine, MaybeInstrumented};
use indicatif::{ProgressBar, ProgressStyle};
use tracing::{info, warn, Level};

use mfu_core::db::{BulkInserter, MetadataDb};
use mfu_core::format;
use mfu_core::raw_io::{self, BatchReadRequest, DeviceReader, PreadDeviceReader};
use mfu_core::xlmeta;

#[derive(Parser, Debug)]
#[command(
    name = "meta-reader",
    about = "Scan XFS devices and populate DuckDB with MinIO metadata"
)]
struct Args {
    /// Raw block device or XFS image file paths
    #[arg(long = "root", required = true)]
    roots: Vec<String>,

    /// Output DuckDB file path
    #[arg(long, default_value = "metadata.db")]
    output: String,

    /// Stop scanning after the first N allocation groups per device (0 = all)
    #[arg(long, default_value = "0")]
    max_ags: u32,
}

/// Owned scan event sent from the scan thread to the writer thread.
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
    info!("output: {}", args.output);

    // Resolve roots: expand whole-disk devices to their partitions
    let devices = resolve_devices(&args.roots)?;
    info!("Resolved {} device(s): {:?}", devices.len(), devices);

    // Open DuckDB and configure for bulk loading
    let db = MetadataDb::open(&args.output).context("open database")?;
    db.configure_bulk_load().context("configure duckdb")?;

    // Phase 1: Scan each device with fxfsp
    // When --max-ags is set, only scan the first partition (quick test mode).
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

    for (device_id, device_path) in scan_devices.iter().enumerate() {
        info!("Scanning device {} ({})", device_id, device_path);
        scan_device(&db, device_id as i32, device_path, args.max_ags)
            .with_context(|| format!("scan device {}", device_path))?;
    }

    // Phase 2: Read format.json from each device via extents
    info!("Phase 2: Reading format.json files via extents");
    let reader = PreadDeviceReader::open(&devices).context("open device readers")?;

    let mut formats = Vec::new();
    for device_id in 0..devices.len() {
        match read_format_json(&db, &reader, device_id) {
            Ok(fmt) => {
                info!(
                    "Device {}: pool={}, disk={}",
                    device_id, fmt.id, fmt.xl.this
                );
                formats.push((device_id, fmt));
            }
            Err(e) => {
                warn!("Device {}: failed to read format.json: {}", device_id, e);
            }
        }
    }

    // Insert cluster topology
    if !formats.is_empty() {
        let cluster = format::build_cluster_config(&formats)?;
        for pool in &cluster.pools {
            for (set_idx, set) in pool.sets.iter().enumerate() {
                for disk in set {
                    db.insert_cluster_disk(
                        &pool.pool_id,
                        pool.pool_index as i32,
                        set_idx as i32,
                        disk.disk_index as i32,
                        &disk.uuid,
                        disk.device_id.map(|d| d as i32),
                    )?;
                }
            }
        }
        info!(
            "Cluster: {} pool(s), {} total set(s)",
            cluster.pools.len(),
            cluster.total_sets()
        );
    }

    // Phase 3: Find and batch-read all xl.meta files
    info!("Phase 3: Finding xl.meta files");
    let xlmeta_inodes = db.find_xlmeta_inodes()?;
    info!("Found {} xl.meta files", xlmeta_inodes.len());

    if !xlmeta_inodes.is_empty() {
        info!("Phase 4: Batch reading and parsing xl.meta files");

        // Build batch read requests
        let mut requests = Vec::new();
        for &(device_id, ino) in &xlmeta_inodes {
            let extents = db.get_extents(device_id, ino)?;
            let size = db.get_inode_size(device_id, ino)?.unwrap_or(0);
            if !extents.is_empty() && size > 0 {
                requests.push(BatchReadRequest {
                    device_id: device_id as usize,
                    ino: ino as u64,
                    extents,
                    size: size as u64,
                });
            }
        }

        info!(
            "Submitting {} extent-based reads (sorted by physical offset)",
            requests.len()
        );
        let results = raw_io::batch_read(&reader, requests);
        info!("Read {} xl.meta files", results.len());

        // Parse and insert objects
        let pb = ProgressBar::new(results.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        let mut parsed = 0u64;
        let mut failed = 0u64;

        db.conn().execute_batch("BEGIN TRANSACTION")?;
        for result in &results {
            match xlmeta::parse(&result.data) {
                Ok(mut meta) => {
                    if let Some((bucket, key)) = resolve_object_path(&db, result.ino) {
                        meta.bucket = bucket;
                        meta.key = key;
                        if let Err(e) = db.insert_object(&meta) {
                            warn!("Failed to insert object: {}", e);
                            failed += 1;
                        } else {
                            parsed += 1;
                        }
                    } else {
                        warn!("Could not resolve path for ino {}", result.ino);
                        failed += 1;
                    }
                }
                Err(e) => {
                    warn!("Failed to parse xl.meta (ino {}): {}", result.ino, e);
                    failed += 1;
                }
            }
            pb.inc(1);
        }
        db.conn().execute_batch("COMMIT")?;
        pb.finish_with_message("done");

        info!("Parsed {} objects ({} failed)", parsed, failed);
    }

    info!("Done. Database written to {}", args.output);
    Ok(())
}

/// Scan a single XFS device using fxfsp, populating inodes/dirs/file_extents in DuckDB.
///
/// Uses a channel to decouple the I/O-bound fxfsp scan from DB writes:
/// - Scan thread: pushes owned ScanEvent variants into a channel (just a memcpy)
/// - Writer thread: drains the channel into DuckDB via Appender API
fn scan_device(db: &MetadataDb, device_id: i32, device_path: &str, max_ags: u32) -> Result<()> {
    let (tx, rx) = mpsc::channel::<ScanEvent>();

    // Writer thread: opens its own connection, drains channel via Appender API
    let db_path = db.path().to_string();
    let writer = thread::spawn(move || -> Result<(u64, u64, u64)> {
        let bulk = BulkInserter::open(&db_path)?;
        let mut inodes = 0u64;
        let mut dirs = 0u64;
        let mut extents = 0u64;

        bulk.write_session(|session| {
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
                        session.append_inode(
                            device_id, ino, mode, size, nlink, uid, gid, mtime_sec, nblocks,
                            ag_number,
                        )?;
                        inodes += 1;
                        if inodes % 100_000 == 0 {
                            info!("  {} inodes written...", inodes);
                        }
                    }
                    ScanEvent::Extent {
                        device_id,
                        ino,
                        logical_offset,
                        physical_offset,
                        length,
                    } => {
                        session.append_extent(
                            device_id, ino, logical_offset, physical_offset, length,
                        )?;
                        extents += 1;
                    }
                    ScanEvent::Dir {
                        device_id,
                        parent_ino,
                        child_ino,
                        name,
                        file_type,
                    } => {
                        session.append_dir(device_id, parent_ino, child_ino, &name, file_type)?;
                        dirs += 1;
                    }
                }
            }
            Ok(())
        })?;

        Ok((inodes, dirs, extents))
    });

    // Scan thread: fxfsp callback just pushes into channel
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

/// Read format.json from a device via extent-based raw reads
fn read_format_json(
    db: &MetadataDb,
    reader: &PreadDeviceReader,
    device_id: usize,
) -> Result<format::DiskFormat> {
    let ino = db
        .resolve_path(device_id as i32, "/.minio.sys/format.json")?
        .context("format.json not found")?;

    let extents = db.get_extents(device_id as i32, ino)?;
    let size = db
        .get_inode_size(device_id as i32, ino)?
        .context("inode size not found")?;

    let data = reader.read_file(device_id, &extents, size as u64)?;
    format::parse_format(&data)
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
                        // Partition dirs are named like "sde1", "nvme0n1p1", etc.
                        // They start with the device name and have a trailing digit.
                        if name.starts_with(dev_name) && name.ends_with(|c: char| c.is_ascii_digit()) {
                            Some(format!("/dev/{}", name))
                        } else {
                            None
                        }
                    })
                    .collect();

                if !parts.is_empty() {
                    // Sort numerically by partition number
                    parts.sort_by(|a, b| {
                        let num_a = a.trim_start_matches(&format!("/dev/{}", dev_name))
                            .parse::<u32>().unwrap_or(0);
                        let num_b = b.trim_start_matches(&format!("/dev/{}", dev_name))
                            .parse::<u32>().unwrap_or(0);
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

        // Not a whole disk or no partitions found — use as-is
        devices.push(root.clone());
    }
    Ok(devices)
}

/// Resolve an xl.meta inode back to bucket/key by walking parent dirs.
///
/// xl.meta lives at: <bucket>/<key>/xl.meta
/// So we need to find the parent (key dir) and grandparent (bucket dir) names.
fn resolve_object_path(db: &MetadataDb, xlmeta_ino: u64) -> Option<(String, String)> {
    let conn = db.conn();

    // Find the parent directory containing this xl.meta
    let mut stmt = conn
        .prepare(
            "SELECT device_id, parent_ino FROM dirs WHERE child_ino = ? AND name = 'xl.meta' LIMIT 1",
        )
        .ok()?;
    let (device_id, key_ino): (i32, i64) = stmt
        .query_row(duckdb::params![xlmeta_ino as i64], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .ok()?;

    // Find the name of the key directory (parent of xl.meta)
    let mut stmt = conn
        .prepare(
            "SELECT parent_ino, name FROM dirs WHERE device_id = ? AND child_ino = ? AND name != '.' AND name != '..' LIMIT 1",
        )
        .ok()?;
    let (bucket_ino, key_name): (i64, String) = stmt
        .query_row(duckdb::params![device_id, key_ino], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .ok()?;

    // Find the name of the bucket directory (parent of key dir)
    let mut stmt = conn
        .prepare(
            "SELECT name FROM dirs WHERE device_id = ? AND child_ino = ? AND name != '.' AND name != '..' LIMIT 1",
        )
        .ok()?;
    let bucket_name: String = stmt
        .query_row(duckdb::params![device_id, bucket_ino], |row| row.get(0))
        .ok()?;

    Some((bucket_name, key_name))
}
