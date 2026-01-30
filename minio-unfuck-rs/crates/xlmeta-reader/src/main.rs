//! xlmeta-reader: Parse all xl.meta files and populate ClickHouse tables
//!
//! Reads xl.meta files sequentially (sorted by physical offset for HDD optimization),
//! parses them using mfu-core, and populates:
//! - `objects` table: object metadata (bucket, key, size, compression, etc.)
//! - `file_shards` table: physical shard locations (populated via SQL join)

use std::collections::HashMap;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use tracing::{info, warn, Level};

use mfu_core::raw_io::{DeviceReader, PreadDeviceReader};
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

    /// Dry run: query metadata but don't write to objects/file_shards tables
    #[arg(long)]
    dry_run: bool,

    /// Limit processing to N objects (for testing)
    #[arg(long)]
    limit: Option<usize>,
}

// ── ClickHouse row types ──────────────────────────────────────────────

#[derive(clickhouse::Row, Serialize, Debug)]
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

#[derive(clickhouse::Row, Serialize, Debug)]
struct ChFileShard {
    device_id: i32,
    bucket: String,
    key: String,
    part_number: i32,
    disk_index: i32,
    shard_ino: i64,
    physical_offset: i64,
    total_length: i64,
}

// ── Query result types ────────────────────────────────────────────────

#[derive(clickhouse::Row, Deserialize, Debug)]
struct XlmetaLocation {
    bucket: String,
    key: String,
    device_id: i32,
    xlmeta_ino: i64,
    data_dir_ino: i64,
    size: i64,
    first_physical_offset: i64,
}

#[derive(clickhouse::Row, Deserialize, Debug)]
struct ExtentRow {
    device_id: i32,
    ino: i64,
    logical_offset: i64,
    physical_offset: i64,
    length: i64,
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

    // Open raw devices
    let reader = PreadDeviceReader::open(&devices).context("open devices")?;

    // Phase 1: Query xl.meta locations (one per object, deduplicated)
    info!("Phase 1: Querying xl.meta locations...");

    // Build query with optional LIMIT
    let limit_clause = args.limit.map(|l| format!("LIMIT {}", l)).unwrap_or_default();

    // Use a subquery to get one xl.meta per (bucket, key), then join for extents
    let xlmeta_query = format!(r#"
        WITH unique_objects AS (
            SELECT
                bucket,
                key,
                argMin(device_id, device_id) as device_id,
                argMin(xlmeta_ino, device_id) as xlmeta_ino,
                argMin(data_dir_ino, device_id) as data_dir_ino
            FROM s3_xlmeta_locations
            GROUP BY bucket, key
            {}
        )
        SELECT
            u.bucket,
            u.key,
            u.device_id,
            u.xlmeta_ino,
            u.data_dir_ino,
            i.size,
            min(fe.physical_offset) as first_physical_offset
        FROM unique_objects u
        JOIN inodes i ON i.device_id = u.device_id AND i.ino = u.xlmeta_ino
        JOIN file_extents fe ON fe.device_id = u.device_id AND fe.ino = u.xlmeta_ino
        GROUP BY u.bucket, u.key, u.device_id, u.xlmeta_ino, u.data_dir_ino, i.size
        ORDER BY u.device_id, first_physical_offset
    "#, limit_clause);

    let xlmeta_locs: Vec<XlmetaLocation> = client
        .query(&xlmeta_query)
        .fetch_all()
        .await
        .context("query xlmeta locations")?;

    info!(
        "Found {} unique objects to process{}",
        xlmeta_locs.len(),
        args.limit.map(|l| format!(" (limited to {})", l)).unwrap_or_default()
    );

    // Phase 2: Read xl.meta files and parse
    info!("Phase 2: Reading and parsing xl.meta files...");

    let pb = ProgressBar::new(xlmeta_locs.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}) {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    let mut objects: Vec<ChObject> = Vec::with_capacity(xlmeta_locs.len());
    let mut errors = 0u64;
    let start = Instant::now();

    // Pre-fetch all extents for the xl.meta files we'll read
    let ino_list: Vec<(i32, i64)> = xlmeta_locs
        .iter()
        .map(|loc| (loc.device_id, loc.xlmeta_ino))
        .collect();

    info!("Fetching extents for {} xl.meta files...", ino_list.len());
    let extent_map = fetch_extents(&client, &ino_list).await?;
    info!("Loaded extents for {} files", extent_map.len());

    for loc in &xlmeta_locs {
        let extent_key = (loc.device_id, loc.xlmeta_ino);
        let extents = match extent_map.get(&extent_key) {
            Some(exts) => exts,
            None => {
                warn!(
                    "No extents for {}:{} device={} ino={}",
                    loc.bucket, loc.key, loc.device_id, loc.xlmeta_ino
                );
                errors += 1;
                pb.inc(1);
                continue;
            }
        };

        // Convert to mfu-core Extent type
        let mfu_extents: Vec<Extent> = extents
            .iter()
            .map(|e| Extent {
                logical_offset: e.logical_offset,
                physical_offset: e.physical_offset,
                length: e.length,
            })
            .collect();

        // Read xl.meta file
        let data = match reader.read_file(loc.device_id as usize, &mfu_extents, loc.size as u64) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "Failed to read xl.meta {}:{} device={}: {}",
                    loc.bucket, loc.key, loc.device_id, e
                );
                errors += 1;
                pb.inc(1);
                continue;
            }
        };

        // Parse xl.meta
        let meta = match xlmeta::parse(&data) {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to parse xl.meta {}:{}: {}",
                    loc.bucket, loc.key, e
                );
                errors += 1;
                pb.inc(1);
                continue;
            }
        };

        // Extract compression from user_meta if present
        let compression = meta
            .user_meta
            .get("x-minio-internal-compression")
            .cloned()
            .unwrap_or_default();

        // Serialize parts to JSON
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

        objects.push(ChObject {
            bucket: loc.bucket.clone(),
            key: loc.key.clone(),
            size: meta.size,
            mod_time: meta.mod_time,
            etag: meta.etag.clone(),
            content_type: meta.content_type.clone(),
            compression,
            data_blocks: meta.data_blocks as i32,
            parity_blocks: meta.parity_blocks as i32,
            block_size: meta.block_size,
            data_dir: meta.data_dir_string(),
            distribution: meta.distribution.iter().map(|&d| d as i32).collect(),
            parts_json,
            xlmeta_device_id: loc.device_id,
            xlmeta_ino: loc.xlmeta_ino,
        });

        // Batch insert (skip in dry-run mode)
        if !args.dry_run && objects.len() >= args.batch_size {
            insert_objects(&client, &mut objects).await?;
        }

        pb.inc(1);
    }

    // Insert remaining objects (skip in dry-run mode)
    if !args.dry_run && !objects.is_empty() {
        insert_objects(&client, &mut objects).await?;
    }

    pb.finish_with_message("done");

    let elapsed = start.elapsed();
    info!(
        "Phase 2 complete: {} objects parsed in {:.1}s ({} errors)",
        xlmeta_locs.len() - errors as usize,
        elapsed.as_secs_f64(),
        errors
    );

    if args.dry_run {
        info!("Dry run: skipping database writes");
        info!("Would have inserted {} objects", xlmeta_locs.len() - errors as usize);
        info!("Sample parsed objects:");
        for obj in objects.iter().take(5) {
            info!(
                "  {}/{}: size={} etag={} compression={} parts={}",
                obj.bucket, obj.key, obj.size, obj.etag, obj.compression,
                obj.parts_json.len()
            );
        }
    } else {
        // Phase 3: Populate file_shards via SQL
        info!("Phase 3: Populating file_shards table...");
        populate_file_shards(&client).await?;
    }

    info!("xlmeta-reader complete");
    Ok(())
}

async fn fetch_extents(
    client: &clickhouse::Client,
    ino_list: &[(i32, i64)],
) -> Result<HashMap<(i32, i64), Vec<ExtentRow>>> {
    // Build IN clause efficiently
    // For large lists, we chunk the query
    let mut result: HashMap<(i32, i64), Vec<ExtentRow>> = HashMap::new();

    const CHUNK_SIZE: usize = 10000;
    for chunk in ino_list.chunks(CHUNK_SIZE) {
        let tuples: Vec<String> = chunk
            .iter()
            .map(|(dev, ino)| format!("({}, {})", dev, ino))
            .collect();
        let in_clause = tuples.join(", ");

        let query = format!(
            r#"
            SELECT device_id, ino, logical_offset, physical_offset, length
            FROM file_extents
            WHERE (device_id, ino) IN ({})
            ORDER BY device_id, ino, logical_offset
            "#,
            in_clause
        );

        let extents: Vec<ExtentRow> = client
            .query(&query)
            .fetch_all()
            .await
            .context("fetch extents")?;

        for ext in extents {
            result
                .entry((ext.device_id, ext.ino))
                .or_default()
                .push(ext);
        }
    }

    Ok(result)
}

async fn insert_objects(client: &clickhouse::Client, objects: &mut Vec<ChObject>) -> Result<()> {
    if objects.is_empty() {
        return Ok(());
    }

    let mut inserter = client.insert("objects")?;
    for obj in objects.drain(..) {
        inserter.write(&obj).await.context("write object")?;
    }
    inserter.end().await.context("end insert")?;

    Ok(())
}

async fn populate_file_shards(client: &clickhouse::Client) -> Result<()> {
    // Use SQL to populate file_shards from existing tables
    // This joins s3_xlmeta_locations with dirs (to find part.* files) and file_extents
    let query = r#"
        INSERT INTO file_shards (device_id, bucket, key, part_number, disk_index, shard_ino, physical_offset, total_length)
        SELECT
            s.device_id,
            s.bucket,
            s.key,
            toInt32(extractAll(d.name, 'part\.([0-9]+)')[1]) as part_number,
            0 as disk_index,  -- Will be filled by separate lookup
            d.child_ino as shard_ino,
            min(fe.physical_offset) as physical_offset,
            sum(fe.length) as total_length
        FROM s3_xlmeta_locations s
        JOIN dirs d ON d.device_id = s.device_id AND d.parent_ino = s.data_dir_ino
        JOIN file_extents fe ON fe.device_id = s.device_id AND fe.ino = d.child_ino
        WHERE d.name LIKE 'part.%'
        GROUP BY s.device_id, s.bucket, s.key, part_number, shard_ino
        SETTINGS join_algorithm = 'partial_merge', max_bytes_before_external_group_by = 10000000000
    "#;

    info!("Running file_shards INSERT query (this may take a while)...");
    let start = Instant::now();

    client
        .query(query)
        .execute()
        .await
        .context("populate file_shards")?;

    info!(
        "file_shards populated in {:.1}s",
        start.elapsed().as_secs_f64()
    );

    // Log count
    let count: u64 = client
        .query("SELECT count() FROM file_shards")
        .fetch_one()
        .await
        .context("count file_shards")?;

    info!("file_shards table: {} rows", count);

    Ok(())
}

/// Resolve `--root` paths to actual device paths.
///
/// If a path points to a whole-disk block device (e.g. `/dev/sde`) that has
/// partitions, expand it to the sorted list of partition devices
/// (`/dev/sde1`, `/dev/sde2`, …) via sysfs. Otherwise return the path as-is.
fn resolve_devices(roots: &[String]) -> Result<Vec<String>> {
    use std::path::Path;

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
