//! xlmeta-reader: Parse xl.meta files and populate ClickHouse objects table
//!
//! Streams xl.meta file extents per device (sorted by physical offset for HDD),
//! uses io_uring for batched async reads (Linux) or pread (other platforms),
//! parses them, and inserts into `objects` table.

mod db;
mod devices;
mod parser;
mod reader;
mod types;

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use mfu_core::types::Extent;
use tracing::{info, warn, Level};

use reader::spawn_reader_thread;
use types::{ChObject, PendingXlmeta, ReadyXlmeta, ReaderResult};

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
    let client = Arc::new(
        clickhouse::Client::default()
            .with_url(&args.clickhouse_url)
            .with_database(&args.clickhouse_db),
    );

    let devices = devices::resolve_devices(&args.roots)?;
    info!("Resolved {} device(s): {:?}", devices.len(), devices);

    if !args.dry_run {
        db::create_objects_table(&client).await?;
    }

    let total_start = Instant::now();
    let mut total_objects = 0u64;
    let mut total_errors = 0u64;

    // Process each device
    for device_id in 0..devices.len() as i32 {
        info!("Processing device {}...", device_id);
        let dev_start = Instant::now();

        // Create temp table (RAII guard ensures cleanup)
        let tmp_guard = db::create_tmp_xlmetas(client.clone(), device_id, args.limit).await?;

        if tmp_guard.count() == 0 {
            info!("  No new objects to process for device {}", device_id);
            continue;
        }

        info!(
            "  {} objects to process for device {}",
            tmp_guard.count(),
            device_id
        );

        // Set up channels for producer-consumer pattern
        let (ready_tx, ready_rx) =
            mpsc::sync_channel::<ReadyXlmeta>(args.queue_depth as usize * 2);
        let (result_tx, result_rx) = mpsc::sync_channel::<ReaderResult>(args.batch_size);

        // Spawn reader thread (io_uring on Linux, pread on other platforms)
        let device_path = devices[device_id as usize].clone();
        let reader_handle =
            spawn_reader_thread(device_path, ready_rx, result_tx, args.queue_depth);

        // Spawn producer task to stream extents from ClickHouse
        let producer_client = client.clone();
        let producer_handle = tokio::spawn(async move {
            let mut cursor = db::query_extents(&producer_client, device_id)?;
            let mut pending: HashMap<i64, PendingXlmeta> = HashMap::new();

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
                        bucket: entry.bucket,
                        key: entry.key,
                        device_id,
                        ino,
                        extents: entry.extents,
                        file_size,
                    };

                    if ready_tx.send(ready).is_err() {
                        break; // Reader has terminated
                    }
                }
            }

            Ok::<_, anyhow::Error>(())
        });

        // Consume results and insert (runs concurrently with producer)
        let total_count = tmp_guard.count();
        let pb = ProgressBar::new(total_count);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}) {msg}",
                )
                .unwrap()
                .progress_chars("#>-"),
        );

        let mut objects_batch: Vec<ChObject> = Vec::with_capacity(args.batch_size);
        let mut objects_count = 0u64;
        let mut errors = 0u64;

        for result in result_rx {
            match result {
                ReaderResult::Parsed(obj) => {
                    objects_batch.push(obj);
                    objects_count += 1;
                    pb.inc(1);

                    if !args.dry_run && objects_batch.len() >= args.batch_size {
                        db::insert_objects(&client, &mut objects_batch).await?;
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
        if !args.dry_run && !objects_batch.is_empty() {
            db::insert_objects(&client, &mut objects_batch).await?;
        }

        pb.finish_with_message("done");

        // Wait for producer and reader to finish
        producer_handle
            .await
            .context("producer task panicked")?
            .context("producer failed")?;

        reader_handle
            .join()
            .map_err(|_| anyhow::anyhow!("reader thread panicked"))?
            .context("reader failed")?;

        // tmp_guard is dropped here, cleaning up the temp table

        total_objects += objects_count;
        total_errors += errors;

        info!(
            "Device {} done: {} objects, {} errors in {:.1}s",
            device_id,
            objects_count,
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
