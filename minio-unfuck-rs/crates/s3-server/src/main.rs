//! s3-server: read-only S3 server backed by ClickHouse + raw device reads
//!
//! Discovers cluster topology from format.json files,
//! batches GetObject requests for HDD throughput optimization.

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use hyper::service::Service;
use tracing::info;

mod backend;
mod batch;
mod cluster;
mod db;
mod reader;

#[derive(Parser, Debug)]
#[command(
    name = "s3-server",
    about = "Read-only S3 server for MinIO erasure-coded data"
)]
struct Args {
    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://localhost:8123")]
    clickhouse_url: String,

    /// ClickHouse database name
    #[arg(long, default_value = "minio")]
    clickhouse_db: String,

    /// Raw block device or XFS image file paths (whole disk expands to partitions)
    #[arg(long = "root")]
    roots: Vec<String>,

    /// Listen address
    #[arg(long, default_value = "0.0.0.0:9000")]
    addr: String,

    /// Batch timeout in milliseconds (time to wait for more requests)
    #[arg(long, default_value = "500")]
    batch_timeout: u64,

    /// Maximum batch size (requests per batch)
    #[arg(long, default_value = "100")]
    batch_size: usize,

    /// io_uring queue depth
    #[arg(long, default_value = "256")]
    queue_depth: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!("s3-server starting");
    info!("clickhouse: {}/{}", args.clickhouse_url, args.clickhouse_db);
    info!("roots: {:?}", args.roots);
    info!("listening on: {}", args.addr);
    info!(
        "batching: {}ms timeout, {} max size",
        args.batch_timeout, args.batch_size
    );

    // Resolve devices (expand whole disks to partitions)
    let devices = resolve_devices(&args.roots)?;
    info!("Resolved {} devices", devices.len());

    // Connect to ClickHouse
    let client = Arc::new(
        clickhouse::Client::default()
            .with_url(&args.clickhouse_url)
            .with_database(&args.clickhouse_db),
    );

    // Discover cluster topology from format.json files
    let cluster = Arc::new(cluster::discover_cluster(&client, &devices).await?);

    // Open device file handles
    let device_fds = open_devices(&devices)?;
    info!("Opened {} device handles", device_fds.len());

    // Clone device_fds for the batcher (need separate file handles)
    let device_fds_for_batcher = open_devices(&devices)?;

    // Create batch executor and batcher
    let executor = Arc::new(batch::BatchExecutor::new(
        client.clone(),
        cluster.clone(),
        device_fds_for_batcher,
    ));
    let batcher = Arc::new(batch::RequestBatcher::new(
        executor,
        args.batch_timeout,
        args.batch_size,
    ));

    // Create S3 backend
    let s3_backend = backend::MfuS3Backend::new(client, cluster, device_fds, batcher);

    // Build S3 service
    let shared = s3s::service::S3ServiceBuilder::new(s3_backend)
        .build()
        .into_shared();

    // Start HTTP server
    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    info!("S3 server listening on {}", args.addr);
    info!("Example:");
    info!("  aws --endpoint-url http://{} s3 ls", args.addr);
    info!("  aws --endpoint-url http://{} s3 ls s3://BUCKET/", args.addr);
    info!(
        "  aws --endpoint-url http://{} s3 cp s3://BUCKET/key ./",
        args.addr
    );

    loop {
        let (stream, _) = listener.accept().await?;
        let svc = shared.clone();
        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            let service = hyper::service::service_fn(move |req| svc.call(req));
            if let Err(e) = hyper_util::server::conn::auto::Builder::new(
                hyper_util::rt::TokioExecutor::new(),
            )
            .serve_connection(io, service)
            .await
            {
                tracing::error!("connection error: {}", e);
            }
        });
    }
}

/// Resolve device paths, expanding whole disks to partitions
fn resolve_devices(roots: &[String]) -> Result<Vec<String>> {
    let mut devices = Vec::new();

    for root in roots {
        // Check if it's a whole disk with partitions
        let partitions = find_partitions(root)?;
        if !partitions.is_empty() {
            info!("{} has {} partitions, expanding", root, partitions.len());
            devices.extend(partitions);
        } else {
            devices.push(root.clone());
        }
    }

    Ok(devices)
}

/// Find partitions for a device (e.g., /dev/sdg -> /dev/sdg1..sdg32)
fn find_partitions(device: &str) -> Result<Vec<String>> {
    use std::fs;
    use std::path::Path;

    let path = Path::new(device);
    let device_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    // Check /sys/block/{device}/ for partitions
    let sys_path = format!("/sys/block/{}", device_name);
    if !Path::new(&sys_path).exists() {
        return Ok(vec![]);
    }

    let mut partitions = Vec::new();
    for entry in fs::read_dir(&sys_path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with(device_name) && name_str != device_name {
            let part_path = format!("/dev/{}", name_str);
            if Path::new(&part_path).exists() {
                partitions.push(part_path);
            }
        }
    }

    // Sort partitions numerically
    partitions.sort_by(|a, b| {
        let a_num: u32 = a
            .trim_start_matches(&format!("/dev/{}", device_name))
            .parse()
            .unwrap_or(0);
        let b_num: u32 = b
            .trim_start_matches(&format!("/dev/{}", device_name))
            .parse()
            .unwrap_or(0);
        a_num.cmp(&b_num)
    });

    Ok(partitions)
}

/// Open device file handles for reading
fn open_devices(devices: &[String]) -> Result<Vec<std::fs::File>> {
    let mut fds = Vec::with_capacity(devices.len());
    for device in devices {
        let file = std::fs::File::open(device)
            .with_context(|| format!("open device {}", device))?;
        fds.push(file);
    }
    Ok(fds)
}
