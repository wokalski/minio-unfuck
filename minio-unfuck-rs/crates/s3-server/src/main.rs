//! s3-server: read-only S3 server backed by DuckDB + raw device reads

use anyhow::{Context, Result};
use clap::Parser;
use hyper::service::Service;

mod backend;

#[derive(Parser, Debug)]
#[command(
    name = "s3-server",
    about = "Read-only S3 server for MinIO erasure-coded data"
)]
struct Args {
    /// Path to DuckDB database (from meta-reader)
    #[arg(long)]
    db: String,

    /// Raw block device or XFS image file paths (in device_id order)
    #[arg(long = "root")]
    roots: Vec<String>,

    /// Listen address
    #[arg(long, default_value = "0.0.0.0:9000")]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    tracing::info!("s3-server starting");
    tracing::info!("db: {}", args.db);
    tracing::info!("devices: {:?}", args.roots);
    tracing::info!("listening on: {}", args.addr);

    // Open database and device readers
    let db = mfu_core::db::MetadataDb::open(&args.db).context("open database")?;
    let reader = mfu_core::raw_io::PreadDeviceReader::open(&args.roots)
        .context("open device readers")?;

    let s3_backend = backend::MfuS3Backend::new(db, reader);

    // Build S3 service (SharedS3Service is Clone + implements hyper::Service)
    let shared = s3s::service::S3ServiceBuilder::new(s3_backend)
        .build()
        .into_shared();

    // Create hyper service
    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    tracing::info!("S3 server listening on {}", args.addr);
    tracing::info!("Example:");
    tracing::info!("  aws --endpoint-url http://localhost:9000 s3 ls");
    tracing::info!("  aws --endpoint-url http://localhost:9000 s3 ls s3://BUCKET/");
    tracing::info!("  aws --endpoint-url http://localhost:9000 s3 cp s3://BUCKET/key ./");

    loop {
        let (stream, _) = listener.accept().await?;
        let svc = shared.clone();
        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            let service = hyper::service::service_fn(move |req| {
                svc.call(req)
            });
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
