//! dbcat: quick verification utility
//!
//! Connects to a DuckDB populated by meta-reader, resolves paths via the dirs table,
//! reads file data via extents + raw device reads, and outputs to stdout.

use std::io::Write;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

use mfu_core::db::MetadataDb;
use mfu_core::raw_io::{DeviceReader, PreadDeviceReader};

#[derive(Parser, Debug)]
#[command(
    name = "dbcat",
    about = "Browse and read files from DuckDB metadata + raw devices"
)]
struct Args {
    /// Path to DuckDB database
    #[arg(long)]
    db: String,

    /// Raw block device or XFS image file paths (in device_id order)
    #[arg(long = "root")]
    roots: Vec<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List directory contents
    Ls {
        /// Path to list (e.g. "/" or "/.minio.sys/")
        path: String,
        /// Device index (default 0)
        #[arg(long, default_value = "0")]
        device: i32,
    },
    /// Read file contents to stdout
    Cat {
        /// Path to read (e.g. "/.minio.sys/format.json")
        path: String,
        /// Device index (default 0)
        #[arg(long, default_value = "0")]
        device: i32,
    },
    /// Show inode metadata
    Stat {
        /// Path to stat
        path: String,
        /// Device index (default 0)
        #[arg(long, default_value = "0")]
        device: i32,
    },
    /// Recursive directory listing
    Tree {
        /// Path to list recursively
        path: String,
        /// Maximum depth
        #[arg(long, default_value = "3")]
        depth: usize,
        /// Device index (default 0)
        #[arg(long, default_value = "0")]
        device: i32,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let db = MetadataDb::open(&args.db).context("open database")?;
    let reader = if !args.roots.is_empty() {
        Some(PreadDeviceReader::open(&args.roots).context("open devices")?)
    } else {
        None
    };

    match &args.command {
        Command::Ls { path, device } => cmd_ls(&db, *device, path),
        Command::Cat { path, device } => {
            let reader = reader.as_ref().context("--root required for cat")?;
            cmd_cat(&db, reader, *device, path)
        }
        Command::Stat { path, device } => cmd_stat(&db, *device, path),
        Command::Tree {
            path,
            depth,
            device,
        } => cmd_tree(&db, *device, path, *depth),
    }
}

fn cmd_ls(db: &MetadataDb, device_id: i32, path: &str) -> Result<()> {
    let ino = db
        .resolve_path(device_id, path)?
        .with_context(|| format!("path not found: {}", path))?;

    let entries = db.list_dir(device_id, ino)?;

    for entry in &entries {
        let type_char = match entry.file_type {
            2 => "d", // directory
            1 => "f", // regular file
            _ => "?",
        };
        println!("{} {}", type_char, entry.name);
    }

    if entries.is_empty() {
        println!("(empty directory)");
    }

    Ok(())
}

fn cmd_cat(
    db: &MetadataDb,
    reader: &PreadDeviceReader,
    device_id: i32,
    path: &str,
) -> Result<()> {
    let ino = db
        .resolve_path(device_id, path)?
        .with_context(|| format!("path not found: {}", path))?;

    let extents = db.get_extents(device_id, ino)?;
    if extents.is_empty() {
        bail!("no extents for inode {} (may be inline or empty)", ino);
    }

    let size = db
        .get_inode_size(device_id, ino)?
        .with_context(|| format!("inode {} not found", ino))?;

    let data = reader.read_file(device_id as usize, &extents, size as u64)?;

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    out.write_all(&data)?;
    out.flush()?;

    Ok(())
}

fn cmd_stat(db: &MetadataDb, device_id: i32, path: &str) -> Result<()> {
    let ino = db
        .resolve_path(device_id, path)?
        .with_context(|| format!("path not found: {}", path))?;

    let size = db.get_inode_size(device_id, ino)?;
    let extents = db.get_extents(device_id, ino)?;

    println!("Path:    {}", path);
    println!("Inode:   {}", ino);
    println!("Size:    {} bytes", size.unwrap_or(0));
    println!("Extents: {}", extents.len());

    for (i, ext) in extents.iter().enumerate() {
        println!(
            "  [{:3}] logical={} physical={} length={}",
            i, ext.logical_offset, ext.physical_offset, ext.length
        );
    }

    Ok(())
}

fn cmd_tree(db: &MetadataDb, device_id: i32, path: &str, max_depth: usize) -> Result<()> {
    let ino = db
        .resolve_path(device_id, path)?
        .with_context(|| format!("path not found: {}", path))?;

    println!("{}", path);
    tree_recursive(db, device_id, ino, "", 0, max_depth)?;
    Ok(())
}

fn tree_recursive(
    db: &MetadataDb,
    device_id: i32,
    parent_ino: i64,
    prefix: &str,
    depth: usize,
    max_depth: usize,
) -> Result<()> {
    if depth >= max_depth {
        return Ok(());
    }

    let entries = db.list_dir(device_id, parent_ino)?;
    let len = entries.len();

    for (i, entry) in entries.iter().enumerate() {
        let is_last = i == len - 1;
        let connector = if is_last { "└── " } else { "├── " };
        let child_prefix = if is_last { "    " } else { "│   " };

        let type_indicator = if entry.file_type == 2 { "/" } else { "" };
        println!(
            "{}{}{}{}",
            prefix, connector, entry.name, type_indicator
        );

        if entry.file_type == 2 {
            let new_prefix = format!("{}{}", prefix, child_prefix);
            tree_recursive(db, device_id, entry.ino, &new_prefix, depth + 1, max_depth)?;
        }
    }

    Ok(())
}
