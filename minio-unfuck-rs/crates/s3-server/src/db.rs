//! ClickHouse database queries for S3 server
//!
//! Handles object lookups, extent queries, and path resolution.

use anyhow::{Context, Result};
use clickhouse::Row;
use mfu_core::types::Extent;
use serde::Deserialize;

/// Stored object from ClickHouse objects table
#[derive(Debug, Clone, Row, Deserialize)]
pub struct StoredObject {
    pub bucket: String,
    pub key: String,
    pub size: i64,
    pub mod_time: i64,
    pub etag: String,
    pub content_type: String,
    pub compression: String,
    pub data_blocks: i32,
    pub parity_blocks: i32,
    pub block_size: i64,
    pub data_dir: String,
    #[serde(deserialize_with = "deserialize_array_i32")]
    pub distribution: Vec<i32>,
    pub parts_json: String,
    pub xlmeta_device_id: i32,
    pub xlmeta_ino: i64,
}

/// Extent row from file_extents table
#[derive(Debug, Clone, Row, Deserialize)]
pub struct ExtentRow {
    pub device_id: i32,
    pub ino: i64,
    pub logical_offset: i64,
    pub physical_offset: i64,
    pub length: i64,
}

/// Inode info from inodes table
#[derive(Debug, Clone, Row, Deserialize)]
pub struct InodeRow {
    pub device_id: i32,
    pub ino: i64,
    pub size: i64,
}

/// Directory entry from dirs table
#[derive(Debug, Clone, Row, Deserialize)]
pub struct DirEntry {
    pub device_id: i32,
    pub parent_ino: i64,
    pub child_ino: i64,
    pub name: String,
}

/// Format.json location
#[derive(Debug, Clone, Row, Deserialize)]
pub struct FormatJsonLocation {
    pub device_id: i32,
    pub format_ino: i64,
}

/// List all buckets (distinct bucket names from objects table)
pub async fn list_buckets(client: &clickhouse::Client) -> Result<Vec<String>> {
    let buckets: Vec<String> = client
        .query("SELECT DISTINCT bucket FROM objects ORDER BY bucket")
        .fetch_all()
        .await
        .context("list buckets")?;
    Ok(buckets)
}

/// Get a single object by bucket and key
pub async fn get_object(
    client: &clickhouse::Client,
    bucket: &str,
    key: &str,
) -> Result<Option<StoredObject>> {
    let mut cursor = client
        .query(
            "SELECT bucket, key, size, mod_time, etag, content_type, compression,
                    data_blocks, parity_blocks, block_size, data_dir,
                    distribution, parts_json, xlmeta_device_id, xlmeta_ino
             FROM objects
             WHERE bucket = ? AND key = ?
             LIMIT 1",
        )
        .bind(bucket)
        .bind(key)
        .fetch::<StoredObject>()
        .context("get object query")?;

    match cursor.next().await? {
        Some(obj) => Ok(Some(obj)),
        None => Ok(None),
    }
}

/// List objects with prefix and pagination
pub async fn list_objects(
    client: &clickhouse::Client,
    bucket: &str,
    prefix: &str,
    marker: &str,
    delimiter: &str,
    max_keys: i32,
) -> Result<Vec<StoredObject>> {
    // Simple listing - no delimiter handling for now
    let _ = delimiter; // TODO: handle delimiter for common prefixes

    let objects: Vec<StoredObject> = client
        .query(
            "SELECT bucket, key, size, mod_time, etag, content_type, compression,
                    data_blocks, parity_blocks, block_size, data_dir,
                    distribution, parts_json, xlmeta_device_id, xlmeta_ino
             FROM objects
             WHERE bucket = ? AND key >= ? AND startsWith(key, ?)
             ORDER BY key
             LIMIT ?",
        )
        .bind(bucket)
        .bind(marker)
        .bind(prefix)
        .bind(max_keys)
        .fetch_all()
        .await
        .context("list objects")?;

    Ok(objects)
}

/// Batch get objects by (bucket, key) pairs
pub async fn batch_get_objects(
    client: &clickhouse::Client,
    keys: &[(&str, &str)],
) -> Result<Vec<Option<StoredObject>>> {
    if keys.is_empty() {
        return Ok(vec![]);
    }

    // Build IN clause
    let in_clause: Vec<String> = keys
        .iter()
        .map(|(b, k)| format!("('{}', '{}')", escape_str(b), escape_str(k)))
        .collect();

    let query = format!(
        "SELECT bucket, key, size, mod_time, etag, content_type, compression,
                data_blocks, parity_blocks, block_size, data_dir,
                distribution, parts_json, xlmeta_device_id, xlmeta_ino
         FROM objects
         WHERE (bucket, key) IN ({})",
        in_clause.join(", ")
    );

    let objects: Vec<StoredObject> = client
        .query(&query)
        .fetch_all()
        .await
        .context("batch get objects")?;

    // Map results back to input order
    let mut result: Vec<Option<StoredObject>> = vec![None; keys.len()];
    for obj in objects {
        for (i, (b, k)) in keys.iter().enumerate() {
            if obj.bucket == *b && obj.key == *k {
                result[i] = Some(obj.clone());
                break;
            }
        }
    }

    Ok(result)
}

/// Get file extents for an inode
pub async fn get_file_extents(
    client: &clickhouse::Client,
    device_id: i32,
    ino: i64,
) -> Result<Vec<Extent>> {
    let rows: Vec<ExtentRow> = client
        .query(
            "SELECT device_id, ino, logical_offset, physical_offset, length
             FROM file_extents
             WHERE device_id = ? AND ino = ?
             ORDER BY logical_offset",
        )
        .bind(device_id)
        .bind(ino)
        .fetch_all()
        .await
        .context("get file extents")?;

    Ok(rows
        .into_iter()
        .map(|r| Extent {
            logical_offset: r.logical_offset,
            physical_offset: r.physical_offset,
            length: r.length,
        })
        .collect())
}

/// Batch get extents for multiple (device_id, ino) pairs
pub async fn batch_get_extents(
    client: &clickhouse::Client,
    keys: &[(i32, i64)],
) -> Result<Vec<(i32, i64, Vec<Extent>)>> {
    if keys.is_empty() {
        return Ok(vec![]);
    }

    let in_clause: Vec<String> = keys
        .iter()
        .map(|(d, i)| format!("({}, {})", d, i))
        .collect();

    let query = format!(
        "SELECT device_id, ino, logical_offset, physical_offset, length
         FROM file_extents
         WHERE (device_id, ino) IN ({})
         ORDER BY device_id, ino, logical_offset",
        in_clause.join(", ")
    );

    let rows: Vec<ExtentRow> = client.query(&query).fetch_all().await.context("batch get extents")?;

    // Group by (device_id, ino)
    let mut result: Vec<(i32, i64, Vec<Extent>)> = Vec::new();
    let mut current: Option<(i32, i64, Vec<Extent>)> = None;

    for row in rows {
        let extent = Extent {
            logical_offset: row.logical_offset,
            physical_offset: row.physical_offset,
            length: row.length,
        };

        match &mut current {
            Some((d, i, extents)) if *d == row.device_id && *i == row.ino => {
                extents.push(extent);
            }
            _ => {
                if let Some(prev) = current.take() {
                    result.push(prev);
                }
                current = Some((row.device_id, row.ino, vec![extent]));
            }
        }
    }

    if let Some(prev) = current {
        result.push(prev);
    }

    Ok(result)
}

/// Get inode size
pub async fn get_inode_size(
    client: &clickhouse::Client,
    device_id: i32,
    ino: i64,
) -> Result<Option<i64>> {
    let rows: Vec<InodeRow> = client
        .query(
            "SELECT device_id, ino, size
             FROM inodes
             WHERE device_id = ? AND ino = ?
             LIMIT 1",
        )
        .bind(device_id)
        .bind(ino)
        .fetch_all()
        .await
        .context("get inode size")?;

    Ok(rows.first().map(|r| r.size))
}

/// Resolve a directory entry by parent inode and name
pub async fn resolve_dir_entry(
    client: &clickhouse::Client,
    device_id: i32,
    parent_ino: i64,
    name: &str,
) -> Result<Option<i64>> {
    let rows: Vec<DirEntry> = client
        .query(
            "SELECT device_id, parent_ino, child_ino, name
             FROM dirs
             WHERE device_id = ? AND parent_ino = ? AND name = ?
             LIMIT 1",
        )
        .bind(device_id)
        .bind(parent_ino)
        .bind(name)
        .fetch_all()
        .await
        .context("resolve dir entry")?;

    Ok(rows.first().map(|r| r.child_ino))
}

/// Look up a directory by name directly (for UUID directories)
pub async fn lookup_dir_by_name(
    client: &clickhouse::Client,
    device_id: i32,
    name: &str,
) -> Result<Option<i64>> {
    let rows: Vec<DirEntry> = client
        .query(
            "SELECT device_id, parent_ino, child_ino, name
             FROM dirs
             WHERE device_id = ? AND name = ?
             LIMIT 1",
        )
        .bind(device_id)
        .bind(name)
        .fetch_all()
        .await
        .context("lookup dir by name")?;

    Ok(rows.first().map(|r| r.child_ino))
}

/// Look up part file inode: find UUID dir, then part.N under it
pub async fn lookup_part_inode(
    client: &clickhouse::Client,
    device_id: i32,
    data_dir_uuid: &str,
    part_name: &str,
) -> Result<Option<i64>> {
    // Step 1: Find UUID directory by name
    let uuid_ino = match lookup_dir_by_name(client, device_id, data_dir_uuid).await? {
        Some(ino) => ino,
        None => return Ok(None),
    };

    // Step 2: Find part file under UUID directory
    resolve_dir_entry(client, device_id, uuid_ino, part_name).await
}

/// Find format.json file locations on all devices
pub async fn find_format_json_inodes(client: &clickhouse::Client) -> Result<Vec<FormatJsonLocation>> {
    let query = r#"
        WITH minio_sys AS (
            SELECT device_id, child_ino
            FROM dirs
            WHERE name = '.minio.sys'
        )
        SELECT d.device_id, d.child_ino as format_ino
        FROM dirs d
        JOIN minio_sys m ON d.device_id = m.device_id AND d.parent_ino = m.child_ino
        WHERE d.name = 'format.json'
    "#;

    let locations: Vec<FormatJsonLocation> = client
        .query(query)
        .fetch_all()
        .await
        .context("find format.json inodes")?;

    Ok(locations)
}

// Helper to escape strings for SQL
fn escape_str(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

// Custom deserializer for Array(Int32) which comes as a string
fn deserialize_array_i32<'de, D>(deserializer: D) -> Result<Vec<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v: Vec<i32> = Vec::deserialize(deserializer)?;
    Ok(v)
}
