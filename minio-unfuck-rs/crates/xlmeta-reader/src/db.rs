//! Database operations for xlmeta-reader
//!
//! Handles all ClickHouse queries and inserts.

use std::sync::Arc;

use anyhow::{Context, Result};
use clickhouse::query::RowCursor;
use tokio::runtime::Handle;

use crate::types::{ChObject, ExtentRow};

/// RAII guard for the tmp_xlmetas table.
/// Automatically drops the table when this guard is dropped.
pub struct TmpXlmetasGuard {
    client: Arc<clickhouse::Client>,
    count: u64,
    runtime_handle: Handle,
}

impl TmpXlmetasGuard {
    /// Returns the number of objects in the temp table
    pub fn count(&self) -> u64 {
        self.count
    }
}

impl Drop for TmpXlmetasGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        // Use block_on to ensure the drop completes
        let _ = self.runtime_handle.block_on(async {
            let _ = client
                .query("DROP TABLE IF EXISTS tmp_xlmetas")
                .execute()
                .await;
            let _ = client
                .query("DROP TABLE IF EXISTS tmp_xlmetas_all")
                .execute()
                .await;
        });
    }
}

/// Creates the objects table if it doesn't exist
pub async fn create_objects_table(client: &clickhouse::Client) -> Result<()> {
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

/// Creates a temporary table with xlmeta locations for a specific device.
/// Skips objects that already exist in the objects table.
/// Returns a guard that will automatically drop the table when it goes out of scope.
///
/// This is done in two steps to avoid memory issues:
/// 1. Create tmp_xlmetas_all with DISTINCT ON (no filtering)
/// 2. Create tmp_xlmetas by filtering out existing objects
pub async fn create_tmp_xlmetas(
    client: Arc<clickhouse::Client>,
    device_id: i32,
    limit: Option<usize>,
) -> Result<TmpXlmetasGuard> {
    let limit_clause = limit.map(|l| format!("LIMIT {}", l)).unwrap_or_default();

    // Clean up any leftover tables
    client
        .query("DROP TABLE IF EXISTS tmp_xlmetas")
        .execute()
        .await
        .context("drop tmp_xlmetas")?;

    client
        .query("DROP TABLE IF EXISTS tmp_xlmetas_all")
        .execute()
        .await
        .context("drop tmp_xlmetas_all")?;

    // Step 1: Create temp table with distinct xlmeta locations (no filtering yet)
    let create_all = format!(
        r#"
        CREATE TABLE tmp_xlmetas_all ENGINE = Memory AS
        SELECT DISTINCT ON (bucket, key)
            toValidUTF8(bucket) as bucket,
            toValidUTF8(key) as key,
            device_id,
            xlmeta_ino,
            data_dir_ino
        FROM s3_xlmeta_locations s
        WHERE s.device_id = {}
        {}
        "#,
        device_id, limit_clause
    );

    client
        .query(&create_all)
        .execute()
        .await
        .context("create tmp_xlmetas_all")?;

    // Step 2: Create final table excluding objects that already exist
    let create_filtered = r#"
        CREATE TABLE tmp_xlmetas ENGINE = Memory AS
        SELECT t.*
        FROM tmp_xlmetas_all t
        WHERE (t.bucket, t.key) NOT IN (
            SELECT bucket, key FROM objects
        )
    "#;

    client
        .query(create_filtered)
        .execute()
        .await
        .context("create tmp_xlmetas filtered")?;

    // Clean up intermediate table
    client
        .query("DROP TABLE IF EXISTS tmp_xlmetas_all")
        .execute()
        .await
        .context("drop tmp_xlmetas_all")?;

    let count: u64 = client
        .query("SELECT count() FROM tmp_xlmetas")
        .fetch_one()
        .await
        .context("count tmp_xlmetas")?;

    Ok(TmpXlmetasGuard {
        client,
        count,
        runtime_handle: Handle::current(),
    })
}

/// Returns a cursor for streaming extents joined with tmp_xlmetas
pub fn query_extents(
    client: &clickhouse::Client,
    device_id: i32,
) -> Result<RowCursor<ExtentRow>> {
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

    client
        .query(&query)
        .fetch::<ExtentRow>()
        .context("fetch extents")
}

/// Inserts a batch of objects into ClickHouse
pub async fn insert_objects(client: &clickhouse::Client, objects: &mut Vec<ChObject>) -> Result<()> {
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

#[cfg(test)]
mod tests {

    // Integration tests would require a ClickHouse instance
    // Unit tests for query building could be added here

    #[test]
    fn test_limit_clause_formatting() {
        let limit: Option<usize> = Some(100);
        let clause = limit.map(|l| format!("LIMIT {}", l)).unwrap_or_default();
        assert_eq!(clause, "LIMIT 100");

        let no_limit: Option<usize> = None;
        let clause = no_limit.map(|l| format!("LIMIT {}", l)).unwrap_or_default();
        assert_eq!(clause, "");
    }
}
