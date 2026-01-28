//! DuckDB schema, bulk insert, and query operations
//!
//! All metadata from XFS scan + parsed xl.meta goes into DuckDB.

use anyhow::{Context, Result};
use duckdb::{params, Appender, Connection};

use crate::types::{Extent, ObjectMeta};

const SCHEMA: &str = r#"
-- All inodes from XFS scan (skip "." and "..")
CREATE TABLE IF NOT EXISTS inodes (
    device_id   INTEGER NOT NULL,
    ino         BIGINT NOT NULL,
    mode        INTEGER NOT NULL,
    size        BIGINT NOT NULL,
    nlink       INTEGER NOT NULL,
    uid         INTEGER NOT NULL,
    gid         INTEGER NOT NULL,
    mtime_sec   BIGINT NOT NULL,
    nblocks     BIGINT NOT NULL,
    ag_number   INTEGER NOT NULL
);

-- Directory entries from XFS scan
CREATE TABLE IF NOT EXISTS dirs (
    device_id   INTEGER NOT NULL,
    parent_ino  BIGINT NOT NULL,
    child_ino   BIGINT NOT NULL,
    name        VARCHAR NOT NULL,
    file_type   INTEGER NOT NULL
);

-- Physical extents for every regular file
CREATE TABLE IF NOT EXISTS file_extents (
    device_id        INTEGER NOT NULL,
    ino              BIGINT NOT NULL,
    logical_offset   BIGINT NOT NULL,
    physical_offset  BIGINT NOT NULL,
    length           BIGINT NOT NULL
);

-- Parsed MinIO objects (from xl.meta files)
CREATE TABLE IF NOT EXISTS objects (
    bucket         VARCHAR NOT NULL,
    key            VARCHAR NOT NULL,
    size           BIGINT NOT NULL,
    mod_time       BIGINT NOT NULL,
    etag           VARCHAR NOT NULL,
    content_type   VARCHAR,
    user_metadata  VARCHAR,
    data_dir       VARCHAR NOT NULL,
    data_blocks    INTEGER NOT NULL,
    parity_blocks  INTEGER NOT NULL,
    block_size     BIGINT NOT NULL,
    distribution   VARCHAR NOT NULL,
    parts          VARCHAR,
    pool_index     INTEGER NOT NULL DEFAULT 0,
    set_index      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, key)
);

-- Cluster topology from format.json
CREATE TABLE IF NOT EXISTS cluster_disks (
    pool_id     VARCHAR NOT NULL,
    pool_index  INTEGER NOT NULL,
    set_index   INTEGER NOT NULL,
    disk_index  INTEGER NOT NULL,
    disk_uuid   VARCHAR NOT NULL,
    device_id   INTEGER,
    PRIMARY KEY (pool_id, set_index, disk_index)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_extents_device_ino ON file_extents(device_id, ino);
CREATE INDEX IF NOT EXISTS idx_extents_phys ON file_extents(device_id, physical_offset);
CREATE INDEX IF NOT EXISTS idx_dirs_parent ON dirs(device_id, parent_ino);
CREATE INDEX IF NOT EXISTS idx_dirs_name ON dirs(name);
CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
"#;

/// DuckDB metadata store
pub struct MetadataDb {
    conn: Connection,
    path: String,
}

impl MetadataDb {
    /// Open (or create) a DuckDB database and ensure the schema exists.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path).context("open duckdb")?;
        conn.execute_batch(SCHEMA).context("create schema")?;
        Ok(Self { conn, path: path.to_string() })
    }

    /// Open an in-memory DuckDB for testing.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().context("open in-memory duckdb")?;
        conn.execute_batch(SCHEMA).context("create schema")?;
        Ok(Self { conn, path: ":memory:".to_string() })
    }

    /// Get the database file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Configure DuckDB for maximum bulk-load throughput.
    pub fn configure_bulk_load(&self) -> Result<()> {
        self.conn.execute_batch(
            "SET memory_limit = '8GB';
             SET threads = 4;
             SET checkpoint_threshold = '2GB';
             SET wal_autocheckpoint = '2GB';",
        ).context("configure bulk load")?;
        Ok(())
    }

    /// Get a reference to the underlying connection
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Insert a parsed object from xl.meta (uses regular INSERT for OR REPLACE semantics)
    pub fn insert_object(&self, meta: &ObjectMeta) -> Result<()> {
        let dist_json = serde_json::to_string(&meta.distribution.iter().map(|&v| v as i32).collect::<Vec<_>>())?;
        let parts_json = serde_json::to_string(&meta.parts.iter().map(|p| {
            serde_json::json!({
                "number": p.number,
                "size": p.size,
                "actual_size": p.actual_size,
            })
        }).collect::<Vec<_>>())?;

        let user_meta_json = if meta.user_meta.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&meta.user_meta)?)
        };

        self.conn.execute(
            "INSERT OR REPLACE INTO objects (bucket, key, size, mod_time, etag, content_type, user_metadata, data_dir, data_blocks, parity_blocks, block_size, distribution, parts, pool_index, set_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                meta.bucket,
                meta.key,
                meta.size,
                meta.mod_time,
                meta.etag,
                if meta.content_type.is_empty() { None } else { Some(&meta.content_type) },
                user_meta_json,
                meta.data_dir_string(),
                meta.data_blocks as i32,
                meta.parity_blocks as i32,
                meta.block_size,
                dist_json,
                parts_json,
                meta.pool_index,
                meta.set_index,
            ],
        )?;
        Ok(())
    }

    /// Insert a directory entry (convenience method for tests and small inserts)
    pub fn insert_dir(
        &self,
        device_id: i32,
        parent_ino: i64,
        child_ino: i64,
        name: &str,
        file_type: i32,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO dirs VALUES (?, ?, ?, ?, ?)",
            params![device_id, parent_ino, child_ino, name, file_type],
        )?;
        Ok(())
    }

    /// Insert a file extent (convenience method for tests and small inserts)
    pub fn insert_file_extent(
        &self,
        device_id: i32,
        ino: i64,
        logical_offset: i64,
        physical_offset: i64,
        length: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO file_extents VALUES (?, ?, ?, ?, ?)",
            params![device_id, ino, logical_offset, physical_offset, length],
        )?;
        Ok(())
    }

    /// Insert an inode (convenience method for tests and small inserts)
    pub fn insert_inode(
        &self,
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
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO inodes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![device_id, ino, mode, size, nlink, uid, gid, mtime_sec, nblocks, ag_number],
        )?;
        Ok(())
    }

    /// Insert a cluster disk entry
    pub fn insert_cluster_disk(
        &self,
        pool_id: &str,
        pool_index: i32,
        set_index: i32,
        disk_index: i32,
        disk_uuid: &str,
        device_id: Option<i32>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO cluster_disks VALUES (?, ?, ?, ?, ?, ?)",
            params![pool_id, pool_index, set_index, disk_index, disk_uuid, device_id],
        )?;
        Ok(())
    }

    // --- Query operations ---

    /// Resolve a path to an inode number on a specific device.
    /// Walks the dirs table from root (ino assumed to be known or resolved step by step).
    pub fn resolve_path(&self, device_id: i32, path: &str) -> Result<Option<i64>> {
        // Start from root inode (typically ino=128 for XFS, but we find root by parent_ino referencing itself
        // or by looking for entries with no parent. For simplicity, resolve step by step.)

        let components: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if components.is_empty() {
            // Root directory — find the root inode
            let mut stmt = self.conn.prepare(
                "SELECT DISTINCT parent_ino FROM dirs WHERE device_id = ? LIMIT 1",
            )?;
            let ino: Option<i64> = stmt
                .query_row(params![device_id], |row| row.get(0))
                .ok();
            return Ok(ino);
        }

        // Find root inode: the parent_ino that is its own parent, or just use the first parent
        // In XFS, root inode is typically 128. We'll find it by looking at parent entries.
        let root_ino: i64 = {
            // Try to find root: an inode that is a parent but never a child with a different parent
            // Simple approach: find the parent of a top-level entry
            let mut stmt = self.conn.prepare(
                "SELECT parent_ino FROM dirs WHERE device_id = ? AND name = ? LIMIT 1",
            )?;
            match stmt.query_row(params![device_id, components[0]], |row| row.get::<_, i64>(0)) {
                Ok(ino) => ino,
                Err(_) => return Ok(None),
            }
        };

        let mut current_ino = root_ino;

        for component in components.iter() {
            let mut stmt = self.conn.prepare(
                "SELECT child_ino FROM dirs WHERE device_id = ? AND parent_ino = ? AND name = ?",
            )?;
            match stmt.query_row(params![device_id, current_ino, *component], |row| {
                row.get::<_, i64>(0)
            }) {
                Ok(ino) => current_ino = ino,
                Err(_) => return Ok(None),
            }
        }

        Ok(Some(current_ino))
    }

    /// Get file extents for an inode
    pub fn get_extents(&self, device_id: i32, ino: i64) -> Result<Vec<Extent>> {
        let mut stmt = self.conn.prepare(
            "SELECT logical_offset, physical_offset, length FROM file_extents WHERE device_id = ? AND ino = ? ORDER BY logical_offset",
        )?;
        let extents = stmt
            .query_map(params![device_id, ino], |row| {
                Ok(Extent {
                    logical_offset: row.get(0)?,
                    physical_offset: row.get(1)?,
                    length: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(extents)
    }

    /// Get inode size
    pub fn get_inode_size(&self, device_id: i32, ino: i64) -> Result<Option<i64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT size FROM inodes WHERE device_id = ? AND ino = ?")?;
        match stmt.query_row(params![device_id, ino], |row| row.get::<_, i64>(0)) {
            Ok(size) => Ok(Some(size)),
            Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// List directory entries
    pub fn list_dir(&self, device_id: i32, parent_ino: i64) -> Result<Vec<DirEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT child_ino, name, file_type FROM dirs WHERE device_id = ? AND parent_ino = ? ORDER BY name",
        )?;
        let entries = stmt
            .query_map(params![device_id, parent_ino], |row| {
                Ok(DirEntry {
                    ino: row.get(0)?,
                    name: row.get(1)?,
                    file_type: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(entries)
    }

    /// Find all xl.meta inodes across all devices
    pub fn find_xlmeta_inodes(&self) -> Result<Vec<(i32, i64)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT device_id, child_ino FROM dirs WHERE name = 'xl.meta'")?;
        let results = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(results)
    }

    /// List buckets from objects table
    pub fn list_buckets(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT bucket FROM objects ORDER BY bucket")?;
        let buckets = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(buckets)
    }

    /// Get object metadata
    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Option<StoredObject>> {
        let mut stmt = self.conn.prepare(
            "SELECT bucket, key, size, mod_time, etag, content_type, user_metadata, data_dir, data_blocks, parity_blocks, block_size, distribution, parts, pool_index, set_index FROM objects WHERE bucket = ? AND key = ?",
        )?;

        match stmt.query_row(params![bucket, key], |row| {
            Ok(StoredObject {
                bucket: row.get(0)?,
                key: row.get(1)?,
                size: row.get(2)?,
                mod_time: row.get(3)?,
                etag: row.get(4)?,
                content_type: row.get(5)?,
                user_metadata: row.get(6)?,
                data_dir: row.get(7)?,
                data_blocks: row.get(8)?,
                parity_blocks: row.get(9)?,
                block_size: row.get(10)?,
                distribution: {
                    let s: String = row.get(11)?;
                    serde_json::from_str(&s).unwrap_or_default()
                },
                parts: row.get(12)?,
                pool_index: row.get(13)?,
                set_index: row.get(14)?,
            })
        }) {
            Ok(obj) => Ok(Some(obj)),
            Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// List objects with prefix/delimiter support
    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        _delimiter: &str,
        max_keys: i32,
    ) -> Result<Vec<StoredObject>> {
        let mut query = String::from(
            "SELECT bucket, key, size, mod_time, etag, content_type, user_metadata, data_dir, data_blocks, parity_blocks, block_size, distribution, parts, pool_index, set_index FROM objects WHERE bucket = ?",
        );
        let mut param_values: Vec<Box<dyn duckdb::ToSql>> = vec![Box::new(bucket.to_string())];

        if !prefix.is_empty() {
            query.push_str(" AND key LIKE ?");
            param_values.push(Box::new(format!("{}%", prefix)));
        }
        if !marker.is_empty() {
            query.push_str(" AND key > ?");
            param_values.push(Box::new(marker.to_string()));
        }
        query.push_str(" ORDER BY key LIMIT ?");
        param_values.push(Box::new(max_keys));

        let params_ref: Vec<&dyn duckdb::ToSql> = param_values.iter().map(|b| b.as_ref()).collect();

        let mut stmt = self.conn.prepare(&query)?;
        let objects = stmt
            .query_map(params_ref.as_slice(), |row| {
                Ok(StoredObject {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    size: row.get(2)?,
                    mod_time: row.get(3)?,
                    etag: row.get(4)?,
                    content_type: row.get(5)?,
                    user_metadata: row.get(6)?,
                    data_dir: row.get(7)?,
                    data_blocks: row.get(8)?,
                    parity_blocks: row.get(9)?,
                    block_size: row.get(10)?,
                    distribution: {
                    let s: String = row.get(11)?;
                    serde_json::from_str(&s).unwrap_or_default()
                },
                    parts: row.get(12)?,
                    pool_index: row.get(13)?,
                    set_index: row.get(14)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(objects)
    }

}

/// High-throughput bulk inserter using DuckDB's Appender API.
/// Bypasses SQL parsing entirely — orders of magnitude faster than individual INSERTs.
/// Owns its own Connection so it can be Send + moved to a writer thread.
pub struct BulkInserter {
    conn: Connection,
}

// Safety: BulkInserter owns its Connection exclusively and is only used from one thread.
unsafe impl Send for BulkInserter {}

impl BulkInserter {
    /// Open a new connection to the same database for bulk writing.
    pub fn open(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path).context("open bulk inserter connection")?;
        Ok(Self { conn })
    }

    /// Run a bulk write session. Creates appenders, calls the provided function,
    /// then flushes and drops the appenders.
    pub fn write_session<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&mut WriteSession<'_>) -> Result<()>,
    {
        let inodes = self.conn.appender("inodes").context("appender for inodes")?;
        let dirs = self.conn.appender("dirs").context("appender for dirs")?;
        let extents = self.conn.appender("file_extents").context("appender for file_extents")?;
        let mut session = WriteSession { inodes, dirs, extents };
        f(&mut session)?;
        session.flush()?;
        Ok(())
    }
}

/// Active write session with open appenders. Cannot outlive the BulkInserter.
pub struct WriteSession<'conn> {
    inodes: Appender<'conn>,
    dirs: Appender<'conn>,
    extents: Appender<'conn>,
}

impl<'conn> WriteSession<'conn> {
    pub fn append_inode(
        &mut self,
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
    ) -> Result<()> {
        self.inodes.append_row(params![
            device_id, ino, mode, size, nlink, uid, gid, mtime_sec, nblocks, ag_number
        ])?;
        Ok(())
    }

    pub fn append_dir(
        &mut self,
        device_id: i32,
        parent_ino: i64,
        child_ino: i64,
        name: &str,
        file_type: i32,
    ) -> Result<()> {
        self.dirs.append_row(params![
            device_id, parent_ino, child_ino, name, file_type
        ])?;
        Ok(())
    }

    pub fn append_extent(
        &mut self,
        device_id: i32,
        ino: i64,
        logical_offset: i64,
        physical_offset: i64,
        length: i64,
    ) -> Result<()> {
        self.extents.append_row(params![
            device_id, ino, logical_offset, physical_offset, length
        ])?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.inodes.flush()?;
        self.dirs.flush()?;
        self.extents.flush()?;
        Ok(())
    }
}

/// A directory entry from the dirs table
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub ino: i64,
    pub name: String,
    pub file_type: i32,
}

/// An object stored in the objects table
#[derive(Debug, Clone)]
pub struct StoredObject {
    pub bucket: String,
    pub key: String,
    pub size: i64,
    pub mod_time: i64,
    pub etag: String,
    pub content_type: Option<String>,
    pub user_metadata: Option<String>,
    pub data_dir: String,
    pub data_blocks: i32,
    pub parity_blocks: i32,
    pub block_size: i64,
    pub distribution: Vec<i32>,
    pub parts: Option<String>,
    pub pool_index: i32,
    pub set_index: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let db = MetadataDb::open_in_memory().unwrap();
        // Verify tables exist
        let count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'inodes'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_insert_and_query_dir() {
        let db = MetadataDb::open_in_memory().unwrap();
        db.insert_dir(0, 128, 256, "testfile", 1).unwrap();
        let entries = db.list_dir(0, 128).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "testfile");
        assert_eq!(entries[0].ino, 256);
    }

    #[test]
    fn test_resolve_path() {
        let db = MetadataDb::open_in_memory().unwrap();
        // Create a directory structure: root(128) -> .minio.sys(200) -> format.json(300)
        db.insert_dir(0, 128, 200, ".minio.sys", 2).unwrap();
        db.insert_dir(0, 200, 300, "format.json", 1).unwrap();
        // Also add a top-level entry so we can find root
        db.insert_dir(0, 128, 200, ".minio.sys", 2).unwrap();

        let ino = db.resolve_path(0, "/.minio.sys/format.json").unwrap();
        assert_eq!(ino, Some(300));
    }

    #[test]
    fn test_insert_and_query_extents() {
        let db = MetadataDb::open_in_memory().unwrap();
        db.insert_file_extent(0, 300, 0, 1048576, 4096).unwrap();
        db.insert_file_extent(0, 300, 4096, 2097152, 4096).unwrap();

        let extents = db.get_extents(0, 300).unwrap();
        assert_eq!(extents.len(), 2);
        assert_eq!(extents[0].physical_offset, 1048576);
        assert_eq!(extents[1].logical_offset, 4096);
    }
}
