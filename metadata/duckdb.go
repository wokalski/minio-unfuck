package metadata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/marcboeker/go-duckdb"
)

// DuckStore provides a DuckDB-based metadata store optimized for bulk inserts
type DuckStore struct {
	db        *sql.DB
	connector *duckdb.Connector
}

const duckDBSchema = `
-- Complete XFS filesystem snapshot (every file/dir)
CREATE TABLE IF NOT EXISTS xfs_entries (
    partition_index INTEGER NOT NULL,
    path VARCHAR NOT NULL,
    inode_number BIGINT NOT NULL,
    is_dir BOOLEAN NOT NULL
);

-- Precomputed object keys (for fast listing)
CREATE TABLE IF NOT EXISTS object_keys (
    bucket VARCHAR NOT NULL,
    key VARCHAR NOT NULL
);

-- Processed objects (background xl.meta parser)
CREATE TABLE IF NOT EXISTS objects (
    bucket VARCHAR NOT NULL,
    key VARCHAR NOT NULL,
    size BIGINT NOT NULL,
    mod_time BIGINT NOT NULL,
    etag VARCHAR NOT NULL,
    content_type VARCHAR,
    data_dir VARCHAR,
    distribution INTEGER[],
    parts JSON,
    PRIMARY KEY (bucket, key)
);

CREATE INDEX IF NOT EXISTS idx_xfs_path ON xfs_entries(path);
CREATE INDEX IF NOT EXISTS idx_keys_bucket ON object_keys(bucket);
CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
`

// NewDuckStore creates a new DuckDB-based metadata store
func NewDuckStore(dbPath string) (*DuckStore, error) {
	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	db := sql.OpenDB(connector)

	// Create schema
	if _, err := db.Exec(duckDBSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	return &DuckStore{db: db, connector: connector}, nil
}

// Close closes the database connection
func (s *DuckStore) Close() error {
	return s.db.Close()
}

// DB returns the underlying database connection
func (s *DuckStore) DB() *sql.DB {
	return s.db
}

// Connector returns the DuckDB connector for creating appenders
func (s *DuckStore) Connector() *duckdb.Connector {
	return s.connector
}

// ObjectPath represents a minimal object record from fast scan (before xl.meta parsing)
type ObjectPath struct {
	Bucket         string
	Key            string
	DataDir        string
	PartitionIndex int
}

// BulkInserter provides efficient bulk insertion using DuckDB's Appender
type BulkInserter struct {
	store       *DuckStore
	conn        driver.Conn
	keyAppender *duckdb.Appender
	xfsAppender *duckdb.Appender
}

// NewBulkInserter creates a new bulk inserter using DuckDB's fast Appender API
func (s *DuckStore) NewBulkInserter(batchSize int) (*BulkInserter, error) {
	conn, err := s.connector.Connect(context.Background())
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	keyAppender, err := duckdb.NewAppenderFromConn(conn, "", "object_keys")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create key appender: %w", err)
	}

	xfsAppender, err := duckdb.NewAppenderFromConn(conn, "", "xfs_entries")
	if err != nil {
		keyAppender.Close()
		conn.Close()
		return nil, fmt.Errorf("create xfs appender: %w", err)
	}

	return &BulkInserter{
		store:       s,
		conn:        conn,
		keyAppender: keyAppender,
		xfsAppender: xfsAppender,
	}, nil
}

// InsertXFSEntry inserts an XFS filesystem entry using DuckDB's fast Appender
func (bi *BulkInserter) InsertXFSEntry(partitionIndex int, path string, inodeNumber uint64, isDir bool) error {
	return bi.xfsAppender.AppendRow(int32(partitionIndex), path, int64(inodeNumber), isDir)
}

// InsertObjectKey appends a bucket/key pair using DuckDB's fast Appender
func (bi *BulkInserter) InsertObjectKey(bucket, key string) error {
	return bi.keyAppender.AppendRow(bucket, key)
}

// InsertObject inserts a processed object (from xl.meta parsing)
func (bi *BulkInserter) InsertObject(obj *ObjectMeta) error {
	distJSON, _ := json.Marshal(obj.Distribution)
	partsJSON, _ := json.Marshal(obj.Parts)

	_, err := bi.store.db.Exec(`
		INSERT OR REPLACE INTO objects
		(bucket, key, size, mod_time, etag, content_type, data_dir, distribution, parts)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		obj.Bucket, obj.Key,
		obj.Size, obj.ModTime.UnixNano(), obj.ETag, obj.ContentType,
		obj.DataDir, string(distJSON), string(partsJSON),
	)
	return err
}

// Deprecated methods for backwards compatibility
func (bi *BulkInserter) InsertBucket(name string, createdAt time.Time) error {
	// Buckets are now derived from xfs_entries paths
	return nil
}

func (bi *BulkInserter) InsertLocation(bucket, key string, partitionIndex int, inodeNumber uint64) error {
	return bi.InsertXFSEntry(partitionIndex, bucket+"/"+key, inodeNumber, true)
}

func (bi *BulkInserter) InsertObjectPath(bucket, key, dataDir string, partitionIndex int) error {
	return bi.InsertXFSEntry(partitionIndex, bucket+"/"+key, 0, true)
}

// Checkpoint flushes both appenders
func (bi *BulkInserter) Checkpoint() error {
	if err := bi.keyAppender.Flush(); err != nil {
		return err
	}
	return bi.xfsAppender.Flush()
}

// Close flushes and closes both appenders
func (bi *BulkInserter) Close() error {
	bi.keyAppender.Close()
	bi.xfsAppender.Close()
	return bi.conn.Close()
}

// ObjectCount returns the number of objects in the database
func (s *DuckStore) ObjectCount(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM objects").Scan(&count)
	return count, err
}

// UnparsedCount returns the number of objects not yet parsed
func (s *DuckStore) UnparsedCount(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM objects WHERE metadata_loaded = FALSE").Scan(&count)
	return count, err
}

// GetUnparsedObjects returns a batch of objects that need xl.meta parsing
func (s *DuckStore) GetUnparsedObjects(ctx context.Context, limit int) ([]ObjectPath, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT bucket, key, data_dir, partition_index
		FROM objects WHERE metadata_loaded = FALSE
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var paths []ObjectPath
	for rows.Next() {
		var p ObjectPath
		if err := rows.Scan(&p.Bucket, &p.Key, &p.DataDir, &p.PartitionIndex); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

// GetObject retrieves object metadata by bucket and key
func (s *DuckStore) GetObject(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	var obj ObjectMeta
	var modTime, size sql.NullInt64
	var contentType, etag sql.NullString
	var userMetaJSON, distJSON, partsJSON sql.NullString
	var poolIndex, setIndex, dataBlocks, parityBlocks sql.NullInt64
	var blockSize sql.NullInt64

	err := s.db.QueryRowContext(ctx, `
		SELECT bucket, key, data_dir, partition_index, metadata_loaded,
		       size, mod_time, etag, content_type, user_metadata,
		       pool_index, set_index, data_blocks, parity_blocks, block_size, distribution, parts
		FROM objects WHERE bucket = ? AND key = ?
	`, bucket, key).Scan(
		&obj.Bucket, &obj.Key, &obj.DataDir, &obj.PartitionIndex, &obj.MetadataLoaded,
		&size, &modTime, &etag, &contentType, &userMetaJSON,
		&poolIndex, &setIndex, &dataBlocks, &parityBlocks, &blockSize,
		&distJSON, &partsJSON)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Fill in parsed metadata if available
	if obj.MetadataLoaded {
		if size.Valid {
			obj.Size = size.Int64
		}
		if modTime.Valid {
			obj.ModTime = time.Unix(0, modTime.Int64)
		}
		if etag.Valid {
			obj.ETag = etag.String
		}
		if contentType.Valid {
			obj.ContentType = contentType.String
		}
		if poolIndex.Valid {
			obj.PoolIndex = int(poolIndex.Int64)
		}
		if setIndex.Valid {
			obj.SetIndex = int(setIndex.Int64)
		}
		if dataBlocks.Valid {
			obj.DataBlocks = int(dataBlocks.Int64)
		}
		if parityBlocks.Valid {
			obj.ParityBlocks = int(parityBlocks.Int64)
		}
		if blockSize.Valid {
			obj.BlockSize = blockSize.Int64
		}
		if userMetaJSON.Valid {
			json.Unmarshal([]byte(userMetaJSON.String), &obj.UserMeta)
		}
		if distJSON.Valid {
			json.Unmarshal([]byte(distJSON.String), &obj.Distribution)
		}
		if partsJSON.Valid {
			json.Unmarshal([]byte(partsJSON.String), &obj.Parts)
		}
	}

	return &obj, nil
}

// ListBuckets returns all buckets
func (s *DuckStore) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT name, created_at FROM buckets ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buckets []BucketInfo
	for rows.Next() {
		var b BucketInfo
		var createdAt int64
		if err := rows.Scan(&b.Name, &createdAt); err != nil {
			return nil, err
		}
		b.CreatedAt = time.Unix(0, createdAt)
		buckets = append(buckets, b)
	}
	return buckets, rows.Err()
}

// ListObjects lists objects with optional prefix filtering
func (s *DuckStore) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (*ObjectList, error) {
	// Simplified implementation - full S3 prefix/delimiter handling would be more complex
	query := `SELECT bucket, key, data_dir, partition_index, metadata_loaded, size, mod_time, etag, content_type FROM objects WHERE bucket = ?`
	args := []any{bucket}

	if prefix != "" {
		query += " AND key LIKE ?"
		args = append(args, prefix+"%")
	}
	if marker != "" {
		query += " AND key > ?"
		args = append(args, marker)
	}
	query += " ORDER BY key LIMIT ?"
	args = append(args, maxKeys+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	info := &ObjectList{}
	count := 0
	for rows.Next() && count < maxKeys {
		var obj ObjectMeta
		var modTime, size sql.NullInt64
		var contentType, etag sql.NullString
		if err := rows.Scan(&obj.Bucket, &obj.Key, &obj.DataDir, &obj.PartitionIndex, &obj.MetadataLoaded,
			&size, &modTime, &etag, &contentType); err != nil {
			return nil, err
		}
		if obj.MetadataLoaded {
			if size.Valid {
				obj.Size = size.Int64
			}
			if modTime.Valid {
				obj.ModTime = time.Unix(0, modTime.Int64)
			}
			if etag.Valid {
				obj.ETag = etag.String
			}
			if contentType.Valid {
				obj.ContentType = contentType.String
			}
		}
		// For unparsed objects, Size/ModTime/ETag will be zero values (dummy metadata)
		info.Objects = append(info.Objects, obj)
		count++
	}

	// Check if truncated
	if rows.Next() {
		info.IsTruncated = true
		if len(info.Objects) > 0 {
			info.NextMarker = info.Objects[len(info.Objects)-1].Key
		}
	}

	return info, rows.Err()
}

// BucketExists checks if a bucket exists
func (s *DuckStore) BucketExists(ctx context.Context, name string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM buckets WHERE name = ?", name).Scan(&count)
	return count > 0, err
}

// ClearAll removes all data from the database (for re-sync)
func (s *DuckStore) ClearAll(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM xfs_entries")
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "DELETE FROM object_keys")
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "DELETE FROM objects")
	return err
}

// UpdateObjectMetadata updates an object with parsed xl.meta data
func (s *DuckStore) UpdateObjectMetadata(ctx context.Context, obj *ObjectMeta) error {
	userMetaJSON, _ := json.Marshal(obj.UserMeta)
	distJSON, _ := json.Marshal(obj.Distribution)
	partsJSON, _ := json.Marshal(obj.Parts)

	_, err := s.db.ExecContext(ctx, `
		UPDATE objects SET
			metadata_loaded = TRUE,
			size = ?,
			mod_time = ?,
			etag = ?,
			content_type = ?,
			user_metadata = ?,
			pool_index = ?,
			set_index = ?,
			data_blocks = ?,
			parity_blocks = ?,
			block_size = ?,
			distribution = ?,
			parts = ?
		WHERE bucket = ? AND key = ?
	`,
		obj.Size, obj.ModTime.UnixNano(), obj.ETag, obj.ContentType, string(userMetaJSON),
		obj.PoolIndex, obj.SetIndex, obj.DataBlocks, obj.ParityBlocks, obj.BlockSize,
		string(distJSON), string(partsJSON),
		obj.Bucket, obj.Key)
	return err
}
