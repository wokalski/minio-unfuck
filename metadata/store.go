// Package metadata provides SQLite-based metadata caching for MinIO objects
package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const schema = `
CREATE TABLE IF NOT EXISTS buckets (
    name TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
    id INTEGER PRIMARY KEY,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    size INTEGER NOT NULL,
    mod_time INTEGER NOT NULL,
    etag TEXT NOT NULL,
    content_type TEXT,
    user_metadata TEXT,  -- JSON

    -- Erasure config (for reads)
    pool_index INTEGER NOT NULL DEFAULT 0,  -- Which pool
    set_index INTEGER NOT NULL DEFAULT 0,   -- Which erasure set within pool
    data_dir TEXT NOT NULL,
    data_blocks INTEGER NOT NULL,
    parity_blocks INTEGER NOT NULL,
    block_size INTEGER NOT NULL,
    distribution TEXT NOT NULL,  -- JSON array

    -- Parts (JSON array for multipart)
    parts TEXT,  -- [{number, size, actual_size}...]

    UNIQUE(bucket, key)
);

CREATE INDEX IF NOT EXISTS idx_objects_prefix ON objects(bucket, key);
CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
`

// ObjectMeta represents cached object metadata
type ObjectMeta struct {
	Bucket      string
	Key         string
	Size        int64
	ModTime     time.Time
	ETag        string
	ContentType string
	UserMeta    map[string]string

	// Location info (always set from fast scan)
	PartitionIndex int  // Which partition we found this on
	DataDir        string
	MetadataLoaded bool // Whether xl.meta has been parsed

	// Erasure config (set after xl.meta parsing)
	PoolIndex    int // Which pool this object belongs to
	SetIndex     int // Which erasure set within the pool
	DataBlocks   int
	ParityBlocks int
	BlockSize    int64
	Distribution []int

	// Parts
	Parts []PartMeta
}

// PartMeta represents a part of a multipart object
type PartMeta struct {
	Number     int   `json:"number"`
	Size       int64 `json:"size"`
	ActualSize int64 `json:"actual_size"`
}

// BucketInfo represents bucket metadata
type BucketInfo struct {
	Name      string
	CreatedAt time.Time
}

// ObjectList represents a paginated list of objects
type ObjectList struct {
	Objects      []ObjectMeta
	Prefixes     []string
	IsTruncated  bool
	NextMarker   string
}

// Store provides metadata storage and retrieval
type Store struct {
	db *sql.DB
}

// NewStore creates a new metadata store
func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Initialize schema
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	return &Store{db: db}, nil
}

// Close closes the database connection
func (s *Store) Close() error {
	return s.db.Close()
}

// UpsertBucket inserts or updates a bucket
func (s *Store) UpsertBucket(ctx context.Context, name string, createdAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO buckets (name, created_at) VALUES (?, ?)
		 ON CONFLICT(name) DO UPDATE SET created_at = excluded.created_at`,
		name, createdAt.Unix())
	return err
}

// ListBuckets returns all buckets
func (s *Store) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, created_at FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buckets []BucketInfo
	for rows.Next() {
		var name string
		var createdAt int64
		if err := rows.Scan(&name, &createdAt); err != nil {
			return nil, err
		}
		buckets = append(buckets, BucketInfo{
			Name:      name,
			CreatedAt: time.Unix(createdAt, 0),
		})
	}
	return buckets, rows.Err()
}

// BucketExists checks if a bucket exists
func (s *Store) BucketExists(ctx context.Context, name string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM buckets WHERE name = ?`, name).Scan(&count)
	return count > 0, err
}

// UpsertObject inserts or updates an object
func (s *Store) UpsertObject(ctx context.Context, obj *ObjectMeta) error {
	userMetaJSON, err := json.Marshal(obj.UserMeta)
	if err != nil {
		return fmt.Errorf("marshal user metadata: %w", err)
	}

	distJSON, err := json.Marshal(obj.Distribution)
	if err != nil {
		return fmt.Errorf("marshal distribution: %w", err)
	}

	partsJSON, err := json.Marshal(obj.Parts)
	if err != nil {
		return fmt.Errorf("marshal parts: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO objects (bucket, key, size, mod_time, etag, content_type, user_metadata,
		                      pool_index, set_index, data_dir, data_blocks, parity_blocks, block_size, distribution, parts)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(bucket, key) DO UPDATE SET
		     size = excluded.size,
		     mod_time = excluded.mod_time,
		     etag = excluded.etag,
		     content_type = excluded.content_type,
		     user_metadata = excluded.user_metadata,
		     pool_index = excluded.pool_index,
		     set_index = excluded.set_index,
		     data_dir = excluded.data_dir,
		     data_blocks = excluded.data_blocks,
		     parity_blocks = excluded.parity_blocks,
		     block_size = excluded.block_size,
		     distribution = excluded.distribution,
		     parts = excluded.parts`,
		obj.Bucket, obj.Key, obj.Size, obj.ModTime.UnixNano(), obj.ETag, obj.ContentType, string(userMetaJSON),
		obj.PoolIndex, obj.SetIndex, obj.DataDir, obj.DataBlocks, obj.ParityBlocks, obj.BlockSize, string(distJSON), string(partsJSON))
	return err
}

// GetObject retrieves object metadata
func (s *Store) GetObject(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	var obj ObjectMeta
	var modTime int64
	var userMetaJSON, distJSON, partsJSON string
	var contentType sql.NullString

	err := s.db.QueryRowContext(ctx,
		`SELECT bucket, key, size, mod_time, etag, content_type, user_metadata,
		        pool_index, set_index, data_dir, data_blocks, parity_blocks, block_size, distribution, parts
		 FROM objects WHERE bucket = ? AND key = ?`,
		bucket, key).Scan(
		&obj.Bucket, &obj.Key, &obj.Size, &modTime, &obj.ETag, &contentType, &userMetaJSON,
		&obj.PoolIndex, &obj.SetIndex, &obj.DataDir, &obj.DataBlocks, &obj.ParityBlocks, &obj.BlockSize, &distJSON, &partsJSON)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	obj.ModTime = time.Unix(0, modTime)
	obj.ContentType = contentType.String

	if userMetaJSON != "" {
		if err := json.Unmarshal([]byte(userMetaJSON), &obj.UserMeta); err != nil {
			return nil, fmt.Errorf("unmarshal user metadata: %w", err)
		}
	}

	if err := json.Unmarshal([]byte(distJSON), &obj.Distribution); err != nil {
		return nil, fmt.Errorf("unmarshal distribution: %w", err)
	}

	if partsJSON != "" {
		if err := json.Unmarshal([]byte(partsJSON), &obj.Parts); err != nil {
			return nil, fmt.Errorf("unmarshal parts: %w", err)
		}
	}

	return &obj, nil
}

// ListObjects lists objects with optional prefix and delimiter support
func (s *Store) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (*ObjectList, error) {
	result := &ObjectList{}

	if maxKeys <= 0 {
		maxKeys = 1000
	}

	// Query one extra to detect truncation
	query := `SELECT bucket, key, size, mod_time, etag, content_type
	          FROM objects
	          WHERE bucket = ? AND key LIKE ? AND key > ?
	          ORDER BY key
	          LIMIT ?`

	prefixPattern := prefix + "%"
	rows, err := s.db.QueryContext(ctx, query, bucket, prefixPattern, marker, maxKeys+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	prefixSet := make(map[string]bool)

	for rows.Next() {
		var obj ObjectMeta
		var modTime int64
		var contentType sql.NullString

		if err := rows.Scan(&obj.Bucket, &obj.Key, &obj.Size, &modTime, &obj.ETag, &contentType); err != nil {
			return nil, err
		}

		obj.ModTime = time.Unix(0, modTime)
		obj.ContentType = contentType.String

		// Handle delimiter
		if delimiter != "" {
			// Check if there's a delimiter after the prefix
			afterPrefix := obj.Key[len(prefix):]
			if idx := findDelimiter(afterPrefix, delimiter); idx >= 0 {
				// This is a common prefix
				commonPrefix := prefix + afterPrefix[:idx+len(delimiter)]
				if !prefixSet[commonPrefix] {
					prefixSet[commonPrefix] = true
					result.Prefixes = append(result.Prefixes, commonPrefix)
				}
				continue
			}
		}

		if len(result.Objects) >= maxKeys {
			result.IsTruncated = true
			result.NextMarker = result.Objects[len(result.Objects)-1].Key
			break
		}

		result.Objects = append(result.Objects, obj)
	}

	return result, rows.Err()
}

func findDelimiter(s, delimiter string) int {
	for i := 0; i <= len(s)-len(delimiter); i++ {
		if s[i:i+len(delimiter)] == delimiter {
			return i
		}
	}
	return -1
}

// DeleteObject removes an object from the cache
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ? AND key = ?`, bucket, key)
	return err
}

// ObjectCount returns the total number of objects in the cache
func (s *Store) ObjectCount(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects`).Scan(&count)
	return count, err
}

// BeginTx starts a transaction
func (s *Store) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, nil)
}

// UpsertObjectTx inserts or updates an object within a transaction
func (s *Store) UpsertObjectTx(ctx context.Context, tx *sql.Tx, obj *ObjectMeta) error {
	userMetaJSON, err := json.Marshal(obj.UserMeta)
	if err != nil {
		return fmt.Errorf("marshal user metadata: %w", err)
	}

	distJSON, err := json.Marshal(obj.Distribution)
	if err != nil {
		return fmt.Errorf("marshal distribution: %w", err)
	}

	partsJSON, err := json.Marshal(obj.Parts)
	if err != nil {
		return fmt.Errorf("marshal parts: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO objects (bucket, key, size, mod_time, etag, content_type, user_metadata,
		                      pool_index, set_index, data_dir, data_blocks, parity_blocks, block_size, distribution, parts)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(bucket, key) DO UPDATE SET
		     size = excluded.size,
		     mod_time = excluded.mod_time,
		     etag = excluded.etag,
		     content_type = excluded.content_type,
		     user_metadata = excluded.user_metadata,
		     pool_index = excluded.pool_index,
		     set_index = excluded.set_index,
		     data_dir = excluded.data_dir,
		     data_blocks = excluded.data_blocks,
		     parity_blocks = excluded.parity_blocks,
		     block_size = excluded.block_size,
		     distribution = excluded.distribution,
		     parts = excluded.parts`,
		obj.Bucket, obj.Key, obj.Size, obj.ModTime.UnixNano(), obj.ETag, obj.ContentType, string(userMetaJSON),
		obj.PoolIndex, obj.SetIndex, obj.DataDir, obj.DataBlocks, obj.ParityBlocks, obj.BlockSize, string(distJSON), string(partsJSON))
	return err
}
