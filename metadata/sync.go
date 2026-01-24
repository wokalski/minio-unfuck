package metadata

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wokalski/minio-unfuck/xlmeta"
)

// SyncConfig configures the sync process
type SyncConfig struct {
	DiskPath    string // Path to a single disk to scan
	PoolIndex   int    // Which pool this disk belongs to
	SetIndex    int    // Which erasure set this disk belongs to
	BatchSize   int    // Number of objects per transaction (default: 1000)
	ProgressLog bool   // Log progress
}

// Syncer synchronizes disk metadata to SQLite
type Syncer struct {
	store  *Store
	config SyncConfig
}

// NewSyncer creates a new syncer
func NewSyncer(store *Store, config SyncConfig) *Syncer {
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	return &Syncer{
		store:  store,
		config: config,
	}
}

// SyncResult contains statistics about the sync operation
type SyncResult struct {
	BucketsFound  int
	ObjectsFound  int
	ObjectsSynced int
	Errors        int
	Duration      time.Duration
}

// Sync performs a full sync from disk to SQLite
func (s *Syncer) Sync(ctx context.Context) (*SyncResult, error) {
	start := time.Now()
	result := &SyncResult{}

	// Find all buckets (directories in the disk root, excluding .minio.sys)
	entries, err := os.ReadDir(s.config.DiskPath)
	if err != nil {
		return nil, fmt.Errorf("read disk root: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if entry.Name() == ".minio.sys" {
			continue
		}

		bucketName := entry.Name()
		bucketPath := filepath.Join(s.config.DiskPath, bucketName)

		// Get bucket creation time from directory
		info, err := entry.Info()
		if err != nil {
			result.Errors++
			continue
		}

		if err := s.store.UpsertBucket(ctx, bucketName, info.ModTime()); err != nil {
			result.Errors++
			continue
		}
		result.BucketsFound++

		// Sync objects in this bucket
		objCount, errCount, err := s.syncBucket(ctx, bucketName, bucketPath)
		if err != nil {
			log.Printf("Error syncing bucket %s: %v", bucketName, err)
		}
		result.ObjectsFound += objCount
		result.ObjectsSynced += objCount - errCount
		result.Errors += errCount
	}

	result.Duration = time.Since(start)
	return result, nil
}

// syncBucket syncs all objects in a bucket
func (s *Syncer) syncBucket(ctx context.Context, bucketName, bucketPath string) (int, int, error) {
	objectCount := 0
	errorCount := 0

	// Start a transaction for batch inserts
	tx, err := s.store.BeginTx(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	batchCount := 0

	// Walk all subdirectories (each is an object key)
	err = filepath.WalkDir(bucketPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		// We're looking for xl.meta files
		if d.IsDir() || d.Name() != "xl.meta" {
			return nil
		}

		// Extract object key from path
		// Path format: bucketPath/key/xl.meta
		relPath, err := filepath.Rel(bucketPath, path)
		if err != nil {
			errorCount++
			return nil
		}

		// Remove /xl.meta suffix to get the key
		objectKey := filepath.Dir(relPath)
		if objectKey == "." {
			return nil
		}

		// Skip if this is inside a data directory (UUID format)
		// We want the xl.meta at the object level, not inside data dirs
		parts := strings.Split(objectKey, string(filepath.Separator))
		if len(parts) > 1 {
			// Check if last part looks like a UUID (data dir)
			lastPart := parts[len(parts)-1]
			if looksLikeUUID(lastPart) {
				return nil
			}
		}

		objectCount++

		// Read and parse xl.meta
		data, err := os.ReadFile(path)
		if err != nil {
			errorCount++
			return nil
		}

		xlMeta, err := xlmeta.Parse(data)
		if err != nil {
			errorCount++
			return nil
		}

		// Convert to metadata.ObjectMeta
		obj := &ObjectMeta{
			Bucket:       bucketName,
			Key:          objectKey,
			Size:         xlMeta.Size,
			ModTime:      xlMeta.ModTime,
			ETag:         xlMeta.ETag,
			ContentType:  xlMeta.ContentType,
			UserMeta:     xlMeta.UserMeta,
			PoolIndex:    s.config.PoolIndex,
			SetIndex:     s.config.SetIndex,
			DataDir:      xlMeta.DataDirString(),
			DataBlocks:   xlMeta.DataBlocks,
			ParityBlocks: xlMeta.ParityBlocks,
			BlockSize:    xlMeta.BlockSize,
			Distribution: xlMeta.Distribution,
		}

		// Convert parts
		for _, p := range xlMeta.Parts {
			obj.Parts = append(obj.Parts, PartMeta{
				Number:     p.Number,
				Size:       p.Size,
				ActualSize: p.ActualSize,
			})
		}

		// Insert into database
		if err := s.store.UpsertObjectTx(ctx, tx, obj); err != nil {
			errorCount++
			return nil
		}

		batchCount++

		// Commit batch
		if batchCount >= s.config.BatchSize {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit batch: %w", err)
			}

			if s.config.ProgressLog {
				log.Printf("Synced %d objects in bucket %s", objectCount, bucketName)
			}

			// Start new transaction
			tx, err = s.store.BeginTx(ctx)
			if err != nil {
				return fmt.Errorf("begin new transaction: %w", err)
			}
			batchCount = 0
		}

		return nil
	})

	if err != nil {
		return objectCount, errorCount, err
	}

	// Commit remaining
	if batchCount > 0 {
		if err := tx.Commit(); err != nil {
			return objectCount, errorCount, fmt.Errorf("commit final batch: %w", err)
		}
	}

	if s.config.ProgressLog {
		log.Printf("Finished syncing bucket %s: %d objects", bucketName, objectCount)
	}

	return objectCount, errorCount, nil
}

// looksLikeUUID checks if a string looks like a UUID
func looksLikeUUID(s string) bool {
	// UUID format: 8-4-4-4-12 hex chars with dashes
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
	}
	return true
}
