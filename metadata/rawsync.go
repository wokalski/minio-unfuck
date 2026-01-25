package metadata

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/wokalski/minio-unfuck/erasure"
	"github.com/wokalski/minio-unfuck/xlmeta"
)

// RawSyncConfig configures the raw disk sync operation
type RawSyncConfig struct {
	DiskPaths    []string // Raw disk paths (e.g., /dev/sde)
	DBPath       string   // Path to DuckDB database file
	Workers      int      // Number of parallel workers per partition
	BatchSize    int      // Batch size for bulk inserts
	ShowProgress bool     // Whether to show progress bar
}

// RawSyncStats holds statistics from the sync operation
type RawSyncStats struct {
	PartitionsFound int
	ObjectsFound    int64
	BucketsFound    int
	Duration        time.Duration
	Errors          []error
}

// RawSyncer handles syncing metadata from raw disk access
type RawSyncer struct {
	config RawSyncConfig
	store  *DuckStore
}

// NewRawSyncer creates a new raw disk syncer
func NewRawSyncer(config RawSyncConfig) (*RawSyncer, error) {
	if config.Workers <= 0 {
		config.Workers = 4
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 10000
	}

	store, err := NewDuckStore(config.DBPath)
	if err != nil {
		return nil, fmt.Errorf("create duckdb store: %w", err)
	}

	return &RawSyncer{
		config: config,
		store:  store,
	}, nil
}

// Close closes the syncer and its database connection
func (rs *RawSyncer) Close() error {
	return rs.store.Close()
}

// Store returns the underlying DuckStore
func (rs *RawSyncer) Store() *DuckStore {
	return rs.store
}

// Sync performs a full sync from raw disks to the database
func (rs *RawSyncer) Sync(ctx context.Context) (*RawSyncStats, error) {
	startTime := time.Now()
	stats := &RawSyncStats{}

	// Clear existing data for fresh sync
	if err := rs.store.ClearAll(ctx); err != nil {
		return nil, fmt.Errorf("clear database: %w", err)
	}

	// Discover all partitions from all disks
	var allPartitions []string
	for _, diskPath := range rs.config.DiskPaths {
		partitions, err := erasure.DiscoverPartitions(diskPath)
		if err != nil {
			stats.Errors = append(stats.Errors, fmt.Errorf("discover partitions %s: %w", diskPath, err))
			continue
		}
		allPartitions = append(allPartitions, partitions...)
	}

	if len(allPartitions) == 0 {
		return nil, fmt.Errorf("no partitions found on disks: %v", rs.config.DiskPaths)
	}

	stats.PartitionsFound = len(allPartitions)
	sort.Strings(allPartitions)

	// First pass: count total objects for progress bar
	var totalObjects int64
	if rs.config.ShowProgress {
		fmt.Printf("Counting objects across %d partitions...\n", len(allPartitions))
		for _, partPath := range allPartitions {
			count, err := rs.countObjects(partPath)
			if err != nil {
				continue // Skip errors in counting phase
			}
			totalObjects += count
		}
		fmt.Printf("Found approximately %d objects\n", totalObjects)
	}

	// Create progress bar
	var bar *progressbar.ProgressBar
	if rs.config.ShowProgress && totalObjects > 0 {
		bar = progressbar.NewOptions64(
			totalObjects,
			progressbar.OptionSetDescription("Syncing"),
			progressbar.OptionSetWidth(40),
			progressbar.OptionShowCount(),
			progressbar.OptionShowIts(),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "=",
				SaucerHead:    ">",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
		)
	}

	// Process partitions
	bucketsSeen := make(map[string]bool)
	var bucketsMu sync.Mutex

	inserter, err := rs.store.NewBulkInserter(rs.config.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("create bulk inserter: %w", err)
	}
	defer inserter.Close()

	var objectsProcessed int64

	for partIdx, partPath := range allPartitions {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}

		if !rs.config.ShowProgress {
			fmt.Printf("Processing partition %d/%d: %s\n", partIdx+1, len(allPartitions), partPath)
		}

		rawFS, err := erasure.OpenRawFS(partPath)
		if err != nil {
			stats.Errors = append(stats.Errors, fmt.Errorf("open %s: %w", partPath, err))
			continue
		}

		err = rs.processPartition(ctx, rawFS, partIdx, inserter, bucketsSeen, &bucketsMu, &objectsProcessed, bar)
		rawFS.Close()

		if err != nil {
			stats.Errors = append(stats.Errors, fmt.Errorf("process %s: %w", partPath, err))
		}
	}

	if bar != nil {
		bar.Finish()
	}

	stats.ObjectsFound = objectsProcessed
	stats.BucketsFound = len(bucketsSeen)
	stats.Duration = time.Since(startTime)

	return stats, nil
}

// countObjects quickly counts xl.meta files in a partition
func (rs *RawSyncer) countObjects(partPath string) (int64, error) {
	rawFS, err := erasure.OpenRawFS(partPath)
	if err != nil {
		return 0, err
	}
	defer rawFS.Close()

	var count int64
	var walkErrors int
	err = fs.WalkDir(rawFS.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			walkErrors++
			if walkErrors <= 5 {
				fmt.Printf("  Walk error at %s: %v\n", path, err)
			}
			return nil // Skip errors
		}
		if d.Name() == "xl.meta" {
			count++
		}
		return nil
	})
	if walkErrors > 5 {
		fmt.Printf("  ... and %d more walk errors\n", walkErrors-5)
	}

	return count, err
}

// processPartition processes a single partition
func (rs *RawSyncer) processPartition(
	ctx context.Context,
	rawFS *erasure.RawFS,
	partIdx int,
	inserter *BulkInserter,
	bucketsSeen map[string]bool,
	bucketsMu *sync.Mutex,
	objectsProcessed *int64,
	bar *progressbar.ProgressBar,
) error {
	return fs.WalkDir(rawFS.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // Skip errors, continue walking
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Only process xl.meta files
		if d.Name() != "xl.meta" {
			return nil
		}

		// Parse path: bucket/key/dataDir/xl.meta
		parts := strings.Split(path, string(filepath.Separator))
		if len(parts) < 3 {
			return nil
		}

		bucket := parts[0]

		// Register bucket
		bucketsMu.Lock()
		if !bucketsSeen[bucket] {
			bucketsSeen[bucket] = true
			inserter.InsertBucket(bucket, time.Now())
		}
		bucketsMu.Unlock()

		// Read xl.meta content
		xlMetaData, err := rawFS.ReadFile(path)
		if err != nil {
			return nil // Skip unreadable files
		}

		// Parse xl.meta
		meta, err := xlmeta.Parse(xlMetaData)
		if err != nil {
			return nil // Skip unparseable files
		}

		// Build object key from path (remove bucket prefix and xl.meta suffix)
		dataDir := parts[len(parts)-2]
		keyParts := parts[1 : len(parts)-2]
		key := strings.Join(keyParts, "/")

		// Create ObjectMeta from xl.meta
		obj := &ObjectMeta{
			Bucket:         bucket,
			Key:            key,
			Size:           meta.Size,
			ModTime:        meta.ModTime,
			ETag:           meta.ETag,
			ContentType:    meta.ContentType,
			UserMeta:       meta.UserMeta,
			PartitionIndex: partIdx,
			DataDir:        dataDir,
			MetadataLoaded: true,
			DataBlocks:     meta.DataBlocks,
			ParityBlocks:   meta.ParityBlocks,
			BlockSize:      meta.BlockSize,
			Distribution:   meta.Distribution,
		}

		// Convert parts
		for _, p := range meta.Parts {
			obj.Parts = append(obj.Parts, PartMeta{
				Number:     p.Number,
				Size:       p.Size,
				ActualSize: p.ActualSize,
			})
		}

		// Insert object
		if err := inserter.InsertObject(obj); err != nil {
			return nil // Skip insert errors
		}

		atomic.AddInt64(objectsProcessed, 1)
		if bar != nil {
			bar.Add(1)
		}

		return nil
	})
}

// TestSync validates a subset of synced objects by reading and decoding them
func (rs *RawSyncer) TestSync(ctx context.Context, sampleSize int) error {
	// Get random sample of objects
	rows, err := rs.store.db.QueryContext(ctx, `
		SELECT bucket, key FROM objects ORDER BY RANDOM() LIMIT ?
	`, sampleSize)
	if err != nil {
		return fmt.Errorf("query sample objects: %w", err)
	}
	defer rows.Close()

	var samples []struct{ bucket, key string }
	for rows.Next() {
		var s struct{ bucket, key string }
		if err := rows.Scan(&s.bucket, &s.key); err != nil {
			return err
		}
		samples = append(samples, s)
	}

	fmt.Printf("Testing %d random objects...\n", len(samples))

	// For each sample, verify we can:
	// 1. Load the object metadata
	// 2. Find the extents
	// 3. Read the xl.meta using extents
	for i, s := range samples {
		obj, err := rs.store.GetObject(ctx, s.bucket, s.key)
		if err != nil {
			return fmt.Errorf("get object %s/%s: %w", s.bucket, s.key, err)
		}
		if obj == nil {
			return fmt.Errorf("object %s/%s not found", s.bucket, s.key)
		}

		// Check that we have extents for the xl.meta
		xlMetaPath := fmt.Sprintf("%s/%s/%s/xl.meta", s.bucket, s.key, obj.DataDir)

		// Query for any partition that has this file's extents
		var extentCount int
		err = rs.store.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM file_extents WHERE file_path = ?
		`, xlMetaPath).Scan(&extentCount)
		if err != nil {
			return fmt.Errorf("count extents for %s: %w", xlMetaPath, err)
		}

		if extentCount == 0 {
			return fmt.Errorf("no extents found for %s", xlMetaPath)
		}

		fmt.Printf("  [%d/%d] %s/%s: OK (metadata loaded, %d extents)\n",
			i+1, len(samples), s.bucket, s.key, extentCount)
	}

	fmt.Println("All test samples passed!")
	return nil
}
