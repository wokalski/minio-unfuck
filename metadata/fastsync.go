package metadata

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/wokalski/minio-unfuck/erasure"
	"github.com/wokalski/minio-unfuck/vendor_fork/xfs/xfs"
)

// FastSyncConfig configures the fast directory scan
type FastSyncConfig struct {
	DiskPaths    []string // Raw disk paths (e.g., /dev/sde)
	DBPath       string   // Path to DuckDB database file
	BatchSize    int      // Batch size for bulk inserts
	ShowProgress bool     // Whether to show progress bar
}

// FastSyncStats holds statistics from the fast scan
type FastSyncStats struct {
	PartitionsFound int
	ObjectsFound    int64
	BucketsFound    int
	Duration        time.Duration
	Errors          []error
}

// FastSyncer handles fast directory scanning without reading xl.meta files
type FastSyncer struct {
	config     FastSyncConfig
	store      *DuckStore
	partitions []*erasure.RawFS
	diskMap    *erasure.DiskMapping
}

// NewFastSyncer creates a new fast syncer
func NewFastSyncer(config FastSyncConfig) (*FastSyncer, error) {
	if config.BatchSize <= 0 {
		config.BatchSize = 10000
	}

	store, err := NewDuckStore(config.DBPath)
	if err != nil {
		return nil, fmt.Errorf("create duckdb store: %w", err)
	}

	return &FastSyncer{
		config: config,
		store:  store,
	}, nil
}

// Close closes the syncer and its resources
func (s *FastSyncer) Close() error {
	for _, p := range s.partitions {
		p.Close()
	}
	return s.store.Close()
}

// Store returns the underlying DuckStore
func (s *FastSyncer) Store() *DuckStore {
	return s.store
}

// DiskMapping returns the disk mapping (available after Sync)
func (s *FastSyncer) DiskMapping() *erasure.DiskMapping {
	return s.diskMap
}

// Partitions returns the opened partitions (available after Sync)
func (s *FastSyncer) Partitions() []*erasure.RawFS {
	return s.partitions
}

func (s *FastSyncer) Sync(ctx context.Context) (*FastSyncStats, error) {
	startTime := time.Now()
	stats := &FastSyncStats{}

	if err := s.store.ClearAll(ctx); err != nil {
		return nil, fmt.Errorf("clear database: %w", err)
	}

	// Discover all partitions from all disks
	var allPartitionPaths []string
	for _, diskPath := range s.config.DiskPaths {
		partitions, err := erasure.DiscoverPartitions(diskPath)
		if err != nil {
			stats.Errors = append(stats.Errors, fmt.Errorf("discover partitions %s: %w", diskPath, err))
			continue
		}
		allPartitionPaths = append(allPartitionPaths, partitions...)
	}

	if len(allPartitionPaths) == 0 {
		return nil, fmt.Errorf("no partitions found on disks: %v", s.config.DiskPaths)
	}

	stats.PartitionsFound = len(allPartitionPaths)
	sort.Strings(allPartitionPaths)

	// Open all partitions
	s.partitions = make([]*erasure.RawFS, len(allPartitionPaths))
	for i, partPath := range allPartitionPaths {
		rawFS, err := erasure.OpenRawFS(partPath)
		if err != nil {
			stats.Errors = append(stats.Errors, fmt.Errorf("open partition %s: %w", partPath, err))
			continue
		}
		s.partitions[i] = rawFS
	}

	// Build disk mapping from format.json
	diskMap, err := erasure.BuildDiskMappingFromRaw(s.partitions)
	if err != nil {
		return nil, fmt.Errorf("build disk mapping: %w", err)
	}
	s.diskMap = diskMap

	// Skip counting phase - it's too slow for raw XFS access
	// Just use a spinner-style progress indicator instead
	var bar *progressbar.ProgressBar
	if s.config.ShowProgress {
		bar = progressbar.NewOptions(-1, // Unknown total
			progressbar.OptionSetDescription("Scanning"),
			progressbar.OptionSetWidth(40),
			progressbar.OptionSpinnerType(14),
			progressbar.OptionShowIts(),
		)
	}

	// Process partitions
	bucketsSeen := make(map[string]bool)
	var bucketsMu sync.Mutex

	inserter, err := s.store.NewBulkInserter(s.config.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("create bulk inserter: %w", err)
	}
	defer inserter.Close()

	var objectsProcessed int64

	// RAW SCAN: No recursion, no abstractions
	// Stream directly from XFS btrees: list buckets → list keys → store (bucket, key, inode)
	for partIdx, part := range s.partitions {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}

		if part == nil {
			return nil, fmt.Errorf("partition %d is nil", partIdx)
		}

		fmt.Printf("\n[Partition %d/%d] Raw XFS scan...\n", partIdx+1, len(s.partitions))
		partStart := time.Now()

		// Get the XFS filesystem directly
		xfsFS := part.XFS()

		// List buckets from root
		var buckets []xfs.DirEntry

		err := xfsFS.WalkDirWithInodes(".", func(entry xfs.DirEntry) error {
			if entry.IsDir && !strings.HasPrefix(entry.Name, ".") {
				buckets = append(buckets, entry)
				bucketsMu.Lock()
				bucketsSeen[entry.Name] = true
				bucketsMu.Unlock()
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("partition %d: list buckets: %w", partIdx, err)
		}

		fmt.Printf("  Found %d buckets\n", len(buckets))

		// For each bucket, stream keys directly from btree - ONE call per bucket, no recursion
		for _, bucket := range buckets {
			bucketStart := time.Now()
			var keyCount int64

			err := xfsFS.WalkDirWithInodes(bucket.Name, func(entry xfs.DirEntry) error {
				if entry.IsDir {
					// DB I/O commented out for speed testing
					// inserter.InsertXFSEntry(partIdx, bucket.Name+"/"+entry.Name, entry.Inode, true)
					// inserter.InsertObjectKey(bucket.Name, entry.Name)
					keyCount++
					atomic.AddInt64(&objectsProcessed, 1)
					if bar != nil {
						bar.Add(1)
					}
				}
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("partition %d: scan bucket %s: %w", partIdx, bucket.Name, err)
			}

			fmt.Printf("  [%s] %d keys in %v\n", bucket.Name, keyCount, time.Since(bucketStart))
		}

		fmt.Printf("[Partition %d] done in %v (total keys: %d)\n", partIdx+1, time.Since(partStart), atomic.LoadInt64(&objectsProcessed))
	}

	if bar != nil {
		bar.Finish()
	}

	stats.ObjectsFound = objectsProcessed
	stats.BucketsFound = len(bucketsSeen)
	stats.Duration = time.Since(startTime)

	return stats, nil
}
