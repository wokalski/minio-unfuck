package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/johannesboyne/gofakes3"

	"github.com/wokalski/minio-unfuck/backend"
	"github.com/wokalski/minio-unfuck/erasure"
	"github.com/wokalski/minio-unfuck/metadata"
)

// stringSlice is a flag type that collects multiple string values
type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	// Common flags
	dbPath := flag.String("db", "metadata.db", "Path to metadata database")
	listenAddr := flag.String("addr", ":9000", "Address to listen on")
	syncOnStart := flag.Bool("sync", true, "Sync metadata on startup")

	// Directory mode flags (existing)
	rootDir := flag.String("root", "", "Root directory containing disk folders (directory mode)")

	// Raw disk mode flags (new)
	var diskPaths stringSlice
	flag.Var(&diskPaths, "disk", "Raw disk path (e.g., /dev/sde). Can be specified multiple times.")

	// Test mode flag
	testCount := flag.Int("test", 0, "Run validation test on N random objects (requires prior sync)")
	quickTest := flag.Bool("quicktest", false, "Quick test: just list files from first partition and exit")
	skipPartitions := flag.Int("skip", 0, "Skip first N partitions (for testing cache effects)")

	// Sync options
	workers := flag.Int("workers", 4, "Number of parallel workers for sync")
	batchSize := flag.Int("batch", 10000, "Batch size for bulk inserts")

	flag.Parse()

	// Determine mode
	diskMode := len(diskPaths) > 0
	dirMode := *rootDir != ""

	if diskMode && dirMode {
		log.Fatal("Cannot specify both -disk and -root. Choose one mode.")
	}

	if !diskMode && !dirMode {
		// Default to directory mode with default path
		*rootDir = ".disks"
		dirMode = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		cancel()
	}()

	if diskMode {
		runDiskMode(ctx, diskPaths, *dbPath, *listenAddr, *syncOnStart, *testCount, *workers, *batchSize, *quickTest)
	} else {
		runDirectoryMode(ctx, *rootDir, *dbPath, *listenAddr, *syncOnStart)
	}
}

func runDiskMode(
	ctx context.Context,
	diskPaths []string,
	dbPath,
	listenAddr string,
	syncOnStart bool,
	testCount,
	workers,
	batchSize int,
	quickTest bool) {
	log.Printf("Running in raw disk mode with disks: %v", diskPaths)

	if quickTest {
		log.Println("Quick test mode: scanning ALL partitions sequentially (no DB)...")

		var allPartitions []string
		for _, diskPath := range diskPaths {
			partitions, err := erasure.DiscoverPartitions(diskPath)
			if err != nil {
				log.Fatalf("Failed to discover partitions on %s: %v", diskPath, err)
			}
			allPartitions = append(allPartitions, partitions...)
		}
		log.Printf("Found %d total partitions", len(allPartitions))

		// Skip partitions if requested (for testing cache effects)
		skip := *skipPartitions
		if skip > 0 && skip < len(allPartitions) {
			log.Printf("Skipping first %d partitions", skip)
			allPartitions = allPartitions[skip:]
		}

		totalStart := time.Now()
		var totalKeys int64

		for partIdx, partPath := range allPartitions {
			partStart := time.Now()

			rawFS, err := erasure.OpenRawFS(partPath)
			if err != nil {
				log.Printf("[Part %d] Failed to open %s: %v", partIdx+1, partPath, err)
				continue
			}

			// List buckets
			buckets, err := fs.ReadDir(rawFS.FS(), ".")
			if err != nil {
				log.Printf("[Part %d] Failed to read root: %v", partIdx+1, err)
				rawFS.Close()
				continue
			}

			var partKeys int64
			for _, bucket := range buckets {
				if !bucket.IsDir() || bucket.Name()[0] == '.' {
					continue
				}

				// Count keys in this bucket
				keys, err := fs.ReadDir(rawFS.FS(), bucket.Name())
				if err != nil {
					continue
				}

				for _, k := range keys {
					if k.IsDir() {
						partKeys++
					}
				}
			}

			totalKeys += partKeys
			rawFS.Close()
			log.Printf("[Partition %d/%d] %d keys in %v", partIdx+1, len(allPartitions), partKeys, time.Since(partStart))
		}

		log.Printf("\nQuick test complete: %d total keys across %d partitions in %v",
			totalKeys, len(allPartitions), time.Since(totalStart))
		log.Printf("Speed: %.0f keys/sec", float64(totalKeys)/time.Since(totalStart).Seconds())
		return
	}

	// Create fast syncer
	syncer, err := metadata.NewFastSyncer(metadata.FastSyncConfig{
		DiskPaths:    diskPaths,
		DBPath:       dbPath,
		BatchSize:    batchSize,
		ShowProgress: true,
	})
	if err != nil {
		log.Fatalf("Failed to create syncer: %v", err)
	}
	defer syncer.Close()

	// Fast directory scan (no xl.meta parsing)
	if syncOnStart {
		log.Println("Starting fast directory scan...")
		stats, err := syncer.Sync(ctx)
		if err != nil {
			log.Fatalf("Sync failed: %v", err)
		}

		log.Printf("Fast scan complete in %v:", stats.Duration)
		log.Printf("  Partitions: %d", stats.PartitionsFound)
		log.Printf("  Buckets: %d", stats.BucketsFound)
		log.Printf("  Objects: %d", stats.ObjectsFound)
		if len(stats.Errors) > 0 {
			log.Printf("  Errors: %d", len(stats.Errors))
			for _, e := range stats.Errors {
				log.Printf("    - %v", e)
			}
		}
	}

	// Get references for backend
	store := syncer.Store()
	partitions := syncer.Partitions()
	diskMap := syncer.DiskMapping()

	// Get object count
	count, err := store.ObjectCount(ctx)
	if err != nil {
		log.Printf("Warning: failed to count objects: %v", err)
	} else {
		log.Printf("Metadata cache contains %d objects", count)
	}

	// Start background xl.meta parser
	bgParser := metadata.NewBackgroundParser(store, partitions, diskMap, metadata.BackgroundParserConfig{
		Workers:      workers,
		BatchSize:    1000,
		ShowProgress: true,
	})
	if err := bgParser.Start(ctx); err != nil {
		log.Printf("Warning: failed to start background parser: %v", err)
	}
	defer bgParser.Stop()

	// Create backend
	be := backend.NewRawBackend(store, partitions, diskMap)

	// Create gofakes3 server
	faker := gofakes3.New(be)

	log.Printf("Starting S3 server on %s", listenAddr)
	log.Println("Note: xl.meta parsing running in background. First GETs may be slower.")
	log.Println("Example usage:")
	log.Printf("  aws --endpoint-url http://localhost%s s3 ls", listenAddr)
	log.Printf("  aws --endpoint-url http://localhost%s s3 ls s3://BUCKET/", listenAddr)
	log.Printf("  aws --endpoint-url http://localhost%s s3 cp s3://BUCKET/file.txt ./", listenAddr)

	server := &http.Server{
		Addr:    listenAddr,
		Handler: faker.Server(),
	}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal("Server error:", err)
	}
}

// runDirectoryMode runs in traditional directory mode (mounted filesystems)
func runDirectoryMode(ctx context.Context, rootDir, dbPath, listenAddr string, syncOnStart bool) {
	// Discover disks and cluster configuration
	log.Printf("Discovering disks in %s...", rootDir)
	cluster, err := erasure.DiscoverClusterInDirectory(rootDir)
	if err != nil {
		log.Fatalf("Failed to discover disks: %v", err)
	}

	log.Printf("Discovered %d pool(s) with %d total erasure set(s)", len(cluster.Pools), cluster.TotalSets())
	for _, pool := range cluster.Pools {
		log.Printf("Pool %d (%s): %d erasure set(s)", pool.PoolIndex, pool.PoolID[:8], pool.SetCount)
		for setIdx, set := range pool.Sets {
			available := 0
			for _, disk := range set {
				if disk.Path != "" {
					available++
				}
			}
			log.Printf("  Set %d: %d/%d disks available", setIdx, available, len(set))
		}
	}

	// Initialize SQLite store
	log.Printf("Opening metadata database: %s", dbPath)
	store, err := metadata.NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to open metadata store: %v", err)
	}
	defer store.Close()

	// Sync metadata if requested - sync from one disk per set per pool
	if syncOnStart {
		log.Println("Syncing metadata from disk...")

		for _, pool := range cluster.Pools {
			for setIdx, set := range pool.Sets {
				// Find first available disk in this set
				var syncDisk string
				for _, info := range set {
					if info.Path != "" {
						syncDisk = info.Path
						break
					}
				}

				if syncDisk == "" {
					log.Printf("Warning: no available disks in pool %d set %d, skipping sync", pool.PoolIndex, setIdx)
					continue
				}

				syncer := metadata.NewSyncer(store, metadata.SyncConfig{
					DiskPath:    syncDisk,
					PoolIndex:   pool.PoolIndex,
					SetIndex:    setIdx,
					BatchSize:   1000,
					ProgressLog: true,
				})

				result, err := syncer.Sync(ctx)
				if err != nil {
					log.Printf("Warning: sync failed for pool %d set %d: %v", pool.PoolIndex, setIdx, err)
					continue
				}

				log.Printf("Pool %d Set %d sync complete: %d buckets, %d objects (%d errors) in %v",
					pool.PoolIndex, setIdx, result.BucketsFound, result.ObjectsSynced, result.Errors, result.Duration)
			}
		}
	}

	// Count objects in cache
	count, err := store.ObjectCount(ctx)
	if err != nil {
		log.Printf("Warning: failed to count objects: %v", err)
	} else {
		log.Printf("Metadata cache contains %d objects", count)
	}

	// Create decoders for all pools and sets: decoders[poolIndex][setIndex]
	decoders := make([][]*erasure.Decoder, len(cluster.Pools))
	for _, pool := range cluster.Pools {
		decoders[pool.PoolIndex] = make([]*erasure.Decoder, pool.SetCount)
		for setIdx, set := range pool.Sets {
			diskPaths := make([]string, len(set))
			for i, info := range set {
				diskPaths[i] = info.Path
			}
			decoders[pool.PoolIndex][setIdx] = erasure.NewDecoder(diskPaths)
		}
	}

	// Create backend
	be := backend.NewMultiPool(store, decoders)

	// Create gofakes3 server
	faker := gofakes3.New(be)

	log.Printf("Starting S3 server on %s", listenAddr)
	log.Println("Example usage:")
	log.Printf("  aws --endpoint-url http://localhost%s s3 ls", listenAddr)
	log.Printf("  aws --endpoint-url http://localhost%s s3 ls s3://BUCKET/", listenAddr)
	log.Printf("  aws --endpoint-url http://localhost%s s3 cp s3://BUCKET/file.txt ./", listenAddr)

	server := &http.Server{
		Addr:    listenAddr,
		Handler: faker.Server(),
	}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal("Server error:", err)
	}
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "MinIO Unfuck - Read-only S3 server for MinIO erasure-coded data\n\n")
		fmt.Fprintf(os.Stderr, "Modes:\n")
		fmt.Fprintf(os.Stderr, "  Directory mode (default): Use mounted disk directories\n")
		fmt.Fprintf(os.Stderr, "    %s -root /path/to/disks\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  Raw disk mode: Read directly from block devices (XFS only)\n")
		fmt.Fprintf(os.Stderr, "    %s -disk /dev/sde -disk /dev/sdf\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}
}
