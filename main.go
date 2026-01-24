package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/johannesboyne/gofakes3"

	"github.com/wokalski/minio-unfuck/backend"
	"github.com/wokalski/minio-unfuck/erasure"
	"github.com/wokalski/minio-unfuck/metadata"
)

func main() {
	// Flags
	rootDir := flag.String("root", ".disks", "Root directory containing disk folders")
	dbPath := flag.String("db", "metadata.db", "Path to SQLite database")
	listenAddr := flag.String("addr", ":9000", "Address to listen on")
	syncOnStart := flag.Bool("sync", true, "Sync metadata on startup")
	flag.Parse()

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

	// Discover disks and pool configuration
	log.Printf("Discovering disks in %s...", *rootDir)
	pool, err := erasure.DiscoverPoolInDirectory(*rootDir)
	if err != nil {
		log.Fatalf("Failed to discover disks: %v", err)
	}

	log.Printf("Discovered pool %s with %d erasure set(s)", pool.PoolID, pool.SetCount)
	for setIdx, set := range pool.Sets {
		available := 0
		for _, disk := range set {
			if disk.Path != "" {
				available++
			}
		}
		log.Printf("  Set %d: %d/%d disks available", setIdx, available, len(set))
	}

	// Initialize SQLite store
	log.Printf("Opening metadata database: %s", *dbPath)
	store, err := metadata.NewStore(*dbPath)
	if err != nil {
		log.Fatalf("Failed to open metadata store: %v", err)
	}
	defer store.Close()

	// Sync metadata if requested - sync from one disk per set
	if *syncOnStart {
		log.Println("Syncing metadata from disk...")

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
				log.Printf("Warning: no available disks in set %d, skipping sync", setIdx)
				continue
			}

			syncer := metadata.NewSyncer(store, metadata.SyncConfig{
				DiskPath:    syncDisk,
				SetIndex:    setIdx,
				BatchSize:   1000,
				ProgressLog: true,
			})

			result, err := syncer.Sync(ctx)
			if err != nil {
				log.Printf("Warning: sync failed for set %d: %v", setIdx, err)
				continue
			}

			log.Printf("Set %d sync complete: %d buckets, %d objects (%d errors) in %v",
				setIdx, result.BucketsFound, result.ObjectsSynced, result.Errors, result.Duration)
		}
	}

	// Count objects in cache
	count, err := store.ObjectCount(ctx)
	if err != nil {
		log.Printf("Warning: failed to count objects: %v", err)
	} else {
		log.Printf("Metadata cache contains %d objects", count)
	}

	// Create one decoder per set
	decoders := make([]*erasure.Decoder, pool.SetCount)
	for setIdx, set := range pool.Sets {
		diskPaths := make([]string, len(set))
		for i, info := range set {
			diskPaths[i] = info.Path
		}
		decoders[setIdx] = erasure.NewDecoder(diskPaths)
	}

	// Create backend
	be := backend.NewMultiSet(store, decoders)

	// Create gofakes3 server
	faker := gofakes3.New(be)

	log.Printf("Starting S3 server on %s", *listenAddr)
	log.Println("Example usage:")
	log.Printf("  aws --endpoint-url http://localhost%s s3 ls", *listenAddr)
	log.Printf("  aws --endpoint-url http://localhost%s s3 ls s3://BUCKET/", *listenAddr)
	log.Printf("  aws --endpoint-url http://localhost%s s3 cp s3://BUCKET/file.txt ./", *listenAddr)

	server := &http.Server{
		Addr:    *listenAddr,
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
