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

	"github.com/minio/minio/internal/backend"
	"github.com/minio/minio/internal/erasure"
	"github.com/minio/minio/internal/metadata"
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

	// Discover disks
	log.Printf("Discovering disks in %s...", *rootDir)
	diskInfos, err := erasure.DiscoverDisksInDirectory(*rootDir)
	if err != nil {
		log.Fatalf("Failed to discover disks: %v", err)
	}

	diskPaths := erasure.GetOrderedDiskPaths(diskInfos)
	log.Printf("Discovered %d disks in pool %s", len(diskInfos), diskInfos[0].PoolID)

	// Initialize SQLite store
	log.Printf("Opening metadata database: %s", *dbPath)
	store, err := metadata.NewStore(*dbPath)
	if err != nil {
		log.Fatalf("Failed to open metadata store: %v", err)
	}
	defer store.Close()

	// Sync metadata if requested
	if *syncOnStart {
		log.Println("Syncing metadata from disk...")

		// Use first disk for sync (xl.meta is identical on all disks in a set)
		var syncDisk string
		for _, info := range diskInfos {
			if info.Path != "" {
				syncDisk = info.Path
				break
			}
		}

		if syncDisk == "" {
			log.Fatal("No available disks found for sync")
		}

		syncer := metadata.NewSyncer(store, metadata.SyncConfig{
			DiskPath:    syncDisk,
			BatchSize:   1000,
			ProgressLog: true,
		})

		result, err := syncer.Sync(ctx)
		if err != nil {
			log.Fatalf("Sync failed: %v", err)
		}

		log.Printf("Sync complete: %d buckets, %d objects synced (%d errors) in %v",
			result.BucketsFound, result.ObjectsSynced, result.Errors, result.Duration)
	}

	// Count objects in cache
	count, err := store.ObjectCount(ctx)
	if err != nil {
		log.Printf("Warning: failed to count objects: %v", err)
	} else {
		log.Printf("Metadata cache contains %d objects", count)
	}

	// Create decoder
	decoder := erasure.NewDecoder(diskPaths)

	// Create backend
	be := backend.New(store, decoder)

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
