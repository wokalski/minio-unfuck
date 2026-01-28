package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/wokalski/minio-unfuck/erasure"
)

type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }
func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var diskPaths stringSlice
	flag.Var(&diskPaths, "disk", "Raw disk path (e.g., /dev/sde). Can be specified multiple times.")
	flag.Parse()

	if len(diskPaths) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: quicktest -disk /dev/sde\n")
		os.Exit(1)
	}

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

	totalStart := time.Now()
	var totalKeys int64

	for partIdx, partPath := range allPartitions {
		partStart := time.Now()

		rawFS, err := erasure.OpenRawFS(partPath)
		if err != nil {
			log.Printf("[Part %d] Failed to open %s: %v", partIdx+1, partPath, err)
			continue
		}

		var partKeys int64
		for range rawFS.All() {
			partKeys++
		}

		totalKeys += partKeys
		rawFS.Close()
		log.Printf("[Partition %d/%d] %d keys in %v", partIdx+1, len(allPartitions), partKeys, time.Since(partStart))
	}

	log.Printf("\nQuick test complete: %d total keys across %d partitions in %v",
		totalKeys, len(allPartitions), time.Since(totalStart))
	log.Printf("Speed: %.0f keys/sec", float64(totalKeys)/time.Since(totalStart).Seconds())
}
