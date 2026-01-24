// test-decoder is a standalone test for the erasure decoder
// It reads a test object and verifies it can be reconstructed correctly
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/wokalski/minio-unfuck/erasure"
)

func main() {
	// Flags
	rootDir := flag.String("root", ".disks", "Root directory containing disk folders")
	bucket := flag.String("bucket", "recordings", "Bucket name")
	key := flag.String("key", "12e6r9jZSmuSQjNj2rUSOx.wav", "Object key")
	output := flag.String("output", "/tmp/restored.wav", "Output file path")
	skipDisks := flag.String("skip", "", "Comma-separated disk indices to skip (0-based)")
	flag.Parse()

	// Discover disks from format.json
	fmt.Printf("Discovering disks in %s...\n", *rootDir)
	diskInfos, err := erasure.DiscoverDisksInDirectory(*rootDir)
	if err != nil {
		log.Fatalf("Failed to discover disks: %v", err)
	}

	diskPaths := erasure.GetOrderedDiskPaths(diskInfos)

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Disks discovered: %d\n", len(diskInfos))
	fmt.Printf("  Pool ID: %s\n", diskInfos[0].PoolID)
	fmt.Printf("  Disk order (by erasure set position):\n")
	for i, info := range diskInfos {
		if info.Path != "" {
			fmt.Printf("    [%2d] %s (uuid: %s...)\n", i, info.Path, info.UUID[:8])
		} else {
			fmt.Printf("    [%2d] <missing> (uuid: %s...)\n", i, info.UUID[:8])
		}
	}
	fmt.Printf("  Object: s3://%s/%s\n", *bucket, *key)
	fmt.Printf("  Output: %s\n", *output)

	// Parse skip disks
	var skip []int
	if *skipDisks != "" {
		for _, s := range strings.Split(*skipDisks, ",") {
			var idx int
			if _, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &idx); err == nil {
				skip = append(skip, idx)
			}
		}
		fmt.Printf("  Skipping disks: %v\n", skip)
	}
	fmt.Println()

	// Create decoder
	decoder := erasure.NewDecoder(diskPaths)

	// Read and decode object
	var data []byte
	var meta interface{}

	if len(skip) > 0 {
		data, meta, err = decoder.ReadObjectWithSkip(*bucket, *key, skip)
	} else {
		data, meta, err = decoder.ReadObject(*bucket, *key)
	}

	if err != nil {
		log.Fatalf("Failed to decode object: %v", err)
	}

	fmt.Printf("Decoded object:\n")
	fmt.Printf("  Size: %d bytes\n", len(data))

	// Print metadata if available
	if m, ok := meta.(interface{ DataDirString() string }); ok {
		fmt.Printf("  DataDir: %s\n", m.DataDirString())
	}

	// Write to output file
	if err := os.WriteFile(*output, data, 0644); err != nil {
		log.Fatalf("Failed to write output: %v", err)
	}

	fmt.Printf("  Written to: %s\n", *output)
	fmt.Println()

	// Verification hint
	fmt.Println("Verify with ffprobe:")
	fmt.Printf("  nix shell nixpkgs#ffmpeg -c ffprobe %s\n", *output)
	fmt.Println()
	fmt.Println("Expected output:")
	fmt.Println("  Duration: 00:00:11.28, bitrate: 128 kb/s")
	fmt.Println("  Stream #0:0: Audio: pcm_alaw, 8000 Hz, 2 channels, s16, 128 kb/s")
}
