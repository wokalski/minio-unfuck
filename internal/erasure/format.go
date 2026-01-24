package erasure

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// DiskFormat represents the format.json structure from MinIO
type DiskFormat struct {
	Version string `json:"version"`
	Format  string `json:"format"`
	ID      string `json:"id"`
	XL      struct {
		Version          string     `json:"version"`
		This             string     `json:"this"`
		Sets             [][]string `json:"sets"`
		DistributionAlgo string     `json:"distributionAlgo"`
	} `json:"xl"`
}

// DiskInfo contains information about a discovered disk
type DiskInfo struct {
	Path       string // Filesystem path to the disk
	UUID       string // Disk UUID from format.json
	SetIndex   int    // Which erasure set this disk belongs to
	DiskIndex  int    // Position within the erasure set (0-based)
	PoolID     string // Pool ID (same for all disks in a pool)
}

// DiscoverDisks reads format.json from each disk path and returns ordered disk info
// The returned slice is ordered by logical position in the erasure set
func DiscoverDisks(diskPaths []string) ([]DiskInfo, error) {
	if len(diskPaths) == 0 {
		return nil, fmt.Errorf("no disk paths provided")
	}

	// Read format.json from each disk
	var formats []DiskFormat
	var validPaths []string

	for _, path := range diskPaths {
		formatPath := filepath.Join(path, ".minio.sys", "format.json")
		data, err := os.ReadFile(formatPath)
		if err != nil {
			continue // Skip disks without format.json
		}

		var format DiskFormat
		if err := json.Unmarshal(data, &format); err != nil {
			continue // Skip invalid format.json
		}

		formats = append(formats, format)
		validPaths = append(validPaths, path)
	}

	if len(formats) == 0 {
		return nil, fmt.Errorf("no valid format.json found in any disk")
	}

	// Use the first format to get the set configuration
	// All disks should have the same sets array
	sets := formats[0].XL.Sets
	if len(sets) == 0 {
		return nil, fmt.Errorf("no erasure sets found in format.json")
	}

	// For now, assume single set (set 0)
	// TODO: Support multiple sets if needed
	set := sets[0]

	// Build UUID to path mapping
	uuidToPath := make(map[string]string)
	for i, format := range formats {
		uuidToPath[format.XL.This] = validPaths[i]
	}

	// Build ordered disk info based on set order
	diskInfos := make([]DiskInfo, len(set))
	for i, uuid := range set {
		path, ok := uuidToPath[uuid]
		if !ok {
			// Disk not found - leave empty, decoder will handle missing disks
			diskInfos[i] = DiskInfo{
				Path:      "",
				UUID:      uuid,
				SetIndex:  0,
				DiskIndex: i,
				PoolID:    formats[0].ID,
			}
			continue
		}

		diskInfos[i] = DiskInfo{
			Path:      path,
			UUID:      uuid,
			SetIndex:  0,
			DiskIndex: i,
			PoolID:    formats[0].ID,
		}
	}

	return diskInfos, nil
}

// DiscoverDisksInDirectory finds all storage directories in a root directory
// and discovers their configuration
func DiscoverDisksInDirectory(rootDir string) ([]DiskInfo, error) {
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, fmt.Errorf("read root directory: %w", err)
	}

	var diskPaths []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		path := filepath.Join(rootDir, entry.Name())
		// Check if this looks like a MinIO disk (has .minio.sys/format.json)
		formatPath := filepath.Join(path, ".minio.sys", "format.json")
		if _, err := os.Stat(formatPath); err == nil {
			diskPaths = append(diskPaths, path)
		}
	}

	if len(diskPaths) == 0 {
		return nil, fmt.Errorf("no MinIO disks found in %s", rootDir)
	}

	return DiscoverDisks(diskPaths)
}

// GetOrderedDiskPaths returns disk paths in the correct erasure set order
func GetOrderedDiskPaths(diskInfos []DiskInfo) []string {
	paths := make([]string, len(diskInfos))
	for i, info := range diskInfos {
		paths[i] = info.Path
	}
	return paths
}
