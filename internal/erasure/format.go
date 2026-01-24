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

// PoolConfig contains the discovered pool configuration
type PoolConfig struct {
	PoolID   string       // Pool UUID
	Sets     [][]DiskInfo // Disks organized by set
	SetCount int          // Number of erasure sets
}

// DiscoverDisks reads format.json from each disk path and returns ordered disk info
// The returned slice is ordered by logical position in the erasure set
// DEPRECATED: Use DiscoverPool for multi-set support
func DiscoverDisks(diskPaths []string) ([]DiskInfo, error) {
	pool, err := DiscoverPool(diskPaths)
	if err != nil {
		return nil, err
	}
	// Return first set for backwards compatibility
	if len(pool.Sets) == 0 {
		return nil, fmt.Errorf("no sets found")
	}
	return pool.Sets[0], nil
}

// DiscoverPool reads format.json from each disk path and returns full pool configuration
func DiscoverPool(diskPaths []string) (*PoolConfig, error) {
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

	// Build UUID to path mapping
	uuidToPath := make(map[string]string)
	for i, format := range formats {
		uuidToPath[format.XL.This] = validPaths[i]
	}

	// Build pool config with all sets
	pool := &PoolConfig{
		PoolID:   formats[0].ID,
		SetCount: len(sets),
		Sets:     make([][]DiskInfo, len(sets)),
	}

	for setIdx, set := range sets {
		pool.Sets[setIdx] = make([]DiskInfo, len(set))
		for diskIdx, uuid := range set {
			path := uuidToPath[uuid] // Empty string if not found
			pool.Sets[setIdx][diskIdx] = DiskInfo{
				Path:      path,
				UUID:      uuid,
				SetIndex:  setIdx,
				DiskIndex: diskIdx,
				PoolID:    formats[0].ID,
			}
		}
	}

	return pool, nil
}

// DiscoverDisksInDirectory finds all storage directories in a root directory
// and discovers their configuration
// DEPRECATED: Use DiscoverPoolInDirectory for multi-set support
func DiscoverDisksInDirectory(rootDir string) ([]DiskInfo, error) {
	pool, err := DiscoverPoolInDirectory(rootDir)
	if err != nil {
		return nil, err
	}
	if len(pool.Sets) == 0 {
		return nil, fmt.Errorf("no sets found")
	}
	return pool.Sets[0], nil
}

// DiscoverPoolInDirectory finds all storage directories and returns full pool configuration
func DiscoverPoolInDirectory(rootDir string) (*PoolConfig, error) {
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

	return DiscoverPool(diskPaths)
}

// GetOrderedDiskPaths returns disk paths in the correct erasure set order
func GetOrderedDiskPaths(diskInfos []DiskInfo) []string {
	paths := make([]string, len(diskInfos))
	for i, info := range diskInfos {
		paths[i] = info.Path
	}
	return paths
}
