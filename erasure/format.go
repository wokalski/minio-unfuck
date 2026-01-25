package erasure

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// DiskFormat represents the format.json structure from MinIO
type DiskFormat struct {
	Version string `json:"version"`
	Format  string `json:"format"`
	ID      string `json:"id"` // Pool ID
	XL      struct {
		Version          string     `json:"version"`
		This             string     `json:"this"`
		Sets             [][]string `json:"sets"`
		DistributionAlgo string     `json:"distributionAlgo"`
	} `json:"xl"`
}

// DiskInfo contains information about a discovered disk
type DiskInfo struct {
	Path      string // Filesystem path to the disk
	UUID      string // Disk UUID from format.json
	PoolIndex int    // Which pool (0, 1, ...)
	SetIndex  int    // Which erasure set within the pool
	DiskIndex int    // Position within the erasure set (0-based)
	PoolID    string // Pool UUID
}

// PoolConfig contains the discovered pool configuration
type PoolConfig struct {
	PoolID    string       // Pool UUID
	PoolIndex int          // Index of this pool (0, 1, ...)
	Sets      [][]DiskInfo // Disks organized by set
	SetCount  int          // Number of erasure sets
}

// ClusterConfig contains all discovered pools
type ClusterConfig struct {
	Pools []*PoolConfig
}

// TotalSets returns total number of sets across all pools
func (c *ClusterConfig) TotalSets() int {
	total := 0
	for _, pool := range c.Pools {
		total += pool.SetCount
	}
	return total
}

// DiscoverCluster reads format.json from all disks and groups them by pool
func DiscoverCluster(diskPaths []string) (*ClusterConfig, error) {
	if len(diskPaths) == 0 {
		return nil, fmt.Errorf("no disk paths provided")
	}

	// Read format.json from each disk
	type diskData struct {
		format DiskFormat
		path   string
	}
	var disks []diskData

	for _, path := range diskPaths {
		formatPath := filepath.Join(path, ".minio.sys", "format.json")
		data, err := os.ReadFile(formatPath)
		if err != nil {
			continue
		}

		var format DiskFormat
		if err := json.Unmarshal(data, &format); err != nil {
			continue
		}

		disks = append(disks, diskData{format: format, path: path})
	}

	if len(disks) == 0 {
		return nil, fmt.Errorf("no valid format.json found in any disk")
	}

	// Group disks by pool ID
	poolDisks := make(map[string][]diskData)
	poolOrder := []string{} // Preserve discovery order

	for _, d := range disks {
		poolID := d.format.ID
		if _, exists := poolDisks[poolID]; !exists {
			poolOrder = append(poolOrder, poolID)
		}
		poolDisks[poolID] = append(poolDisks[poolID], d)
	}

	// Build cluster config
	cluster := &ClusterConfig{
		Pools: make([]*PoolConfig, len(poolOrder)),
	}

	for poolIdx, poolID := range poolOrder {
		disksInPool := poolDisks[poolID]

		// Get sets configuration from first disk in pool
		sets := disksInPool[0].format.XL.Sets
		if len(sets) == 0 {
			return nil, fmt.Errorf("pool %s has no erasure sets", poolID)
		}

		// Build UUID to path mapping for this pool
		uuidToPath := make(map[string]string)
		for _, d := range disksInPool {
			uuidToPath[d.format.XL.This] = d.path
		}

		// Build pool config
		pool := &PoolConfig{
			PoolID:    poolID,
			PoolIndex: poolIdx,
			SetCount:  len(sets),
			Sets:      make([][]DiskInfo, len(sets)),
		}

		for setIdx, set := range sets {
			pool.Sets[setIdx] = make([]DiskInfo, len(set))
			for diskIdx, uuid := range set {
				path := uuidToPath[uuid]
				pool.Sets[setIdx][diskIdx] = DiskInfo{
					Path:      path,
					UUID:      uuid,
					PoolIndex: poolIdx,
					SetIndex:  setIdx,
					DiskIndex: diskIdx,
					PoolID:    poolID,
				}
			}
		}

		cluster.Pools[poolIdx] = pool
	}

	return cluster, nil
}

// DiscoverClusterInDirectory finds all storage directories and returns cluster configuration
func DiscoverClusterInDirectory(rootDir string) (*ClusterConfig, error) {
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
		formatPath := filepath.Join(path, ".minio.sys", "format.json")
		if _, err := os.Stat(formatPath); err == nil {
			diskPaths = append(diskPaths, path)
		}
	}

	if len(diskPaths) == 0 {
		return nil, fmt.Errorf("no MinIO disks found in %s", rootDir)
	}

	return DiscoverCluster(diskPaths)
}

// Legacy functions for backwards compatibility

// DiscoverPool returns the first pool only (deprecated, use DiscoverCluster)
func DiscoverPool(diskPaths []string) (*PoolConfig, error) {
	cluster, err := DiscoverCluster(diskPaths)
	if err != nil {
		return nil, err
	}
	if len(cluster.Pools) == 0 {
		return nil, fmt.Errorf("no pools found")
	}
	return cluster.Pools[0], nil
}

// DiscoverPoolInDirectory returns the first pool only (deprecated)
func DiscoverPoolInDirectory(rootDir string) (*PoolConfig, error) {
	cluster, err := DiscoverClusterInDirectory(rootDir)
	if err != nil {
		return nil, err
	}
	if len(cluster.Pools) == 0 {
		return nil, fmt.Errorf("no pools found")
	}
	return cluster.Pools[0], nil
}

// DiscoverDisks returns the first set of the first pool (deprecated)
func DiscoverDisks(diskPaths []string) ([]DiskInfo, error) {
	pool, err := DiscoverPool(diskPaths)
	if err != nil {
		return nil, err
	}
	if len(pool.Sets) == 0 {
		return nil, fmt.Errorf("no sets found")
	}
	return pool.Sets[0], nil
}

// DiscoverDisksInDirectory returns the first set of the first pool (deprecated)
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

// GetOrderedDiskPaths returns disk paths from a slice of DiskInfo
func GetOrderedDiskPaths(diskInfos []DiskInfo) []string {
	paths := make([]string, len(diskInfos))
	for i, info := range diskInfos {
		paths[i] = info.Path
	}
	return paths
}

// DiskMapping maps disk UUIDs to partition indices for raw disk access
type DiskMapping struct {
	UUIDToPartition map[string]int // disk UUID -> partition index
	PartitionToUUID map[int]string // partition index -> disk UUID
	DiskOrder       []string       // ordered list of disk UUIDs from format.json
	PoolID          string         // pool ID
}

// GetPartitionForDiskIndex returns the partition index for a given disk index in the distribution
func (dm *DiskMapping) GetPartitionForDiskIndex(diskIndex int) (int, bool) {
	if diskIndex < 0 || diskIndex >= len(dm.DiskOrder) {
		return 0, false
	}
	uuid := dm.DiskOrder[diskIndex]
	partIdx, ok := dm.UUIDToPartition[uuid]
	return partIdx, ok
}

// BuildDiskMappingFromRaw reads format.json from raw partitions and builds the disk mapping
func BuildDiskMappingFromRaw(partitions []*RawFS) (*DiskMapping, error) {
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions provided")
	}

	dm := &DiskMapping{
		UUIDToPartition: make(map[string]int),
		PartitionToUUID: make(map[int]string),
	}

	// Read format.json from each partition
	for partIdx, part := range partitions {
		if part == nil {
			continue // Skip failed partitions
		}
		format, err := readFormatFromFS(part.FS())
		if err != nil {
			// Skip partitions without format.json
			continue
		}

		// Store this partition's UUID
		diskUUID := format.XL.This
		dm.UUIDToPartition[diskUUID] = partIdx
		dm.PartitionToUUID[partIdx] = diskUUID

		// Use the first partition's format.json to get the disk order
		if dm.DiskOrder == nil && len(format.XL.Sets) > 0 {
			dm.DiskOrder = format.XL.Sets[0]
			dm.PoolID = format.ID
		}
	}

	if dm.DiskOrder == nil {
		return nil, fmt.Errorf("no valid format.json found in any partition")
	}

	return dm, nil
}

// readFormatFromFS reads format.json from an fs.FS
func readFormatFromFS(fsys fs.FS) (*DiskFormat, error) {
	f, err := fsys.Open(".minio.sys/format.json")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var format DiskFormat
	if err := json.NewDecoder(f).Decode(&format); err != nil {
		return nil, err
	}
	return &format, nil
}
