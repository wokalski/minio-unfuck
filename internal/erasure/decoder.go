package erasure

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/reedsolomon"

	"github.com/minio/minio/internal/xlmeta"
)

// Decoder handles erasure decoding for objects
type Decoder struct {
	diskPaths []string // Paths to disk roots
}

// NewDecoder creates a new erasure decoder
func NewDecoder(diskPaths []string) *Decoder {
	return &Decoder{
		diskPaths: diskPaths,
	}
}

// ReadObject reads and reconstructs an erasure-coded object
// Returns the complete object data
func (d *Decoder) ReadObject(bucket, key string) ([]byte, *xlmeta.ObjectMeta, error) {
	// Step 1: Read xl.meta from any available disk
	meta, err := d.readMetadata(bucket, key)
	if err != nil {
		return nil, nil, fmt.Errorf("read metadata: %w", err)
	}

	// Step 2: Read and decode the object
	data, err := d.decodeObject(bucket, key, meta)
	if err != nil {
		return nil, nil, fmt.Errorf("decode object: %w", err)
	}

	return data, meta, nil
}

// ReadObjectWithSkip reads an object while skipping specified disk indices
// Used for testing reconstruction with missing shards
func (d *Decoder) ReadObjectWithSkip(bucket, key string, skipDisks []int) ([]byte, *xlmeta.ObjectMeta, error) {
	meta, err := d.readMetadata(bucket, key)
	if err != nil {
		return nil, nil, fmt.Errorf("read metadata: %w", err)
	}

	data, err := d.decodeObjectWithSkip(bucket, key, meta, skipDisks)
	if err != nil {
		return nil, nil, fmt.Errorf("decode object: %w", err)
	}

	return data, meta, nil
}

// ReadObjectWithMeta reads an object using pre-parsed metadata
// This is more efficient when metadata is already available (e.g., from SQLite cache)
func (d *Decoder) ReadObjectWithMeta(bucket, key string, meta *xlmeta.ObjectMeta) ([]byte, error) {
	return d.decodeObject(bucket, key, meta)
}

// readMetadata reads xl.meta from the first available disk
func (d *Decoder) readMetadata(bucket, key string) (*xlmeta.ObjectMeta, error) {
	for i, diskPath := range d.diskPaths {
		metaPath := filepath.Join(diskPath, bucket, key, "xl.meta")
		data, err := os.ReadFile(metaPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			continue // Try next disk
		}

		meta, err := xlmeta.Parse(data)
		if err != nil {
			continue // Try next disk
		}

		meta.Bucket = bucket
		meta.Key = key

		// Log which disk we got metadata from
		_ = i // Could log: fmt.Printf("Read metadata from disk %d\n", i)

		return meta, nil
	}

	return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
}

// decodeObject decodes all parts of an object
func (d *Decoder) decodeObject(bucket, key string, meta *xlmeta.ObjectMeta) ([]byte, error) {
	return d.decodeObjectWithSkip(bucket, key, meta, nil)
}

// decodeObjectWithSkip decodes an object while skipping certain disks
func (d *Decoder) decodeObjectWithSkip(bucket, key string, meta *xlmeta.ObjectMeta, skipDisks []int) ([]byte, error) {
	// Build skip map
	skipMap := make(map[int]bool)
	for _, idx := range skipDisks {
		skipMap[idx] = true
	}

	var result []byte

	// Process each part
	for _, part := range meta.Parts {
		partData, err := d.decodePart(bucket, key, meta, part, skipMap)
		if err != nil {
			return nil, fmt.Errorf("decode part %d: %w", part.Number, err)
		}
		result = append(result, partData...)
	}

	// Trim to actual size (in case last shard has padding)
	if int64(len(result)) > meta.Size {
		result = result[:meta.Size]
	}

	return result, nil
}

// decodePart decodes a single part of an object
func (d *Decoder) decodePart(bucket, key string, meta *xlmeta.ObjectMeta, part xlmeta.PartMeta, skipMap map[int]bool) ([]byte, error) {
	dataDir := meta.DataDirString()
	shardSize := meta.ShardSize()

	// Calculate number of blocks in this part
	numBlocks := (part.Size + meta.BlockSize - 1) / meta.BlockSize
	if numBlocks == 0 {
		numBlocks = 1
	}

	var result []byte

	// Process block by block
	for block := int64(0); block < numBlocks; block++ {
		blockData, err := d.decodeBlock(bucket, key, dataDir, part.Number, block, meta, shardSize, skipMap)
		if err != nil {
			return nil, fmt.Errorf("decode block %d: %w", block, err)
		}
		result = append(result, blockData...)
	}

	// Trim to part size
	if int64(len(result)) > part.Size {
		result = result[:part.Size]
	}

	return result, nil
}

// decodeBlock decodes a single block of a part
func (d *Decoder) decodeBlock(bucket, key, dataDir string, partNum int, blockIdx int64, meta *xlmeta.ObjectMeta, shardSize int64, skipMap map[int]bool) ([]byte, error) {
	dataBlocks := meta.DataBlocks
	parityBlocks := meta.ParityBlocks
	totalShards := dataBlocks + parityBlocks

	// Create Reed-Solomon encoder/decoder
	enc, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return nil, fmt.Errorf("create RS encoder: %w", err)
	}

	// Read shards in parallel
	shards := make([][]byte, totalShards)
	errors := make([]error, totalShards)
	var wg sync.WaitGroup

	// Build reverse mapping: shardIdx -> diskIdx
	// Distribution[diskIdx] = erasureIndex (1-based shard number)
	// So we need to find which diskIdx has Distribution[diskIdx] = shardIdx+1
	shardToDisk := make(map[int]int)
	for diskIdx, erasureIdx := range meta.Distribution {
		shardIdx := erasureIdx - 1 // Convert 1-based to 0-based
		shardToDisk[shardIdx] = diskIdx
	}

	for shardIdx := 0; shardIdx < totalShards; shardIdx++ {
		diskIdx, ok := shardToDisk[shardIdx]
		if !ok {
			errors[shardIdx] = fmt.Errorf("shard %d: no distribution mapping", shardIdx)
			continue
		}

		if diskIdx < 0 || diskIdx >= len(d.diskPaths) {
			errors[shardIdx] = fmt.Errorf("shard %d: invalid disk index %d", shardIdx, diskIdx)
			continue
		}

		// Skip if in skip map
		if skipMap[diskIdx] {
			// Leave shard as nil for reconstruction
			continue
		}

		wg.Add(1)
		go func(shardIdx, diskIdx int) {
			defer wg.Done()

			shardPath := ShardPath(d.diskPaths[diskIdx], bucket, key, dataDir, partNum)
			data, err := ReadShard(shardPath, int(blockIdx), shardSize, true)
			if err != nil {
				errors[shardIdx] = err
				return
			}
			shards[shardIdx] = data
		}(shardIdx, diskIdx)
	}

	wg.Wait()

	// Count available shards
	available := 0
	for i, shard := range shards {
		if shard != nil && len(shard) > 0 {
			available++
		} else if errors[i] != nil {
			// Log error but continue (might be able to reconstruct)
			_ = errors[i]
		}
	}

	if available < dataBlocks {
		return nil, fmt.Errorf("insufficient shards: have %d, need %d", available, dataBlocks)
	}

	// Normalize shard sizes (all data shards should be same size)
	// Find the max shard size
	maxSize := int64(0)
	for _, shard := range shards[:dataBlocks] {
		if shard != nil && int64(len(shard)) > maxSize {
			maxSize = int64(len(shard))
		}
	}

	// Pad shorter shards (for last block which might have smaller shards)
	for i := range shards {
		if shards[i] != nil && int64(len(shards[i])) < maxSize {
			padded := make([]byte, maxSize)
			copy(padded, shards[i])
			shards[i] = padded
		}
	}

	// Check if reconstruction is needed
	needsReconstruction := false
	for i := 0; i < totalShards; i++ {
		if shards[i] == nil || len(shards[i]) == 0 {
			needsReconstruction = true
			// Allocate space for reconstruction
			if maxSize > 0 {
				shards[i] = make([]byte, maxSize)
			}
		}
	}

	if needsReconstruction {
		// Only reconstruct data blocks (we don't need parity)
		if err := enc.ReconstructData(shards); err != nil {
			return nil, fmt.Errorf("reconstruction failed: %w", err)
		}
	}

	// Concatenate data blocks (first dataBlocks shards)
	var blockData []byte
	for i := 0; i < dataBlocks; i++ {
		blockData = append(blockData, shards[i]...)
	}

	return blockData, nil
}

// WriteObject writes decoded object data to a writer
func (d *Decoder) WriteObject(bucket, key string, w io.Writer) error {
	data, _, err := d.ReadObject(bucket, key)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// WriteObjectToFile writes decoded object data to a file
func (d *Decoder) WriteObjectToFile(bucket, key, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer file.Close()

	return d.WriteObject(bucket, key, file)
}
