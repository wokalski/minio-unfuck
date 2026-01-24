// Package erasure provides Reed-Solomon erasure coding for MinIO data
package erasure

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/minio/highwayhash"
)

const (
	// HashSize is the size of HighwayHash256 checksum
	HashSize = 32
)

// MagicHighwayHash256Key is MinIO's fixed key for HighwayHash
var MagicHighwayHash256Key = []byte{
	0x4b, 0xe7, 0x34, 0xfa, 0x8e, 0x23, 0x8a, 0xcd,
	0x26, 0x3e, 0x83, 0xe6, 0xbb, 0x96, 0x85, 0x52,
	0x04, 0x0f, 0x93, 0x5d, 0xa3, 0x9f, 0x44, 0x14,
	0x97, 0xe0, 0x9d, 0x13, 0x22, 0xde, 0x36, 0xa0,
}

// ShardPath returns the path to a shard file
func ShardPath(diskPath, bucket, object, dataDir string, partNumber int) string {
	return filepath.Join(diskPath, bucket, object, dataDir, fmt.Sprintf("part.%d", partNumber))
}

// ReadShard reads a shard from disk
// For single-block files: [32-byte hash][data]
// For multi-block files: each block has [32-byte hash][shardSize data]
//
// Parameters:
//   - path: path to the shard file
//   - blockIndex: which block to read (0-based)
//   - shardSize: size of data per shard per block (without hash)
//   - verifyBitrot: whether to verify the HighwayHash256 checksum
//
// Returns the shard data (without hash prefix) or error
func ReadShard(path string, blockIndex int, shardSize int64, verifyBitrot bool) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Shard missing, will need reconstruction
		}
		return nil, fmt.Errorf("open shard: %w", err)
	}
	defer file.Close()

	// Get file size to determine actual data size
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat shard: %w", err)
	}

	fileSize := stat.Size()
	if fileSize <= HashSize {
		return nil, fmt.Errorf("shard too small: %d bytes", fileSize)
	}

	// Calculate offset for this block
	// Each block has: [32-byte hash][shardSize data]
	blockOffset := int64(blockIndex) * (HashSize + shardSize)

	// For the last block, data might be smaller
	remainingFile := fileSize - blockOffset
	if remainingFile <= HashSize {
		return nil, nil // No data for this block
	}

	// Actual data size in this block
	dataSize := remainingFile - HashSize
	if dataSize > shardSize {
		dataSize = shardSize
	}

	// Seek to block start
	if _, err := file.Seek(blockOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to block: %w", err)
	}

	// Read hash
	hashBuf := make([]byte, HashSize)
	if _, err := io.ReadFull(file, hashBuf); err != nil {
		return nil, fmt.Errorf("read hash: %w", err)
	}

	// Read data
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, fmt.Errorf("read data: %w", err)
	}

	// Verify bitrot if requested
	if verifyBitrot {
		h, err := highwayhash.New(MagicHighwayHash256Key)
		if err != nil {
			return nil, fmt.Errorf("create hasher: %w", err)
		}
		h.Write(data)
		computed := h.Sum(nil)

		match := true
		for i := 0; i < HashSize; i++ {
			if hashBuf[i] != computed[i] {
				match = false
				break
			}
		}
		if !match {
			return nil, fmt.Errorf("bitrot detected in block %d", blockIndex)
		}
	}

	return data, nil
}

// ReadShardFile reads an entire shard file, extracting data from all blocks
// Returns all data concatenated (without hash prefixes)
func ReadShardFile(path string, shardSize int64, verifyBitrot bool) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Shard missing
		}
		return nil, fmt.Errorf("open shard: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat shard: %w", err)
	}

	fileSize := stat.Size()
	if fileSize <= HashSize {
		return nil, fmt.Errorf("shard too small: %d bytes", fileSize)
	}

	// For single-block (small) files: [32-byte hash][data]
	// For multi-block files: each block has [32-byte hash][shardSize data]

	// Calculate number of blocks
	// If file <= HashSize + shardSize, it's a single block
	blockWithHash := HashSize + shardSize
	numBlocks := (fileSize + blockWithHash - 1) / blockWithHash
	if numBlocks == 0 {
		numBlocks = 1
	}

	var result []byte
	var h interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
		Reset()
	}

	if verifyBitrot {
		h, err = highwayhash.New(MagicHighwayHash256Key)
		if err != nil {
			return nil, fmt.Errorf("create hasher: %w", err)
		}
	}

	hashBuf := make([]byte, HashSize)
	dataBuf := make([]byte, shardSize)

	for block := int64(0); block < numBlocks; block++ {
		offset := block * blockWithHash
		remaining := fileSize - offset

		if remaining <= HashSize {
			break // No more complete blocks
		}

		// Read hash
		if _, err := file.ReadAt(hashBuf, offset); err != nil {
			return nil, fmt.Errorf("read hash at block %d: %w", block, err)
		}

		// Calculate data size for this block
		dataSize := remaining - HashSize
		if dataSize > shardSize {
			dataSize = shardSize
		}

		// Read data
		data := dataBuf[:dataSize]
		if _, err := file.ReadAt(data, offset+HashSize); err != nil {
			return nil, fmt.Errorf("read data at block %d: %w", block, err)
		}

		// Verify bitrot
		if verifyBitrot {
			h.Reset()
			h.Write(data)
			computed := h.Sum(nil)

			match := true
			for i := 0; i < HashSize; i++ {
				if hashBuf[i] != computed[i] {
					match = false
					break
				}
			}
			if !match {
				return nil, fmt.Errorf("bitrot detected in block %d", block)
			}
		}

		result = append(result, data...)
	}

	return result, nil
}
