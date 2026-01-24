// Package xlmeta provides parsing for MinIO's xl.meta format
package xlmeta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/tinylib/msgp/msgp"
)

var xlHeader = [4]byte{'X', 'L', '2', ' '}

// ObjectMeta contains the metadata needed to read an erasure-coded object
type ObjectMeta struct {
	// Object identification
	Bucket string
	Key    string

	// Version info
	VersionID [16]byte
	DataDir   [16]byte // Directory containing part files

	// Erasure configuration
	DataBlocks   int   // Number of data shards (e.g., 11)
	ParityBlocks int   // Number of parity shards (e.g., 5)
	BlockSize    int64 // Block size (e.g., 1 MiB)
	ErasureIndex int   // This disk's shard index (1-based)
	Distribution []int // Maps shard index to disk index

	// Parts info
	Parts []PartMeta

	// Object metadata
	Size        int64
	ModTime     time.Time
	ETag        string
	ContentType string
	UserMeta    map[string]string
}

// PartMeta represents a single part of a multipart object
type PartMeta struct {
	Number     int
	Size       int64
	ActualSize int64 // For compressed parts
}

// DataDirString returns the DataDir as a UUID string
func (m *ObjectMeta) DataDirString() string {
	return uuidString(m.DataDir)
}

// ShardSize returns the size of each shard for a given block
func (m *ObjectMeta) ShardSize() int64 {
	return ceilDiv(m.BlockSize, int64(m.DataBlocks))
}

// TotalShards returns the total number of shards
func (m *ObjectMeta) TotalShards() int {
	return m.DataBlocks + m.ParityBlocks
}

func ceilDiv(a, b int64) int64 {
	return (a + b - 1) / b
}

func uuidString(b [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Parse parses an xl.meta file and returns the object metadata
func Parse(data []byte) (*ObjectMeta, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("xl.meta too short: %d bytes", len(data))
	}

	// Check header
	if !bytes.Equal(data[:4], xlHeader[:]) {
		return nil, fmt.Errorf("invalid xl.meta header: expected %v, got %v", xlHeader[:], data[:4])
	}

	// Parse version
	major := binary.LittleEndian.Uint16(data[4:6])
	minor := binary.LittleEndian.Uint16(data[6:8])

	if major != 1 {
		return nil, fmt.Errorf("unsupported xl.meta major version: %d", major)
	}

	payload := data[8:]

	// For v1.3+, format is: [msgpack bytes metadata][uint32 CRC][optional inline data]
	if minor >= 3 {
		return parseV1_3(payload)
	}

	// For older versions
	return nil, fmt.Errorf("xl.meta version %d.%d not supported (need >= 1.3)", major, minor)
}

// parseV1_3 parses xl.meta v1.3 format (indexed)
func parseV1_3(payload []byte) (*ObjectMeta, error) {
	// Read metadata blob (msgpack bytes)
	metaBlob, remaining, err := msgp.ReadBytesZC(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata blob: %w", err)
	}

	// Read and verify CRC
	crc, _, err := msgp.ReadUint32Bytes(remaining)
	if err != nil {
		return nil, fmt.Errorf("failed to read CRC: %w", err)
	}

	expectedCRC := uint32(xxhash.Sum64(metaBlob))
	if crc != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch: expected %x, got %x", expectedCRC, crc)
	}

	// Parse metadata blob
	return parseMetadataBlob(metaBlob)
}

// parseMetadataBlob parses the indexed metadata format (v1.3)
func parseMetadataBlob(blob []byte) (*ObjectMeta, error) {
	var err error

	// Read header version
	_, blob, err = msgp.ReadUint8Bytes(blob)
	if err != nil {
		return nil, fmt.Errorf("failed to read header version: %w", err)
	}

	// Read meta version
	_, blob, err = msgp.ReadUint8Bytes(blob)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta version: %w", err)
	}

	// Read version count
	versions, blob, err := msgp.ReadIntBytes(blob)
	if err != nil {
		return nil, fmt.Errorf("failed to read version count: %w", err)
	}

	if versions <= 0 {
		return nil, fmt.Errorf("no versions found")
	}

	// Read first (latest) version: [header bytes][meta bytes]
	// Skip header bytes (we don't need them for our use case)
	_, blob, err = msgp.ReadBytesZC(blob)
	if err != nil {
		return nil, fmt.Errorf("failed to read version header: %w", err)
	}

	// Read version meta bytes
	verMeta, _, err := msgp.ReadBytesZC(blob)
	if err != nil {
		return nil, fmt.Errorf("failed to read version meta: %w", err)
	}

	// Parse version metadata (it's a msgpack map)
	return parseVersionMeta(verMeta)
}

// parseVersionMeta parses the xlMetaV2Version msgpack map
func parseVersionMeta(data []byte) (*ObjectMeta, error) {
	var err error
	meta := &ObjectMeta{}

	// Read map size
	mapLen, data, err := msgp.ReadMapHeaderBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to read version map header: %w", err)
	}

	var versionType uint8
	var key []byte
	for i := uint32(0); i < mapLen; i++ {
		// Read key
		key, data, err = msgp.ReadStringZC(data)
		if err != nil {
			return nil, fmt.Errorf("failed to read map key: %w", err)
		}

		keyStr := string(key)
		switch keyStr {
		case "Type":
			versionType, data, err = msgp.ReadUint8Bytes(data)
			if err != nil {
				return nil, fmt.Errorf("failed to read Type: %w", err)
			}
		case "V2Obj":
			// Read the nested map for V2Obj
			_, data, err = parseV2ObjInline(data, meta)
			if err != nil {
				return nil, fmt.Errorf("failed to parse V2Obj: %w", err)
			}
		default:
			// Skip unknown fields
			data, err = msgp.Skip(data)
			if err != nil {
				return nil, fmt.Errorf("failed to skip field %s: %w", keyStr, err)
			}
		}
	}

	if versionType != 1 {
		return nil, fmt.Errorf("not an object (type=%d)", versionType)
	}

	return meta, nil
}

// parseV2ObjInline parses the xlMetaV2Object msgpack map inline
func parseV2ObjInline(data []byte, meta *ObjectMeta) ([]byte, []byte, error) {
	var err error

	mapLen, data, err := msgp.ReadMapHeaderBytes(data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read V2Obj map header: %w", err)
	}

	var partNumbers []int
	var partSizes []int64
	var partActualSizes []int64

	for i := uint32(0); i < mapLen; i++ {
		key, newData, err := msgp.ReadStringZC(data)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read V2Obj key: %w", err)
		}
		data = newData

		keyStr := string(key)
		switch keyStr {
		case "ID":
			var id []byte
			id, data, err = msgp.ReadBytesZC(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read ID: %w", err)
			}
			if len(id) == 16 {
				copy(meta.VersionID[:], id)
			}
		case "DDir":
			var ddir []byte
			ddir, data, err = msgp.ReadBytesZC(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read DDir: %w", err)
			}
			if len(ddir) == 16 {
				copy(meta.DataDir[:], ddir)
			}
		case "EcAlgo":
			_, data, err = msgp.ReadUint8Bytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read EcAlgo: %w", err)
			}
		case "EcM":
			meta.DataBlocks, data, err = msgp.ReadIntBytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read EcM: %w", err)
			}
		case "EcN":
			meta.ParityBlocks, data, err = msgp.ReadIntBytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read EcN: %w", err)
			}
		case "EcBSize":
			meta.BlockSize, data, err = msgp.ReadInt64Bytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read EcBSize: %w", err)
			}
		case "EcIndex":
			meta.ErasureIndex, data, err = msgp.ReadIntBytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read EcIndex: %w", err)
			}
		case "EcDist":
			var arrLen uint32
			arrLen, data, err = msgp.ReadArrayHeaderBytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read EcDist header: %w", err)
			}
			meta.Distribution = make([]int, arrLen)
			for j := uint32(0); j < arrLen; j++ {
				var v uint8
				v, data, err = msgp.ReadUint8Bytes(data)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to read EcDist[%d]: %w", j, err)
				}
				meta.Distribution[j] = int(v)
			}
		case "CSumAlgo":
			_, data, err = msgp.ReadUint8Bytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read CSumAlgo: %w", err)
			}
		case "PartNums":
			var arrLen uint32
			arrLen, data, err = msgp.ReadArrayHeaderBytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read PartNums header: %w", err)
			}
			partNumbers = make([]int, arrLen)
			for j := uint32(0); j < arrLen; j++ {
				partNumbers[j], data, err = msgp.ReadIntBytes(data)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to read PartNums[%d]: %w", j, err)
				}
			}
		case "PartSizes":
			var arrLen uint32
			arrLen, data, err = msgp.ReadArrayHeaderBytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read PartSizes header: %w", err)
			}
			partSizes = make([]int64, arrLen)
			for j := uint32(0); j < arrLen; j++ {
				partSizes[j], data, err = msgp.ReadInt64Bytes(data)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to read PartSizes[%d]: %w", j, err)
				}
			}
		case "PartASizes":
			var arrLen uint32
			arrLen, data, err = msgp.ReadArrayHeaderBytes(data)
			if err != nil {
				// May be nil/omitted
				data, _ = msgp.Skip(data)
				continue
			}
			partActualSizes = make([]int64, arrLen)
			for j := uint32(0); j < arrLen; j++ {
				partActualSizes[j], data, err = msgp.ReadInt64Bytes(data)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to read PartASizes[%d]: %w", j, err)
				}
			}
		case "Size":
			meta.Size, data, err = msgp.ReadInt64Bytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read Size: %w", err)
			}
		case "MTime":
			var mtime int64
			mtime, data, err = msgp.ReadInt64Bytes(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read MTime: %w", err)
			}
			meta.ModTime = time.Unix(0, mtime)
		case "MetaUsr":
			meta.UserMeta, data, err = parseStringMap(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read MetaUsr: %w", err)
			}
			if ct, ok := meta.UserMeta["content-type"]; ok {
				meta.ContentType = ct
			}
			if etag, ok := meta.UserMeta["etag"]; ok {
				meta.ETag = etag
			}
		default:
			// Skip unknown fields
			data, err = msgp.Skip(data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to skip V2Obj field %s: %w", keyStr, err)
			}
		}
	}

	// Build parts from partNumbers and partSizes
	if len(partNumbers) > 0 {
		meta.Parts = make([]PartMeta, len(partNumbers))
		for i := range partNumbers {
			meta.Parts[i] = PartMeta{
				Number: partNumbers[i],
			}
			if i < len(partSizes) {
				meta.Parts[i].Size = partSizes[i]
			}
			if i < len(partActualSizes) {
				meta.Parts[i].ActualSize = partActualSizes[i]
			} else {
				meta.Parts[i].ActualSize = meta.Parts[i].Size
			}
		}
	}

	return nil, data, nil
}

// parseStringMap parses a msgpack map[string]string
func parseStringMap(data []byte) (map[string]string, []byte, error) {
	mapLen, data, err := msgp.ReadMapHeaderBytes(data)
	if err != nil {
		return nil, nil, err
	}

	result := make(map[string]string, mapLen)
	for i := uint32(0); i < mapLen; i++ {
		var key, val []byte
		key, data, err = msgp.ReadStringZC(data)
		if err != nil {
			return nil, nil, err
		}

		// Value might be string or bytes
		if msgp.IsNil(data) {
			data, err = msgp.ReadNilBytes(data)
			if err != nil {
				return nil, nil, err
			}
			continue
		}

		typ := msgp.NextType(data)
		switch typ {
		case msgp.StrType:
			val, data, err = msgp.ReadStringZC(data)
		case msgp.BinType:
			val, data, err = msgp.ReadBytesZC(data)
		default:
			data, err = msgp.Skip(data)
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		result[string(key)] = string(val)
	}

	return result, data, nil
}
