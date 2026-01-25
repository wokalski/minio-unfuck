// Package backend provides a gofakes3.Backend implementation for raw disk access
package backend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/johannesboyne/gofakes3"

	"github.com/wokalski/minio-unfuck/erasure"
	"github.com/wokalski/minio-unfuck/metadata"
	"github.com/wokalski/minio-unfuck/xlmeta"
)

// RawBackend implements gofakes3.Backend using DuckDB and raw XFS access
type RawBackend struct {
	store      *metadata.DuckStore
	partitions []*erasure.RawFS
	diskMap    *erasure.DiskMapping
	syncTime   time.Time // Time when sync started (for dummy metadata)
}

// NewRawBackend creates a new backend for raw disk access
func NewRawBackend(store *metadata.DuckStore, partitions []*erasure.RawFS, diskMap *erasure.DiskMapping) *RawBackend {
	return &RawBackend{
		store:      store,
		partitions: partitions,
		diskMap:    diskMap,
		syncTime:   time.Now(),
	}
}

// ListBuckets returns all buckets
func (b *RawBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	ctx := context.Background()
	buckets, err := b.store.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]gofakes3.BucketInfo, len(buckets))
	for i, bucket := range buckets {
		result[i] = gofakes3.BucketInfo{
			Name:         bucket.Name,
			CreationDate: gofakes3.NewContentTime(bucket.CreatedAt),
		}
	}
	return result, nil
}

// ListBucket lists objects in a bucket
func (b *RawBackend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	ctx := context.Background()

	prefixStr := ""
	delimiter := ""
	marker := ""
	maxKeys := 1000

	if prefix != nil {
		if prefix.HasPrefix {
			prefixStr = prefix.Prefix
		}
		if prefix.HasDelimiter {
			delimiter = prefix.Delimiter
		}
	}

	if page.HasMarker {
		marker = page.Marker
	}
	if page.MaxKeys > 0 {
		maxKeys = int(page.MaxKeys)
	}

	listInfo, err := b.store.ListObjects(ctx, name, prefixStr, marker, delimiter, maxKeys)
	if err != nil {
		return nil, err
	}

	result := gofakes3.NewObjectList()
	result.IsTruncated = listInfo.IsTruncated
	result.NextMarker = listInfo.NextMarker

	for _, obj := range listInfo.Objects {
		// Use real metadata if parsed, otherwise dummy metadata
		var modTime time.Time
		var etag string
		var size int64

		if obj.MetadataLoaded {
			modTime = obj.ModTime
			etag = obj.ETag
			size = obj.Size
		} else {
			// Dummy metadata for unparsed objects
			modTime = b.syncTime
			etag = generateDummyETag(obj.Key)
			size = 0
		}

		result.Add(&gofakes3.Content{
			Key:          obj.Key,
			LastModified: gofakes3.NewContentTime(modTime),
			ETag:         `"` + etag + `"`,
			Size:         size,
			StorageClass: gofakes3.StorageStandard,
		})
	}

	for _, pfx := range listInfo.Prefixes {
		result.AddPrefix(pfx)
	}

	return result, nil
}

// CreateBucket creates a new bucket (not supported - read-only)
func (b *RawBackend) CreateBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

// BucketExists checks if a bucket exists
func (b *RawBackend) BucketExists(name string) (bool, error) {
	ctx := context.Background()
	return b.store.BucketExists(ctx, name)
}

// DeleteBucket deletes a bucket (not supported - read-only)
func (b *RawBackend) DeleteBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

// ForceDeleteBucket force-deletes a bucket (not supported - read-only)
func (b *RawBackend) ForceDeleteBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

// GetObject retrieves an object (with lazy xl.meta parsing)
func (b *RawBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	ctx := context.Background()

	// Get metadata from DuckDB
	obj, err := b.store.GetObject(ctx, bucketName, objectName)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	// Lazy parse xl.meta if not yet loaded
	if !obj.MetadataLoaded {
		if err := metadata.ParseObjectOnDemand(ctx, b.store, b.partitions, obj); err != nil {
			return nil, fmt.Errorf("parse xl.meta on demand: %w", err)
		}
	}

	// Build xlmeta for decoder
	xlMeta := &xlmeta.ObjectMeta{
		Bucket:       obj.Bucket,
		Key:          obj.Key,
		DataBlocks:   obj.DataBlocks,
		ParityBlocks: obj.ParityBlocks,
		BlockSize:    obj.BlockSize,
		Distribution: obj.Distribution,
		Size:         obj.Size,
		ModTime:      obj.ModTime,
		ETag:         obj.ETag,
		ContentType:  obj.ContentType,
		UserMeta:     obj.UserMeta,
	}

	// Parse DataDir UUID
	if err := parseUUIDToBytes(obj.DataDir, &xlMeta.DataDir); err != nil {
		return nil, fmt.Errorf("parse data dir: %w", err)
	}

	// Convert parts
	for _, p := range obj.Parts {
		xlMeta.Parts = append(xlMeta.Parts, xlmeta.PartMeta{
			Number:     p.Number,
			Size:       p.Size,
			ActualSize: p.ActualSize,
		})
	}

	// Create decoder for this object using disk mapping
	decoder := b.createDecoder(obj.Distribution)

	// Read object data using decoder
	data, err := decoder.ReadObjectWithMeta(bucketName, objectName, xlMeta)
	if err != nil {
		return nil, fmt.Errorf("decode object: %w", err)
	}

	// Handle range request
	var contents io.ReadCloser
	var size int64 = obj.Size
	var objRange *gofakes3.ObjectRange

	if rangeRequest != nil {
		start := rangeRequest.Start
		end := rangeRequest.End

		if rangeRequest.FromEnd {
			start = obj.Size - end
			end = obj.Size - 1
		}

		if end >= obj.Size {
			end = obj.Size - 1
		}

		if start < 0 {
			start = 0
		}

		rangeData := data[start : end+1]
		contents = io.NopCloser(bytes.NewReader(rangeData))
		size = int64(len(rangeData))

		objRange = &gofakes3.ObjectRange{
			Start:  start,
			Length: size,
		}
	} else {
		contents = io.NopCloser(bytes.NewReader(data))
	}

	meta := make(map[string]string)
	meta["Last-Modified"] = obj.ModTime.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	if obj.ContentType != "" {
		meta["Content-Type"] = obj.ContentType
	}

	return &gofakes3.Object{
		Name:     obj.Key,
		Metadata: meta,
		Size:     size,
		Contents: contents,
		Hash:     []byte(obj.ETag),
		Range:    objRange,
	}, nil
}

// HeadObject retrieves object metadata without the body
func (b *RawBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	ctx := context.Background()

	obj, err := b.store.GetObject(ctx, bucketName, objectName)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	// Use real metadata if parsed, otherwise dummy
	var modTime time.Time
	var etag string
	var size int64
	var contentType string

	if obj.MetadataLoaded {
		modTime = obj.ModTime
		etag = obj.ETag
		size = obj.Size
		contentType = obj.ContentType
	} else {
		// Dummy metadata
		modTime = b.syncTime
		etag = generateDummyETag(objectName)
		size = 0
		contentType = "application/octet-stream"
	}

	meta := make(map[string]string)
	meta["Last-Modified"] = modTime.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	if contentType != "" {
		meta["Content-Type"] = contentType
	}

	return &gofakes3.Object{
		Name:     obj.Key,
		Metadata: meta,
		Size:     size,
		Contents: io.NopCloser(strings.NewReader("")),
		Hash:     []byte(etag),
	}, nil
}

// DeleteObject deletes an object (not supported - read-only)
func (b *RawBackend) DeleteObject(bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	return gofakes3.ObjectDeleteResult{}, gofakes3.ErrNotImplemented
}

// PutObject stores an object (not supported - read-only)
func (b *RawBackend) PutObject(bucketName, key string, meta map[string]string, input io.Reader, size int64, conditions *gofakes3.PutConditions) (gofakes3.PutObjectResult, error) {
	return gofakes3.PutObjectResult{}, gofakes3.ErrNotImplemented
}

// DeleteMulti deletes multiple objects (not supported - read-only)
func (b *RawBackend) DeleteMulti(bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	return gofakes3.MultiDeleteResult{}, gofakes3.ErrNotImplemented
}

// CopyObject copies an object (not supported - read-only)
func (b *RawBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	return gofakes3.CopyObjectResult{}, gofakes3.ErrNotImplemented
}

// createDecoder creates a decoder using the disk mapping for this object's distribution
func (b *RawBackend) createDecoder(distribution []int) *erasure.Decoder {
	// Map distribution to partition paths
	paths := make([]string, len(distribution))
	for shardIdx, diskIdx := range distribution {
		partIdx, ok := b.diskMap.GetPartitionForDiskIndex(diskIdx)
		if ok && partIdx < len(b.partitions) && b.partitions[partIdx] != nil {
			paths[shardIdx] = b.partitions[partIdx].DevicePath()
		}
	}
	return erasure.NewDecoder(paths)
}

// generateDummyETag generates a deterministic ETag from the key
func generateDummyETag(key string) string {
	hash := md5.Sum([]byte(key))
	return hex.EncodeToString(hash[:])
}
