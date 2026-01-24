// Package backend provides a gofakes3.Backend implementation using SQLite and erasure decoding
package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/johannesboyne/gofakes3"

	"github.com/minio/minio/internal/erasure"
	"github.com/minio/minio/internal/metadata"
	"github.com/minio/minio/internal/xlmeta"
)

// Backend implements gofakes3.Backend using SQLite for metadata and erasure decoding for data
type Backend struct {
	store    *metadata.Store
	decoders []*erasure.Decoder // One decoder per erasure set
}

// New creates a new backend with a single decoder (for single-set configurations)
func New(store *metadata.Store, decoder *erasure.Decoder) *Backend {
	return &Backend{
		store:    store,
		decoders: []*erasure.Decoder{decoder},
	}
}

// NewMultiSet creates a new backend with multiple decoders (one per erasure set)
func NewMultiSet(store *metadata.Store, decoders []*erasure.Decoder) *Backend {
	return &Backend{
		store:    store,
		decoders: decoders,
	}
}

// ListBuckets returns all buckets
func (b *Backend) ListBuckets() ([]gofakes3.BucketInfo, error) {
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
func (b *Backend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
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
		result.Add(&gofakes3.Content{
			Key:          obj.Key,
			LastModified: gofakes3.NewContentTime(obj.ModTime),
			ETag:         `"` + obj.ETag + `"`,
			Size:         obj.Size,
			StorageClass: gofakes3.StorageStandard,
		})
	}

	for _, pfx := range listInfo.Prefixes {
		result.AddPrefix(pfx)
	}

	return result, nil
}

// CreateBucket creates a new bucket (not supported - read-only)
func (b *Backend) CreateBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

// BucketExists checks if a bucket exists
func (b *Backend) BucketExists(name string) (bool, error) {
	ctx := context.Background()
	return b.store.BucketExists(ctx, name)
}

// DeleteBucket deletes a bucket (not supported - read-only)
func (b *Backend) DeleteBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

// ForceDeleteBucket force-deletes a bucket (not supported - read-only)
func (b *Backend) ForceDeleteBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

// GetObject retrieves an object
func (b *Backend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	ctx := context.Background()

	// Get metadata from SQLite
	obj, err := b.store.GetObject(ctx, bucketName, objectName)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	// Convert metadata to xlmeta format for decoder
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

	// Select correct decoder based on set index
	if obj.SetIndex < 0 || obj.SetIndex >= len(b.decoders) {
		return nil, fmt.Errorf("invalid set index %d (have %d decoders)", obj.SetIndex, len(b.decoders))
	}
	decoder := b.decoders[obj.SetIndex]

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
			// Suffix range: last N bytes
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
func (b *Backend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	ctx := context.Background()

	obj, err := b.store.GetObject(ctx, bucketName, objectName)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	meta := make(map[string]string)
	meta["Last-Modified"] = obj.ModTime.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	if obj.ContentType != "" {
		meta["Content-Type"] = obj.ContentType
	}

	return &gofakes3.Object{
		Name:     obj.Key,
		Metadata: meta,
		Size:     obj.Size,
		Contents: io.NopCloser(strings.NewReader("")),
		Hash:     []byte(obj.ETag),
	}, nil
}

// DeleteObject deletes an object (not supported - read-only)
func (b *Backend) DeleteObject(bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	return gofakes3.ObjectDeleteResult{}, gofakes3.ErrNotImplemented
}

// PutObject stores an object (not supported - read-only)
func (b *Backend) PutObject(bucketName, key string, meta map[string]string, input io.Reader, size int64, conditions *gofakes3.PutConditions) (gofakes3.PutObjectResult, error) {
	return gofakes3.PutObjectResult{}, gofakes3.ErrNotImplemented
}

// DeleteMulti deletes multiple objects (not supported - read-only)
func (b *Backend) DeleteMulti(bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	return gofakes3.MultiDeleteResult{}, gofakes3.ErrNotImplemented
}

// CopyObject copies an object (not supported - read-only)
func (b *Backend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	return gofakes3.CopyObjectResult{}, gofakes3.ErrNotImplemented
}

// parseUUIDToBytes parses a UUID string to a 16-byte array
func parseUUIDToBytes(s string, out *[16]byte) error {
	// Remove dashes
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return fmt.Errorf("invalid UUID length: %d", len(s))
	}

	for i := 0; i < 16; i++ {
		var b byte
		_, err := fmt.Sscanf(s[i*2:i*2+2], "%02x", &b)
		if err != nil {
			return err
		}
		out[i] = b
	}
	return nil
}
