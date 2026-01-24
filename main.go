package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/johannesboyne/gofakes3"
	"github.com/minio/minio/cmd"
)

// Config holds the server configuration
type Config struct {
	RootDir      string // Root directory containing disk folders
	DiskPrefix   string // Prefix for disk folders (e.g., "storage" for storage1, storage2, etc.)
	DiskStart    int    // First disk number
	DiskEnd      int    // Last disk number
	DrivesPerSet int    // Number of drives per erasure set
	Stride       int    // Interleaving stride (e.g., 4 gives order: 1,5,9,13,2,6,10,14...)
	ListenAddr   string // Address to listen on
}

// MinioBackend implements gofakes3.Backend using MinIO's erasure decoding
type MinioBackend struct {
	objLayer cmd.ObjectLayer
}

func (b *MinioBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	ctx := context.Background()
	buckets, err := b.objLayer.ListBuckets(ctx, cmd.BucketOptions{})
	if err != nil {
		return nil, err
	}

	result := make([]gofakes3.BucketInfo, len(buckets))
	for i, bucket := range buckets {
		result[i] = gofakes3.BucketInfo{
			Name:         bucket.Name,
			CreationDate: gofakes3.NewContentTime(bucket.Created),
		}
	}
	return result, nil
}

func (b *MinioBackend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
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

	listInfo, err := b.objLayer.ListObjects(ctx, name, prefixStr, marker, delimiter, maxKeys)
	if err != nil {
		if strings.Contains(err.Error(), "bucket not found") || strings.Contains(err.Error(), "does not exist") {
			return nil, gofakes3.BucketNotFound(name)
		}
		return nil, err
	}

	result := gofakes3.NewObjectList()
	result.IsTruncated = listInfo.IsTruncated
	result.NextMarker = listInfo.NextMarker

	for _, obj := range listInfo.Objects {
		if prefix != nil {
			var match gofakes3.PrefixMatch
			if !prefix.Match(obj.Name, &match) {
				continue
			}
			if match.CommonPrefix {
				result.AddPrefix(obj.Name)
				continue
			}
		}

		result.Add(&gofakes3.Content{
			Key:          obj.Name,
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

func (b *MinioBackend) CreateBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

func (b *MinioBackend) BucketExists(name string) (bool, error) {
	ctx := context.Background()
	_, err := b.objLayer.GetBucketInfo(ctx, name, cmd.BucketOptions{})
	if err != nil {
		if strings.Contains(err.Error(), "bucket not found") || strings.Contains(err.Error(), "does not exist") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (b *MinioBackend) DeleteBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

func (b *MinioBackend) ForceDeleteBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

func (b *MinioBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	ctx := context.Background()

	var rs *cmd.HTTPRangeSpec
	if rangeRequest != nil {
		rs = &cmd.HTTPRangeSpec{
			Start:          rangeRequest.Start,
			End:            rangeRequest.End,
			IsSuffixLength: rangeRequest.FromEnd,
		}
	}

	reader, err := b.objLayer.GetObjectNInfo(ctx, bucketName, objectName, rs, nil, cmd.ObjectOptions{})
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") {
			return nil, gofakes3.KeyNotFound(objectName)
		}
		return nil, err
	}

	meta := make(map[string]string)
	meta["Last-Modified"] = reader.ObjInfo.ModTime.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")

	obj := &gofakes3.Object{
		Name:     reader.ObjInfo.Name,
		Metadata: meta,
		Size:     reader.ObjInfo.Size,
		Contents: reader,
		Hash:     []byte(reader.ObjInfo.ETag),
	}

	if rangeRequest != nil {
		obj.Range = &gofakes3.ObjectRange{
			Start:  rangeRequest.Start,
			Length: reader.ObjInfo.Size,
		}
	}

	return obj, nil
}

func (b *MinioBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	ctx := context.Background()

	info, err := b.objLayer.GetObjectInfo(ctx, bucketName, objectName, cmd.ObjectOptions{})
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") {
			return nil, gofakes3.KeyNotFound(objectName)
		}
		return nil, err
	}

	meta := make(map[string]string)
	meta["Last-Modified"] = info.ModTime.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")

	return &gofakes3.Object{
		Name:     info.Name,
		Metadata: meta,
		Size:     info.Size,
		Contents: io.NopCloser(strings.NewReader("")),
		Hash:     []byte(info.ETag),
	}, nil
}

func (b *MinioBackend) DeleteObject(bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	return gofakes3.ObjectDeleteResult{}, gofakes3.ErrNotImplemented
}

func (b *MinioBackend) PutObject(bucketName, key string, meta map[string]string, input io.Reader, size int64, conditions *gofakes3.PutConditions) (gofakes3.PutObjectResult, error) {
	return gofakes3.PutObjectResult{}, gofakes3.ErrNotImplemented
}

func (b *MinioBackend) DeleteMulti(bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	return gofakes3.MultiDeleteResult{}, gofakes3.ErrNotImplemented
}

func (b *MinioBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	return gofakes3.CopyObjectResult{}, gofakes3.ErrNotImplemented
}

// --- MinIO initialization ---

func mustGetNewEndpoints(poolIdx int, drivesPerSet int, args ...string) (endpoints cmd.Endpoints) {
	endpoints, err := cmd.NewEndpoints(args...)
	if err != nil {
		panic(err)
	}
	for i := range endpoints {
		endpoints[i].SetPoolIndex(poolIdx)
		endpoints[i].SetSetIndex(i / drivesPerSet)
		endpoints[i].SetDiskIndex(i % drivesPerSet)
	}
	return endpoints
}

func mustGetPoolEndpoints(drivesPerSet int, args ...string) cmd.EndpointServerPools {
	totalDisks := len(args)
	setCount := totalDisks / drivesPerSet

	if totalDisks%drivesPerSet != 0 {
		log.Fatalf("Total disks (%d) must be divisible by drives per set (%d)", totalDisks, drivesPerSet)
	}

	endpoints := mustGetNewEndpoints(0, drivesPerSet, args...)
	return []cmd.PoolEndpoints{{
		SetCount:     setCount,
		DrivesPerSet: drivesPerSet,
		Endpoints:    endpoints,
		CmdLine:      strings.Join(args, " "),
	}}
}

func newTestObjectLayer(ctx context.Context, endpointServerPools cmd.EndpointServerPools) (newObject cmd.ObjectLayer, err error) {
	cmd.InitAllSubsystems(ctx)
	return cmd.NewErasureServerPools(ctx, endpointServerPools)
}

func initObjectLayer(ctx context.Context, endpointServerPools cmd.EndpointServerPools) (cmd.ObjectLayer, error) {
	objLayer, err := newTestObjectLayer(ctx, endpointServerPools)
	if err != nil {
		return nil, err
	}
	return objLayer, nil
}

func buildDiskPaths(cfg Config) []string {
	totalDisks := cfg.DiskEnd - cfg.DiskStart + 1

	var paths []string
	if cfg.Stride <= 1 {
		// Sequential order: 1, 2, 3, 4, ...
		for i := cfg.DiskStart; i <= cfg.DiskEnd; i++ {
			path := filepath.Join(cfg.RootDir, fmt.Sprintf("%s%d", cfg.DiskPrefix, i))
			paths = append(paths, path)
		}
	} else {
		// Interleaved order with stride
		// stride=4 gives: 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15, 4, 8, 12, 16
		numGroups := totalDisks / cfg.Stride
		for j := 0; j < numGroups; j++ {
			for k := 0; k < cfg.Stride; k++ {
				diskNum := k*numGroups + j + cfg.DiskStart
				path := filepath.Join(cfg.RootDir, fmt.Sprintf("%s%d", cfg.DiskPrefix, diskNum))
				paths = append(paths, path)
			}
		}
	}
	fmt.Println("Built disk paths:", paths)
	return paths
}

func prepareErasure(ctx context.Context, cfg Config) (cmd.ObjectLayer, error) {
	fsDirs := buildDiskPaths(cfg)

	log.Printf("Disk configuration:")
	log.Printf("  Root directory: %s", cfg.RootDir)
	log.Printf("  Disk prefix: %s", cfg.DiskPrefix)
	log.Printf("  Disk range: %d-%d (%d disks)", cfg.DiskStart, cfg.DiskEnd, len(fsDirs))
	log.Printf("  Drives per set: %d", cfg.DrivesPerSet)
	log.Printf("  Number of sets: %d", len(fsDirs)/cfg.DrivesPerSet)
	log.Printf("  Stride: %d", cfg.Stride)
	log.Printf("  Disk paths: %v", fsDirs)

	obj, err := initObjectLayer(ctx, mustGetPoolEndpoints(cfg.DrivesPerSet, fsDirs...))
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.RootDir, "root", ".disks", "Root directory containing disk folders")
	flag.StringVar(&cfg.DiskPrefix, "prefix", "storage", "Prefix for disk folders (e.g., 'storage' for storage1, storage2)")
	flag.IntVar(&cfg.DiskStart, "start", 1, "First disk number")
	flag.IntVar(&cfg.DiskEnd, "end", 16, "Last disk number")
	flag.IntVar(&cfg.DrivesPerSet, "set-size", 16, "Number of drives per erasure set")
	flag.IntVar(&cfg.Stride, "stride", 4, "Interleaving stride (4 gives: 1,5,9,13,2,6,10,14...; 0 or 1 for sequential)")
	flag.StringVar(&cfg.ListenAddr, "addr", ":9000", "Address to listen on")
	flag.Parse()

	// Validate configuration
	totalDisks := cfg.DiskEnd - cfg.DiskStart + 1
	if totalDisks < cfg.DrivesPerSet {
		log.Fatalf("Total disks (%d) must be >= drives per set (%d)", totalDisks, cfg.DrivesPerSet)
	}
	if totalDisks%cfg.DrivesPerSet != 0 {
		log.Fatalf("Total disks (%d) must be divisible by drives per set (%d)", totalDisks, cfg.DrivesPerSet)
	}
	if cfg.Stride > 1 && totalDisks%cfg.Stride != 0 {
		log.Fatalf("Total disks (%d) must be divisible by stride (%d)", totalDisks, cfg.Stride)
	}

	ctx := context.Background()

	log.Println("Initializing MinIO erasure backend...")
	obj, err := prepareErasure(ctx, cfg)
	if err != nil {
		log.Fatal("Failed to initialize erasure backend:", err)
	}
	defer obj.Shutdown(ctx)

	backend := &MinioBackend{objLayer: obj}
	faker := gofakes3.New(backend)

	log.Printf("Starting S3 server on %s", cfg.ListenAddr)
	log.Println("Example usage:")
	log.Printf("  aws --endpoint-url http://localhost%s s3 ls", cfg.ListenAddr)
	log.Printf("  aws --endpoint-url http://localhost%s s3 ls s3://BUCKET/", cfg.ListenAddr)
	log.Printf("  aws --endpoint-url http://localhost%s s3 cp s3://BUCKET/file.txt ./", cfg.ListenAddr)

	if err := http.ListenAndServe(cfg.ListenAddr, faker.Server()); err != nil {
		log.Fatal("Server error:", err)
	}
}
