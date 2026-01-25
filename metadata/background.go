package metadata

import (
	"context"
	"fmt"
	"io/fs"
	"sync"
	"sync/atomic"

	"github.com/schollz/progressbar/v3"
	"github.com/wokalski/minio-unfuck/erasure"
	"github.com/wokalski/minio-unfuck/xlmeta"
)

// BackgroundParserConfig configures the background xl.meta parser
type BackgroundParserConfig struct {
	Workers      int  // Number of parallel workers
	BatchSize    int  // Number of objects to process per batch
	ShowProgress bool // Whether to show progress bar
}

// BackgroundParser parses xl.meta files in the background
type BackgroundParser struct {
	config     BackgroundParserConfig
	store      *DuckStore
	partitions []*erasure.RawFS
	diskMap    *erasure.DiskMapping
	running    atomic.Bool
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewBackgroundParser creates a new background parser
func NewBackgroundParser(store *DuckStore, partitions []*erasure.RawFS, diskMap *erasure.DiskMapping, config BackgroundParserConfig) *BackgroundParser {
	if config.Workers <= 0 {
		config.Workers = 4
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}

	return &BackgroundParser{
		config:     config,
		store:      store,
		partitions: partitions,
		diskMap:    diskMap,
	}
}

// Start begins background parsing in a goroutine
func (bp *BackgroundParser) Start(ctx context.Context) error {
	if bp.running.Load() {
		return fmt.Errorf("parser already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	bp.cancel = cancel
	bp.running.Store(true)

	bp.wg.Add(1)
	go func() {
		defer bp.wg.Done()
		defer bp.running.Store(false)
		bp.run(ctx)
	}()

	return nil
}

// Stop stops the background parser and waits for it to finish
func (bp *BackgroundParser) Stop() {
	if bp.cancel != nil {
		bp.cancel()
	}
	bp.wg.Wait()
}

// IsRunning returns whether the parser is currently running
func (bp *BackgroundParser) IsRunning() bool {
	return bp.running.Load()
}

// run is the main parsing loop
func (bp *BackgroundParser) run(ctx context.Context) {
	// Get initial count
	totalUnparsed, err := bp.store.UnparsedCount(ctx)
	if err != nil {
		return
	}

	if totalUnparsed == 0 {
		return
	}

	// Create progress bar
	var bar *progressbar.ProgressBar
	if bp.config.ShowProgress {
		bar = progressbar.NewOptions64(
			totalUnparsed,
			progressbar.OptionSetDescription("Parsing xl.meta"),
			progressbar.OptionSetWidth(40),
			progressbar.OptionShowCount(),
			progressbar.OptionShowIts(),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "=",
				SaucerHead:    ">",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
		)
	}

	// Create worker pool
	jobs := make(chan ObjectPath, bp.config.Workers*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < bp.config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bp.worker(ctx, jobs, bar)
		}()
	}

	// Feed jobs
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		default:
		}

		// Get batch of unparsed objects
		paths, err := bp.store.GetUnparsedObjects(ctx, bp.config.BatchSize)
		if err != nil || len(paths) == 0 {
			break
		}

		for _, path := range paths {
			select {
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				return
			case jobs <- path:
			}
		}
	}

	close(jobs)
	wg.Wait()

	if bar != nil {
		bar.Finish()
	}
}

// worker processes xl.meta files
func (bp *BackgroundParser) worker(ctx context.Context, jobs <-chan ObjectPath, bar *progressbar.ProgressBar) {
	for path := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := bp.parseObject(ctx, path); err != nil {
			// Log error but continue
			continue
		}

		if bar != nil {
			bar.Add(1)
		}
	}
}

// parseObject parses a single object's xl.meta and updates the database
func (bp *BackgroundParser) parseObject(ctx context.Context, path ObjectPath) error {
	// Get partition
	if path.PartitionIndex < 0 || path.PartitionIndex >= len(bp.partitions) {
		return fmt.Errorf("invalid partition index: %d", path.PartitionIndex)
	}
	part := bp.partitions[path.PartitionIndex]
	if part == nil {
		return fmt.Errorf("partition %d not available", path.PartitionIndex)
	}

	// Build xl.meta path
	xlMetaPath := fmt.Sprintf("%s/%s/%s/xl.meta", path.Bucket, path.Key, path.DataDir)

	// Read xl.meta
	xlMetaData, err := readFileFromFS(part.FS(), xlMetaPath)
	if err != nil {
		return fmt.Errorf("read xl.meta: %w", err)
	}

	// Parse xl.meta
	meta, err := xlmeta.Parse(xlMetaData)
	if err != nil {
		return fmt.Errorf("parse xl.meta: %w", err)
	}

	// Build ObjectMeta
	obj := &ObjectMeta{
		Bucket:         path.Bucket,
		Key:            path.Key,
		DataDir:        path.DataDir,
		PartitionIndex: path.PartitionIndex,
		MetadataLoaded: true,
		Size:           meta.Size,
		ModTime:        meta.ModTime,
		ETag:           meta.ETag,
		ContentType:    meta.ContentType,
		UserMeta:       meta.UserMeta,
		DataBlocks:     meta.DataBlocks,
		ParityBlocks:   meta.ParityBlocks,
		BlockSize:      meta.BlockSize,
		Distribution:   meta.Distribution,
	}

	// Convert parts
	for _, p := range meta.Parts {
		obj.Parts = append(obj.Parts, PartMeta{
			Number:     p.Number,
			Size:       p.Size,
			ActualSize: p.ActualSize,
		})
	}

	// Update database
	if err := bp.store.UpdateObjectMetadata(ctx, obj); err != nil {
		return fmt.Errorf("update metadata: %w", err)
	}

	return nil
}

// readFileFromFS reads a file from an fs.FS
func readFileFromFS(fsys fs.FS, path string) ([]byte, error) {
	f, err := fsys.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	data := make([]byte, stat.Size())
	_, err = f.(interface{ Read([]byte) (int, error) }).Read(data)
	return data, err
}

// ParseObjectOnDemand parses an object's xl.meta on-demand (for lazy loading)
func ParseObjectOnDemand(ctx context.Context, store *DuckStore, partitions []*erasure.RawFS, obj *ObjectMeta) error {
	if obj.MetadataLoaded {
		return nil // Already parsed
	}

	if obj.PartitionIndex < 0 || obj.PartitionIndex >= len(partitions) {
		return fmt.Errorf("invalid partition index: %d", obj.PartitionIndex)
	}
	part := partitions[obj.PartitionIndex]
	if part == nil {
		return fmt.Errorf("partition %d not available", obj.PartitionIndex)
	}

	// Build xl.meta path
	xlMetaPath := fmt.Sprintf("%s/%s/%s/xl.meta", obj.Bucket, obj.Key, obj.DataDir)

	// Read xl.meta
	xlMetaData, err := readFileFromFS(part.FS(), xlMetaPath)
	if err != nil {
		return fmt.Errorf("read xl.meta: %w", err)
	}

	// Parse xl.meta
	meta, err := xlmeta.Parse(xlMetaData)
	if err != nil {
		return fmt.Errorf("parse xl.meta: %w", err)
	}

	// Update ObjectMeta fields
	obj.MetadataLoaded = true
	obj.Size = meta.Size
	obj.ModTime = meta.ModTime
	obj.ETag = meta.ETag
	obj.ContentType = meta.ContentType
	obj.UserMeta = meta.UserMeta
	obj.DataBlocks = meta.DataBlocks
	obj.ParityBlocks = meta.ParityBlocks
	obj.BlockSize = meta.BlockSize
	obj.Distribution = meta.Distribution

	// Convert parts
	obj.Parts = nil
	for _, p := range meta.Parts {
		obj.Parts = append(obj.Parts, PartMeta{
			Number:     p.Number,
			Size:       p.Size,
			ActualSize: p.ActualSize,
		})
	}

	// Update database
	if err := store.UpdateObjectMetadata(ctx, obj); err != nil {
		return fmt.Errorf("update metadata: %w", err)
	}

	return nil
}
