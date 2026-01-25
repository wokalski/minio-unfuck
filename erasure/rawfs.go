// Package erasure provides raw filesystem access for reading MinIO data directly from block devices
package erasure

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"syscall"
	"unsafe"

	"github.com/wokalski/minio-unfuck/vendor_fork/xfs/xfs"
)

// BLKGETSIZE64 ioctl to get block device size in bytes
const BLKGETSIZE64 = 0x80081272

// RawFS provides read access to a filesystem on a raw block device
type RawFS struct {
	devicePath string
	file       *os.File
	fs         *xfs.FileSystem
}

// FileExtent represents a physical extent of a file on disk
type FileExtent struct {
	LogicalOffset  int64 // Byte offset within the file
	PhysicalOffset int64 // Byte offset on the partition
	Length         int64 // Length in bytes
}

// OpenRawFS opens a block device and returns a RawFS for accessing files
func OpenRawFS(devicePath string) (*RawFS, error) {
	f, err := os.Open(devicePath)
	if err != nil {
		return nil, fmt.Errorf("open device %s: %w", devicePath, err)
	}

	// Check filesystem type
	fsType, err := detectFSType(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("detect fs type: %w", err)
	}

	if fsType != "xfs" {
		f.Close()
		return nil, fmt.Errorf("unsupported filesystem type: %s (only XFS supported)", fsType)
	}

	// Get device size - for block devices, use ioctl
	deviceSize, err := getDeviceSize(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("get device size: %w", err)
	}

	// Create XFS filesystem
	sr := io.NewSectionReader(f, 0, deviceSize)
	xfsFS, err := xfs.NewFS(sr, nil)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("create xfs filesystem: %w", err)
	}

	return &RawFS{
		devicePath: devicePath,
		file:       f,
		fs:         xfsFS,
	}, nil
}

// Close closes the underlying file handle
func (r *RawFS) Close() error {
	return r.file.Close()
}

// FS returns the io/fs compatible filesystem for walking
func (r *RawFS) FS() fs.FS {
	return r.fs
}

// XFS returns the raw XFS filesystem for direct access to XFS-specific methods
func (r *RawFS) XFS() *xfs.FileSystem {
	return r.fs
}

// DevicePath returns the path to the block device
func (r *RawFS) DevicePath() string {
	return r.devicePath
}

// BlockSize returns the filesystem block size
func (r *RawFS) BlockSize() uint32 {
	return r.fs.BlockSize()
}

// GetFileExtents returns the physical extents for a file
// The returned extents can be used to read the file directly from the block device
func (r *RawFS) GetFileExtents(path string) ([]FileExtent, error) {
	// Get file info which includes the inode
	info, err := r.fs.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}

	// Get the inode from FileInfo
	fileInfo, ok := info.(interface{ GetInode() *xfs.Inode })
	if !ok {
		return nil, fmt.Errorf("cannot get inode for %s", path)
	}

	inode := fileInfo.GetInode()
	if inode == nil {
		return nil, fmt.Errorf("nil inode for %s", path)
	}

	// Get extents with byte offsets
	sb := r.fs.GetSuperBlock()
	xfsExtents := inode.GetExtentsBytes(sb)
	if xfsExtents == nil {
		return nil, fmt.Errorf("no extents for %s (may be directory or symlink)", path)
	}

	// Convert to our FileExtent type
	extents := make([]FileExtent, len(xfsExtents))
	for i, e := range xfsExtents {
		extents[i] = FileExtent{
			LogicalOffset:  e.LogicalOffset,
			PhysicalOffset: e.PhysicalOffset,
			Length:         e.Length,
		}
	}

	return extents, nil
}

// ReadFileAtExtents reads a file using pre-computed extents
// This allows reading files directly from the block device without filesystem overhead
func (r *RawFS) ReadFileAtExtents(extents []FileExtent, fileSize int64) ([]byte, error) {
	data := make([]byte, fileSize)
	var bytesRead int64

	for _, ext := range extents {
		// Don't read beyond file size
		toRead := ext.Length
		if bytesRead+toRead > fileSize {
			toRead = fileSize - bytesRead
		}
		if toRead <= 0 {
			break
		}

		// Read directly from device at physical offset
		n, err := r.file.ReadAt(data[ext.LogicalOffset:ext.LogicalOffset+toRead], ext.PhysicalOffset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read at offset %d: %w", ext.PhysicalOffset, err)
		}
		bytesRead += int64(n)
	}

	return data[:bytesRead], nil
}

// ReadFile reads a file using the filesystem (normal path, for comparison)
func (r *RawFS) ReadFile(path string) ([]byte, error) {
	f, err := r.fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// getDeviceSize returns the size of a file or block device
func getDeviceSize(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}

	// For regular files, use the file size
	if info.Mode().IsRegular() {
		return info.Size(), nil
	}

	// For block devices, use ioctl
	if info.Mode()&os.ModeDevice != 0 {
		var size int64
		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), BLKGETSIZE64, uintptr(unsafe.Pointer(&size)))
		if errno != 0 {
			return 0, fmt.Errorf("ioctl BLKGETSIZE64: %v", errno)
		}
		return size, nil
	}

	return 0, fmt.Errorf("unsupported file type: %v", info.Mode())
}

// detectFSType detects the filesystem type from magic bytes
func detectFSType(f *os.File) (string, error) {
	buf := make([]byte, 1024)

	// Check XFS magic at offset 0: "XFSB"
	_, err := f.ReadAt(buf[:4], 0)
	if err != nil {
		return "", fmt.Errorf("read xfs magic: %w", err)
	}
	if string(buf[:4]) == "XFSB" {
		return "xfs", nil
	}

	// Check ext4 magic at offset 0x438: 0xEF53
	_, err = f.ReadAt(buf[:2], 0x438)
	if err != nil {
		return "", fmt.Errorf("read ext4 magic: %w", err)
	}
	if buf[0] == 0x53 && buf[1] == 0xEF {
		return "ext4", nil
	}

	return "unknown", nil
}

// DiscoverPartitions returns partition device paths for a whole disk
// e.g., /dev/sde -> [/dev/sde1, /dev/sde2, ...]
func DiscoverPartitions(diskPath string) ([]string, error) {
	// Read /sys/block/sdX/sdX*/partition to find partitions
	// For /dev/sde, check /sys/block/sde/sde*/partition

	// Extract device name from path
	var devName string
	if len(diskPath) > 5 && diskPath[:5] == "/dev/" {
		devName = diskPath[5:]
	} else {
		return nil, fmt.Errorf("invalid device path: %s", diskPath)
	}

	sysPath := fmt.Sprintf("/sys/block/%s", devName)
	entries, err := os.ReadDir(sysPath)
	if err != nil {
		return nil, fmt.Errorf("read sysfs: %w", err)
	}

	var partitions []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// Check if this is a partition (has "partition" file)
		partFile := fmt.Sprintf("%s/%s/partition", sysPath, entry.Name())
		if _, err := os.Stat(partFile); err == nil {
			partitions = append(partitions, "/dev/"+entry.Name())
		}
	}

	return partitions, nil
}
