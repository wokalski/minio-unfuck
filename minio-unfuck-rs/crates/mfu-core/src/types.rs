/// A physical extent on a device (for extent-based raw I/O)
#[derive(Debug, Clone, Copy)]
pub struct Extent {
    pub logical_offset: i64,  // byte offset within the file
    pub physical_offset: i64, // byte offset on the device
    pub length: i64,          // extent length in bytes
}
