//! Shard reading with HighwayHash256 bitrot verification
//!
//! Port of erasure/shard.go. Reads raw shard bytes and extracts blocks.
//!
//! Shard file layout (per block): `[32-byte HighwayHash256][up to shard_size bytes]`

use anyhow::{bail, Result};
use highway::{HighwayHash, HighwayHasher, Key};

/// Size of HighwayHash256 checksum in bytes
pub const HASH_SIZE: usize = 32;

/// MinIO's fixed key for HighwayHash256 (32 bytes → 4 little-endian u64s)
const MAGIC_KEY_BYTES: [u8; 32] = [
    0x4b, 0xe7, 0x34, 0xfa, 0x8e, 0x23, 0x8a, 0xcd, 0x26, 0x3e, 0x83, 0xe6, 0xbb, 0x96, 0x85,
    0x52, 0x04, 0x0f, 0x93, 0x5d, 0xa3, 0x9f, 0x44, 0x14, 0x97, 0xe0, 0x9d, 0x13, 0x22, 0xde,
    0x36, 0xa0,
];

/// Get the HighwayHash key from MinIO's magic bytes
pub fn highway_key() -> Key {
    Key([
        u64::from_le_bytes(MAGIC_KEY_BYTES[0..8].try_into().unwrap()),
        u64::from_le_bytes(MAGIC_KEY_BYTES[8..16].try_into().unwrap()),
        u64::from_le_bytes(MAGIC_KEY_BYTES[16..24].try_into().unwrap()),
        u64::from_le_bytes(MAGIC_KEY_BYTES[24..32].try_into().unwrap()),
    ])
}

/// Construct the shard file path for a given part
pub fn shard_path(bucket: &str, object: &str, data_dir: &str, part_number: i32) -> String {
    format!("{}/{}/{}/part.{}", bucket, object, data_dir, part_number)
}

/// Read a single block from shard data (already loaded into memory).
///
/// Returns the shard data for the given block (without hash prefix), or None if no data
/// for this block.
///
/// Parameters:
/// - `shard_data`: full contents of the shard file
/// - `block_index`: which block to read (0-based)
/// - `shard_size`: max data bytes per shard per block (without hash)
/// - `verify_bitrot`: whether to verify the HighwayHash256 checksum
pub fn read_shard_block(
    shard_data: &[u8],
    block_index: usize,
    shard_size: i64,
    verify_bitrot: bool,
) -> Result<Option<Vec<u8>>> {
    let file_size = shard_data.len() as i64;
    if file_size <= HASH_SIZE as i64 {
        bail!("shard too small: {} bytes", file_size);
    }

    // Calculate offset for this block
    let block_offset = block_index as i64 * (HASH_SIZE as i64 + shard_size);

    // For the last block, data might be smaller
    let remaining = file_size - block_offset;
    if remaining <= HASH_SIZE as i64 {
        return Ok(None); // No data for this block
    }

    let data_size = std::cmp::min(remaining - HASH_SIZE as i64, shard_size);
    let hash_start = block_offset as usize;
    let data_start = hash_start + HASH_SIZE;
    let data_end = data_start + data_size as usize;

    let hash_buf = &shard_data[hash_start..data_start];
    let data = &shard_data[data_start..data_end];

    if verify_bitrot {
        verify_highway_hash(data, hash_buf, block_index)?;
    }

    Ok(Some(data.to_vec()))
}

/// Read all blocks from shard data, returning concatenated data (without hashes).
pub fn read_shard_all_blocks(
    shard_data: &[u8],
    shard_size: i64,
    verify_bitrot: bool,
) -> Result<Vec<u8>> {
    let file_size = shard_data.len() as i64;
    if file_size <= HASH_SIZE as i64 {
        bail!("shard too small: {} bytes", file_size);
    }

    let block_with_hash = HASH_SIZE as i64 + shard_size;
    let num_blocks = (file_size + block_with_hash - 1) / block_with_hash;

    let mut result = Vec::with_capacity(file_size as usize);

    for block in 0..num_blocks {
        let offset = block * block_with_hash;
        let remaining = file_size - offset;
        if remaining <= HASH_SIZE as i64 {
            break;
        }

        let hash_start = offset as usize;
        let data_start = hash_start + HASH_SIZE;
        let data_size = std::cmp::min(remaining - HASH_SIZE as i64, shard_size) as usize;
        let data_end = data_start + data_size;

        let hash_buf = &shard_data[hash_start..data_start];
        let data = &shard_data[data_start..data_end];

        if verify_bitrot {
            verify_highway_hash(data, hash_buf, block as usize)?;
        }

        result.extend_from_slice(data);
    }

    Ok(result)
}

/// Verify HighwayHash256 of data against expected hash
fn verify_highway_hash(data: &[u8], expected_hash: &[u8], block_index: usize) -> Result<()> {
    let mut hasher = HighwayHasher::new(highway_key());
    hasher.append(data);
    let computed = hasher.finalize256();

    // finalize256() returns [u64; 4] — convert to bytes for comparison
    let mut computed_bytes = [0u8; 32];
    for (i, &val) in computed.iter().enumerate() {
        computed_bytes[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
    }

    if computed_bytes != expected_hash {
        bail!("bitrot detected in block {}", block_index);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_path_format() {
        let path = shard_path("bucket", "key/with/slashes", "uuid-123", 1);
        assert_eq!(path, "bucket/key/with/slashes/uuid-123/part.1");
    }

    #[test]
    fn test_read_shard_block_detects_bitrot() {
        let shard_size = 1024i64;
        let mut data = vec![0u8; HASH_SIZE + shard_size as usize];

        // Compute correct hash for zeroed data
        let key = highway_key();
        let mut hasher = HighwayHasher::new(key);
        hasher.append(&data[HASH_SIZE..]);
        let hash = hasher.finalize256();
        for (i, &val) in hash.iter().enumerate() {
            data[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
        }

        // Corrupt one byte of data
        data[HASH_SIZE + 10] = 0xFF;

        let result = read_shard_block(&data, 0, shard_size, true);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bitrot"));
    }

    #[test]
    fn test_read_shard_block_valid_hash() {
        let shard_size = 64i64;
        let shard_data = vec![0xAB; shard_size as usize];
        let mut data = vec![0u8; HASH_SIZE + shard_size as usize];

        // Copy shard data
        data[HASH_SIZE..].copy_from_slice(&shard_data);

        // Compute correct hash
        let key = highway_key();
        let mut hasher = HighwayHasher::new(key);
        hasher.append(&shard_data);
        let hash = hasher.finalize256();
        for (i, &val) in hash.iter().enumerate() {
            data[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
        }

        let result = read_shard_block(&data, 0, shard_size, true).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), shard_data);
    }

    #[test]
    fn test_read_shard_block_skip_verification() {
        let shard_size = 64i64;
        let mut data = vec![0u8; HASH_SIZE + shard_size as usize];

        // Set shard data but leave hash as zeros (invalid)
        data[HASH_SIZE..].fill(0xAB);

        // Without verification, should still return data
        let result = read_shard_block(&data, 0, shard_size, false).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), shard_size as usize);
    }

    #[test]
    fn test_read_shard_block_returns_none_past_end() {
        let shard_size = 64i64;
        let data = vec![0u8; HASH_SIZE + shard_size as usize];

        // Block 1 should not exist
        let result = read_shard_block(&data, 1, shard_size, false).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_shard_too_small_error() {
        let data = vec![0u8; HASH_SIZE - 1];
        let result = read_shard_block(&data, 0, 1024, false);
        assert!(result.is_err());
    }
}
