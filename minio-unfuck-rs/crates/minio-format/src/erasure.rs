//! Reed-Solomon erasure decoder
//!
//! Port of erasure/decoder.go. Reconstructs objects from erasure-coded shards.

use anyhow::{bail, Context, Result};
use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::shard;
use crate::types::ObjectMeta;

/// Trait for reading shard data. Abstracts over filesystem vs raw device reads.
pub trait ShardReader {
    /// Read a shard file for the given disk, returning its full contents.
    /// Returns Ok(None) if the shard is missing (disk unavailable, file not found).
    fn read_shard(
        &self,
        disk_index: usize,
        bucket: &str,
        key: &str,
        data_dir: &str,
        part_number: i32,
    ) -> Result<Option<Vec<u8>>>;
}

/// Filesystem-based shard reader (reads from disk paths)
pub struct FsShardReader {
    pub disk_paths: Vec<String>,
}

impl ShardReader for FsShardReader {
    fn read_shard(
        &self,
        disk_index: usize,
        bucket: &str,
        key: &str,
        data_dir: &str,
        part_number: i32,
    ) -> Result<Option<Vec<u8>>> {
        if disk_index >= self.disk_paths.len() || self.disk_paths[disk_index].is_empty() {
            return Ok(None);
        }
        let path = format!(
            "{}/{}/{}/{}/part.{}",
            self.disk_paths[disk_index], bucket, key, data_dir, part_number
        );
        match std::fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

/// Decode a complete object from erasure-coded shards.
///
/// For each part → decode all blocks → truncate to part.size.
/// Concatenate all parts → truncate to meta.size.
pub fn decode_object(
    reader: &dyn ShardReader,
    meta: &ObjectMeta,
    skip_disks: &[usize],
) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(meta.size as usize);

    for part in &meta.parts {
        let part_data = decode_part(reader, meta, part.number, part.size, skip_disks)?;
        result.extend_from_slice(&part_data);
    }

    // Trim to actual object size
    if result.len() as i64 > meta.size {
        result.truncate(meta.size as usize);
    }

    Ok(result)
}

/// Decode a single part of an object
fn decode_part(
    reader: &dyn ShardReader,
    meta: &ObjectMeta,
    part_number: i32,
    part_size: i64,
    skip_disks: &[usize],
) -> Result<Vec<u8>> {
    let data_dir = meta.data_dir_string();
    let shard_size = meta.shard_size();

    // Calculate number of blocks in this part
    let num_blocks = if part_size == 0 {
        1
    } else {
        ((part_size + meta.block_size - 1) / meta.block_size) as usize
    };

    let mut result = Vec::with_capacity(part_size as usize);

    for block in 0..num_blocks {
        let block_data = decode_block(
            reader,
            meta,
            &data_dir,
            part_number,
            block,
            shard_size,
            skip_disks,
        )
        .with_context(|| format!("decode block {}", block))?;
        result.extend_from_slice(&block_data);
    }

    // Trim to part size
    if result.len() as i64 > part_size {
        result.truncate(part_size as usize);
    }

    Ok(result)
}

/// Decode a single block of a part
fn decode_block(
    reader: &dyn ShardReader,
    meta: &ObjectMeta,
    data_dir: &str,
    part_number: i32,
    block_index: usize,
    shard_size: i64,
    skip_disks: &[usize],
) -> Result<Vec<u8>> {
    let data_blocks = meta.data_blocks;
    let parity_blocks = meta.parity_blocks;
    let total_shards = data_blocks + parity_blocks;

    // Build reverse mapping: shard_idx (0-based) -> disk_idx (0-based)
    // Distribution[disk_idx] = erasure_index (1-based shard number)
    let mut shard_to_disk: Vec<Option<usize>> = vec![None; total_shards];
    for (disk_idx, &erasure_idx) in meta.distribution.iter().enumerate() {
        let shard_idx = erasure_idx as usize - 1; // 1-based to 0-based
        if shard_idx < total_shards {
            shard_to_disk[shard_idx] = Some(disk_idx);
        }
    }

    // Helper to read one shard block
    let read_one_shard = |shard_idx: usize| -> Option<Vec<u8>> {
        let disk_idx = shard_to_disk[shard_idx]?;
        if skip_disks.contains(&disk_idx) {
            return None;
        }
        let shard_data = reader
            .read_shard(disk_idx, &meta.bucket, &meta.key, data_dir, part_number)
            .ok()??;
        shard::read_shard_block(&shard_data, block_index, shard_size, true)
            .ok()?
    };

    // Step 1: Read only data shards (first data_blocks)
    let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(total_shards);
    for shard_idx in 0..data_blocks {
        shards.push(read_one_shard(shard_idx));
    }

    // Count successful data shards
    let data_success = shards.iter().filter(|s| s.is_some()).count();

    // Step 2: If all data shards present, fast path — just concatenate
    if data_success == data_blocks {
        let mut block_data = Vec::new();
        for s in &shards {
            block_data.extend_from_slice(s.as_ref().unwrap());
        }
        return Ok(block_data);
    }

    // Step 3: Need reconstruction — read parity shards
    for shard_idx in data_blocks..total_shards {
        shards.push(read_one_shard(shard_idx));
    }

    let available = shards.iter().filter(|s| s.is_some()).count();
    if available < data_blocks {
        bail!(
            "insufficient shards: have {}, need {}",
            available,
            data_blocks
        );
    }

    // Create Reed-Solomon decoder
    let rs = ReedSolomon::new(data_blocks, parity_blocks)
        .map_err(|e| anyhow::anyhow!("create RS encoder: {:?}", e))?;

    // Normalize shard sizes — all must be the same length for RS
    let max_size = shards
        .iter()
        .filter_map(|s| s.as_ref().map(|v| v.len()))
        .max()
        .unwrap_or(0);

    // Pad shorter shards to max_size
    let mut rs_shards: Vec<Option<Vec<u8>>> = shards
        .into_iter()
        .map(|s| {
            s.map(|mut v| {
                v.resize(max_size, 0);
                v
            })
        })
        .collect();

    // Reconstruct missing data shards
    rs.reconstruct_data(&mut rs_shards)
        .map_err(|e| anyhow::anyhow!("reconstruction failed: {:?}", e))?;

    // Concatenate data shards
    let mut block_data = Vec::with_capacity(data_blocks * max_size);
    for i in 0..data_blocks {
        if let Some(ref shard_data) = rs_shards[i] {
            block_data.extend_from_slice(shard_data);
        } else {
            bail!("data shard {} still missing after reconstruction", i);
        }
    }

    Ok(block_data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PartMeta;

    /// Mock ShardReader for testing
    struct MockShardReader {
        shards: Vec<Option<Vec<u8>>>,
    }

    impl ShardReader for MockShardReader {
        fn read_shard(
            &self,
            disk_index: usize,
            _bucket: &str,
            _key: &str,
            _data_dir: &str,
            _part_number: i32,
        ) -> Result<Option<Vec<u8>>> {
            Ok(self.shards.get(disk_index).cloned().flatten())
        }
    }

    #[test]
    fn test_fs_shard_reader_file_not_found() {
        let reader = FsShardReader {
            disk_paths: vec!["/nonexistent".into()],
        };
        let result = reader
            .read_shard(0, "bucket", "key", "datadir", 1)
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_fs_shard_reader_disk_index_out_of_range() {
        let reader = FsShardReader { disk_paths: vec![] };
        let result = reader
            .read_shard(5, "bucket", "key", "datadir", 1)
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_with_mock_reader_all_present() {
        use crate::shard::{highway_key, HASH_SIZE};
        use highway::{HighwayHash, HighwayHasher};

        // Create a simple test case with 2 data blocks, 1 parity block
        let data_blocks = 2;
        let parity_blocks = 1;
        let shard_size = 4;

        // Create shard data with valid hashes
        fn make_shard_with_hash(data: &[u8]) -> Vec<u8> {
            let mut result = vec![0u8; HASH_SIZE + data.len()];
            result[HASH_SIZE..].copy_from_slice(data);

            let key = highway_key();
            let mut hasher = HighwayHasher::new(key);
            hasher.append(data);
            let hash = hasher.finalize256();
            for (i, &val) in hash.iter().enumerate() {
                result[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
            }
            result
        }

        let shard0_data = [0xAA, 0xBB, 0xCC, 0xDD];
        let shard1_data = [0x11, 0x22, 0x33, 0x44];

        // Create parity (XOR of data shards)
        let mut parity_data = [0u8; 4];
        for i in 0..4 {
            parity_data[i] = shard0_data[i] ^ shard1_data[i];
        }

        let reader = MockShardReader {
            shards: vec![
                Some(make_shard_with_hash(&shard0_data)),
                Some(make_shard_with_hash(&shard1_data)),
                Some(make_shard_with_hash(&parity_data)),
            ],
        };

        let mut meta = ObjectMeta::default();
        meta.bucket = "test".to_string();
        meta.key = "key".to_string();
        meta.data_blocks = data_blocks;
        meta.parity_blocks = parity_blocks;
        meta.block_size = (shard_size * data_blocks) as i64;
        meta.size = (shard_size * data_blocks) as i64;
        meta.distribution = vec![1, 2, 3]; // disk 0 -> shard 1, disk 1 -> shard 2, disk 2 -> shard 3
        meta.parts = vec![PartMeta {
            number: 1,
            size: meta.size,
            actual_size: meta.size,
        }];

        let result = decode_object(&reader, &meta, &[]).unwrap();
        assert_eq!(result.len(), (shard_size * data_blocks) as usize);
        assert_eq!(&result[0..4], &shard0_data);
        assert_eq!(&result[4..8], &shard1_data);
    }
}
