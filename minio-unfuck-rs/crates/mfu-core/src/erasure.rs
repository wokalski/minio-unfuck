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
    use std::path::PathBuf;

    fn disks_root() -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.pop();
        path.push(".disks");
        path
    }

    fn make_reader() -> FsShardReader {
        let root = disks_root();
        let mut paths = Vec::new();
        for i in 1..=16 {
            paths.push(format!("{}/storage{}", root.display(), i));
        }
        FsShardReader { disk_paths: paths }
    }

    #[test]
    fn test_decode_object_all_shards() {
        let root = disks_root();
        let meta_path = root.join("storage1/recordings/12e6r9jZSmuSQjNj2rUSOx.wav/xl.meta");
        if !meta_path.exists() {
            eprintln!("skipping test: fixture not found");
            return;
        }

        let meta_data = std::fs::read(&meta_path).unwrap();
        let mut meta = crate::xlmeta::parse(&meta_data).unwrap();
        meta.bucket = "recordings".to_string();
        meta.key = "12e6r9jZSmuSQjNj2rUSOx.wav".to_string();

        let reader = make_reader();
        let result = decode_object(&reader, &meta, &[]).unwrap();
        assert_eq!(result.len() as i64, meta.size);

        // Check WAV header magic
        if result.len() >= 4 {
            assert_eq!(&result[..4], b"RIFF", "should be a valid WAV file");
        }
    }

    #[test]
    fn test_decode_object_with_skip() {
        let root = disks_root();
        let meta_path = root.join("storage1/recordings/12e6r9jZSmuSQjNj2rUSOx.wav/xl.meta");
        if !meta_path.exists() {
            eprintln!("skipping test: fixture not found");
            return;
        }

        let meta_data = std::fs::read(&meta_path).unwrap();
        let mut meta = crate::xlmeta::parse(&meta_data).unwrap();
        meta.bucket = "recordings".to_string();
        meta.key = "12e6r9jZSmuSQjNj2rUSOx.wav".to_string();

        let reader = make_reader();

        // Skip up to parity_blocks disks — should still reconstruct
        let skip: Vec<usize> = (0..meta.parity_blocks).collect();
        let result = decode_object(&reader, &meta, &skip).unwrap();
        assert_eq!(result.len() as i64, meta.size);

        // Compare with non-skip result
        let reference = decode_object(&reader, &meta, &[]).unwrap();
        assert_eq!(result, reference, "reconstruction should produce identical output");
    }
}
