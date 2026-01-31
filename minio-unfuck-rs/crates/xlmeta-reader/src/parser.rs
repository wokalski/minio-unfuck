//! xl.meta parsing logic
//!
//! Converts raw xl.meta bytes into ChObject records.

use mfu_core::xlmeta;

use crate::types::{ChObject, ReadyXlmeta, ReaderResult};

/// Parse xl.meta buffer into ChObject or error
pub fn parse_xlmeta(buffer: &[u8], xlmeta: &ReadyXlmeta) -> ReaderResult {
    match xlmeta::parse(buffer) {
        Ok(meta) => {
            let compression = meta
                .user_meta
                .get("x-minio-internal-compression")
                .cloned()
                .unwrap_or_default();

            let parts_json = serde_json::to_string(
                &meta
                    .parts
                    .iter()
                    .map(|p| {
                        serde_json::json!({
                            "number": p.number,
                            "size": p.size,
                            "actual_size": p.actual_size
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_else(|_| "[]".to_string());

            let data_dir = meta.data_dir_string();
            let distribution: Vec<i32> = meta.distribution.iter().map(|&d| d as i32).collect();

            ReaderResult::Parsed(ChObject {
                bucket: xlmeta.bucket.clone(),
                key: xlmeta.key.clone(),
                size: meta.size,
                mod_time: meta.mod_time,
                etag: meta.etag,
                content_type: meta.content_type,
                compression,
                data_blocks: meta.data_blocks as i32,
                parity_blocks: meta.parity_blocks as i32,
                block_size: meta.block_size,
                data_dir,
                distribution,
                parts_json,
                xlmeta_device_id: xlmeta.device_id,
                xlmeta_ino: xlmeta.ino,
            })
        }
        Err(e) => ReaderResult::Error {
            bucket: xlmeta.bucket.clone(),
            key: xlmeta.key.clone(),
            error: format!("parse error: {}", e),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mfu_core::types::Extent;

    #[test]
    fn test_parse_invalid_xlmeta() {
        let xlmeta = ReadyXlmeta {
            bucket: "test-bucket".to_string(),
            key: "test-key".to_string(),
            device_id: 0,
            ino: 12345,
            extents: vec![Extent {
                logical_offset: 0,
                physical_offset: 0,
                length: 4,
            }],
            file_size: 4,
        };

        // Invalid xl.meta data (just zeros)
        let buffer = vec![0u8; 4];
        let result = parse_xlmeta(&buffer, &xlmeta);

        match result {
            ReaderResult::Error { bucket, key, error } => {
                assert_eq!(bucket, "test-bucket");
                assert_eq!(key, "test-key");
                assert!(error.contains("parse error"));
            }
            ReaderResult::Parsed(_) => panic!("Expected error for invalid data"),
        }
    }
}
