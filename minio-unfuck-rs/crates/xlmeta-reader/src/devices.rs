//! Device resolution utilities
//!
//! Resolves block device paths to partitions.

use std::path::Path;

use anyhow::Result;
use tracing::info;

/// Resolve `--root` paths to actual device paths.
///
/// If a path refers to a whole disk (e.g., /dev/sdg), expands it to
/// all its partitions in numerical order (e.g., /dev/sdg1, /dev/sdg2, ...).
/// Otherwise, returns the path as-is.
pub fn resolve_devices(roots: &[String]) -> Result<Vec<String>> {
    let mut devices = Vec::new();
    for root in roots {
        let path = Path::new(root);

        if let Some(dev_name) = path.file_name().and_then(|n| n.to_str()) {
            let sysfs_dir = Path::new("/sys/block").join(dev_name);
            if sysfs_dir.is_dir() {
                let mut parts: Vec<String> = std::fs::read_dir(&sysfs_dir)?
                    .filter_map(|e| e.ok())
                    .filter_map(|e| {
                        let name = e.file_name().to_string_lossy().into_owned();
                        if name.starts_with(dev_name) && name.ends_with(|c: char| c.is_ascii_digit())
                        {
                            Some(format!("/dev/{}", name))
                        } else {
                            None
                        }
                    })
                    .collect();

                if !parts.is_empty() {
                    parts.sort_by(|a, b| {
                        let num_a = a
                            .trim_start_matches(&format!("/dev/{}", dev_name))
                            .parse::<u32>()
                            .unwrap_or(0);
                        let num_b = b
                            .trim_start_matches(&format!("/dev/{}", dev_name))
                            .parse::<u32>()
                            .unwrap_or(0);
                        num_a.cmp(&num_b)
                    });
                    info!(
                        "{} is a whole disk with {} partition(s), expanding",
                        root,
                        parts.len()
                    );
                    devices.extend(parts);
                    continue;
                }
            }
        }

        devices.push(root.clone());
    }
    Ok(devices)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_single_partition() {
        // A single partition path should be returned as-is
        let result = resolve_devices(&["/dev/sda1".to_string()]).unwrap();
        assert_eq!(result, vec!["/dev/sda1".to_string()]);
    }

    #[test]
    fn test_resolve_nonexistent_path() {
        // A nonexistent path should be returned as-is
        let result = resolve_devices(&["/dev/nonexistent".to_string()]).unwrap();
        assert_eq!(result, vec!["/dev/nonexistent".to_string()]);
    }
}
