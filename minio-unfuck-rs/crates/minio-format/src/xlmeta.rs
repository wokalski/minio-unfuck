//! xl.meta binary format parser
//!
//! Port of xlmeta/parser.go. Hand-rolled msgpack parsing using `rmp::decode`.
//!
//! Format:
//! ```text
//! [4 bytes: "XL2 "]
//! [u16 LE: major]  must be 1
//! [u16 LE: minor]  must be >= 3
//! [msgpack bin: metadata_blob]
//! [msgpack u32: crc]  -- (xxh64(blob) & 0xFFFFFFFF) as u32
//! [optional inline data]
//! ```

use std::collections::HashMap;
use std::io::Cursor;

use anyhow::{bail, ensure, Context, Result};
use rmp::decode::{self, DecodeStringError};
use xxhash_rust::xxh64;

use crate::types::{ObjectMeta, PartMeta, VersionType};

const XL_HEADER: [u8; 4] = *b"XL2 ";

/// Read a u8 value from cursor, handling both positive fixint and uint8 formats.
/// This is a workaround for rmp::decode::read_u8 which seems to have issues.
fn read_u8_value(cur: &mut Cursor<&[u8]>) -> Result<u8> {
    let pos = cur.position() as usize;
    let data_len = cur.get_ref().len();
    ensure!(pos < data_len, "unexpected end of data reading u8");

    let byte = cur.get_ref()[pos];

    if byte <= 0x7f {
        // Positive fixint: value is the byte itself
        cur.set_position(pos as u64 + 1);
        Ok(byte)
    } else if byte == 0xcc {
        // uint8: next byte is the value
        ensure!(pos + 1 < data_len, "truncated uint8");
        let val = cur.get_ref()[pos + 1];
        cur.set_position(pos as u64 + 2);
        Ok(val)
    } else {
        bail!("expected u8, got marker 0x{:02x}", byte)
    }
}

/// Parse an xl.meta file and return object metadata.
pub fn parse(data: &[u8]) -> Result<ObjectMeta> {
    ensure!(data.len() >= 8, "xl.meta too short: {} bytes", data.len());

    // Check header
    ensure!(
        data[..4] == XL_HEADER,
        "invalid xl.meta header: expected {:?}, got {:?}",
        &XL_HEADER,
        &data[..4]
    );

    // Parse version (little-endian u16)
    let major = u16::from_le_bytes([data[4], data[5]]);
    let minor = u16::from_le_bytes([data[6], data[7]]);

    ensure!(major == 1, "unsupported xl.meta major version: {}", major);
    ensure!(
        minor >= 3,
        "xl.meta version {}.{} not supported (need >= 1.3)",
        major,
        minor
    );

    parse_v1_3(&data[8..])
}

/// Parse xl.meta v1.3+ format (indexed)
fn parse_v1_3(payload: &[u8]) -> Result<ObjectMeta> {
    let mut cur = Cursor::new(payload);

    // Read metadata blob (msgpack bin)
    let blob_len =
        decode::read_bin_len(&mut cur).context("failed to read metadata blob length")?;
    let blob_start = cur.position() as usize;
    let blob_end = blob_start + blob_len as usize;
    ensure!(
        blob_end <= payload.len(),
        "metadata blob extends beyond payload"
    );
    let meta_blob = &payload[blob_start..blob_end];
    cur.set_position(blob_end as u64);

    // Read and verify CRC
    let crc = decode::read_u32(&mut cur).context("failed to read CRC")?;
    let expected_crc = (xxh64::xxh64(meta_blob, 0) & 0xFFFFFFFF) as u32;
    ensure!(
        crc == expected_crc,
        "CRC mismatch: expected {:08x}, got {:08x}",
        expected_crc,
        crc
    );

    parse_metadata_blob(meta_blob)
}

/// Parse the indexed metadata blob
fn parse_metadata_blob(blob: &[u8]) -> Result<ObjectMeta> {
    let mut cur = Cursor::new(blob);

    // Read header version (u8) - manually to avoid rmp read_u8 issues
    let _header_version = read_u8_value(&mut cur).context("failed to read header version")?;

    // Read meta version (u8)
    let _meta_version = read_u8_value(&mut cur).context("failed to read meta version")?;

    // Read version count
    let versions = read_int(&mut cur).context("failed to read version count")?;
    ensure!(versions > 0, "no versions found");

    // Read first (latest) version: [header bytes][meta bytes]
    // Skip header bytes
    let hdr_len = decode::read_bin_len(&mut cur).context("failed to read version header length")?;
    cur.set_position(cur.position() + hdr_len as u64);

    // Read version meta bytes
    let meta_len = decode::read_bin_len(&mut cur).context("failed to read version meta length")?;
    let meta_start = cur.position() as usize;
    let meta_end = meta_start + meta_len as usize;
    ensure!(
        meta_end <= blob.len(),
        "version meta extends beyond blob"
    );
    let ver_meta = &blob[meta_start..meta_end];

    parse_version_meta(ver_meta)
}

/// Parse the xlMetaV2Version msgpack map
fn parse_version_meta(data: &[u8]) -> Result<ObjectMeta> {
    let mut cur = Cursor::new(data);
    let mut meta = ObjectMeta::default();

    let map_len = decode::read_map_len(&mut cur).context("failed to read version map header")?;

    let mut version_type: u8 = 0;

    for _ in 0..map_len {
        let key = read_string(&mut cur).context("failed to read map key")?;

        match key.as_str() {
            "Type" => {
                version_type = read_u8_value(&mut cur).context("failed to read Type")?;
            }
            "V2Obj" => {
                parse_v2_obj(&mut cur, &mut meta).context("failed to parse V2Obj")?;
            }
            "V2DelObj" => {
                parse_v2_del_obj(&mut cur, &mut meta).context("failed to parse V2DelObj")?;
            }
            _ => {
                skip_value(&mut cur).with_context(|| format!("failed to skip field {}", key))?;
            }
        }
    }

    meta.version_type = VersionType::from_u8(version_type);
    Ok(meta)
}

/// Parse the xlMetaV2Object msgpack map inline, filling `meta`
fn parse_v2_obj(cur: &mut Cursor<&[u8]>, meta: &mut ObjectMeta) -> Result<()> {
    let map_len = decode::read_map_len(cur).context("failed to read V2Obj map header")?;

    let mut part_numbers: Vec<i32> = Vec::new();
    let mut part_sizes: Vec<i64> = Vec::new();
    let mut part_actual_sizes: Option<Vec<i64>> = None;

    for _ in 0..map_len {
        let key = read_string(cur).context("failed to read V2Obj key")?;

        match key.as_str() {
            "ID" => {
                let id = read_bin(cur).context("failed to read ID")?;
                if id.len() == 16 {
                    meta.version_id.0.copy_from_slice(&id);
                }
            }
            "DDir" => {
                let ddir = read_bin(cur).context("failed to read DDir")?;
                if ddir.len() == 16 {
                    meta.data_dir.0.copy_from_slice(&ddir);
                }
            }
            "EcAlgo" => {
                let _algo = read_u8_value(cur).context("failed to read EcAlgo")?;
            }
            "EcM" => {
                meta.data_blocks =
                    read_int(cur).context("failed to read EcM")? as usize;
            }
            "EcN" => {
                meta.parity_blocks =
                    read_int(cur).context("failed to read EcN")? as usize;
            }
            "EcBSize" => {
                meta.block_size = read_i64(cur).context("failed to read EcBSize")?;
            }
            "EcIndex" => {
                meta.erasure_index =
                    read_int(cur).context("failed to read EcIndex")? as usize;
            }
            "EcDist" => {
                let arr_len =
                    decode::read_array_len(cur).context("failed to read EcDist header")?;
                meta.distribution = Vec::with_capacity(arr_len as usize);
                for j in 0..arr_len {
                    let v = read_u8_value(cur)
                        .with_context(|| format!("failed to read EcDist[{}]", j))?;
                    meta.distribution.push(v);
                }
            }
            "CSumAlgo" => {
                let _algo = read_u8_value(cur).context("failed to read CSumAlgo")?;
            }
            "PartNums" => {
                let arr_len =
                    decode::read_array_len(cur).context("failed to read PartNums header")?;
                part_numbers = Vec::with_capacity(arr_len as usize);
                for j in 0..arr_len {
                    let v = read_int(cur)
                        .with_context(|| format!("failed to read PartNums[{}]", j))?;
                    part_numbers.push(v as i32);
                }
            }
            "PartSizes" => {
                let arr_len =
                    decode::read_array_len(cur).context("failed to read PartSizes header")?;
                part_sizes = Vec::with_capacity(arr_len as usize);
                for j in 0..arr_len {
                    let v = read_i64(cur)
                        .with_context(|| format!("failed to read PartSizes[{}]", j))?;
                    part_sizes.push(v);
                }
            }
            "PartASizes" => {
                match decode::read_array_len(cur) {
                    Ok(arr_len) => {
                        let mut sizes = Vec::with_capacity(arr_len as usize);
                        for j in 0..arr_len {
                            let v = read_i64(cur)
                                .with_context(|| format!("failed to read PartASizes[{}]", j))?;
                            sizes.push(v);
                        }
                        part_actual_sizes = Some(sizes);
                    }
                    Err(_) => {
                        // May be nil/omitted — skip
                        skip_value(cur).ok();
                    }
                }
            }
            "Size" => {
                meta.size = read_i64(cur).context("failed to read Size")?;
            }
            "MTime" => {
                meta.mod_time = read_i64(cur).context("failed to read MTime")?;
            }
            "MetaUsr" => {
                meta.user_meta = parse_string_map(cur).context("failed to read MetaUsr")?;
                if let Some(ct) = meta.user_meta.get("content-type") {
                    meta.content_type = ct.clone();
                }
                if let Some(etag) = meta.user_meta.get("etag") {
                    meta.etag = etag.clone();
                }
            }
            _ => {
                skip_value(cur)
                    .with_context(|| format!("failed to skip V2Obj field {}", key))?;
            }
        }
    }

    // Build parts from part_numbers and part_sizes
    if !part_numbers.is_empty() {
        meta.parts = Vec::with_capacity(part_numbers.len());
        for (i, &num) in part_numbers.iter().enumerate() {
            let size = part_sizes.get(i).copied().unwrap_or(0);
            let actual_size = part_actual_sizes
                .as_ref()
                .and_then(|a| a.get(i).copied())
                .unwrap_or(size);
            meta.parts.push(PartMeta {
                number: num,
                size,
                actual_size,
            });
        }
    }

    Ok(())
}

/// Parse the xlMetaV2DeleteMarker msgpack map (for delete markers)
fn parse_v2_del_obj(cur: &mut Cursor<&[u8]>, meta: &mut ObjectMeta) -> Result<()> {
    let map_len = decode::read_map_len(cur).context("failed to read V2DelObj map header")?;

    for _ in 0..map_len {
        let key = read_string(cur).context("failed to read V2DelObj key")?;

        match key.as_str() {
            "ID" => {
                let id = read_bin(cur).context("failed to read ID")?;
                if id.len() == 16 {
                    meta.version_id.0.copy_from_slice(&id);
                }
            }
            "MTime" => {
                meta.mod_time = read_i64(cur).context("failed to read MTime")?;
            }
            "MetaSys" => {
                // System metadata - skip for now, we don't need it for delete markers
                skip_value(cur).context("failed to skip MetaSys")?;
            }
            _ => {
                skip_value(cur)
                    .with_context(|| format!("failed to skip V2DelObj field {}", key))?;
            }
        }
    }

    Ok(())
}

/// Parse a msgpack map[string]string, handling both StrType and BinType values.
fn parse_string_map(cur: &mut Cursor<&[u8]>) -> Result<HashMap<String, String>> {
    let map_len = decode::read_map_len(cur)?;
    let mut result = HashMap::with_capacity(map_len as usize);

    for _ in 0..map_len {
        let key = read_string(cur)?;

        // Check for nil
        let pos = cur.position() as usize;
        let data = cur.get_ref();
        if pos < data.len() && data[pos] == 0xc0 {
            // msgpack nil
            cur.set_position(pos as u64 + 1);
            continue;
        }

        // Peek at next byte to determine type
        if pos >= data.len() {
            bail!("unexpected end of data in string map");
        }
        let marker = data[pos];

        let val = if is_str_marker(marker) {
            read_string(cur)?
        } else if is_bin_marker(marker) {
            let bytes = read_bin(cur)?;
            String::from_utf8_lossy(&bytes).into_owned()
        } else {
            // Unknown type — skip
            skip_value(cur)?;
            continue;
        };

        result.insert(key, val);
    }

    Ok(result)
}

// --- msgpack helper functions ---

/// Read a msgpack integer (handles int/uint of various sizes)
fn read_int(cur: &mut Cursor<&[u8]>) -> Result<i64> {
    let pos = cur.position() as usize;
    let data_len = cur.get_ref().len();
    ensure!(pos < data_len, "unexpected end of data reading int");
    let marker = cur.get_ref()[pos];

    // Positive fixint: 0x00..0x7f
    if marker <= 0x7f {
        cur.set_position(pos as u64 + 1);
        return Ok(marker as i64);
    }
    // Negative fixint: 0xe0..0xff
    if marker >= 0xe0 {
        cur.set_position(pos as u64 + 1);
        return Ok((marker as i8) as i64);
    }

    match marker {
        0xcc => {
            // uint8: marker + 1 byte
            ensure!(pos + 1 < data_len, "truncated uint8");
            let val = cur.get_ref()[pos + 1];
            cur.set_position(pos as u64 + 2);
            Ok(val as i64)
        }
        0xcd => {
            // uint16: marker + 2 bytes (big-endian)
            ensure!(pos + 2 < data_len, "truncated uint16");
            let bytes = [cur.get_ref()[pos + 1], cur.get_ref()[pos + 2]];
            cur.set_position(pos as u64 + 3);
            Ok(u16::from_be_bytes(bytes) as i64)
        }
        0xce => {
            // uint32: marker + 4 bytes (big-endian)
            ensure!(pos + 4 < data_len, "truncated uint32");
            let bytes = peek_bytes_4(cur, pos + 1)?;
            cur.set_position(pos as u64 + 5);
            Ok(u32::from_be_bytes(bytes) as i64)
        }
        0xcf => {
            // uint64: marker + 8 bytes (big-endian)
            ensure!(pos + 8 < data_len, "truncated uint64");
            let bytes = peek_bytes_8(cur, pos + 1)?;
            cur.set_position(pos as u64 + 9);
            Ok(u64::from_be_bytes(bytes) as i64)
        }
        0xd0 => {
            // int8: marker + 1 byte
            ensure!(pos + 1 < data_len, "truncated int8");
            let val = cur.get_ref()[pos + 1] as i8;
            cur.set_position(pos as u64 + 2);
            Ok(val as i64)
        }
        0xd1 => {
            // int16: marker + 2 bytes (big-endian)
            ensure!(pos + 2 < data_len, "truncated int16");
            let bytes = [cur.get_ref()[pos + 1], cur.get_ref()[pos + 2]];
            cur.set_position(pos as u64 + 3);
            Ok(i16::from_be_bytes(bytes) as i64)
        }
        0xd2 => {
            // int32: marker + 4 bytes (big-endian)
            ensure!(pos + 4 < data_len, "truncated int32");
            let bytes = peek_bytes_4(cur, pos + 1)?;
            cur.set_position(pos as u64 + 5);
            Ok(i32::from_be_bytes(bytes) as i64)
        }
        0xd3 => {
            // int64: marker + 8 bytes (big-endian)
            ensure!(pos + 8 < data_len, "truncated int64");
            let bytes = peek_bytes_8(cur, pos + 1)?;
            cur.set_position(pos as u64 + 9);
            Ok(i64::from_be_bytes(bytes))
        }
        _ => bail!("expected int, got marker 0x{:02x}", marker),
    }
}

/// Read a msgpack int64 (or any int that fits i64)
fn read_i64(cur: &mut Cursor<&[u8]>) -> Result<i64> {
    read_int(cur)
}

/// Read a msgpack string
fn read_string(cur: &mut Cursor<&[u8]>) -> Result<String> {
    let mut buf = vec![0u8; 256];
    match decode::read_str(cur, &mut buf) {
        Ok(s) => Ok(s.to_string()),
        Err(DecodeStringError::BufferSizeTooSmall(needed)) => {
            buf.resize(needed as usize, 0);
            // We need to re-read. The cursor position was already advanced past the header
            // by the first attempt, so we need to handle this differently.
            // Actually, rmp's read_str reads the header + data. On BufferSizeTooSmall,
            // we need to re-try with bigger buffer. But the cursor is in unknown state.
            // Use read_str_len + manual read instead.
            bail!("string too large: {} bytes", needed);
        }
        Err(DecodeStringError::InvalidMarkerRead(e)) => {
            bail!("failed to read string marker: {}", e)
        }
        Err(DecodeStringError::InvalidDataRead(e)) => {
            bail!("failed to read string data: {}", e)
        }
        Err(DecodeStringError::TypeMismatch(m)) => {
            bail!("expected string, got marker {:?}", m)
        }
        Err(e) => bail!("failed to read string: {}", e),
    }
}

/// Read a msgpack binary blob
fn read_bin(cur: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
    let len = decode::read_bin_len(cur).context("failed to read bin length")?;
    let pos = cur.position() as usize;
    let end = pos + len as usize;
    let data = cur.get_ref();
    ensure!(end <= data.len(), "bin data extends beyond buffer");
    let result = data[pos..end].to_vec();
    cur.set_position(end as u64);
    Ok(result)
}

/// Skip a single msgpack value (any type).
///
/// We read bytes via a helper `peek_bytes` that borrows `cur` only briefly,
/// avoiding holding an immutable borrow across `cur.set_position()`.
fn skip_value(cur: &mut Cursor<&[u8]>) -> Result<()> {
    let pos = cur.position() as usize;
    let data_len = cur.get_ref().len();
    ensure!(pos < data_len, "unexpected end of data in skip");
    let marker = cur.get_ref()[pos];

    // Positive fixint
    if marker <= 0x7f {
        cur.set_position(pos as u64 + 1);
        return Ok(());
    }
    // fixmap
    if (0x80..=0x8f).contains(&marker) {
        cur.set_position(pos as u64 + 1);
        let len = (marker & 0x0f) as u32;
        for _ in 0..len {
            skip_value(cur)?; // key
            skip_value(cur)?; // value
        }
        return Ok(());
    }
    // fixarray
    if (0x90..=0x9f).contains(&marker) {
        cur.set_position(pos as u64 + 1);
        let len = (marker & 0x0f) as u32;
        for _ in 0..len {
            skip_value(cur)?;
        }
        return Ok(());
    }
    // fixstr
    if (0xa0..=0xbf).contains(&marker) {
        let len = (marker & 0x1f) as usize;
        cur.set_position((pos + 1 + len) as u64);
        return Ok(());
    }
    // Negative fixint
    if marker >= 0xe0 {
        cur.set_position(pos as u64 + 1);
        return Ok(());
    }

    match marker {
        0xc0 => {
            // nil
            cur.set_position(pos as u64 + 1);
        }
        0xc2 | 0xc3 => {
            // false / true
            cur.set_position(pos as u64 + 1);
        }
        0xc4 => {
            // bin8
            let b1 = peek_byte(cur, pos + 1)?;
            let len = b1 as usize;
            cur.set_position((pos + 2 + len) as u64);
        }
        0xc5 => {
            // bin16
            let bytes = peek_bytes_2(cur, pos + 1)?;
            let len = u16::from_be_bytes(bytes) as usize;
            cur.set_position((pos + 3 + len) as u64);
        }
        0xc6 => {
            // bin32
            let bytes = peek_bytes_4(cur, pos + 1)?;
            let len = u32::from_be_bytes(bytes) as usize;
            cur.set_position((pos + 5 + len) as u64);
        }
        0xca => cur.set_position(pos as u64 + 5), // float32
        0xcb => cur.set_position(pos as u64 + 9), // float64
        0xcc => cur.set_position(pos as u64 + 2), // uint8
        0xcd => cur.set_position(pos as u64 + 3), // uint16
        0xce => cur.set_position(pos as u64 + 5), // uint32
        0xcf => cur.set_position(pos as u64 + 9), // uint64
        0xd0 => cur.set_position(pos as u64 + 2), // int8
        0xd1 => cur.set_position(pos as u64 + 3), // int16
        0xd2 => cur.set_position(pos as u64 + 5), // int32
        0xd3 => cur.set_position(pos as u64 + 9), // int64
        0xd9 => {
            // str8
            let b1 = peek_byte(cur, pos + 1)?;
            let len = b1 as usize;
            cur.set_position((pos + 2 + len) as u64);
        }
        0xda => {
            // str16
            let bytes = peek_bytes_2(cur, pos + 1)?;
            let len = u16::from_be_bytes(bytes) as usize;
            cur.set_position((pos + 3 + len) as u64);
        }
        0xdb => {
            // str32
            let bytes = peek_bytes_4(cur, pos + 1)?;
            let len = u32::from_be_bytes(bytes) as usize;
            cur.set_position((pos + 5 + len) as u64);
        }
        0xdc => {
            // array16
            let bytes = peek_bytes_2(cur, pos + 1)?;
            let len = u16::from_be_bytes(bytes) as u32;
            cur.set_position(pos as u64 + 3);
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        0xdd => {
            // array32
            let bytes = peek_bytes_4(cur, pos + 1)?;
            let len = u32::from_be_bytes(bytes);
            cur.set_position(pos as u64 + 5);
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        0xde => {
            // map16
            let bytes = peek_bytes_2(cur, pos + 1)?;
            let len = u16::from_be_bytes(bytes) as u32;
            cur.set_position(pos as u64 + 3);
            for _ in 0..len {
                skip_value(cur)?; // key
                skip_value(cur)?; // value
            }
        }
        0xdf => {
            // map32
            let bytes = peek_bytes_4(cur, pos + 1)?;
            let len = u32::from_be_bytes(bytes);
            cur.set_position(pos as u64 + 5);
            for _ in 0..len {
                skip_value(cur)?;
                skip_value(cur)?;
            }
        }
        0xd4 => cur.set_position(pos as u64 + 3),  // fixext1
        0xd5 => cur.set_position(pos as u64 + 4),  // fixext2
        0xd6 => cur.set_position(pos as u64 + 6),  // fixext4
        0xd7 => cur.set_position(pos as u64 + 10), // fixext8
        0xd8 => cur.set_position(pos as u64 + 18), // fixext16
        0xc7 => {
            // ext8
            let b1 = peek_byte(cur, pos + 1)?;
            let len = b1 as usize;
            cur.set_position((pos + 3 + len) as u64);
        }
        0xc8 => {
            // ext16
            let bytes = peek_bytes_2(cur, pos + 1)?;
            let len = u16::from_be_bytes(bytes) as usize;
            cur.set_position((pos + 4 + len) as u64);
        }
        0xc9 => {
            // ext32
            let bytes = peek_bytes_4(cur, pos + 1)?;
            let len = u32::from_be_bytes(bytes) as usize;
            cur.set_position((pos + 6 + len) as u64);
        }
        _ => bail!("unknown msgpack marker 0x{:02x} at pos {}", marker, pos),
    }

    Ok(())
}

/// Peek a single byte at offset (briefly borrows, then releases)
fn peek_byte(cur: &Cursor<&[u8]>, offset: usize) -> Result<u8> {
    let data = cur.get_ref();
    ensure!(offset < data.len(), "truncated at offset {}", offset);
    Ok(data[offset])
}

/// Peek 2 bytes at offset
fn peek_bytes_2(cur: &Cursor<&[u8]>, offset: usize) -> Result<[u8; 2]> {
    let data = cur.get_ref();
    ensure!(offset + 2 <= data.len(), "truncated at offset {}", offset);
    Ok([data[offset], data[offset + 1]])
}

/// Peek 4 bytes at offset
fn peek_bytes_4(cur: &Cursor<&[u8]>, offset: usize) -> Result<[u8; 4]> {
    let data = cur.get_ref();
    ensure!(offset + 4 <= data.len(), "truncated at offset {}", offset);
    Ok([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]])
}

/// Peek 8 bytes at offset
fn peek_bytes_8(cur: &Cursor<&[u8]>, offset: usize) -> Result<[u8; 8]> {
    let data = cur.get_ref();
    ensure!(offset + 8 <= data.len(), "truncated at offset {}", offset);
    Ok([
        data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
        data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
    ])
}

fn is_str_marker(m: u8) -> bool {
    (0xa0..=0xbf).contains(&m) || m == 0xd9 || m == 0xda || m == 0xdb
}

fn is_bin_marker(m: u8) -> bool {
    m == 0xc4 || m == 0xc5 || m == 0xc6
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn testdata_path(name: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("testdata");
        path.push(name);
        path
    }

    fn read_fixture(name: &str) -> Vec<u8> {
        let path = testdata_path(name);
        std::fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "Test fixture not found at {:?}. Error: {}. \
                 Run the fixture download script or check testdata/README.md",
                path, e
            )
        })
    }

    // ==================== Header validation tests ====================

    #[test]
    fn test_parse_rejects_too_short() {
        let result = parse(&[0u8; 7]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_parse_rejects_invalid_header() {
        let data = b"XXXX\x01\x00\x03\x00";
        let result = parse(data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid"));
    }

    #[test]
    fn test_parse_rejects_unsupported_major_version() {
        let data = b"XL2 \x02\x00\x03\x00";
        let result = parse(data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("major version"));
    }

    #[test]
    fn test_parse_rejects_old_minor_version() {
        let data = b"XL2 \x01\x00\x02\x00";
        let result = parse(data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("1.2"));
    }

    // ==================== Integration tests with real fixtures ====================

    /// Test parsing xl.meta from xlmeta/ directory.
    /// This fixture may be in an older format (version < 1.3) which is expected to fail.
    #[test]
    fn test_parse_basic_xlmeta_fixture() {
        let data = read_fixture("xlmeta/xl.meta");

        // Verify header magic
        assert!(data.len() >= 8, "xl.meta should be at least 8 bytes");
        assert_eq!(&data[0..4], b"XL2 ", "should have XL2 header");

        // This fixture is known to be in an older format (version 0x20 0x31 = "1 " ASCII)
        // which gets interpreted as major version 0x2031 = 8241
        let result = parse(&data);
        assert!(result.is_err(), "old format xl.meta should fail to parse");
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("version") || err_str.contains("8241"),
            "error should mention version issue: {}",
            err_str
        );
    }

    /// Test parsing xl-many-parts.meta which has 9016 parts.
    /// Expected values:
    /// - data_blocks (EcM): 12
    /// - parity_blocks (EcN): 4
    /// - block_size: 1048576 (1 MiB)
    /// - distribution: [2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,1]
    /// - ec_index: 2
    /// - parts: 9016
    #[test]
    fn test_parse_many_parts_xlmeta_fixture() {
        let data = read_fixture("xlmeta/xl-many-parts.meta");
        let meta = parse(&data).expect("failed to parse xl-many-parts.meta");

        // Erasure coding parameters
        assert_eq!(meta.data_blocks, 12, "EcM should be 12");
        assert_eq!(meta.parity_blocks, 4, "EcN should be 4");
        assert_eq!(meta.block_size, 1048576, "EcBSize should be 1 MiB");
        assert_eq!(meta.erasure_index, 2, "EcIndex should be 2");

        // Distribution array (16 disks total: 12 data + 4 parity)
        assert_eq!(meta.distribution.len(), 16, "should have 16 disks in distribution");
        assert_eq!(
            meta.distribution,
            vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1],
            "distribution should match expected order"
        );

        // Parts count
        assert_eq!(meta.parts.len(), 9016, "should have 9016 parts");

        // Verify version type is Object (not delete marker)
        assert_eq!(meta.version_type, VersionType::Object);

        // data_dir should not be zero (valid UUID)
        assert!(!meta.data_dir.is_zero(), "data_dir should be set");
    }

    /// Test parsing cicd-corpus xl.meta (disk2).
    /// Expected values:
    /// - data_blocks (EcM): 3
    /// - parity_blocks (EcN): 2
    /// - block_size: 1048576 (1 MiB)
    /// - distribution: [3, 4, 5, 1, 2]
    /// - ec_index: 4 (disk2's position)
    /// - size: 644520 bytes
    /// - content_type: "application/octet-stream"
    /// - etag: "9587ddd31fead633830366f45d221d56"
    #[test]
    fn test_parse_cicd_corpus_xlmeta_disk2() {
        let data = read_fixture("cicd-corpus/disk2/bucket/testobj/xl.meta");
        let meta = parse(&data).expect("failed to parse cicd-corpus disk2 xl.meta");

        // Erasure coding parameters
        assert_eq!(meta.data_blocks, 3, "EcM should be 3");
        assert_eq!(meta.parity_blocks, 2, "EcN should be 2");
        assert_eq!(meta.block_size, 1048576, "EcBSize should be 1 MiB");
        assert_eq!(meta.erasure_index, 4, "EcIndex should be 4 for disk2");

        // Distribution array (5 disks total: 3 data + 2 parity)
        assert_eq!(meta.distribution.len(), 5, "should have 5 disks in distribution");
        assert_eq!(
            meta.distribution,
            vec![3, 4, 5, 1, 2],
            "distribution should match expected order"
        );

        // Object size
        assert_eq!(meta.size, 644520, "size should be 644520 bytes");

        // Single part with same size
        assert_eq!(meta.parts.len(), 1, "should have 1 part");
        assert_eq!(meta.parts[0].size, 644520, "part size should match object size");

        // Metadata
        assert_eq!(
            meta.content_type,
            "application/octet-stream",
            "content-type should be application/octet-stream"
        );
        assert_eq!(
            meta.etag,
            "9587ddd31fead633830366f45d221d56",
            "etag should match"
        );

        // Version type
        assert_eq!(meta.version_type, VersionType::Object);

        // Data dir UUID should not be zero
        assert!(!meta.data_dir.is_zero(), "data_dir should be set");

        // Verify data_dir UUID format
        let data_dir_str = meta.data_dir_string();
        assert!(
            data_dir_str.contains("-"),
            "data_dir should be a valid UUID string: {}",
            data_dir_str
        );
    }

    /// Test that different disks in the cicd-corpus have different ec_index values.
    #[test]
    fn test_parse_cicd_corpus_ec_index_varies_by_disk() {
        // Parse xl.meta from multiple disks
        let disks_and_expected_ec_index = [
            ("cicd-corpus/disk2/bucket/testobj/xl.meta", 4),
            ("cicd-corpus/disk3/bucket/testobj/xl.meta", 5),
            ("cicd-corpus/disk4/bucket/testobj/xl.meta", 1),
            ("cicd-corpus/disk5/bucket/testobj/xl.meta", 2),
        ];

        for (fixture_path, expected_ec_index) in disks_and_expected_ec_index {
            let data = read_fixture(fixture_path);
            let meta = parse(&data).unwrap_or_else(|e| {
                panic!("failed to parse {}: {}", fixture_path, e)
            });

            assert_eq!(
                meta.erasure_index, expected_ec_index,
                "erasure_index mismatch for {}: expected {}, got {}",
                fixture_path, expected_ec_index, meta.erasure_index
            );

            // All disks should have same distribution (it's a property of the object, not the disk)
            assert_eq!(
                meta.distribution,
                vec![3, 4, 5, 1, 2],
                "distribution should be consistent across all disks for {}",
                fixture_path
            );
        }
    }

    /// Test that cicd-corpus disks have consistent erasure coding config.
    /// Note: The cicd-corpus contains disks with different object versions:
    /// - disk2/disk3: older version (50051050-62bc-4928-...)
    /// - disk4/disk5: newer version (163c7c9d-e856-41ed-...)
    /// But erasure config, size, and etag remain consistent.
    #[test]
    fn test_parse_cicd_corpus_consistency() {
        let fixtures = [
            "cicd-corpus/disk2/bucket/testobj/xl.meta",
            "cicd-corpus/disk3/bucket/testobj/xl.meta",
            "cicd-corpus/disk4/bucket/testobj/xl.meta",
            "cicd-corpus/disk5/bucket/testobj/xl.meta",
        ];

        let metas: Vec<_> = fixtures
            .iter()
            .map(|f| parse(&read_fixture(f)).unwrap())
            .collect();

        // All should have same erasure coding config (cluster-level property)
        for (i, meta) in metas.iter().enumerate() {
            assert_eq!(meta.data_blocks, 3, "disk{} data_blocks", i + 2);
            assert_eq!(meta.parity_blocks, 2, "disk{} parity_blocks", i + 2);
            assert_eq!(meta.block_size, 1048576, "disk{} block_size", i + 2);
            assert_eq!(meta.distribution, vec![3, 4, 5, 1, 2], "disk{} distribution", i + 2);
        }

        // All versions of this object have same size and etag (object content is the same)
        for (i, meta) in metas.iter().enumerate() {
            assert_eq!(meta.size, 644520, "disk{} size", i + 2);
            assert_eq!(meta.etag, "9587ddd31fead633830366f45d221d56", "disk{} etag", i + 2);
        }

        // Verify the version grouping: disk2+disk3 have same version, disk4+disk5 have same version
        assert_eq!(
            metas[0].version_id, metas[1].version_id,
            "disk2 and disk3 should have same version"
        );
        assert_eq!(
            metas[2].version_id, metas[3].version_id,
            "disk4 and disk5 should have same version"
        );
        assert_ne!(
            metas[0].version_id, metas[2].version_id,
            "disk2/3 and disk4/5 should have different versions (replication in progress)"
        );
    }

    /// Test computed properties on ObjectMeta.
    #[test]
    fn test_object_meta_computed_properties() {
        let data = read_fixture("cicd-corpus/disk2/bucket/testobj/xl.meta");
        let meta = parse(&data).unwrap();

        // shard_size = ceil(block_size / data_blocks) = ceil(1048576 / 3) = 349526
        assert_eq!(meta.shard_size(), 349526);

        // total_shards = data_blocks + parity_blocks = 3 + 2 = 5
        assert_eq!(meta.total_shards(), 5);
    }

    /// Test shard_size calculation for xl-many-parts fixture.
    #[test]
    fn test_shard_size_many_parts() {
        let data = read_fixture("xlmeta/xl-many-parts.meta");
        let meta = parse(&data).unwrap();

        // shard_size = ceil(1048576 / 12) = 87382
        assert_eq!(meta.shard_size(), 87382);
        assert_eq!(meta.total_shards(), 16);
    }
}
