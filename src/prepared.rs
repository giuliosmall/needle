//! Shared types + PLAIN/ZSTD helpers for RAP-prepared layouts.
//!
//! Primary emission is the custom Parquet writer in `parquet_lowlevel/`.
//! This module keeps FrameLoc / manifest / PLAIN encode-decode, plus a
//! **legacy** `.rapz` / `.rapi` sidecar writer (no longer the generate path).

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

/// ZSTD skippable frame magic (little-endian): 0x184D2A50 .. 0x184D2A5F
pub const ZSTD_SKIPPABLE_MAGIC: u32 = 0x184D2A50;
/// Regular ZSTD frame magic
const _ZSTD_MAGIC: u32 = 0xFD2FB528;

pub const ALIGN_BLOCK: u64 = 4096;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedManifest {
    pub kind: String, // "zstd_frames" | "aligned" | "interleaved"
    pub align_block: u64,
    pub columns: Vec<String>,
    pub keys: Vec<PreparedKeyMeta>,
    pub data_file: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedKeyMeta {
    pub key: String,
    /// Absolute row numbers in the companion Parquet (for join-back / secondary).
    pub row_numbers: Vec<u64>,
    pub value_count: u64,
    /// Per-column frame locations (zstd_frames / aligned).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frames: Vec<FrameLoc>,
    /// Single contiguous span covering all interleaved columns for this key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contiguous: Option<ByteSpan>,
    /// Whether the key's primary fetch offset is 4KB-aligned.
    #[serde(default)]
    pub aligned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrameLoc {
    pub column: String,
    pub offset: u64,
    pub size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteSpan {
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct ListenRec {
    pub user_id: String,
    pub timestamp_ms: i64,
    pub track_uri: String,
    pub duration_ms: i64,
}

/// Write a ZSTD-frame-per-key prepared file (+ optional 4KB alignment).
pub fn write_zstd_frames(
    out_dir: &Path,
    file_stem: &str,
    listens: &[ListenRec],
    align: bool,
) -> Result<(PathBuf, PreparedManifest)> {
    fs::create_dir_all(out_dir)?;
    let data_name = format!("{file_stem}.rapz");
    let data_path = out_dir.join(&data_name);
    let mut buf: Vec<u8> = Vec::new();
    let columns = vec![
        "timestamp".to_string(),
        "track_uri".to_string(),
        "duration_ms".to_string(),
    ];

    // Group by key (caller should sort).
    let groups = group_by_key(listens);
    let mut keys_meta = Vec::new();
    let mut row_base = 0u64;

    for (key, rows) in &groups {
        // Optionally pad so the *start* of this key's data is 4KB-aligned.
        if align {
            pad_to_alignment(&mut buf, ALIGN_BLOCK);
        }
        let key_start = buf.len() as u64;
        let mut frames = Vec::new();

        // PLAIN payloads per column, each as its own ZSTD frame.
        let ts_plain = encode_i64_plain(rows.iter().map(|r| r.timestamp_ms));
        let track_plain = encode_str_plain(rows.iter().map(|r| r.track_uri.as_str()));
        let dur_plain = encode_i64_plain(rows.iter().map(|r| r.duration_ms));

        for (col, plain) in [
            ("timestamp", ts_plain),
            ("track_uri", track_plain),
            ("duration_ms", dur_plain),
        ] {
            let frame = zstd::encode_all(plain.as_slice(), 3)
                .with_context(|| format!("zstd compress {col}"))?;
            let offset = buf.len() as u64;
            let size = frame.len() as u32;
            buf.extend_from_slice(&frame);
            frames.push(FrameLoc {
                column: col.to_string(),
                offset,
                size,
            });
        }

        let row_numbers: Vec<u64> = (row_base..row_base + rows.len() as u64).collect();
        row_base += rows.len() as u64;
        keys_meta.push(PreparedKeyMeta {
            key: key.clone(),
            row_numbers,
            value_count: rows.len() as u64,
            frames,
            contiguous: None,
            aligned: align && (key_start % ALIGN_BLOCK == 0),
        });
    }

    fs::write(&data_path, &buf)?;
    let manifest = PreparedManifest {
        kind: if align {
            "aligned".into()
        } else {
            "zstd_frames".into()
        },
        align_block: if align { ALIGN_BLOCK } else { 0 },
        columns,
        keys: keys_meta,
        data_file: data_name,
        note: if align {
            "ZSTD frame per key + skippable-frame padding to 4KB-aligned fetches".into()
        } else {
            "Separate ZSTD frame per key per column; PLAIN encoding; index stores frame locs".into()
        },
    };
    let man_path = out_dir.join(format!("{file_stem}.rapz.manifest.json"));
    serde_json::to_writer_pretty(File::create(&man_path)?, &manifest)?;
    Ok((data_path, manifest))
}

/// Interleaved layout: for each key, contiguous [colA][skip][colB][skip][colC].
pub fn write_interleaved(
    out_dir: &Path,
    file_stem: &str,
    listens: &[ListenRec],
    align: bool,
) -> Result<(PathBuf, PreparedManifest)> {
    fs::create_dir_all(out_dir)?;
    let data_name = format!("{file_stem}.rapi");
    let data_path = out_dir.join(&data_name);
    let mut buf: Vec<u8> = Vec::new();
    let columns = vec![
        "timestamp".to_string(),
        "track_uri".to_string(),
        "duration_ms".to_string(),
    ];
    let groups = group_by_key(listens);
    let mut keys_meta = Vec::new();
    let mut row_base = 0u64;

    for (key, rows) in &groups {
        if align {
            pad_to_alignment(&mut buf, ALIGN_BLOCK);
        }
        let start = buf.len() as u64;
        let mut frames = Vec::new();

        let payloads = [
            (
                "timestamp",
                encode_i64_plain(rows.iter().map(|r| r.timestamp_ms)),
            ),
            (
                "track_uri",
                encode_str_plain(rows.iter().map(|r| r.track_uri.as_str())),
            ),
            (
                "duration_ms",
                encode_i64_plain(rows.iter().map(|r| r.duration_ms)),
            ),
        ];

        for (i, (col, plain)) in payloads.into_iter().enumerate() {
            let frame = zstd::encode_all(plain.as_slice(), 3)?;
            let offset = buf.len() as u64;
            let size = frame.len() as u32;
            buf.extend_from_slice(&frame);
            frames.push(FrameLoc {
                column: col.to_string(),
                offset,
                size,
            });
            // Skippable bridge between columns (article: ZSTD skippable frames
            // so a sequential column reader can skip foreign data).
            if i + 1 < 3 {
                write_skippable(&mut buf, b"COLBRIDGE");
            }
        }

        let end = buf.len() as u64;
        let row_numbers: Vec<u64> = (row_base..row_base + rows.len() as u64).collect();
        row_base += rows.len() as u64;
        keys_meta.push(PreparedKeyMeta {
            key: key.clone(),
            row_numbers,
            value_count: rows.len() as u64,
            frames,
            contiguous: Some(ByteSpan {
                offset: start,
                size: end - start,
            }),
            aligned: align && (start % ALIGN_BLOCK == 0),
        });
    }

    fs::write(&data_path, &buf)?;
    let manifest = PreparedManifest {
        kind: "interleaved".into(),
        align_block: if align { ALIGN_BLOCK } else { 0 },
        columns,
        keys: keys_meta,
        data_file: data_name,
        note: "Interleaved multi-column data per key; one contiguous RAP ranged read".into(),
    };
    let man_path = out_dir.join(format!("{file_stem}.rapi.manifest.json"));
    serde_json::to_writer_pretty(File::create(&man_path)?, &manifest)?;
    Ok((data_path, manifest))
}

/// Decompress a single frame from a prepared file and decode PLAIN values.
pub fn read_frame_i64(data: &[u8], loc: &FrameLoc) -> Result<Vec<i64>> {
    let slice = frame_slice(data, loc)?;
    let plain = zstd::decode_all(slice)?;
    decode_i64_plain(&plain)
}

pub fn read_frame_str(data: &[u8], loc: &FrameLoc) -> Result<Vec<String>> {
    let slice = frame_slice(data, loc)?;
    let plain = zstd::decode_all(slice)?;
    decode_str_plain(&plain)
}

fn frame_slice<'a>(data: &'a [u8], loc: &FrameLoc) -> Result<&'a [u8]> {
    let start = loc.offset as usize;
    let end = start + loc.size as usize;
    if end > data.len() {
        bail!(
            "frame {}@{}+{} past EOF ({})",
            loc.column,
            loc.offset,
            loc.size,
            data.len()
        );
    }
    Ok(&data[start..end])
}

/// Pad `buf` to next multiple of `align` using a ZSTD skippable frame.
fn pad_to_alignment(buf: &mut Vec<u8>, align: u64) {
    let cur = buf.len() as u64;
    let rem = cur % align;
    if rem == 0 {
        return;
    }
    let need = align - rem;
    // Skippable frame overhead = 8 bytes (magic + size). Payload fills the rest.
    if need <= 8 {
        // Need to skip to next boundary: pad with a skippable that overshoots,
        // then another? Simpler: always write skippable with payload so that
        // total added == need + k*align if need < 8. Easiest: pad to align+8
        // if need < 8 by targeting cur + need + align.
        let target = ((cur / align) + 2) * align;
        let pad_total = target - cur;
        write_skippable_sized(buf, (pad_total - 8) as usize);
    } else {
        write_skippable_sized(buf, (need - 8) as usize);
    }
    debug_assert_eq!(buf.len() as u64 % align, 0);
}

fn write_skippable(buf: &mut Vec<u8>, payload: &[u8]) {
    write_skippable_sized_payload(buf, payload);
}

fn write_skippable_sized(buf: &mut Vec<u8>, payload_len: usize) {
    let payload = vec![0u8; payload_len];
    write_skippable_sized_payload(buf, &payload);
}

fn write_skippable_sized_payload(buf: &mut Vec<u8>, payload: &[u8]) {
    buf.extend_from_slice(&ZSTD_SKIPPABLE_MAGIC.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
}

fn group_by_key(listens: &[ListenRec]) -> Vec<(String, Vec<&ListenRec>)> {
    let mut out: Vec<(String, Vec<&ListenRec>)> = Vec::new();
    for l in listens {
        if out.last().map(|(k, _)| k == &l.user_id).unwrap_or(false) {
            out.last_mut().unwrap().1.push(l);
        } else {
            out.push((l.user_id.clone(), vec![l]));
        }
    }
    out
}

pub fn encode_i64_plain(vals: impl Iterator<Item = i64>) -> Vec<u8> {
    let mut out = Vec::new();
    for v in vals {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

pub fn decode_i64_plain(data: &[u8]) -> Result<Vec<i64>> {
    if data.len() % 8 != 0 {
        bail!("i64 plain length {} not multiple of 8", data.len());
    }
    Ok(data
        .chunks_exact(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect())
}

pub fn encode_str_plain<'a>(vals: impl Iterator<Item = &'a str>) -> Vec<u8> {
    // Parquet PLAIN for BYTE_ARRAY: 4-byte little-endian length + bytes
    let mut out = Vec::new();
    for s in vals {
        let b = s.as_bytes();
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b);
    }
    out
}

pub fn decode_str_plain(mut data: &[u8]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    while !data.is_empty() {
        if data.len() < 4 {
            bail!("truncated string plain");
        }
        let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        data = &data[4..];
        if data.len() < len {
            bail!("truncated string plain body");
        }
        out.push(String::from_utf8(data[..len].to_vec())?);
        data = &data[len..];
    }
    Ok(out)
}

/// Load a prepared manifest next to a data file.
pub fn load_manifest(path: &Path) -> Result<PreparedManifest> {
    let s = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    Ok(serde_json::from_str(&s)?)
}

/// Find prepared manifests under a directory.
pub fn find_manifests(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    for e in fs::read_dir(dir)? {
        let p = e?.path();
        if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
            if name.ends_with(".manifest.json") || name.ends_with(".frames.json") {
                out.push(p);
            }
        }
    }
    out.sort();
    Ok(out)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skippable_alignment() {
        let mut buf = vec![0u8; 100];
        pad_to_alignment(&mut buf, 4096);
        assert_eq!(buf.len() % 4096, 0);
    }

    #[test]
    fn roundtrip_plain_str() {
        let enc = encode_str_plain(["a", "bb", ""].into_iter());
        let dec = decode_str_plain(&enc).unwrap();
        assert_eq!(dec, vec!["a", "bb", ""]);
    }

    #[test]
    fn roundtrip_plain_i64() {
        let vals = vec![0i64, -1, 42, i64::MIN + 1, i64::MAX];
        let enc = encode_i64_plain(vals.iter().copied());
        assert_eq!(decode_i64_plain(&enc).unwrap(), vals);
    }

    #[test]
    fn frame_read_roundtrip() {
        let plain = encode_i64_plain([10, 20, 30].into_iter());
        let frame = zstd::encode_all(plain.as_slice(), 3).unwrap();
        let mut data = vec![0u8; 16]; // prefix
        let offset = data.len() as u64;
        data.extend_from_slice(&frame);
        let loc = FrameLoc {
            column: "duration_ms".into(),
            offset,
            size: frame.len() as u32,
        };
        assert_eq!(read_frame_i64(&data, &loc).unwrap(), vec![10, 20, 30]);

        let plain_s = encode_str_plain(["x", "yz"].into_iter());
        let frame_s = zstd::encode_all(plain_s.as_slice(), 3).unwrap();
        let offset_s = data.len() as u64;
        data.extend_from_slice(&frame_s);
        let loc_s = FrameLoc {
            column: "track_uri".into(),
            offset: offset_s,
            size: frame_s.len() as u32,
        };
        assert_eq!(read_frame_str(&data, &loc_s).unwrap(), vec!["x", "yz"]);
    }
}
