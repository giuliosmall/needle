//! Custom Parquet file writer: PLAIN pages, per-key ZSTD frames, skippable pads,
//! offset index, footer. See `mod.rs` for layout notes.

use super::thrift::{
    self, ColumnChunkInfo, Kv, PageLoc, RowGroupInfo, SchemaField, CONV_TIMESTAMP_MILLIS,
    CONV_UTF8, CODEC_ZSTD, ENC_PLAIN, ENC_RLE, TYPE_BYTE_ARRAY, TYPE_INT64,
};
use crate::prepared::{
    encode_i64_plain, encode_str_plain, ByteSpan, FrameLoc, ListenRec, PreparedKeyMeta,
    PreparedManifest, ALIGN_BLOCK, ZSTD_SKIPPABLE_MAGIC,
};
use anyhow::{Context, Result, bail};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const MAGIC: &[u8; 4] = b"PAR1";
const ZSTD_LEVEL: i32 = 3;
const CREATED_BY: &str = "rap-rust parquet_lowlevel 0.1.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// Concatenated ZSTD frame per key inside each column's data page.
    ZstdFrames { align: bool },
    /// Same as ZstdFrames plus skippable padding so each key's fetch is 4KB-aligned.
    Aligned { align: bool },
    /// Host column (`timestamp`) interleaves sibling columns as skippable frames.
    Interleaved { align: bool },
}

impl Layout {
    pub fn kind(self) -> &'static str {
        match self {
            Layout::ZstdFrames { align: true } | Layout::Aligned { .. } => "aligned",
            Layout::ZstdFrames { .. } => "zstd_frames",
            Layout::Interleaved { .. } => "interleaved",
        }
    }

    fn align(self) -> bool {
        match self {
            Layout::ZstdFrames { align } => align,
            Layout::Aligned { align } => align,
            Layout::Interleaved { align } => align,
        }
    }

    fn interleaved(self) -> bool {
        matches!(self, Layout::Interleaved { .. })
    }
}

const COLS: [&str; 4] = ["user_id", "timestamp", "track_uri", "duration_ms"];
/// Columns RAP addresses via frame_locs (value columns).
const VALUE_COLS: [&str; 3] = ["timestamp", "track_uri", "duration_ms"];
const HOST_COL: &str = "timestamp";

struct KeyGroup {
    key: String,
    rows: Vec<ListenRec>,
}

/// Write a RAP-prepared Parquet file (single row group) and a `.frames.json` manifest.
pub fn write_listens_parquet(
    path: &Path,
    listens: &[ListenRec],
    layout: Layout,
) -> Result<PreparedManifest> {
    if listens.is_empty() {
        bail!("no rows to write");
    }
    let groups = group_by_key(listens);
    let num_rows = listens.len() as i64;
    let align = layout.align();
    let align_block = if align { ALIGN_BLOCK } else { 0 };

    // PLAIN + ZSTD frame per key per column.
    let mut col_frames: Vec<Vec<Vec<u8>>> = vec![Vec::new(); 4]; // [col][key] -> zstd frame
    let mut col_plain_len: Vec<Vec<usize>> = vec![Vec::new(); 4];
    for g in &groups {
        let user_plain = encode_str_plain(g.rows.iter().map(|r| r.user_id.as_str()));
        let ts_plain = encode_i64_plain(g.rows.iter().map(|r| r.timestamp_ms));
        let track_plain = encode_str_plain(g.rows.iter().map(|r| r.track_uri.as_str()));
        let dur_plain = encode_i64_plain(g.rows.iter().map(|r| r.duration_ms));
        for (i, plain) in [user_plain, ts_plain, track_plain, dur_plain]
            .into_iter()
            .enumerate()
        {
            col_plain_len[i].push(plain.len());
            col_frames[i].push(
                zstd::encode_all(plain.as_slice(), ZSTD_LEVEL)
                    .with_context(|| format!("zstd {}", COLS[i]))?,
            );
        }
    }

    let mut file: Vec<u8> = Vec::new();
    file.extend_from_slice(MAGIC);

    let mut chunks: Vec<ColumnChunkInfo> = Vec::with_capacity(4);
    let mut page_locs: Vec<PageLoc> = Vec::with_capacity(4);
    // RAP index material: per-key frames gathered while writing pages.
    let mut keys_meta: Vec<PreparedKeyMeta> = groups
        .iter()
        .map(|g| PreparedKeyMeta {
            key: g.key.clone(),
            row_numbers: Vec::new(), // filled below
            value_count: g.rows.len() as u64,
            frames: Vec::new(),
            contiguous: None,
            aligned: false,
        })
        .collect();
    let mut row_base = 0u64;
    for (i, g) in groups.iter().enumerate() {
        keys_meta[i].row_numbers = (row_base..row_base + g.rows.len() as u64).collect();
        row_base += g.rows.len() as u64;
    }

    for (ci, col_name) in COLS.iter().enumerate() {
        let uncompressed: usize = col_plain_len[ci].iter().sum();
        let num_values = num_rows as i32;

        // Build payload (may iterate once to lock header size vs alignment).
        let mut header_size_guess = 24usize;
        let (header, payload, frame_infos) = loop {
            let payload_start = file.len() as u64 + header_size_guess as u64;
            let extra = if layout.interleaved() && *col_name == HOST_COL {
                Some(interleave_extras(&col_frames))
            } else {
                None
            };
            let (payload, infos) = build_payload(
                payload_start,
                &col_frames[ci],
                align && value_col(col_name),
                extra.as_deref(),
            );
            let header = thrift::data_page_v1_header(
                uncompressed as i32,
                payload.len() as i32,
                num_values,
            );
            if header.len() == header_size_guess {
                break (header, payload, infos);
            }
            header_size_guess = header.len();
        };

        let page_offset = file.len() as u64;
        file.extend_from_slice(&header);
        debug_assert_eq!(file.len() as u64, page_offset + header.len() as u64);
        file.extend_from_slice(&payload);

        let total_comp = (header.len() + payload.len()) as i64;
        let total_uncomp = (header.len() + uncompressed) as i64;

        chunks.push(ColumnChunkInfo {
            physical: if *col_name == "timestamp" || *col_name == "duration_ms" {
                TYPE_INT64
            } else {
                TYPE_BYTE_ARRAY
            },
            path: col_name.to_string(),
            num_values: num_rows,
            uncompressed_size: total_uncomp,
            compressed_size: total_comp,
            data_page_offset: page_offset as i64,
            encodings: vec![ENC_PLAIN, ENC_RLE],
            codec: CODEC_ZSTD,
            offset_index_offset: None,
            offset_index_length: None,
            data_page_count: 1,
        });
        page_locs.push(PageLoc {
            offset: page_offset as i64,
            compressed_page_size: total_comp as i32,
            first_row_index: 0,
        });

        // Record RAP frame locs. For interleaved, only the host column contributes
        // value-col frames (siblings live in skippable wrappers). Other columns'
        // official frames are for standard readers only.
        if layout.interleaved() {
            if *col_name == HOST_COL {
                for (ki, info) in frame_infos.iter().enumerate() {
                    keys_meta[ki].frames.push(FrameLoc {
                        column: HOST_COL.to_string(),
                        offset: info.frame_offset,
                        size: info.frame_size,
                    });
                    for (col, off, sz) in &info.extras {
                        keys_meta[ki].frames.push(FrameLoc {
                            column: col.clone(),
                            offset: *off,
                            size: *sz,
                        });
                    }
                    if let Some(span) = info.contiguous.clone() {
                        keys_meta[ki].contiguous = Some(span);
                    }
                    keys_meta[ki].aligned = info.aligned;
                }
            }
        } else if value_col(col_name) {
            for (ki, info) in frame_infos.iter().enumerate() {
                keys_meta[ki].frames.push(FrameLoc {
                    column: col_name.to_string(),
                    offset: info.frame_offset,
                    size: info.frame_size,
                });
                if *col_name == HOST_COL {
                    keys_meta[ki].aligned = info.aligned;
                }
            }
        }
    }

    // Offset indexes (one page per column).
    for (i, loc) in page_locs.iter().enumerate() {
        let blob = thrift::offset_index(std::slice::from_ref(loc));
        let off = file.len() as i64;
        file.extend_from_slice(&blob);
        chunks[i].offset_index_offset = Some(off);
        chunks[i].offset_index_length = Some(blob.len() as i32);
    }

    let rg = RowGroupInfo {
        num_rows,
        total_byte_size: chunks.iter().map(|c| c.uncompressed_size).sum(),
        file_offset: chunks[0].data_page_offset,
        total_compressed_size: chunks.iter().map(|c| c.compressed_size).sum(),
        ordinal: 0,
        columns: chunks,
    };

    let schema = vec![
        SchemaField {
            name: "user_id".into(),
            physical: TYPE_BYTE_ARRAY,
            converted: Some(CONV_UTF8),
            logical: Some("utf8"),
        },
        SchemaField {
            name: "timestamp".into(),
            physical: TYPE_INT64,
            converted: Some(CONV_TIMESTAMP_MILLIS),
            logical: Some("ts_millis"),
        },
        SchemaField {
            name: "track_uri".into(),
            physical: TYPE_BYTE_ARRAY,
            converted: Some(CONV_UTF8),
            logical: Some("utf8"),
        },
        SchemaField {
            name: "duration_ms".into(),
            physical: TYPE_INT64,
            converted: None,
            logical: None,
        },
    ];

    let note = match layout {
        Layout::ZstdFrames { align: false } => {
            "ZSTD frame per key inside each column data page (PLAIN); RAP stores frame_locs"
        }
        Layout::ZstdFrames { align: true } | Layout::Aligned { .. } => {
            "ZSTD frames + skippable padding so each key fetch is 4KB-aligned"
        }
        Layout::Interleaved { .. } => {
            "timestamp page interleaves sibling cols as skippable ZSTD frames; RAP one ranged read"
        }
    };

    let kv = vec![
        Kv {
            key: "rap.layout".into(),
            value: layout.kind().into(),
        },
        Kv {
            key: "rap.align_block".into(),
            value: align_block.to_string(),
        },
        Kv {
            key: "rap.note".into(),
            value: note.into(),
        },
    ];

    let meta = thrift::file_metadata(&schema, num_rows, &[rg], CREATED_BY, &kv);
    let meta_len = meta.len() as u32;
    file.extend_from_slice(&meta);
    file.extend_from_slice(&meta_len.to_le_bytes());
    file.extend_from_slice(MAGIC);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    f.write_all(&file)?;
    f.flush()?;

    let data_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.parquet")
        .to_string();

    let manifest = PreparedManifest {
        kind: layout.kind().into(),
        align_block,
        columns: VALUE_COLS.iter().map(|s| s.to_string()).collect(),
        keys: keys_meta,
        data_file: data_name,
        note: note.into(),
    };

    // Sidecar index helper (points at this .parquet, not a .rapz).
    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
        if let Some(dir) = path.parent() {
            let man_path = dir.join(format!("{stem}.frames.json"));
            serde_json::to_writer_pretty(File::create(&man_path)?, &manifest)?;
        }
    }

    Ok(manifest)
}

struct FrameInfo {
    frame_offset: u64,
    frame_size: u32,
    extras: Vec<(String, u64, u32)>, // (column, offset, size) of inner zstd frames
    contiguous: Option<ByteSpan>,
    aligned: bool,
}

/// Per-key extra zstd frames to wrap as skippable inside the host column.
fn interleave_extras(col_frames: &[Vec<Vec<u8>>]) -> Vec<Vec<(String, Vec<u8>)>> {
    // col 2 = track_uri, col 3 = duration_ms
    (0..col_frames[0].len())
        .map(|ki| {
            vec![
                ("track_uri".to_string(), col_frames[2][ki].clone()),
                ("duration_ms".to_string(), col_frames[3][ki].clone()),
            ]
        })
        .collect()
}

fn build_payload(
    payload_start: u64,
    frames: &[Vec<u8>],
    align: bool,
    extras: Option<&[Vec<(String, Vec<u8>)>]>,
) -> (Vec<u8>, Vec<FrameInfo>) {
    let mut buf = Vec::new();
    let mut infos = Vec::with_capacity(frames.len());
    for (i, frame) in frames.iter().enumerate() {
        if align {
            pad_to_file_align(&mut buf, payload_start, ALIGN_BLOCK);
        }
        let key_start = payload_start + buf.len() as u64;
        let off = key_start;
        buf.extend_from_slice(frame);
        let mut extra_locs = Vec::new();
        if let Some(ex) = extras {
            for (col, inner) in &ex[i] {
                // Skippable wrapper so a sequential ZSTD reader of this page skips sibling cols.
                let inner_off = payload_start + buf.len() as u64 + 8;
                write_skippable(&mut buf, inner);
                extra_locs.push((col.clone(), inner_off, inner.len() as u32));
            }
        }
        let key_end = payload_start + buf.len() as u64;
        let contiguous = extras.map(|_| ByteSpan {
            offset: key_start,
            size: key_end - key_start,
        });
        infos.push(FrameInfo {
            frame_offset: off,
            frame_size: frame.len() as u32,
            extras: extra_locs,
            contiguous,
            aligned: align && (key_start % ALIGN_BLOCK == 0),
        });
    }
    (buf, infos)
}

fn value_col(name: &str) -> bool {
    VALUE_COLS.iter().any(|c| *c == name)
}

fn group_by_key(listens: &[ListenRec]) -> Vec<KeyGroup> {
    let mut out: Vec<KeyGroup> = Vec::new();
    for l in listens {
        if out.last().map(|g| g.key == l.user_id).unwrap_or(false) {
            out.last_mut().unwrap().rows.push(l.clone());
        } else {
            out.push(KeyGroup {
                key: l.user_id.clone(),
                rows: vec![l.clone()],
            });
        }
    }
    out
}

fn pad_to_file_align(buf: &mut Vec<u8>, payload_start: u64, align: u64) {
    let cur = payload_start + buf.len() as u64;
    let rem = cur % align;
    if rem == 0 {
        return;
    }
    let need = align - rem;
    let pad_total = if need <= 8 { need + align } else { need };
    write_skippable_sized(buf, (pad_total - 8) as usize);
    debug_assert_eq!((payload_start + buf.len() as u64) % align, 0);
}

fn write_skippable(buf: &mut Vec<u8>, payload: &[u8]) {
    buf.extend_from_slice(&ZSTD_SKIPPABLE_MAGIC.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
}

fn write_skippable_sized(buf: &mut Vec<u8>, payload_len: usize) {
    write_skippable(buf, &vec![0u8; payload_len]);
}

/// PAR1 magic at both ends + `SerializedFileReader` can open the footer.
pub fn verify_parquet_file(path: &Path) -> Result<String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() < 8 {
        bail!("{} too small to be Parquet", path.display());
    }
    if &bytes[..4] != MAGIC {
        bail!("{} missing start PAR1 magic", path.display());
    }
    if &bytes[bytes.len() - 4..] != MAGIC {
        bail!("{} missing end PAR1 magic", path.display());
    }
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)
        .with_context(|| format!("SerializedFileReader {}", path.display()))?;
    let md = reader.metadata();
    let rows = md.file_metadata().num_rows();
    let rgs = md.num_row_groups();
    let cols = if rgs > 0 {
        md.row_group(0).num_columns()
    } else {
        0
    };
    let created = md
        .file_metadata()
        .created_by()
        .unwrap_or("-")
        .to_string();
    Ok(format!(
        "PAR1 ok  rows={rows} row_groups={rgs} cols={cols} created_by={created}"
    ))
}

/// Try a full Arrow decode (proves pages decompress). Returns (num_rows, error?).
pub fn try_arrow_read(path: &Path) -> Result<usize> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut n = 0usize;
    for batch in reader {
        n += batch?.num_rows();
    }
    Ok(n)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::prepared::{decode_i64_plain, read_frame_i64, read_frame_str};

    fn sample() -> Vec<ListenRec> {
        vec![
            ListenRec {
                user_id: "user_a".into(),
                timestamp_ms: 1,
                track_uri: "t1".into(),
                duration_ms: 10,
            },
            ListenRec {
                user_id: "user_a".into(),
                timestamp_ms: 2,
                track_uri: "t2".into(),
                duration_ms: 20,
            },
            ListenRec {
                user_id: "user_b".into(),
                timestamp_ms: 3,
                track_uri: "t3".into(),
                duration_ms: 30,
            },
            ListenRec {
                user_id: "user_c".into(),
                timestamp_ms: 4,
                track_uri: "t4".into(),
                duration_ms: 40,
            },
        ]
    }

    #[test]
    fn writes_readable_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let man = write_listens_parquet(&path, &sample(), Layout::ZstdFrames { align: false })
            .unwrap();
        let proof = verify_parquet_file(&path).unwrap();
        assert!(proof.contains("PAR1 ok"), "{proof}");
        assert_eq!(man.keys.len(), 3);
        assert!(!man.keys[0].frames.is_empty());
        let n = try_arrow_read(&path).expect("arrow read");
        assert_eq!(n, 4);
    }

    #[test]
    fn par1_magic_both_ends_and_row_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("magic.parquet");
        write_listens_parquet(&path, &sample(), Layout::ZstdFrames { align: false }).unwrap();
        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
        assert_eq!(try_arrow_read(&path).unwrap(), sample().len());
    }

    #[test]
    fn plain_int64_utf8_roundtrip_via_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.parquet");
        let rows = sample();
        let man = write_listens_parquet(&path, &rows, Layout::ZstdFrames { align: false }).unwrap();
        let data = fs::read(&path).unwrap();
        let ka = man.keys.iter().find(|k| k.key == "user_a").unwrap();
        let ts = ka.frames.iter().find(|f| f.column == "timestamp").unwrap();
        let tracks = ka.frames.iter().find(|f| f.column == "track_uri").unwrap();
        let durs = ka.frames.iter().find(|f| f.column == "duration_ms").unwrap();
        assert_eq!(read_frame_i64(&data, ts).unwrap(), vec![1, 2]);
        assert_eq!(read_frame_str(&data, tracks).unwrap(), vec!["t1", "t2"]);
        assert_eq!(read_frame_i64(&data, durs).unwrap(), vec![10, 20]);
    }

    #[test]
    fn zstd_multi_frame_page_whole_and_individual() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frames.parquet");
        let man = write_listens_parquet(&path, &sample(), Layout::ZstdFrames { align: false }).unwrap();
        let data = fs::read(&path).unwrap();

        // Individual frames decode.
        let mut all_ts = Vec::new();
        for k in &man.keys {
            let f = k.frames.iter().find(|f| f.column == "timestamp").unwrap();
            all_ts.extend(read_frame_i64(&data, f).unwrap());
        }
        assert_eq!(all_ts, vec![1, 2, 3, 4]);

        // Whole-page decompress: concatenate all timestamp frames in file order and
        // decompress as a multi-frame ZSTD stream (same bytes as page payload).
        let ts_frames: Vec<_> = man
            .keys
            .iter()
            .map(|k| {
                k.frames
                    .iter()
                    .find(|f| f.column == "timestamp")
                    .unwrap()
                    .clone()
            })
            .collect();
        let start = ts_frames[0].offset as usize;
        let end = ts_frames.last().unwrap().offset as usize
            + ts_frames.last().unwrap().size as usize;
        let page_payload = &data[start..end];
        let plain = zstd::decode_all(page_payload).expect("multi-frame page decode");
        assert_eq!(decode_i64_plain(&plain).unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn aligned_mode_key_frame_offsets_4k() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aligned.parquet");
        let man = write_listens_parquet(&path, &sample(), Layout::Aligned { align: true }).unwrap();
        verify_parquet_file(&path).unwrap();
        assert_eq!(try_arrow_read(&path).unwrap(), 4);
        for k in &man.keys {
            assert!(k.aligned, "key {} not aligned", k.key);
            for f in &k.frames {
                if f.column == "timestamp" || f.column == "track_uri" || f.column == "duration_ms" {
                    assert_eq!(
                        f.offset % 4096,
                        0,
                        "frame {} offset {} not 4KB aligned",
                        f.column,
                        f.offset
                    );
                }
            }
        }
    }

    #[test]
    fn interleaved_contiguous_and_arrow_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("interleaved.parquet");
        let rows = sample();
        let man =
            write_listens_parquet(&path, &rows, Layout::Interleaved { align: true }).unwrap();
        verify_parquet_file(&path).unwrap();
        assert_eq!(try_arrow_read(&path).unwrap(), 4, "sibling chunks readable by Arrow");

        let data = fs::read(&path).unwrap();
        let ka = man.keys.iter().find(|k| k.key == "user_a").unwrap();
        let span = ka.contiguous.as_ref().expect("contiguous span");
        let bundle = &data[span.offset as usize..(span.offset + span.size) as usize];
        assert!(!bundle.is_empty());
        // Contiguous span should cover the host + skippable sibling frames.
        let ts = ka.frames.iter().find(|f| f.column == "timestamp").unwrap();
        let track = ka.frames.iter().find(|f| f.column == "track_uri").unwrap();
        let dur = ka.frames.iter().find(|f| f.column == "duration_ms").unwrap();
        assert!(ts.offset >= span.offset);
        assert!(track.offset >= span.offset);
        assert!(dur.offset >= span.offset);
        assert_eq!(read_frame_i64(&data, ts).unwrap(), vec![1, 2]);
        assert_eq!(read_frame_str(&data, track).unwrap(), vec!["t1", "t2"]);
        assert_eq!(read_frame_i64(&data, dur).unwrap(), vec![10, 20]);
    }

    #[test]
    fn aligned_and_interleaved_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let rows = sample();
        for (name, layout) in [
            ("aligned.parquet", Layout::Aligned { align: true }),
            ("interleaved.parquet", Layout::Interleaved { align: true }),
        ] {
            let path = dir.path().join(name);
            let man = write_listens_parquet(&path, &rows, layout).unwrap();
            verify_parquet_file(&path).unwrap();
            assert_eq!(try_arrow_read(&path).unwrap(), 4);
            assert_eq!(man.keys.len(), 3);
            if matches!(layout, Layout::Interleaved { .. }) {
                assert!(man.keys[0].contiguous.is_some());
            }
            if matches!(layout, Layout::Aligned { .. }) {
                assert!(man.keys.iter().all(|k| k.aligned));
            }
        }
    }

    fn walk_column_page_headers(data: &[u8], n_cols: usize) -> Vec<super::super::thrift::DataPageV1Header> {
        use super::super::thrift::{decode_data_page_v1_header, PAGE_DATA};
        assert_eq!(&data[..4], b"PAR1");
        let mut pos = 4usize;
        let mut out = Vec::new();
        for _ in 0..n_cols {
            let (hdr, n) = decode_data_page_v1_header(&data[pos..]).expect("page header");
            assert_eq!(hdr.page_type, PAGE_DATA);
            pos += n + hdr.compressed_page_size as usize;
            out.push(hdr);
        }
        out
    }

    #[test]
    fn page_headers_decode_from_zstd_and_aligned_files() {
        use super::super::thrift::{ENC_PLAIN, ENC_RLE, PAGE_DATA};
        let dir = tempfile::tempdir().unwrap();
        let rows = sample();
        let n_rows = rows.len() as i32;
        for (name, layout) in [
            ("zstd.parquet", Layout::ZstdFrames { align: false }),
            ("aligned.parquet", Layout::Aligned { align: true }),
            ("interleaved.parquet", Layout::Interleaved { align: true }),
        ] {
            let path = dir.path().join(name);
            write_listens_parquet(&path, &rows, layout).unwrap();
            let data = fs::read(&path).unwrap();
            let headers = walk_column_page_headers(&data, 4);
            assert_eq!(headers.len(), 4, "{name}");
            for h in &headers {
                assert_eq!(h.page_type, PAGE_DATA);
                assert_eq!(h.encoding, ENC_PLAIN);
                assert_eq!(h.definition_level_encoding, ENC_RLE);
                assert_eq!(h.repetition_level_encoding, ENC_RLE);
                assert_eq!(h.num_values, n_rows);
                assert!(h.uncompressed_page_size > 0, "{name} uncompressed");
                assert!(h.compressed_page_size > 0, "{name} compressed");
                // Re-encode and compare fields (aligned/interleaved payloads differ).
                let bytes = super::super::thrift::data_page_v1_header(
                    h.uncompressed_page_size,
                    h.compressed_page_size,
                    h.num_values,
                );
                let (again, _) = super::super::thrift::decode_data_page_v1_header(&bytes).unwrap();
                assert_eq!(again.uncompressed_page_size, h.uncompressed_page_size);
                assert_eq!(again.compressed_page_size, h.compressed_page_size);
                assert_eq!(again.num_values, h.num_values);
                assert_eq!(again.encoding, h.encoding);
            }
            // Aligned value-col pages include skippable pads → compressed >= uncompressed.
            if matches!(layout, Layout::Aligned { .. }) {
                assert!(headers.iter().any(|h| h.compressed_page_size >= 4096));
            }
        }
    }
}
