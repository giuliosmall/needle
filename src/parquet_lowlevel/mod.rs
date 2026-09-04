//! Custom low-level Parquet writer used by RAP `--mode zstd-frames|aligned|interleaved`.
//!
//! We construct data pages ourselves (Thrift compact page headers, PLAIN values,
//! concatenated ZSTD frames, skippable padding) then emit a spec-valid file:
//! `PAR1` + column chunks + OffsetIndex + FileMetaData + footer + `PAR1`.
//!
//! ## Layouts
//!
//! **A. ZSTD frame resets (`zstd-frames`)** - one data page per column (single row
//! group). Compressed page payload is `[zstd_frame(key0)][zstd_frame(key1)]…`.
//! Standard ZSTD decompress of the whole page concatenates frames → full PLAIN
//! column. RAP ranged-reads individual frames via `frame_locs`.
//!
//! **B. Storage alignment (`aligned`)** - same, but ZSTD *skippable* frames pad so
//! the next key's frame starts on a 4KB boundary. Skippable frames are ignored by
//! a conventional page decompress.
//!
//! **C. Interleaving (`interleaved`)** - Parquet requires each column chunk to be a
//! contiguous region, so we still write official `track_uri` / `duration_ms` chunks
//! (duplicate PLAIN/ZSTD) for analytics readers. Article intent lives **inside**
//! the `timestamp` page:
//!
//! ```text
//! for each key:
//!   [zstd(timestamp PLAIN)]
//!   [skippable | zstd(track_uri PLAIN)]   // sequential timestamp reader skips this
//!   [skippable | zstd(duration_ms PLAIN)]
//!   [optional skippable pad to 4KB]
//! ```
//!
//! RAP issues one contiguous ranged read (`contiguous`) covering that bundle.
//! Official sibling chunks exist so `SerializedFileReader` / Arrow can decode the
//! file as ordinary Parquet. The duplication is the cost of staying spec-valid
//! rather than inventing a fake multi-column chunk.
//!
//! PLAIN only (no RLE/delta across frames). Required INT64 / BYTE_ARRAY (UTF8).

mod paged;
mod thrift;
mod tiny;
mod writer;

pub use paged::{pages_for_rows, write_paged_plain_into, PageLocInfo, PagedWriteMeta};
pub use tiny::{write_tiny_plain_into, TinyRow};
pub use writer::{try_arrow_read, verify_parquet_file, write_listens_parquet, Layout};
