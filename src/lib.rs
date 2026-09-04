//! Random Access Parquet (RAP) - faithful recreation of Spotify's approach:
//! external index maps keys → {file ordinal, row numbers} then precise ranged
//! reads fetch only the pages needed.
//!
//! See: https://engineering.atspotify.com/2026/7/indexing-the-data-lake-for-online-point-queries

pub mod index;
pub mod metadata;
pub mod parquet_lowlevel;
pub mod prepared;
pub mod query;
pub mod secondary;
pub mod s3;
pub mod storage;
pub mod lake;
pub mod writer;

pub use index::{IndexBuilder, RapIndex, RapIndexEntry};
pub use metadata::{CachedFileMeta, MetaCache};
pub use query::{QueryOptions, QueryResult, RapQuerier};
pub use writer::{WriteMode, WriterOptions, write_sample_dataset};
