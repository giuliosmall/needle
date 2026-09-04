//! Cached Parquet footer + page/column index locations.
//!
//! Article reader path step 2: "use cached file metadata (footer + page/column
//! indexes) to map row numbers → byte ranges for needed columns".

use anyhow::{bail, Context, Result};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::page_index::offset_index::PageLocation;
use std::collections::HashMap;
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Per-column page locations within one row group.
#[derive(Debug, Clone)]
pub struct ColumnPageIndex {
    pub column_path: String,
    pub column_index: usize,
    pub pages: Vec<PageLocation>,
}

/// Cached metadata for one Parquet file - enough to map rows → byte ranges
/// without re-reading the footer.
#[derive(Debug, Clone)]
pub struct CachedFileMeta {
    pub path: PathBuf,
    pub num_rows: i64,
    pub row_group_row_counts: Vec<i64>,
    /// Cumulative row offsets: row_group_starts[i] = first global row of RG i.
    pub row_group_starts: Vec<i64>,
    /// offset indexes per row group, per projected column (by name).
    pub page_indexes: Vec<HashMap<String, ColumnPageIndex>>,
    /// Full arrow-rs metadata (kept for building selective readers).
    pub arrow_meta: ArrowReaderMetadata,
}

impl CachedFileMeta {
    /// Map a global file row number to (row_group_idx, row_within_rg).
    pub fn locate_row(&self, global_row: u64) -> Result<(usize, i64)> {
        let r = global_row as i64;
        if r < 0 || r >= self.num_rows {
            bail!(
                "row {global_row} out of range (file has {} rows)",
                self.num_rows
            );
        }
        for (rg, &start) in self.row_group_starts.iter().enumerate() {
            let count = self.row_group_row_counts[rg];
            if r >= start && r < start + count {
                return Ok((rg, r - start));
            }
        }
        bail!("row {global_row} not found in any row group");
    }

    /// Given global row numbers and a column name, return the unique page byte
    /// ranges that must be fetched (article: precise ranged reads).
    pub fn page_ranges_for_rows(
        &self,
        column: &str,
        global_rows: &[u64],
    ) -> Result<Vec<(usize /*rg*/, Range<u64>)>> {
        let mut ranges: Vec<(usize, Range<u64>)> = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for &grow in global_rows {
            let (rg, local) = self.locate_row(grow)?;
            let col_idx = self.page_indexes[rg]
                .get(column)
                .with_context(|| format!("no page index for column '{column}' in RG {rg}"))?;

            let page_i = find_page(&col_idx.pages, local)?;
            let page = &col_idx.pages[page_i];
            let start = page.offset as u64;
            let end = start + page.compressed_page_size as u64;
            let key = (rg, start, end);
            if seen.insert(key) {
                ranges.push((rg, start..end));
            }
        }

        // Coalesce adjacent/overlapping ranges within the same RG (article: reader coalesces).
        ranges.sort_by_key(|(rg, r)| (*rg, r.start));
        let mut coalesced: Vec<(usize, Range<u64>)> = Vec::new();
        for (rg, r) in ranges {
            if let Some((crg, cr)) = coalesced.last_mut() {
                if *crg == rg && cr.end >= r.start {
                    cr.end = cr.end.max(r.end);
                    continue;
                }
            }
            coalesced.push((rg, r));
        }
        Ok(coalesced)
    }

    /// Summarize which pages cover a set of rows (for demo / timing output).
    pub fn describe_pages(&self, column: &str, global_rows: &[u64]) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for &grow in global_rows {
            let (rg, local) = self.locate_row(grow)?;
            let col_idx = match self.page_indexes[rg].get(column) {
                Some(c) => c,
                None => {
                    out.push(format!(
                        "row {grow}: RG{rg} local={local} (no offset index for {column})"
                    ));
                    continue;
                }
            };
            let page_i = find_page(&col_idx.pages, local)?;
            let page = &col_idx.pages[page_i];
            let key = (rg, page_i);
            if seen.insert(key) {
                out.push(format!(
                    "row {grow} → RG{rg} page[{page_i}] offset={} size={} first_row={}",
                    page.offset, page.compressed_page_size, page.first_row_index
                ));
            }
        }
        Ok(out)
    }
}

fn find_page(pages: &[PageLocation], local_row: i64) -> Result<usize> {
    if pages.is_empty() {
        bail!("empty page index");
    }
    // pages sorted by first_row_index; find last page with first_row_index <= local_row
    let mut lo = 0usize;
    let mut hi = pages.len();
    while lo + 1 < hi {
        let mid = (lo + hi) / 2;
        if pages[mid].first_row_index <= local_row {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Process-wide cache of footers / page indexes (article: "cached file metadata").
#[derive(Default)]
pub struct MetaCache {
    inner: RwLock<HashMap<PathBuf, Arc<CachedFileMeta>>>,
}

impl MetaCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_load(&self, path: &Path) -> Result<Arc<CachedFileMeta>> {
        {
            let guard = self.inner.read().unwrap();
            if let Some(m) = guard.get(path) {
                return Ok(Arc::clone(m));
            }
        }
        let meta = Arc::new(load_file_meta(path)?);
        let mut guard = self.inner.write().unwrap();
        guard
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::clone(&meta));
        Ok(meta)
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn load_file_meta(path: &Path) -> Result<CachedFileMeta> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    // Enable page index so we get OffsetIndex (PageLocation) for ranged reads.
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let arrow_meta = ArrowReaderMetadata::load(&file, options)
        .with_context(|| format!("load parquet metadata {}", path.display()))?;

    let pq: &ParquetMetaData = arrow_meta.metadata();
    let num_rows = pq.file_metadata().num_rows();
    let n_rg = pq.num_row_groups();

    let mut row_group_row_counts = Vec::with_capacity(n_rg);
    let mut row_group_starts = Vec::with_capacity(n_rg);
    let mut running = 0i64;
    for i in 0..n_rg {
        let rc = pq.row_group(i).num_rows();
        row_group_row_counts.push(rc);
        row_group_starts.push(running);
        running += rc;
    }

    // Offset indexes: Option<Vec<Vec<Option<OffsetIndexMetaData>>>>
    // structure from parquet crate: offset_index() -> Option<&Vec<Vec<Option<...>>>>
    let mut page_indexes: Vec<HashMap<String, ColumnPageIndex>> = Vec::with_capacity(n_rg);

    let offset_indexes = pq.offset_index();
    for rg in 0..n_rg {
        let mut map = HashMap::new();
        let rg_meta = pq.row_group(rg);
        for col in 0..rg_meta.num_columns() {
            let col_meta = rg_meta.column(col);
            let name = col_meta.column_path().string();
            // Prefer leaf name (last path segment) as lookup key.
            let leaf = name.rsplit('.').next().unwrap_or(&name).to_string();

            if let Some(offset_index) = offset_indexes {
                if let Some(rg_oi) = offset_index.get(rg) {
                    if let Some(oi) = rg_oi.get(col) {
                        map.insert(
                            leaf.clone(),
                            ColumnPageIndex {
                                column_path: name,
                                column_index: col,
                                pages: oi.page_locations().clone(),
                            },
                        );
                        continue;
                    }
                }
            }

            // Fallback: synthesise a single "page" spanning the whole column chunk
            // when OffsetIndex is absent (still allows a ranged read of the chunk).
            let start = col_meta.byte_range().0;
            let len = col_meta.byte_range().1;
            map.insert(
                leaf,
                ColumnPageIndex {
                    column_path: name,
                    column_index: col,
                    pages: vec![PageLocation {
                        offset: start as i64,
                        compressed_page_size: len as i32,
                        first_row_index: 0,
                    }],
                },
            );
        }
        page_indexes.push(map);
    }

    Ok(CachedFileMeta {
        path: path.to_path_buf(),
        num_rows,
        row_group_row_counts,
        row_group_starts,
        page_indexes,
        arrow_meta,
    })
}

/// Perform a precise ranged read from a local file (article: std::fs seek).
pub fn ranged_read(path: &Path, range: &Range<u64>) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = File::open(path)?;
    let len = (range.end - range.start) as usize;
    f.seek(SeekFrom::Start(range.start))?;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{write_sample_dataset, WriteMode, WriterOptions};
    use parquet::file::page_index::offset_index::PageLocation;
    use std::sync::Arc;

    #[test]
    fn find_page_binary_search() {
        let pages = vec![
            PageLocation {
                offset: 0,
                compressed_page_size: 100,
                first_row_index: 0,
            },
            PageLocation {
                offset: 100,
                compressed_page_size: 100,
                first_row_index: 50,
            },
            PageLocation {
                offset: 200,
                compressed_page_size: 100,
                first_row_index: 100,
            },
        ];
        assert_eq!(find_page(&pages, 0).unwrap(), 0);
        assert_eq!(find_page(&pages, 49).unwrap(), 0);
        assert_eq!(find_page(&pages, 50).unwrap(), 1);
        assert_eq!(find_page(&pages, 99).unwrap(), 1);
        assert_eq!(find_page(&pages, 100).unwrap(), 2);
        assert_eq!(find_page(&pages, 999).unwrap(), 2);
    }

    #[test]
    fn find_page_single_and_empty() {
        assert!(find_page(&[], 0).is_err());
        let one = vec![PageLocation {
            offset: 10,
            compressed_page_size: 5,
            first_row_index: 0,
        }];
        assert_eq!(find_page(&one, 0).unwrap(), 0);
        assert_eq!(find_page(&one, 100).unwrap(), 0);
    }

    #[test]
    fn locate_row_first_last_and_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let opts = WriterOptions {
            out_dir: data.clone(),
            num_users: 10,
            listens_per_user: 5,
            num_files: 1,
            mode: WriteMode::Sorted,
            rows_per_row_group: 16,
            write_page_index: true,
            seed: 7,
            one_page_per_key: false,
        };
        let paths = write_sample_dataset(&opts).unwrap();
        let meta = load_file_meta(&paths[0]).unwrap();
        assert_eq!(meta.num_rows, 50);
        assert_eq!(meta.locate_row(0).unwrap(), (0, 0));
        let (rg, local) = meta.locate_row((meta.num_rows - 1) as u64).unwrap();
        assert!(rg < meta.row_group_row_counts.len());
        assert!(local >= 0);
        assert!(meta.locate_row(meta.num_rows as u64).is_err());
        assert!(meta.locate_row(u64::MAX).is_err());
    }

    #[test]
    fn coalesce_adjacent_page_ranges() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let opts = WriterOptions {
            out_dir: data.clone(),
            num_users: 20,
            listens_per_user: 8,
            num_files: 1,
            mode: WriteMode::Sorted,
            rows_per_row_group: 64,
            write_page_index: true,
            seed: 11,
            one_page_per_key: false,
        };
        let paths = write_sample_dataset(&opts).unwrap();
        let meta = load_file_meta(&paths[0]).unwrap();
        // Many consecutive rows → adjacent pages should coalesce.
        let rows: Vec<u64> = (0..40).collect();
        let ranges = meta.page_ranges_for_rows("track_uri", &rows).unwrap();
        assert!(!ranges.is_empty());
        // Coalesced list is sorted and non-overlapping within an RG.
        for w in ranges.windows(2) {
            let (rg0, r0) = &w[0];
            let (rg1, r1) = &w[1];
            if rg0 == rg1 {
                assert!(
                    r0.end < r1.start,
                    "ranges should not overlap after coalesce"
                );
            } else {
                assert!(rg0 < rg1);
            }
        }
        // Touching many rows should yield fewer ranges than rows.
        assert!(ranges.len() < rows.len());
    }

    #[test]
    fn meta_cache_returns_cached_arc() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let opts = WriterOptions {
            out_dir: data.clone(),
            num_users: 6,
            listens_per_user: 4,
            num_files: 1,
            mode: WriteMode::Sorted,
            rows_per_row_group: 32,
            write_page_index: true,
            seed: 3,
            one_page_per_key: false,
        };
        let paths = write_sample_dataset(&opts).unwrap();
        let cache = MetaCache::new();
        assert!(cache.is_empty());
        let a = cache.get_or_load(&paths[0]).unwrap();
        let b = cache.get_or_load(&paths[0]).unwrap();
        assert_eq!(cache.len(), 1);
        assert!(Arc::ptr_eq(&a, &b));
    }
}
