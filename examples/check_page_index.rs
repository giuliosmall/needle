use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::PageIndexPolicy;
use std::env;
use std::fs::File;

fn main() {
    let path = env::args().nth(1).expect("path");
    let file = File::open(&path).unwrap();
    let opts = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let meta = ArrowReaderMetadata::load(&file, opts).unwrap();
    let pq = meta.metadata();
    println!("num_rows={}", pq.file_metadata().num_rows());
    println!("num_row_groups={}", pq.num_row_groups());
    match pq.offset_index() {
        None => println!("offset_index: NONE"),
        Some(oi) => {
            println!("offset_index: {} row groups", oi.len());
            for (rg, cols) in oi.iter().enumerate() {
                for (c, oim) in cols.iter().enumerate() {
                    let pages = oim.page_locations();
                    println!("  RG{rg} col{c}: {} pages", pages.len());
                    for (i, p) in pages.iter().take(8).enumerate() {
                        println!(
                            "    page[{i}] offset={} size={} first_row={}",
                            p.offset, p.compressed_page_size, p.first_row_index
                        );
                    }
                    if pages.len() > 8 {
                        println!("    … {} more", pages.len() - 8);
                    }
                }
            }
        }
    }
}
