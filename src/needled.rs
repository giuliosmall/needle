//! `needled` — long-lived HTTP JSON query daemon.
//!
//! Distinct from `needle serve`, which is a Range file server.

use clap::Parser;
use needle::server::{self, DaemonOptions};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "needled",
    about = "Needle HTTP query daemon (JSON point queries; not a Range file server)",
    version
)]
struct Args {
    /// RAP index root (registry.json + fragments/).
    #[arg(long, default_value = "data/rap-index", env = "NEEDLE_INDEX")]
    index: PathBuf,
    /// Listen address. Port 0 lets the OS pick an ephemeral port.
    #[arg(long, default_value = "127.0.0.1:7780", env = "NEEDLE_BIND")]
    bind: String,
    /// Load hash buckets per key instead of the full index at startup.
    #[arg(long, default_value_t = false)]
    lazy_buckets: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    server::serve_forever(DaemonOptions {
        index: args.index,
        bind: args.bind,
        lazy_buckets: args.lazy_buckets,
    })
}
