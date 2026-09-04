//! `needled` — long-lived HTTP JSON query daemon.
//!
//! Distinct from `needle serve`, which is a Range file server.

use clap::Parser;
use needle::server::{self, DaemonCli};

#[derive(Parser, Debug)]
#[command(
    name = "needled",
    about = "Needle HTTP query daemon (JSON point queries; not a Range file server)",
    version
)]
struct Args {
    #[command(flatten)]
    daemon: DaemonCli,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    server::serve_forever(args.daemon.into())
}
