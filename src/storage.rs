//! Ranged byte access — local filesystem and HTTP Range (article: object-store reads).
//!
//! Production RAP issues `Range:` requests against GCS / S3 Express. Here we provide
//! a common trait plus:
//! - `LocalFile` — `seek` + `read` (existing path)
//! - `HttpRange` — `reqwest` blocking client with `Range: bytes=start-end`
//! - a tiny `tiny_http` server that serves Parquet/prepared files for demos

use anyhow::{Context, Result, bail};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// Common ranged-read interface (article: precise ranged reads).
pub trait RangeReader: Send + Sync {
    fn read_range(&self, range: &Range<u64>) -> Result<Vec<u8>>;
    fn label(&self) -> String;
}

/// Local filesystem seek+read.
pub struct LocalFile {
    path: PathBuf,
}

impl LocalFile {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl RangeReader for LocalFile {
    fn read_range(&self, range: &Range<u64>) -> Result<Vec<u8>> {
        let mut f = File::open(&self.path)
            .with_context(|| format!("open {}", self.path.display()))?;
        let len = (range.end - range.start) as usize;
        f.seek(SeekFrom::Start(range.start))?;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn label(&self) -> String {
        format!("local:{}", self.path.display())
    }
}

/// HTTP Range client against a base URL (file served by our demo server or any Range-capable origin).
/// Implemented with raw TCP so we stay on rustc 1.85 without pulling idna/icu.
pub struct HttpRange {
    url: String,
}

impl HttpRange {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl RangeReader for HttpRange {
    fn read_range(&self, range: &Range<u64>) -> Result<Vec<u8>> {
        http_get_range(&self.url, range)
    }

    fn label(&self) -> String {
        self.url.clone()
    }
}

fn http_get_range(url: &str, range: &Range<u64>) -> Result<Vec<u8>> {
    // Expect http://host:port/path
    let bare = url.strip_prefix("http://").context("only http:// supported")?;
    let (hostport, path) = bare.split_once('/').unwrap_or((bare, ""));
    let path = format!("/{}", path);
    let end_incl = range.end.saturating_sub(1);
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {hostport}\r\nRange: bytes={}-{}\r\nConnection: close\r\n\r\n",
        range.start, end_incl
    );
    let mut stream = std::net::TcpStream::connect(hostport)
        .with_context(|| format!("connect {hostport}"))?;
    // Avoid indefinite hang if the peer keeps the socket open.
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.write_all(req.as_bytes())?;

    let mut resp = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => resp.extend_from_slice(&buf[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => return Err(e.into()),
        }
        // Once we have headers, stop when Content-Length body is complete.
        if let Some(sep) = resp.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&resp[..sep]).unwrap_or("");
            if let Some(cl) = headers
                .lines()
                .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            {
                if let Ok(n) = cl.split(':').nth(1).unwrap_or("").trim().parse::<usize>() {
                    if resp.len() >= sep + 4 + n {
                        resp.truncate(sep + 4 + n);
                        break;
                    }
                }
            }
        }
    }
    let sep = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .context("no HTTP header terminator")?;
    let headers = std::str::from_utf8(&resp[..sep]).unwrap_or("");
    let status_line = headers.lines().next().unwrap_or("");
    if !(status_line.contains(" 206") || status_line.contains(" 200")) {
        bail!("HTTP Range {url} bad status: {status_line}");
    }
    Ok(resp[sep + 4..].to_vec())
}


/// Compare local vs HTTP Range byte-for-byte for a set of ranges (demo proof).
pub fn prove_http_matches_local(
    path: &Path,
    base_url: &str,
    ranges: &[Range<u64>],
) -> Result<ProveResult> {
    let local = LocalFile::new(path);
    let url = format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("file")
    );
    let http = HttpRange::new(url);
    let mut compared = 0u64;
    for r in ranges {
        let a = local.read_range(r)?;
        let b = http.read_range(r)?;
        if a != b {
            bail!(
                "mismatch on range {}..{}: local {} bytes vs http {} bytes",
                r.start,
                r.end,
                a.len(),
                b.len()
            );
        }
        compared += a.len() as u64;
    }
    Ok(ProveResult {
        ranges: ranges.len(),
        bytes_compared: compared,
        url: http.label(),
        local: local.label(),
    })
}

#[derive(Debug)]
pub struct ProveResult {
    pub ranges: usize,
    pub bytes_compared: u64,
    pub url: String,
    pub local: String,
}

/// Background tiny HTTP server serving files from `root` with Range support.
pub struct RangeHttpServer {
    pub addr: SocketAddr,
    handle: Option<JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl RangeHttpServer {
    /// Bind `127.0.0.1:0` (ephemeral) and serve `root` directory.
    pub fn start(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let server = tiny_http::Server::http("127.0.0.1:0")
            .map_err(|e| anyhow::anyhow!("bind http server: {e}"))?;
        let addr = server.server_addr().to_ip().expect("ip addr");
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_c = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            serve_loop(server, root, stop_c);
        });
        // Tiny settle so accept is ready.
        thread::sleep(std::time::Duration::from_millis(20));
        Ok(Self {
            addr,
            handle: Some(handle),
            stop,
        })
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn stop(mut self) {
        self.stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Wake the server with a dummy request.
        let _ = std::net::TcpStream::connect(self.addr);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for RangeHttpServer {
    fn drop(&mut self) {
        self.stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(self.addr);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn serve_loop(
    server: tiny_http::Server,
    root: PathBuf,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    loop {
        if stop.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        let request = match server.recv_timeout(std::time::Duration::from_millis(200)) {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(_) => break,
        };
        if stop.load(std::sync::atomic::Ordering::SeqCst) {
            let _ = request.respond(tiny_http::Response::empty(503));
            break;
        }
        if let Err(e) = handle_request(request, &root) {
            eprintln!("http server: {e:#}");
        }
    }
}

fn handle_request(request: tiny_http::Request, root: &Path) -> Result<()> {
    let url = request.url().to_string();
    // Strip query; take last path segment as filename.
    let path_part = url.split('?').next().unwrap_or(&url);
    let name = path_part.trim_start_matches('/');
    if name.is_empty() || name.contains("..") {
        let _ = request.respond(tiny_http::Response::from_string("bad path").with_status_code(400));
        return Ok(());
    }
    let file_path = root.join(name);
    if !file_path.is_file() {
        let _ = request.respond(tiny_http::Response::from_string("not found").with_status_code(404));
        return Ok(());
    }
    let mut file = File::open(&file_path)?;
    let meta = file.metadata()?;
    let total = meta.len();

    let range_header = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Range"))
        .map(|h| h.value.as_str().to_string());

    if let Some(rh) = range_header {
        // Parse bytes=start-end
        let (start, end_incl) = parse_range(&rh, total)?;
        let len = end_incl - start + 1;
        file.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; len as usize];
        file.read_exact(&mut buf)?;
        let response = tiny_http::Response::from_data(buf)
            .with_status_code(206)
            .with_header(
                tiny_http::Header::from_bytes(
                    &b"Content-Range"[..],
                    format!("bytes {start}-{end_incl}/{total}").as_bytes(),
                )
                .unwrap(),
            )
            .with_header(
                tiny_http::Header::from_bytes(&b"Accept-Ranges"[..], &b"bytes"[..]).unwrap(),
            )
            .with_header(
                tiny_http::Header::from_bytes(&b"Connection"[..], &b"close"[..]).unwrap(),
            );
        request.respond(response)?;
    } else {
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let response = tiny_http::Response::from_data(buf)
            .with_header(
                tiny_http::Header::from_bytes(&b"Accept-Ranges"[..], &b"bytes"[..]).unwrap(),
            )
            .with_header(
                tiny_http::Header::from_bytes(&b"Connection"[..], &b"close"[..]).unwrap(),
            );
        request.respond(response)?;
    }
    Ok(())
}

fn parse_range(h: &str, total: u64) -> Result<(u64, u64)> {
    // "bytes=START-END" or "bytes=START-"
    let h = h.trim();
    let rest = h
        .strip_prefix("bytes=")
        .ok_or_else(|| anyhow::anyhow!("bad Range header: {h}"))?;
    let mut parts = rest.splitn(2, '-');
    let start: u64 = parts
        .next()
        .unwrap_or("0")
        .parse()
        .context("range start")?;
    let end_s = parts.next().unwrap_or("");
    let end_incl: u64 = if end_s.is_empty() {
        total.saturating_sub(1)
    } else {
        end_s.parse().context("range end")?
    };
    if start > end_incl || end_incl >= total {
        bail!("range out of bounds {start}-{end_incl} / {total}");
    }
    Ok((start, end_incl))
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn local_vs_http_range_byte_identical() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("blob.bin");
        let payload: Vec<u8> = (0u8..200).collect();
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&payload).unwrap();
        }
        let server = RangeHttpServer::start(tmp.path()).unwrap();
        let base = server.base_url();
        // Port was ephemeral (:0).
        assert!(base.starts_with("http://127.0.0.1:"));
        let ranges = vec![0u64..10, 10..50, 50..200, 7..8];
        let proof = prove_http_matches_local(&path, &base, &ranges).unwrap();
        assert_eq!(proof.ranges, 4);
        assert_eq!(proof.bytes_compared, 10 + 40 + 150 + 1);

        let local = LocalFile::new(&path);
        let http = HttpRange::new(format!("{base}/blob.bin"));
        for r in &ranges {
            assert_eq!(local.read_range(r).unwrap(), http.read_range(r).unwrap());
        }
        server.stop();
    }

    #[test]
    fn parse_range_variants() {
        assert_eq!(parse_range("bytes=0-9", 100).unwrap(), (0, 9));
        assert_eq!(parse_range("bytes=5-", 10).unwrap(), (5, 9));
        assert!(parse_range("bytes=50-40", 100).is_err());
        assert!(parse_range("bytes=0-100", 100).is_err());
    }
}
