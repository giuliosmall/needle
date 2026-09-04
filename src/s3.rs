//! Minimal path-style S3 client for local MinIO (no cloud / no gcloud).
//!
//! Raw TCP + AWS SigV4 — same rustc-1.85 constraint as `HttpRange` (no reqwest).
//! Supports Range GET, full GET, PUT, HEAD, ListObjectsV2.
//! Anonymous GET works when the bucket allows download (our lake setup).

use anyhow::{Context, Result, bail};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Default)]
pub struct S3Stats {
    pub range_gets: AtomicU64,
    pub full_gets: AtomicU64,
    pub puts: AtomicU64,
    pub mc_fallbacks: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
}

impl S3Stats {
    pub fn snapshot(&self) -> S3StatsSnap {
        S3StatsSnap {
            range_gets: self.range_gets.load(Ordering::Relaxed),
            full_gets: self.full_gets.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            mc_fallbacks: self.mc_fallbacks.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.range_gets.store(0, Ordering::Relaxed);
        self.full_gets.store(0, Ordering::Relaxed);
        self.puts.store(0, Ordering::Relaxed);
        self.mc_fallbacks.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct S3StatsSnap {
    pub range_gets: u64,
    pub full_gets: u64,
    pub puts: u64,
    pub mc_fallbacks: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub anonymous_read: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: std::env::var("RAP_S3_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:9000".into()),
            access_key: std::env::var("RAP_S3_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".into()),
            secret_key: std::env::var("RAP_S3_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".into()),
            region: std::env::var("RAP_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
            anonymous_read: std::env::var("RAP_S3_ANON_READ")
                .map(|v| v != "0" && v.to_lowercase() != "false")
                .unwrap_or(true),
        }
    }
}

#[derive(Clone)]
pub struct S3Client {
    pub cfg: S3Config,
    pub stats: Arc<S3Stats>,
}

impl S3Client {
    pub fn new(cfg: S3Config) -> Self {
        Self {
            cfg,
            stats: Arc::new(S3Stats::default()),
        }
    }

    pub fn from_env() -> Self {
        Self::new(S3Config::default())
    }

    pub fn parse_uri(uri: &str) -> Result<(String, String)> {
        if let Some(rest) = uri.strip_prefix("s3://") {
            let (bucket, key) = rest
                .split_once('/')
                .ok_or_else(|| anyhow::anyhow!("s3 uri missing key: {uri}"))?;
            return Ok((bucket.to_string(), key.to_string()));
        }
        if let Some(bare) = uri.strip_prefix("http://") {
            let (_host, path) = bare
                .split_once('/')
                .ok_or_else(|| anyhow::anyhow!("http s3 uri missing path: {uri}"))?;
            let (bucket, key) = path
                .split_once('/')
                .ok_or_else(|| anyhow::anyhow!("http s3 uri missing key: {uri}"))?;
            return Ok((bucket.to_string(), key.to_string()));
        }
        bail!("not an s3/http object uri: {uri}");
    }

    pub fn is_remote_uri(uri: &str) -> bool {
        uri.starts_with("s3://") || (uri.starts_with("http://") && uri.contains("/"))
    }

    pub fn s3_uri(bucket: &str, key: &str) -> String {
        format!("s3://{bucket}/{key}")
    }

    pub fn put_object(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        let uri = format!("/{bucket}/{key}");
        // MinIO accepts UNSIGNED-PAYLOAD; hashing 1M tiny bodies was wasted CPU.
        let (status, _h, resp) =
            self.http("PUT", &uri, "", body, "UNSIGNED-PAYLOAD", None, true)?;
        if !(200..300).contains(&status) {
            self.stats.mc_fallbacks.fetch_add(1, Ordering::Relaxed);
            static FIRST: std::sync::Once = std::sync::Once::new();
            FIRST.call_once(|| {
                eprintln!(
                    "S3 PUT fallback status={status} uri={uri} body_prefix={}",
                    String::from_utf8_lossy(&resp).chars().take(300).collect::<String>()
                );
            });
            // Fallback: mc cp (handles SigV4); keeps lake generate unblocked.
            self.put_object_mc(bucket, key, body).with_context(|| {
                format!(
                    "S3 PUT {uri} status {status}: {}; mc fallback also failed",
                    String::from_utf8_lossy(&resp)
                )
            })?;
            return Ok(());
        }
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_written
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn put_object_mc(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        use std::process::{Command, Stdio};
        let mc = std::path::Path::new("tools/mc");
        let mc = if mc.exists() {
            mc.to_path_buf()
        } else {
            std::path::PathBuf::from("/workspace/rap-rust/tools/mc")
        };
        if !mc.exists() {
            bail!("mc binary not found for PUT fallback");
        }
        // Ensure alias
        let _ = Command::new(&mc)
            .args([
                "alias",
                "set",
                "local",
                &format!("http://{}", self.cfg.endpoint),
                &self.cfg.access_key,
                &self.cfg.secret_key,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let tmp = std::env::temp_dir().join(format!(
            "rap-put-{}-{}.bin",
            std::process::id(),
            {
                use sha2::{Digest, Sha256};
                let mut h = Sha256::new();
                h.update(key.as_bytes());
                hex::encode(&h.finalize()[..8])
            }
        ));
        std::fs::write(&tmp, body)?;
        let dest = format!("local/{bucket}/{key}");
        let st = Command::new(&mc)
            .args(["cp", tmp.to_str().unwrap(), &dest])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .context("mc cp")?;
        let _ = std::fs::remove_file(&tmp);
        if !st.status.success() {
            bail!(
                "mc cp failed: {}",
                String::from_utf8_lossy(&st.stderr)
            );
        }
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_written
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let uri = format!("/{bucket}/{key}");
        let payload_hash = hex_sha256(b"");
        let sign = !self.cfg.anonymous_read;
        let (status, _h, body) =
            self.http("GET", &uri, "", &[], &payload_hash, None, sign)?;
        if status != 200 {
            bail!(
                "S3 GET {uri} status {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        self.stats.full_gets.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_read
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(body)
    }

    pub fn get_range(&self, bucket: &str, key: &str, range: &Range<u64>) -> Result<Vec<u8>> {
        let uri = format!("/{bucket}/{key}");
        let payload_hash = hex_sha256(b"");
        let end_incl = range.end.saturating_sub(1);
        let range_hdr = format!("bytes={}-{}", range.start, end_incl);
        let sign = !self.cfg.anonymous_read;
        let (status, _h, body) =
            self.http("GET", &uri, "", &[], &payload_hash, Some(&range_hdr), sign)?;
        if !(status == 206 || status == 200) {
            bail!(
                "S3 Range GET {uri} {range_hdr} status {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        self.stats.range_gets.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_read
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(body)
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> Result<u64> {
        let uri = format!("/{bucket}/{key}");
        let payload_hash = hex_sha256(b"");
        let sign = !self.cfg.anonymous_read;
        let (status, headers, body) =
            self.http("HEAD", &uri, "", &[], &payload_hash, None, sign)?;
        if status != 200 {
            bail!(
                "S3 HEAD {uri} status {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        for line in headers.lines() {
            if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                return Ok(v.trim().parse()?);
            }
        }
        bail!("S3 HEAD missing Content-Length");
    }

    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut qparts = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            if let Some(t) = &token {
                qparts.push(("continuation-token".to_string(), t.clone()));
            }
            qparts.sort_by(|a, b| a.0.cmp(&b.0));
            let query = qparts
                .iter()
                .map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v)))
                .collect::<Vec<_>>()
                .join("&");
            let uri = format!("/{bucket}");
            let payload_hash = hex_sha256(b"");
            let (status, _h, body) =
                self.http("GET", &uri, &query, &[], &payload_hash, None, true)?;
            if status != 200 {
                bail!(
                    "S3 ListObjects {bucket} status {status}: {}",
                    String::from_utf8_lossy(&body)
                );
            }
            let xml = String::from_utf8_lossy(&body);
            for key in extract_xml_tags(&xml, "Key") {
                out.push(key);
            }
            let truncated = extract_xml_tags(&xml, "IsTruncated")
                .first()
                .map(|s| s.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            if !truncated {
                break;
            }
            token = extract_xml_tags(&xml, "NextContinuationToken")
                .into_iter()
                .next();
            if token.is_none() {
                break;
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn http(
        &self,
        method: &str,
        canonical_uri: &str,
        query: &str,
        body: &[u8],
        payload_hash: &str,
        range: Option<&str>,
        sign: bool,
    ) -> Result<(u16, String, Vec<u8>)> {
        match self.http_once(method, canonical_uri, query, body, payload_hash, range, sign, true) {
            Ok(v) => Ok(v),
            Err(_) => self.http_once(
                method,
                canonical_uri,
                query,
                body,
                payload_hash,
                range,
                sign,
                false,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn http_once(
        &self,
        method: &str,
        canonical_uri: &str,
        query: &str,
        body: &[u8],
        payload_hash: &str,
        range: Option<&str>,
        sign: bool,
        try_reuse: bool,
    ) -> Result<(u16, String, Vec<u8>)> {
        let host = &self.cfg.endpoint;
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let encoded_uri = encode_path(canonical_uri);
        let request_target = if query.is_empty() {
            encoded_uri.clone()
        } else {
            format!("{encoded_uri}?{query}")
        };

        let auth_header = if sign {
            let credential_scope =
                format!("{}/{}/s3/aws4_request", date_stamp, self.cfg.region);
            let mut pairs: Vec<(String, String)> = vec![
                ("host".into(), host.clone()),
                ("x-amz-content-sha256".into(), payload_hash.to_string()),
                ("x-amz-date".into(), amz_date.clone()),
            ];
            if let Some(r) = range {
                pairs.push(("range".into(), r.to_string()));
            }
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let canonical_headers: String = pairs
                .iter()
                .map(|(n, v)| format!("{n}:{v}\n"))
                .collect();
            let signed_headers = pairs
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(";");
            // Canonical URI must match the request-line encoding (path segments
            // URI-encoded, slashes kept). Signing the raw `date=…` path while
            // sending `date%3D…` made every lake PUT 403 → mc cp (~100 obj/s).
            let canonical_request = format!(
                "{method}\n{encoded_uri}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
            );
            let canonical_hash = hex_sha256(canonical_request.as_bytes());
            let string_to_sign = format!(
                "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_hash}"
            );
            let key = signing_key(
                &self.cfg.secret_key,
                &date_stamp,
                &self.cfg.region,
                "s3",
            )?;
            let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes())?);
            format!(
                "Authorization: AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}\r\n",
                self.cfg.access_key
            )
        } else {
            String::new()
        };

        let mut extra = String::new();
        if let Some(r) = range {
            extra.push_str(&format!("Range: {r}\r\n"));
        }
        let content_len = if matches!(method, "PUT" | "POST") {
            format!("Content-Length: {}\r\n", body.len())
        } else {
            String::new()
        };

        let req = format!(
            "{method} {request_target} HTTP/1.1\r\n\
             Host: {host}\r\n\
             x-amz-content-sha256: {payload_hash}\r\n\
             x-amz-date: {amz_date}\r\n\
             {extra}\
             {auth_header}\
             {content_len}\
             Connection: keep-alive\r\n\
             \r\n"
        );

        let mut stream = if try_reuse {
            take_pooled(host).unwrap_or(connect_host(host)?)
        } else {
            connect_host(host)?
        };
        if let Err(e) = stream.write_all(req.as_bytes()) {
            return Err(e.into());
        }
        if !body.is_empty() {
            stream.write_all(body)?;
        }
        stream.flush()?;

        let (status, headers, body_out, reuse) = read_http_response(&mut stream, method)?;
        if reuse {
            put_pooled(host, stream);
        }
        Ok((status, headers, body_out))
    }
}


thread_local! {
    static POOLED: RefCell<Option<(String, TcpStream)>> = RefCell::new(None);
}

fn connect_host(host: &str) -> Result<TcpStream> {
    let stream = TcpStream::connect(host).with_context(|| format!("connect {host}"))?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(60)))?;
    stream.set_write_timeout(Some(Duration::from_secs(60)))?;
    Ok(stream)
}

fn take_pooled(host: &str) -> Option<TcpStream> {
    POOLED.with(|slot| {
        match slot.borrow_mut().take() {
            Some((h, s)) if h == host => Some(s),
            Some((_, s)) => {
                drop(s);
                None
            }
            None => None,
        }
    })
}

fn put_pooled(host: &str, stream: TcpStream) {
    POOLED.with(|slot| {
        *slot.borrow_mut() = Some((host.to_string(), stream));
    });
}

fn header_has_close(headers: &str) -> bool {
    headers.lines().any(|l| {
        let l = l.to_ascii_lowercase();
        l.starts_with("connection:") && l.contains("close")
    })
}

/// Read one HTTP/1.1 response; `reuse` is true when the socket can be pooled.
fn read_http_response(
    stream: &mut TcpStream,
    method: &str,
) -> Result<(u16, String, Vec<u8>, bool)> {
    let mut resp = Vec::with_capacity(1024);
    let mut tmp = [0u8; 16384];
    let sep = loop {
        let n = match stream.read(&mut tmp) {
            Ok(0) => bail!("eof before HTTP headers"),
            Ok(n) => n,
            Err(e) => return Err(e.into()),
        };
        resp.extend_from_slice(&tmp[..n]);
        if let Some(s) = find_header_end(&resp) {
            break s;
        }
        if resp.len() > 1024 * 1024 {
            bail!("HTTP headers too large");
        }
    };
    let headers = std::str::from_utf8(&resp[..sep]).unwrap_or("").to_string();
    let status = headers
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let header_end = sep + 4;
    let reuse = !header_has_close(&headers);
    if method == "HEAD" {
        return Ok((status, headers, Vec::new(), reuse));
    }
    let cl = content_length(&headers);
    let mut body = resp[header_end..].to_vec();
    if let Some(need) = cl {
        while body.len() < need {
            let n = stream.read(&mut tmp)?;
            if n == 0 {
                bail!("eof in HTTP body ({} of {} bytes)", body.len(), need);
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(need);
        Ok((status, headers, body, reuse))
    } else {
        // No Content-Length: drain until close; do not reuse.
        loop {
            match stream.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => body.extend_from_slice(&tmp[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok((status, headers, body, false))
    }
}

fn find_header_end(resp: &[u8]) -> Option<usize> {
    resp.windows(4).position(|w| w == b"\r\n\r\n")
}

fn content_length(headers: &str) -> Option<usize> {
    headers.lines().find_map(|l| {
        let l = l.to_ascii_lowercase();
        l.strip_prefix("content-length:")
            .and_then(|v| v.trim().parse().ok())
    })
}

fn hex_sha256(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key).context("hmac key")?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Result<Vec<u8>> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let k_region = hmac_sha256(&k_date, region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, service.as_bytes())?;
    hmac_sha256(&k_service, b"aws4_request")
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// URI-encode each path segment (keep `/`), per AWS SigV4 S3 rules.
fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|seg| {
            if seg.is_empty() {
                String::new()
            } else {
                urlencode(seg)
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn extract_xml_tags(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let start = i + open.len();
        let after = &rest[start..];
        if let Some(j) = after.find(&close) {
            out.push(after[..j].to_string());
            rest = &after[j + close.len()..];
        } else {
            break;
        }
    }
    out
}

/// `RangeReader` over a MinIO/S3 object (path-style).
pub struct S3RangeReader {
    client: S3Client,
    bucket: String,
    key: String,
}

impl S3RangeReader {
    pub fn new(client: S3Client, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    pub fn from_uri(client: S3Client, uri: &str) -> Result<Self> {
        let (bucket, key) = S3Client::parse_uri(uri)?;
        Ok(Self::new(client, bucket, key))
    }
}

impl crate::storage::RangeReader for S3RangeReader {
    fn read_range(&self, range: &Range<u64>) -> Result<Vec<u8>> {
        self.client.get_range(&self.bucket, &self.key, range)
    }

    fn label(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.key)
    }
}


/// Parquet `ChunkReader` over MinIO Range GETs (fat-file RAP decode).
///
/// Footer parse uses `get_read` near EOF (a few bytes). With OffsetIndex +
/// `RowSelection`, page bodies come through `get_bytes` of individual pages —
/// never a whole-object GET.
pub struct S3ChunkReader {
    client: S3Client,
    bucket: String,
    key: String,
    len: u64,
}

impl S3ChunkReader {
    pub fn open(client: S3Client, bucket: impl Into<String>, key: impl Into<String>) -> Result<Self> {
        let bucket = bucket.into();
        let key = key.into();
        let len = match client.head_object(&bucket, &key) {
            Ok(n) => n,
            Err(_) => {
                // Retry signed HEAD if anonymous download does not cover HEAD.
                let mut signed = client.clone();
                signed.cfg.anonymous_read = false;
                signed.head_object(&bucket, &key)?
            }
        };
        Ok(Self {
            client,
            bucket,
            key,
            len,
        })
    }

    pub fn from_uri(client: S3Client, uri: &str) -> Result<Self> {
        let (bucket, key) = S3Client::parse_uri(uri)?;
        Self::open(client, bucket, key)
    }

    pub fn len(&self) -> u64 {
        self.len
    }
}

impl parquet::file::reader::Length for S3ChunkReader {
    fn len(&self) -> u64 {
        self.len
    }
}

/// Sequential reader that Range-GETs 64 KiB windows as `Read` is consumed.
pub struct S3ReadWindow {
    client: S3Client,
    bucket: String,
    key: String,
    pos: u64,
    end: u64,
    buf: Vec<u8>,
    buf_pos: usize,
}

impl Read for S3ReadWindow {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.end || out.is_empty() {
            return Ok(0);
        }
        if self.buf_pos >= self.buf.len() {
            let n = (self.end - self.pos).min(64 * 1024);
            let range = self.pos..self.pos + n;
            self.buf = self
                .client
                .get_range(&self.bucket, &self.key, &range)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            self.buf_pos = 0;
        }
        let avail = self.buf.len() - self.buf_pos;
        let n = avail.min(out.len());
        out[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
        self.buf_pos += n;
        self.pos += n as u64;
        Ok(n)
    }
}

impl parquet::file::reader::ChunkReader for S3ChunkReader {
    type T = S3ReadWindow;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        if start > self.len {
            return Err(parquet::errors::ParquetError::EOF(format!(
                "get_read offset {start} past object len {}",
                self.len
            )));
        }
        Ok(S3ReadWindow {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            pos: start,
            end: self.len,
            buf: Vec::new(),
            buf_pos: 0,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        if length == 0 {
            return Ok(bytes::Bytes::new());
        }
        let end = start.saturating_add(length as u64);
        if start > self.len || end > self.len {
            return Err(parquet::errors::ParquetError::EOF(format!(
                "get_bytes {start}+{length} past object len {}",
                self.len
            )));
        }
        let data = self
            .client
            .get_range(&self.bucket, &self.key, &(start..end))
            .map_err(|e| parquet::errors::ParquetError::External(e.into()))?;
        if data.len() != length {
            return Err(parquet::errors::ParquetError::EOF(format!(
                "expected {length} bytes at {start}, got {}",
                data.len()
            )));
        }
        Ok(bytes::Bytes::from(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayon::prelude::*;

    #[test]
    fn parse_s3_uri() {
        let (b, k) = S3Client::parse_uri("s3://rap-lake/date=2024-01-01/part.parquet").unwrap();
        assert_eq!(b, "rap-lake");
        assert_eq!(k, "date=2024-01-01/part.parquet");
    }

    #[test]
    fn put_throughput_local_minio() {
        use std::time::Instant;
        let client = S3Client::from_env();
        if std::net::TcpStream::connect(&client.cfg.endpoint).is_err() {
            eprintln!("skip: minio not listening");
            return;
        }
        let body = vec![b'x'; 700];
        // sequential
        let t0 = Instant::now();
        let n = 400usize;
        for i in 0..n {
            client
                .put_object("rap-lake", &format!(".rap-bench/rs-seq-{i}.bin"), &body)
                .unwrap();
        }
        let seq = t0.elapsed().as_secs_f64();
        // parallel
        let t1 = Instant::now();
        let n2 = 2000usize;
        (0..n2).into_par_iter().for_each(|i| {
            client
                .put_object("rap-lake", &format!(".rap-bench/rs-par-{i}.bin"), &body)
                .unwrap();
        });
        let par = t1.elapsed().as_secs_f64();
        let snap = client.stats.snapshot();
        eprintln!(
            "S3 PUT seq {n} in {seq:.3}s ({:.0}/s); par {n2} in {par:.3}s ({:.0}/s); puts={} mc={}",
            n as f64 / seq,
            n2 as f64 / par.max(1e-9),
            snap.puts,
            snap.mc_fallbacks
        );
        assert!(snap.mc_fallbacks == 0, "PUT used mc fallback");
        assert!(n2 as f64 / par > 500.0, "parallel PUT too slow: {:.0}/s", n2 as f64 / par);

        // Lake keys contain `=` (`date=YYYY-MM-DD/...`); signature must encode them.
        client
            .put_object(
                "rap-lake",
                "date=2024-01-01/bucket=000/part-sigtest.parquet",
                &body,
            )
            .unwrap();
        let snap2 = client.stats.snapshot();
        assert_eq!(
            snap2.mc_fallbacks, snap.mc_fallbacks,
            "PUT of date=… path fell back to mc (SigV4 encoding bug)"
        );
    }
}
