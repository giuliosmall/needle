//! Minimal S3 client for local MinIO (path-style HTTP) and AWS (virtual-hosted HTTPS).
//!
//! Raw TCP (+ optional `native_tls`) + AWS SigV4 - same rustc-1.85 constraint as
//! `HttpRange` (no reqwest). Supports Range GET, full GET, PUT, HEAD, ListObjectsV2.
//! Anonymous GET works when the bucket allows download (our lake setup).
//! Credentials: `NEEDLE_S3_*` / `RAP_S3_*`, then `AWS_*`, then `~/.aws/{credentials,config}`.
//! STS `session_token` is signed as `x-amz-security-token`. HTTP 429/5xx and connect
//! failures retry with jittered backoff. GET bodies are checked against
//! `x-amz-checksum-sha256` / `x-amz-checksum-crc32` / `Content-MD5` when present.

use anyhow::{bail, Context, Result};
use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
    /// STS session token (`NEEDLE_S3_SESSION_TOKEN` / `AWS_SESSION_TOKEN` / profile).
    /// When set, SigV4 includes `x-amz-security-token`.
    pub session_token: Option<String>,
    pub region: String,
    pub anonymous_read: bool,
    /// HTTPS/TLS. Env `RAP_S3_TLS` / `NEEDLE_S3_TLS` = 1/true; also true when
    /// `endpoint` contains `amazonaws.com` (or was given as `https://…`).
    pub use_tls: bool,
    /// Path-style (`/{bucket}/{key}`, Host: endpoint). Env `RAP_S3_PATH_STYLE`
    /// defaults true (MinIO). `false` = virtual-hosted (`/{key}`, Host: `{bucket}.{host}`).
    pub path_style: bool,
}

fn env_first_from(names: &[&str], get: &mut impl FnMut(&str) -> Option<String>) -> Option<String> {
    names.iter().find_map(|n| {
        get(n).and_then(|s| {
            let t = s.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        })
    })
}

#[derive(Debug, Default, Clone)]
struct AwsFileCreds {
    access_key: Option<String>,
    secret_key: Option<String>,
    session_token: Option<String>,
    region: Option<String>,
}

fn parse_ini_section(contents: &str, section: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let mut in_section = false;
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(rest) = line.strip_prefix('[') {
            if let Some(name) = rest.strip_suffix(']') {
                in_section = name.trim().eq_ignore_ascii_case(section);
            }
            continue;
        }
        if !in_section {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().trim_matches('"').to_string();
            if !key.is_empty() && !val.is_empty() {
                out.insert(key, val);
            }
        }
    }
    out
}

fn load_aws_files(home: &Path, profile: &str) -> AwsFileCreds {
    let mut out = AwsFileCreds::default();
    let cred_path = home.join(".aws").join("credentials");
    if let Ok(s) = std::fs::read_to_string(cred_path) {
        let sec = parse_ini_section(&s, profile);
        out.access_key = sec.get("aws_access_key_id").cloned();
        out.secret_key = sec.get("aws_secret_access_key").cloned();
        out.session_token = sec
            .get("aws_session_token")
            .cloned()
            .or_else(|| sec.get("aws_security_token").cloned());
        out.region = sec.get("region").cloned();
    }
    let cfg_path = home.join(".aws").join("config");
    if let Ok(s) = std::fs::read_to_string(cfg_path) {
        let section = if profile.eq_ignore_ascii_case("default") {
            "default".to_string()
        } else {
            format!("profile {profile}")
        };
        let sec = parse_ini_section(&s, &section);
        if out.region.is_none() {
            out.region = sec.get("region").cloned();
        }
    }
    out
}

fn s3_config_from_env_and_files(
    mut get: impl FnMut(&str) -> Option<String>,
    home: Option<PathBuf>,
) -> S3Config {
    let raw_endpoint = env_first_from(&["NEEDLE_S3_ENDPOINT", "RAP_S3_ENDPOINT"], &mut get)
        .unwrap_or_else(|| "127.0.0.1:9000".into());
    let scheme_https = raw_endpoint.trim().starts_with("https://");
    let endpoint = normalize_endpoint(&raw_endpoint);
    let env_tls = env_first_from(&["NEEDLE_S3_TLS", "RAP_S3_TLS"], &mut get);
    let profile = env_first_from(&["AWS_PROFILE"], &mut get).unwrap_or_else(|| "default".into());
    let file = home
        .as_deref()
        .map(|h| load_aws_files(h, &profile))
        .unwrap_or_default();
    let access_key = env_first_from(
        &[
            "NEEDLE_S3_ACCESS_KEY",
            "RAP_S3_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
        ],
        &mut get,
    )
    .or(file.access_key)
    .unwrap_or_else(|| "minioadmin".into());
    let secret_key = env_first_from(
        &[
            "NEEDLE_S3_SECRET_KEY",
            "RAP_S3_SECRET_KEY",
            "AWS_SECRET_ACCESS_KEY",
        ],
        &mut get,
    )
    .or(file.secret_key)
    .unwrap_or_else(|| "minioadmin".into());
    let session_token = env_first_from(
        &[
            "NEEDLE_S3_SESSION_TOKEN",
            "RAP_S3_SESSION_TOKEN",
            "AWS_SESSION_TOKEN",
        ],
        &mut get,
    )
    .or(file.session_token);
    let region = env_first_from(
        &[
            "NEEDLE_S3_REGION",
            "RAP_S3_REGION",
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
        ],
        &mut get,
    )
    .or(file.region)
    .unwrap_or_else(|| "us-east-1".into());
    let env_anon = env_first_from(&["NEEDLE_S3_ANON_READ", "RAP_S3_ANON_READ"], &mut get);
    let env_path = env_first_from(&["NEEDLE_S3_PATH_STYLE", "RAP_S3_PATH_STYLE"], &mut get);
    S3Config {
        endpoint: endpoint.clone(),
        access_key,
        secret_key,
        session_token,
        region,
        anonymous_read: env_anon
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(!endpoint.contains("amazonaws.com")),
        use_tls: infer_use_tls(&endpoint, env_tls.as_deref(), scheme_https),
        path_style: infer_path_style(env_path.as_deref(), &endpoint),
    }
}

impl Default for S3Config {
    fn default() -> Self {
        let home = std::env::var("HOME")
            .ok()
            .or_else(|| std::env::var("USERPROFILE").ok())
            .map(PathBuf::from);
        s3_config_from_env_and_files(|n| std::env::var(n).ok(), home)
    }
}

/// HEAD result: object size plus optional ETag (quotes stripped).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadObject {
    pub size: u64,
    pub etag: Option<String>,
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
        if let Some(rest) = strip_s3_scheme(uri) {
            if rest.is_empty() {
                bail!("s3 uri missing bucket: {uri}");
            }
            return Ok(match rest.split_once('/') {
                Some((bucket, key)) => (bucket.to_string(), key.to_string()),
                None => (rest.to_string(), String::new()),
            });
        }
        if let Some(bare) = uri
            .strip_prefix("https://")
            .or_else(|| uri.strip_prefix("http://"))
        {
            return parse_http_s3_uri(bare, uri);
        }
        bail!("not an s3/http object uri: {uri}");
    }

    pub fn is_remote_uri(uri: &str) -> bool {
        uri.starts_with("s3://")
            || uri.starts_with("s3a://")
            || uri.starts_with("s3n://")
            || ((uri.starts_with("http://") || uri.starts_with("https://")) && uri.contains('/'))
    }

    pub fn s3_uri(bucket: &str, key: &str) -> String {
        format!("s3://{bucket}/{key}")
    }

    fn object_path(&self, bucket: &str, key: &str) -> String {
        if self.cfg.path_style {
            if key.is_empty() {
                format!("/{bucket}")
            } else {
                format!("/{bucket}/{key}")
            }
        } else if key.is_empty() {
            "/".to_string()
        } else {
            format!("/{key}")
        }
    }

    /// Host header value (virtual-hosted strips the endpoint port).
    fn host_header(&self, bucket: &str) -> String {
        if self.cfg.path_style {
            self.cfg.endpoint.clone()
        } else {
            let host = split_host_port(&self.cfg.endpoint).0;
            format!("{bucket}.{host}")
        }
    }

    /// TCP connect address (`host:port`); adds :443 when TLS and no port given.
    fn connect_addr(&self, bucket: &str) -> String {
        if self.cfg.path_style {
            let (host, port) = split_host_port(&self.cfg.endpoint);
            match port {
                Some(p) => format!("{host}:{p}"),
                None if self.cfg.use_tls => format!("{host}:443"),
                None => format!("{host}:80"),
            }
        } else {
            let (ep_host, ep_port) = split_host_port(&self.cfg.endpoint);
            let host = format!("{bucket}.{ep_host}");
            match ep_port {
                Some(p) => format!("{host}:{p}"),
                None if self.cfg.use_tls => format!("{host}:443"),
                None => format!("{host}:80"),
            }
        }
    }

    fn sni_name(&self, bucket: &str) -> String {
        split_host_port(&self.host_header(bucket)).0.to_string()
    }

    pub fn put_object(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        let uri = self.object_path(bucket, key);
        // MinIO accepts UNSIGNED-PAYLOAD; hashing 1M tiny bodies was wasted CPU.
        let (status, _h, resp) = self.http(
            "PUT",
            bucket,
            &uri,
            "",
            body,
            "UNSIGNED-PAYLOAD",
            None,
            None,
            true,
        )?;
        if !(200..300).contains(&status) {
            self.stats.mc_fallbacks.fetch_add(1, Ordering::Relaxed);
            static FIRST: std::sync::Once = std::sync::Once::new();
            FIRST.call_once(|| {
                eprintln!(
                    "S3 PUT fallback status={status} uri={uri} body_prefix={}",
                    String::from_utf8_lossy(&resp)
                        .chars()
                        .take(300)
                        .collect::<String>()
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

    /// Create-only PUT (`If-None-Match: *`). 412 means another writer already created the object.
    /// Does not fall back to `mc` (that would clobber).
    pub fn put_object_if_none_match(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        self.put_object_conditional(bucket, key, body, ("if-none-match", "*"))
    }

    /// Compare-and-swap PUT (`If-Match: <etag>`). 412 means the generation changed.
    pub fn put_object_if_match(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> Result<()> {
        self.put_object_conditional(bucket, key, body, ("if-match", etag))
    }

    fn put_object_conditional(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        cond: (&str, &str),
    ) -> Result<()> {
        let uri = self.object_path(bucket, key);
        let (status, _h, resp) = self.http(
            "PUT",
            bucket,
            &uri,
            "",
            body,
            "UNSIGNED-PAYLOAD",
            None,
            Some(cond),
            true,
        )?;
        if status == 412 {
            bail!("s3_precondition_failed: concurrent writer lost the race for {uri}");
        }
        if !(200..300).contains(&status) {
            bail!(
                "S3 conditional PUT {uri} status {status}: {}",
                String::from_utf8_lossy(&resp)
            );
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
        let tmp = std::env::temp_dir().join(format!("rap-put-{}-{}.bin", std::process::id(), {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(key.as_bytes());
            hex::encode(&h.finalize()[..8])
        }));
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
            bail!("mc cp failed: {}", String::from_utf8_lossy(&st.stderr));
        }
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_written
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let uri = self.object_path(bucket, key);
        let payload_hash = hex_sha256(b"");
        let sign = !self.cfg.anonymous_read;
        let (status, headers, body) = self.http(
            "GET",
            bucket,
            &uri,
            "",
            &[],
            &payload_hash,
            None,
            None,
            sign,
        )?;
        if status != 200 {
            bail!(
                "S3 GET {uri} status {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        verify_s3_body_checksums(&headers, &body)?;
        self.stats.full_gets.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_read
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(body)
    }

    pub fn get_range(&self, bucket: &str, key: &str, range: &Range<u64>) -> Result<Vec<u8>> {
        let uri = self.object_path(bucket, key);
        let payload_hash = hex_sha256(b"");
        let end_incl = range.end.saturating_sub(1);
        let range_hdr = format!("bytes={}-{}", range.start, end_incl);
        let sign = !self.cfg.anonymous_read;
        let (status, headers, body) = self.http(
            "GET",
            bucket,
            &uri,
            "",
            &[],
            &payload_hash,
            Some(&range_hdr),
            None,
            sign,
        )?;
        if !(status == 206 || status == 200) {
            bail!(
                "S3 Range GET {uri} {range_hdr} status {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        verify_s3_body_checksums(&headers, &body)?;
        self.stats.range_gets.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_read
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(body)
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> Result<u64> {
        Ok(self.head_object_meta(bucket, key)?.size)
    }

    pub fn head_object_meta(&self, bucket: &str, key: &str) -> Result<HeadObject> {
        let uri = self.object_path(bucket, key);
        let payload_hash = hex_sha256(b"");
        let sign = !self.cfg.anonymous_read;
        let (status, headers, body) = self.http(
            "HEAD",
            bucket,
            &uri,
            "",
            &[],
            &payload_hash,
            None,
            None,
            sign,
        )?;
        if status != 200 {
            bail!(
                "S3 HEAD {uri} status {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        parse_head_headers(&headers)
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
            let uri = self.object_path(bucket, "");
            let payload_hash = hex_sha256(b"");
            let (status, _h, body) = self.http(
                "GET",
                bucket,
                &uri,
                &query,
                &[],
                &payload_hash,
                None,
                None,
                true,
            )?;
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
        bucket: &str,
        canonical_uri: &str,
        query: &str,
        body: &[u8],
        payload_hash: &str,
        range: Option<&str>,
        extra_signed: Option<(&str, &str)>,
        sign: bool,
    ) -> Result<(u16, String, Vec<u8>)> {
        // Initial attempt + up to 4 retries on connect failure and 429/5xx.
        const MAX_RETRIES: u32 = 4;
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                std::thread::sleep(s3_retry_delay(attempt - 1));
            }
            let try_reuse = attempt == 0;
            match self.http_once(
                method,
                bucket,
                canonical_uri,
                query,
                body,
                payload_hash,
                range,
                extra_signed,
                sign,
                try_reuse,
            ) {
                Ok((status, headers, body_out)) => {
                    if retryable_http_status(status) && attempt < MAX_RETRIES {
                        last_err = Some(anyhow::anyhow!(
                            "S3 {method} {canonical_uri} status {status}"
                        ));
                        continue;
                    }
                    return Ok((status, headers, body_out));
                }
                Err(e) => {
                    if attempt < MAX_RETRIES {
                        last_err = Some(e);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("S3 {method} {canonical_uri} failed")))
    }

    #[allow(clippy::too_many_arguments)]
    fn http_once(
        &self,
        method: &str,
        bucket: &str,
        canonical_uri: &str,
        query: &str,
        body: &[u8],
        payload_hash: &str,
        range: Option<&str>,
        extra_signed: Option<(&str, &str)>,
        sign: bool,
        try_reuse: bool,
    ) -> Result<(u16, String, Vec<u8>)> {
        let host = self.host_header(bucket);
        let connect_to = self.connect_addr(bucket);
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let encoded_uri = encode_path(canonical_uri);
        let request_target = if query.is_empty() {
            encoded_uri.clone()
        } else {
            format!("{encoded_uri}?{query}")
        };

        let session_token = self
            .cfg
            .session_token
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty());
        let content_md5 = if matches!(method, "PUT" | "POST") {
            Some(b64_encode(&md5_digest(body)))
        } else {
            None
        };

        let auth_header = if sign {
            let credential_scope = format!("{}/{}/s3/aws4_request", date_stamp, self.cfg.region);
            let mut pairs: Vec<(String, String)> = vec![
                ("host".into(), host.clone()),
                ("x-amz-content-sha256".into(), payload_hash.to_string()),
                ("x-amz-date".into(), amz_date.clone()),
            ];
            if let Some(r) = range {
                pairs.push(("range".into(), r.to_string()));
            }
            if let Some(tok) = session_token {
                pairs.push(("x-amz-security-token".into(), tok.to_string()));
            }
            if let Some(md5) = content_md5.as_deref() {
                pairs.push(("content-md5".into(), md5.to_string()));
            }
            if let Some((n, v)) = extra_signed {
                pairs.push((n.to_ascii_lowercase(), v.to_string()));
            }
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let canonical_headers: String =
                pairs.iter().map(|(n, v)| format!("{n}:{v}\n")).collect();
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
            let string_to_sign =
                format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_hash}");
            let key = signing_key(&self.cfg.secret_key, &date_stamp, &self.cfg.region, "s3")?;
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
        if sign {
            if let Some(tok) = session_token {
                extra.push_str(&format!("x-amz-security-token: {tok}\r\n"));
            }
        }
        if let Some(md5) = content_md5.as_deref() {
            extra.push_str(&format!("Content-MD5: {md5}\r\n"));
        }
        if let Some((n, v)) = extra_signed {
            extra.push_str(&format!("{n}: {v}\r\n"));
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
            take_pooled(&connect_to).unwrap_or(self.connect_stream(bucket)?)
        } else {
            self.connect_stream(bucket)?
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
            put_pooled(&connect_to, stream);
        }
        Ok((status, headers, body_out))
    }

    fn connect_stream(&self, bucket: &str) -> Result<S3Stream> {
        let addr = self.connect_addr(bucket);
        let tcp = TcpStream::connect(&addr).with_context(|| format!("connect {addr}"))?;
        tcp.set_nodelay(true)?;
        tcp.set_read_timeout(Some(Duration::from_secs(60)))?;
        tcp.set_write_timeout(Some(Duration::from_secs(60)))?;
        if self.cfg.use_tls {
            let sni = self.sni_name(bucket);
            let connector = native_tls::TlsConnector::new().context("tls connector")?;
            let tls = connector
                .connect(&sni, tcp)
                .map_err(|e| anyhow::anyhow!("tls handshake {sni}: {e}"))?;
            Ok(S3Stream::Tls(tls))
        } else {
            Ok(S3Stream::Plain(tcp))
        }
    }
}

enum S3Stream {
    Plain(TcpStream),
    Tls(native_tls::TlsStream<TcpStream>),
}

impl Read for S3Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            S3Stream::Plain(s) => s.read(buf),
            S3Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for S3Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            S3Stream::Plain(s) => s.write(buf),
            S3Stream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            S3Stream::Plain(s) => s.flush(),
            S3Stream::Tls(s) => s.flush(),
        }
    }
}

thread_local! {
    static POOLED: RefCell<Option<(String, S3Stream)>> = RefCell::new(None);
}

fn take_pooled(addr: &str) -> Option<S3Stream> {
    POOLED.with(|slot| match slot.borrow_mut().take() {
        Some((h, s)) if h == addr => Some(s),
        Some((_, s)) => {
            drop(s);
            None
        }
        None => None,
    })
}

fn put_pooled(addr: &str, stream: S3Stream) {
    POOLED.with(|slot| {
        *slot.borrow_mut() = Some((addr.to_string(), stream));
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
    stream: &mut impl Read,
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

fn sha256_raw(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

fn retryable_http_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// Full jitter exponential backoff. `retry` is 0-based (first retry = 0).
fn s3_retry_delay(retry: u32) -> Duration {
    let cap_ms = 400u64;
    let base_ms = (25u64 << retry.min(4)).min(cap_ms);
    let jitter = rand::thread_rng().gen_range(0..=base_ms);
    Duration::from_millis(jitter)
}

fn header_value<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    for line in headers.lines() {
        let Some((n, v)) = line.split_once(':') else {
            continue;
        };
        if n.trim().eq_ignore_ascii_case(name) {
            return Some(v.trim());
        }
    }
    None
}

const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn b64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    let mut i = 0;
    while i < data.len() {
        let remaining = data.len() - i;
        let b0 = data[i];
        let b1 = if remaining > 1 { data[i + 1] } else { 0 };
        let b2 = if remaining > 2 { data[i + 2] } else { 0 };
        out.push(B64_ALPHABET[(b0 >> 2) as usize] as char);
        out.push(B64_ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if remaining == 1 {
            out.push('=');
            out.push('=');
        } else {
            out.push(B64_ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
            if remaining == 2 {
                out.push('=');
            } else {
                out.push(B64_ALPHABET[(b2 & 0x3f) as usize] as char);
            }
        }
        i += 3;
    }
    out
}

fn b64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let filtered: Vec<u8> = s
        .bytes()
        .filter(|b| !b.is_ascii_whitespace() && *b != b'=')
        .collect();
    if filtered.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::with_capacity(filtered.len() * 3 / 4);
    let mut i = 0;
    while i < filtered.len() {
        let v0 = val(filtered[i])?;
        let v1 = if i + 1 < filtered.len() {
            val(filtered[i + 1])?
        } else {
            return None;
        };
        out.push((v0 << 2) | (v1 >> 4));
        if i + 2 < filtered.len() {
            let v2 = val(filtered[i + 2])?;
            out.push((v1 << 4) | (v2 >> 2));
            if i + 3 < filtered.len() {
                let v3 = val(filtered[i + 3])?;
                out.push((v2 << 6) | v3);
            }
        }
        i += 4;
    }
    Some(out)
}

fn checksum_header_bytes(raw: &str, expected_len: usize) -> Option<Vec<u8>> {
    let s = raw.trim().trim_matches('"');
    if let Some(v) = b64_decode(s) {
        if v.len() == expected_len {
            return Some(v);
        }
    }
    if s.len() == expected_len * 2 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        if let Ok(v) = hex::decode(s) {
            if v.len() == expected_len {
                return Some(v);
            }
        }
    }
    None
}

/// IEEE CRC-32 (ISO 3309 / ITU-T V.42), as used by `x-amz-checksum-crc32`.
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn md5_digest(data: &[u8]) -> [u8; 16] {
    fn f(x: u32, y: u32, z: u32) -> u32 {
        (x & y) | (!x & z)
    }
    fn g(x: u32, y: u32, z: u32) -> u32 {
        (x & z) | (y & !z)
    }
    fn h(x: u32, y: u32, z: u32) -> u32 {
        x ^ y ^ z
    }
    fn i_fn(x: u32, y: u32, z: u32) -> u32 {
        y ^ (x | !z)
    }
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];
    let mut a0 = 0x6745_2301u32;
    let mut b0 = 0xefcd_ab89u32;
    let mut c0 = 0x98ba_dcfeu32;
    let mut d0 = 0x1032_5476u32;
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_le_bytes());
    for chunk in padded.chunks_exact(64) {
        let mut m = [0u32; 16];
        for (i, slot) in m.iter_mut().enumerate() {
            let o = i * 4;
            *slot = u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
        }
        let mut a = a0;
        let mut b = b0;
        let mut c = c0;
        let mut d = d0;
        for i in 0..64 {
            let (fval, gidx) = if i < 16 {
                (f(b, c, d), i)
            } else if i < 32 {
                (g(b, c, d), (5 * i + 1) % 16)
            } else if i < 48 {
                (h(b, c, d), (3 * i + 5) % 16)
            } else {
                (i_fn(b, c, d), (7 * i) % 16)
            };
            let fval = fval
                .wrapping_add(a)
                .wrapping_add(K[i])
                .wrapping_add(m[gidx]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(fval.rotate_left(S[i]));
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }
    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&a0.to_le_bytes());
    out[4..8].copy_from_slice(&b0.to_le_bytes());
    out[8..12].copy_from_slice(&c0.to_le_bytes());
    out[12..16].copy_from_slice(&d0.to_le_bytes());
    out
}

fn verify_s3_body_checksums(headers: &str, body: &[u8]) -> Result<()> {
    if let Some(raw) = header_value(headers, "x-amz-checksum-sha256") {
        let got = sha256_raw(body);
        match checksum_header_bytes(raw, 32) {
            Some(want) if want.as_slice() == got.as_slice() => {}
            _ => {
                bail!("s3_checksum_mismatch: x-amz-checksum-sha256 {raw}");
            }
        }
    }
    if let Some(raw) = header_value(headers, "x-amz-checksum-crc32") {
        let got = crc32_ieee(body).to_be_bytes();
        match checksum_header_bytes(raw, 4) {
            Some(want) if want.as_slice() == got.as_slice() => {}
            _ => {
                bail!("s3_checksum_mismatch: x-amz-checksum-crc32 {raw}");
            }
        }
    }
    if let Some(raw) = header_value(headers, "content-md5") {
        let got = md5_digest(body);
        match checksum_header_bytes(raw, 16) {
            Some(want) if want.as_slice() == got.as_slice() => {}
            _ => {
                bail!("s3_checksum_mismatch: Content-MD5 {raw}");
            }
        }
    }
    Ok(())
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

fn parse_head_headers(headers: &str) -> Result<HeadObject> {
    let mut size = None;
    let mut etag = None;
    for line in headers.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("content-length") {
            size = Some(
                value
                    .trim()
                    .parse::<u64>()
                    .with_context(|| format!("S3 HEAD Content-Length {}", value.trim()))?,
            );
        } else if name.trim().eq_ignore_ascii_case("etag") {
            let v = value.trim().trim_matches('"');
            if !v.is_empty() {
                etag = Some(v.to_string());
            }
        }
    }
    let size = size.ok_or_else(|| anyhow::anyhow!("S3 HEAD missing Content-Length"))?;
    Ok(HeadObject { size, etag })
}

fn parse_env_bool(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn infer_use_tls(endpoint: &str, env_tls: Option<&str>, scheme_https: bool) -> bool {
    if let Some(v) = env_tls {
        return parse_env_bool(v);
    }
    scheme_https || endpoint.contains("amazonaws.com")
}

fn infer_path_style(rap_path_style: Option<&str>, endpoint: &str) -> bool {
    match rap_path_style {
        Some(v) => parse_env_bool(v),
        None => !endpoint.contains("amazonaws.com"),
    }
}

fn normalize_endpoint(raw: &str) -> String {
    let s = raw.trim();
    let s = s
        .strip_prefix("https://")
        .or_else(|| s.strip_prefix("http://"))
        .unwrap_or(s);
    s.trim_end_matches('/').to_string()
}

fn strip_s3_scheme(uri: &str) -> Option<&str> {
    uri.strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .or_else(|| uri.strip_prefix("s3n://"))
}

fn parse_http_s3_uri(bare: &str, uri: &str) -> Result<(String, String)> {
    let (hostport, path) = bare
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("http s3 uri missing path: {uri}"))?;
    let path = path.split_once('?').map(|(p, _)| p).unwrap_or(path);
    let host = split_host_port(hostport).0;
    if let Some(bucket) = virtual_hosted_s3_bucket(host) {
        return Ok((bucket.to_string(), path.to_string()));
    }
    let (bucket, key) = path
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("http s3 uri missing key: {uri}"))?;
    Ok((bucket.to_string(), key.to_string()))
}

/// Split `host` or `host:port` (IPv6 `[::1]:443` supported). Port must be all digits.
fn split_host_port(endpoint: &str) -> (&str, Option<&str>) {
    let endpoint = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))
        .unwrap_or(endpoint);
    if let Some(rest) = endpoint.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            let host = &endpoint[..=end];
            let after = &rest[end + 1..];
            if let Some(port) = after.strip_prefix(':') {
                if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) {
                    return (host, Some(port));
                }
            }
            return (host, None);
        }
    }
    if let Some((h, p)) = endpoint.rsplit_once(':') {
        if !h.is_empty() && !h.contains(':') && p.chars().all(|c| c.is_ascii_digit()) {
            return (h, Some(p));
        }
    }
    (endpoint, None)
}

fn is_s3_service_host(lower: &str) -> bool {
    if lower == "s3.amazonaws.com" {
        return true;
    }
    if let Some(rest) = lower.strip_prefix("s3.") {
        return rest == "amazonaws.com"
            || rest.ends_with(".amazonaws.com")
            || rest.ends_with("amazonaws.com");
    }
    if let Some(rest) = lower.strip_prefix("s3-") {
        return rest.ends_with(".amazonaws.com") || rest.ends_with("amazonaws.com");
    }
    false
}

/// Virtual-hosted AWS host → bucket (`bucket.s3.amazonaws.com`, `bucket.s3.region.amazonaws.com`).
fn virtual_hosted_s3_bucket(host: &str) -> Option<&str> {
    let host = host.trim_end_matches('.');
    let lower = host.to_ascii_lowercase();
    if is_s3_service_host(&lower) {
        return None;
    }
    if let Some(idx) = lower.find(".s3.") {
        let rest = &lower[idx + 1..];
        if rest.ends_with("amazonaws.com") && idx > 0 {
            return Some(&host[..idx]);
        }
    }
    if let Some(idx) = lower.find(".s3-") {
        let rest = &lower[idx + 1..];
        if rest.ends_with("amazonaws.com") && idx > 0 {
            return Some(&host[..idx]);
        }
    }
    None
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
/// `RowSelection`, page bodies come through `get_bytes` of individual pages -
/// never a whole-object GET.
pub struct S3ChunkReader {
    client: S3Client,
    bucket: String,
    key: String,
    len: u64,
}

impl S3ChunkReader {
    pub fn open(
        client: S3Client,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Self> {
        let bucket = bucket.into();
        let key = key.into();
        let len = match client.head_object_meta(&bucket, &key) {
            Ok(h) => h.size,
            Err(_) => {
                // Retry signed HEAD if anonymous download does not cover HEAD.
                let mut signed = client.clone();
                signed.cfg.anonymous_read = false;
                signed.head_object_meta(&bucket, &key)?.size
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
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::AtomicBool;

    struct MockS3 {
        endpoint: String,
        hits: Arc<AtomicU64>,
        stop: Arc<AtomicBool>,
        join: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for MockS3 {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(&self.endpoint);
            if let Some(j) = self.join.take() {
                let _ = j.join();
            }
        }
    }

    fn test_client(endpoint: &str, session_token: Option<&str>) -> S3Client {
        S3Client::new(S3Config {
            endpoint: endpoint.to_string(),
            access_key: "AKIA_TEST".into(),
            secret_key: "testsecret".into(),
            session_token: session_token.map(|s| s.to_string()),
            region: "us-east-1".into(),
            anonymous_read: false,
            use_tls: false,
            path_style: true,
        })
    }

    fn start_mock(
        handler: impl Fn(u64, &str) -> (u16, Vec<(String, String)>, Vec<u8>) + Send + Sync + 'static,
    ) -> MockS3 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock s3");
        let endpoint = listener.local_addr().unwrap().to_string();
        let hits = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let hits_c = hits.clone();
        let stop_c = stop.clone();
        let handler = Arc::new(handler);
        let join = std::thread::spawn(move || {
            for incoming in listener.incoming() {
                if stop_c.load(Ordering::SeqCst) {
                    break;
                }
                let mut stream = match incoming {
                    Ok(s) => s,
                    Err(_) => break,
                };
                if stop_c.load(Ordering::SeqCst) {
                    break;
                }
                let _ = stream.set_nodelay(true);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                let header_end = loop {
                    match stream.read(&mut tmp) {
                        Ok(0) => break None,
                        Ok(n) => {
                            buf.extend_from_slice(&tmp[..n]);
                            if let Some(s) = find_header_end(&buf) {
                                break Some(s);
                            }
                            if buf.len() > 1024 * 1024 {
                                break None;
                            }
                        }
                        Err(_) => break None,
                    }
                };
                let Some(sep) = header_end else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&buf[..sep]).into_owned();
                let n = hits_c.fetch_add(1, Ordering::SeqCst) + 1;
                let (status, extra, body) = handler(n, &headers);
                let reason = match status {
                    200 => "OK",
                    206 => "Partial Content",
                    403 => "Forbidden",
                    404 => "Not Found",
                    503 => "Service Unavailable",
                    _ => "Error",
                };
                let mut resp = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
                    body.len()
                );
                for (k, v) in extra {
                    resp.push_str(&format!("{k}: {v}\r\n"));
                }
                resp.push_str("\r\n");
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        MockS3 {
            endpoint,
            hits,
            stop,
            join: Some(join),
        }
    }

    #[test]
    fn parse_s3_uri() {
        let (b, k) = S3Client::parse_uri("s3://rap-lake/date=2024-01-01/part.parquet").unwrap();
        assert_eq!(b, "rap-lake");
        assert_eq!(k, "date=2024-01-01/part.parquet");
    }

    #[test]
    fn parse_https_virtual_hosted_uri() {
        let (b, k) = S3Client::parse_uri("https://bucket.s3.amazonaws.com/key").unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "key");
        let (b, k) =
            S3Client::parse_uri("https://my-bucket.s3.us-east-1.amazonaws.com/path/to/obj.parquet")
                .unwrap();
        assert_eq!(b, "my-bucket");
        assert_eq!(k, "path/to/obj.parquet");
        let (b, k) = S3Client::parse_uri("https://bucket.s3.amazonaws.com:443/dir/file").unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "dir/file");
    }

    #[test]
    fn parse_https_path_style_uri() {
        let (b, k) = S3Client::parse_uri("https://s3.amazonaws.com/bucket/key").unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "key");
        let (b, k) =
            S3Client::parse_uri("https://minio.example.com/mybucket/dir/file.parquet").unwrap();
        assert_eq!(b, "mybucket");
        assert_eq!(k, "dir/file.parquet");
        let (b, k) =
            S3Client::parse_uri("http://127.0.0.1:9000/rap-lake/date=2024-01-01/part.parquet")
                .unwrap();
        assert_eq!(b, "rap-lake");
        assert_eq!(k, "date=2024-01-01/part.parquet");
    }

    #[test]
    fn is_remote_uri_https() {
        assert!(S3Client::is_remote_uri(
            "https://bucket.s3.amazonaws.com/key"
        ));
        assert!(S3Client::is_remote_uri("https://host/bucket/key"));
        assert!(S3Client::is_remote_uri("s3://bucket/key"));
        assert!(S3Client::is_remote_uri("s3a://bucket/key"));
        assert!(S3Client::is_remote_uri("http://127.0.0.1:9000/bucket/key"));
        assert!(!S3Client::is_remote_uri("/local/path"));
        assert!(!S3Client::is_remote_uri("file:///tmp/x"));
    }

    #[test]
    fn s3config_tls_default_for_amazonaws_endpoint() {
        assert!(infer_use_tls("s3.amazonaws.com", None, false));
        assert!(infer_use_tls("s3.us-east-1.amazonaws.com", None, false));
        assert!(!infer_use_tls("127.0.0.1:9000", None, false));
        assert!(!infer_use_tls("minio.local:9000", None, false));
        assert!(infer_use_tls("127.0.0.1:9000", Some("1"), false));
        assert!(infer_use_tls("127.0.0.1:9000", Some("true"), false));
        assert!(!infer_use_tls("s3.amazonaws.com", Some("0"), false));
        assert!(!infer_use_tls("s3.amazonaws.com", Some("false"), false));
        assert!(infer_use_tls("127.0.0.1:9000", None, true));
        assert!(infer_path_style(None, "127.0.0.1:9000"));
        assert!(!infer_path_style(None, "s3.amazonaws.com"));
        assert!(!infer_path_style(Some("0"), "127.0.0.1:9000"));
        assert!(!infer_path_style(Some("false"), "127.0.0.1:9000"));
        assert!(infer_path_style(Some("1"), "s3.amazonaws.com"));
    }

    #[test]
    fn virtual_hosted_request_shape() {
        let client = S3Client::new(S3Config {
            endpoint: "s3.us-east-1.amazonaws.com".into(),
            access_key: "x".into(),
            secret_key: "y".into(),
            session_token: None,
            region: "us-east-1".into(),
            anonymous_read: true,
            use_tls: true,
            path_style: false,
        });
        assert_eq!(
            client.object_path("mybkt", "date=2024-01-01/part.parquet"),
            "/date=2024-01-01/part.parquet"
        );
        assert_eq!(
            client.host_header("mybkt"),
            "mybkt.s3.us-east-1.amazonaws.com"
        );
        assert_eq!(
            client.connect_addr("mybkt"),
            "mybkt.s3.us-east-1.amazonaws.com:443"
        );
        assert_eq!(client.sni_name("mybkt"), "mybkt.s3.us-east-1.amazonaws.com");
    }

    #[test]
    fn path_style_request_shape_minio() {
        let client = S3Client::new(S3Config {
            endpoint: "127.0.0.1:9000".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            session_token: None,
            region: "us-east-1".into(),
            anonymous_read: true,
            use_tls: false,
            path_style: true,
        });
        assert_eq!(client.object_path("rap-lake", "a/b"), "/rap-lake/a/b");
        assert_eq!(client.host_header("rap-lake"), "127.0.0.1:9000");
        assert_eq!(client.connect_addr("rap-lake"), "127.0.0.1:9000");
        assert!(!client.cfg.use_tls);
        assert!(client.cfg.path_style);
    }

    #[test]
    fn parse_etag_header_any_case() {
        let m = parse_head_headers("HTTP/1.1 200 OK\r\nContent-Length: 42\r\nETag: \"abcDEF\"\r\n")
            .unwrap();
        assert_eq!(m.size, 42);
        assert_eq!(m.etag.as_deref(), Some("abcDEF"));
        let m2 =
            parse_head_headers("HTTP/1.1 200 OK\r\ncontent-length: 1\r\netag: xyz\r\n").unwrap();
        assert_eq!(m2.size, 1);
        assert_eq!(m2.etag.as_deref(), Some("xyz"));
    }

    #[test]
    fn head_object_meta_etag_on_minio() {
        let client = S3Client::from_env();
        if std::net::TcpStream::connect(&client.cfg.endpoint).is_err() {
            eprintln!("skip: minio not listening");
            return;
        }
        let key = ".rap-bench/head-etag-test.bin";
        let body = b"hello-etag";
        if client.put_object("rap-lake", key, body).is_err() {
            eprintln!("skip: minio put failed");
            return;
        }
        let meta = client.head_object_meta("rap-lake", key).unwrap();
        assert_eq!(meta.size, body.len() as u64);
        if let Some(etag) = meta.etag {
            assert!(
                !etag.contains('"'),
                "etag quotes should be stripped: {etag}"
            );
        }
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
        assert!(
            n2 as f64 / par > 500.0,
            "parallel PUT too slow: {:.0}/s",
            n2 as f64 / par
        );

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

    #[test]
    fn checksum_primitives_and_verify() {
        assert_eq!(
            hex::encode(md5_digest(b"")),
            "d41d8cd98f00b204e9800998ecf8427e"
        );
        assert_eq!(
            hex::encode(md5_digest(b"hello")),
            "5d41402abc4b2a76b9719d911017c592"
        );
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF4_3926);
        assert_eq!(b64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(b64_decode("aGVsbG8=").as_deref(), Some(&b"hello"[..]));
        assert_eq!(b64_encode(&md5_digest(b"")), "1B2M2Y8AsgTpgAmY7PhCfg==");
        let body = b"hello";
        let sha = b64_encode(&sha256_raw(body));
        verify_s3_body_checksums(
            &format!("HTTP/1.1 200 OK\r\nx-amz-checksum-sha256: {sha}\r\n"),
            body,
        )
        .unwrap();
        let crc = b64_encode(&crc32_ieee(body).to_be_bytes());
        verify_s3_body_checksums(
            &format!("HTTP/1.1 200 OK\r\nx-amz-checksum-crc32: {crc}\r\n"),
            body,
        )
        .unwrap();
        let md = b64_encode(&md5_digest(body));
        verify_s3_body_checksums(&format!("HTTP/1.1 200 OK\r\nContent-MD5: {md}\r\n"), body)
            .unwrap();
        let err =
            verify_s3_body_checksums("HTTP/1.1 200 OK\r\nx-amz-checksum-sha256: AAAA\r\n", body)
                .unwrap_err();
        assert!(
            format!("{err:#}").contains("s3_checksum_mismatch"),
            "{err:#}"
        );
    }

    #[test]
    fn credential_chain_needle_beats_aws_and_files() {
        let tmp = tempfile::tempdir().unwrap();
        let aws = tmp.path().join(".aws");
        std::fs::create_dir_all(&aws).unwrap();
        std::fs::write(
            aws.join("credentials"),
            "[default]\naws_access_key_id = FILEAK\naws_secret_access_key = FILEsk\naws_session_token = FILEtok\n",
        )
        .unwrap();
        std::fs::write(aws.join("config"), "[default]\nregion = ap-south-1\n").unwrap();

        let mut env: HashMap<String, String> = HashMap::new();
        env.insert("NEEDLE_S3_ACCESS_KEY".into(), "nkey".into());
        env.insert("NEEDLE_S3_SECRET_KEY".into(), "nsecret".into());
        env.insert("NEEDLE_S3_SESSION_TOKEN".into(), "ntok".into());
        env.insert("NEEDLE_S3_REGION".into(), "us-west-2".into());
        env.insert("AWS_ACCESS_KEY_ID".into(), "akey".into());
        env.insert("AWS_SECRET_ACCESS_KEY".into(), "asecret".into());
        env.insert("AWS_SESSION_TOKEN".into(), "atok".into());
        env.insert("AWS_REGION".into(), "eu-west-1".into());
        let cfg =
            s3_config_from_env_and_files(|n| env.get(n).cloned(), Some(tmp.path().to_path_buf()));
        assert_eq!(cfg.access_key, "nkey");
        assert_eq!(cfg.secret_key, "nsecret");
        assert_eq!(cfg.session_token.as_deref(), Some("ntok"));
        assert_eq!(cfg.region, "us-west-2");

        env.clear();
        env.insert("AWS_ACCESS_KEY_ID".into(), "akey".into());
        env.insert("AWS_SECRET_ACCESS_KEY".into(), "asecret".into());
        env.insert("AWS_SESSION_TOKEN".into(), "atok".into());
        env.insert("AWS_DEFAULT_REGION".into(), "eu-central-1".into());
        let cfg =
            s3_config_from_env_and_files(|n| env.get(n).cloned(), Some(tmp.path().to_path_buf()));
        assert_eq!(cfg.access_key, "akey");
        assert_eq!(cfg.secret_key, "asecret");
        assert_eq!(cfg.session_token.as_deref(), Some("atok"));
        assert_eq!(cfg.region, "eu-central-1");

        env.clear();
        let cfg =
            s3_config_from_env_and_files(|n| env.get(n).cloned(), Some(tmp.path().to_path_buf()));
        assert_eq!(cfg.access_key, "FILEAK");
        assert_eq!(cfg.secret_key, "FILEsk");
        assert_eq!(cfg.session_token.as_deref(), Some("FILEtok"));
        assert_eq!(cfg.region, "ap-south-1");
    }

    #[test]
    fn credential_chain_aws_profile_and_config() {
        let tmp = tempfile::tempdir().unwrap();
        let aws = tmp.path().join(".aws");
        std::fs::create_dir_all(&aws).unwrap();
        std::fs::write(
            aws.join("credentials"),
            "[default]\naws_access_key_id = DEF\naws_secret_access_key = defs\n\n[dev]\naws_access_key_id = DEV\naws_secret_access_key = devs\naws_session_token = devtok\n",
        )
        .unwrap();
        std::fs::write(
            aws.join("config"),
            "[default]\nregion = us-east-1\n\n[profile dev]\nregion = eu-west-1\n",
        )
        .unwrap();
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert("AWS_PROFILE".into(), "dev".into());
        let cfg =
            s3_config_from_env_and_files(|n| env.get(n).cloned(), Some(tmp.path().to_path_buf()));
        assert_eq!(cfg.access_key, "DEV");
        assert_eq!(cfg.secret_key, "devs");
        assert_eq!(cfg.session_token.as_deref(), Some("devtok"));
        assert_eq!(cfg.region, "eu-west-1");
    }

    #[test]
    fn mock_range_get_retries_on_503() {
        let mock = start_mock(|n, _headers| {
            if n == 1 {
                (
                    503,
                    vec![("Content-Type".into(), "application/xml".into())],
                    b"<Error>slow down</Error>".to_vec(),
                )
            } else {
                (
                    206,
                    vec![
                        ("Content-Type".into(), "application/octet-stream".into()),
                        ("Content-Range".into(), "bytes 0-3/4".into()),
                    ],
                    b"abcd".to_vec(),
                )
            }
        });
        let client = test_client(&mock.endpoint, None);
        let body = client
            .get_range("bkt", "obj", &(0..4))
            .expect("range get after 503 retry");
        assert_eq!(body, b"abcd");
        let hits = mock.hits.load(Ordering::SeqCst);
        assert!(hits >= 2, "expected >= 2 requests, got {hits}");
    }

    #[test]
    fn mock_session_token_required() {
        let mock = start_mock(|_n, headers| {
            let has_tok = headers.lines().any(|l| {
                let l = l.to_ascii_lowercase();
                l.starts_with("x-amz-security-token:") && l.contains("sts-session-token")
            });
            if has_tok {
                (200, vec![], b"ok-token".to_vec())
            } else {
                (403, vec![], b"missing token".to_vec())
            }
        });
        let denied = test_client(&mock.endpoint, None);
        let err = denied.get_object("bkt", "obj").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("403"), "{msg}");
        let hits_after_deny = mock.hits.load(Ordering::SeqCst);
        assert_eq!(hits_after_deny, 1, "403 must not be retried");

        let allowed = test_client(&mock.endpoint, Some("sts-session-token"));
        let body = allowed.get_object("bkt", "obj").expect("token accepted");
        assert_eq!(body, b"ok-token");
    }

    #[test]
    fn mock_get_checksum_mismatch() {
        let mock = start_mock(|_n, _headers| {
            (
                200,
                vec![(
                    "x-amz-checksum-sha256".into(),
                    // 32 zero bytes, not the SHA-256 of the body
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into(),
                )],
                b"hello".to_vec(),
            )
        });
        let client = test_client(&mock.endpoint, None);
        let err = client.get_object("bkt", "obj").unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("s3_checksum_mismatch"),
            "expected s3_checksum_mismatch, got {msg}"
        );
    }

    #[test]
    fn mock_does_not_retry_404() {
        let mock = start_mock(|_n, _headers| (404, vec![], b"nope".to_vec()));
        let client = test_client(&mock.endpoint, None);
        let err = client.get_object("bkt", "missing").unwrap_err();
        assert!(format!("{err:#}").contains("404"));
        assert_eq!(mock.hits.load(Ordering::SeqCst), 1);
    }

    fn start_exclusive_put_mock() -> MockS3 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind exclusive mock");
        let endpoint = listener.local_addr().unwrap().to_string();
        let hits = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let store = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
        let hits_c = hits.clone();
        let stop_c = stop.clone();
        let join = std::thread::spawn(move || {
            exclusive_put_loop(listener, store, hits_c, stop_c);
        });
        MockS3 {
            endpoint,
            hits,
            stop,
            join: Some(join),
        }
    }

    fn exclusive_put_loop(
        listener: TcpListener,
        store: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
        hits: Arc<AtomicU64>,
        stop: Arc<AtomicBool>,
    ) {
        for incoming in listener.incoming() {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            let mut stream = match incoming {
                Ok(s) => s,
                Err(_) => break,
            };
            if stop.load(Ordering::SeqCst) {
                break;
            }
            let _ = stream.set_nodelay(true);
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            let header_end = loop {
                match stream.read(&mut tmp) {
                    Ok(0) => break None,
                    Ok(n) => {
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(s) = find_header_end(&buf) {
                            break Some(s);
                        }
                        if buf.len() > 1024 * 1024 {
                            break None;
                        }
                    }
                    Err(_) => break None,
                }
            };
            let Some(sep) = header_end else {
                continue;
            };
            let headers = String::from_utf8_lossy(&buf[..sep]).into_owned();
            let cl = content_length(&headers).unwrap_or(0);
            let mut body = buf[sep + 4..].to_vec();
            while body.len() < cl {
                match stream.read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => body.extend_from_slice(&tmp[..n]),
                    Err(_) => break,
                }
            }
            body.truncate(cl);
            hits.fetch_add(1, Ordering::SeqCst);
            let method = headers
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().next())
                .unwrap_or("");
            let if_none = headers.lines().any(|l| {
                let l = l.to_ascii_lowercase();
                l.starts_with("if-none-match:") && l.contains('*')
            });
            let (status, extra, resp_body): (u16, Vec<(String, String)>, Vec<u8>) = {
                let mut g = store.lock().expect("store");
                if method.eq_ignore_ascii_case("HEAD") {
                    match g.as_ref() {
                        Some(b) => (
                            200,
                            vec![
                                ("Content-Length".to_string(), b.len().to_string()),
                                ("ETag".to_string(), "\"gen1\"".to_string()),
                            ],
                            Vec::new(),
                        ),
                        None => (404, vec![], b"missing".to_vec()),
                    }
                } else if method.eq_ignore_ascii_case("GET") {
                    match g.as_ref() {
                        Some(b) => (200, vec![], b.clone()),
                        None => (404, vec![], b"missing".to_vec()),
                    }
                } else if method.eq_ignore_ascii_case("PUT") && if_none {
                    if g.is_some() {
                        (412, vec![], b"precondition".to_vec())
                    } else {
                        *g = Some(body);
                        (
                            200,
                            vec![("ETag".to_string(), "\"gen1\"".to_string())],
                            Vec::new(),
                        )
                    }
                } else {
                    (400, vec![], b"bad".to_vec())
                }
            };
            let reason = match status {
                200 => "OK",
                412 => "Precondition Failed",
                404 => "Not Found",
                _ => "Error",
            };
            let mut resp = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
                resp_body.len()
            );
            for (k, v) in extra {
                resp.push_str(&format!("{k}: {v}\r\n"));
            }
            resp.push_str("\r\n");
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(&resp_body);
            let _ = stream.flush();
        }
    }

    #[test]
    fn mock_put_if_none_match_second_writer_fails() {
        let mock = start_exclusive_put_mock();
        let client = test_client(&mock.endpoint, None);
        let body_a = br#"{"format_version":1,"fragments":["frag-a"]}"#;
        let body_b = br#"{"format_version":1,"fragments":["frag-b"]}"#;
        client
            .put_object_if_none_match("bkt", "idx/registry.json", body_a)
            .expect("first exclusive put");
        let err = client
            .put_object_if_none_match("bkt", "idx/registry.json", body_b)
            .expect_err("second exclusive put must 412");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("s3_precondition_failed") || msg.contains("412"),
            "got {msg}"
        );
        let got = client
            .get_object("bkt", "idx/registry.json")
            .expect("get winner");
        let v: serde_json::Value = serde_json::from_slice(&got).expect("v1 json");
        assert_eq!(v["format_version"], 1);
        assert_eq!(v["fragments"], serde_json::json!(["frag-a"]));
        assert_eq!(&got, body_a);
    }

    #[test]
    fn mock_put_if_none_match_two_threads_one_wins() {
        let mock = start_exclusive_put_mock();
        let ep = mock.endpoint.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let b1 = barrier.clone();
        let b2 = barrier.clone();
        let ep1 = ep.clone();
        let ep2 = ep;
        let h1 = std::thread::spawn(move || {
            let c = test_client(&ep1, None);
            let body = br#"{"format_version":1,"fragments":["frag-a"]}"#;
            b1.wait();
            c.put_object_if_none_match("bkt", "idx/registry.json", body)
        });
        let h2 = std::thread::spawn(move || {
            let c = test_client(&ep2, None);
            let body = br#"{"format_version":1,"fragments":["frag-b"]}"#;
            b2.wait();
            c.put_object_if_none_match("bkt", "idx/registry.json", body)
        });
        let r1 = h1.join().expect("t1");
        let r2 = h2.join().expect("t2");
        let wins = [r1.is_ok(), r2.is_ok()].into_iter().filter(|w| *w).count();
        let losses = [r1.is_err(), r2.is_err()]
            .into_iter()
            .filter(|w| *w)
            .count();
        assert_eq!(wins, 1, "exactly one publisher must win");
        assert_eq!(losses, 1, "exactly one publisher must error");
        let client = test_client(&mock.endpoint, None);
        let got = client.get_object("bkt", "idx/registry.json").unwrap();
        let v: serde_json::Value = serde_json::from_slice(&got).unwrap();
        assert_eq!(v["format_version"], 1);
        let frags = v["fragments"].as_array().expect("fragments");
        assert_eq!(frags.len(), 1);
        let id = frags[0].as_str().unwrap();
        assert!(id == "frag-a" || id == "frag-b", "got {id}");
    }

    #[test]
    fn publish_registry_s3_writes_valid_v1_json() {
        let mock = start_exclusive_put_mock();
        let client = test_client(&mock.endpoint, None);
        crate::index::publish_registry_s3(
            &client,
            "bkt",
            "idx/registry.json",
            &[String::from("frag-v1")],
        )
        .expect("publish");
        let got = client.get_object("bkt", "idx/registry.json").unwrap();
        let v: serde_json::Value = serde_json::from_slice(&got).unwrap();
        assert_eq!(v["format_version"], 1);
        assert_eq!(v["fragments"], serde_json::json!(["frag-v1"]));
    }
}
