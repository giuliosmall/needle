//! Long-lived HTTP query daemon (`needled`).
//!
//! Loads the RAP index once (or hash-buckets lazily per key), reuses
//! [`MetaCache`](crate::metadata::MetaCache) for the process when the full
//! index is resident, and serves JSON point queries.
//! Distinct from `needle serve`, which is a Range file server.

use crate::index::{
    load_index, load_index_entries_for_keys, load_index_file_dictionary, FileDict,
    IndexFragmentMeta, STALE_FILE_IDENTITY,
};
use crate::query::{ExplainResult, QueryOptions, QueryResult, RapQuerier};
use crate::s3::S3Client;
use anyhow::{bail, Context, Result};
use clap::Parser;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

/// Clap flags shared by `needled` and `needle daemon`.
#[derive(Parser, Debug)]
pub struct DaemonCli {
    /// RAP index root (registry.json + fragments/).
    #[arg(long, default_value = "data/rap-index", env = "NEEDLE_INDEX")]
    pub index: PathBuf,
    /// Listen address. Default 127.0.0.1:7780 is plaintext HTTP for local demos.
    /// Port 0 lets the OS pick an ephemeral port. Non-loopback binds require
    /// `--tls-cert`/`--tls-key` (or `--insecure`).
    #[arg(long, default_value = "127.0.0.1:7780", env = "NEEDLE_BIND")]
    pub bind: String,
    /// Load hash buckets per key instead of the full index at startup (default).
    /// Pass `--full-index` for a full RAM load.
    #[arg(long)]
    pub lazy_buckets: bool,
    /// Load every index bucket into RAM (opt-in; default is lazy buckets).
    #[arg(long)]
    pub full_index: bool,
    /// PEM certificate for TLS (pair with `--tls-key`).
    #[arg(long, value_name = "PATH")]
    pub tls_cert: Option<PathBuf>,
    /// PEM private key for TLS (PKCS#8, or RSA PKCS#1 which is converted).
    #[arg(long, value_name = "PATH")]
    pub tls_key: Option<PathBuf>,
    /// Bearer token for `/v1/query`, `/v1/explain`, `/v1/stats`. Optional on loopback.
    #[arg(long, env = "NEEDLED_TOKEN", hide_env_values = true)]
    pub token: Option<String>,
    /// Allow plaintext and/or no token on a non-loopback bind (prints an INSECURE warning).
    #[arg(long, default_value_t = false)]
    pub insecure: bool,
}

/// Bind / load options for [`start`] and [`serve_forever`].
pub struct DaemonOptions {
    pub index: PathBuf,
    /// Listen address. Default `"127.0.0.1:7780"`. Port `0` is allowed.
    pub bind: String,
    /// If true, keep the file dictionary and load buckets per key; else `load_index` once.
    pub lazy_buckets: bool,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub token: Option<String>,
    pub insecure: bool,
}

impl Default for DaemonOptions {
    fn default() -> Self {
        Self {
            index: PathBuf::from("data/rap-index"),
            bind: "127.0.0.1:7780".into(),
            lazy_buckets: true,
            tls_cert: None,
            tls_key: None,
            token: None,
            insecure: false,
        }
    }
}

impl From<DaemonCli> for DaemonOptions {
    fn from(c: DaemonCli) -> Self {
        Self {
            index: c.index,
            bind: c.bind,
            // Default is lazy buckets; `--full-index` is the opt-in full RAM load.
            lazy_buckets: !c.full_index,
            tls_cert: c.tls_cert,
            tls_key: c.tls_key,
            token: c.token.filter(|s| !s.is_empty()),
            insecure: c.insecure,
        }
    }
}

/// Loopback listen targets: `127.0.0.1`, `::1`, `localhost` (any case), and other
/// [`IpAddr::is_loopback`] addresses. Empty bind uses the loopback default.
pub fn bind_is_loopback(bind: &str) -> bool {
    let bind = bind.trim();
    if bind.is_empty() {
        return true;
    }
    let host = bind_host(bind);
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

/// Host part of `host:port`, `[::1]:port`, or a bare host.
fn bind_host(bind: &str) -> &str {
    let bind = bind.trim();
    if let Some(rest) = bind.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    if let Some((host, port)) = bind.rsplit_once(':') {
        if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) {
            return host;
        }
    }
    bind
}

/// Ok if this bind may listen given TLS certs / `--insecure`. Non-loopback
/// without certs requires `insecure`.
pub fn requires_tls(bind: &str, insecure: bool, has_certs: bool) -> Result<()> {
    if has_certs || bind_is_loopback(bind) || insecure {
        Ok(())
    } else {
        bail!(
            "refusing to listen on {bind} without TLS; pass --tls-cert and --tls-key, or --insecure"
        )
    }
}

/// Warning text when `--insecure` actually relaxes TLS or auth on a non-loopback bind.
pub fn warn_insecure(
    bind: &str,
    insecure: bool,
    has_certs: bool,
    has_token: bool,
) -> Option<String> {
    if !insecure || bind_is_loopback(bind) {
        return None;
    }
    let mut reasons = Vec::new();
    if !has_certs {
        reasons.push("plaintext HTTP (no --tls-cert/--tls-key)");
    }
    if !has_token {
        reasons.push("no bearer token");
    }
    if reasons.is_empty() {
        return None;
    }
    Some(format!(
        "INSECURE: listening on {bind} with {} because --insecure was set. Do not expose this to the network.",
        reasons.join(" and ")
    ))
}

fn has_configured_token(token: Option<&str>) -> bool {
    token.map(|s| !s.is_empty()).unwrap_or(false)
}

fn validate_listen_policy(opts: &DaemonOptions) -> Result<()> {
    let bind = if opts.bind.trim().is_empty() {
        "127.0.0.1:7780"
    } else {
        opts.bind.trim()
    };
    let has_certs = match (&opts.tls_cert, &opts.tls_key) {
        (Some(_), Some(_)) => true,
        (None, None) => false,
        (Some(_), None) => bail!("--tls-cert requires --tls-key"),
        (None, Some(_)) => bail!("--tls-key requires --tls-cert"),
    };
    requires_tls(bind, opts.insecure, has_certs)?;
    let has_token = has_configured_token(opts.token.as_deref());
    if !has_token && !bind_is_loopback(bind) && !opts.insecure {
        bail!(
            "refusing to listen on {bind} without a bearer token; set --token / NEEDLED_TOKEN, or pass --insecure"
        );
    }
    if let Some(msg) = warn_insecure(bind, opts.insecure, has_certs, has_token) {
        eprintln!("{msg}");
    }
    Ok(())
}

pub struct DaemonHandle {
    addr: SocketAddr,
    tls: bool,
    handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl DaemonHandle {
    /// `http(s)://127.0.0.1:PORT` (actual bound address, including ephemeral ports).
    pub fn base_url(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{}://{}", scheme, self.addr)
    }

    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        if self.handle.is_none() {
            return;
        }
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(200));
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Bind (`opts.bind`; port 0 allowed — tiny_http / the OS picks). Returns a handle
/// whose [`DaemonHandle::base_url`] uses the actual bound address.
///
/// If `host:0` is rejected, falls back to `127.0.0.1:(18780 + pid % 1000)`.
pub fn start(opts: DaemonOptions) -> Result<DaemonHandle> {
    let (server, state, addr, tls) = bind_and_load(opts)?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        serve_loop(server, state, stop_c);
    });
    Ok(DaemonHandle {
        addr,
        tls,
        handle: Some(handle),
        stop,
    })
}

/// Bind, print the listen URL, and block until the accept loop ends (Ctrl-C).
pub fn serve_forever(opts: DaemonOptions) -> Result<()> {
    let index_display = opts.index.display().to_string();
    let (server, state, addr, tls) = bind_and_load(opts)?;
    let scheme = if tls { "https" } else { "http" };
    eprintln!(
        "needled listening on {scheme}://{}  index={}  lazy_buckets={}",
        addr, index_display, state.lazy_buckets
    );
    let stop = Arc::new(AtomicBool::new(false));
    serve_loop(server, state, stop);
    Ok(())
}

struct LoadedInner {
    files: FileDict,
    fragments: Vec<IndexFragmentMeta>,
    querier: Option<RapQuerier>,
    registry_mtime: Option<SystemTime>,
}

struct DaemonState {
    index_dir: PathBuf,
    lazy_buckets: bool,
    token: Option<String>,
    inner: Mutex<LoadedInner>,
}

fn registry_mtime(index: &Path) -> Option<SystemTime> {
    fs::metadata(index.join("registry.json"))
        .and_then(|m| m.modified())
        .ok()
}

fn load_inner(opts: &DaemonOptions) -> Result<LoadedInner> {
    let mt = registry_mtime(&opts.index);
    if opts.lazy_buckets {
        let (files, fragments, _root) = load_index_file_dictionary(&opts.index)?;
        Ok(LoadedInner {
            files,
            fragments,
            querier: None,
            registry_mtime: mt,
        })
    } else {
        let index = load_index(&opts.index)?;
        Ok(LoadedInner {
            files: index.files.clone(),
            fragments: index.fragments.clone(),
            querier: Some(RapQuerier::new(index)),
            registry_mtime: mt,
        })
    }
}

fn refresh_if_changed(state: &DaemonState) -> Result<()> {
    let mt = registry_mtime(&state.index_dir);
    let mut inner = state.inner.lock().unwrap();
    if inner.registry_mtime == mt {
        return Ok(());
    }
    *inner = load_inner(&DaemonOptions {
        index: state.index_dir.clone(),
        lazy_buckets: state.lazy_buckets,
        ..Default::default()
    })?;
    Ok(())
}

fn bind_and_load(
    opts: DaemonOptions,
) -> Result<(tiny_http::Server, Arc<DaemonState>, SocketAddr, bool)> {
    validate_listen_policy(&opts)?;
    let inner = load_inner(&opts)?;
    let token = opts
        .token
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let ssl = match (&opts.tls_cert, &opts.tls_key) {
        (Some(cert), Some(key)) => Some(load_tls_config(cert, key)?),
        _ => None,
    };
    let tls = ssl.is_some();
    let state = Arc::new(DaemonState {
        index_dir: opts.index.clone(),
        lazy_buckets: opts.lazy_buckets,
        token,
        inner: Mutex::new(inner),
    });
    let bind = if opts.bind.trim().is_empty() {
        "127.0.0.1:7780".to_string()
    } else {
        opts.bind.trim().to_string()
    };
    let server = match open_server(&bind, ssl.as_ref()) {
        Ok(s) => s,
        Err(e) if bind_requests_ephemeral(&bind) => {
            let port = 18780 + (std::process::id() % 1000);
            let fallback = format!("127.0.0.1:{port}");
            open_server(&fallback, ssl.as_ref()).with_context(|| {
                format!("fallback bind {fallback} after ephemeral bind {bind} failed: {e:#}")
            })?
        }
        Err(e) => return Err(e),
    };
    let addr = server
        .server_addr()
        .to_ip()
        .ok_or_else(|| anyhow::anyhow!("needled requires an IP bind address"))?;
    Ok((server, state, addr, tls))
}

fn bind_requests_ephemeral(bind: &str) -> bool {
    bind.rsplit_once(':')
        .map(|(_, p)| p == "0")
        .unwrap_or(false)
}

fn open_server(bind: &str, ssl: Option<&tiny_http::SslConfig>) -> Result<tiny_http::Server> {
    match ssl {
        None => tiny_http::Server::http(bind).map_err(|e| anyhow::anyhow!("bind {bind}: {e}")),
        Some(cfg) => tiny_http::Server::https(
            bind,
            tiny_http::SslConfig {
                certificate: cfg.certificate.clone(),
                private_key: cfg.private_key.clone(),
            },
        )
        .map_err(|e| anyhow::anyhow!("bind {bind} tls: {e}")),
    }
}

fn load_tls_config(cert: &Path, key: &Path) -> Result<tiny_http::SslConfig> {
    let certificate =
        fs::read(cert).with_context(|| format!("read tls cert {}", cert.display()))?;
    let key_raw = fs::read(key).with_context(|| format!("read tls key {}", key.display()))?;
    let private_key = pem_key_to_pkcs8(&key_raw)?;
    native_tls::Identity::from_pkcs8(&certificate, &private_key)
        .map_err(|e| anyhow::anyhow!("tls identity (PKCS#8 PEM): {e}"))?;
    Ok(tiny_http::SslConfig {
        certificate,
        private_key,
    })
}

fn pem_key_to_pkcs8(key_pem: &[u8]) -> Result<Vec<u8>> {
    let text = std::str::from_utf8(key_pem).context("tls key is not UTF-8 PEM")?;
    if pem_has_label(text, "ENCRYPTED PRIVATE KEY") {
        bail!("encrypted TLS keys are not supported; use an unencrypted PKCS#8 PEM");
    }
    if pem_has_label(text, "PRIVATE KEY") {
        let block = extract_pem(text, "PRIVATE KEY")?;
        let mut out = block.as_bytes().to_vec();
        if !out.ends_with(b"\n") {
            out.push(b'\n');
        }
        return Ok(out);
    }
    if pem_has_label(text, "RSA PRIVATE KEY") {
        let der = pem_body_der(text, "RSA PRIVATE KEY")?;
        let pkcs8 = wrap_rsa_pkcs1_in_pkcs8(&der);
        return Ok(pem_encode("PRIVATE KEY", &pkcs8));
    }
    bail!("tls key must be PKCS#8 PEM (BEGIN PRIVATE KEY) or RSA PKCS#1 PEM")
}

fn pem_has_label(text: &str, label: &str) -> bool {
    text.contains(&format!("BEGIN {label}"))
}

fn extract_pem<'a>(text: &'a str, label: &str) -> Result<&'a str> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let start = text
        .find(&begin)
        .with_context(|| format!("PEM missing {begin}"))?;
    let after = &text[start..];
    let end_at = after
        .find(&end)
        .with_context(|| format!("PEM missing {end}"))?;
    Ok(&after[..end_at + end.len()])
}

fn pem_body_der(text: &str, label: &str) -> Result<Vec<u8>> {
    let block = extract_pem(text, label)?;
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let inner = block
        .strip_prefix(&begin)
        .and_then(|s| s.strip_suffix(&end))
        .unwrap_or(block);
    let b64: String = inner.chars().filter(|c| !c.is_whitespace()).collect();
    b64_decode(&b64)
}

fn pem_encode(label: &str, der: &[u8]) -> Vec<u8> {
    let b64 = b64_encode(der);
    let mut s = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        s.push_str(std::str::from_utf8(chunk).unwrap());
        s.push('\n');
    }
    s.push_str("-----END ");
    s.push_str(label);
    s.push_str("-----\n");
    s.into_bytes()
}

fn wrap_rsa_pkcs1_in_pkcs8(pkcs1: &[u8]) -> Vec<u8> {
    // AlgorithmIdentifier rsaEncryption: 1.2.840.113549.1.1.1
    const ALG: &[u8] = &[
        0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00,
    ];
    let mut inner = Vec::with_capacity(3 + ALG.len() + 4 + pkcs1.len());
    inner.extend_from_slice(&[0x02, 0x01, 0x00]);
    inner.extend_from_slice(ALG);
    inner.push(0x04);
    extend_der_len(&mut inner, pkcs1.len());
    inner.extend_from_slice(pkcs1);
    let mut out = Vec::with_capacity(4 + inner.len());
    out.push(0x30);
    extend_der_len(&mut out, inner.len());
    out.extend_from_slice(&inner);
    out
}

fn extend_der_len(out: &mut Vec<u8>, len: usize) {
    if len < 0x80 {
        out.push(len as u8);
    } else if len <= 0xff {
        out.push(0x81);
        out.push(len as u8);
    } else if len <= 0xffff {
        out.push(0x82);
        out.push((len >> 8) as u8);
        out.push((len & 0xff) as u8);
    } else {
        out.push(0x83);
        out.push((len >> 16) as u8);
        out.push((len >> 8) as u8);
        out.push((len & 0xff) as u8);
    }
}

fn b64_encode(data: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    let mut i = 0;
    while i + 3 <= data.len() {
        let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8) | (data[i + 2] as u32);
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        out.push(T[((n >> 6) & 63) as usize] as char);
        out.push(T[(n & 63) as usize] as char);
        i += 3;
    }
    match data.len() - i {
        1 => {
            let n = (data[i] as u32) << 16;
            out.push(T[((n >> 18) & 63) as usize] as char);
            out.push(T[((n >> 12) & 63) as usize] as char);
            out.push('=');
            out.push('=');
        }
        2 => {
            let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8);
            out.push(T[((n >> 18) & 63) as usize] as char);
            out.push(T[((n >> 12) & 63) as usize] as char);
            out.push(T[((n >> 6) & 63) as usize] as char);
            out.push('=');
        }
        _ => {}
    }
    out
}

fn b64_decode(s: &str) -> Result<Vec<u8>> {
    fn val(c: u8) -> Result<u8> {
        Ok(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => bail!("invalid base64 in PEM"),
        })
    }
    let bytes: Vec<u8> = s.bytes().filter(|c| *c != b'=').collect();
    if bytes.len() % 4 == 1 {
        bail!("invalid base64 length in PEM");
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let n = ((val(bytes[i])? as u32) << 18)
            | ((val(bytes[i + 1])? as u32) << 12)
            | ((val(bytes[i + 2])? as u32) << 6)
            | (val(bytes[i + 3])? as u32);
        out.push((n >> 16) as u8);
        out.push((n >> 8) as u8);
        out.push(n as u8);
        i += 4;
    }
    match bytes.len() - i {
        0 => {}
        2 => {
            let n = ((val(bytes[i])? as u32) << 18) | ((val(bytes[i + 1])? as u32) << 12);
            out.push((n >> 16) as u8);
        }
        3 => {
            let n = ((val(bytes[i])? as u32) << 18)
                | ((val(bytes[i + 1])? as u32) << 12)
                | ((val(bytes[i + 2])? as u32) << 6);
            out.push((n >> 16) as u8);
            out.push((n >> 8) as u8);
        }
        _ => bail!("invalid base64 length in PEM"),
    }
    Ok(out)
}

fn serve_loop(server: tiny_http::Server, state: Arc<DaemonState>, stop: Arc<AtomicBool>) {
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let request = match server.recv_timeout(Duration::from_millis(200)) {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(_) => break,
        };
        if stop.load(Ordering::SeqCst) {
            let _ = request.respond(tiny_http::Response::empty(503));
            break;
        }
        handle_one(request, &state);
    }
}

/// Frozen HTTP error codes (`HTTP.md`). Additive codes are allowed; these names stay.
pub const ERR_UNAUTHENTICATED: &str = "unauthenticated";
pub const ERR_BAD_REQUEST: &str = "bad_request";
pub const ERR_NOT_FOUND: &str = "not_found";
pub const ERR_METHOD_NOT_ALLOWED: &str = "method_not_allowed";
pub const ERR_STALE_FILE_IDENTITY: &str = STALE_FILE_IDENTITY;
pub const ERR_ICEBERG_UNSUPPORTED_DELETES: &str = "iceberg_unsupported_deletes";
pub const ERR_CATALOG_ERROR: &str = "catalog_error";
pub const ERR_S3_IDENTITY_MISMATCH: &str = "s3_identity_mismatch";
pub const ERR_S3_CHECKSUM_MISMATCH: &str = "s3_checksum_mismatch";
pub const ERR_INTERNAL: &str = "internal";

/// Frozen top-level keys on a successful `/v1/query` body. Additive keys allowed.
pub const QUERY_JSON_KEYS: &[&str] = &[
    "key",
    "rows",
    "covering",
    "covering_values",
    "timings",
    "totals",
];
pub const QUERY_TIMINGS_KEYS: &[&str] = &[
    "index_lookup_ms",
    "metadata_resolve_ms",
    "ranged_read_ms",
    "decode_extract_ms",
    "total_ms",
];
pub const QUERY_TOTALS_KEYS: &[&str] = &[
    "rows",
    "value_count",
    "bytes_ranged",
    "pages_touched",
    "files_touched",
    "skipped_by_predicate",
    "offset",
    "limit",
];
pub const COVERING_VALUES_KEYS: &[&str] = &[
    "file",
    "value_count",
    "listen_count",
    "total_duration_ms",
    "min_ts",
    "max_ts",
];
pub const EXPLAIN_JSON_KEYS: &[&str] = &[
    "key",
    "bucket",
    "num_entries",
    "num_entries_after_predicates",
    "files",
    "covering",
    "page_descriptions",
    "estimated_bytes",
    "estimated_range_gets",
    "covering_only",
    "columns",
    "since_ms",
    "until_ms",
    "skipped_by_predicate",
];
pub const STATS_JSON_KEYS: &[&str] = &[
    "index",
    "lazy_buckets",
    "num_files",
    "num_fragments",
    "fragments",
];

fn error_body(code: &str, message: impl AsRef<str>) -> Value {
    json!({"error": code, "message": message.as_ref()})
}

fn error_body_with(code: &str, message: impl AsRef<str>, extra: &Value) -> Value {
    let mut body = error_body(code, message);
    if let (Some(map), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
        for (k, v) in extra {
            if k != "error" && k != "message" {
                map.insert(k.clone(), v.clone());
            }
        }
    }
    body
}

fn extract_json_object(s: &str) -> Option<Value> {
    let start = s.find('{')?;
    let end = s.rfind('}')?;
    if end <= start {
        return None;
    }
    serde_json::from_str(&s[start..=end])
        .ok()
        .filter(Value::is_object)
}

fn looks_like_catalog_error(lower: &str) -> bool {
    if lower.contains(ERR_CATALOG_ERROR) {
        return true;
    }
    if lower.contains("no current snapshot")
        || lower.contains("has no metadata/")
        || lower.contains("no *.metadata.json")
        || lower.contains("*.metadata.json")
    {
        return true;
    }
    lower.contains("iceberg")
        && (lower.contains("metadata")
            || lower.contains("snapshot")
            || lower.contains("manifest-list")
            || lower.contains("manifest list"))
}

/// Map a query/explain/stats failure to a frozen `{error, message, …}` body.
pub fn map_anyhow_error(err: &anyhow::Error) -> (u16, Value) {
    map_error_message(&format!("{err:#}"))
}

fn map_error_message(msg: &str) -> (u16, Value) {
    if let Some(obj) = extract_json_object(msg) {
        if let Some(code) = obj.get("error").and_then(|v| v.as_str()) {
            let remote = obj
                .get("path")
                .and_then(|p| p.as_str())
                .is_some_and(S3Client::is_remote_uri);
            let mapped = if code == STALE_FILE_IDENTITY && remote {
                ERR_S3_IDENTITY_MISMATCH
            } else if code == STALE_FILE_IDENTITY {
                ERR_STALE_FILE_IDENTITY
            } else if code == ERR_S3_IDENTITY_MISMATCH {
                ERR_S3_IDENTITY_MISMATCH
            } else if code == ERR_S3_CHECKSUM_MISMATCH {
                ERR_S3_CHECKSUM_MISMATCH
            } else if code == ERR_ICEBERG_UNSUPPORTED_DELETES {
                ERR_ICEBERG_UNSUPPORTED_DELETES
            } else if code == ERR_CATALOG_ERROR {
                ERR_CATALOG_ERROR
            } else {
                ""
            };
            if !mapped.is_empty() {
                let human = obj
                    .get("message")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| match mapped {
                        ERR_S3_IDENTITY_MISMATCH => {
                            "S3 object identity no longer matches the index".into()
                        }
                        ERR_STALE_FILE_IDENTITY => {
                            "indexed file no longer matches live identity".into()
                        }
                        _ => msg.to_string(),
                    });
                return (500, error_body_with(mapped, human, &obj));
            }
        }
    }
    let lower = msg.to_ascii_lowercase();
    if lower.contains("unsupported delete") || lower.contains(ERR_ICEBERG_UNSUPPORTED_DELETES) {
        return (500, error_body(ERR_ICEBERG_UNSUPPORTED_DELETES, msg));
    }
    if lower.contains(ERR_S3_CHECKSUM_MISMATCH) || lower.contains("checksum mismatch") {
        return (500, error_body(ERR_S3_CHECKSUM_MISMATCH, msg));
    }
    if lower.contains(ERR_S3_IDENTITY_MISMATCH) {
        return (500, error_body(ERR_S3_IDENTITY_MISMATCH, msg));
    }
    if lower.contains(STALE_FILE_IDENTITY) {
        let code = if lower.contains("s3://")
            || lower.contains("s3a://")
            || lower.contains("https://")
            || lower.contains("http://")
        {
            ERR_S3_IDENTITY_MISMATCH
        } else {
            ERR_STALE_FILE_IDENTITY
        };
        return (500, error_body(code, msg));
    }
    if looks_like_catalog_error(&lower) {
        return (500, error_body(ERR_CATALOG_ERROR, msg));
    }
    (500, error_body(ERR_INTERNAL, msg))
}

fn handle_one(request: tiny_http::Request, state: &DaemonState) {
    let _ = refresh_if_changed(state);
    if request.method() != &tiny_http::Method::Get {
        respond_json(
            request,
            405,
            error_body(ERR_METHOD_NOT_ALLOWED, "method not allowed"),
        );
        return;
    }
    let url = request.url().to_string();
    let (path, params) = parse_url(&url);
    if let Some(err) = authorize(&request, &path, state.token.as_deref()) {
        respond_json(request, 401, error_body(ERR_UNAUTHENTICATED, err));
        return;
    }
    let outcome = match path.as_str() {
        "/health" => Ok((200, json!({"ok": true}))),
        "/v1/query" => handle_query(&params, state),
        "/v1/explain" => handle_explain(&params, state),
        "/v1/stats" => Ok((200, stats_json(state))),
        _ => Ok((404, error_body(ERR_NOT_FOUND, "not found"))),
    };
    match outcome {
        Ok((code, body)) => respond_json(request, code, body),
        Err(e) => {
            let (code, body) = map_anyhow_error(&e);
            respond_json(request, code, body);
        }
    }
}

fn is_data_path(path: &str) -> bool {
    matches!(path, "/v1/query" | "/v1/explain" | "/v1/stats")
}

/// Returns an error message (no index paths, keys, or fragment ids) when auth fails.
fn authorize(
    request: &tiny_http::Request,
    path: &str,
    token: Option<&str>,
) -> Option<&'static str> {
    let Some(expected) = token.filter(|s| !s.is_empty()) else {
        return None;
    };
    if !is_data_path(path) {
        return None;
    }
    match bearer_token(request) {
        Some(got) if token_eq(expected, &got) => None,
        Some(_) => Some("invalid token"),
        None => Some("unauthorized"),
    }
}

fn bearer_token(request: &tiny_http::Request) -> Option<String> {
    for h in request.headers() {
        if !h.field.equiv("Authorization") {
            continue;
        }
        let v = h.value.as_str().trim();
        if v.len() >= 7 && v[..7].eq_ignore_ascii_case("Bearer ") {
            return Some(v[7..].trim().to_string());
        }
    }
    None
}

fn token_eq(expected: &str, got: &str) -> bool {
    let a = expected.as_bytes();
    let b = got.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn handle_query(params: &HashMap<String, String>, state: &DaemonState) -> Result<(u16, Value)> {
    let Some(key) = params.get("key").filter(|s| !s.is_empty()) else {
        return Ok((400, error_body(ERR_BAD_REQUEST, "missing key")));
    };
    let opts = match query_options_from_params(params) {
        Ok(o) => o,
        Err(msg) => return Ok((400, error_body(ERR_BAD_REQUEST, msg))),
    };
    let result = with_querier(state, key, |q| q.query_with(key, &opts))?;
    Ok((200, query_result_json(&result)))
}

fn handle_explain(params: &HashMap<String, String>, state: &DaemonState) -> Result<(u16, Value)> {
    let Some(key) = params.get("key").filter(|s| !s.is_empty()) else {
        return Ok((400, error_body(ERR_BAD_REQUEST, "missing key")));
    };
    let opts = match query_options_from_params(params) {
        Ok(o) => o,
        Err(msg) => return Ok((400, error_body(ERR_BAD_REQUEST, msg))),
    };
    let result = with_querier(state, key, |q| q.explain(key, &opts))?;
    Ok((200, explain_result_json(&result)))
}

fn with_querier<T>(
    state: &DaemonState,
    key: &str,
    f: impl FnOnce(&RapQuerier) -> Result<T>,
) -> Result<T> {
    let inner = state.inner.lock().unwrap();
    if let Some(q) = &inner.querier {
        f(q)
    } else {
        let index = load_index_entries_for_keys(
            &state.index_dir,
            inner.files.clone(),
            &inner.fragments,
            &[key.to_string()],
        )?;
        drop(inner);
        let q = RapQuerier::new(index);
        f(&q)
    }
}

fn stats_json(state: &DaemonState) -> Value {
    let inner = state.inner.lock().unwrap();
    let fragments: Vec<Value> = inner
        .fragments
        .iter()
        .map(|m| {
            json!({
                "fragment_id": m.fragment_id,
                "num_buckets": m.num_buckets,
                "num_files": m.file_count.unwrap_or(m.files.len() as u32),
                "created_at": m.created_at,
                "key_columns": m.key_columns,
                "value_columns": m.value_columns,
            })
        })
        .collect();
    let mut body = json!({
        "index": state.index_dir.display().to_string(),
        "lazy_buckets": state.lazy_buckets,
        "num_files": inner.files.len(),
        "num_fragments": inner.fragments.len(),
        "fragments": fragments,
    });
    if let Some(q) = &inner.querier {
        body["num_keys"] = json!(q.index.num_keys());
        body["num_entries"] = json!(q.index.num_entries());
        body["fragment_id"] = json!(q.index.fragments.first().map(|f| f.fragment_id.as_str()));
        body["num_buckets"] = json!(q
            .index
            .fragments
            .first()
            .map(|f| f.num_buckets)
            .unwrap_or(0));
    } else if let Some(m) = inner.fragments.first() {
        body["fragment_id"] = json!(m.fragment_id);
        body["num_buckets"] = json!(m.num_buckets);
    }
    body
}

fn query_result_json(result: &QueryResult) -> Value {
    let t = &result.timings;
    json!({
        "key": result.key,
        "rows": result.json_rows(),
        "covering": result.covering_hits,
        "covering_values": result.covering_values_json(),
        "timings": {
            "index_lookup_ms": t.index_lookup.as_millis() as u64,
            "metadata_resolve_ms": t.metadata_resolve.as_millis() as u64,
            "ranged_read_ms": t.ranged_read_demo.as_millis() as u64,
            "decode_extract_ms": t.decode_extract.as_millis() as u64,
            "total_ms": t.total.as_millis() as u64,
        },
        "totals": {
            "rows": result.rows.len(),
            "value_count": result.total_value_count,
            "bytes_ranged": t.bytes_ranged,
            "pages_touched": t.pages_touched,
            "files_touched": t.files_touched,
            "skipped_by_predicate": result.skipped_by_predicate,
            "offset": result.offset,
            "limit": result.limit,
        },
    })
}

fn explain_result_json(e: &ExplainResult) -> Value {
    json!({
        "key": e.key,
        "bucket": e.bucket,
        "num_entries": e.num_entries,
        "num_entries_after_predicates": e.num_entries_after_predicates,
        "files": e.files,
        "covering": e.covering,
        "page_descriptions": e.page_descriptions,
        "estimated_bytes": e.estimated_bytes,
        "estimated_range_gets": e.estimated_range_gets,
        "covering_only": e.covering_only,
        "columns": e.columns,
        "since_ms": e.since_ms,
        "until_ms": e.until_ms,
        "skipped_by_predicate": e.skipped_by_predicate,
    })
}

fn query_options_from_params(params: &HashMap<String, String>) -> Result<QueryOptions, String> {
    Ok(QueryOptions {
        offset: parse_opt(params, "offset")?.unwrap_or(0),
        limit: parse_opt(params, "limit")?,
        columns: parse_columns(params.get("columns")),
        since_ms: parse_opt(params, "since_ms")?,
        until_ms: parse_opt(params, "until_ms")?,
        covering_only: params
            .get("covering_only")
            .map(|s| parse_bool(s))
            .unwrap_or(false),
        min_listen_count: parse_opt(params, "min_listens")?,
        verify: params.get("verify").map(|s| parse_bool(s)).unwrap_or(true),
        ..Default::default()
    })
}

fn parse_columns(raw: Option<&String>) -> Option<Vec<String>> {
    let s = raw?;
    let cols: Vec<String> = s
        .split(',')
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(str::to_string)
        .collect();
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}

fn parse_opt<T: std::str::FromStr>(
    params: &HashMap<String, String>,
    name: &str,
) -> Result<Option<T>, String> {
    match params.get(name) {
        None => Ok(None),
        Some(s) if s.is_empty() => Ok(None),
        Some(s) => s
            .parse::<T>()
            .map(Some)
            .map_err(|_| format!("invalid {name}")),
    }
}

fn parse_bool(s: &str) -> bool {
    matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

fn parse_url(url: &str) -> (String, HashMap<String, String>) {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .and_then(|u| u.find('/').map(|i| &u[i..]))
        .unwrap_or(url);
    let (path_raw, query) = rest.split_once('?').unwrap_or((rest, ""));
    let path_raw = path_raw.split('#').next().unwrap_or(path_raw);
    let mut path = if path_raw.is_empty() {
        "/".to_string()
    } else {
        path_raw.to_string()
    };
    if path.len() > 1 {
        while path.ends_with('/') {
            path.pop();
        }
    }
    let mut params = HashMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        params.insert(percent_decode(k), percent_decode(v));
    }
    (path, params)
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                if let Some(v) = from_hex(bytes[i + 1], bytes[i + 2]) {
                    out.push(v);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn from_hex(a: u8, b: u8) -> Option<u8> {
    fn nibble(c: u8) -> Option<u8> {
        Some(match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => return None,
        })
    }
    Some((nibble(a)? << 4) | nibble(b)?)
}

fn json_header(name: &[u8], value: &[u8]) -> tiny_http::Header {
    tiny_http::Header::from_bytes(name, value).expect("static header")
}

fn respond_json(request: tiny_http::Request, status: u16, body: Value) {
    let payload = serde_json::to_string(&body)
        .unwrap_or_else(|_| "{\"error\":\"internal\",\"message\":\"serialize\"}".into());
    let response = tiny_http::Response::from_string(payload)
        .with_status_code(status)
        .with_header(json_header(b"Content-Type", b"application/json"))
        .with_header(json_header(b"Connection", b"close"));
    let _ = request.respond(response);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{
        forget_keys, load_index, stale_file_identity_error, FileIdent, IndexBuilder,
    };
    use crate::writer::{write_sample_dataset, WriteMode, WriterOptions};
    use clap::Parser;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::path::Path;
    use std::process::Command;

    fn tiny_dataset(tmp: &Path) -> PathBuf {
        let data = tmp.join("parquet");
        let idx = tmp.join("rap-index");
        let opts = WriterOptions {
            out_dir: data,
            num_users: 8,
            listens_per_user: 3,
            num_files: 1,
            mode: WriteMode::Sorted,
            rows_per_row_group: 32,
            write_page_index: true,
            seed: 123,
            one_page_per_key: false,
        };
        let paths = write_sample_dataset(&opts).unwrap();
        IndexBuilder::new(&idx, 8)
            .with_covering(true)
            .build_fragment(&paths, "frag-d", None)
            .unwrap();
        idx
    }

    /// Raw HTTP/1.1 GET over `TcpStream` (no reqwest), same style as `storage::HttpRange`.
    fn http_get(url: &str) -> Result<(u16, String)> {
        http_get_auth(url, None)
    }

    fn http_get_auth(url: &str, bearer: Option<&str>) -> Result<(u16, String)> {
        let (tls, bare) = if let Some(b) = url.strip_prefix("https://") {
            (true, b)
        } else if let Some(b) = url.strip_prefix("http://") {
            (false, b)
        } else {
            bail!("url must be http:// or https://");
        };
        let (hostport, path) = match bare.split_once('/') {
            Some((h, p)) => (h, format!("/{p}")),
            None => (bare, "/".to_string()),
        };
        let auth = match bearer {
            Some(t) => format!("Authorization: Bearer {t}\r\n"),
            None => String::new(),
        };
        let req =
            format!("GET {path} HTTP/1.1\r\nHost: {hostport}\r\n{auth}Connection: close\r\n\r\n");
        let mut last_err = None;
        for _ in 0..25 {
            match http_get_once(hostport, &req, tls) {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("http get {url} failed")))
    }

    fn http_get_once(hostport: &str, req: &str, tls: bool) -> Result<(u16, String)> {
        let tcp = TcpStream::connect(hostport).with_context(|| format!("connect {hostport}"))?;
        tcp.set_read_timeout(Some(Duration::from_secs(10)))?;
        tcp.set_write_timeout(Some(Duration::from_secs(10)))?;
        if tls {
            let connector = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .context("tls connector")?;
            let stream = connector
                .connect("localhost", tcp)
                .map_err(|e| anyhow::anyhow!("tls handshake: {e}"))?;
            read_http_exchange(stream, req)
        } else {
            read_http_exchange(tcp, req)
        }
    }

    fn read_http_exchange(mut stream: impl Read + Write, req: &str) -> Result<(u16, String)> {
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
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = String::from_utf8_lossy(&resp[sep + 4..]).into_owned();
        Ok((status, body))
    }

    fn assert_401_no_secrets(body: &str, idx: &Path, token: &str) {
        let v: Value = serde_json::from_str(body).expect("401 json");
        assert_eq!(v["error"], json!(ERR_UNAUTHENTICATED), "401 body={body}");
        let msg = v["message"].as_str().expect("401 message");
        assert!(
            msg == "unauthorized" || msg == "invalid token",
            "401 message={msg}"
        );
        if let Some(p) = idx.to_str() {
            assert!(!body.contains(p), "401 must not leak index path: {body}");
        }
        assert!(!body.contains(token), "401 must not leak token: {body}");
        assert!(
            !body.to_ascii_lowercase().contains("fragment"),
            "401 must not leak fragment ids: {body}"
        );
    }

    fn assert_object_has_keys(v: &Value, keys: &[&str], label: &str) {
        let obj = v
            .as_object()
            .unwrap_or_else(|| panic!("{label} object: {v}"));
        for k in keys {
            assert!(obj.contains_key(*k), "{label} missing frozen key {k}: {v}");
        }
    }

    fn assert_query_json_frozen(v: &Value) {
        assert_object_has_keys(v, QUERY_JSON_KEYS, "query");
        assert_object_has_keys(&v["timings"], QUERY_TIMINGS_KEYS, "timings");
        assert_object_has_keys(&v["totals"], QUERY_TOTALS_KEYS, "totals");
        assert!(v["rows"].is_array(), "rows array: {v}");
        assert!(v["covering"].is_array(), "covering array: {v}");
        let covering = v["covering_values"].as_array().expect("covering_values");
        for item in covering {
            assert_object_has_keys(item, COVERING_VALUES_KEYS, "covering_values[]");
        }
    }

    #[test]
    fn health_query_and_stop() {
        let tmp = tempfile::tempdir().unwrap();
        let idx = tiny_dataset(tmp.path());
        let handle = start(DaemonOptions {
            index: idx,
            bind: "127.0.0.1:0".into(),
            lazy_buckets: false,
            ..Default::default()
        })
        .expect("start needled");
        let base = handle.base_url();
        assert!(
            base.starts_with("http://127.0.0.1:"),
            "ephemeral bind url: {base}"
        );
        assert_ne!(base, "http://127.0.0.1:0");

        let (st, body) = http_get(&format!("{base}/health")).expect("GET /health");
        assert_eq!(st, 200, "health status, body={body}");
        let v: Value = serde_json::from_str(&body).expect("health json");
        assert_eq!(v["ok"], json!(true));

        let (st, body) = http_get(&format!("{base}/v1/query?key=user_0000")).expect("GET query");
        assert_eq!(st, 200, "query status, body={body}");
        let q: Value = serde_json::from_str(&body).expect("query json");
        assert_eq!(q["key"], json!("user_0000"));
        assert_query_json_frozen(&q);
        let rows = q["rows"].as_array().expect("rows array");
        assert!(!rows.is_empty(), "expected rows for user_0000, body={body}");
        for r in rows {
            assert!(r.is_object(), "row must be a JSON object, got {r}");
        }

        let (st, body) = http_get(&format!("{base}/v1/query")).expect("missing key");
        assert_eq!(st, 400, "missing key, body={body}");
        let err: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(err["error"], json!(ERR_BAD_REQUEST));
        assert_eq!(err["message"], json!("missing key"));

        let (st, body) = http_get(&format!("{base}/no-such-route")).expect("404");
        assert_eq!(st, 404, "body={body}");
        let err: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(err["error"], json!(ERR_NOT_FOUND));

        let (st, body) = http_get(&format!("{base}/v1/explain?key=user_0000")).expect("explain");
        assert_eq!(st, 200, "explain status, body={body}");
        let expl: Value = serde_json::from_str(&body).unwrap();
        assert_object_has_keys(&expl, EXPLAIN_JSON_KEYS, "explain");

        let (st, body) = http_get(&format!("{base}/v1/stats")).expect("stats");
        assert_eq!(st, 200, "stats status, body={body}");
        let stats: Value = serde_json::from_str(&body).unwrap();
        assert_object_has_keys(&stats, STATS_JSON_KEYS, "stats");

        handle.stop();
    }

    #[test]
    fn daemon_reloads_after_forget() {
        let tmp = tempfile::tempdir().unwrap();
        let idx = tiny_dataset(tmp.path());
        let handle = start(DaemonOptions {
            index: idx.clone(),
            bind: "127.0.0.1:0".into(),
            lazy_buckets: false,
            ..Default::default()
        })
        .expect("start needled");
        let base = handle.base_url();
        let (st, body) = http_get(&format!("{base}/v1/query?key=user_0000")).unwrap();
        assert_eq!(st, 200, "{body}");
        let q: Value = serde_json::from_str(&body).unwrap();
        assert!(!q["rows"].as_array().unwrap().is_empty());

        forget_keys(&idx, &[String::from("user_0000")], Some("forget-d")).unwrap();
        let (st, body) = http_get(&format!("{base}/v1/query?key=user_0000")).unwrap();
        assert_eq!(st, 200, "{body}");
        let q: Value = serde_json::from_str(&body).unwrap();
        assert!(
            q["rows"].as_array().unwrap().is_empty(),
            "needled must reload after forget, body={body}"
        );

        handle.stop();
    }

    #[test]
    fn bind_is_loopback_examples() {
        assert!(bind_is_loopback("127.0.0.1:0"));
        assert!(bind_is_loopback("127.0.0.1:7780"));
        assert!(bind_is_loopback("localhost:7780"));
        assert!(bind_is_loopback("LOCALHOST:1"));
        assert!(bind_is_loopback("[::1]:7780"));
        assert!(bind_is_loopback("::1:7780"));
        assert!(bind_is_loopback(""));
        assert!(!bind_is_loopback("0.0.0.0:7780"));
        assert!(!bind_is_loopback("192.0.2.1:7780"));
        assert!(!bind_is_loopback("[::]:7780"));
    }

    #[test]
    fn requires_tls_policy() {
        assert!(requires_tls("127.0.0.1:0", false, false).is_ok());
        assert!(requires_tls("localhost:7780", false, false).is_ok());
        assert!(requires_tls("0.0.0.0:7780", false, false).is_err());
        assert!(requires_tls("0.0.0.0:7780", true, false).is_ok());
        assert!(requires_tls("0.0.0.0:7780", false, true).is_ok());
        assert!(requires_tls("192.0.2.10:7780", false, false).is_err());
        assert!(requires_tls("192.0.2.10:7780", true, false).is_ok());
    }

    #[test]
    fn warn_insecure_contains_insecure() {
        let msg = warn_insecure("0.0.0.0:7780", true, false, true).expect("warn");
        assert!(msg.contains("INSECURE"), "{msg}");
        let msg = warn_insecure("0.0.0.0:7780", true, true, false).expect("warn");
        assert!(msg.contains("INSECURE"), "{msg}");
        assert!(warn_insecure("127.0.0.1:7780", true, false, false).is_none());
        assert!(warn_insecure("0.0.0.0:7780", false, false, false).is_none());
        assert!(warn_insecure("0.0.0.0:7780", true, true, true).is_none());
    }

    #[test]
    fn start_refuses_non_loopback_without_tls_or_token() {
        let err = match start(DaemonOptions {
            index: PathBuf::from("/nonexistent-needle-index"),
            bind: "0.0.0.0:7780".into(),
            ..Default::default()
        }) {
            Ok(_) => panic!("expected non-loopback bind without TLS to fail"),
            Err(e) => e,
        };
        let s = format!("{err:#}");
        assert!(
            s.contains("TLS") || s.contains("tls") || s.contains("insecure"),
            "{s}"
        );
    }

    #[test]
    fn daemon_cli_parses_tls_token_insecure() {
        let c = DaemonCli::try_parse_from([
            "needled",
            "--index",
            "/tmp/idx",
            "--bind",
            "0.0.0.0:7780",
            "--tls-cert",
            "/tmp/c.pem",
            "--tls-key",
            "/tmp/k.pem",
            "--token",
            "s3cret",
            "--insecure",
            "--lazy-buckets",
        ])
        .unwrap();
        assert_eq!(c.bind, "0.0.0.0:7780");
        assert_eq!(c.tls_cert.as_deref(), Some(Path::new("/tmp/c.pem")));
        assert_eq!(c.tls_key.as_deref(), Some(Path::new("/tmp/k.pem")));
        assert_eq!(c.token.as_deref(), Some("s3cret"));
        assert!(c.insecure);
        assert!(c.lazy_buckets);
        let opts = DaemonOptions::from(c);
        assert_eq!(opts.token.as_deref(), Some("s3cret"));
        assert!(opts.lazy_buckets);
    }

    #[test]
    fn daemon_cli_defaults_lazy_full_index_opt_in() {
        let c = DaemonCli::try_parse_from(["needled"]).unwrap();
        assert!(!c.full_index);
        let opts = DaemonOptions::from(c);
        assert!(
            opts.lazy_buckets,
            "default must be lazy buckets (full RAM is opt-in)"
        );

        let c = DaemonCli::try_parse_from(["needled", "--full-index"]).unwrap();
        assert!(c.full_index);
        let opts = DaemonOptions::from(c);
        assert!(
            !opts.lazy_buckets,
            "--full-index must load every bucket into RAM"
        );
    }

    #[test]
    fn bearer_required_when_token_set() {
        let tmp = tempfile::tempdir().unwrap();
        let idx = tiny_dataset(tmp.path());
        let token = "test-token-ws3";
        let handle = start(DaemonOptions {
            index: idx.clone(),
            bind: "127.0.0.1:0".into(),
            token: Some(token.into()),
            ..Default::default()
        })
        .expect("start needled");
        let base = handle.base_url();
        assert!(base.starts_with("http://127.0.0.1:"));

        let (st, body) = http_get(&format!("{base}/v1/query?key=user_0000")).expect("no auth");
        assert_eq!(st, 401, "query without Authorization, body={body}");
        assert_401_no_secrets(&body, &idx, token);

        let (st, body) = http_get(&format!("{base}/v1/stats")).expect("stats no auth");
        assert_eq!(st, 401, "stats without Authorization, body={body}");
        assert_401_no_secrets(&body, &idx, token);

        let (st, body) = http_get(&format!("{base}/health")).expect("health");
        assert_eq!(st, 200, "health status, body={body}");

        let (st, body) =
            http_get_auth(&format!("{base}/v1/query?key=user_0000"), Some(token)).expect("auth");
        assert_eq!(st, 200, "query with bearer, body={body}");
        let q: Value = serde_json::from_str(&body).expect("query json");
        assert_eq!(q["key"], json!("user_0000"));
        assert!(!q["rows"].as_array().expect("rows").is_empty());

        let (st, body) =
            http_get_auth(&format!("{base}/v1/query?key=user_0000"), Some("wrong")).expect("wrong");
        assert_eq!(st, 401, "wrong token, body={body}");
        assert_401_no_secrets(&body, &idx, token);

        handle.stop();
    }

    fn openssl_ok() -> bool {
        Command::new("openssl")
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn gen_self_signed(dir: &Path) -> Result<(PathBuf, PathBuf)> {
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        let st = Command::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout"])
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .args(["-days", "1", "-subj", "/CN=localhost"])
            .status()
            .context("openssl req")?;
        if !st.success() {
            bail!("openssl req failed");
        }
        Ok((cert, key))
    }

    #[test]
    fn tls_query_with_bearer() {
        if !openssl_ok() {
            eprintln!("skip: openssl CLI missing");
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let idx = tiny_dataset(tmp.path());
        let (cert, key) = match gen_self_signed(tmp.path()) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("skip: openssl cert gen failed: {e:#}");
                return;
            }
        };
        let token = "tls-token-ws3";
        let handle = start(DaemonOptions {
            index: idx,
            bind: "127.0.0.1:0".into(),
            tls_cert: Some(cert),
            tls_key: Some(key),
            token: Some(token.into()),
            ..Default::default()
        })
        .expect("start needled tls");
        let base = handle.base_url();
        assert!(
            base.starts_with("https://127.0.0.1:"),
            "tls base url: {base}"
        );
        assert_ne!(base, "https://127.0.0.1:0");

        let (st, body) = http_get_auth(&format!("{base}/v1/query?key=user_0000"), Some(token))
            .expect("tls query");
        assert_eq!(st, 200, "tls query status, body={body}");
        let q: Value = serde_json::from_str(&body).expect("query json");
        assert!(!q["rows"].as_array().expect("rows").is_empty());

        let (st, body) = http_get(&format!("{base}/health")).expect("tls health");
        assert_eq!(st, 200, "tls health, body={body}");

        handle.stop();
    }

    #[test]
    fn query_json_golden_frozen_keys() {
        let golden: Value =
            serde_json::from_str(include_str!("../tests/fixtures/v1-query-success.json"))
                .expect("golden query json");
        assert_query_json_frozen(&golden);
        assert_eq!(golden["key"], json!("user_0000"));

        let tmp = tempfile::tempdir().unwrap();
        let idx = tiny_dataset(tmp.path());
        let q = RapQuerier::new(load_index(&idx).unwrap());
        let live = query_result_json(&q.query("user_0000").unwrap());
        assert_query_json_frozen(&live);
        let expl = explain_result_json(&q.explain("user_0000", &QueryOptions::default()).unwrap());
        assert_object_has_keys(&expl, EXPLAIN_JSON_KEYS, "explain");
    }

    #[test]
    fn frozen_error_codes_from_anyhow() {
        let stored = FileIdent {
            path: "/tmp/local.parquet".into(),
            etag: None,
            size: Some(10),
            mtime_ms: Some(1),
        };
        let live = FileIdent {
            path: stored.path.clone(),
            etag: None,
            size: Some(11),
            mtime_ms: Some(1),
        };
        let (st, body) = map_anyhow_error(&stale_file_identity_error(&stored, Some(&live)));
        assert_eq!(st, 500);
        assert_eq!(body["error"], json!(ERR_STALE_FILE_IDENTITY));
        assert!(body["message"].as_str().is_some());
        assert_eq!(body["path"], json!("/tmp/local.parquet"));
        assert!(body["stored"].is_object());
        assert!(body["live"].is_object());

        let remote = FileIdent {
            path: "s3://bucket/key.parquet".into(),
            etag: Some("abc".into()),
            size: Some(4),
            mtime_ms: None,
        };
        let (st, body) = map_anyhow_error(&stale_file_identity_error(&remote, None));
        assert_eq!(st, 500);
        assert_eq!(body["error"], json!(ERR_S3_IDENTITY_MISMATCH));
        assert_eq!(body["path"], json!("s3://bucket/key.parquet"));

        let (st, body) = map_error_message(
            "needle refuses Iceberg tables with unsupported delete files; apply deletes or compact first (file_format=ORC)",
        );
        assert_eq!(st, 500);
        assert_eq!(body["error"], json!(ERR_ICEBERG_UNSUPPORTED_DELETES));

        let (st, body) =
            map_error_message("no current snapshot in /tmp/table/metadata/v1.metadata.json");
        assert_eq!(st, 500);
        assert_eq!(body["error"], json!(ERR_CATALOG_ERROR));

        let (st, body) = map_error_message("s3_checksum_mismatch: Content-MD5");
        assert_eq!(st, 500);
        assert_eq!(body["error"], json!(ERR_S3_CHECKSUM_MISMATCH));

        let (st, body) = map_error_message("s3_identity_mismatch: etag abc vs xyz");
        assert_eq!(st, 500);
        assert_eq!(body["error"], json!(ERR_S3_IDENTITY_MISMATCH));
    }
}
