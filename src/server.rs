//! Long-lived HTTP query daemon (`needled`).
//!
//! Loads the RAP index once (or hash-buckets lazily per key), reuses
//! [`MetaCache`](crate::metadata::MetaCache) for the process when the full
//! index is resident, and serves JSON point queries.
//! Distinct from `needle serve`, which is a Range file server.

use crate::index::{
    load_index, load_index_entries_for_keys, load_index_file_dictionary, IndexFragmentMeta,
};
use crate::query::{ExplainResult, ListenRow, QueryOptions, QueryResult, RapQuerier};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Bind / load options for [`start`] and [`serve_forever`].
pub struct DaemonOptions {
    pub index: PathBuf,
    /// Listen address. Default `"127.0.0.1:7780"`. Port `0` is allowed.
    pub bind: String,
    /// If true, keep the file dictionary and load buckets per key; else `load_index` once.
    pub lazy_buckets: bool,
}

pub struct DaemonHandle {
    addr: SocketAddr,
    handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl DaemonHandle {
    /// `http://127.0.0.1:PORT` (actual bound address, including ephemeral ports).
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
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
    let (server, state, addr) = bind_and_load(opts)?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        serve_loop(server, state, stop_c);
    });
    Ok(DaemonHandle {
        addr,
        handle: Some(handle),
        stop,
    })
}

/// Bind, print the listen URL, and block until the accept loop ends (Ctrl-C).
pub fn serve_forever(opts: DaemonOptions) -> Result<()> {
    let index_display = opts.index.display().to_string();
    let (server, state, addr) = bind_and_load(opts)?;
    eprintln!(
        "needled listening on http://{}  index={}  lazy_buckets={}",
        addr, index_display, state.lazy_buckets
    );
    let stop = Arc::new(AtomicBool::new(false));
    serve_loop(server, state, stop);
    Ok(())
}

struct DaemonState {
    lazy_buckets: bool,
    root: PathBuf,
    files: Arc<Vec<PathBuf>>,
    fragments: Vec<IndexFragmentMeta>,
    /// Present when `lazy_buckets` is false; holds the process `MetaCache`.
    querier: Option<RapQuerier>,
}

fn bind_and_load(opts: DaemonOptions) -> Result<(tiny_http::Server, Arc<DaemonState>, SocketAddr)> {
    let state = Arc::new(load_state(&opts)?);
    let bind = if opts.bind.trim().is_empty() {
        "127.0.0.1:7780".to_string()
    } else {
        opts.bind
    };
    let server = match open_http(&bind) {
        Ok(s) => s,
        Err(e) if bind_requests_ephemeral(&bind) => {
            let port = 18780 + (std::process::id() % 1000);
            let fallback = format!("127.0.0.1:{port}");
            open_http(&fallback).with_context(|| {
                format!("fallback bind {fallback} after ephemeral bind {bind} failed: {e:#}")
            })?
        }
        Err(e) => return Err(e),
    };
    let addr = server
        .server_addr()
        .to_ip()
        .ok_or_else(|| anyhow::anyhow!("needled requires an IP bind address"))?;
    Ok((server, state, addr))
}

fn bind_requests_ephemeral(bind: &str) -> bool {
    bind.rsplit_once(':')
        .map(|(_, p)| p == "0")
        .unwrap_or(false)
}

fn open_http(bind: &str) -> Result<tiny_http::Server> {
    tiny_http::Server::http(bind).map_err(|e| anyhow::anyhow!("bind {bind}: {e}"))
}

fn load_state(opts: &DaemonOptions) -> Result<DaemonState> {
    if opts.lazy_buckets {
        let (files, fragments, root) = load_index_file_dictionary(&opts.index)?;
        Ok(DaemonState {
            lazy_buckets: true,
            root,
            files,
            fragments,
            querier: None,
        })
    } else {
        let index = load_index(&opts.index)?;
        Ok(DaemonState {
            lazy_buckets: false,
            root: index.root.clone(),
            files: Arc::clone(&index.files),
            fragments: index.fragments.clone(),
            querier: Some(RapQuerier::new(index)),
        })
    }
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

fn handle_one(request: tiny_http::Request, state: &DaemonState) {
    if request.method() != &tiny_http::Method::Get {
        respond_json(request, 405, json!({"error": "method not allowed"}));
        return;
    }
    let url = request.url().to_string();
    let (path, params) = parse_url(&url);
    let outcome = match path.as_str() {
        "/health" => Ok((200, json!({"ok": true}))),
        "/v1/query" => handle_query(&params, state),
        "/v1/explain" => handle_explain(&params, state),
        "/v1/stats" => Ok((200, stats_json(state))),
        _ => Ok((404, json!({"error": "not found"}))),
    };
    match outcome {
        Ok((code, body)) => respond_json(request, code, body),
        Err(e) => respond_json(request, 500, json!({"error": format!("{e:#}")})),
    }
}

fn handle_query(params: &HashMap<String, String>, state: &DaemonState) -> Result<(u16, Value)> {
    let Some(key) = params.get("key").filter(|s| !s.is_empty()) else {
        return Ok((400, json!({"error": "missing key"})));
    };
    let opts = match query_options_from_params(params) {
        Ok(o) => o,
        Err(msg) => return Ok((400, json!({"error": msg}))),
    };
    let result = with_querier(state, key, |q| q.query_with(key, &opts))?;
    Ok((200, query_result_json(&result)))
}

fn handle_explain(params: &HashMap<String, String>, state: &DaemonState) -> Result<(u16, Value)> {
    let Some(key) = params.get("key").filter(|s| !s.is_empty()) else {
        return Ok((400, json!({"error": "missing key"})));
    };
    let opts = match query_options_from_params(params) {
        Ok(o) => o,
        Err(msg) => return Ok((400, json!({"error": msg}))),
    };
    let result = with_querier(state, key, |q| q.explain(key, &opts))?;
    Ok((200, explain_result_json(&result)))
}

fn with_querier<T>(
    state: &DaemonState,
    key: &str,
    f: impl FnOnce(&RapQuerier) -> Result<T>,
) -> Result<T> {
    if let Some(q) = &state.querier {
        f(q)
    } else {
        let index = load_index_entries_for_keys(
            &state.root,
            Arc::clone(&state.files),
            &state.fragments,
            &[key.to_string()],
        )?;
        let q = RapQuerier::new(index);
        f(&q)
    }
}

fn stats_json(state: &DaemonState) -> Value {
    let fragments: Vec<Value> = state
        .fragments
        .iter()
        .map(|m| {
            json!({
                "fragment_id": m.fragment_id,
                "num_buckets": m.num_buckets,
                "num_files": m.files.len(),
                "created_at": m.created_at,
                "key_columns": m.key_columns,
                "value_columns": m.value_columns,
            })
        })
        .collect();
    let mut body = json!({
        "index": state.root.display().to_string(),
        "lazy_buckets": state.lazy_buckets,
        "num_files": state.files.len(),
        "num_fragments": state.fragments.len(),
        "fragments": fragments,
    });
    if let Some(q) = &state.querier {
        body["num_keys"] = json!(q.index.num_keys());
        body["num_entries"] = json!(q.index.num_entries());
        body["fragment_id"] = json!(q.index.fragments.first().map(|f| f.fragment_id.as_str()));
        body["num_buckets"] = json!(q
            .index
            .fragments
            .first()
            .map(|f| f.num_buckets)
            .unwrap_or(0));
    } else if let Some(m) = state.fragments.first() {
        body["fragment_id"] = json!(m.fragment_id);
        body["num_buckets"] = json!(m.num_buckets);
    }
    body
}

fn query_result_json(result: &QueryResult) -> Value {
    let t = &result.timings;
    json!({
        "key": result.key,
        "rows": result.rows.iter().map(listen_row_json).collect::<Vec<_>>(),
        "covering": result.covering_hits,
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

fn listen_row_json(r: &ListenRow) -> Value {
    json!({
        "user_id": r.user_id,
        "timestamp_ms": r.timestamp_ms,
        "track_uri": r.track_uri,
        "duration_ms": r.duration_ms,
        "source_file": r.source_file,
        "row_number": r.row_number,
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
    let payload = serde_json::to_string(&body).unwrap_or_else(|_| "{\"error\":\"serialize\"}".into());
    let response = tiny_http::Response::from_string(payload)
        .with_status_code(status)
        .with_header(json_header(b"Content-Type", b"application/json"))
        .with_header(json_header(b"Connection", b"close"));
    let _ = request.respond(response);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::IndexBuilder;
    use crate::writer::{WriteMode, WriterOptions, write_sample_dataset};
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::path::Path;

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
        let bare = url
            .strip_prefix("http://")
            .context("only http:// supported")?;
        let (hostport, path) = match bare.split_once('/') {
            Some((h, p)) => (h, format!("/{p}")),
            None => (bare, "/".to_string()),
        };
        let req = format!(
            "GET {path} HTTP/1.1\r\nHost: {hostport}\r\nConnection: close\r\n\r\n"
        );
        let mut last_err = None;
        for _ in 0..25 {
            match http_get_once(hostport, &req) {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("http get {url} failed")))
    }

    fn http_get_once(hostport: &str, req: &str) -> Result<(u16, String)> {
        let mut stream =
            TcpStream::connect(hostport).with_context(|| format!("connect {hostport}"))?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
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

    #[test]
    fn health_query_and_stop() {
        let tmp = tempfile::tempdir().unwrap();
        let idx = tiny_dataset(tmp.path());
        let handle = start(DaemonOptions {
            index: idx,
            bind: "127.0.0.1:0".into(),
            lazy_buckets: false,
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
        assert!(
            body.contains("user_0000") || body.contains("rows"),
            "query body: {body}"
        );
        let q: Value = serde_json::from_str(&body).expect("query json");
        let rows = q["rows"].as_array().expect("rows array");
        assert!(!rows.is_empty(), "expected rows for user_0000, body={body}");

        handle.stop();
    }
}
