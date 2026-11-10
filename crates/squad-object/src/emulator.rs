//! A small in-process S3 server for tests.
//!
//! It implements the operations [`S3Store`](crate::S3Store) uses —
//! `PutObject` (also conditional), `GetObject` with ranges, `HeadObject`,
//! `DeleteObject` and `ListObjectsV2` with continuation tokens — and
//! **verifies the Signature V4 of every request**, so tests against it
//! exercise the client's signing. It can also fail or drop requests on
//! demand to test retries.
//!
//! This is test infrastructure, not a storage service: objects live in
//! memory and one bucket and one credential are known.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::sigv4::{self, Signable};
use crate::S3Config;

pub const ACCESS_KEY: &str = "squad-test-access";
pub const SECRET_KEY: &str = "squad-test-secret/Key+With=Symbols";
pub const REGION: &str = "eu-test-1";
pub const BUCKET: &str = "squad-bucket";

#[derive(Default)]
struct State {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
    /// Remaining requests to answer with status 500.
    fail_next: AtomicU64,
    /// Remaining requests to drop without an answer.
    drop_next: AtomicU64,
    /// Remaining writes to apply and then drop without an answer.
    drop_after_next: AtomicU64,
    requests: AtomicU64,
    rejected: AtomicU64,
    page_size: AtomicU64,
    stop: AtomicBool,
}

/// A running emulator. Stops when dropped.
pub struct S3Emulator {
    addr: SocketAddr,
    state: Arc<State>,
}

struct Request {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
}

fn take_one(counter: &AtomicU64) -> bool {
    counter.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok()
}

impl S3Emulator {
    /// Starts the emulator on a free local port.
    pub fn start() -> S3Emulator {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind emulator");
        let addr = listener.local_addr().expect("emulator address");
        let state = Arc::new(State::default());
        state.page_size.store(1000, Ordering::SeqCst);
        let accept_state = state.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if accept_state.stop.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                let state = accept_state.clone();
                std::thread::spawn(move || serve(stream, &state));
            }
        });
        S3Emulator { addr, state }
    }

    /// Configuration for an [`S3Store`](crate::S3Store) that talks to
    /// this emulator, storing its objects under `prefix`.
    pub fn config(&self, prefix: &str) -> S3Config {
        S3Config {
            endpoint: Some(format!("http://{}", self.addr)),
            region: REGION.to_string(),
            bucket: BUCKET.to_string(),
            access_key: ACCESS_KEY.to_string(),
            secret_key: SECRET_KEY.to_string(),
            prefix: prefix.to_string(),
            path_style: true,
        }
    }

    /// URL for [`open_url`](crate::open_url).
    pub fn url(&self, prefix: &str) -> String {
        format!(
            "s3://{BUCKET}/{prefix}?endpoint=http://{}&region={REGION}&path_style=true&access_key={ACCESS_KEY}&secret_key={}",
            self.addr,
            sigv4::uri_encode(SECRET_KEY, false)
        )
    }

    /// Answers the next `n` requests with status 500.
    pub fn fail_next(&self, n: u64) {
        self.state.fail_next.store(n, Ordering::SeqCst);
    }

    /// Closes the connection of the next `n` requests without answering.
    pub fn drop_next(&self, n: u64) {
        self.state.drop_next.store(n, Ordering::SeqCst);
    }

    /// Applies the next `n` writes but closes the connection instead of
    /// confirming them.
    pub fn drop_after_next(&self, n: u64) {
        self.state.drop_after_next.store(n, Ordering::SeqCst);
    }

    /// Maximum number of keys per list response (default 1000).
    pub fn set_page_size(&self, n: u64) {
        self.state.page_size.store(n.max(1), Ordering::SeqCst);
    }

    /// Requests received so far, including failed ones.
    pub fn requests(&self) -> u64 {
        self.state.requests.load(Ordering::SeqCst)
    }

    /// Requests rejected because of a bad signature.
    pub fn rejected(&self) -> u64 {
        self.state.rejected.load(Ordering::SeqCst)
    }

    /// Keys of all stored objects (bucket-relative).
    pub fn keys(&self) -> Vec<String> {
        self.state.objects.lock().keys().cloned().collect()
    }
}

impl Drop for S3Emulator {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::SeqCst);
        // Unblock the accept loop.
        let _ = TcpStream::connect(self.addr);
    }
}

fn read_request(reader: &mut BufReader<TcpStream>) -> Option<Request> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h).ok()? == 0 {
            return None;
        }
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        let (k, v) = h.split_once(':')?;
        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
    }
    let len: usize = headers.iter().find(|(k, _)| k == "content-length").and_then(|(_, v)| v.parse().ok()).unwrap_or(0);
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).ok()?;
    let (raw_path, raw_query) = target.split_once('?').unwrap_or((&target, ""));
    let query = raw_query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (sigv4::uri_decode(k), sigv4::uri_decode(v))
        })
        .collect();
    Some(Request { method, path: sigv4::uri_decode(raw_path), query, headers, body })
}

/// Recomputes the request's signature from what arrived on the wire.
fn signature_ok(req: &Request) -> Result<(), String> {
    let auth = req.header("authorization").ok_or("missing Authorization header")?;
    let rest = auth.strip_prefix("AWS4-HMAC-SHA256 ").ok_or("unsupported signature algorithm")?;
    let mut credential = "";
    let mut signed = "";
    let mut signature = "";
    for part in rest.split(',') {
        match part.trim().split_once('=') {
            Some(("Credential", v)) => credential = v,
            Some(("SignedHeaders", v)) => signed = v,
            Some(("Signature", v)) => signature = v,
            _ => {}
        }
    }
    let scope: Vec<&str> = credential.split('/').collect();
    if scope.len() != 5
        || scope[0] != ACCESS_KEY
        || scope[2] != REGION
        || scope[3] != "s3"
        || scope[4] != "aws4_request"
    {
        return Err(format!("unexpected credential scope {credential:?}"));
    }
    let date = req.header("x-amz-date").ok_or("missing x-amz-date")?;
    if !date.starts_with(scope[1]) {
        return Err("credential date does not match x-amz-date".into());
    }
    let declared = req.header("x-amz-content-sha256").ok_or("missing x-amz-content-sha256")?;
    if declared != sigv4::sha256_hex(&req.body) {
        return Err("payload hash does not match the body".into());
    }
    let names: Vec<&str> = signed.split(';').collect();
    for required in ["host", "x-amz-content-sha256", "x-amz-date"] {
        if !names.contains(&required) {
            return Err(format!("header {required} is not signed"));
        }
    }
    let mut headers = Vec::new();
    for name in names {
        let value = req.header(name).ok_or_else(|| format!("signed header {name} is absent"))?;
        headers.push((name.to_string(), value.to_string()));
    }
    let signable =
        Signable { method: &req.method, path: &req.path, query: &req.query, headers: &headers, payload_hash: declared };
    let (expected, _) = sigv4::signature(&signable, SECRET_KEY, REGION, date);
    if expected == signature {
        Ok(())
    } else {
        Err("signature does not match".into())
    }
}

fn respond(stream: &mut TcpStream, status: u16, headers: &[(&str, String)], body: &[u8], head: bool) -> bool {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        403 => "Forbidden",
        404 => "Not Found",
        412 => "Precondition Failed",
        416 => "Range Not Satisfiable",
        500 => "Internal Server Error",
        _ => "Error",
    };
    let mut out = format!("HTTP/1.1 {status} {reason}\r\n");
    let has_len = headers.iter().any(|(k, _)| *k == "content-length");
    for (k, v) in headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    if !has_len {
        out.push_str(&format!("content-length: {}\r\n", body.len()));
    }
    out.push_str("\r\n");
    let mut bytes = out.into_bytes();
    if !head {
        bytes.extend_from_slice(body);
    }
    stream.write_all(&bytes).and_then(|_| stream.flush()).is_ok()
}

fn error_xml(code: &str, message: &str) -> Vec<u8> {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>{message}</Message></Error>")
        .into_bytes()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn serve(stream: TcpStream, state: &State) {
    let _ = stream.set_nodelay(true);
    let Ok(mut out) = stream.try_clone() else { return };
    let mut reader = BufReader::new(stream);
    while let Some(req) = read_request(&mut reader) {
        state.requests.fetch_add(1, Ordering::SeqCst);
        if take_one(&state.drop_next) {
            return;
        }
        if take_one(&state.fail_next) {
            if !respond(&mut out, 500, &[], &error_xml("InternalError", "injected failure"), req.method == "HEAD") {
                return;
            }
            continue;
        }
        let head = req.method == "HEAD";
        if let Err(why) = signature_ok(&req) {
            state.rejected.fetch_add(1, Ordering::SeqCst);
            if !respond(&mut out, 403, &[], &error_xml("SignatureDoesNotMatch", &why), head) {
                return;
            }
            continue;
        }
        let bucket_root = format!("/{BUCKET}");
        let Some(rest) = req.path.strip_prefix(&bucket_root) else {
            if !respond(&mut out, 404, &[], &error_xml("NoSuchBucket", "unknown bucket"), head) {
                return;
            }
            continue;
        };
        let key = rest.trim_start_matches('/').to_string();
        let is_write = matches!(req.method.as_str(), "PUT" | "DELETE");
        let lose_answer = is_write && take_one(&state.drop_after_next);
        let (status, headers, body): (u16, Vec<(&str, String)>, Vec<u8>) = match (req.method.as_str(), key.is_empty()) {
            ("GET", true) => list(state, &req),
            ("PUT", false) => {
                let mut objects = state.objects.lock();
                if req.header("if-none-match") == Some("*") && objects.contains_key(&key) {
                    (412, vec![], error_xml("PreconditionFailed", "object exists"))
                } else {
                    objects.insert(key, req.body.clone());
                    (200, vec![], Vec::new())
                }
            }
            ("GET" | "HEAD", false) => match state.objects.lock().get(&key) {
                None => (404, vec![], error_xml("NoSuchKey", "The specified key does not exist.")),
                Some(data) => match req.header("range").and_then(|r| r.strip_prefix("bytes=")) {
                    Some(range) => {
                        let (a, b) = range.split_once('-').unwrap_or((range, ""));
                        match (a.parse::<usize>(), b.parse::<usize>()) {
                            (Ok(a), Ok(b)) if a <= b && a < data.len() => {
                                let end = b.min(data.len() - 1);
                                let cr = format!("bytes {a}-{end}/{}", data.len());
                                (206, vec![("content-range", cr)], data[a..=end].to_vec())
                            }
                            _ => (416, vec![], error_xml("InvalidRange", "The requested range is not satisfiable")),
                        }
                    }
                    None => (200, vec![], data.clone()),
                },
            },
            ("DELETE", false) => {
                state.objects.lock().remove(&key);
                (204, vec![], Vec::new())
            }
            _ => (404, vec![], error_xml("NotImplemented", "operation not supported by the emulator")),
        };
        if lose_answer {
            return;
        }
        if !respond(&mut out, status, &headers, &body, head) {
            return;
        }
    }
}

fn list(state: &State, req: &Request) -> (u16, Vec<(&'static str, String)>, Vec<u8>) {
    let param = |name: &str| req.query.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
    if param("list-type").as_deref() != Some("2") {
        return (404, vec![], error_xml("NotImplemented", "only ListObjectsV2 is supported"));
    }
    let prefix = param("prefix").unwrap_or_default();
    let after = param("continuation-token").unwrap_or_default();
    let page = state.page_size.load(Ordering::SeqCst) as usize;
    let objects = state.objects.lock();
    let mut matching = objects.iter().filter(|(k, _)| k.starts_with(&prefix) && k.as_str() > after.as_str());
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>");
    let mut last = None;
    let mut count = 0;
    for (k, v) in matching.by_ref().take(page) {
        xml.push_str(&format!("<Contents><Key>{}</Key><Size>{}</Size></Contents>", xml_escape(k), v.len()));
        last = Some(k.clone());
        count += 1;
    }
    let truncated = matching.next().is_some();
    xml.push_str(&format!("<KeyCount>{count}</KeyCount><IsTruncated>{truncated}</IsTruncated>"));
    if let (true, Some(last)) = (truncated, last) {
        xml.push_str(&format!("<NextContinuationToken>{}</NextContinuationToken>", xml_escape(&last)));
    }
    xml.push_str("</ListBucketResult>");
    (200, vec![("content-type", "application/xml".to_string())], xml.into_bytes())
}
