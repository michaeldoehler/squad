//! Client for Amazon S3 and compatible object stores.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::sigv4::{self, Signable};
use crate::{check_key, Error, ObjectMeta, ObjectStore, Result};

/// Where and how to reach a bucket.
#[derive(Clone)]
pub struct S3Config {
    /// Service endpoint, e.g. `http://localhost:9000`. `None` uses
    /// `https://s3.<region>.amazonaws.com`.
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// Prepended to every key, without leading or trailing slash.
    pub prefix: String,
    /// Address the bucket as `endpoint/bucket/key` instead of
    /// `bucket.endpoint/key`. Most S3-compatible services need this.
    pub path_style: bool,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("path_style", &self.path_style)
            .finish_non_exhaustive()
    }
}

/// Smallest part S3 accepts in a multipart upload, except for the last.
const MIN_PART: usize = 5 << 20;

/// [`ObjectStore`] on an S3-compatible service.
///
/// Requests are signed with AWS Signature Version 4 and sent through one
/// HTTP agent per store, which keeps connections open for reuse.
///
/// Failures are classified. Connection errors, timeouts, 5xx responses,
/// 429 and S3's `RequestTimeout` are transient: the request is retried
/// up to [`with_retries`](S3Store::with_retries) times in total, after
/// a random delay below an exponentially growing bound (full jitter, base
/// 50 ms, at most 5 s). Every other 4xx response is permanent and
/// returned at once as [`Error::Remote`]; exhausted retries end in
/// [`Error::Transport`].
///
/// Objects larger than the multipart threshold (default 16 MiB) are
/// uploaded in parts (default 8 MiB each); a failed multipart upload is
/// aborted so that its parts do not linger.
pub struct S3Store {
    config: S3Config,
    agent: ureq::Agent,
    /// `scheme://authority` requests go to.
    base: String,
    /// Value of the `Host` header.
    host: String,
    /// Path prefix in front of the object key (`/bucket` in path style).
    root: String,
    retries: u32,
    backoff_base: Duration,
    backoff_cap: Duration,
    multipart_threshold: usize,
    part_size: usize,
    stats: Stats,
}

#[derive(Default)]
struct Stats {
    requests: AtomicU64,
    retries: AtomicU64,
    multipart_uploads: AtomicU64,
}

/// Counters of an [`S3Store`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct S3Stats {
    /// HTTP requests sent, including retries.
    pub requests: u64,
    /// Requests that were repeated after a transient failure.
    pub retries: u64,
    /// Objects uploaded with a multipart upload.
    pub multipart_uploads: u64,
}

struct Response {
    status: u16,
    body: Vec<u8>,
    content_length: Option<u64>,
    etag: Option<String>,
}

/// Why one attempt failed.
enum Failure {
    /// Worth another attempt.
    Transient(String),
    /// Will not get better by retrying.
    Permanent(Error),
}

/// True for HTTP statuses that a retry may cure.
fn transient_status(status: u16, body: &[u8]) -> bool {
    status >= 500 || status == 429 || status == 408 || (status == 400 && error_code(body) == Some("RequestTimeout"))
}

/// 64 random bits for backoff jitter.
fn jitter() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut h = RandomState::new().build_hasher();
    h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    h.write_u128(SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos()));
    h.finish()
}

/// Delay before retry number `attempt` (1-based): uniformly random below
/// `min(cap, base * 2^(attempt-1))`.
pub(crate) fn backoff_delay(attempt: u32, base: Duration, cap: Duration, random: u64) -> Duration {
    let bound = base.saturating_mul(1u32 << (attempt.saturating_sub(1)).min(20)).min(cap);
    let nanos = bound.as_nanos() as u64;
    if nanos == 0 {
        return Duration::ZERO;
    }
    Duration::from_nanos(random % nanos)
}

impl S3Store {
    pub fn new(config: S3Config) -> Result<S3Store> {
        if config.bucket.is_empty() {
            return Err(Error::Invalid("S3 bucket name is empty".into()));
        }
        let endpoint = config.endpoint.clone().unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", config.region));
        let endpoint = endpoint.trim_end_matches('/');
        let (scheme, authority) = endpoint
            .split_once("://")
            .ok_or_else(|| Error::Invalid(format!("S3 endpoint {endpoint:?} lacks a scheme")))?;
        if !matches!(scheme, "http" | "https") {
            return Err(Error::Invalid(format!("S3 endpoint {endpoint:?} must use http or https")));
        }
        if authority.is_empty() || authority.contains('/') {
            return Err(Error::Invalid(format!("S3 endpoint {endpoint:?} must be scheme://host[:port]")));
        }
        let (host, root) = if config.path_style {
            (authority.to_string(), format!("/{}", config.bucket))
        } else {
            (format!("{}.{authority}", config.bucket), String::new())
        };
        let mut store = S3Store {
            base: format!("{scheme}://{host}"),
            host,
            root,
            agent: ureq::AgentBuilder::new().build(),
            retries: 5,
            backoff_base: Duration::from_millis(50),
            backoff_cap: Duration::from_secs(5),
            multipart_threshold: 16 << 20,
            part_size: 8 << 20,
            stats: Stats::default(),
            config,
        };
        store = store.with_timeouts(Duration::from_secs(10), Duration::from_secs(60));
        Ok(store)
    }

    /// Number of attempts per request before giving up (default 5).
    pub fn with_retries(mut self, attempts: u32) -> S3Store {
        self.retries = attempts.max(1);
        self
    }

    /// Bounds of the random delay between attempts: the first retry waits
    /// below `base`, each further one below twice as long, never above
    /// `cap` (defaults 50 ms and 5 s).
    pub fn with_backoff(mut self, base: Duration, cap: Duration) -> S3Store {
        self.backoff_base = base;
        self.backoff_cap = cap.max(base);
        self
    }

    /// Time allowed to establish a connection, and to wait for any single
    /// read or write on it (defaults 10 s and 60 s). Replaces the HTTP
    /// agent, and with it the pool of open connections.
    pub fn with_timeouts(mut self, connect: Duration, io: Duration) -> S3Store {
        self.agent = ureq::AgentBuilder::new()
            .timeout_connect(connect)
            .timeout_read(io)
            .timeout_write(io)
            .max_idle_connections_per_host(16)
            .build();
        self
    }

    /// Objects larger than `threshold` bytes are uploaded in parts of
    /// `part_size` bytes (defaults 16 MiB and 8 MiB). S3 requires parts of
    /// at least 5 MiB, so smaller values are raised to that.
    pub fn with_multipart(mut self, threshold: usize, part_size: usize) -> S3Store {
        self.part_size = part_size.max(MIN_PART);
        self.multipart_threshold = threshold.max(self.part_size);
        self
    }

    /// Request counters since the store was created.
    pub fn stats(&self) -> S3Stats {
        S3Stats {
            requests: self.stats.requests.load(Ordering::Relaxed),
            retries: self.stats.retries.load(Ordering::Relaxed),
            multipart_uploads: self.stats.multipart_uploads.load(Ordering::Relaxed),
        }
    }

    fn full_key(&self, key: &str) -> String {
        if self.config.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.config.prefix)
        }
    }

    /// One signed request, without retries.
    fn send(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        extra: &[(&str, String)],
        body: Option<&[u8]>,
    ) -> std::result::Result<Response, Failure> {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        let payload_hash = sigv4::sha256_hex(body.unwrap_or_default());
        let date = sigv4::amz_date(SystemTime::now());
        let mut headers = vec![
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), date.clone()),
        ];
        headers.extend(extra.iter().map(|(k, v)| (k.to_string(), v.clone())));
        let signable = Signable { method, path, query, headers: &headers, payload_hash: &payload_hash };
        let auth = sigv4::authorization(
            &signable,
            &self.config.access_key,
            &self.config.secret_key,
            &self.config.region,
            &date,
        );

        let mut url = format!("{}{}", self.base, sigv4::uri_encode(path, true));
        if !query.is_empty() {
            url.push('?');
            url.push_str(&sigv4::canonical_query(query));
        }
        let mut req = self.agent.request(method, &url).set("authorization", &auth);
        for (k, v) in headers.iter().filter(|(k, _)| k != "host") {
            req = req.set(k, v);
        }
        let result = match body {
            Some(b) => req.send_bytes(b),
            None => req.call(),
        };
        let resp = match result {
            Ok(r) => r,
            Err(ureq::Error::Status(_, r)) => r,
            Err(ureq::Error::Transport(t)) => {
                return Err(match t.kind() {
                    ureq::ErrorKind::InvalidUrl
                    | ureq::ErrorKind::UnknownScheme
                    | ureq::ErrorKind::InvalidProxyUrl
                    | ureq::ErrorKind::ProxyUnauthorized => Failure::Permanent(Error::Invalid(t.to_string())),
                    // Name resolution, connecting, timeouts, broken
                    // connections, malformed responses.
                    _ => Failure::Transient(t.to_string()),
                });
            }
        };
        let status = resp.status();
        let content_length = resp.header("content-length").and_then(|v| v.parse().ok());
        let etag = resp.header("etag").map(str::to_string);
        let mut out = Vec::new();
        if method != "HEAD" {
            resp.into_reader()
                .read_to_end(&mut out)
                .map_err(|e| Failure::Transient(format!("reading the response: {e}")))?;
        }
        Ok(Response { status, body: out, content_length, etag })
    }

    /// A request with retries of transient failures. Returns the response
    /// for every status that is not transient; the second value is the
    /// number of attempts that preceded the one answered.
    fn request(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        extra: &[(&str, String)],
        body: Option<&[u8]>,
    ) -> Result<(Response, u32)> {
        let mut last = String::new();
        for attempt in 0..self.retries {
            if attempt > 0 {
                self.stats.retries.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(backoff_delay(attempt, self.backoff_base, self.backoff_cap, jitter()));
            }
            match self.send(method, path, query, extra, body) {
                Ok(r) if transient_status(r.status, &r.body) => {
                    last = format!("status {}: {}", r.status, error_message(&r.body));
                }
                Ok(r) => return Ok((r, attempt)),
                Err(Failure::Transient(e)) => last = e,
                Err(Failure::Permanent(e)) => return Err(e),
            }
        }
        Err(Error::Transport(format!("{method} {path} failed after {} attempts: {last}", self.retries)))
    }

    fn object_path(&self, key: &str) -> Result<String> {
        check_key(key)?;
        Ok(format!("{}/{}", self.root, self.full_key(key)))
    }

    fn failed(r: &Response) -> Error {
        Error::Remote { status: r.status, message: error_message(&r.body) }
    }

    /// Uploads `data` in parts: create, upload each part, complete. Any
    /// failure aborts the upload.
    fn put_multipart(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.object_path(key)?;
        let (r, _) = self.request("POST", &path, &[("uploads".to_string(), String::new())], &[], Some(b""))?;
        if r.status != 200 {
            return Err(Self::failed(&r));
        }
        let text = String::from_utf8_lossy(&r.body);
        let upload_id = element(&text, "UploadId").map(xml_unescape).ok_or_else(|| Error::Remote {
            status: r.status,
            message: "CreateMultipartUpload answered without an UploadId".into(),
        })?;
        let result = self.upload_parts(&path, &upload_id, data);
        if result.is_err() {
            let query = [("uploadId".to_string(), upload_id.clone())];
            let _ = self.request("DELETE", &path, &query, &[], None);
        }
        result?;
        self.stats.multipart_uploads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn upload_parts(&self, path: &str, upload_id: &str, data: &[u8]) -> Result<()> {
        let mut etags = Vec::new();
        for (i, part) in data.chunks(self.part_size).enumerate() {
            let number = i + 1;
            let query =
                [("partNumber".to_string(), number.to_string()), ("uploadId".to_string(), upload_id.to_string())];
            let (r, _) = self.request("PUT", path, &query, &[], Some(part))?;
            if !(200..300).contains(&r.status) {
                return Err(Self::failed(&r));
            }
            let etag = r.etag.ok_or_else(|| Error::Remote {
                status: r.status,
                message: format!("part {number} was accepted without an ETag"),
            })?;
            etags.push((number, etag));
        }
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (number, etag) in &etags {
            xml.push_str(&format!("<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>", xml_escape(etag)));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let query = [("uploadId".to_string(), upload_id.to_string())];
        let (r, earlier) = self.request("POST", path, &query, &[], Some(xml.as_bytes()))?;
        match r.status {
            // S3 may report a failure in the body of a 200 response.
            200 if !String::from_utf8_lossy(&r.body).contains("<Error>") => Ok(()),
            200 => Err(Error::Remote { status: 200, message: error_message(&r.body) }),
            // A retry after a lost confirmation finds the upload finished.
            404 if earlier > 0 => Ok(()),
            _ => Err(Self::failed(&r)),
        }
    }
}

/// Text of the `<Message>` element of an S3 error document, if any.
fn error_message(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    element(&text, "Message").map(xml_unescape).unwrap_or_else(|| text.chars().take(200).collect())
}

/// The `<Code>` of an S3 error document, if any.
fn error_code(body: &[u8]) -> Option<&str> {
    std::str::from_utf8(body).ok().and_then(|t| element(t, "Code"))
}

/// Content of the first `<name>…</name>` in `xml`.
fn element<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{name}>"))? + start;
    Some(&xml[start..end])
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

impl S3Store {
    /// ListObjectsV2, all pages, optionally starting after a key.
    fn list_from(&self, prefix: &str, after: Option<&str>) -> Result<Vec<ObjectMeta>> {
        let strip = if self.config.prefix.is_empty() { 0 } else { self.config.prefix.len() + 1 };
        let full_prefix = if strip == 0 { prefix.to_string() } else { format!("{}/{prefix}", self.config.prefix) };
        let path = if self.root.is_empty() { "/".to_string() } else { self.root.clone() };
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query =
                vec![("list-type".to_string(), "2".to_string()), ("prefix".to_string(), full_prefix.clone())];
            if let Some(t) = &token {
                query.push(("continuation-token".to_string(), t.clone()));
            } else if let Some(after) = after {
                let full = if strip == 0 { after.to_string() } else { format!("{}/{after}", self.config.prefix) };
                query.push(("start-after".to_string(), full));
            }
            let (r, _) = self.request("GET", &path, &query, &[], None)?;
            if r.status != 200 {
                return Err(Self::failed(&r));
            }
            let xml = String::from_utf8_lossy(&r.body);
            let mut rest: &str = &xml;
            while let Some(start) = rest.find("<Contents>") {
                let end = rest[start..].find("</Contents>").map_or(rest.len(), |e| start + e);
                let item = &rest[start..end];
                let key = xml_unescape(element(item, "Key").unwrap_or_default());
                let size = element(item, "Size").and_then(|s| s.trim().parse().ok()).unwrap_or(0);
                if key.len() >= strip {
                    out.push(ObjectMeta { key: key[strip..].to_string(), size });
                }
                rest = &rest[end..];
            }
            let truncated = element(&xml, "IsTruncated").is_some_and(|v| v.trim() == "true");
            token = element(&xml, "NextContinuationToken").map(xml_unescape);
            if !truncated || token.is_none() {
                break;
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }
}

impl ObjectStore for S3Store {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if data.len() > self.multipart_threshold {
            return self.put_multipart(key, data);
        }
        let (r, _) = self.request("PUT", &self.object_path(key)?, &[], &[], Some(data))?;
        match r.status {
            200..=299 => Ok(()),
            _ => Err(Self::failed(&r)),
        }
    }

    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<bool> {
        let path = self.object_path(key)?;
        let (r, earlier_attempts) =
            self.request("PUT", &path, &[], &[("if-none-match", "*".to_string())], Some(data))?;
        match r.status {
            200..=299 => Ok(true),
            // A retried request may find the object its own lost first
            // attempt created.
            412 | 409 if earlier_attempts > 0 => Ok(self.get(key)?.as_deref() == Some(data)),
            412 | 409 => Ok(false),
            _ => Err(Self::failed(&r)),
        }
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let (r, _) = self.request("GET", &self.object_path(key)?, &[], &[], None)?;
        match r.status {
            200 => Ok(Some(r.body)),
            404 => Ok(None),
            _ => Err(Self::failed(&r)),
        }
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return match self.head(key)? {
                Some(m) if offset <= m.size => Ok(Vec::new()),
                Some(m) => Err(Error::Invalid(format!(
                    "range {offset}+0 lies beyond the end of object {key:?} ({} bytes)",
                    m.size
                ))),
                None => Err(crate::missing(key)),
            };
        }
        let end = offset.checked_add(len - 1).ok_or_else(|| Error::Invalid("range overflows".into()))?;
        let range = format!("bytes={offset}-{end}");
        let (r, _) = self.request("GET", &self.object_path(key)?, &[], &[("range", range)], None)?;
        match r.status {
            200 | 206 if r.body.len() as u64 == len => Ok(r.body),
            // A server that ignores the header sends the whole object.
            200 => crate::range_of(key, &r.body, offset, len),
            206 | 416 => Err(Error::Invalid(format!("range {offset}+{len} lies beyond the end of object {key:?}"))),
            404 => Err(crate::missing(key)),
            _ => Err(Self::failed(&r)),
        }
    }

    fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let (r, _) = self.request("HEAD", &self.object_path(key)?, &[], &[], None)?;
        match r.status {
            200 => Ok(Some(ObjectMeta { key: key.to_string(), size: r.content_length.unwrap_or(0) })),
            404 => Ok(None),
            _ => Err(Self::failed(&r)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.list_from(prefix, None)
    }

    fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>> {
        self.list_from(prefix, Some(after))
    }

    fn delete(&self, key: &str) -> Result<()> {
        let (r, _) = self.request("DELETE", &self.object_path(key)?, &[], &[], None)?;
        match r.status {
            200..=299 | 404 => Ok(()),
            _ => Err(Self::failed(&r)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_bounded_and_grows() {
        let (base, cap) = (Duration::from_millis(50), Duration::from_secs(5));
        for attempt in 1..30 {
            let bound = base.saturating_mul(1 << (attempt - 1).min(20)).min(cap);
            for r in [0, 1, 12345, u64::MAX / 3, u64::MAX] {
                assert!(backoff_delay(attempt, base, cap, r) < bound.max(Duration::from_nanos(1)));
            }
        }
        // Full jitter: the whole range down to zero is used.
        assert_eq!(backoff_delay(3, base, cap, 0), Duration::ZERO);
        assert!(backoff_delay(3, base, cap, 199_999_999) > Duration::from_millis(199));
        assert_eq!(backoff_delay(1, Duration::ZERO, Duration::ZERO, 7), Duration::ZERO);
    }

    #[test]
    fn statuses_are_classified() {
        for status in [500, 502, 503, 504, 429, 408] {
            assert!(transient_status(status, b""), "{status}");
        }
        for status in [400, 401, 403, 404, 409, 412, 416] {
            assert!(!transient_status(status, b"<Error><Code>AccessDenied</Code></Error>"), "{status}");
        }
        assert!(transient_status(400, b"<Error><Code>RequestTimeout</Code></Error>"));
    }
}
