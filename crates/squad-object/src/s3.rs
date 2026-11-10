//! Client for Amazon S3 and compatible object stores.

use std::io::Read;
use std::time::{Duration, SystemTime};

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

/// [`ObjectStore`] on an S3-compatible service.
///
/// Requests are signed with AWS Signature Version 4. Requests that fail
/// with a connection error, a 5xx status or 429 are retried with
/// exponential backoff. Objects are written with a single `PUT`, which
/// S3 limits to 5 GiB; squad writes objects of a few megabytes.
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
}

struct Response {
    status: u16,
    body: Vec<u8>,
    content_length: Option<u64>,
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
        if authority.is_empty() || authority.contains('/') {
            return Err(Error::Invalid(format!("S3 endpoint {endpoint:?} must be scheme://host[:port]")));
        }
        let (host, root) = if config.path_style {
            (authority.to_string(), format!("/{}", config.bucket))
        } else {
            (format!("{}.{authority}", config.bucket), String::new())
        };
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(10))
            .timeout_read(Duration::from_secs(60))
            .timeout_write(Duration::from_secs(60))
            .build();
        Ok(S3Store { base: format!("{scheme}://{host}"), host, root, agent, retries: 5, config })
    }

    /// Number of attempts per request before giving up (default 5).
    pub fn with_retries(mut self, attempts: u32) -> S3Store {
        self.retries = attempts.max(1);
        self
    }

    fn full_key(&self, key: &str) -> String {
        if self.config.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.config.prefix)
        }
    }

    /// One signed request, without retries. `Ok` for any HTTP status.
    fn send(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        extra: &[(&str, String)],
        body: Option<&[u8]>,
    ) -> std::result::Result<Response, String> {
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
            Err(ureq::Error::Transport(t)) => return Err(t.to_string()),
        };
        let status = resp.status();
        let content_length = resp.header("content-length").and_then(|v| v.parse().ok());
        let mut out = Vec::new();
        if method != "HEAD" {
            resp.into_reader().read_to_end(&mut out).map_err(|e| format!("reading response: {e}"))?;
        }
        Ok(Response { status, body: out, content_length })
    }

    /// A request with retries. Returns the response for every status
    /// below 500 except 429; the second value is the number of attempts
    /// that preceded the successful one.
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
                std::thread::sleep(Duration::from_millis(25u64 << attempt.min(6)));
            }
            match self.send(method, path, query, extra, body) {
                Ok(r) if r.status >= 500 || r.status == 429 => {
                    last = format!("status {}: {}", r.status, error_message(&r.body));
                }
                Ok(r) => return Ok((r, attempt)),
                Err(e) => last = e,
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
}

/// Text of the `<Message>` element of an S3 error document, if any.
fn error_message(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    element(&text, "Message").map(xml_unescape).unwrap_or_else(|| text.chars().take(200).collect())
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

impl ObjectStore for S3Store {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
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

    fn delete(&self, key: &str) -> Result<()> {
        let (r, _) = self.request("DELETE", &self.object_path(key)?, &[], &[], None)?;
        match r.status {
            200..=299 | 404 => Ok(()),
            _ => Err(Self::failed(&r)),
        }
    }
}
