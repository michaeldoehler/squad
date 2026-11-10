use std::sync::Arc;

use crate::{Error, LocalStore, MemoryStore, ObjectStore, Result};

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 3 <= bytes.len() {
            if let Some(v) = s.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Opens the object store a URL describes.
///
/// * `memory://` – a new volatile store.
/// * `file:///absolute/path` or `file://relative/path` – a directory.
/// * `s3://bucket/prefix?region=…&endpoint=…&path_style=true` – an
///   S3-compatible bucket (feature `s3`). Further parameters:
///   `access_key`, `secret_key`. Parameters that are absent are taken
///   from `AWS_REGION`, `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID` and
///   `AWS_SECRET_ACCESS_KEY`. `path_style` defaults to `true` when an
///   endpoint is given and `false` for AWS itself.
pub fn open_url(url: &str) -> Result<Arc<dyn ObjectStore>> {
    let (scheme, rest) =
        url.split_once("://").ok_or_else(|| Error::Invalid(format!("object store URL {url:?} lacks a scheme")))?;
    match scheme {
        "memory" => Ok(Arc::new(MemoryStore::new())),
        "file" => {
            if rest.is_empty() {
                return Err(Error::Invalid("file:// URL without a path".into()));
            }
            Ok(Arc::new(LocalStore::new(percent_decode(rest))?))
        }
        "s3" => open_s3(rest),
        other => Err(Error::Invalid(format!("unknown object store scheme {other:?}"))),
    }
}

#[cfg(not(feature = "s3"))]
fn open_s3(_: &str) -> Result<Arc<dyn ObjectStore>> {
    Err(Error::Invalid("this build does not include the S3 client (feature `s3`)".into()))
}

#[cfg(feature = "s3")]
fn open_s3(rest: &str) -> Result<Arc<dyn ObjectStore>> {
    let (location, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
    let mut params: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        params.insert(percent_decode(k), percent_decode(v));
    }
    let mut take =
        |name: &str, env: &str| params.remove(name).or_else(|| std::env::var(env).ok().filter(|v| !v.is_empty()));
    let endpoint = take("endpoint", "AWS_ENDPOINT_URL");
    let region = take("region", "AWS_REGION").unwrap_or_else(|| "us-east-1".to_string());
    let access_key = take("access_key", "AWS_ACCESS_KEY_ID")
        .ok_or_else(|| Error::Invalid("no S3 access key: set access_key=… or AWS_ACCESS_KEY_ID".into()))?;
    let secret_key = take("secret_key", "AWS_SECRET_ACCESS_KEY")
        .ok_or_else(|| Error::Invalid("no S3 secret key: set secret_key=… or AWS_SECRET_ACCESS_KEY".into()))?;
    let path_style = match params.remove("path_style").as_deref() {
        Some("true" | "1") => true,
        Some("false" | "0") => false,
        Some(other) => return Err(Error::Invalid(format!("path_style must be true or false, not {other:?}"))),
        None => endpoint.is_some(),
    };
    if let Some(unknown) = params.keys().next() {
        return Err(Error::Invalid(format!("unknown S3 URL parameter {unknown:?}")));
    }
    let config = crate::S3Config {
        endpoint,
        region,
        bucket: percent_decode(bucket),
        access_key,
        secret_key,
        prefix: percent_decode(prefix.trim_matches('/')),
        path_style,
    };
    Ok(Arc::new(crate::S3Store::new(config)?))
}
