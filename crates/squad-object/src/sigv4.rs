//! AWS Signature Version 4 for S3 requests.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Percent-encodes everything but unreserved characters; `/` is kept in
/// paths and encoded in query strings.
pub(crate) fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(any(test, feature = "test-util"))]
pub(crate) fn uri_decode(s: &str) -> String {
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

/// `YYYYMMDDTHHMMSSZ` for the given time.
pub(crate) fn amz_date(now: SystemTime) -> String {
    let secs = now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()) as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

/// The parts of a request that are signed.
pub(crate) struct Signable<'a> {
    pub method: &'a str,
    /// Decoded absolute path, e.g. `/bucket/some key`.
    pub path: &'a str,
    /// Decoded query parameters.
    pub query: &'a [(String, String)],
    /// Headers to sign: lowercase names, trimmed values. Must include
    /// `host`, `x-amz-date` and `x-amz-content-sha256`.
    pub headers: &'a [(String, String)],
    /// Hex SHA-256 of the body.
    pub payload_hash: &'a str,
}

pub(crate) fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> =
        query.iter().map(|(k, v)| (uri_encode(k, false), uri_encode(v, false))).collect();
    pairs.sort();
    pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
}

/// Returns `(signature, signed header list)`.
pub(crate) fn signature(req: &Signable, secret_key: &str, region: &str, date: &str) -> (String, String) {
    let mut headers: Vec<(String, String)> = req.headers.to_vec();
    headers.sort();
    let signed = headers.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{}\n", v.trim())).collect();
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method,
        uri_encode(req.path, true),
        canonical_query(req.query),
        canonical_headers,
        signed,
        req.payload_hash
    );
    let day = &date[..8];
    let scope = format!("{day}/{region}/s3/aws4_request");
    let to_sign = format!("AWS4-HMAC-SHA256\n{date}\n{scope}\n{}", sha256_hex(canonical.as_bytes()));
    let k_date = hmac(format!("AWS4{secret_key}").as_bytes(), day.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, b"s3");
    let k_signing = hmac(&k_service, b"aws4_request");
    (hex(&hmac(&k_signing, to_sign.as_bytes())), signed)
}

/// Value of the `Authorization` header.
pub(crate) fn authorization(req: &Signable, access_key: &str, secret_key: &str, region: &str, date: &str) -> String {
    let (sig, signed) = signature(req, secret_key, region, date);
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{}/{region}/s3/aws4_request, SignedHeaders={signed}, Signature={sig}",
        &date[..8]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// "GET Object" example from the Amazon S3 API reference
    /// (Signature Calculations for the Authorization Header).
    #[test]
    fn matches_the_documented_example() {
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(sha256_hex(b""), empty);
        let headers = vec![
            ("host".to_string(), "examplebucket.s3.amazonaws.com".to_string()),
            ("range".to_string(), "bytes=0-9".to_string()),
            ("x-amz-content-sha256".to_string(), empty.to_string()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        let req = Signable { method: "GET", path: "/test.txt", query: &[], headers: &headers, payload_hash: empty };
        let (sig, signed) =
            signature(&req, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", "20130524T000000Z");
        assert_eq!(signed, "host;range;x-amz-content-sha256;x-amz-date");
        assert_eq!(sig, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");
    }

    /// "GET Bucket (List Objects)" example from the same page.
    #[test]
    fn matches_the_documented_list_example() {
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = vec![
            ("host".to_string(), "examplebucket.s3.amazonaws.com".to_string()),
            ("x-amz-content-sha256".to_string(), empty.to_string()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        let query = vec![("max-keys".to_string(), "2".to_string()), ("prefix".to_string(), "J".to_string())];
        let req = Signable { method: "GET", path: "/", query: &query, headers: &headers, payload_hash: empty };
        let (sig, _) = signature(&req, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", "20130524T000000Z");
        assert_eq!(sig, "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7");
    }

    #[test]
    fn dates_and_encoding() {
        assert_eq!(amz_date(UNIX_EPOCH), "19700101T000000Z");
        assert_eq!(amz_date(UNIX_EPOCH + std::time::Duration::from_secs(1_369_353_600)), "20130524T000000Z");
        assert_eq!(amz_date(UNIX_EPOCH + std::time::Duration::from_secs(951_782_400 + 86_399)), "20000229T235959Z");
        assert_eq!(uri_encode("a b/c+d~é", true), "a%20b/c%2Bd~%C3%A9");
        assert_eq!(uri_encode("a/b", false), "a%2Fb");
        assert_eq!(uri_decode("a%20b%2Fc"), "a b/c");
    }
}
