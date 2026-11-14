//! Field access on JSON documents stored as text, without building a tree.
//!
//! The text is expected to be well-formed JSON as a database writes it
//! (for squad: the output form of `jsonb`). A top-level field's text is
//! what PostgreSQL's `->>` returns: a string field unescaped, `null` as
//! SQL NULL, anything else as the JSON text of the value, which for
//! normalized output is exactly the stored slice.

use std::borrow::Cow;

use crate::util::hash_bytes;

/// Hash of a top-level key, as entered into the bloom filter of a JSON
/// chunk.
pub fn json_key_hash(key: &[u8]) -> u64 {
    let mut buf = Vec::with_capacity(key.len() + 1);
    buf.push(1);
    buf.extend_from_slice(key);
    hash_bytes(&buf)
}

/// Hash of a top-level key with the text of its value.
pub fn json_pair_hash(key: &[u8], value: &[u8]) -> u64 {
    let mut buf = Vec::with_capacity(key.len() + value.len() + 2);
    buf.push(2);
    buf.extend_from_slice(key);
    buf.push(0);
    buf.extend_from_slice(value);
    hash_bytes(&buf)
}

fn skip_ws(s: &[u8], mut i: usize) -> usize {
    while i < s.len() && matches!(s[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// End (exclusive) of the string starting with the quote at `i`.
fn string_end(s: &[u8], mut i: usize) -> Option<usize> {
    i += 1;
    while i < s.len() {
        match s[i] {
            b'"' => return Some(i + 1),
            b'\\' => i += 2,
            _ => i += 1,
        }
    }
    None
}

/// End (exclusive) of the value starting at `i`.
fn value_end(s: &[u8], i: usize) -> Option<usize> {
    match *s.get(i)? {
        b'"' => string_end(s, i),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut j = i;
            while j < s.len() {
                match s[j] {
                    b'"' => {
                        j = string_end(s, j)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(j + 1);
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            None
        }
        _ => {
            let mut j = i;
            while j < s.len() && !matches!(s[j], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r') {
                j += 1;
            }
            (j > i).then_some(j)
        }
    }
}

fn hex(b: &[u8]) -> Option<u32> {
    std::str::from_utf8(b).ok().and_then(|h| u32::from_str_radix(h, 16).ok())
}

/// The content of a JSON string literal (with its quotes), unescaped.
fn unescape(lit: &[u8]) -> Option<Cow<'_, [u8]>> {
    let inner = lit.get(1..lit.len().checked_sub(1)?)?;
    if !inner.contains(&b'\\') {
        return Some(Cow::Borrowed(inner));
    }
    let mut out = Vec::with_capacity(inner.len());
    let mut i = 0;
    while i < inner.len() {
        let c = inner[i];
        if c != b'\\' {
            out.push(c);
            i += 1;
            continue;
        }
        let e = *inner.get(i + 1)?;
        i += 2;
        match e {
            b'"' => out.push(b'"'),
            b'\\' => out.push(b'\\'),
            b'/' => out.push(b'/'),
            b'b' => out.push(8),
            b'f' => out.push(12),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'u' => {
                let mut cp = hex(inner.get(i..i + 4)?)?;
                i += 4;
                if (0xD800..0xDC00).contains(&cp) && inner.get(i..i + 2) == Some(b"\\u") {
                    let low = hex(inner.get(i + 2..i + 6)?)?;
                    if (0xDC00..0xE000).contains(&low) {
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                        i += 6;
                    }
                }
                let ch = char::from_u32(cp)?;
                let mut buf = [0u8; 4];
                out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            }
            _ => return None,
        }
    }
    Some(Cow::Owned(out))
}

/// Calls `f(key, text)` for every top-level field of an object; `text`
/// is `None` for a JSON null. Returns false if the document is not an
/// object or not well-formed (fields seen before the problem were
/// reported).
pub fn for_each_field(doc: &[u8], mut f: impl FnMut(&[u8], Option<&[u8]>)) -> bool {
    let mut i = skip_ws(doc, 0);
    if doc.get(i) != Some(&b'{') {
        return false;
    }
    i = skip_ws(doc, i + 1);
    if doc.get(i) == Some(&b'}') {
        return true;
    }
    loop {
        if doc.get(i) != Some(&b'"') {
            return false;
        }
        let Some(kend) = string_end(doc, i) else { return false };
        let Some(key) = unescape(&doc[i..kend]) else { return false };
        i = skip_ws(doc, kend);
        if doc.get(i) != Some(&b':') {
            return false;
        }
        i = skip_ws(doc, i + 1);
        let Some(vend) = value_end(doc, i) else { return false };
        let raw = &doc[i..vend];
        match raw {
            b"null" => f(&key, None),
            [b'"', ..] => match unescape(raw) {
                Some(v) => f(&key, Some(&v)),
                None => return false,
            },
            _ => f(&key, Some(raw)),
        }
        i = skip_ws(doc, vend);
        match doc.get(i) {
            Some(b',') => i = skip_ws(doc, i + 1),
            Some(b'}') => return true,
            _ => return false,
        }
    }
}

/// The text of top-level field `key` (`doc ->> key`), or `None` if the
/// document is not an object, lacks the key or holds a JSON null there.
pub fn field_text<'a>(doc: &'a [u8], key: &[u8]) -> Option<Cow<'a, [u8]>> {
    let mut i = skip_ws(doc, 0);
    if doc.get(i) != Some(&b'{') {
        return None;
    }
    i = skip_ws(doc, i + 1);
    if doc.get(i) == Some(&b'}') {
        return None;
    }
    let mut found = None;
    loop {
        if doc.get(i) != Some(&b'"') {
            return found;
        }
        let kend = string_end(doc, i)?;
        let lit = &doc[i..kend];
        // Plain keys compare without unescaping.
        let matches = if lit.contains(&b'\\') {
            unescape(lit).is_some_and(|k| *k == *key)
        } else {
            &lit[1..lit.len() - 1] == key
        };
        i = skip_ws(doc, kend);
        if doc.get(i) != Some(&b':') {
            return found;
        }
        i = skip_ws(doc, i + 1);
        let vend = value_end(doc, i)?;
        if matches {
            let raw = &doc[i..vend];
            // With duplicate keys the last one counts, as for `jsonb`.
            found = match raw {
                b"null" => None,
                [b'"', ..] => unescape(raw),
                _ => Some(Cow::Borrowed(raw)),
            };
        }
        i = skip_ws(doc, vend);
        match doc.get(i) {
            Some(b',') => i = skip_ws(doc, i + 1),
            _ => return found,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(doc: &str, key: &str) -> Option<String> {
        field_text(doc.as_bytes(), key.as_bytes()).map(|c| String::from_utf8(c.into_owned()).unwrap())
    }

    #[test]
    fn fields() {
        let doc = r#"{"a": 1, "s": "x\"yü😀", "n": null, "o": {"k": [1, "}"]}, "b": true, "e": ""}"#;
        assert_eq!(text(doc, "a").as_deref(), Some("1"));
        assert_eq!(text(doc, "s").as_deref(), Some("x\"yü😀"));
        assert_eq!(text(doc, "n"), None);
        assert_eq!(text(doc, "o").as_deref(), Some(r#"{"k": [1, "}"]}"#));
        assert_eq!(text(doc, "b").as_deref(), Some("true"));
        assert_eq!(text(doc, "e").as_deref(), Some(""));
        assert_eq!(text(doc, "missing"), None);
        assert_eq!(text("[1, 2]", "a"), None);
        assert_eq!(text("\"str\"", "a"), None);
        assert_eq!(text("{}", "a"), None);
        assert_eq!(text(r#"{"k\"q": 5}"#, "k\"q").as_deref(), Some("5"));
        let mut seen = Vec::new();
        assert!(for_each_field(doc.as_bytes(), |k, v| seen.push((k.to_vec(), v.map(<[u8]>::to_vec)))));
        assert_eq!(seen.len(), 6);
        assert_eq!(seen[2], (b"n".to_vec(), None));
    }
}
