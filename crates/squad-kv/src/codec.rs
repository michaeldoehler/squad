//! Encoding of versioned keys and values handed to the engine.
//!
//! ```text
//! data key  = 0x01, escaped(user key), 0x00 0x01, !commit_ts (8 bytes BE)
//! meta key  = 0x00, name
//! ```
//!
//! Inside the escaped user key every `0x00` byte becomes `0x00 0xFF`, so the
//! terminator `0x00 0x01` sorts below any continuation and the byte order of
//! encoded keys equals the order of `(user key asc, commit_ts desc)`.

pub(crate) const META_LAST_TS: &[u8] = b"\x00last_ts";
/// Never stored. A read replica deletes it to keep a batch that became
/// empty from being skipped by the engine.
pub(crate) const META_REPLAY_PAD: &[u8] = b"\x00replay_pad";
pub(crate) const DATA_LO: &[u8] = b"\x01";
pub(crate) const DATA_HI: &[u8] = b"\x02";

fn push_escaped(out: &mut Vec<u8>, key: &[u8]) {
    out.push(0x01);
    for &b in key {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.push(0x00);
}

/// Smallest encoded key belonging to `key` (below all of its versions).
pub(crate) fn key_start(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 12);
    push_escaped(&mut out, key);
    out.push(0x01);
    out
}

/// Smallest encoded key above all versions of `key`.
pub(crate) fn key_end(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 4);
    push_escaped(&mut out, key);
    out.push(0x02);
    out
}

pub(crate) fn encode(key: &[u8], ts: u64) -> Vec<u8> {
    let mut out = key_start(key);
    out.extend_from_slice(&(!ts).to_be_bytes());
    out
}

/// Splits an encoded data key into user key and commit timestamp.
pub(crate) fn decode(enc: &[u8]) -> Option<(Vec<u8>, u64)> {
    if enc.len() < 11 || enc[0] != 0x01 {
        return None;
    }
    let (body, ts) = enc.split_at(enc.len() - 8);
    let body = &body[1..body.len() - 2];
    let mut key = Vec::with_capacity(body.len());
    let mut i = 0;
    while i < body.len() {
        key.push(body[i]);
        i += if body[i] == 0 { 2 } else { 1 };
    }
    Some((key, !u64::from_be_bytes(ts.try_into().ok()?)))
}

/// True if `enc` is a version of the user key whose `key_start` is `start`.
pub(crate) fn same_key(enc: &[u8], start: &[u8]) -> bool {
    enc.len() == start.len() + 8 && enc.starts_with(start)
}

pub(crate) fn encode_value(v: Option<&[u8]>) -> Vec<u8> {
    match v {
        None => vec![0],
        Some(v) => {
            let mut out = Vec::with_capacity(v.len() + 1);
            out.push(1);
            out.extend_from_slice(v);
            out
        }
    }
}

/// `None` for a tombstone.
pub(crate) fn decode_value(mut v: Vec<u8>) -> Option<Vec<u8>> {
    if v.first() == Some(&1) {
        v.remove(0);
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_is_preserved() {
        let keys: [&[u8]; 6] = [b"", b"\x00", b"\x00\x00", b"\x00\x01", b"a", b"a\x00"];
        for a in keys {
            for b in keys {
                assert_eq!(a.cmp(b), key_start(a).cmp(&key_start(b)), "{a:?} {b:?}");
                assert!(encode(a, 5) < encode(a, 4));
                assert!(encode(a, u64::MAX) >= key_start(a));
                assert!(encode(a, 0) < key_end(a));
                if a < b {
                    assert!(key_end(a) <= key_start(b));
                }
            }
            assert_eq!(decode(&encode(a, 77)), Some((a.to_vec(), 77)));
        }
    }
}
