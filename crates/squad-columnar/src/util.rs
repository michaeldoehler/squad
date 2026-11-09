//! Byte-level helpers: little-endian reader/writer, bit packing, hashing.

use crate::types::Datum;
use crate::{Error, Result};

pub(crate) fn corrupt(what: &str) -> Error {
    Error::Corrupt(what.to_string())
}

/// Bounds-checked reader over a byte slice.
pub(crate) struct Cur<'a> {
    buf: &'a [u8],
}

impl<'a> Cur<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Cur<'a> {
        Cur { buf }
    }
    pub(crate) fn remaining(&self) -> usize {
        self.buf.len()
    }
    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.buf.len() < n {
            return Err(corrupt("unexpected end of data"));
        }
        let (a, b) = self.buf.split_at(n);
        self.buf = b;
        Ok(a)
    }
    pub(crate) fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.buf)
    }
    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }
    pub(crate) fn i128(&mut self) -> Result<i128> {
        Ok(i128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub(crate) fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    pub(crate) fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| corrupt("invalid UTF-8 in metadata"))
    }
    pub(crate) fn datum(&mut self) -> Result<Datum> {
        Ok(match self.u8()? {
            0 => Datum::Null,
            1 => Datum::Bool(self.u8()? != 0),
            2 => Datum::Int(self.i64()?),
            3 => Datum::Float(f64::from_bits(self.u64()?)),
            4 => Datum::Decimal(self.i128()?),
            5 => Datum::Bytes(self.bytes()?.to_vec()),
            _ => return Err(corrupt("unknown value tag")),
        })
    }
}

pub(crate) trait Put {
    fn put_u8(&mut self, v: u8);
    fn put_u32(&mut self, v: u32);
    fn put_u64(&mut self, v: u64);
    fn put_bytes(&mut self, v: &[u8]);
    fn put_datum(&mut self, v: &Datum);
}

impl Put for Vec<u8> {
    fn put_u8(&mut self, v: u8) {
        self.push(v);
    }
    fn put_u32(&mut self, v: u32) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn put_u64(&mut self, v: u64) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn put_bytes(&mut self, v: &[u8]) {
        self.put_u32(v.len() as u32);
        self.extend_from_slice(v);
    }
    fn put_datum(&mut self, v: &Datum) {
        match v {
            Datum::Null => self.push(0),
            Datum::Bool(b) => {
                self.push(1);
                self.push(*b as u8);
            }
            Datum::Int(i) => {
                self.push(2);
                self.put_u64(*i as u64);
            }
            Datum::Float(f) => {
                self.push(3);
                self.put_u64(f.to_bits());
            }
            Datum::Decimal(d) => {
                self.push(4);
                self.extend_from_slice(&d.to_le_bytes());
            }
            Datum::Bytes(b) => {
                self.push(5);
                self.put_bytes(b);
            }
        }
    }
}

/// Number of bits needed to represent `v`.
pub(crate) fn bits_for(v: u64) -> u8 {
    (64 - v.leading_zeros()) as u8
}

/// Appends `values` using `width` bits each, followed by eight bytes of
/// padding so that the reader can load whole words.
pub(crate) fn pack(values: impl Iterator<Item = u64>, width: u8, out: &mut Vec<u8>) {
    if width > 0 {
        let mut acc: u128 = 0;
        let mut bits = 0u32;
        for v in values {
            acc |= (v as u128) << bits;
            bits += width as u32;
            while bits >= 8 {
                out.push(acc as u8);
                acc >>= 8;
                bits -= 8;
            }
        }
        if bits > 0 {
            out.push(acc as u8);
        }
    }
    out.extend_from_slice(&[0u8; 8]);
}

/// Size in bytes of `n` packed values including the padding.
pub(crate) fn packed_len(n: usize, width: u8) -> usize {
    (n * width as usize).div_ceil(8) + 8
}

/// Reads `n` values of `width` bits written by [`pack`].
pub(crate) fn unpack(cur: &mut Cur, width: u8, n: usize) -> Result<Vec<u64>> {
    if width > 64 {
        return Err(corrupt("bit width out of range"));
    }
    let data = cur.take(packed_len(n, width))?;
    if width == 0 {
        return Ok(vec![0; n]);
    }
    let mut out = Vec::with_capacity(n);
    let w = width as usize;
    if width <= 56 {
        let mask = (1u64 << width) - 1;
        for i in 0..n {
            let bit = i * w;
            let word = u64::from_le_bytes(data[bit / 8..bit / 8 + 8].try_into().unwrap());
            out.push((word >> (bit % 8)) & mask);
        }
    } else {
        let mask = if width == 64 { u64::MAX as u128 } else { (1u128 << width) - 1 };
        for i in 0..n {
            let bit = i * w;
            let mut buf = [0u8; 16];
            let start = bit / 8;
            let end = (start + 16).min(data.len());
            buf[..end - start].copy_from_slice(&data[start..end]);
            out.push(((u128::from_le_bytes(buf) >> (bit % 8)) & mask) as u64);
        }
    }
    Ok(out)
}

/// 64-bit hash with good avalanche, for bloom filters, sketches and hash
/// tables keyed by bytes.
#[inline]
pub fn hash_bytes(data: &[u8]) -> u64 {
    let mut h: u64 = 0x9E37_79B9_7F4A_7C15 ^ (data.len() as u64).wrapping_mul(0xA24B_AED4_963E_E407);
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        let v = u64::from_le_bytes(c.try_into().unwrap());
        h = (h ^ v).wrapping_mul(0x9FB2_1C65_1E98_DF25);
        h ^= h >> 29;
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut buf = [0u8; 8];
        buf[..rem.len()].copy_from_slice(rem);
        h = (h ^ u64::from_le_bytes(buf)).wrapping_mul(0x9FB2_1C65_1E98_DF25);
    }
    mix(h)
}

#[inline]
pub fn hash_u64(v: u64) -> u64 {
    mix(v.wrapping_add(0x9E37_79B9_7F4A_7C15))
}

#[inline]
fn mix(mut h: u64) -> u64 {
    h ^= h >> 32;
    h = h.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    h ^= h >> 32;
    h = h.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    h ^ (h >> 32)
}

/// Hasher for integer-keyed hash maps: the keys are already well mixed
/// by [`hash_u64`] or are small dense ids.
#[derive(Default, Clone, Copy)]
pub struct IntHasher(u64);

impl std::hash::Hasher for IntHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0 = hash_bytes(bytes) ^ self.0.rotate_left(17);
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = hash_u64(v ^ self.0.rotate_left(17));
    }
    fn write_i64(&mut self, v: i64) {
        self.write_u64(v as u64);
    }
    fn write_u32(&mut self, v: u32) {
        self.write_u64(v as u64);
    }
    fn write_u128(&mut self, v: u128) {
        self.write_u64(v as u64);
        self.write_u64((v >> 64) as u64);
    }
    fn write_i128(&mut self, v: i128) {
        self.write_u128(v as u128);
    }
    fn write_usize(&mut self, v: usize) {
        self.write_u64(v as u64);
    }
    fn write_u8(&mut self, v: u8) {
        self.write_u64(v as u64);
    }
}

pub(crate) type FastMap<K, V> = std::collections::HashMap<K, V, std::hash::BuildHasherDefault<IntHasher>>;
pub(crate) type FastSet<K> = std::collections::HashSet<K, std::hash::BuildHasherDefault<IntHasher>>;
