//! On-disk page layout.
//!
//! Every page is [`PAGE_SIZE`] bytes and starts with a 24-byte header:
//!
//! ```text
//!  0..4   CRC32C of bytes 4..PAGE_SIZE (stamped when written to disk)
//!  4      page type
//!  6..8   number of cells                  (leaf, internal)
//!  8..10  start of the cell content area   (leaf, internal)
//!         payload length                   (overflow)
//! 10..12  fragmented free bytes            (leaf, internal)
//! 12..16  next leaf / rightmost child / next overflow page / next free page
//! 16..20  previous leaf
//! ```
//!
//! Leaf and internal pages are slotted: a slot array of `u16` cell offsets
//! grows upward after the header, cell bodies grow downward from the end.
//!
//! * Leaf cell: `klen:u16, vlen:u32, key, value` – if the top bit of `vlen`
//!   is set the value lives in an overflow chain and the cell stores the id
//!   of its first page instead of the bytes.
//! * Internal cell: `klen:u16, child:u32, key` – `child` holds all keys
//!   smaller than `key`; keys at or above the last cell's key live under the
//!   rightmost child stored in the header.

pub const PAGE_SIZE: usize = 8192;
pub type PageBuf = [u8; PAGE_SIZE];

pub const HDR: usize = 24;
pub const T_META: u8 = 1;
pub const T_LEAF: u8 = 2;
pub const T_INTERNAL: u8 = 3;
pub const T_OVERFLOW: u8 = 4;
pub const T_FREE: u8 = 5;

/// Bytes available for slots and cells.
pub const CAPACITY: usize = PAGE_SIZE - HDR;
/// Values longer than this are stored in overflow pages.
pub const INLINE_MAX: usize = 2000;
/// Payload bytes per overflow page.
pub const OVERFLOW_CAP: usize = PAGE_SIZE - HDR;
const OVERFLOW_FLAG: u32 = 1 << 31;
/// Largest value length an engine accepts.
pub const MAX_VALUE_LEN: usize = (OVERFLOW_FLAG - 1) as usize;

const O_TYPE: usize = 4;
const O_NKEYS: usize = 6;
const O_CELL_START: usize = 8;
const O_FRAG: usize = 10;
const O_NEXT: usize = 12;
const O_PREV: usize = 16;

#[inline]
pub fn rd16(p: &[u8], off: usize) -> usize {
    u16::from_le_bytes([p[off], p[off + 1]]) as usize
}
#[inline]
pub fn wr16(p: &mut [u8], off: usize, v: usize) {
    p[off..off + 2].copy_from_slice(&(v as u16).to_le_bytes());
}
#[inline]
pub fn rd32(p: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([p[off], p[off + 1], p[off + 2], p[off + 3]])
}
#[inline]
pub fn wr32(p: &mut [u8], off: usize, v: u32) {
    p[off..off + 4].copy_from_slice(&v.to_le_bytes());
}
#[inline]
pub fn rd64(p: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(p[off..off + 8].try_into().unwrap())
}
#[inline]
pub fn wr64(p: &mut [u8], off: usize, v: u64) {
    p[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

pub fn new_page(ty: u8) -> PageBuf {
    let mut p = [0u8; PAGE_SIZE];
    init(&mut p, ty);
    p
}

pub fn init(p: &mut PageBuf, ty: u8) {
    p.fill(0);
    p[O_TYPE] = ty;
    // PAGE_SIZE does not fit the u16 field; 0 encodes "empty, starts at end".
    set_cell_start(p, PAGE_SIZE);
}

#[inline]
pub fn page_type(p: &PageBuf) -> u8 {
    p[O_TYPE]
}
#[inline]
pub fn nkeys(p: &PageBuf) -> usize {
    rd16(p, O_NKEYS)
}
#[inline]
fn cell_start(p: &PageBuf) -> usize {
    match rd16(p, O_CELL_START) {
        0 => PAGE_SIZE,
        v => v,
    }
}
#[inline]
fn set_cell_start(p: &mut PageBuf, v: usize) {
    wr16(p, O_CELL_START, if v == PAGE_SIZE { 0 } else { v });
}
#[inline]
pub fn next(p: &PageBuf) -> u32 {
    rd32(p, O_NEXT)
}
#[inline]
pub fn set_next(p: &mut PageBuf, v: u32) {
    wr32(p, O_NEXT, v)
}
#[inline]
pub fn prev(p: &PageBuf) -> u32 {
    rd32(p, O_PREV)
}
#[inline]
pub fn set_prev(p: &mut PageBuf, v: u32) {
    wr32(p, O_PREV, v)
}

pub fn checksum(p: &PageBuf) -> u32 {
    crc32c::crc32c(&p[4..])
}
pub fn stamp_checksum(p: &mut PageBuf) {
    let c = checksum(p);
    wr32(p, 0, c);
}
pub fn checksum_ok(p: &PageBuf) -> bool {
    rd32(p, 0) == checksum(p)
}

#[inline]
fn slot(p: &PageBuf, i: usize) -> usize {
    rd16(p, HDR + 2 * i)
}

/// Key of cell `i` (leaf or internal).
#[inline]
pub fn key(p: &PageBuf, i: usize) -> &[u8] {
    let off = slot(p, i);
    let klen = rd16(p, off);
    &p[off + 6..off + 6 + klen]
}

/// Child pointer of internal cell `i`.
#[inline]
pub fn child(p: &PageBuf, i: usize) -> u32 {
    rd32(p, slot(p, i) + 2)
}

pub fn set_child(p: &mut PageBuf, i: usize, c: u32) {
    let off = slot(p, i);
    wr32(p, off + 2, c);
}

pub enum ValRef<'a> {
    Inline(&'a [u8]),
    Overflow { len: usize, first: u32 },
}

/// Value of leaf cell `i`.
pub fn value(p: &PageBuf, i: usize) -> ValRef<'_> {
    let off = slot(p, i);
    let klen = rd16(p, off);
    let vlen = rd32(p, off + 2);
    let at = off + 6 + klen;
    if vlen & OVERFLOW_FLAG != 0 {
        ValRef::Overflow { len: (vlen & !OVERFLOW_FLAG) as usize, first: rd32(p, at) }
    } else {
        ValRef::Inline(&p[at..at + vlen as usize])
    }
}

fn cell_size_at(p: &PageBuf, off: usize) -> usize {
    let klen = rd16(p, off);
    if page_type(p) == T_LEAF {
        let vlen = rd32(p, off + 2);
        6 + klen + if vlen & OVERFLOW_FLAG != 0 { 4 } else { vlen as usize }
    } else {
        6 + klen
    }
}

/// Raw bytes of cell `i`.
pub fn raw_cell(p: &PageBuf, i: usize) -> &[u8] {
    let off = slot(p, i);
    &p[off..off + cell_size_at(p, off)]
}

/// Key stored in a raw cell.
pub fn cell_key(cell: &[u8]) -> &[u8] {
    let klen = rd16(cell, 0);
    &cell[6..6 + klen]
}

pub fn leaf_cell_inline(key: &[u8], val: &[u8]) -> Vec<u8> {
    let mut c = Vec::with_capacity(6 + key.len() + val.len());
    c.extend_from_slice(&(key.len() as u16).to_le_bytes());
    c.extend_from_slice(&(val.len() as u32).to_le_bytes());
    c.extend_from_slice(key);
    c.extend_from_slice(val);
    c
}

pub fn leaf_cell_overflow(key: &[u8], len: usize, first: u32) -> Vec<u8> {
    let mut c = Vec::with_capacity(10 + key.len());
    c.extend_from_slice(&(key.len() as u16).to_le_bytes());
    c.extend_from_slice(&(len as u32 | OVERFLOW_FLAG).to_le_bytes());
    c.extend_from_slice(key);
    c.extend_from_slice(&first.to_le_bytes());
    c
}

pub fn internal_cell(key: &[u8], child: u32) -> Vec<u8> {
    let mut c = Vec::with_capacity(6 + key.len());
    c.extend_from_slice(&(key.len() as u16).to_le_bytes());
    c.extend_from_slice(&child.to_le_bytes());
    c.extend_from_slice(key);
    c
}

/// Binary search for `k`. `Ok(i)` if cell `i` has exactly that key,
/// otherwise `Err(i)` with the index of the first cell with a larger key.
#[inline]
pub fn search(p: &PageBuf, k: &[u8]) -> Result<usize, usize> {
    let mut lo = 0;
    let mut hi = nkeys(p);
    while lo < hi {
        let mid = (lo + hi) / 2;
        match key(p, mid).cmp(k) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(mid),
        }
    }
    Err(lo)
}

/// Total free bytes, counting fragmented space.
pub fn free_total(p: &PageBuf) -> usize {
    cell_start(p) - HDR - 2 * nkeys(p) + rd16(p, O_FRAG)
}

/// Inserts a raw cell at slot index `i`. Returns false, leaving the page
/// untouched, if it does not fit.
pub fn insert_cell(p: &mut PageBuf, i: usize, cell: &[u8]) -> bool {
    let n = nkeys(p);
    let need = cell.len() + 2;
    if cell_start(p) - HDR - 2 * n < need {
        if free_total(p) < need {
            return false;
        }
        compact(p);
    }
    let start = cell_start(p) - cell.len();
    p[start..start + cell.len()].copy_from_slice(cell);
    set_cell_start(p, start);
    p.copy_within(HDR + 2 * i..HDR + 2 * n, HDR + 2 * i + 2);
    wr16(p, HDR + 2 * i, start);
    wr16(p, O_NKEYS, n + 1);
    true
}

pub fn remove_cell(p: &mut PageBuf, i: usize) {
    let n = nkeys(p);
    let off = slot(p, i);
    let size = cell_size_at(p, off);
    p.copy_within(HDR + 2 * (i + 1)..HDR + 2 * n, HDR + 2 * i);
    wr16(p, O_NKEYS, n - 1);
    if n == 1 {
        set_cell_start(p, PAGE_SIZE);
        wr16(p, O_FRAG, 0);
    } else if off == cell_start(p) {
        set_cell_start(p, off + size);
    } else {
        let frag = rd16(p, O_FRAG) + size;
        wr16(p, O_FRAG, frag);
    }
}

fn compact(p: &mut PageBuf) {
    let n = nkeys(p);
    let old = *p;
    let mut start = PAGE_SIZE;
    for i in 0..n {
        let cell = raw_cell(&old, i);
        start -= cell.len();
        p[start..start + cell.len()].copy_from_slice(cell);
        wr16(p, HDR + 2 * i, start);
    }
    set_cell_start(p, start);
    wr16(p, O_FRAG, 0);
}

/// Payload of an overflow page.
pub fn overflow_data(p: &PageBuf) -> &[u8] {
    let len = rd16(p, O_CELL_START).min(OVERFLOW_CAP);
    &p[HDR..HDR + len]
}

pub fn new_overflow(data: &[u8], next_page: u32) -> PageBuf {
    let mut p = [0u8; PAGE_SIZE];
    p[O_TYPE] = T_OVERFLOW;
    wr16(&mut p, O_CELL_START, data.len());
    set_next(&mut p, next_page);
    p[HDR..HDR + data.len()].copy_from_slice(data);
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slotted_page_roundtrip() {
        let mut p = new_page(T_LEAF);
        let mut keys: Vec<Vec<u8>> = (0..150u32).map(|i| format!("key{i:05}").into_bytes()).collect();
        for k in &keys {
            let i = search(&p, k).unwrap_err();
            assert!(insert_cell(&mut p, i, &leaf_cell_inline(k, &[7u8; 20])));
        }
        keys.sort();
        assert_eq!(nkeys(&p), keys.len());
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(key(&p, i), &k[..]);
        }
        // Remove every other cell, then refill: exercises fragmentation
        // accounting and compaction.
        for i in (0..keys.len()).rev().step_by(2) {
            remove_cell(&mut p, i);
        }
        let free = free_total(&p);
        let big = leaf_cell_inline(b"zzz", &vec![1u8; free - 2 - 9]);
        let n = nkeys(&p);
        assert!(insert_cell(&mut p, n, &big));
        assert_eq!(free_total(&p), 0);
        assert!(!insert_cell(&mut p, 0, &leaf_cell_inline(b"a", b"")));
        match value(&p, nkeys(&p) - 1) {
            ValRef::Inline(v) => assert_eq!(v.len(), free - 11),
            _ => panic!(),
        }
    }
}
