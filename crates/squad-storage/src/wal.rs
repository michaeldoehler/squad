//! Write-ahead log.
//!
//! The log is a sequence of segment files `wal/<seq>.wal`. Each segment
//! starts with a 16-byte header (magic, sequence number) followed by
//! records, one per write batch:
//!
//! ```text
//! len:u32  crc:u32  lsn:u64  payload[len]
//! payload = count:u32, then per op: kind:u8 (1 put, 0 delete),
//!           klen:u32, key, and for puts vlen:u32, value
//! ```
//!
//! `crc` is the CRC32C of the LSN and the payload. LSNs are consecutive
//! across segments, which lets recovery tell a clean continuation from a
//! torn tail or a stale file.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::vfs::{Vfs, VfsFile};
use crate::{Result, WriteBatch};

pub const SEG_MAGIC: &[u8; 8] = b"SQWAL001";
pub const SEG_HEADER: usize = 16;
const REC_HEADER: usize = 16;
/// Buffered bytes that trigger a write to the file even without a sync.
const BUF_LIMIT: usize = 1 << 20;

pub fn segment_name(seq: u64) -> String {
    format!("{seq:016x}.wal")
}

pub fn parse_segment_name(name: &str) -> Option<u64> {
    let hex = name.strip_suffix(".wal")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Creates segment `seq` with a durable header. The caller still has to
/// sync the directory.
pub fn create_segment(vfs: &dyn Vfs, dir: &Path, seq: u64) -> Result<Arc<dyn VfsFile>> {
    let file = vfs.create(&dir.join(segment_name(seq)))?;
    let mut hdr = [0u8; SEG_HEADER];
    hdr[..8].copy_from_slice(SEG_MAGIC);
    hdr[8..].copy_from_slice(&seq.to_le_bytes());
    file.write_at(&hdr, 0)?;
    file.sync()?;
    Ok(file)
}

pub fn encode_record(buf: &mut Vec<u8>, lsn: u64, batch: &WriteBatch) {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; 8]);
    buf.extend_from_slice(&lsn.to_le_bytes());
    buf.extend_from_slice(&(batch.ops.len() as u32).to_le_bytes());
    for (k, v) in &batch.ops {
        buf.push(v.is_some() as u8);
        buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
        buf.extend_from_slice(k);
        if let Some(v) = v {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
    }
    let len = (buf.len() - start - REC_HEADER) as u32;
    let crc = crc32c::crc32c(&buf[start + 8..]);
    buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
    buf[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

/// One decoded operation: key and, for puts, the value.
pub type Op<'a> = (&'a [u8], Option<&'a [u8]>);

fn decode_payload(mut p: &[u8]) -> Option<Vec<Op<'_>>> {
    fn take<'a>(p: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        if p.len() < n {
            return None;
        }
        let (a, b) = p.split_at(n);
        *p = b;
        Some(a)
    }
    fn u32_of(p: &mut &[u8]) -> Option<usize> {
        take(p, 4).map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
    }
    let count = u32_of(&mut p)?;
    let mut ops = Vec::with_capacity(count.min(1 << 16));
    for _ in 0..count {
        let kind = take(&mut p, 1)?[0];
        let klen = u32_of(&mut p)?;
        let key = take(&mut p, klen)?;
        let val = match kind {
            1 => {
                let vlen = u32_of(&mut p)?;
                Some(take(&mut p, vlen)?)
            }
            0 => None,
            _ => return None,
        };
        ops.push((key, val));
    }
    p.is_empty().then_some(ops)
}

/// Decodes the record at `off` of a segment image. Returns the LSN, the
/// operations and the offset of the next record, or `None` if the bytes at
/// `off` are not a complete, intact record.
pub fn read_record(data: &[u8], off: usize) -> Option<(u64, Vec<Op<'_>>, usize)> {
    let hdr = data.get(off..off + REC_HEADER)?;
    let len = u32::from_le_bytes(hdr[..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
    let lsn = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
    let end = off.checked_add(REC_HEADER)?.checked_add(len)?;
    let body = data.get(off + 8..end)?;
    if crc32c::crc32c(body) != crc {
        return None;
    }
    let ops = decode_payload(&body[8..])?;
    Some((lsn, ops, end))
}

/// Appender state, guarded by the engine's WAL mutex.
pub struct Wal {
    pub dir: PathBuf,
    cur: Arc<dyn VfsFile>,
    pub cur_seq: u64,
    cur_off: u64,
    buf: Vec<u8>,
    /// LSN of the most recently appended record.
    pub last_lsn: u64,
    /// Rotated-out segments that may still hold unsynced records.
    pending_sync: Vec<Arc<dyn VfsFile>>,
    /// Bytes appended since the last checkpoint rotation.
    pub bytes: u64,
    /// Write every record to the file immediately instead of at sync time.
    immediate: bool,
}

impl Wal {
    pub fn new(dir: PathBuf, cur: Arc<dyn VfsFile>, cur_seq: u64, last_lsn: u64, immediate: bool) -> Wal {
        Wal {
            dir,
            cur,
            cur_seq,
            cur_off: SEG_HEADER as u64,
            buf: Vec::new(),
            last_lsn,
            pending_sync: Vec::new(),
            bytes: 0,
            immediate,
        }
    }

    pub fn append(&mut self, batch: &WriteBatch) -> Result<u64> {
        let lsn = self.last_lsn + 1;
        let before = self.buf.len();
        encode_record(&mut self.buf, lsn, batch);
        self.bytes += (self.buf.len() - before) as u64;
        self.last_lsn = lsn;
        if self.immediate || self.buf.len() >= BUF_LIMIT {
            self.flush_buf()?;
        }
        Ok(lsn)
    }

    /// Hands buffered records to the operating system (no fsync).
    pub fn flush_buf(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.cur.write_at(&self.buf, self.cur_off)?;
            self.cur_off += self.buf.len() as u64;
            self.buf.clear();
        }
        Ok(())
    }

    /// Files that must be fsynced, oldest first, to make everything
    /// appended so far durable. Call after [`Wal::flush_buf`].
    pub fn files_to_sync(&mut self) -> Vec<Arc<dyn VfsFile>> {
        let mut files = std::mem::take(&mut self.pending_sync);
        files.push(self.cur.clone());
        files
    }

    /// Switches to a new, already created segment.
    pub fn rotate(&mut self, file: Arc<dyn VfsFile>, seq: u64) -> Result<u64> {
        self.flush_buf()?;
        let old = std::mem::replace(&mut self.cur, file);
        self.pending_sync.push(old);
        self.cur_seq = seq;
        self.cur_off = SEG_HEADER as u64;
        Ok(std::mem::take(&mut self.bytes))
    }

    /// Forgets rotated-out segments once a checkpoint has superseded them.
    pub fn forget_pending(&mut self) {
        // Checkpoints are serialized and rotate exactly once, so everything
        // queued here predates the checkpoint that just completed.
        self.pending_sync.clear();
    }
}
