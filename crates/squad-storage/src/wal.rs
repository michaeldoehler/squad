//! Write-ahead log.
//!
//! The log is a sequence of segment files `wal/<seq>.wal`. Each segment
//! starts with a 16-byte header (magic, sequence number) followed by
//! records, one per write batch:
//!
//! ```text
//! len:u32  crc:u32  lsn:u64  synced:u64  payload[len]
//! payload = count:u32, then per op: kind:u8 (1 put, 0 delete),
//!           klen:u32, key, and for puts vlen:u32, value
//! ```
//!
//! `crc` is the CRC32C of `lsn`, `synced` and the payload. LSNs are
//! consecutive across segments, which lets recovery tell a clean
//! continuation from a torn tail or a stale file.
//!
//! `synced` is the highest LSN that was known to be on stable storage
//! (fsynced, or covered by a checkpoint) when the record was appended.
//! Recovery uses it to tell a torn tail from corruption: a crash can only
//! damage records that were not yet synced, so a damaged or missing
//! record `n` followed by an intact record that says `synced >= n` was
//! damaged after it had been made durable. Log objects in an object
//! store use the same record format with `synced` set to zero.
//!
//! A record's payload is limited to `u32::MAX` bytes; larger write
//! batches are refused.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::vfs::{Vfs, VfsFile};
use crate::{Result, StorageError, WriteBatch};

pub const SEG_MAGIC: &[u8; 8] = b"SQWAL002";
/// Magic of segments written before the `synced` field existed.
pub const SEG_MAGIC_V1: &[u8; 8] = b"SQWAL001";
pub const SEG_HEADER: usize = 16;
const REC_HEADER: usize = 24;
/// Largest payload a record can carry.
pub const MAX_RECORD_PAYLOAD: u64 = u32::MAX as u64;
/// Buffered bytes that trigger a write to the file even without a sync.
const BUF_LIMIT: usize = 1 << 20;
/// Segments grow in zero-filled steps of this size. Appending inside
/// space that already exists does not change the file's metadata, so the
/// fsync of a commit only has to write the record itself; extending a
/// file on every commit would make each fsync wait for a file system
/// journal commit. A zero-filled tail never parses as a record.
const PREALLOC: u64 = 256 << 10;

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
    file.write_at(&vec![0u8; PREALLOC as usize - SEG_HEADER], SEG_HEADER as u64)?;
    file.sync()?;
    Ok(file)
}

/// Size of the payload of `batch`'s record.
pub fn payload_len(batch: &WriteBatch) -> u64 {
    4 + batch.ops.iter().map(|(k, v)| 5 + k.len() as u64 + v.as_ref().map_or(0, |v| 4 + v.len() as u64)).sum::<u64>()
}

/// Appends the record of `batch` to `buf`, with `synced` zero.
pub fn encode_record(buf: &mut Vec<u8>, lsn: u64, batch: &WriteBatch) -> Result<()> {
    encode_record_synced(buf, lsn, 0, batch)
}

/// Appends the record of `batch` to `buf`. Fails, leaving `buf`
/// untouched, if the payload would exceed [`MAX_RECORD_PAYLOAD`].
pub fn encode_record_synced(buf: &mut Vec<u8>, lsn: u64, synced: u64, batch: &WriteBatch) -> Result<()> {
    let len = payload_len(batch);
    if len > MAX_RECORD_PAYLOAD {
        return Err(StorageError::Invalid(format!(
            "a write batch of {len} bytes exceeds the log record limit of {MAX_RECORD_PAYLOAD} bytes"
        )));
    }
    let start = buf.len();
    buf.reserve(REC_HEADER + len as usize);
    buf.extend_from_slice(&(len as u32).to_le_bytes());
    buf.extend_from_slice(&[0u8; 4]);
    buf.extend_from_slice(&lsn.to_le_bytes());
    buf.extend_from_slice(&synced.to_le_bytes());
    // `len` bounds the count and every length below.
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
    debug_assert_eq!((buf.len() - start - REC_HEADER) as u64, len);
    let crc = crc32c::crc32c(&buf[start + 8..]);
    buf[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
    Ok(())
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

/// A decoded record.
pub struct Record<'a> {
    pub lsn: u64,
    /// Highest LSN known to be durable when the record was appended.
    pub synced: u64,
    pub ops: Vec<Op<'a>>,
    /// Offset of the byte after the record.
    pub end: usize,
}

/// Decodes the record at `off` of a segment image or log object, or
/// returns `None` if the bytes at `off` are not a complete, intact record.
pub fn decode_record(data: &[u8], off: usize) -> Option<Record<'_>> {
    let hdr = data.get(off..off.checked_add(REC_HEADER)?)?;
    let len = u32::from_le_bytes(hdr[..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
    let lsn = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
    let synced = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
    let end = off.checked_add(REC_HEADER)?.checked_add(len)?;
    let body = data.get(off + 8..end)?;
    if crc32c::crc32c(body) != crc {
        return None;
    }
    let ops = decode_payload(&body[16..])?;
    Some(Record { lsn, synced, ops, end })
}

/// Decodes the record at `off` of a segment image. Returns the LSN, the
/// operations and the offset of the next record, or `None` if the bytes at
/// `off` are not a complete, intact record.
pub fn read_record(data: &[u8], off: usize) -> Option<(u64, Vec<Op<'_>>, usize)> {
    decode_record(data, off).map(|r| (r.lsn, r.ops, r.end))
}

/// After damage at `off`, the offset of the next intact record whose
/// LSN lies above `after`, if there is one. Candidates are filtered by
/// their LSN field before the checksum is computed, so scanning a
/// zero-filled tail is cheap.
pub fn resync(data: &[u8], off: usize, after: u64) -> Option<usize> {
    let mut pos = off.checked_add(1)?;
    while pos + REC_HEADER <= data.len() {
        let lsn = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let plausible = lsn > after && lsn - after <= 1 << 40 && len <= data.len() - pos - REC_HEADER;
        if plausible && decode_record(data, pos).is_some() {
            return Some(pos);
        }
        pos += 1;
    }
    None
}

/// Appender state, guarded by the engine's WAL mutex.
pub struct Wal {
    pub dir: PathBuf,
    cur: Arc<dyn VfsFile>,
    pub cur_seq: u64,
    cur_off: u64,
    /// Length of the current segment file; everything from `cur_off` up
    /// to here is zeros.
    allocated: u64,
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
            allocated: PREALLOC,
            buf: Vec::new(),
            last_lsn,
            pending_sync: Vec::new(),
            bytes: 0,
            immediate,
        }
    }

    /// Appends the record of `batch`. `synced` is the highest LSN that
    /// is on stable storage now.
    pub fn append(&mut self, batch: &WriteBatch, synced: u64) -> Result<u64> {
        let lsn = self.last_lsn + 1;
        let before = self.buf.len();
        encode_record_synced(&mut self.buf, lsn, synced, batch)?;
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
            let end = self.cur_off + self.buf.len() as u64;
            if end > self.allocated {
                // Extend ahead of the write position.
                let new_len = end + PREALLOC;
                self.cur.write_at(&vec![0u8; (new_len - end) as usize], end)?;
                self.allocated = new_len;
            }
            self.cur.write_at(&self.buf, self.cur_off)?;
            self.cur_off = end;
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
        self.allocated = PREALLOC;
        Ok(std::mem::take(&mut self.bytes))
    }

    /// Forgets rotated-out segments once a checkpoint has superseded them.
    pub fn forget_pending(&mut self) {
        // Checkpoints are serialized and rotate exactly once, so everything
        // queued here predates the checkpoint that just completed.
        self.pending_sync.clear();
    }
}
