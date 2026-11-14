//! The durable engine: B+tree + buffer pool + write-ahead log.
//!
//! # Durability model
//!
//! * `data.db` only ever changes during a checkpoint and always holds the
//!   complete state as of the checkpoint LSN recorded in its header page.
//! * Every write batch is appended to the WAL as one logical redo record
//!   and applied to the in-memory tree. Dirty pages stay in memory until
//!   the next checkpoint (no-steal).
//! * A checkpoint snapshots the dirty pages, writes them to a checksummed
//!   double-write journal, fsyncs it, copies them into `data.db`, fsyncs
//!   that, and only then deletes the journal and the WAL segments it
//!   supersedes. A crash at any point leaves either the old state plus its
//!   WAL, or a complete journal that recovery re-applies.
//! * Recovery re-applies a complete journal, then replays WAL records with
//!   consecutive LSNs above the checkpoint LSN and stops at the first gap
//!   or damaged record. Such a stop is a torn tail if every intact record
//!   after it was appended before the missing record was synced; then
//!   nothing that was ever acknowledged as durable is lost and the rest
//!   is discarded. If a later intact record says the missing one had
//!   already been synced (see the `synced` field in [`crate::wal`]), the
//!   log was damaged after the fact and the open fails with
//!   [`StorageError::Corruption`] instead of dropping committed data.

use std::fmt;
use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLock};

use crate::btree::{self, Meta, Writer};
use crate::page::{self, PageBuf, HDR, PAGE_SIZE, T_FREE, T_INTERNAL, T_LEAF, T_META, T_OVERFLOW};
use crate::pool::Pool;
use crate::vfs::{RealVfs, Vfs, VfsFile, VfsLock};
use crate::wal::{self, Wal};
use crate::{Engine, EngineStats, Result, StorageError, SyncMode, VerifyReport, WriteBatch, MAX_KEY_LEN};

const DATA_FILE: &str = "data.db";
const DATA_TMP: &str = "data.db.tmp";
const JOURNAL_FILE: &str = "checkpoint.journal";
const LOCK_FILE: &str = "LOCK";
const WAL_DIR: &str = "wal";

const DB_MAGIC: &[u8; 8] = b"SQUADDB1";
const FORMAT_VERSION: u32 = 1;
const JOURNAL_MAGIC: &[u8; 8] = b"SQJRNL01";
const JOURNAL_END: &[u8; 4] = b"JEND";
const JOURNAL_HEADER: usize = 24;
const JOURNAL_ENTRY: usize = 4 + PAGE_SIZE;
const IO_CHUNK: usize = 1 << 20;

/// Configuration of a [`DiskEngine`].
#[derive(Clone)]
pub struct DiskOptions {
    /// When commits are forced to stable storage.
    pub sync_mode: SyncMode,
    /// Target size of the page cache, in 8 KiB pages. Dirty pages are never
    /// evicted, so the cache can temporarily exceed this between
    /// checkpoints.
    pub cache_pages: usize,
    /// A checkpoint is triggered once the WAL grows beyond this many bytes.
    pub checkpoint_wal_bytes: u64,
    /// File system to use.
    pub vfs: Arc<dyn Vfs>,
}

impl Default for DiskOptions {
    fn default() -> Self {
        DiskOptions {
            sync_mode: SyncMode::default(),
            cache_pages: 16 * 1024,
            checkpoint_wal_bytes: 64 << 20,
            vfs: Arc::new(RealVfs),
        }
    }
}

impl fmt::Debug for DiskOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiskOptions")
            .field("sync_mode", &self.sync_mode)
            .field("cache_pages", &self.cache_pages)
            .field("checkpoint_wal_bytes", &self.checkpoint_wal_bytes)
            .finish_non_exhaustive()
    }
}

fn header_page(meta: &Meta, lsn: u64) -> PageBuf {
    let mut p = page::new_page(T_META);
    p[HDR..HDR + 8].copy_from_slice(DB_MAGIC);
    page::wr32(&mut p, HDR + 8, FORMAT_VERSION);
    page::wr32(&mut p, HDR + 12, meta.root);
    page::wr32(&mut p, HDR + 16, meta.free_head);
    page::wr32(&mut p, HDR + 20, meta.page_count);
    page::wr64(&mut p, HDR + 24, lsn);
    page::stamp_checksum(&mut p);
    p
}

fn parse_header(p: &PageBuf) -> Result<(Meta, u64)> {
    let bad = |m: &str| StorageError::Corruption(format!("data file header: {m}"));
    if !page::checksum_ok(p) {
        return Err(bad("checksum mismatch"));
    }
    if page::page_type(p) != T_META || &p[HDR..HDR + 8] != DB_MAGIC {
        return Err(bad("not a squad data file"));
    }
    let version = page::rd32(p, HDR + 8);
    if version != FORMAT_VERSION {
        return Err(bad(&format!("unsupported format version {version}")));
    }
    let meta =
        Meta { root: page::rd32(p, HDR + 12), free_head: page::rd32(p, HDR + 16), page_count: page::rd32(p, HDR + 20) };
    if meta.root == 0 || meta.root >= meta.page_count || meta.free_head >= meta.page_count {
        return Err(bad("page pointers out of range"));
    }
    Ok((meta, page::rd64(p, HDR + 24)))
}

fn read_all(file: &dyn VfsFile) -> Result<Vec<u8>> {
    let size = file.size()? as usize;
    let mut buf = vec![0u8; size];
    let n = file.read_at(&mut buf, 0)?;
    buf.truncate(n);
    Ok(buf)
}

fn stamped(p: &PageBuf) -> PageBuf {
    let mut c = *p;
    page::stamp_checksum(&mut c);
    c
}

/// Writes page images into the data file, coalescing runs of adjacent
/// pages into single writes. `pages` must be sorted by page id.
fn apply_pages<'a>(data: &dyn VfsFile, pages: impl Iterator<Item = (u32, &'a PageBuf)>) -> Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut start = 0u32;
    let mut next = 0u32;
    for (pid, p) in pages {
        if !buf.is_empty() && (pid != next || buf.len() >= IO_CHUNK) {
            data.write_at(&buf, start as u64 * PAGE_SIZE as u64)?;
            buf.clear();
        }
        if buf.is_empty() {
            start = pid;
        }
        buf.extend_from_slice(p);
        next = pid + 1;
    }
    if !buf.is_empty() {
        data.write_at(&buf, start as u64 * PAGE_SIZE as u64)?;
    }
    Ok(())
}

/// Crash-atomically installs `pages` and a new header into the data file.
fn write_checkpoint(
    vfs: &dyn Vfs,
    dir: &Path,
    data: &dyn VfsFile,
    pages: &[(u32, Arc<PageBuf>)],
    meta: Meta,
    lsn: u64,
    fsyncs: &AtomicU64,
) -> Result<()> {
    let header = header_page(&meta, lsn);
    let images = || std::iter::once((0u32, header)).chain(pages.iter().map(|(pid, p)| (*pid, stamped(p))));

    // 1. Journal: a self-contained, checksummed copy of everything we are
    //    about to overwrite in place.
    let jpath = dir.join(JOURNAL_FILE);
    let journal = vfs.create(&jpath)?;
    let mut buf =
        Vec::with_capacity(IO_CHUNK.min(JOURNAL_HEADER + (pages.len() + 1) * JOURNAL_ENTRY + 8) + JOURNAL_ENTRY);
    buf.extend_from_slice(JOURNAL_MAGIC);
    buf.extend_from_slice(&lsn.to_le_bytes());
    buf.extend_from_slice(&(pages.len() as u32 + 1).to_le_bytes());
    buf.extend_from_slice(&[0u8; 4]);
    let mut crc = 0u32;
    let mut off = 0u64;
    let mut flush = |buf: &mut Vec<u8>, crc: &mut u32| -> Result<()> {
        *crc = crc32c::crc32c_append(*crc, buf);
        journal.write_at(buf, off)?;
        off += buf.len() as u64;
        buf.clear();
        Ok(())
    };
    for (pid, p) in images() {
        buf.extend_from_slice(&pid.to_le_bytes());
        buf.extend_from_slice(&p);
        if buf.len() >= IO_CHUNK {
            flush(&mut buf, &mut crc)?;
        }
    }
    flush(&mut buf, &mut crc)?;
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(JOURNAL_END);
    journal.write_at(&buf, off)?;
    journal.sync()?;
    vfs.sync_dir(dir)?;

    // 2. Data file, in place. A torn write here is repaired from the journal.
    let mut tmp: Vec<(u32, PageBuf)> = Vec::with_capacity(128);
    let mut it = images().peekable();
    while it.peek().is_some() {
        tmp.clear();
        tmp.extend(it.by_ref().take(128));
        apply_pages(data, tmp.iter().map(|(pid, p)| (*pid, p)))?;
    }
    data.sync()?;

    // 3. The journal is no longer needed.
    vfs.remove(&jpath)?;
    vfs.sync_dir(dir)?;
    fsyncs.fetch_add(4, Ordering::Relaxed);
    Ok(())
}

/// Finishes or discards a checkpoint that was interrupted by a crash.
fn recover_journal(vfs: &dyn Vfs, dir: &Path) -> Result<()> {
    let jpath = dir.join(JOURNAL_FILE);
    if !vfs.exists(&jpath)? {
        return Ok(());
    }
    let bytes = read_all(&*vfs.open(&jpath)?)?;
    let valid = (|| {
        if bytes.len() < JOURNAL_HEADER + 8 || &bytes[..8] != JOURNAL_MAGIC {
            return None;
        }
        let n = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let body = JOURNAL_HEADER + n.checked_mul(JOURNAL_ENTRY)?;
        if bytes.len() != body + 8 || &bytes[body + 4..] != JOURNAL_END {
            return None;
        }
        let crc = u32::from_le_bytes(bytes[body..body + 4].try_into().unwrap());
        (crc32c::crc32c(&bytes[..body]) == crc).then_some(n)
    })();
    if let Some(n) = valid {
        let data_path = dir.join(DATA_FILE);
        if !vfs.exists(&data_path)? {
            return Err(StorageError::Corruption("checkpoint journal present but data file missing".into()));
        }
        let data = vfs.open(&data_path)?;
        let entries = (0..n).map(|i| {
            let at = JOURNAL_HEADER + i * JOURNAL_ENTRY;
            let pid = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
            let p: &PageBuf = bytes[at + 4..at + JOURNAL_ENTRY].try_into().unwrap();
            (pid, p)
        });
        apply_pages(&*data, entries)?;
        data.sync()?;
    }
    vfs.remove(&jpath)?;
    vfs.sync_dir(dir)?;
    Ok(())
}

/// Creates an empty database: header page plus an empty root leaf.
fn create_fresh(vfs: &dyn Vfs, dir: &Path) -> Result<()> {
    let tmp = dir.join(DATA_TMP);
    let file = vfs.create(&tmp)?;
    let meta = Meta { root: 1, free_head: 0, page_count: 2 };
    let mut buf = Vec::with_capacity(2 * PAGE_SIZE);
    buf.extend_from_slice(&header_page(&meta, 0));
    buf.extend_from_slice(&stamped(&page::new_page(T_LEAF)));
    file.write_at(&buf, 0)?;
    file.sync()?;
    vfs.rename(&tmp, &dir.join(DATA_FILE))?;
    vfs.sync_dir(dir)?;
    Ok(())
}

fn list_segments(vfs: &dyn Vfs, wal_dir: &Path) -> Result<Vec<u64>> {
    let mut seqs: Vec<u64> = vfs.list(wal_dir)?.iter().filter_map(|n| wal::parse_segment_name(n)).collect();
    seqs.sort_unstable();
    Ok(seqs)
}

fn remove_segments(vfs: &dyn Vfs, wal_dir: &Path, below: u64) -> Result<()> {
    let mut any = false;
    for seq in list_segments(vfs, wal_dir)? {
        if seq < below {
            vfs.remove(&wal_dir.join(wal::segment_name(seq)))?;
            any = true;
        }
    }
    if any {
        vfs.sync_dir(wal_dir)?;
    }
    Ok(())
}

struct Stats {
    checkpoints: AtomicU64,
    fsyncs: AtomicU64,
}

struct Inner {
    vfs: Arc<dyn Vfs>,
    dir: PathBuf,
    opts: DiskOptions,
    data: Arc<dyn VfsFile>,
    pool: Pool,
    /// Tree-level lock: shared for readers, exclusive for batch application.
    tree: RwLock<Meta>,
    wal: Mutex<Wal>,
    /// Group commit: true while a leader is fsyncing the log. Followers
    /// wait on `sync_done` and are all woken when the leader finishes.
    sync_leader: Mutex<bool>,
    sync_done: Condvar,
    durable_lsn: AtomicU64,
    /// Serializes checkpoints; holds the LSN of the last completed one.
    ckpt: Mutex<u64>,
    poisoned: AtomicBool,
    poison_msg: Mutex<String>,
    stop: AtomicBool,
    stats: Stats,
    _lock: Box<dyn VfsLock>,
}

/// Durable [`Engine`] storing its data in a directory.
pub struct DiskEngine {
    inner: Arc<Inner>,
    flusher: Option<JoinHandle<()>>,
}

impl fmt::Debug for DiskEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiskEngine").field("dir", &self.inner.dir).finish_non_exhaustive()
    }
}

impl DiskEngine {
    /// Opens the database in `dir`, creating it if the directory is empty
    /// or missing, and runs crash recovery if needed.
    pub fn open(dir: &Path, opts: DiskOptions) -> Result<DiskEngine> {
        let vfs = opts.vfs.clone();
        vfs.create_dir_all(dir)?;
        let lock = vfs.lock(&dir.join(LOCK_FILE)).map_err(|e| match e.kind() {
            io::ErrorKind::WouldBlock => StorageError::Locked(dir.display().to_string()),
            _ => StorageError::Io(e),
        })?;
        let wal_dir = dir.join(WAL_DIR);
        vfs.create_dir_all(&wal_dir)?;

        recover_journal(&*vfs, dir)?;
        let tmp = dir.join(DATA_TMP);
        if vfs.exists(&tmp)? {
            vfs.remove(&tmp)?;
        }
        let data_path = dir.join(DATA_FILE);
        if !vfs.exists(&data_path)? {
            for seq in list_segments(&*vfs, &wal_dir)? {
                let size = vfs.open(&wal_dir.join(wal::segment_name(seq)))?.size()?;
                if size > wal::SEG_HEADER as u64 {
                    return Err(StorageError::Corruption("write-ahead log present but data file missing".into()));
                }
            }
            remove_segments(&*vfs, &wal_dir, u64::MAX)?;
            create_fresh(&*vfs, dir)?;
        }
        let data = vfs.open(&data_path)?;
        let mut hdr = [0u8; PAGE_SIZE];
        if data.read_at(&mut hdr, 0)? != PAGE_SIZE {
            return Err(StorageError::Corruption("data file is shorter than its header".into()));
        }
        let (mut meta, ckpt_lsn) = parse_header(&hdr)?;
        let pool = Pool::new(data.clone(), opts.cache_pages);
        let stats = Stats { checkpoints: AtomicU64::new(0), fsyncs: AtomicU64::new(0) };

        // Replay the log on top of the checkpointed state.
        let segs = list_segments(&*vfs, &wal_dir)?;
        let mut expected = ckpt_lsn + 1;
        let mut flushed_lsn = ckpt_lsn;
        let mut reusable = None;
        let flush_limit = (opts.cache_pages as u64 / 2).max(64);
        // Once a record is missing: its LSN and where it was expected.
        let mut missing: Option<(u64, String)> = None;
        // LSN of the last intact record, in log order.
        let mut last_seen = 0u64;
        for &seq in &segs {
            let name = wal::segment_name(seq);
            let file = vfs.open(&wal_dir.join(&name))?;
            let bytes = read_all(&*file)?;
            if bytes.len() >= wal::SEG_HEADER && &bytes[..8] == wal::SEG_MAGIC_V1 {
                // An empty segment of the earlier format, as that version
                // leaves it after a checkpoint, holds nothing to replay;
                // it is replaced by a segment of the current format below.
                if bytes[wal::SEG_HEADER..].iter().all(|&b| b == 0) {
                    continue;
                }
                return Err(StorageError::Corruption(format!(
                    "write-ahead log segment {name} has log format 1, written by an earlier squad version; \
                     open the directory once with that version so that it checkpoints the log"
                )));
            }
            if bytes.len() < wal::SEG_HEADER || &bytes[..8] != wal::SEG_MAGIC || bytes[8..16] != seq.to_le_bytes() {
                continue;
            }
            if segs.len() == 1 && bytes.len() == wal::SEG_HEADER {
                reusable = Some((file.clone(), seq));
            }
            let mut off = wal::SEG_HEADER;
            loop {
                let Some(rec) = wal::decode_record(&bytes, off) else {
                    // The zero-filled end of the segment, or damage: go
                    // on with the next intact record, if there is one.
                    match wal::resync(&bytes, off, last_seen) {
                        Some(next) => {
                            off = next;
                            continue;
                        }
                        None => break,
                    }
                };
                let at = off;
                off = rec.end;
                last_seen = last_seen.max(rec.lsn);
                if missing.is_none() && rec.lsn > expected {
                    missing = Some((expected, format!("segment {name} before offset {at}")));
                }
                if let Some((lsn, place)) = &missing {
                    if rec.synced >= *lsn {
                        return Err(StorageError::Corruption(format!(
                            "write-ahead log is damaged: the record with LSN {lsn} ({place}) is missing or corrupt, \
                             but the intact record with LSN {} in segment {name} at offset {at} was written after \
                             it had been made durable. Refusing to open: replaying only up to the damage would \
                             drop committed transactions. Restore the database from a backup or its remote copy.",
                            rec.lsn
                        )));
                    }
                    continue;
                }
                if rec.lsn < expected {
                    continue;
                }
                let lsn = rec.lsn;
                let mut w = Writer { pool: &pool, meta: &mut meta };
                for (key, val) in rec.ops {
                    match val {
                        Some(v) => w.put(key, v)?,
                        None => {
                            w.delete(key)?;
                        }
                    }
                }
                expected += 1;
                if pool.dirty_count() > flush_limit {
                    let (epoch, pages) = pool.collect_dirty();
                    write_checkpoint(&*vfs, dir, &*data, &pages, meta, lsn, &stats.fsyncs)?;
                    pool.mark_clean(pages.iter().map(|(pid, _)| *pid), epoch);
                    flushed_lsn = lsn;
                }
            }
        }
        let last_lsn = expected - 1;
        if last_lsn > flushed_lsn {
            let (epoch, pages) = pool.collect_dirty();
            write_checkpoint(&*vfs, dir, &*data, &pages, meta, last_lsn, &stats.fsyncs)?;
            pool.mark_clean(pages.iter().map(|(pid, _)| *pid), epoch);
        }

        // Start from a clean log. An untouched single segment is reused so
        // that reopening a cleanly closed database costs no fsync.
        let (seg_file, seg_seq) = match reusable {
            Some(seg) => seg,
            None => {
                remove_segments(&*vfs, &wal_dir, u64::MAX)?;
                let seq = segs.last().map_or(1, |s| s + 1);
                let file = wal::create_segment(&*vfs, &wal_dir, seq)?;
                vfs.sync_dir(&wal_dir)?;
                (file, seq)
            }
        };
        let immediate = opts.sync_mode == SyncMode::Async;
        let inner = Arc::new(Inner {
            wal: Mutex::new(Wal::new(wal_dir, seg_file, seg_seq, last_lsn, immediate)),
            vfs,
            dir: dir.to_path_buf(),
            data,
            pool,
            tree: RwLock::new(meta),
            sync_leader: Mutex::new(false),
            sync_done: Condvar::new(),
            durable_lsn: AtomicU64::new(last_lsn),
            ckpt: Mutex::new(last_lsn),
            poisoned: AtomicBool::new(false),
            poison_msg: Mutex::new(String::new()),
            stop: AtomicBool::new(false),
            stats,
            _lock: lock,
            opts,
        });
        let flusher = immediate.then(|| {
            let weak = Arc::downgrade(&inner);
            thread::Builder::new()
                .name("squad-wal-flusher".into())
                .spawn(move || flusher_loop(weak))
                .expect("failed to spawn WAL flusher thread")
        });
        Ok(DiskEngine { inner, flusher })
    }

    /// LSN of the most recent write batch (0 for an empty database).
    pub fn last_lsn(&self) -> u64 {
        self.inner.wal.lock().last_lsn
    }

    /// LSN up to which the data file is current.
    pub fn checkpoint_lsn(&self) -> u64 {
        *self.inner.ckpt.lock()
    }
}

fn flusher_loop(weak: Weak<Inner>) {
    loop {
        thread::park_timeout(Duration::from_millis(5));
        let Some(inner) = weak.upgrade() else { return };
        if inner.stop.load(Ordering::Acquire) {
            return;
        }
        if inner.poisoned.load(Ordering::Acquire) {
            continue;
        }
        let last = inner.wal.lock().last_lsn;
        let _ = inner.sync_through(last, false);
    }
}

impl Drop for DiskEngine {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::Release);
        if let Some(h) = self.flusher.take() {
            h.thread().unpark();
            let _ = h.join();
        }
        if !self.inner.poisoned.load(Ordering::Acquire) {
            // Best effort: a clean shutdown leaves an empty log behind.
            let _ = self.inner.checkpoint();
        }
    }
}

impl Inner {
    fn check(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(StorageError::Poisoned(self.poison_msg.lock().clone()));
        }
        Ok(())
    }

    /// Poisons the engine if `r` is an error.
    fn fail<T>(&self, r: Result<T>) -> Result<T> {
        if let Err(e) = &r {
            let mut msg = self.poison_msg.lock();
            if !self.poisoned.swap(true, Ordering::AcqRel) {
                *msg = e.to_string();
            }
        }
        r
    }

    /// Blocks until `lsn` is durable. One caller at a time becomes the
    /// leader and fsyncs on behalf of everyone waiting (group commit).
    fn sync_through(&self, lsn: u64, linger: bool) -> Result<()> {
        {
            let mut leader_active = self.sync_leader.lock();
            loop {
                if self.durable_lsn.load(Ordering::Acquire) >= lsn {
                    return Ok(());
                }
                self.check()?;
                if !*leader_active {
                    *leader_active = true;
                    break;
                }
                self.sync_done.wait(&mut leader_active);
            }
        }
        if linger {
            // Give concurrent committers a moment to join this fsync.
            thread::sleep(Duration::from_micros(200));
        }
        let r = self.sync_wal();
        let r = self.fail(r);
        *self.sync_leader.lock() = false;
        self.sync_done.notify_all();
        r
    }

    /// Flushes and fsyncs the log. Only the group-commit leader calls this.
    fn sync_wal(&self) -> Result<()> {
        let (files, target) = {
            let mut wal = self.wal.lock();
            wal.flush_buf()?;
            (wal.files_to_sync(), wal.last_lsn)
        };
        for f in files {
            f.sync()?;
            self.stats.fsyncs.fetch_add(1, Ordering::Relaxed);
        }
        self.durable_lsn.fetch_max(target, Ordering::AcqRel);
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        self.check()?;
        let mut ckpt = self.ckpt.lock();
        self.check()?;
        let r = self.checkpoint_locked(&mut ckpt);
        self.fail(r)
    }

    fn checkpoint_locked(&self, ckpt_lsn: &mut u64) -> Result<()> {
        let (wal_dir, new_seq) = {
            let wal = self.wal.lock();
            if wal.last_lsn == *ckpt_lsn {
                return Ok(());
            }
            (wal.dir.clone(), wal.cur_seq + 1)
        };
        // Prepare the next log segment before pausing anyone.
        let new_seg = wal::create_segment(&*self.vfs, &wal_dir, new_seq)?;
        self.vfs.sync_dir(&wal_dir)?;

        // Pause writers just long enough to switch segments and snapshot
        // the dirty pages; the snapshot shares page memory with the cache.
        let (epoch, pages, meta, lsn) = {
            let meta = self.tree.write();
            let mut wal = self.wal.lock();
            wal.rotate(new_seg, new_seq)?;
            let (epoch, pages) = self.pool.collect_dirty();
            (epoch, pages, *meta, wal.last_lsn)
        };
        write_checkpoint(&*self.vfs, &self.dir, &*self.data, &pages, meta, lsn, &self.stats.fsyncs)?;
        self.pool.mark_clean(pages.iter().map(|(pid, _)| *pid), epoch);
        *ckpt_lsn = lsn;
        self.durable_lsn.fetch_max(lsn, Ordering::AcqRel);
        self.wal.lock().forget_pending();
        remove_segments(&*self.vfs, &wal_dir, new_seq)?;
        self.stats.checkpoints.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn maybe_checkpoint(&self) {
        let wal_bytes = self.wal.lock().bytes;
        let dirty_limit = (self.opts.cache_pages as u64 / 2).max(64);
        if wal_bytes < self.opts.checkpoint_wal_bytes && self.pool.dirty_count() <= dirty_limit {
            return;
        }
        if let Some(mut ckpt) = self.ckpt.try_lock() {
            if self.check().is_ok() {
                let r = self.checkpoint_locked(&mut ckpt);
                let _ = self.fail(r);
            }
        }
    }
}

impl Engine for DiskEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let meta = self.inner.tree.read();
        btree::get(&self.inner.pool, meta.root, key)
    }

    fn scan(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, reverse: bool, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let meta = self.inner.tree.read();
        btree::scan(&self.inner.pool, meta.root, lo, hi, reverse, limit)
    }

    fn write(&self, batch: WriteBatch) -> Result<u64> {
        let inner = &*self.inner;
        for (k, v) in &batch.ops {
            if k.len() > MAX_KEY_LEN {
                return Err(StorageError::KeyTooLarge(k.len()));
            }
            if v.as_ref().is_some_and(|v| v.len() > page::MAX_VALUE_LEN) {
                return Err(StorageError::Invalid("value exceeds the maximum value length".into()));
            }
        }
        let len = wal::payload_len(&batch);
        if len > wal::MAX_RECORD_PAYLOAD {
            return Err(StorageError::Invalid(format!(
                "a write batch of {len} bytes exceeds the log record limit of {} bytes",
                wal::MAX_RECORD_PAYLOAD
            )));
        }
        inner.check()?;
        if batch.is_empty() {
            return Ok(inner.wal.lock().last_lsn);
        }
        let lsn = {
            let mut meta = inner.tree.write();
            inner.check()?;
            let synced = inner.durable_lsn.load(Ordering::Acquire);
            let lsn = inner.fail(inner.wal.lock().append(&batch, synced))?;
            let mut w = Writer { pool: &inner.pool, meta: &mut meta };
            let applied = batch.ops.iter().try_for_each(|(k, v)| match v {
                Some(v) => w.put(k, v),
                None => w.delete(k).map(|_| ()),
            });
            inner.fail(applied)?;
            lsn
        };
        inner.maybe_checkpoint();
        Ok(lsn)
    }

    fn sync(&self, lsn: u64) -> Result<()> {
        let inner = &*self.inner;
        if inner.opts.sync_mode == SyncMode::Async {
            return inner.check();
        }
        inner.sync_through(lsn, inner.opts.sync_mode == SyncMode::Group)
    }

    fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    fn backup(&self, dest: &Path) -> Result<()> {
        let inner = &*self.inner;
        if !inner.vfs.list(dest)?.is_empty() {
            return Err(StorageError::Invalid(format!("backup target {} is not empty", dest.display())));
        }
        inner.check()?;
        // Holding the checkpoint lock keeps the data file frozen at a
        // consistent image while writers carry on in the log.
        let mut ckpt = inner.ckpt.lock();
        inner.check()?;
        let r = inner.checkpoint_locked(&mut ckpt);
        inner.fail(r)?;
        inner.vfs.create_dir_all(dest)?;
        let tmp = dest.join(DATA_TMP);
        let out = inner.vfs.create(&tmp)?;
        let size = inner.data.size()?;
        let mut buf = vec![0u8; IO_CHUNK];
        let mut off = 0u64;
        while off < size {
            let n = inner.data.read_at(&mut buf, off)?;
            if n == 0 {
                return Err(StorageError::Corruption("data file shrank during backup".into()));
            }
            out.write_at(&buf[..n], off)?;
            off += n as u64;
        }
        out.sync()?;
        inner.vfs.rename(&tmp, &dest.join(DATA_FILE))?;
        inner.vfs.sync_dir(dest)?;
        Ok(())
    }

    fn verify(&self) -> Result<VerifyReport> {
        let inner = &*self.inner;
        let _ckpt = inner.ckpt.lock();
        let meta = inner.tree.read();
        let mut report = VerifyReport::default();

        // Pass 1: checksums of everything in the data file.
        let size = inner.data.size()?;
        if size % PAGE_SIZE as u64 != 0 {
            report.errors.push(format!("data file size {size} is not a multiple of the page size"));
        }
        let mut buf = [0u8; PAGE_SIZE];
        for pid in 0..size / PAGE_SIZE as u64 {
            if inner.data.read_at(&mut buf, pid * PAGE_SIZE as u64)? != PAGE_SIZE || !page::checksum_ok(&buf) {
                report.errors.push(format!("checksum mismatch on page {pid}"));
            }
            report.pages_checked += 1;
        }

        // Pass 2: structure of the current tree.
        let mut v = Verifier {
            pool: &inner.pool,
            seen: vec![false; meta.page_count as usize],
            errors: Vec::new(),
            entries: 0,
            leaves: Vec::new(),
            leaf_depth: None,
        };
        v.seen[0] = true;
        v.walk(meta.root, None, None, 0);
        for (i, &(pid, prev, next)) in v.leaves.iter().enumerate() {
            let want_prev = if i > 0 { v.leaves[i - 1].0 } else { 0 };
            let want_next = v.leaves.get(i + 1).map_or(0, |l| l.0);
            if prev != want_prev || next != want_next {
                v.errors.push(format!(
                    "leaf {pid} has sibling links ({prev}, {next}), expected ({want_prev}, {want_next})"
                ));
            }
        }
        let mut pid = meta.free_head;
        while pid != 0 {
            if !v.mark(pid, "free list") {
                break;
            }
            match inner.pool.get(pid) {
                Ok(p) if page::page_type(&p) == T_FREE => pid = page::next(&p),
                Ok(_) => {
                    v.errors.push(format!("free list contains in-use page {pid}"));
                    break;
                }
                Err(e) => {
                    v.errors.push(format!("free list page {pid}: {e}"));
                    break;
                }
            }
        }
        let leaked = v.seen.iter().filter(|s| !**s).count();
        if leaked > 0 {
            v.errors.push(format!("{leaked} pages are neither reachable nor on the free list"));
        }
        report.entries = v.entries;
        report.errors.extend(v.errors);
        Ok(report)
    }

    fn stats(&self) -> EngineStats {
        let inner = &*self.inner;
        let (cached, hits, misses) = inner.pool.counters();
        EngineStats {
            page_count: inner.tree.read().page_count as u64,
            cached_pages: cached,
            dirty_pages: inner.pool.dirty_count(),
            cache_hits: hits,
            cache_misses: misses,
            wal_bytes: inner.wal.lock().bytes,
            checkpoints: inner.stats.checkpoints.load(Ordering::Relaxed),
            fsyncs: inner.stats.fsyncs.load(Ordering::Relaxed),
        }
    }
}

struct Verifier<'a> {
    pool: &'a Pool,
    seen: Vec<bool>,
    errors: Vec<String>,
    entries: u64,
    /// `(page id, prev, next)` of every leaf in key order.
    leaves: Vec<(u32, u32, u32)>,
    leaf_depth: Option<usize>,
}

impl Verifier<'_> {
    fn mark(&mut self, pid: u32, what: &str) -> bool {
        match self.seen.get_mut(pid as usize) {
            None => {
                self.errors.push(format!("{what} references page {pid} beyond the end of the file"));
                false
            }
            Some(true) => {
                self.errors.push(format!("{what} references page {pid}, which is already in use elsewhere"));
                false
            }
            Some(s) => {
                *s = true;
                true
            }
        }
    }

    fn check_keys(&mut self, pid: u32, p: &PageBuf, lo: Option<&[u8]>, hi: Option<&[u8]>) {
        let n = page::nkeys(p);
        for i in 0..n {
            let k = page::key(p, i);
            if i > 0 && page::key(p, i - 1) >= k {
                self.errors.push(format!("page {pid}: keys {} and {i} are out of order", i - 1));
            }
            if lo.is_some_and(|lo| k < lo) || hi.is_some_and(|hi| k >= hi) {
                self.errors.push(format!("page {pid}: key {i} is outside the range of its parent"));
            }
        }
    }

    fn walk(&mut self, pid: u32, lo: Option<&[u8]>, hi: Option<&[u8]>, depth: usize) {
        if depth > 64 {
            self.errors.push(format!("tree deeper than 64 levels at page {pid}"));
            return;
        }
        if !self.mark(pid, "tree") {
            return;
        }
        let p = match self.pool.get(pid) {
            Ok(p) => p,
            Err(e) => {
                self.errors.push(format!("page {pid}: {e}"));
                return;
            }
        };
        match page::page_type(&p) {
            T_LEAF => {
                self.check_keys(pid, &p, lo, hi);
                let n = page::nkeys(&p);
                self.entries += n as u64;
                for i in 0..n {
                    if let page::ValRef::Overflow { len, first } = page::value(&p, i) {
                        self.walk_overflow(pid, i, len, first);
                    }
                }
                match self.leaf_depth {
                    None => self.leaf_depth = Some(depth),
                    Some(d) if d != depth => {
                        self.errors.push(format!("leaf {pid} is at depth {depth}, other leaves at {d}"))
                    }
                    _ => {}
                }
                self.leaves.push((pid, page::prev(&p), page::next(&p)));
            }
            T_INTERNAL => {
                self.check_keys(pid, &p, lo, hi);
                let n = page::nkeys(&p);
                for i in 0..n {
                    let child_lo = if i == 0 { lo } else { Some(page::key(&p, i - 1)) };
                    self.walk(page::child(&p, i), child_lo, Some(page::key(&p, i)), depth + 1);
                }
                let child_lo = if n == 0 { lo } else { Some(page::key(&p, n - 1)) };
                self.walk(page::next(&p), child_lo, hi, depth + 1);
            }
            t => self.errors.push(format!("page {pid} has type {t}, expected a tree page")),
        }
    }

    fn walk_overflow(&mut self, leaf: u32, cell: usize, len: usize, first: u32) {
        let mut pid = first;
        let mut total = 0;
        while pid != 0 {
            if !self.mark(pid, "overflow chain") {
                return;
            }
            match self.pool.get(pid) {
                Ok(p) if page::page_type(&p) == T_OVERFLOW => {
                    total += page::overflow_data(&p).len();
                    pid = page::next(&p);
                }
                Ok(_) => {
                    self.errors.push(format!("overflow chain of leaf {leaf} cell {cell} hits non-overflow page {pid}"));
                    return;
                }
                Err(e) => {
                    self.errors.push(format!("overflow page {pid}: {e}"));
                    return;
                }
            }
        }
        if total != len {
            self.errors.push(format!("leaf {leaf} cell {cell}: overflow chain holds {total} bytes, expected {len}"));
        }
    }
}
