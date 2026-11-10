//! Buffer pool: a bounded page cache in front of the data file.
//!
//! Pages are immutable `Arc<PageBuf>` snapshots. A writer mutates a page
//! through [`Pool::update`], which copies it first if a reader or an
//! in-flight checkpoint still holds the old snapshot (copy-on-write).
//!
//! The pool is no-steal: a dirty page is never written to the data file or
//! evicted before a checkpoint has made it durable, so the data file always
//! holds exactly the state of the last completed checkpoint.

use std::cell::Cell;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::page::{self, PageBuf, PAGE_SIZE};
use crate::vfs::VfsFile;
use crate::{Result, StorageError};

const SHARDS: usize = 64;

/// Cache hits are counted per thread and added to the shared counter in
/// batches, so that readers do not write to a shared cache line on every
/// page access.
const HIT_BATCH: u64 = 1024;
thread_local! {
    static LOCAL_HITS: Cell<u64> = const { Cell::new(0) };
}

#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 << 8) | b as u64;
        }
    }
    fn write_u32(&mut self, v: u32) {
        self.0 = (v as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}
type IdMap = HashMap<u32, usize, BuildHasherDefault<IdHasher>>;

struct Entry {
    pid: u32,
    page: Arc<PageBuf>,
    /// 0 when clean, otherwise the pool epoch of the last modification.
    dirty_epoch: u64,
    /// Set on access, cleared by the eviction clock. Atomic so that
    /// readers can set it under the shared lock.
    referenced: AtomicBool,
}

#[derive(Default)]
struct Shard {
    map: IdMap,
    slots: Vec<Option<Entry>>,
    free: Vec<usize>,
    hand: usize,
}

impl Shard {
    fn insert(&mut self, e: Entry, cap: usize) {
        if self.map.len() >= cap {
            self.evict_one();
        }
        let pid = e.pid;
        let idx = match self.free.pop() {
            Some(i) => {
                self.slots[i] = Some(e);
                i
            }
            None => {
                self.slots.push(Some(e));
                self.slots.len() - 1
            }
        };
        self.map.insert(pid, idx);
    }

    /// CLOCK eviction of one clean page. Does nothing if every page is dirty.
    fn evict_one(&mut self) {
        let n = self.slots.len();
        for _ in 0..2 * n {
            let i = self.hand;
            self.hand = (self.hand + 1) % n;
            if let Some(e) = &mut self.slots[i] {
                if e.dirty_epoch != 0 {
                    continue;
                }
                if e.referenced.swap(false, Ordering::Relaxed) {
                    continue;
                }
                let pid = e.pid;
                self.slots[i] = None;
                self.free.push(i);
                self.map.remove(&pid);
                return;
            }
        }
    }
}

pub struct Pool {
    shards: Vec<RwLock<Shard>>,
    file: Arc<dyn VfsFile>,
    cap_per_shard: usize,
    epoch: AtomicU64,
    dirty: AtomicU64,
    misses: AtomicU64,
    hits: AtomicU64,
}

impl Pool {
    pub fn new(file: Arc<dyn VfsFile>, cache_pages: usize) -> Pool {
        Pool {
            shards: (0..SHARDS).map(|_| RwLock::new(Shard::default())).collect(),
            file,
            cap_per_shard: (cache_pages / SHARDS).max(4),
            epoch: AtomicU64::new(1),
            dirty: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }

    fn load(&self, pid: u32) -> Result<Arc<PageBuf>> {
        self.misses.fetch_add(1, Ordering::Relaxed);
        let mut buf = Box::new([0u8; PAGE_SIZE]);
        let n = self.file.read_at(&mut buf[..], pid as u64 * PAGE_SIZE as u64)?;
        if n != PAGE_SIZE {
            return Err(StorageError::Corruption(format!("page {pid} lies beyond the end of the data file")));
        }
        if !page::checksum_ok(&buf) {
            return Err(StorageError::Corruption(format!("checksum mismatch on page {pid}")));
        }
        Ok(Arc::from(buf))
    }

    /// Returns a snapshot of page `pid`, reading it from disk if needed.
    #[inline]
    pub fn get(&self, pid: u32) -> Result<Arc<PageBuf>> {
        let shard = &self.shards[pid as usize % SHARDS];
        {
            // Hot path: a shared lock and no writes to shared memory other
            // than the page's reference count.
            let s = shard.read();
            if let Some(&i) = s.map.get(&pid) {
                let e = s.slots[i].as_ref().unwrap();
                if !e.referenced.load(Ordering::Relaxed) {
                    e.referenced.store(true, Ordering::Relaxed);
                }
                LOCAL_HITS.with(|h| {
                    let n = h.get() + 1;
                    if n == HIT_BATCH {
                        self.hits.fetch_add(HIT_BATCH, Ordering::Relaxed);
                        h.set(0);
                    } else {
                        h.set(n);
                    }
                });
                return Ok(e.page.clone());
            }
        }
        let page = self.load(pid)?;
        let mut s = shard.write();
        if let Some(&i) = s.map.get(&pid) {
            return Ok(s.slots[i].as_ref().unwrap().page.clone());
        }
        s.insert(
            Entry { pid, page: page.clone(), dirty_epoch: 0, referenced: AtomicBool::new(true) },
            self.cap_per_shard,
        );
        Ok(page)
    }

    /// Mutates page `pid` in place and marks it dirty. The closure must not
    /// touch the pool.
    pub fn update<R>(&self, pid: u32, f: impl FnOnce(&mut PageBuf) -> R) -> Result<R> {
        let shard = &self.shards[pid as usize % SHARDS];
        let mut s = shard.write();
        if !s.map.contains_key(&pid) {
            drop(s);
            let page = self.load(pid)?;
            s = shard.write();
            if !s.map.contains_key(&pid) {
                s.insert(Entry { pid, page, dirty_epoch: 0, referenced: AtomicBool::new(true) }, self.cap_per_shard);
            }
        }
        let i = s.map[&pid];
        let e = s.slots[i].as_mut().unwrap();
        if e.dirty_epoch == 0 {
            self.dirty.fetch_add(1, Ordering::Relaxed);
        }
        e.dirty_epoch = self.epoch.load(Ordering::Relaxed);
        e.referenced.store(true, Ordering::Relaxed);
        Ok(f(Arc::make_mut(&mut e.page)))
    }

    /// Installs a new image for page `pid` and marks it dirty.
    pub fn put(&self, pid: u32, page: PageBuf) {
        let epoch = self.epoch.load(Ordering::Relaxed);
        let mut s = self.shards[pid as usize % SHARDS].write();
        if let Some(&i) = s.map.get(&pid) {
            let e = s.slots[i].as_mut().unwrap();
            if e.dirty_epoch == 0 {
                self.dirty.fetch_add(1, Ordering::Relaxed);
            }
            e.dirty_epoch = epoch;
            e.referenced.store(true, Ordering::Relaxed);
            e.page = Arc::new(page);
        } else {
            self.dirty.fetch_add(1, Ordering::Relaxed);
            s.insert(
                Entry { pid, page: Arc::new(page), dirty_epoch: epoch, referenced: AtomicBool::new(true) },
                self.cap_per_shard,
            );
        }
    }

    /// Snapshots all dirty pages, sorted by page id, and starts a new epoch.
    /// The caller must exclude writers. Pass the returned epoch to
    /// [`Pool::mark_clean`] once the snapshot is durable.
    pub fn collect_dirty(&self) -> (u64, Vec<(u32, Arc<PageBuf>)>) {
        let mut out = Vec::new();
        for shard in &self.shards {
            let s = shard.read();
            for e in s.slots.iter().flatten() {
                if e.dirty_epoch != 0 {
                    out.push((e.pid, e.page.clone()));
                }
            }
        }
        out.sort_unstable_by_key(|(pid, _)| *pid);
        (self.epoch.fetch_add(1, Ordering::Relaxed), out)
    }

    /// Marks pages clean unless they were modified after `epoch`.
    pub fn mark_clean(&self, pids: impl Iterator<Item = u32>, epoch: u64) {
        for pid in pids {
            let mut s = self.shards[pid as usize % SHARDS].write();
            if let Some(&i) = s.map.get(&pid) {
                let e = s.slots[i].as_mut().unwrap();
                if e.dirty_epoch != 0 && e.dirty_epoch <= epoch {
                    e.dirty_epoch = 0;
                    self.dirty.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    pub fn dirty_count(&self) -> u64 {
        self.dirty.load(Ordering::Relaxed)
    }

    /// (cached pages, cache hits, cache misses)
    pub fn counters(&self) -> (u64, u64, u64) {
        let mut cached = 0;
        for shard in &self.shards {
            cached += shard.read().map.len() as u64;
        }
        (cached, self.hits.load(Ordering::Relaxed), self.misses.load(Ordering::Relaxed))
    }
}
