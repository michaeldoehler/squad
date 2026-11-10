use std::collections::HashMap;

use parking_lot::Mutex;

use crate::{range_of, ObjectMeta, ObjectStore, Result};

#[derive(Hash, PartialEq, Eq, Clone)]
enum Slot {
    Whole(String),
    Range(String, u64, u64),
}

impl Slot {
    fn key(&self) -> &str {
        match self {
            Slot::Whole(k) | Slot::Range(k, ..) => k,
        }
    }
}

#[derive(Default)]
struct State {
    entries: HashMap<Slot, (Vec<u8>, u64)>,
    bytes: usize,
    tick: u64,
    hits: u64,
    misses: u64,
}

/// Keeps recently read objects and ranges of another store in memory.
///
/// Reads of cached data do not touch the underlying store. Writes and
/// deletes go through and drop what was cached for the key. The cache is
/// bounded by total bytes and evicts the least recently used entries.
///
/// It assumes that objects only change through this wrapper, which holds
/// for squad: it writes every object once and never modifies it.
pub struct CachedStore<S> {
    inner: S,
    capacity: usize,
    max_entry: usize,
    state: Mutex<State>,
}

impl<S: ObjectStore> CachedStore<S> {
    /// Caches up to `capacity` bytes. Reads larger than an eighth of the
    /// capacity are passed through uncached.
    pub fn new(inner: S, capacity: usize) -> CachedStore<S> {
        CachedStore { inner, capacity, max_entry: capacity / 8, state: Mutex::new(State::default()) }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// `(hits, misses)` of reads so far.
    pub fn stats(&self) -> (u64, u64) {
        let s = self.state.lock();
        (s.hits, s.misses)
    }

    fn lookup(&self, slot: &Slot) -> Option<Vec<u8>> {
        let mut s = self.state.lock();
        s.tick += 1;
        let tick = s.tick;
        match s.entries.get_mut(slot) {
            Some((data, used)) => {
                *used = tick;
                let data = data.clone();
                s.hits += 1;
                Some(data)
            }
            None => {
                s.misses += 1;
                None
            }
        }
    }

    fn remember(&self, slot: Slot, data: &[u8]) {
        if data.len() > self.max_entry {
            return;
        }
        let mut s = self.state.lock();
        s.tick += 1;
        let tick = s.tick;
        if let Some((old, _)) = s.entries.insert(slot, (data.to_vec(), tick)) {
            s.bytes -= old.len();
        }
        s.bytes += data.len();
        while s.bytes > self.capacity {
            let Some(oldest) = s.entries.iter().min_by_key(|(_, (_, used))| *used).map(|(k, _)| k.clone()) else {
                break;
            };
            if let Some((old, _)) = s.entries.remove(&oldest) {
                s.bytes -= old.len();
            }
        }
    }

    fn forget(&self, key: &str) {
        let mut s = self.state.lock();
        let stale: Vec<Slot> = s.entries.keys().filter(|slot| slot.key() == key).cloned().collect();
        for slot in stale {
            if let Some((old, _)) = s.entries.remove(&slot) {
                s.bytes -= old.len();
            }
        }
    }
}

impl<S: ObjectStore> ObjectStore for CachedStore<S> {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.forget(key);
        self.inner.put(key, data)?;
        self.remember(Slot::Whole(key.to_string()), data);
        Ok(())
    }

    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<bool> {
        self.forget(key);
        self.inner.put_if_absent(key, data)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let slot = Slot::Whole(key.to_string());
        if let Some(data) = self.lookup(&slot) {
            return Ok(Some(data));
        }
        let data = self.inner.get(key)?;
        if let Some(d) = &data {
            self.remember(slot, d);
        }
        Ok(data)
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        // A cached whole object answers any range of it.
        if let Some(whole) = self.lookup(&Slot::Whole(key.to_string())) {
            return range_of(key, &whole, offset, len);
        }
        let slot = Slot::Range(key.to_string(), offset, len);
        if let Some(data) = self.lookup(&slot) {
            return Ok(data);
        }
        let data = self.inner.get_range(key, offset, len)?;
        self.remember(slot, &data);
        Ok(data)
    }

    fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        self.inner.head(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.forget(key);
        self.inner.delete(key)
    }
}
