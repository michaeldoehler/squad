use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::{check_key, missing, range_of, Error, ObjectMeta, ObjectStore, Result};

/// A decision about one operation of a [`MemoryStore`] with fault
/// injection enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// The operation works.
    None,
    /// The operation fails and has no effect.
    Fail,
    /// The operation takes effect but reports failure, as when the
    /// response to a successful upload is lost.
    FailAfterEffect,
}

type FaultFn = dyn Fn(&str, &str) -> Fault + Send + Sync;

/// Volatile in-memory [`ObjectStore`]. Cloning yields a handle to the
/// same objects.
#[derive(Clone, Default)]
pub struct MemoryStore {
    objects: Arc<Mutex<BTreeMap<String, Arc<Vec<u8>>>>>,
    faults: Arc<Mutex<Option<Box<FaultFn>>>>,
    puts: Arc<AtomicU64>,
    put_bytes: Arc<AtomicU64>,
    requests: Arc<AtomicU64>,
}

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore::default()
    }

    /// Installs a hook that is asked before every operation whether it
    /// should fail. It receives the operation name (`"put"`, `"get"`,
    /// `"list"`, `"delete"`, `"head"`) and the key or prefix. `None`
    /// removes the hook.
    pub fn set_faults(&self, hook: Option<Box<FaultFn>>) {
        *self.faults.lock() = hook;
    }

    /// Number of successful puts and the bytes they carried.
    pub fn put_stats(&self) -> (u64, u64) {
        (self.puts.load(Ordering::Relaxed), self.put_bytes.load(Ordering::Relaxed))
    }

    /// Number of operations attempted.
    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// Total size of all objects.
    pub fn total_bytes(&self) -> u64 {
        self.objects.lock().values().map(|v| v.len() as u64).sum()
    }

    fn fault(&self, op: &str, key: &str) -> Fault {
        self.requests.fetch_add(1, Ordering::Relaxed);
        match &*self.faults.lock() {
            Some(hook) => hook(op, key),
            None => Fault::None,
        }
    }

    fn injected(op: &str, key: &str) -> Error {
        Error::Transport(format!("injected fault during {op} of {key:?}"))
    }

    fn store(&self, key: &str, data: &[u8], only_if_absent: bool) -> Result<bool> {
        check_key(key)?;
        let fault = self.fault("put", key);
        if fault == Fault::Fail {
            return Err(Self::injected("put", key));
        }
        let created = {
            let mut objects = self.objects.lock();
            if only_if_absent && objects.contains_key(key) {
                false
            } else {
                objects.insert(key.to_string(), Arc::new(data.to_vec()));
                true
            }
        };
        if created {
            self.puts.fetch_add(1, Ordering::Relaxed);
            self.put_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        }
        if fault == Fault::FailAfterEffect {
            return Err(Self::injected("put", key));
        }
        Ok(created)
    }

    fn read(&self, op: &str, key: &str) -> Result<Option<Arc<Vec<u8>>>> {
        check_key(key)?;
        if self.fault(op, key) != Fault::None {
            return Err(Self::injected(op, key));
        }
        Ok(self.objects.lock().get(key).cloned())
    }
}

impl ObjectStore for MemoryStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.store(key, data, false).map(|_| ())
    }

    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<bool> {
        self.store(key, data, true)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.read("get", key)?.map(|v| (*v).clone()))
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        let data = self.read("get", key)?.ok_or_else(|| missing(key))?;
        range_of(key, &data, offset, len)
    }

    fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        Ok(self.read("head", key)?.map(|v| ObjectMeta { key: key.to_string(), size: v.len() as u64 }))
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        if self.fault("list", prefix) != Fault::None {
            return Err(Self::injected("list", prefix));
        }
        let objects = self.objects.lock();
        Ok(objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| ObjectMeta { key: k.clone(), size: v.len() as u64 })
            .collect())
    }

    fn delete(&self, key: &str) -> Result<()> {
        check_key(key)?;
        let fault = self.fault("delete", key);
        if fault == Fault::Fail {
            return Err(Self::injected("delete", key));
        }
        self.objects.lock().remove(key);
        if fault == Fault::FailAfterEffect {
            return Err(Self::injected("delete", key));
        }
        Ok(())
    }
}
