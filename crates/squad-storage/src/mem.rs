use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::{Engine, EngineStats, Result, StorageError, VerifyReport, WriteBatch, MAX_KEY_LEN};

/// Volatile in-memory [`Engine`]. Nothing survives the process.
#[derive(Default)]
pub struct MemEngine {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    lsn: AtomicU64,
}

impl MemEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Engine for MemEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.map.read().get(key).cloned())
    }

    fn scan(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, reverse: bool, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // BTreeMap::range panics on inverted or empty-excluded ranges.
        match (&lo, &hi) {
            (Bound::Included(a), Bound::Included(b)) if a > b => return Ok(Vec::new()),
            (Bound::Included(a), Bound::Excluded(b))
            | (Bound::Excluded(a), Bound::Included(b))
            | (Bound::Excluded(a), Bound::Excluded(b))
                if a >= b =>
            {
                return Ok(Vec::new())
            }
            _ => {}
        }
        let map = self.map.read();
        let range = map.range::<[u8], _>((lo, hi));
        let clone = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
        Ok(if reverse { range.rev().take(limit).map(clone).collect() } else { range.take(limit).map(clone).collect() })
    }

    fn write(&self, batch: WriteBatch) -> Result<u64> {
        if let Some((k, _)) = batch.ops.iter().find(|(k, _)| k.len() > MAX_KEY_LEN) {
            return Err(StorageError::KeyTooLarge(k.len()));
        }
        let mut map = self.map.write();
        for (k, v) in batch.ops {
            match v {
                Some(v) => {
                    map.insert(k, v);
                }
                None => {
                    map.remove(&k);
                }
            }
        }
        Ok(self.lsn.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn sync(&self, _lsn: u64) -> Result<()> {
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        Ok(())
    }

    fn backup(&self, _dest: &Path) -> Result<()> {
        Err(StorageError::Invalid("in-memory databases cannot be backed up to a directory".into()))
    }

    fn verify(&self) -> Result<VerifyReport> {
        Ok(VerifyReport { pages_checked: 0, entries: self.map.read().len() as u64, errors: Vec::new() })
    }

    fn stats(&self) -> EngineStats {
        EngineStats::default()
    }
}
