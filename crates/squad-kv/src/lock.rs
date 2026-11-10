use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::{KvError, Result};

/// Strength of a row lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Compatible with other shared locks (`FOR SHARE`).
    Shared,
    /// Compatible with nothing (`FOR UPDATE`, and every row write).
    Exclusive,
}

/// What to do when a lock is held by someone else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitPolicy {
    /// Block until the lock is free; `Some(d)` gives up after `d`.
    Wait(Option<Duration>),
    /// Fail immediately with [`KvError::LockNotAvailable`].
    NoWait,
    /// Report `false` instead of locking.
    Skip,
}

#[derive(Default)]
struct Holders {
    exclusive: Option<u64>,
    shared: Vec<u64>,
}

#[derive(Default)]
struct State {
    locks: HashMap<Vec<u8>, Holders>,
    /// Wait-for graph: transaction -> transactions it currently waits on.
    waiting: HashMap<u64, Vec<u64>>,
}

impl State {
    fn blockers(&self, txn: u64, key: &[u8], mode: LockMode) -> Vec<u64> {
        let Some(h) = self.locks.get(key) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        if let Some(x) = h.exclusive {
            if x != txn {
                out.push(x);
            }
        }
        if mode == LockMode::Exclusive {
            out.extend(h.shared.iter().copied().filter(|&s| s != txn));
        }
        out
    }

    fn would_deadlock(&self, me: u64, blockers: &[u64]) -> bool {
        let mut stack: Vec<u64> = blockers.to_vec();
        let mut seen: Vec<u64> = Vec::new();
        while let Some(t) = stack.pop() {
            if t == me {
                return true;
            }
            if seen.contains(&t) {
                continue;
            }
            seen.push(t);
            if let Some(next) = self.waiting.get(&t) {
                stack.extend_from_slice(next);
            }
        }
        false
    }
}

/// Table of row locks shared by all transactions of a store.
#[derive(Default)]
pub(crate) struct LockManager {
    state: Mutex<State>,
    released: Condvar,
}

impl LockManager {
    /// Returns `Ok(false)` only for [`WaitPolicy::Skip`] on a busy lock.
    pub(crate) fn acquire(&self, txn: u64, key: &[u8], mode: LockMode, wait: WaitPolicy) -> Result<bool> {
        let mut st = self.state.lock();
        let deadline = match wait {
            WaitPolicy::Wait(Some(d)) => Some(Instant::now() + d),
            _ => None,
        };
        loop {
            let blockers = st.blockers(txn, key, mode);
            if blockers.is_empty() {
                st.waiting.remove(&txn);
                let h = st.locks.entry(key.to_vec()).or_default();
                match mode {
                    LockMode::Exclusive => {
                        h.shared.retain(|&s| s != txn);
                        h.exclusive = Some(txn);
                    }
                    LockMode::Shared => {
                        if h.exclusive != Some(txn) && !h.shared.contains(&txn) {
                            h.shared.push(txn);
                        }
                    }
                }
                return Ok(true);
            }
            match wait {
                WaitPolicy::NoWait => return Err(KvError::LockNotAvailable),
                WaitPolicy::Skip => return Ok(false),
                WaitPolicy::Wait(_) => {}
            }
            if st.would_deadlock(txn, &blockers) {
                st.waiting.remove(&txn);
                return Err(KvError::Deadlock);
            }
            st.waiting.insert(txn, blockers);
            match deadline {
                Some(d) => {
                    if self.released.wait_until(&mut st, d).timed_out() {
                        st.waiting.remove(&txn);
                        return Err(KvError::LockTimeout);
                    }
                }
                None => self.released.wait(&mut st),
            }
        }
    }

    pub(crate) fn release_all<'a>(&self, txn: u64, keys: impl Iterator<Item = &'a Vec<u8>>) {
        let mut st = self.state.lock();
        let mut any = false;
        for key in keys {
            any = true;
            if let Some(h) = st.locks.get_mut(key) {
                if h.exclusive == Some(txn) {
                    h.exclusive = None;
                }
                h.shared.retain(|&s| s != txn);
                if h.exclusive.is_none() && h.shared.is_empty() {
                    st.locks.remove(key);
                }
            }
        }
        st.waiting.remove(&txn);
        if any {
            self.released.notify_all();
        }
    }
}
