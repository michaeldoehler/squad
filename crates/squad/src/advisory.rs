//! Advisory locks: `pg_advisory_lock` and its relatives.
//!
//! The locks live in memory, in one table per open database, and are
//! held by sessions (identified by their backend pid). As in PostgreSQL:
//!
//! - A key is either one `bigint` or a pair of `int4`; the two key spaces
//!   are separate, so `(0, 1)` and `1` are different locks.
//! - A lock is exclusive or shared. An exclusive lock conflicts with any
//!   lock of another session on the same key; a shared lock conflicts
//!   with exclusive locks of other sessions. A session never conflicts
//!   with itself.
//! - Locks are counted: a session that takes a lock twice must release
//!   it twice.
//! - Session-level locks are held until they are released or the
//!   session ends, also across a rollback; transaction-level locks
//!   (`pg_advisory_xact_lock…`) until the transaction ends.
//!
//! Waiting ends with the lock, a cancel request, `statement_timeout` or
//! `lock_timeout`. Unlike PostgreSQL, squad does not detect deadlocks
//! between advisory locks: two sessions that wait for each other wait
//! until one of these ends the wait.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::{sqlstate, Error, Result};

/// A lock key: one `bigint`, or two `int4` packed into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    pair: bool,
    id: i64,
}

impl Key {
    pub(crate) fn single(id: i64) -> Key {
        Key { pair: false, id }
    }

    pub(crate) fn pair(a: i32, b: i32) -> Key {
        Key { pair: true, id: ((a as i64) << 32) | (b as u32 as i64) }
    }
}

/// What one session holds on one key.
#[derive(Debug, Default, Clone, Copy)]
struct Held {
    exclusive_session: u32,
    exclusive_xact: u32,
    shared_session: u32,
    shared_xact: u32,
}

impl Held {
    fn exclusive(&self) -> bool {
        self.exclusive_session + self.exclusive_xact > 0
    }

    fn any(&self) -> bool {
        self.exclusive() || self.shared_session + self.shared_xact > 0
    }

    fn count(&mut self, shared: bool, xact: bool) -> &mut u32 {
        match (shared, xact) {
            (false, false) => &mut self.exclusive_session,
            (false, true) => &mut self.exclusive_xact,
            (true, false) => &mut self.shared_session,
            (true, true) => &mut self.shared_xact,
        }
    }
}

/// How long an acquisition may wait.
pub(crate) enum Wait<'a> {
    /// Do not wait (`pg_try_advisory_lock…`).
    No,
    /// Wait; the closure is called regularly and ends the wait with its
    /// error (cancel request, statement timeout).
    Yes { lock_timeout: Option<Duration>, interrupted: &'a mut dyn FnMut() -> Result<()> },
}

#[derive(Default)]
pub(crate) struct AdvisoryLocks {
    table: Mutex<HashMap<Key, HashMap<i32, Held>>>,
    released: Condvar,
}

/// How often a waiting session looks for interrupts.
const POLL: Duration = Duration::from_millis(20);

impl AdvisoryLocks {
    /// Takes a lock for session `pid`. Returns false if `wait` is
    /// [`Wait::No`] and another session holds a conflicting lock.
    pub(crate) fn acquire(&self, pid: i32, key: Key, shared: bool, xact: bool, mut wait: Wait<'_>) -> Result<bool> {
        let started = Instant::now();
        let mut table = self.table.lock();
        loop {
            let conflict = table.get(&key).is_some_and(|holders| {
                holders.iter().any(|(&other, held)| other != pid && if shared { held.exclusive() } else { held.any() })
            });
            if !conflict {
                *table.entry(key).or_default().entry(pid).or_default().count(shared, xact) += 1;
                return Ok(true);
            }
            match &mut wait {
                Wait::No => return Ok(false),
                Wait::Yes { lock_timeout, interrupted } => {
                    if lock_timeout.is_some_and(|t| started.elapsed() >= t) {
                        return Err(Error::new(
                            sqlstate::LOCK_NOT_AVAILABLE,
                            "canceling statement due to lock timeout",
                        ));
                    }
                    self.released.wait_for(&mut table, POLL);
                    // Interrupts are checked without the table locked.
                    let result = parking_lot::MutexGuard::unlocked(&mut table, || interrupted());
                    result?;
                }
            }
        }
    }

    /// Releases one session-level lock. False if the session holds none
    /// of that kind on the key.
    pub(crate) fn release(&self, pid: i32, key: Key, shared: bool) -> bool {
        let mut table = self.table.lock();
        let Some(holders) = table.get_mut(&key) else { return false };
        let Some(held) = holders.get_mut(&pid) else { return false };
        let n = held.count(shared, false);
        if *n == 0 {
            return false;
        }
        *n -= 1;
        if !held.any() {
            holders.remove(&pid);
            if holders.is_empty() {
                table.remove(&key);
            }
        }
        drop(table);
        self.released.notify_all();
        true
    }

    /// Releases the transaction-level locks of a session (at the end of
    /// its transaction), or with `session` also all its session-level
    /// locks.
    pub(crate) fn release_all(&self, pid: i32, xact: bool, session: bool) {
        let mut table = self.table.lock();
        let mut changed = false;
        table.retain(|_, holders| {
            if let Some(held) = holders.get_mut(&pid) {
                if xact && held.exclusive_xact + held.shared_xact > 0 {
                    held.exclusive_xact = 0;
                    held.shared_xact = 0;
                    changed = true;
                }
                if session && held.exclusive_session + held.shared_session > 0 {
                    held.exclusive_session = 0;
                    held.shared_session = 0;
                    changed = true;
                }
                if !held.any() {
                    holders.remove(&pid);
                }
            }
            !holders.is_empty()
        });
        drop(table);
        if changed {
            self.released.notify_all();
        }
    }
}

/// Implements the advisory lock functions for `catalog_fn`. `None` if
/// `name` is not one of them.
pub(crate) fn call(
    name: &str,
    args: &[crate::types::Value],
    cx: &mut crate::exec::ExecCx,
) -> Option<Result<crate::types::Value>> {
    use crate::types::Value;
    if !name.contains("advisory") {
        return None;
    }
    let pid = cx.session.pid;
    let locks = cx.shared.advisory.clone();
    if name == "pg_advisory_unlock_all" {
        locks.release_all(pid, false, true);
        return Some(Ok(Value::Null));
    }
    // The functions are strict: a NULL argument gives NULL and no lock.
    if args.iter().any(Value::is_null) {
        return Some(Ok(Value::Null));
    }
    let key = match args {
        [Value::Int8(id)] => Key::single(*id),
        [a] => match a.as_i64() {
            Some(id) => Key::single(id),
            None => return Some(Err(Error::internal(format!("{name}: bad argument")))),
        },
        [a, b] => match (a.as_i64(), b.as_i64()) {
            (Some(a), Some(b)) => Key::pair(a as i32, b as i32),
            _ => return Some(Err(Error::internal(format!("{name}: bad arguments")))),
        },
        _ => return Some(Err(Error::internal(format!("{name}: bad arguments")))),
    };
    let shared = name.ends_with("_shared");
    let xact = name.contains("_xact_");
    if name.starts_with("pg_advisory_unlock") {
        let released = locks.release(pid, key, shared);
        if !released {
            let mode = if shared { "ShareLock" } else { "ExclusiveLock" };
            cx.session.notices.push(crate::Notice::warning(format!("you don't own a lock of type {mode}")));
        }
        return Some(Ok(Value::Bool(released)));
    }
    let lock_timeout = cx.session.lock_timeout;
    let result = if name.starts_with("pg_try_") {
        locks.acquire(pid, key, shared, xact, Wait::No).map(Value::Bool)
    } else {
        let mut interrupted = || cx.check_interrupts_now();
        locks
            .acquire(pid, key, shared, xact, Wait::Yes { lock_timeout, interrupted: &mut interrupted })
            .map(|_| Value::Null)
    };
    Some(result)
}
