//! The sessions of a database as `pg_stat_activity` lists them, and
//! `pg_cancel_backend` / `pg_terminate_backend`.
//!
//! Every session opened through [`Database::connect`](crate::Database),
//! [`Database::session`](crate::Database) or the server registers here
//! under its backend pid; the database's own background sessions do not.
//! A session reports what it is doing at the start and end of every
//! statement.
//!
//! Cancelling sets the session's cancel flag, exactly like a protocol
//! cancel request. Terminating sets the flag and marks the session as
//! terminated: its running statement fails with `57P01` ("terminating
//! connection due to administrator command"), and so does every later
//! statement. A server connection then sends that error as FATAL and
//! closes; an idle server connection is woken through the hook it
//! installed (see [`Session::set_terminate_hook`](crate::Session)).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::exec::ExecCx;
use crate::session::CancelHandle;
use crate::{sqlstate, Error, Notice, Result, Value};

/// What a session was last seen doing.
#[derive(Debug, Clone)]
pub(crate) struct Activity {
    pub(crate) user: String,
    pub(crate) application_name: String,
    /// `active`, `idle`, `idle in transaction` or `idle in transaction
    /// (aborted)`.
    pub(crate) state: &'static str,
    /// The running statement, or the last one while idle.
    pub(crate) query: String,
    pub(crate) query_start: Option<i64>,
    pub(crate) xact_start: Option<i64>,
    pub(crate) state_change: i64,
}

pub(crate) struct Backend {
    pub(crate) pid: i32,
    pub(crate) cancel: CancelHandle,
    /// Microseconds since the Postgres epoch.
    pub(crate) backend_start: i64,
    pub(crate) activity: Mutex<Activity>,
    /// Wakes the owner of the session after it was terminated, e.g. a
    /// server connection waiting for its client.
    terminate_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl Backend {
    pub(crate) fn set_terminate_hook(&self, hook: Box<dyn Fn() + Send + Sync>) {
        *self.terminate_hook.lock() = Some(hook);
    }
}

/// The registered sessions of one database.
#[derive(Default)]
pub(crate) struct Backends {
    map: Mutex<HashMap<i32, Arc<Backend>>>,
    /// Notified whenever a session goes away.
    gone: Condvar,
}

impl Backends {
    pub(crate) fn register(&self, pid: i32, cancel: CancelHandle, now: i64, user: &str) -> Arc<Backend> {
        let backend = Arc::new(Backend {
            pid,
            cancel,
            backend_start: now,
            activity: Mutex::new(Activity {
                user: user.to_string(),
                application_name: String::new(),
                state: "idle",
                query: String::new(),
                query_start: None,
                xact_start: None,
                state_change: now,
            }),
            terminate_hook: Mutex::new(None),
        });
        self.map.lock().insert(pid, backend.clone());
        backend
    }

    pub(crate) fn unregister(&self, pid: i32) {
        self.map.lock().remove(&pid);
        self.gone.notify_all();
    }

    /// All registered sessions, in pid order.
    pub(crate) fn list(&self) -> Vec<Arc<Backend>> {
        let mut all: Vec<Arc<Backend>> = self.map.lock().values().cloned().collect();
        all.sort_by_key(|b| b.pid);
        all
    }

    fn get(&self, pid: i32) -> Option<Arc<Backend>> {
        self.map.lock().get(&pid).cloned()
    }

    /// `pg_cancel_backend`: false if no session has this pid.
    pub(crate) fn cancel(&self, pid: i32) -> bool {
        match self.get(pid) {
            Some(b) => {
                b.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// `pg_terminate_backend`: false if no session has this pid.
    pub(crate) fn terminate(&self, pid: i32) -> bool {
        let Some(b) = self.get(pid) else { return false };
        b.cancel.terminate();
        if let Some(hook) = b.terminate_hook.lock().as_ref() {
            hook();
        }
        true
    }

    /// Waits until the session with this pid is gone; false on timeout.
    pub(crate) fn wait_gone(&self, pid: i32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut map = self.map.lock();
        while map.contains_key(&pid) {
            if self.gone.wait_until(&mut map, deadline).timed_out() {
                return !map.contains_key(&pid);
            }
        }
        true
    }
}

/// `pg_cancel_backend(pid)` and `pg_terminate_backend(pid [, timeout])`,
/// or `None` for other functions.
pub(crate) fn call(name: &str, args: &[Value], cx: &mut ExecCx) -> Option<Result<Value>> {
    let terminate = match name {
        "pg_cancel_backend" => false,
        "pg_terminate_backend" => true,
        _ => return None,
    };
    Some(signal(name, terminate, args, cx))
}

fn integer(name: &str, v: &Value, type_name: &str) -> Result<i64> {
    match v {
        Value::Text(s) => s.trim().parse().map_err(|_| Error::invalid_text(type_name, s)),
        v => v.as_i64().ok_or_else(|| {
            Error::new(sqlstate::UNDEFINED_FUNCTION, format!("function {name}({}) does not exist", v.type_of().name()))
        }),
    }
}

fn signal(name: &str, terminate: bool, args: &[Value], cx: &mut ExecCx) -> Result<Value> {
    // Strict functions: NULL in, NULL out.
    if args.iter().any(Value::is_null) {
        return Ok(Value::Null);
    }
    let pid = integer(name, &args[0], "integer")?;
    let pid = i32::try_from(pid).map_err(|_| Error::out_of_range("integer"))?;
    let timeout = match args.get(1) {
        Some(v) => integer(name, v, "bigint")?,
        None => 0,
    };
    if timeout < 0 {
        return Err(Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "\"timeout\" must not be negative"));
    }
    let backends = &cx.shared.backends;
    let signalled = if terminate { backends.terminate(pid) } else { backends.cancel(pid) };
    if !signalled {
        cx.session.notices.push(Notice::warning(format!("PID {pid} is not a PostgreSQL backend process")));
        return Ok(Value::Bool(false));
    }
    if pid == cx.session.pid {
        // As in PostgreSQL, the signal reaches the calling statement
        // itself at once.
        cx.check_interrupts_now()?;
    }
    if terminate && timeout > 0 {
        let deadline = Instant::now() + Duration::from_millis(timeout as u64);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if cx.shared.backends.wait_gone(pid, left.min(Duration::from_millis(100))) {
                break;
            }
            cx.check_interrupts_now()?;
            if left.is_zero() {
                cx.session.notices.push(Notice::warning(format!(
                    "backend with PID {pid} did not terminate within {timeout} milliseconds"
                )));
                return Ok(Value::Bool(false));
            }
        }
    }
    Ok(Value::Bool(true))
}
