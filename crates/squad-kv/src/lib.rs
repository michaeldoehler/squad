//! Transactional key-value layer of the squad database.
//!
//! [`Store`] turns a plain ordered [`Engine`](squad_storage::Engine) into a
//! multi-version store with snapshot reads, buffered writes, row locks with
//! deadlock detection, savepoints and optional serializable validation.
//!
//! # Model
//!
//! Every committed write is stored under `(user key, commit timestamp)`, with
//! newer versions sorting first. A transaction reads at a snapshot timestamp
//! and buffers its own writes in memory; at commit the buffered writes are
//! stamped with a fresh timestamp and handed to the engine as one atomic
//! batch. Readers therefore never block writers and writers never block
//! readers. Versions that no active snapshot can see are removed lazily when
//! the key is written again and by [`Store::gc`].
//!
//! ```
//! use squad_kv::{Store, Isolation};
//! let store = Store::in_memory();
//! let mut tx = store.begin(Isolation::RepeatableRead);
//! tx.put(b"k", b"v")?;
//! tx.commit()?;
//! let tx = store.begin(Isolation::ReadCommitted);
//! assert_eq!(tx.get(b"k")?.as_deref(), Some(&b"v"[..]));
//! # Ok::<(), squad_kv::KvError>(())
//! ```

mod codec;
mod error;
mod lock;
mod store;
mod txn;

pub use error::{KvError, Result};
pub use lock::{LockMode, WaitPolicy};
pub use store::Store;
pub use txn::{Cursor, Isolation, Txn};
