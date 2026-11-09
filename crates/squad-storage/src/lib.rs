//! Storage layer of the squad database.
//!
//! This crate provides a durable, ordered key-value map behind the [`Engine`]
//! trait. Two implementations exist:
//!
//! * [`MemEngine`] – a volatile in-memory engine, used for tests and for
//!   `Database::open_in_memory`.
//! * [`DiskEngine`] – a page-based B+tree with a buffer pool, a write-ahead
//!   log, fuzzy checkpoints and crash recovery.
//!
//! The engine knows nothing about transactions or SQL. Multi-versioning,
//! snapshots and row locks live one layer up, in the `squad-kv` crate, which
//! encodes versions into the keys it hands to the engine.

mod btree;
mod disk;
mod engine;
mod error;
mod mem;
mod page;
mod pool;
mod sim;
mod vfs;
mod wal;

pub use disk::{DiskEngine, DiskOptions};
pub use engine::{Engine, EngineStats, SyncMode, VerifyReport, WriteBatch, MAX_KEY_LEN};
pub use error::{Result, StorageError};
pub use mem::MemEngine;
pub use sim::SimVfs;
pub use vfs::{RealVfs, Vfs, VfsFile, VfsLock};
