//! Object storage for the squad database.
//!
//! An [`ObjectStore`] is a flat namespace of immutable byte objects under
//! `/`-separated keys, the common denominator of a local directory and
//! S3-compatible services. squad uses it as the durable home of a
//! database whose local directory is only a working copy.
//!
//! Implementations:
//!
//! * [`MemoryStore`] – volatile, for tests.
//! * [`LocalStore`] – a directory; puts are atomic and durable.
//! * [`S3Store`] – Amazon S3 and compatible services (feature `s3`).
//! * [`CachedStore`] – wraps another store with a bounded in-memory cache.
//!
//! [`open_url`] builds a store from a URL such as `file:///var/backup`,
//! `memory://` or `s3://bucket/prefix?region=eu-central-1`.

mod cache;
mod local;
mod memory;
#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "s3")]
mod sigv4;
mod url;

#[cfg(feature = "test-util")]
pub mod emulator;

pub use cache::CachedStore;
pub use local::LocalStore;
pub use memory::{Fault, MemoryStore};
#[cfg(feature = "s3")]
pub use s3::{S3Config, S3Stats, S3Store};
pub use url::open_url;

use std::io;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors of an object store.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object store I/O error: {0}")]
    Io(#[from] io::Error),
    /// The key, URL or configuration is not acceptable.
    #[error("invalid object store argument: {0}")]
    Invalid(String),
    /// The service answered with an error status.
    #[error("object store request failed with status {status}: {message}")]
    Remote { status: u16, message: String },
    /// The service could not be reached or the connection broke, after
    /// all retries.
    #[error("object store unreachable: {0}")]
    Transport(String),
}

/// Key and size of a stored object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
}

/// A store of immutable byte objects.
///
/// Keys are `/`-separated UTF-8 paths without empty, `.` or `..`
/// segments. All operations are safe to call from many threads.
pub trait ObjectStore: Send + Sync + 'static {
    /// Atomically creates or replaces the object. When this returns the
    /// object is durable.
    fn put(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Creates the object only if no object with this key exists. Returns
    /// `false`, leaving the existing object untouched, if one does.
    ///
    /// The provided implementation checks and then writes, which is not
    /// atomic; the stores in this crate override it with an atomic
    /// operation (for S3: a conditional `PUT` with `If-None-Match: *`).
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<bool> {
        if self.head(key)?.is_some() {
            return Ok(false);
        }
        self.put(key, data)?;
        Ok(true)
    }

    /// The whole object, or `None` if it does not exist.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Reads exactly `len` bytes at `offset`. A missing object or a range
    /// reaching beyond its end is an error.
    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>>;

    /// Size of the object, or `None` if it does not exist.
    fn head(&self, key: &str) -> Result<Option<ObjectMeta>>;

    /// All objects whose key starts with `prefix`, sorted by key.
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;

    /// The objects whose key starts with `prefix` and sorts after
    /// `after`, sorted by key. For a reader that follows keys which only
    /// ever grow, this costs in proportion to what is new, where the
    /// store supports it.
    fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>> {
        let mut all = self.list(prefix)?;
        all.retain(|m| m.key.as_str() > after);
        Ok(all)
    }

    /// Deletes the object. Deleting a missing object succeeds.
    fn delete(&self, key: &str) -> Result<()>;

    /// Stores the contents of the local file `path` as the object, like
    /// [`put`](ObjectStore::put). The provided implementation reads the
    /// file into memory; the stores in this crate copy or upload it in
    /// pieces, so that large objects need not fit in memory.
    fn put_file(&self, key: &str, path: &std::path::Path) -> Result<()> {
        let data = std::fs::read(path)?;
        self.put(key, &data)
    }
}

/// Rejects keys that could escape a directory or are ambiguous.
pub(crate) fn check_key(key: &str) -> Result<()> {
    let ok = !key.is_empty()
        && !key.starts_with('/')
        && !key.ends_with('/')
        && key.split('/').all(|s| !s.is_empty() && s != "." && s != "..")
        && !key.contains('\0')
        && !key.contains('\\');
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid(format!("malformed object key {key:?}")))
    }
}

pub(crate) fn range_of(key: &str, data: &[u8], offset: u64, len: u64) -> Result<Vec<u8>> {
    let end = offset.checked_add(len).filter(|e| *e <= data.len() as u64).ok_or_else(|| {
        Error::Invalid(format!("range {offset}+{len} lies beyond the end of object {key:?} ({} bytes)", data.len()))
    })?;
    Ok(data[offset as usize..end as usize].to_vec())
}

fn missing(key: &str) -> Error {
    Error::Remote { status: 404, message: format!("object {key:?} does not exist") }
}

impl<T: ObjectStore + ?Sized> ObjectStore for std::sync::Arc<T> {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        (**self).put(key, data)
    }
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<bool> {
        (**self).put_if_absent(key, data)
    }
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        (**self).get(key)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        (**self).get_range(key, offset, len)
    }
    fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        (**self).head(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        (**self).list(prefix)
    }
    fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>> {
        (**self).list_after(prefix, after)
    }
    fn delete(&self, key: &str) -> Result<()> {
        (**self).delete(key)
    }
}
