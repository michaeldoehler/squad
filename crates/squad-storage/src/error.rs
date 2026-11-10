use std::io;

/// Result alias used throughout the storage layer.
pub type Result<T, E = StorageError> = std::result::Result<T, E>;

/// Errors raised by a storage [`Engine`](crate::Engine).
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// An operating-system level I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// On-disk data failed an integrity check (bad checksum, impossible
    /// structure, unknown format version).
    #[error("data corruption detected: {0}")]
    Corruption(String),
    /// The key exceeds [`MAX_KEY_LEN`](crate::MAX_KEY_LEN).
    #[error("key of {0} bytes exceeds the maximum key length")]
    KeyTooLarge(usize),
    /// The database directory is already opened by another process.
    #[error("database is locked by another process: {0}")]
    Locked(String),
    /// The engine hit an earlier unrecoverable error and refuses further
    /// writes until it is reopened.
    #[error("storage engine is in a failed state: {0}")]
    Poisoned(String),
    /// The remote object store failed, or another writer took over the
    /// remote location.
    #[error("remote storage: {0}")]
    Remote(String),
    /// Invalid configuration or argument.
    #[error("invalid argument: {0}")]
    Invalid(String),
}
