use squad_storage::StorageError;

pub type Result<T, E = KvError> = std::result::Result<T, E>;

/// Errors of the transactional layer.
#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Waiting for a row lock would close a cycle of waiting transactions.
    #[error("deadlock detected")]
    Deadlock,
    /// `NOWAIT` was requested and the lock is held by another transaction.
    #[error("could not obtain lock on row")]
    LockNotAvailable,
    /// The lock wait exceeded the configured timeout.
    #[error("canceling statement due to lock timeout")]
    LockTimeout,
    /// Another transaction committed a change to a key this transaction
    /// wrote or (under serializable isolation) read.
    #[error("could not serialize access due to concurrent update")]
    SerializationFailure,
    /// A write was attempted in a read-only transaction.
    #[error("cannot execute write in a read-only transaction")]
    ReadOnly,
}
