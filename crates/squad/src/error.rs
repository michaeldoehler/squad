//! Error type carrying a Postgres SQLSTATE code.

use std::fmt;

use squad_kv::KvError;
use squad_storage::StorageError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// SQLSTATE codes used by squad. The values are identical to Postgres.
pub mod sqlstate {
    pub const SUCCESSFUL_COMPLETION: &str = "00000";
    pub const WARNING: &str = "01000";
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const CARDINALITY_VIOLATION: &str = "21000";
    pub const STRING_DATA_RIGHT_TRUNCATION: &str = "22001";
    pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";
    pub const NULL_VALUE_NOT_ALLOWED: &str = "22004";
    pub const INVALID_DATETIME_FORMAT: &str = "22007";
    pub const DATETIME_FIELD_OVERFLOW: &str = "22008";
    pub const DIVISION_BY_ZERO: &str = "22012";
    pub const INVALID_ESCAPE_SEQUENCE: &str = "22025";
    pub const INVALID_PARAMETER_VALUE: &str = "22023";
    pub const INVALID_REGULAR_EXPRESSION: &str = "2201B";
    pub const INVALID_ARGUMENT_FOR_LOG: &str = "2201E";
    pub const INVALID_ARGUMENT_FOR_POWER: &str = "2201F";
    pub const SUBSTRING_ERROR: &str = "22011";
    pub const ARRAY_SUBSCRIPT_ERROR: &str = "2202E";
    pub const INVALID_TEXT_REPRESENTATION: &str = "22P02";
    pub const INVALID_BINARY_REPRESENTATION: &str = "22P03";
    pub const INVALID_JSON_TEXT: &str = "22032";
    pub const CHARACTER_NOT_IN_REPERTOIRE: &str = "22021";
    pub const INTEGRITY_CONSTRAINT_VIOLATION: &str = "23000";
    pub const NOT_NULL_VIOLATION: &str = "23502";
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const UNIQUE_VIOLATION: &str = "23505";
    pub const CHECK_VIOLATION: &str = "23514";
    pub const ACTIVE_SQL_TRANSACTION: &str = "25001";
    pub const READ_ONLY_SQL_TRANSACTION: &str = "25006";
    pub const NO_ACTIVE_SQL_TRANSACTION: &str = "25P01";
    pub const IN_FAILED_SQL_TRANSACTION: &str = "25P02";
    pub const INVALID_SQL_STATEMENT_NAME: &str = "26000";
    pub const INVALID_AUTHORIZATION_SPECIFICATION: &str = "28000";
    pub const INVALID_PASSWORD: &str = "28P01";
    pub const DEPENDENT_OBJECTS_STILL_EXIST: &str = "2BP01";
    pub const INVALID_CURSOR_NAME: &str = "34000";
    pub const INVALID_SAVEPOINT_SPECIFICATION: &str = "3B001";
    pub const INVALID_CATALOG_NAME: &str = "3D000";
    pub const INVALID_SCHEMA_NAME: &str = "3F000";
    pub const SERIALIZATION_FAILURE: &str = "40001";
    pub const DEADLOCK_DETECTED: &str = "40P01";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const INSUFFICIENT_PRIVILEGE: &str = "42501";
    pub const CANNOT_COERCE: &str = "42846";
    pub const GROUPING_ERROR: &str = "42803";
    pub const WINDOWING_ERROR: &str = "42P20";
    pub const INVALID_RECURSION: &str = "42P19";
    pub const INVALID_FOREIGN_KEY: &str = "42830";
    pub const INVALID_NAME: &str = "42602";
    pub const DATATYPE_MISMATCH: &str = "42804";
    pub const INDETERMINATE_DATATYPE: &str = "42P18";
    pub const WRONG_OBJECT_TYPE: &str = "42809";
    pub const UNDEFINED_COLUMN: &str = "42703";
    pub const UNDEFINED_FUNCTION: &str = "42883";
    pub const UNDEFINED_TABLE: &str = "42P01";
    pub const UNDEFINED_PARAMETER: &str = "42P02";
    pub const UNDEFINED_OBJECT: &str = "42704";
    pub const DUPLICATE_COLUMN: &str = "42701";
    pub const DUPLICATE_CURSOR: &str = "42P03";
    pub const DUPLICATE_DATABASE: &str = "42P04";
    pub const DUPLICATE_FUNCTION: &str = "42723";
    pub const DUPLICATE_PREPARED_STATEMENT: &str = "42P05";
    pub const DUPLICATE_SCHEMA: &str = "42P06";
    pub const DUPLICATE_TABLE: &str = "42P07";
    pub const DUPLICATE_ALIAS: &str = "42712";
    pub const DUPLICATE_OBJECT: &str = "42710";
    pub const AMBIGUOUS_COLUMN: &str = "42702";
    pub const AMBIGUOUS_FUNCTION: &str = "42725";
    pub const INVALID_COLUMN_REFERENCE: &str = "42P10";
    pub const INVALID_TABLE_DEFINITION: &str = "42P16";
    pub const INVALID_OBJECT_DEFINITION: &str = "42P17";
    pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000";
    pub const STATEMENT_TOO_COMPLEX: &str = "54001";
    pub const OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55000";
    pub const LOCK_NOT_AVAILABLE: &str = "55P03";
    pub const QUERY_CANCELED: &str = "57014";
    pub const ADMIN_SHUTDOWN: &str = "57P01";
    pub const TOO_MANY_CONNECTIONS: &str = "53300";
    pub const DISK_FULL: &str = "53100";
    pub const OUT_OF_MEMORY: &str = "53200";
    pub const IO_ERROR: &str = "58030";
    pub const PROTOCOL_VIOLATION: &str = "08P01";
    pub const INTERNAL_ERROR: &str = "XX000";
    pub const DATA_CORRUPTED: &str = "XX001";
}

#[derive(Debug, Clone)]
struct ErrorData {
    code: &'static str,
    message: String,
    detail: Option<String>,
    hint: Option<String>,
    position: Option<usize>,
}

/// A database error with the same fields a Postgres `ErrorResponse` has.
#[derive(Debug, Clone)]
pub struct Error(Box<ErrorData>);

impl Error {
    pub fn new(code: &'static str, message: impl Into<String>) -> Error {
        Error(Box::new(ErrorData { code, message: message.into(), detail: None, hint: None, position: None }))
    }

    /// The five-character SQLSTATE code, e.g. `"23505"`.
    pub fn code(&self) -> &'static str {
        self.0.code
    }
    pub fn message(&self) -> &str {
        &self.0.message
    }
    pub fn detail(&self) -> Option<&str> {
        self.0.detail.as_deref()
    }
    pub fn hint(&self) -> Option<&str> {
        self.0.hint.as_deref()
    }
    /// 1-based character offset into the query text, if known.
    pub fn position(&self) -> Option<usize> {
        self.0.position
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Error {
        self.0.detail = Some(detail.into());
        self
    }
    pub fn with_hint(mut self, hint: impl Into<String>) -> Error {
        self.0.hint = Some(hint.into());
        self
    }
    pub fn with_position(mut self, position: usize) -> Error {
        self.0.position = Some(position);
        self
    }

    pub fn syntax(message: impl Into<String>) -> Error {
        Error::new(sqlstate::SYNTAX_ERROR, message)
    }
    pub fn unsupported(what: impl fmt::Display) -> Error {
        Error::new(sqlstate::FEATURE_NOT_SUPPORTED, format!("not supported: {what}"))
    }
    pub fn internal(message: impl Into<String>) -> Error {
        Error::new(sqlstate::INTERNAL_ERROR, message)
    }
    pub fn type_mismatch(message: impl Into<String>) -> Error {
        Error::new(sqlstate::DATATYPE_MISMATCH, message)
    }
    pub fn invalid_text(type_name: &str, input: &str) -> Error {
        Error::new(
            sqlstate::INVALID_TEXT_REPRESENTATION,
            format!("invalid input syntax for type {type_name}: \"{input}\""),
        )
    }
    pub fn out_of_range(type_name: &str) -> Error {
        Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, format!("{type_name} out of range"))
    }
    pub fn division_by_zero() -> Error {
        Error::new(sqlstate::DIVISION_BY_ZERO, "division by zero")
    }
}

/// How serious a [`Notice`] is, as in a Postgres `NoticeResponse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Warning,
    Notice,
}

impl Severity {
    /// The severity as Postgres spells it: `WARNING` or `NOTICE`.
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Warning => "WARNING",
            Severity::Notice => "NOTICE",
        }
    }
}

/// A message about a statement that succeeded, like a Postgres
/// `NoticeResponse`. squad uses notices to say that a statement was
/// accepted but had no effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub severity: Severity,
    /// SQLSTATE: `01000` for warnings, `00000` for notices.
    pub code: &'static str,
    pub message: String,
    pub hint: Option<String>,
}

impl Notice {
    pub fn warning(message: impl Into<String>) -> Notice {
        Notice { severity: Severity::Warning, code: sqlstate::WARNING, message: message.into(), hint: None }
    }

    pub fn info(message: impl Into<String>) -> Notice {
        Notice {
            severity: Severity::Notice,
            code: sqlstate::SUCCESSFUL_COMPLETION,
            message: message.into(),
            hint: None,
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Notice {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_code(mut self, code: &'static str) -> Notice {
        self.code = code;
        self
    }
}

impl fmt::Display for Notice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.severity.as_str(), self.message)?;
        if let Some(h) = &self.hint {
            write!(f, "\nHINT: {h}")?;
        }
        Ok(())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ERROR: {} (SQLSTATE {})", self.0.message, self.0.code)?;
        if let Some(d) = &self.0.detail {
            write!(f, "\nDETAIL: {d}")?;
        }
        if let Some(h) = &self.0.hint {
            write!(f, "\nHINT: {h}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Error {
        let code = match &e {
            StorageError::Io(_) => sqlstate::IO_ERROR,
            StorageError::Corruption(_) => sqlstate::DATA_CORRUPTED,
            StorageError::KeyTooLarge(_) => sqlstate::PROGRAM_LIMIT_EXCEEDED,
            StorageError::Locked(_) => sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
            _ => sqlstate::INTERNAL_ERROR,
        };
        let message = match &e {
            StorageError::KeyTooLarge(n) => format!("index row size {n} exceeds the maximum key size"),
            other => other.to_string(),
        };
        Error::new(code, message)
    }
}

impl From<KvError> for Error {
    fn from(e: KvError) -> Error {
        match e {
            KvError::Storage(s) => s.into(),
            KvError::Deadlock => Error::new(sqlstate::DEADLOCK_DETECTED, "deadlock detected"),
            KvError::LockNotAvailable => Error::new(sqlstate::LOCK_NOT_AVAILABLE, "could not obtain lock on row"),
            KvError::LockTimeout => Error::new(sqlstate::LOCK_NOT_AVAILABLE, "canceling statement due to lock timeout"),
            KvError::SerializationFailure => {
                Error::new(sqlstate::SERIALIZATION_FAILURE, "could not serialize access due to concurrent update")
            }
            KvError::SnapshotTooOld => Error::new(
                sqlstate::SERIALIZATION_FAILURE,
                "canceling statement due to conflict with recovery: the replica replaced the state this snapshot reads",
            ),
            KvError::ReadOnly => {
                Error::new(sqlstate::READ_ONLY_SQL_TRANSACTION, "cannot execute statement in a read-only transaction")
            }
        }
    }
}
