//! squad is an embeddable OLTP database that behaves like PostgreSQL.
//!
//! It runs inside your process as a library: there is no server to
//! operate and no network hop between the application and its data. SQL
//! dialect, data types, error codes (SQLSTATE) and transaction semantics
//! follow Postgres, and unlike single-writer embedded databases it uses
//! multi-version concurrency control, so many connections can write at
//! the same time.
//!
//! ```
//! use squad::{Config, Database};
//!
//! # fn main() -> squad::Result<()> {
//! # let dir = tempfile::tempdir().unwrap();
//! let db = Database::open(dir.path(), Config::default())?;
//! let conn = db.connect();
//! conn.batch_execute(
//!     "CREATE TABLE users (id bigint PRIMARY KEY, name text NOT NULL);
//!      INSERT INTO users VALUES (42, 'Ada');",
//! )?;
//! let stmt = conn.prepare("SELECT name FROM users WHERE id = $1")?;
//! let name: String = stmt.query_one(&[&42i64])?.get(0);
//! assert_eq!(name, "Ada");
//! # Ok(())
//! # }
//! ```

mod agg;
mod api;
pub mod bind;
pub mod catalog;
mod convert;
mod ddl;
mod dml;
pub mod encoding;
pub mod error;
pub mod exec;
mod explain;
pub mod expr;
pub mod plan;
pub mod session;
mod system;
pub mod types;
pub mod value;

pub use api::{Config, Connection, Database, Row, RowIndex, Statement, Transaction};
pub use bind::OutputColumn;
pub use convert::{FromSql, ToSql};
pub use error::{sqlstate, Error, Result};
pub use session::{QueryResult, Session, TxnStatus};
pub use squad_storage::{SyncMode, VerifyReport};
pub use types::{FnCtx, Interval, Type, Value};

/// Postgres version squad reports to clients and aims to be compatible
/// with.
pub const SERVER_VERSION: &str = "16.4";
pub const SERVER_VERSION_NUM: &str = "160004";
/// Result of `SELECT version()`.
pub const VERSION_STRING: &str = concat!("PostgreSQL 16.4 (squad ", env!("CARGO_PKG_VERSION"), ")");
