//! One shard as a connection sees it: a local database in this process or
//! a remote squad server reached over the Postgres wire protocol.

use std::collections::HashMap;
use std::sync::OnceLock;

use parking_lot::Mutex;
use postgres::types::{private::BytesMut, FromSql, IsNull, ToSql as PgToSql, Type as PgType};
use squad::{sqlstate, Error, FnCtx, OutputColumn, Result, ToSql, Type, Value};

/// Result of one statement on one shard.
#[derive(Debug, Clone, Default)]
pub(crate) struct Output {
    pub columns: Vec<OutputColumn>,
    pub rows: Vec<Vec<Value>>,
    pub affected: u64,
}

pub(crate) enum ShardConn {
    Local { conn: squad::Connection, stmts: HashMap<String, squad::Statement> },
    Remote { client: Box<postgres::Client>, stmts: HashMap<String, postgres::Statement> },
}

/// Prepared statements kept per shard connection.
const STATEMENT_CACHE: usize = 512;

/// A value travelling to or from a remote shard in binary format.
#[derive(Debug)]
struct Wire(Value);

impl PgToSql for Wire {
    fn to_sql(&self, ty: &PgType, out: &mut BytesMut) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if self.0.is_null() {
            return Ok(IsNull::Yes);
        }
        let mut buf = Vec::new();
        match Type::from_oid(ty.oid()) {
            Some(target) => {
                let v = squad::value::cast(self.0.clone(), &target.base(), &FnCtx::default())?;
                squad::value::encode_binary(&v, &target, &mut buf)?;
            }
            // A type only the server knows (an enum): its text form.
            None => buf.extend_from_slice(squad::value::format_value(&self.0, &FnCtx::default()).as_bytes()),
        }
        out.extend_from_slice(&buf);
        Ok(IsNull::No)
    }

    fn accepts(_: &PgType) -> bool {
        true
    }

    postgres::types::to_sql_checked!();
}

impl<'a> FromSql<'a> for Wire {
    fn from_sql(ty: &PgType, raw: &'a [u8]) -> std::result::Result<Wire, Box<dyn std::error::Error + Sync + Send>> {
        let target = Type::from_oid(ty.oid()).unwrap_or(Type::Text);
        Ok(Wire(squad::value::decode_binary(raw, &target)?))
    }

    fn from_sql_null(_: &PgType) -> std::result::Result<Wire, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Wire(Value::Null))
    }

    fn accepts(_: &PgType) -> bool {
        true
    }
}

/// `squad::Error` wants a `'static` SQLSTATE; codes from a remote server
/// are interned (there are only a few hundred distinct ones).
fn static_code(code: &str) -> &'static str {
    static CODES: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let mut codes = CODES.get_or_init(Default::default).lock();
    if let Some(c) = codes.get(code) {
        return c;
    }
    let leaked: &'static str = Box::leak(code.to_string().into_boxed_str());
    codes.insert(code.to_string(), leaked);
    leaked
}

fn remote_error(e: postgres::Error) -> Error {
    match e.as_db_error() {
        Some(db) => {
            let mut err = Error::new(static_code(db.code().code()), db.message());
            if let Some(d) = db.detail() {
                err = err.with_detail(d);
            }
            if let Some(h) = db.hint() {
                err = err.with_hint(h);
            }
            err
        }
        None => Error::new(sqlstate::IO_ERROR, format!("remote shard: {e}")),
    }
}

impl ShardConn {
    pub(crate) fn connect_remote(url: &str) -> Result<ShardConn> {
        let client = postgres::Client::connect(url, postgres::NoTls)
            .map_err(|e| Error::new(sqlstate::IO_ERROR, format!("cannot connect to shard {url}: {e}")))?;
        Ok(ShardConn::Remote { client: Box::new(client), stmts: HashMap::new() })
    }

    /// Forgets prepared statements, after the schema changed.
    pub(crate) fn reset(&mut self) {
        match self {
            ShardConn::Local { stmts, .. } => stmts.clear(),
            ShardConn::Remote { stmts, .. } => stmts.clear(),
        }
    }

    /// Runs one statement.
    pub(crate) fn run(&mut self, sql: &str, params: &[Value]) -> Result<Output> {
        match self {
            ShardConn::Local { conn, stmts } => {
                if params.is_empty() {
                    // The engine caches plans of parameterless text itself.
                    let last = conn.simple_query(sql)?.pop().unwrap_or_default();
                    return Ok(Output { columns: last.columns, rows: last.rows, affected: last.rows_affected });
                }
                if stmts.len() >= STATEMENT_CACHE {
                    stmts.clear();
                }
                let stmt = match stmts.get(sql) {
                    Some(s) => s,
                    None => {
                        let s = conn.prepare(sql)?;
                        stmts.entry(sql.to_string()).or_insert(s)
                    }
                };
                let refs: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
                if stmt.columns().is_empty() {
                    Ok(Output { affected: stmt.execute(&refs)?, ..Default::default() })
                } else {
                    let rows: Vec<Vec<Value>> = stmt.query(&refs)?.iter().map(|r| r.values().to_vec()).collect();
                    Ok(Output { columns: stmt.columns().to_vec(), affected: rows.len() as u64, rows })
                }
            }
            ShardConn::Remote { client, stmts } => {
                if stmts.len() >= STATEMENT_CACHE {
                    stmts.clear();
                }
                let stmt = match stmts.get(sql) {
                    Some(s) => s.clone(),
                    None => {
                        let s = client.prepare(sql).map_err(remote_error)?;
                        stmts.insert(sql.to_string(), s.clone());
                        s
                    }
                };
                // Parameters the rewritten statement no longer mentions at
                // its end are not declared by the server.
                let wire: Vec<Wire> = params.iter().take(stmt.params().len()).cloned().map(Wire).collect();
                let refs: Vec<&(dyn PgToSql + Sync)> = wire.iter().map(|w| w as &(dyn PgToSql + Sync)).collect();
                if stmt.columns().is_empty() {
                    return Ok(Output { affected: client.execute(&stmt, &refs).map_err(remote_error)?, ..Default::default() });
                }
                let columns: Vec<OutputColumn> = stmt
                    .columns()
                    .iter()
                    .map(|c| OutputColumn { name: c.name().to_string(), ty: Type::from_oid(c.type_().oid()).unwrap_or(Type::Text) })
                    .collect();
                let mut rows = Vec::new();
                for row in client.query(&stmt, &refs).map_err(remote_error)? {
                    let mut out = Vec::with_capacity(columns.len());
                    for i in 0..columns.len() {
                        out.push(row.try_get::<_, Wire>(i).map_err(remote_error)?.0);
                    }
                    rows.push(out);
                }
                Ok(Output { columns, affected: rows.len() as u64, rows })
            }
        }
    }

    /// Result columns of a statement, without running it.
    pub(crate) fn describe(&mut self, sql: &str) -> Result<Vec<OutputColumn>> {
        match self {
            ShardConn::Local { conn, .. } => Ok(conn.prepare(sql)?.columns().to_vec()),
            ShardConn::Remote { client, .. } => Ok(client
                .prepare(sql)
                .map_err(remote_error)?
                .columns()
                .iter()
                .map(|c| OutputColumn { name: c.name().to_string(), ty: Type::from_oid(c.type_().oid()).unwrap_or(Type::Text) })
                .collect()),
        }
    }
}
