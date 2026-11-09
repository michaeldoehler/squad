//! System catalogs (`pg_catalog`, `information_schema`) and the catalog
//! helper functions tools expect.

use crate::exec::ExecCx;
use crate::expr::Row;
use crate::types::{Type, Value};
use crate::{Error, Result};

/// Columns of a system relation, if `schema.name` is one.
pub(crate) fn table_columns(schema: Option<&str>, name: &str) -> Option<Vec<(&'static str, Type)>> {
    let _ = (schema, name);
    None
}

/// Result type of a catalog helper function, if `name` is one.
pub(crate) fn catalog_fn_type(name: &str, args: usize) -> Option<Type> {
    let _ = (name, args);
    None
}

pub(crate) fn scan(name: &str, cx: &mut ExecCx) -> Result<Vec<Row>> {
    let _ = cx;
    Err(Error::internal(format!("unknown system relation {name}")))
}

pub(crate) fn catalog_fn(name: &str, args: Vec<Value>, cx: &mut ExecCx) -> Result<Value> {
    let _ = (args, cx);
    Err(Error::internal(format!("unknown catalog function {name}")))
}
