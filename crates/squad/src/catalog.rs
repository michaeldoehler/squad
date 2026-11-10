//! Schema objects and their persistent representation.
//!
//! The whole catalog is one immutable snapshot ([`Catalog`]) that is stored
//! as a single record in the key-value store. DDL builds a modified copy and
//! writes it inside the surrounding transaction, which makes DDL
//! transactional for free: readers keep using the snapshot they started
//! with, and a rolled-back transaction simply never publishes its copy.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::types::Type;
use crate::{sqlstate, Error, Result};

/// Key under which the serialized catalog lives.
pub const CATALOG_KEY: &[u8] = b"\x00\x00\x00\x00catalog";
/// First OID handed out to user objects, as in Postgres.
pub const FIRST_USER_OID: u32 = 16384;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    /// Declared type as written (`character varying(20)`), for reporting.
    pub decl: String,
    /// Postgres type OID of the declared type (`varchar` differs from `text`).
    pub decl_oid: u32,
    /// Type modifier: max length for varchar/char, precision/scale for
    /// numeric (`((p << 16) | s) + 4`), `-1` if none.
    pub typmod: i32,
    pub not_null: bool,
    /// SQL text of the default expression.
    pub default: Option<String>,
    /// `GENERATED ALWAYS AS IDENTITY`: explicit values are rejected.
    pub identity_always: bool,
    /// Hidden columns are not expanded by `*` (the implicit row id).
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Index {
    pub id: u32,
    pub name: String,
    /// Positions of the indexed columns in the table.
    pub columns: Vec<usize>,
    pub unique: bool,
    pub primary: bool,
    /// Created by a PRIMARY KEY / UNIQUE constraint rather than CREATE INDEX.
    pub constraint: bool,
    /// `NULLS NOT DISTINCT`
    pub nulls_not_distinct: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FkAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<usize>,
    pub ref_table: u32,
    pub ref_columns: Vec<usize>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Check {
    pub name: String,
    /// SQL text of the boolean expression.
    pub expr: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub id: u32,
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    /// Positions of the primary key columns. Tables declared without a
    /// primary key get a hidden `int8` row id column that takes this role.
    pub pk: Vec<usize>,
    /// True if `pk` is the hidden row id rather than a declared key.
    pub rowid: bool,
    pub indexes: Vec<Index>,
    pub checks: Vec<Check>,
    pub foreign_keys: Vec<ForeignKey>,
    /// Set for tables created `USING columnar`.
    #[serde(default)]
    pub columnar: Option<Columnar>,
}

/// Options of a columnar table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Columnar {
    /// Columns segments are sorted by; statistics prune best on them.
    pub order_by: Vec<usize>,
}

impl Table {
    pub fn oid(&self) -> u32 {
        FIRST_USER_OID + self.id
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Columns visible to `SELECT *` and positional `INSERT`.
    pub fn visible_columns(&self) -> impl Iterator<Item = (usize, &Column)> {
        self.columns.iter().enumerate().filter(|(_, c)| !c.hidden)
    }

    pub fn qualified_name(&self) -> String {
        if self.schema == "public" {
            self.name.clone()
        } else {
            format!("{}.{}", self.schema, self.name)
        }
    }

    /// Name of the primary key constraint, if the table declares one.
    pub fn pk_name(&self) -> Option<&str> {
        self.indexes.iter().find(|i| i.primary).map(|i| i.name.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sequence {
    pub id: u32,
    pub schema: String,
    pub name: String,
    pub start: i64,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub cycle: bool,
    /// `(table id, column name)` if the sequence belongs to a serial or
    /// identity column and is dropped together with it.
    pub owned_by: Option<(u32, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct View {
    pub id: u32,
    pub schema: String,
    pub name: String,
    /// The defining `SELECT`.
    pub sql: String,
    /// Column names, if given explicitly in `CREATE VIEW v(a, b)`.
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumType {
    pub id: u32,
    pub schema: String,
    pub name: String,
    pub labels: Vec<String>,
}

type Name = (String, String);

/// An immutable snapshot of all schema objects.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Catalog {
    /// Bumped by every DDL statement; cached plans are tied to it.
    pub version: u64,
    pub next_id: u32,
    pub schemas: Vec<String>,
    #[serde(with = "pairs")]
    pub tables: BTreeMap<Name, Arc<Table>>,
    #[serde(with = "pairs")]
    pub sequences: BTreeMap<Name, Arc<Sequence>>,
    #[serde(with = "pairs")]
    pub views: BTreeMap<Name, Arc<View>>,
    #[serde(with = "pairs")]
    pub enums: BTreeMap<Name, Arc<EnumType>>,
}

/// JSON object keys must be strings, so maps keyed by `(schema, name)` are
/// stored as arrays of values; the key is recomputed from the value.
mod pairs {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub(super) trait Named {
        fn key(&self) -> Name;
    }
    macro_rules! named {
        ($($t:ty),*) => {$(impl Named for $t {
            fn key(&self) -> Name { (self.schema.clone(), self.name.clone()) }
        })*};
    }
    named!(Table, Sequence, View, EnumType);

    pub(super) fn serialize<S: Serializer, T: Serialize>(
        map: &BTreeMap<Name, Arc<T>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.collect_seq(map.values().map(|v| &**v))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>, T: Deserialize<'de> + Named>(
        d: D,
    ) -> Result<BTreeMap<Name, Arc<T>>, D::Error> {
        let items = Vec::<T>::deserialize(d)?;
        Ok(items.into_iter().map(|t| (t.key(), Arc::new(t))).collect())
    }
}

/// What a relation name resolves to.
#[derive(Debug, Clone)]
pub enum Relation {
    Table(Arc<Table>),
    View(Arc<View>),
}

impl Catalog {
    /// The catalog of a freshly created database.
    pub fn bootstrap() -> Catalog {
        Catalog { version: 1, next_id: 1, schemas: vec!["public".into()], ..Default::default() }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Catalog> {
        serde_json::from_slice(bytes)
            .map_err(|e| Error::new(sqlstate::DATA_CORRUPTED, format!("catalog is unreadable: {e}")))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("catalog serializes")
    }

    pub fn alloc_id(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub fn has_schema(&self, schema: &str) -> bool {
        self.schemas.iter().any(|s| s == schema)
    }

    /// Resolves an optionally schema-qualified name against a search path.
    fn lookup<'a, T>(
        map: &'a BTreeMap<Name, Arc<T>>,
        schema: Option<&str>,
        name: &str,
        search_path: &[String],
    ) -> Option<&'a Arc<T>> {
        match schema {
            Some(s) => map.get(&(s.to_string(), name.to_string())),
            None => search_path.iter().find_map(|s| map.get(&(s.clone(), name.to_string()))),
        }
    }

    pub fn table(&self, schema: Option<&str>, name: &str, path: &[String]) -> Option<&Arc<Table>> {
        Self::lookup(&self.tables, schema, name, path)
    }

    pub fn table_by_id(&self, id: u32) -> Option<&Arc<Table>> {
        self.tables.values().find(|t| t.id == id)
    }

    pub fn view(&self, schema: Option<&str>, name: &str, path: &[String]) -> Option<&Arc<View>> {
        Self::lookup(&self.views, schema, name, path)
    }

    pub fn sequence(&self, schema: Option<&str>, name: &str, path: &[String]) -> Option<&Arc<Sequence>> {
        Self::lookup(&self.sequences, schema, name, path)
    }

    pub fn enum_type(&self, schema: Option<&str>, name: &str, path: &[String]) -> Option<&Arc<EnumType>> {
        Self::lookup(&self.enums, schema, name, path)
    }

    pub fn relation(&self, schema: Option<&str>, name: &str, path: &[String]) -> Option<Relation> {
        match schema {
            Some(_) => self
                .table(schema, name, path)
                .map(|t| Relation::Table(t.clone()))
                .or_else(|| self.view(schema, name, path).map(|v| Relation::View(v.clone()))),
            // Honour search path order across both kinds.
            None => path.iter().find_map(|s| self.relation(Some(s), name, path)),
        }
    }

    /// True if any relation-like object (table, view, sequence, index)
    /// already uses this name in the schema; they share one namespace.
    pub fn relation_name_taken(&self, schema: &str, name: &str) -> bool {
        let key = (schema.to_string(), name.to_string());
        self.tables.contains_key(&key)
            || self.views.contains_key(&key)
            || self.sequences.contains_key(&key)
            || self.tables.values().any(|t| t.schema == schema && t.indexes.iter().any(|i| i.name == name))
    }

    /// Finds an index by name: `(table, position in table.indexes)`.
    pub fn index(&self, schema: Option<&str>, name: &str, path: &[String]) -> Option<(&Arc<Table>, usize)> {
        let find = |s: &str| {
            self.tables
                .values()
                .filter(|t| t.schema == s)
                .find_map(|t| t.indexes.iter().position(|i| i.name == name).map(|p| (t, p)))
        };
        match schema {
            Some(s) => find(s),
            None => path.iter().find_map(|s| find(s)),
        }
    }

    /// Tables holding a foreign key that references `table_id`.
    pub fn referencing(&self, table_id: u32) -> Vec<(Arc<Table>, ForeignKey)> {
        let mut out = Vec::new();
        for t in self.tables.values() {
            for fk in &t.foreign_keys {
                if fk.ref_table == table_id {
                    out.push((t.clone(), fk.clone()));
                }
            }
        }
        out
    }
}

/// Key prefix under which the rows of a table or the entries of an index
/// are stored.
pub fn object_prefix(id: u32) -> [u8; 4] {
    id.to_be_bytes()
}

/// Key of a sequence's current state.
pub fn sequence_key(id: u32) -> Vec<u8> {
    let mut k = b"\x00\x00\x00\x00seq/".to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut c = Catalog::bootstrap();
        let id = c.alloc_id();
        c.tables.insert(
            ("public".into(), "t".into()),
            Arc::new(Table {
                id,
                schema: "public".into(),
                name: "t".into(),
                columns: vec![Column {
                    name: "id".into(),
                    ty: Type::Array(Box::new(Type::Int4)),
                    decl: "integer[]".into(),
                    decl_oid: 1007,
                    typmod: -1,
                    not_null: true,
                    default: None,
                    identity_always: false,
                    hidden: false,
                }],
                pk: vec![0],
                rowid: false,
                indexes: vec![],
                checks: vec![],
                foreign_keys: vec![],
                columnar: None,
            }),
        );
        let back = Catalog::from_bytes(&c.to_bytes()).unwrap();
        assert_eq!(back.tables, c.tables);
        assert_eq!(back.next_id, 2);
        let path = vec!["public".to_string()];
        assert!(back.table(None, "t", &path).is_some());
        assert!(back.table(Some("other"), "t", &path).is_none());
    }
}
