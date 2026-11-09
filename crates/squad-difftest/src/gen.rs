//! Random schema, data and statement generation.
//!
//! Everything is derived from one seeded generator, so a seed identifies a
//! run exactly. Expressions are generated per value kind, which keeps the
//! large majority of statements well-typed while still exercising implicit
//! casts between the members of a kind (e.g. `smallint` and `bigint`).

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum K {
    Int,
    Num,
    Float,
    Text,
    Bool,
    Date,
    Ts,
    Tstz,
    Uuid,
    Bytea,
    Jsonb,
    IntArr,
    TextArr,
    Enum,
}

const ALL_KINDS: [K; 14] = [
    K::Int,
    K::Num,
    K::Float,
    K::Text,
    K::Bool,
    K::Date,
    K::Ts,
    K::Tstz,
    K::Uuid,
    K::Bytea,
    K::Jsonb,
    K::IntArr,
    K::TextArr,
    K::Enum,
];

impl K {
    fn orderable(self) -> bool {
        // jsonb has a btree order in Postgres, but it is rarely relied on.
        self != K::Jsonb
    }
}

#[derive(Debug, Clone)]
pub struct Col {
    pub name: String,
    /// SQL type as declared.
    pub decl: String,
    pub kind: K,
    pub not_null: bool,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub cols: Vec<Col>,
    pub pk: Vec<usize>,
    pub indexes: Vec<String>,
}

/// What kind of statement was generated; decides how results are compared.
#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    /// A query; `ordered` if its ORDER BY determines the row order fully.
    Query { sql: String, ordered: bool },
    /// Data modification of the named table.
    Dml { sql: String, table: String },
    Other { sql: String },
}

impl Stmt {
    pub fn sql(&self) -> &str {
        match self {
            Stmt::Query { sql, .. } | Stmt::Dml { sql, .. } | Stmt::Other { sql } => sql,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Only {
    All,
    Select,
    Dml,
    Ddl,
    Txn,
}

#[derive(Clone, PartialEq)]
enum TxnState {
    None,
    Open,
}

/// Columns visible to an expression: `(reference, kind)`.
type Scope = Vec<(String, K)>;

pub struct Gen {
    rng: StdRng,
    pub tables: Vec<Table>,
    alias: usize,
    txn: TxnState,
    /// Schema as of BEGIN and of each savepoint, to undo DDL in the model.
    saved: Vec<Vec<Table>>,
    savepoints: usize,
    next_id: i64,
    extra: usize,
    /// The open transaction was aborted by an error and must be rolled back.
    pending_rollback: bool,
    pub only: Only,
}

const TEXTS: [&str; 14] =
    ["", "a", "b", "ab", "abc", "A", "foo bar", "x%y", "h€llo", "10", " pad ", "a_c", "Zed", "b,c"];
const UUIDS: [&str; 3] = [
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "00000000-0000-0000-0000-000000000000",
    "ffffffff-ffff-4fff-bfff-ffffffffffff",
];
const JSONS: [&str; 8] = [
    r#"{"a": 1, "b": "x"}"#,
    r#"{"a": 2}"#,
    "[1, 2, 3]",
    r#"{"a": {"c": null}, "b": [1, "y"]}"#,
    r#""s""#,
    "3",
    "null",
    "{}",
];
const ENUMS: [&str; 3] = ["sad", "ok", "happy"];

impl Gen {
    pub fn new(seed: u64, only: Only) -> Gen {
        Gen {
            rng: StdRng::seed_from_u64(seed),
            tables: Vec::new(),
            alias: 0,
            txn: TxnState::None,
            saved: Vec::new(),
            savepoints: 0,
            next_id: 100,
            extra: 0,
            pending_rollback: false,
            only,
        }
    }

    fn chance(&mut self, p: f64) -> bool {
        self.rng.gen_bool(p)
    }

    fn below(&mut self, n: usize) -> usize {
        self.rng.gen_range(0..n)
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        items.choose(&mut self.rng).expect("non-empty choice")
    }

    fn fresh(&mut self, prefix: &str) -> String {
        self.alias += 1;
        format!("{prefix}{}", self.alias)
    }

    // ------------------------------------------------------------------
    // Literals
    // ------------------------------------------------------------------

    fn int_value(&mut self) -> i64 {
        match self.below(40) {
            // Extreme values live in the data only: Postgres folds constant
            // expressions while planning and would report their overflow
            // even for queries that return no rows.
            0 => 1000,
            1 => -1000,
            _ => self.rng.gen_range(-6..12),
        }
    }

    /// A literal that is unmistakably of the given kind.
    fn lit(&mut self, k: K) -> String {
        if self.chance(0.06) {
            return format!("NULL::{}", cast_name(k));
        }
        self.lit_not_null(k)
    }

    fn lit_not_null(&mut self, k: K) -> String {
        match k {
            K::Int => {
                let v = self.int_value();
                if v < 0 {
                    format!("({v})")
                } else {
                    v.to_string()
                }
            }
            K::Num => {
                let cents = self.rng.gen_range(-2000..5000i64);
                let s = format!("{}{}.{:02}", if cents < 0 { "-" } else { "" }, cents.abs() / 100, cents.abs() % 100);
                if cents < 0 {
                    format!("({s})")
                } else {
                    s
                }
            }
            K::Float => {
                let q = self.rng.gen_range(-40..80i64);
                format!("({}::float8)", q as f64 * 0.25)
            }
            K::Text => format!("'{}'", self.pick(&TEXTS)),
            K::Bool => self.pick(&["true", "false"]).to_string(),
            K::Date => format!("'{}'::date", self.date_text()),
            K::Ts => format!("'{} {}'::timestamp", self.date_text(), self.time_text()),
            K::Tstz => {
                let tz = *self.pick(&["+00", "+02", "-05"]);
                format!("'{} {}{tz}'::timestamptz", self.date_text(), self.time_text())
            }
            K::Uuid => format!("'{}'::uuid", self.pick(&UUIDS)),
            K::Bytea => format!("'{}'::bytea", self.pick(&["\\x", "\\x00ff", "\\xdeadbeef", "\\x61"])),
            K::Jsonb => format!("'{}'::jsonb", self.pick(&JSONS)),
            K::IntArr => format!("'{}'::int[]", self.pick(&["{}", "{1,2,3}", "{1,NULL}", "{3}", "{2,2,1}"])),
            K::TextArr => format!("'{}'::text[]", self.pick(&["{}", "{a,b}", "{\"a b\",c}", "{a,NULL}", "{b}"])),
            K::Enum => format!("'{}'::mood", self.pick(&ENUMS)),
        }
    }

    fn date_text(&mut self) -> String {
        match self.below(12) {
            0 => "1999-12-31".to_string(),
            1 => "2024-02-29".to_string(),
            _ => format!("2024-{:02}-{:02}", self.rng.gen_range(1..=3), self.rng.gen_range(1..=28)),
        }
    }

    fn time_text(&mut self) -> String {
        match self.below(4) {
            0 => "00:00:00".to_string(),
            1 => "23:59:59.999999".to_string(),
            _ => format!("{:02}:{:02}:00", self.rng.gen_range(0..24), self.rng.gen_range(0..60) / 15 * 15),
        }
    }

    /// A value for storing into a column (untyped where possible, so that
    /// assignment coercion is exercised too).
    fn col_value(&mut self, c: &Col) -> String {
        if !c.not_null && self.chance(0.12) {
            return "NULL".to_string();
        }
        match c.kind {
            K::Int => match c.decl.as_str() {
                "smallint" => self.rng.gen_range(-6..12).to_string(),
                // Large, but small enough that sums and products stay within
                // the 28 digits squad's numeric type holds.
                "bigint" if self.chance(0.03) => "1099511627776".to_string(),
                "integer" if self.chance(0.03) => self.pick(&["2147483647", "-2147483648", "100000"]).to_string(),
                _ => self.int_value().to_string(),
            },
            K::Text if c.decl.starts_with("varchar") => format!("'{}'", self.pick(&["", "a", "ab", "abc", "h€llo", "Zed"])),
            K::Num => self.lit_not_null(K::Num).trim_matches(|ch| ch == '(' || ch == ')').to_string(),
            K::Float => (self.rng.gen_range(-40..80i64) as f64 * 0.25).to_string(),
            K::Date | K::Ts | K::Tstz | K::Uuid | K::Bytea | K::Jsonb | K::IntArr | K::TextArr | K::Enum => {
                let l = self.lit_not_null(c.kind);
                l[..l.rfind("::").unwrap()].to_string()
            }
            k => self.lit_not_null(k),
        }
    }

    // ------------------------------------------------------------------
    // Schema and data
    // ------------------------------------------------------------------

    fn col_decl(&mut self, k: K) -> String {
        match k {
            K::Int => self.pick(&["smallint", "integer", "integer", "bigint"]).to_string(),
            K::Num => "numeric(9,2)".to_string(),
            K::Float => "double precision".to_string(),
            K::Text => self.pick(&["text", "text", "varchar(6)"]).to_string(),
            K::Bool => "boolean".to_string(),
            K::Date => "date".to_string(),
            K::Ts => "timestamp".to_string(),
            K::Tstz => "timestamptz".to_string(),
            K::Uuid => "uuid".to_string(),
            K::Bytea => "bytea".to_string(),
            K::Jsonb => "jsonb".to_string(),
            K::IntArr => "integer[]".to_string(),
            K::TextArr => "text[]".to_string(),
            K::Enum => "mood".to_string(),
        }
    }

    /// Statements creating the schema and loading the initial rows.
    pub fn setup(&mut self) -> Vec<String> {
        let mut out = vec!["CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')".to_string()];
        let ntables = self.rng.gen_range(2..=4);
        for t in 0..ntables {
            let name = format!("t{t}");
            let composite = self.chance(0.3);
            let mut cols =
                vec![Col { name: "id".into(), decl: "integer".into(), kind: K::Int, not_null: true, unique: false }];
            if composite {
                cols.push(Col { name: "k".into(), decl: "integer".into(), kind: K::Int, not_null: true, unique: false });
            }
            let pk: Vec<usize> = (0..cols.len()).collect();
            let ncols = self.rng.gen_range(2..=7);
            for i in 0..ncols {
                // Favour the kinds expressions are richest for.
                let kind = if self.chance(0.55) {
                    *self.pick(&[K::Int, K::Int, K::Text, K::Num, K::Bool])
                } else {
                    *self.pick(&ALL_KINDS)
                };
                let decl = self.col_decl(kind);
                let not_null = self.chance(0.2);
                let unique = self.chance(0.08) && matches!(kind, K::Int | K::Text);
                cols.push(Col { name: format!("c{i}"), decl, kind, not_null, unique });
            }
            let mut defs = Vec::new();
            for c in &cols {
                let mut d = format!("{} {}", c.name, c.decl);
                if c.not_null {
                    d.push_str(" NOT NULL");
                }
                if c.unique {
                    d.push_str(" UNIQUE");
                }
                defs.push(d);
            }
            // Defaults and checks on a few columns.
            for i in pk.len()..cols.len() {
                if self.chance(0.15) {
                    let v = self.col_value(&Col { not_null: true, ..cols[i].clone() });
                    defs[i].push_str(&format!(" DEFAULT {v}"));
                }
                if cols[i].kind == K::Int && self.chance(0.15) {
                    defs[i].push_str(&format!(" CHECK ({} > -1000)", cols[i].name));
                }
            }
            defs.push(format!("PRIMARY KEY ({})", pk.iter().map(|&i| cols[i].name.clone()).collect::<Vec<_>>().join(", ")));
            // A foreign key to an earlier table with a single-column key.
            if t > 0 && self.chance(0.4) {
                let parents: Vec<String> = self.tables.iter().filter(|p| p.pk.len() == 1).map(|p| p.name.clone()).collect();
                if !parents.is_empty() {
                    let parent = self.pick(&parents).clone();
                    let action = *self.pick(&["", " ON DELETE CASCADE", " ON DELETE SET NULL"]);
                    cols.push(Col { name: "ref".into(), decl: "integer".into(), kind: K::Int, not_null: false, unique: false });
                    defs.push(format!("ref integer REFERENCES {parent} (id){action}"));
                    let last = defs.len() - 1;
                    defs.swap(last - 1, last);
                }
            }
            out.push(format!("CREATE TABLE {name} ({})", defs.join(", ")));
            let mut table = Table { name: name.clone(), cols, pk, indexes: Vec::new() };
            if self.chance(0.5) {
                let c = self.below(table.cols.len() - 1) + 1;
                if table.cols[c].kind.orderable() {
                    let ix = format!("{name}_ix{}", table.indexes.len());
                    let unique = if self.chance(0.1) { "UNIQUE " } else { "" };
                    out.push(format!("CREATE {unique}INDEX {ix} ON {name} ({})", table.cols[c].name));
                    if !unique.is_empty() {
                        table.cols[c].unique = true;
                    }
                    table.indexes.push(ix);
                }
            }
            self.tables.push(table);
        }
        for t in 0..self.tables.len() {
            let rows = match self.below(10) {
                0 => 0,
                1 => 1,
                _ => self.rng.gen_range(3..25),
            };
            for r in 0..rows {
                let table = self.tables[t].clone();
                let mut vals = Vec::new();
                for (i, c) in table.cols.iter().enumerate() {
                    vals.push(if c.name == "id" {
                        (if table.pk.len() > 1 { r / 3 + 1 } else { r + 1 }).to_string()
                    } else if i == 1 && table.pk.len() > 1 {
                        (r % 3).to_string()
                    } else if c.name == "ref" {
                        if self.chance(0.3) { "NULL".to_string() } else { self.rng.gen_range(1..5).to_string() }
                    } else {
                        self.col_value(c)
                    });
                }
                out.push(format!("INSERT INTO {} VALUES ({})", table.name, vals.join(", ")));
            }
        }
        out
    }

    // ------------------------------------------------------------------
    // Expressions
    // ------------------------------------------------------------------

    fn col_of(&mut self, k: K, sc: &Scope) -> Option<String> {
        let hits: Vec<&String> = sc.iter().filter(|(_, ck)| *ck == k).map(|(n, _)| n).collect();
        if hits.is_empty() {
            None
        } else {
            Some((*self.pick(&hits)).clone())
        }
    }

    fn leaf(&mut self, k: K, sc: &Scope) -> String {
        if self.chance(0.7) {
            if let Some(c) = self.col_of(k, sc) {
                return c;
            }
        }
        self.lit(k)
    }

    fn small(&mut self) -> i64 {
        self.rng.gen_range(1..5)
    }

    fn expr(&mut self, k: K, depth: u32, sc: &Scope) -> String {
        if depth == 0 || self.chance(0.25) {
            return self.leaf(k, sc);
        }
        let d = depth - 1;
        // Constructs available for every kind.
        match self.below(14) {
            0 => return format!("coalesce({}, {})", self.expr(k, d, sc), self.expr(k, d, sc)),
            1 => {
                return format!(
                    "CASE WHEN {} THEN {} ELSE {} END",
                    self.expr(K::Bool, d, sc),
                    self.expr(k, d, sc),
                    self.expr(k, d, sc)
                )
            }
            2 if k.orderable() && k != K::Bool => {
                let f = *self.pick(&["greatest", "least"]);
                return format!("{f}({}, {})", self.expr(k, d, sc), self.expr(k, d, sc));
            }
            3 if k.orderable() => return format!("nullif({}, {})", self.expr(k, d, sc), self.expr(k, d, sc)),
            4 if !matches!(k, K::Jsonb | K::IntArr | K::TextArr | K::Bool | K::Uuid | K::Bytea) && !self.tables.is_empty() => {
                // Scalar subquery over a table that has a column of the kind.
                let cands: Vec<(String, String)> = self
                    .tables
                    .iter()
                    .flat_map(|t| t.cols.iter().filter(|c| c.kind == k).map(move |c| (t.name.clone(), c.name.clone())))
                    .collect();
                if !cands.is_empty() {
                    let (t, c) = self.pick(&cands).clone();
                    let a = self.fresh("s");
                    let agg = *self.pick(&["min", "max"]);
                    let mut inner: Scope = vec![(format!("{a}.{c}"), k)];
                    inner.extend(sc.iter().cloned());
                    let filter = if self.chance(0.6) {
                        format!(" WHERE {}", self.expr(K::Bool, d.min(1), &inner))
                    } else {
                        String::new()
                    };
                    return format!("(SELECT {agg}({a}.{c}) FROM {t} {a}{filter})");
                }
            }
            _ => {}
        }
        match k {
            K::Int => match self.below(26) {
                0..=2 => format!("({} + {})", self.expr(k, d, sc), self.expr(k, d, sc)),
                3 => format!("({} - {})", self.expr(k, d, sc), self.expr(k, d, sc)),
                4 => format!("({} * {})", self.expr(k, d, sc), self.expr(k, d, sc)),
                5 => format!("({} / {})", self.expr(k, d, sc), self.small()),
                6 => format!("({} % {})", self.expr(k, d, sc), self.small()),
                7 => format!("abs({})", self.expr(k, d, sc)),
                8 => format!("length({})", self.expr(K::Text, d, sc)),
                9 => format!("extract(year FROM {})::int", self.expr(K::Date, d, sc)),
                10 => format!("cardinality({})", self.expr(K::IntArr, d, sc)),
                11 => format!("({})[{}]", self.expr(K::IntArr, d, sc), self.small()),
                12 => format!("({})::int", self.expr(K::Bool, d, sc)),
                13 => format!("strpos({}, {})", self.expr(K::Text, d, sc), self.expr(K::Text, d, sc)),
                14 => format!("round({})::int", self.expr(K::Num, d, sc)),
                15 => format!("({})::bigint", self.expr(k, d, sc)),
                16 => format!("extract(day FROM {})::int", self.expr(K::Ts, d, sc)),
                17 => format!("({} - {})", self.expr(K::Date, d, sc), self.expr(K::Date, d, sc)),
                18 => format!("CASE {} WHEN {} THEN {} WHEN {} THEN {} END", self.expr(k, d, sc), self.lit_not_null(k), self.expr(k, d, sc), self.lit_not_null(k), self.expr(k, d, sc)),
                19 => format!("({})::smallint", self.expr(k, d, sc)),
                20 => format!("sign({})::int", self.expr(K::Num, d, sc)),
                21 => format!("octet_length({})", self.expr(K::Text, d, sc)),
                22 => format!("ascii({})", self.expr(K::Text, d, sc)),
                23 => format!("array_position({}, {})", self.expr(K::IntArr, d, sc), self.small()),
                24 => format!("date_part('month', {})::int", self.expr(K::Date, d, sc)),
                _ => format!("mod({}, {})", self.expr(k, d, sc), self.small()),
            },
            K::Num => match self.below(14) {
                0 | 1 => format!("({} + {})", self.expr(k, d, sc), self.expr(k, d, sc)),
                2 => format!("({} - {})", self.expr(k, d, sc), self.expr(k, d, sc)),
                3 => format!("({} * {})", self.expr(k, d, sc), self.leaf(k, sc)),
                4 => format!("({} / {})", self.expr(k, d, sc), self.small()),
                5 => format!("round({}, 1)", self.expr(k, d, sc)),
                6 => format!("({})::numeric", self.expr(K::Int, d, sc)),
                7 => format!("abs({})", self.expr(k, d, sc)),
                8 => format!("trunc({})", self.expr(k, d, sc)),
                9 => format!("ceil({})", self.expr(k, d, sc)),
                10 => format!("({} + {})", self.expr(k, d, sc), self.expr(K::Int, d, sc)),
                11 => format!("round({} / {}, 3)", self.expr(k, d, sc), self.small()),
                12 => format!("power({}, 2)", self.leaf(k, sc)),
                _ => format!("({})::numeric(12,3)", self.expr(k, d, sc)),
            },
            K::Float => match self.below(6) {
                0 => format!("({} + {})", self.expr(k, d, sc), self.expr(k, d, sc)),
                1 => format!("({} * {})", self.expr(k, d, sc), self.leaf(k, sc)),
                2 => format!("({})::float8", self.expr(K::Int, d, sc)),
                3 => format!("floor({})", self.expr(k, d, sc)),
                4 => format!("sqrt(abs({}))", self.expr(k, d, sc)),
                _ => format!("({} / {})", self.expr(k, d, sc), self.small()),
            },
            K::Text if self.chance(0.3) => self.probe(d, sc),
            K::Text => match self.below(30) {
                0 | 1 => format!("({} || {})", self.expr(k, d, sc), self.expr(k, d, sc)),
                2 => format!("upper({})", self.expr(k, d, sc)),
                3 => format!("lower({})", self.expr(k, d, sc)),
                4 => format!("substr({}, {}, {})", self.expr(k, d, sc), self.small(), self.small()),
                5 => format!("replace({}, 'a', 'xy')", self.expr(k, d, sc)),
                6 => format!("trim({})", self.expr(k, d, sc)),
                7 => format!("left({}, {})", self.expr(k, d, sc), self.small()),
                8 => format!("({})::text", self.expr(K::Int, d, sc)),
                9 => format!("({})::text", self.expr(K::Num, d, sc)),
                10 => format!("({})::text", { let kk = *self.pick(&[K::Bool, K::Date, K::Enum, K::Uuid]); self.expr(kk, d, sc) }),
                11 => format!("concat({}, {})", self.expr(k, d, sc), self.expr(K::Int, d, sc)),
                12 => format!("lpad({}, 5, 'x')", self.expr(k, d, sc)),
                13 => format!("({} ->> 'b')", self.expr(K::Jsonb, d, sc)),
                14 => format!("md5({})", self.expr(k, d, sc)),
                15 => format!("reverse({})", self.expr(k, d, sc)),
                16 => format!("initcap({})", self.expr(k, d, sc)),
                17 => format!("right({}, {})", self.expr(k, d, sc), self.small()),
                18 => format!("rpad({}, 4, 'ab')", self.expr(k, d, sc)),
                19 => format!("split_part({}, ',', {})", self.expr(k, d, sc), self.small()),
                20 => format!("btrim({}, 'a ')", self.expr(k, d, sc)),
                21 => format!("translate({}, 'ab', 'x')", self.expr(k, d, sc)),
                22 => format!("to_char({}, 'YYYY-MM-DD HH24:MI')", self.expr(K::Ts, d, sc)),
                23 => format!("array_to_string({}, '-')", self.expr(K::TextArr, d, sc)),
                24 => format!("concat_ws('/', {}, {}, {})", self.expr(k, d, sc), self.expr(K::Int, d, sc), self.expr(k, d, sc)),
                25 => format!("substring({} from {})", self.expr(k, d, sc), self.small()),
                26 => format!("regexp_replace({}, '[ab]', '#', 'g')", self.expr(k, d, sc)),
                27 => format!("quote_literal({})", self.expr(k, d, sc)),
                28 => format!("({})::varchar(3)", self.expr(k, d, sc)),
                _ => format!("format('%s-%s', {}, {})", self.expr(k, d, sc), self.expr(K::Int, d, sc)),
            },
            K::Bool => self.bool_expr(d, sc),
            K::Date => match self.below(3) {
                0 => format!("({} + {})", self.expr(k, d, sc), self.small()),
                1 => format!("({})::date", self.expr(K::Ts, d, sc)),
                _ => format!("({} - {})", self.expr(k, d, sc), self.small()),
            },
            K::Ts => match self.below(3) {
                0 => format!("({} + interval '{} hours')", self.expr(k, d, sc), self.small()),
                1 => format!("date_trunc('{}', {})", self.pick(&["day", "month", "hour"]), self.expr(k, d, sc)),
                _ => format!("({})::timestamp", self.expr(K::Date, d, sc)),
            },
            K::Tstz => format!("({} + interval '{} days')", self.expr(k, d, sc), self.small()),
            K::Bytea => format!("({} || {})", self.expr(k, d, sc), self.expr(k, d, sc)),
            K::Jsonb => match self.below(5) {
                0 => format!("({} -> 'a')", self.expr(k, d, sc)),
                1 => format!("({} -> {})", self.expr(k, d, sc), self.below(3)),
                2 => format!("jsonb_build_object('k', {})", self.expr(K::Int, d, sc)),
                3 => {
                    let kk = *self.pick(&[K::Int, K::Text, K::Bool]);
                    format!("to_jsonb(({})::{})", self.expr(kk, d, sc), cast_name(kk))
                }
                _ => format!("({} || {})", self.expr(k, d, sc), self.expr(k, d, sc)),
            },
            K::IntArr => match self.below(3) {
                0 => format!("ARRAY[({})::int, ({})::int]", self.expr(K::Int, d, sc), self.expr(K::Int, d, sc)),
                1 => format!("({} || {})", self.expr(k, d, sc), self.expr(k, d, sc)),
                _ => format!("array_append({}, ({})::int)", self.expr(k, d, sc), self.expr(K::Int, d, sc)),
            },
            K::TextArr => match self.below(2) {
                0 => format!("ARRAY[({})::text, ({})::text]", self.expr(K::Text, d, sc), self.expr(K::Text, d, sc)),
                _ => format!("string_to_array({}, ',')", self.expr(K::Text, d, sc)),
            },
            K::Uuid | K::Enum => self.leaf(k, sc),
        }
    }

    /// A text-typed expression that renders the result of some other
    /// computation: the cheapest way to compare value formatting and the
    /// semantics of many functions at once.
    fn probe(&mut self, d: u32, sc: &Scope) -> String {
        let n = self.small();
        let (i, num, f, t) = (K::Int, K::Num, K::Float, K::Text);
        match self.below(64) {
            // Dates, times and intervals.
            0 => format!("({} - {})::text", self.expr(K::Ts, d, sc), self.expr(K::Ts, d, sc)),
            1 => format!("age({}, {})::text", self.expr(K::Ts, d, sc), self.expr(K::Ts, d, sc)),
            2 => format!("(interval '{n} hours 30 minutes' * {})::text", self.expr(i, d, sc)),
            3 => format!("date_trunc('{}', {})::text", self.pick(&["week", "quarter", "year", "minute"]), self.expr(K::Ts, d, sc)),
            4 => format!("extract({} FROM {})::text", self.pick(&["dow", "doy", "epoch", "week", "quarter", "isodow", "hour", "second"]), self.expr(K::Ts, d, sc)),
            5 => format!("({} AT TIME ZONE '{}')::text", self.expr(K::Tstz, d, sc), self.pick(&["Europe/Berlin", "America/New_York", "UTC", "Asia/Tokyo"])),
            6 => format!("to_char({}, '{}')", self.expr(K::Ts, d, sc), self.pick(&["Dy DD Mon YYYY", "HH12:MI AM", "YYYY\"Q\"Q", "FMMonth FMDD, YYYY", "IYYY-IW-ID", "DDD HH24:MI:SS.MS"])),
            7 => format!("({} + {})::text", self.expr(K::Date, d, sc), self.pick(&["interval '1 month'", "interval '1 year 2 days'", "interval '36 hours'", "time '10:30'"])),
            8 => format!("({})::text", self.expr(K::Tstz, d, sc)),
            9 => format!("(({})::timestamptz)::text", self.expr(K::Ts, d, sc)),
            10 => format!("justify_hours(interval '{} hours')::text", self.rng.gen_range(1..100)),
            11 => format!("(({} - {}) > interval '{n} days')::text", self.expr(K::Ts, d, sc), self.expr(K::Ts, d, sc)),
            12 => format!("make_date(2024, {n}, {})::text", self.rng.gen_range(1..29)),
            13 => format!("(({})::time)::text", self.expr(K::Ts, d, sc)),
            // Numbers.
            14 => format!("sqrt(abs({}))::text", self.expr(num, d, sc)),
            15 => format!("round(ln(abs({}) + 1), 6)::text", self.expr(num, d, sc)),
            16 => format!("round(exp({n}::numeric / 3), 8)::text"),
            17 => format!("to_char({}, '{}')", self.expr(num, d, sc), self.pick(&["999D99", "FM9990.00", "S9999.9", "000000", "9,999.99PR"])),
            18 => format!("width_bucket({}, 0, 50, 5)::text", self.expr(num, d, sc)),
            19 => format!("round({}, -1)::text", self.expr(num, d, sc)),
            20 => format!("({} % {n})::text", self.expr(num, d, sc)),
            21 => format!("({} / 3)::text", self.expr(f, d, sc)),
            22 => format!("power({}, 0.5)::text", self.expr(f, d, sc)),
            23 => format!("round(({})::numeric, 2)::text", self.expr(f, d, sc)),
            24 => format!("(({})::int)::text", self.expr(f, d, sc)),
            25 => format!("(({})::numeric)::text", self.expr(f, d, sc)),
            26 => format!("(({})::float8)::text", self.expr(num, d, sc)),
            27 => format!("(({})::real)::text", self.expr(num, d, sc)),
            28 => format!("ceil({})::text", self.expr(f, d, sc)),
            29 => format!("div({}, {n})::text", self.expr(num, d, sc)),
            30 => format!("(({})::int)::text", self.expr(num, d, sc)),
            31 => format!("((('1' || {n})::int) + 1)::text"),
            32 => format!("(({})::boolean)::text", self.expr(i, d, sc)),
            33 => format!("trim_scale({})::text", self.expr(num, d, sc)),
            // jsonb.
            34 => format!("jsonb_set(jsonb_build_object('a', {}), '{{a}}', '{n}')::text", self.expr(i, d, sc)),
            35 => format!("jsonb_strip_nulls({})::text", self.expr(K::Jsonb, d, sc)),
            36 => format!("({} #> '{{a,c}}')::text", self.expr(K::Jsonb, d, sc)),
            37 => format!("({} #>> '{{b,1}}')", self.expr(K::Jsonb, d, sc)),
            38 => format!("({} - 'a')::text", self.expr(K::Jsonb, d, sc)),
            39 => format!("jsonb_typeof({})", self.expr(K::Jsonb, d, sc)),
            40 => format!("({} < {})::text", self.expr(K::Jsonb, d, sc), self.expr(K::Jsonb, d, sc)),
            41 => format!("jsonb_build_array({}, {}, {})::text", self.expr(i, d, sc), self.expr(t, d, sc), self.expr(K::Bool, d, sc)),
            42 => format!("to_jsonb({})::text", self.expr(K::IntArr, d, sc)),
            43 => format!("jsonb_pretty({})", self.leaf(K::Jsonb, sc)),
            44 => format!("to_json(({})::text)::text", self.expr(t, d, sc)),
            // Arrays.
            45 => format!("array_remove({}, {n})::text", self.expr(K::IntArr, d, sc)),
            46 => format!("array_replace({}, {n}, 9)::text", self.expr(K::IntArr, d, sc)),
            47 => format!("array_cat({}, {})::text", self.expr(K::TextArr, d, sc), self.expr(K::TextArr, d, sc)),
            48 => format!("(({})[1:{n}])::text", self.expr(K::IntArr, d, sc)),
            49 => format!("array_length({}, 1)::text", self.expr(K::TextArr, d, sc)),
            50 => format!("array_positions({}, {n})::text", self.expr(K::IntArr, d, sc)),
            51 => format!("({} = ALL ({}))::text", self.expr(i, d, sc), self.expr(K::IntArr, d, sc)),
            52 => format!("array_prepend({}, {})::text", self.expr(t, d, sc), self.expr(K::TextArr, d, sc)),
            53 => format!("({} < {})::text", self.expr(K::IntArr, d, sc), self.expr(K::IntArr, d, sc)),
            // Strings and bytes.
            54 => format!("overlay({} placing 'XX' from {n})", self.expr(t, d, sc)),
            55 => format!("position('a' in {})::text", self.expr(t, d, sc)),
            56 => format!("starts_with({}, 'a')::text", self.expr(t, d, sc)),
            57 => format!("encode({}, '{}')", self.expr(K::Bytea, d, sc), self.pick(&["hex", "base64", "escape"])),
            58 => format!("length({})::text", self.expr(K::Bytea, d, sc)),
            59 => format!("({} SIMILAR TO '(a|b)%')::text", self.expr(t, d, sc)),
            60 => format!("(regexp_match({}, '([a-z])([a-z])'))::text", self.expr(t, d, sc)),
            61 => format!("(({}, {}) < ({}, {}))::text", self.expr(i, d, sc), self.expr(t, d, sc), self.expr(i, d, sc), self.expr(t, d, sc)),
            62 => format!("(ROW({}, {}) IS NULL)::text", self.expr(i, d, sc), self.expr(t, d, sc)),
            _ => format!("({} BETWEEN SYMMETRIC {} AND {})::text", self.expr(i, d, sc), self.expr(i, d, sc), self.expr(i, d, sc)),
        }
    }

    fn bool_expr(&mut self, d: u32, sc: &Scope) -> String {
        match self.below(22) {
            0..=5 => {
                let k = *self.pick(&[K::Int, K::Int, K::Int, K::Text, K::Text, K::Num, K::Float, K::Date, K::Ts, K::Tstz, K::Enum, K::Uuid, K::Bytea, K::IntArr, K::TextArr, K::Bool]);
                let op = *self.pick(&["=", "<>", "<", "<=", ">", ">="]);
                format!("({} {op} {})", self.expr(k, d, sc), self.expr(k, d, sc))
            }
            6 if self.chance(0.5) => {
                // Comparisons across numeric kinds exercise implicit casts.
                let a = *self.pick(&[K::Int, K::Num, K::Float]);
                let b = *self.pick(&[K::Int, K::Num, K::Float]);
                let op = *self.pick(&["=", "<>", "<", ">="]);
                format!("({} {op} {})", self.expr(a, d, sc), self.expr(b, d, sc))
            }
            6 => format!("({} = {})", self.expr(K::Jsonb, d, sc), self.expr(K::Jsonb, d, sc)),
            7 => format!("({} AND {})", self.expr(K::Bool, d, sc), self.expr(K::Bool, d, sc)),
            8 => format!("({} OR {})", self.expr(K::Bool, d, sc), self.expr(K::Bool, d, sc)),
            9 => format!("(NOT {})", self.expr(K::Bool, d, sc)),
            10 => {
                let k = *self.pick(&ALL_KINDS);
                let not = if self.chance(0.5) { " NOT" } else { "" };
                format!("({} IS{not} NULL)", self.expr(k, d, sc))
            }
            11 => {
                let k = *self.pick(&[K::Int, K::Text, K::Num, K::Enum]);
                let not = if self.chance(0.3) { "NOT " } else { "" };
                let n = self.rng.gen_range(1..4);
                let items: Vec<String> = (0..n).map(|_| self.lit(k)).collect();
                format!("({} {not}IN ({}))", self.expr(k, d, sc), items.join(", "))
            }
            12 => {
                let k = *self.pick(&[K::Int, K::Num, K::Text, K::Date]);
                let not = if self.chance(0.3) { "NOT " } else { "" };
                format!("({} {not}BETWEEN {} AND {})", self.expr(k, d, sc), self.expr(k, d, sc), self.expr(k, d, sc))
            }
            13 => {
                let op = *self.pick(&["LIKE", "NOT LIKE", "ILIKE"]);
                let pat = *self.pick(&["a%", "%b", "%a%", "_", "a_c", "%", "x\\%y", "A%"]);
                format!("({} {op} '{pat}')", self.expr(K::Text, d, sc))
            }
            14 => {
                let k = *self.pick(&[K::Int, K::Text, K::Bool, K::Num]);
                let not = if self.chance(0.5) { " NOT" } else { "" };
                format!("({} IS{not} DISTINCT FROM {})", self.expr(k, d, sc), self.expr(k, d, sc))
            }
            15 => format!("({} ~ '{}')", self.expr(K::Text, d, sc), self.pick(&["^a", "b$", "[0-9]+", "a.c"])),
            16 => match self.below(3) {
                0 => format!("({} @> {})", self.expr(K::Jsonb, d, sc), self.expr(K::Jsonb, d, sc)),
                1 => format!("({} ? 'a')", self.expr(K::Jsonb, d, sc)),
                _ => format!("(jsonb_typeof({}) = 'object')", self.expr(K::Jsonb, d, sc)),
            },
            17 => match self.below(3) {
                0 => format!("({} = ANY ({}))", self.expr(K::Int, d, sc), self.expr(K::IntArr, d, sc)),
                1 => format!("({} @> {})", self.expr(K::IntArr, d, sc), self.expr(K::IntArr, d, sc)),
                _ => format!("({} && {})", self.expr(K::TextArr, d, sc), self.expr(K::TextArr, d, sc)),
            },
            18 => {
                let t = *self.pick(&["TRUE", "NOT TRUE", "FALSE", "UNKNOWN", "NOT UNKNOWN"]);
                format!("({} IS {t})", self.expr(K::Bool, d, sc))
            }
            _ => self.subquery_predicate(d, sc),
        }
    }

    /// EXISTS / IN / ANY / ALL over a table, correlated with the scope.
    fn subquery_predicate(&mut self, d: u32, sc: &Scope) -> String {
        if self.tables.is_empty() {
            return self.leaf(K::Bool, sc);
        }
        let t = self.pick(&self.tables.clone()).clone();
        let a = self.fresh("s");
        let mut inner: Scope = t.cols.iter().map(|c| (format!("{a}.{}", c.name), c.kind)).collect();
        let own = inner.clone();
        inner.extend(sc.iter().cloned());
        let filter = if self.chance(0.7) {
            format!(" WHERE {}", self.expr(K::Bool, d.min(1), &inner))
        } else {
            String::new()
        };
        match self.below(4) {
            0 => {
                let not = if self.chance(0.3) { "NOT " } else { "" };
                format!("({not}EXISTS (SELECT 1 FROM {} {a}{filter}))", t.name)
            }
            1 | 2 => {
                let (col, k) = self.pick(&own).clone();
                if k == K::Jsonb {
                    return format!("(EXISTS (SELECT 1 FROM {} {a}{filter}))", t.name);
                }
                let not = if self.chance(0.3) { "NOT " } else { "" };
                format!("({} {not}IN (SELECT {col} FROM {} {a}{filter}))", self.expr(k, d.min(1), sc), t.name)
            }
            _ => {
                let cands: Vec<(String, K)> = own.iter().filter(|(_, k)| matches!(k, K::Int | K::Num | K::Text)).cloned().collect();
                if cands.is_empty() {
                    return format!("(EXISTS (SELECT 1 FROM {} {a}{filter}))", t.name);
                }
                let (col, k) = self.pick(&cands).clone();
                let op = *self.pick(&["=", "<>", "<", ">="]);
                let q = *self.pick(&["ANY", "ALL"]);
                format!("({} {op} {q} (SELECT {col} FROM {} {a}{filter}))", self.expr(k, d.min(1), sc), t.name)
            }
        }
    }

    // ------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------

    /// FROM clause: returns `(sql, scope, total order columns, WITH items)`;
    /// the order columns are present only if every item is a base table.
    fn from_clause(&mut self, depth: u32) -> (String, Scope, Option<Vec<String>>, Vec<String>) {
        let mut sql = String::new();
        let mut scope: Scope = Vec::new();
        let mut order: Option<Vec<String>> = Some(Vec::new());
        let mut with = Vec::new();
        let n = if depth >= 2 { self.rng.gen_range(1..=3) } else { 1 + self.below(2) };
        let mut has_id: Vec<String> = Vec::new();
        for i in 0..n {
            let choice = self.below(20);
            let (item, cols, alias, base): (String, Scope, String, Option<Table>) = if choice < 14 || depth == 0 {
                let t = self.pick(&self.tables.clone()).clone();
                let a = self.fresh("a");
                let cols = t.cols.iter().map(|c| (format!("{a}.{}", c.name), c.kind)).collect();
                (format!("{} {a}", t.name), cols, a, Some(t))
            } else if choice < 16 {
                let (q, kinds, _) = self.select(depth - 1, None, false);
                let a = self.fresh("d");
                let cols = kinds.iter().enumerate().map(|(j, k)| (format!("{a}.c{j}"), *k)).collect();
                (format!("({q}) {a}"), cols, a, None)
            } else if choice < 18 {
                let (q, kinds, _) = self.select(depth - 1, None, false);
                let a = self.fresh("q");
                let recursive = self.chance(0.3);
                if recursive {
                    let lim = self.rng.gen_range(1..8);
                    with.push(format!(
                        "RECURSIVE {a} (c0) AS (SELECT 1 UNION ALL SELECT c0 + 1 FROM {a} WHERE c0 < {lim})"
                    ));
                    (a.clone(), vec![(format!("{a}.c0"), K::Int)], a, None)
                } else {
                    let names: Vec<String> = (0..kinds.len()).map(|j| format!("c{j}")).collect();
                    with.push(format!("{a} ({}) AS ({q})", names.join(", ")));
                    let cols = kinds.iter().enumerate().map(|(j, k)| (format!("{a}.c{j}"), *k)).collect();
                    (a.clone(), cols, a, None)
                }
            } else if choice == 18 {
                let a = self.fresh("v");
                let rows: Vec<String> =
                    (0..self.rng.gen_range(1..4)).map(|_| format!("({}, {})", self.lit(K::Int), self.lit(K::Text))).collect();
                (
                    format!("(VALUES {}) AS {a} (c0, c1)", rows.join(", ")),
                    vec![(format!("{a}.c0"), K::Int), (format!("{a}.c1"), K::Text)],
                    a,
                    None,
                )
            } else {
                let a = self.fresh("g");
                (format!("generate_series(1, {}) AS {a} (c0)", self.rng.gen_range(0..6)), vec![(format!("{a}.c0"), K::Int)], a, None)
            };
            match (&base, &mut order) {
                (Some(t), Some(o)) => o.extend(t.pk.iter().map(|&p| format!("{alias}.{}", t.cols[p].name))),
                _ => order = None,
            }
            let this_has_id = base.is_some();
            if i == 0 {
                sql = item;
            } else {
                let mut both = scope.clone();
                both.extend(cols.iter().cloned());
                let kind = *self.pick(&["JOIN", "JOIN", "LEFT JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN", ","]);
                match kind {
                    "CROSS JOIN" | "," => sql = format!("{sql}{}{item}", if kind == "," { ", " } else { " CROSS JOIN " }),
                    _ if this_has_id && !has_id.is_empty() && self.chance(0.15) && n == 2 => {
                        sql = format!("{sql} {kind} {item} USING (id)");
                    }
                    _ => {
                        // Mostly equi-joins on columns of the same kind.
                        let cond = if self.chance(0.7) {
                            let pairs: Vec<(String, String)> = scope
                                .iter()
                                .flat_map(|(l, lk)| {
                                    cols.iter()
                                        .filter(move |(_, rk)| rk == lk && lk.orderable() && !matches!(lk, K::Bool))
                                        .map(move |(r, _)| (l.clone(), r.clone()))
                                })
                                .collect();
                            if pairs.is_empty() {
                                self.expr(K::Bool, 1, &both)
                            } else {
                                let (l, r) = self.pick(&pairs).clone();
                                if self.chance(0.2) {
                                    format!("{l} = {r} AND {}", self.expr(K::Bool, 1, &both))
                                } else {
                                    format!("{l} = {r}")
                                }
                            }
                        } else {
                            self.expr(K::Bool, 1, &both)
                        };
                        // A full join needs a hashable or mergeable
                        // condition in Postgres; keep those simple.
                        let simple = cond.split(" AND ").next().unwrap().to_string();
                        let equi = simple.split(" = ").count() == 2 && !simple.contains('(');
                        if kind == "FULL JOIN" && !equi {
                            sql = format!("{sql} LEFT JOIN {item} ON {cond}");
                        } else if kind == "FULL JOIN" {
                            sql = format!("{sql} FULL JOIN {item} ON {simple}");
                        } else {
                            sql = format!("{sql} {kind} {item} ON {cond}");
                        }
                    }
                }
            }
            if this_has_id {
                has_id.push(alias.clone());
            }
            scope.extend(cols);
        }
        // Occasionally a lateral subquery correlated with what came before.
        if depth >= 1 && self.chance(0.08) {
            let t = self.pick(&self.tables.clone()).clone();
            let (x, l) = (self.fresh("x"), self.fresh("l"));
            let c = self.pick(&t.cols).clone();
            let mut inner: Scope = t.cols.iter().map(|c| (format!("{x}.{}", c.name), c.kind)).collect();
            inner.extend(scope.iter().cloned());
            let cond = self.expr(K::Bool, 1, &inner);
            let agg = if c.kind.orderable() && !matches!(c.kind, K::Bool | K::Uuid | K::Bytea) { "max" } else { "count" };
            let kind = if agg == "count" { K::Int } else { c.kind };
            sql = format!("{sql} LEFT JOIN LATERAL (SELECT {agg}({x}.{}) AS v FROM {} {x} WHERE {cond}) {l} ON true", c.name, t.name);
            scope.push((format!("{l}.v"), kind));
        }
        (sql, scope, order, with)
    }

    fn aggregate(&mut self, sc: &Scope) -> (String, K) {
        let filter = if self.chance(0.15) {
            format!(" FILTER (WHERE {})", self.expr(K::Bool, 1, sc))
        } else {
            String::new()
        };
        let (call, k) = match self.below(19) {
            0 | 1 => ("count(*)".to_string(), K::Int),
            2 => {
                let k = *self.pick(&ALL_KINDS);
                (format!("count({})", self.expr(k, 1, sc)), K::Int)
            }
            3 => {
                let k = *self.pick(&[K::Int, K::Text, K::Num]);
                (format!("count(DISTINCT {})", self.expr(k, 1, sc)), K::Int)
            }
            4 => (format!("sum({})", self.expr(K::Int, 1, sc)), K::Num),
            5 => (format!("sum({})", self.expr(K::Num, 1, sc)), K::Num),
            6 => (format!("avg({})", { let kk = *self.pick(&[K::Int, K::Num]); self.expr(kk, 1, sc) }), K::Num),
            7 | 8 => {
                let k = *self.pick(&[K::Int, K::Text, K::Num, K::Date, K::Ts, K::Enum, K::Float]);
                let f = *self.pick(&["min", "max"]);
                (format!("{f}({})", self.expr(k, 1, sc)), k)
            }
            9 => {
                let f = *self.pick(&["bool_and", "bool_or"]);
                (format!("{f}({})", self.expr(K::Bool, 1, sc)), K::Bool)
            }
            10 => {
                let e = self.expr(K::Text, 1, sc);
                (format!("string_agg({e}, ',' ORDER BY {e})"), K::Text)
            }
            11 => {
                let e = self.expr(K::Int, 1, sc);
                (format!("array_agg(({e})::int ORDER BY ({e})::int)"), K::IntArr)
            }
            12 => {
                let f = *self.pick(&["stddev_pop", "var_samp", "stddev", "variance"]);
                (format!("round({f}({}), 6)", self.expr(K::Int, 1, sc)), K::Num)
            }
            13 => {
                let f = *self.pick(&["bit_and", "bit_or"]);
                (format!("{f}({})", self.expr(K::Int, 1, sc)), K::Int)
            }
            14 => {
                let e = self.expr(K::Int, 1, sc);
                (format!("jsonb_agg({e} ORDER BY {e})"), K::Jsonb)
            }
            // Float sums depend on the order of addition unless every value
            // is exactly representable, so only plain columns are summed.
            15 => (format!("sum({})", self.leaf(K::Float, sc)), K::Float),
            16 => (format!("avg({})", self.leaf(K::Float, sc)), K::Float),
            17 => {
                let e = format!("({})::text", self.expr(K::Text, 1, sc));
                (format!("string_agg(DISTINCT {e}, '|' ORDER BY {e})"), K::Text)
            }
            _ => {
                let k = *self.pick(&[K::IntArr, K::TextArr]);
                let f = *self.pick(&["min", "max"]);
                (format!("{f}({})", self.expr(k, 1, sc)), k)
            }
        };
        if call.contains("ORDER BY") && !filter.is_empty() {
            return (call, k);
        }
        (format!("{call}{filter}"), k)
    }

    fn window_call(&mut self, sc: &Scope, total: &[String]) -> (String, K) {
        let part = if self.chance(0.6) {
            let (c, _) = self.pick(sc).clone();
            format!("PARTITION BY {c} ")
        } else {
            String::new()
        };
        let dir = *self.pick(&["", " DESC"]);
        let order = format!("ORDER BY {}", total.iter().map(|c| format!("{c}{dir}")).collect::<Vec<_>>().join(", "));
        let frame = match self.below(6) {
            0 => " ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            1 => " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            2 => " ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING",
            3 => " ROWS 2 PRECEDING",
            _ => "",
        };
        match self.below(16) {
            0 => (format!("row_number() OVER ({part}{order})"), K::Int),
            1 => {
                // rank is deterministic under any ordering.
                let (c, k) = self.pick(sc).clone();
                let by = if k.orderable() { c } else { total[0].clone() };
                (format!("rank() OVER ({part}ORDER BY {by})"), K::Int)
            }
            2 => {
                let (c, k) = self.pick(sc).clone();
                let by = if k.orderable() { c } else { total[0].clone() };
                (format!("dense_rank() OVER ({part}ORDER BY {by})"), K::Int)
            }
            3 => (format!("sum({}) OVER ({part}{order}{frame})", self.expr(K::Int, 1, sc)), K::Num),
            4 => (format!("count(*) OVER ({part}{order}{frame})"), K::Int),
            5 => {
                let k = *self.pick(&[K::Int, K::Text, K::Num]);
                let e = self.expr(k, 1, sc);
                let f = *self.pick(&["lag", "lead"]);
                (format!("{f}({e}) OVER ({part}{order})"), k)
            }
            6 => {
                let k = *self.pick(&[K::Int, K::Text, K::Date]);
                let f = *self.pick(&["first_value", "last_value"]);
                (format!("{f}({}) OVER ({part}{order}{frame})", self.expr(k, 1, sc)), k)
            }
            7 => (format!("ntile({}) OVER ({part}{order})", self.small()), K::Int),
            8 => (format!("max({}) OVER ({part}{order}{frame})", self.expr(K::Text, 1, sc)), K::Text),
            9 => (format!("count({}) OVER ({part})", self.expr(K::Int, 1, sc)), K::Int),
            10 => (format!("nth_value({}, 2) OVER ({part}{order}{frame})", self.expr(K::Int, 1, sc)), K::Int),
            11 => (format!("round(percent_rank() OVER ({part}{order})::numeric, 6)"), K::Num),
            12 => (format!("round(cume_dist() OVER ({part}{order})::numeric, 6)"), K::Num),
            13 => {
                // RANGE and GROUPS frames with offsets over the first key.
                let unit = *self.pick(&["RANGE", "GROUPS"]);
                let f = *self.pick(&["sum", "count", "min", "max"]);
                let call = format!(
                    "{f}({}) OVER ({part}ORDER BY {}{dir} {unit} BETWEEN {} PRECEDING AND {} FOLLOWING)",
                    self.expr(K::Int, 1, sc),
                    total[0],
                    self.below(3),
                    self.below(3)
                );
                (call, if f == "sum" { K::Num } else { K::Int })
            }
            14 => (format!("avg({}) FILTER (WHERE {}) OVER ({part}{order})", self.expr(K::Num, 1, sc), self.expr(K::Bool, 1, sc)), K::Num),
            _ => {
                // RANGE frame with peers: deterministic for any ordering.
                let (c, k) = self.pick(sc).clone();
                let by = if k.orderable() { c } else { total[0].clone() };
                (format!("sum({}) OVER ({part}ORDER BY {by})", self.expr(K::Int, 1, sc)), K::Num)
            }
        }
    }

    /// Generates a SELECT. `want` fixes the kinds of the output columns.
    /// Returns `(sql, output kinds, rows are totally ordered)`.
    pub fn select(&mut self, depth: u32, want: Option<&[K]>, top: bool) -> (String, Vec<K>, bool) {
        // Set operations combine two queries of the same shape.
        if depth >= 1 && self.chance(0.08) {
            let kinds: Vec<K> = match want {
                Some(w) => w.to_vec(),
                None => (0..self.rng.gen_range(1..3)).map(|_| *self.pick(&[K::Int, K::Text, K::Num, K::Bool, K::Date])).collect(),
            };
            let (l, _, _) = self.select(depth - 1, Some(&kinds), false);
            let (r, _, _) = self.select(depth - 1, Some(&kinds), false);
            let op = *self.pick(&["UNION", "UNION ALL", "INTERSECT", "EXCEPT", "EXCEPT ALL", "INTERSECT ALL"]);
            // Rename through a derived table so column names are known.
            let a = self.fresh("u");
            let names: Vec<String> = (0..kinds.len()).map(|j| format!("c{j}")).collect();
            let sql = format!("SELECT * FROM (({l}) {op} ({r})) AS {a} ({})", names.join(", "));
            return (sql, kinds, false);
        }

        let (from, scope, total, with) = self.from_clause(depth);
        let filter = if self.chance(0.65) { Some(self.expr(K::Bool, depth.min(2) + 1, &scope)) } else { None };
        let mut items: Vec<(String, K)> = Vec::new();
        let mut group = String::new();
        let mut having = String::new();
        let mut distinct = "";
        let mut distinct_on: Option<String> = None;
        let mode = if want.is_some() { 0 } else { self.below(10) };
        match mode {
            // Grouped query.
            6 | 7 => {
                let ngroup = self.below(3);
                let mut keys = Vec::new();
                for _ in 0..ngroup {
                    let (c, k) = if self.chance(0.2) {
                        // Group by an expression instead of a plain column.
                        let k = *self.pick(&[K::Int, K::Text, K::Bool]);
                        (self.expr(k, 1, &scope), k)
                    } else {
                        self.pick(&scope).clone()
                    };
                    if c.contains("SELECT") || !c.contains('.') {
                        continue;
                    }
                    if !keys.contains(&c) {
                        keys.push(c.clone());
                        items.push((c, k));
                    }
                }
                for _ in 0..self.rng.gen_range(1..3) {
                    items.push(self.aggregate(&scope));
                }
                if !keys.is_empty() {
                    group = format!(" GROUP BY {}", keys.join(", "));
                }
                if self.chance(0.25) {
                    having = format!(" HAVING count(*) {} {}", self.pick(&[">", "<=", "="]), self.small());
                }
            }
            // Window functions over base tables.
            8 if total.as_ref().is_some_and(|t| !t.is_empty()) => {
                let total = total.clone().unwrap();
                let (c, k) = self.pick(&scope).clone();
                items.push((c, k));
                for _ in 0..self.rng.gen_range(1..3) {
                    items.push(self.window_call(&scope, &total));
                }
            }
            _ => {
                let kinds: Vec<K> = match want {
                    Some(w) => w.to_vec(),
                    None => (0..self.rng.gen_range(1..5))
                        .map(|_| if self.chance(0.6) { *self.pick(&[K::Int, K::Text, K::Num, K::Bool]) } else { *self.pick(&ALL_KINDS) })
                        .collect(),
                };
                for k in kinds {
                    items.push((self.expr(k, depth.min(2) + 1, &scope), k));
                }
                if mode == 9 {
                    distinct = "DISTINCT ";
                }
                if mode == 5 {
                    let cands: Vec<String> = scope.iter().filter(|(_, k)| k.orderable()).map(|(c, _)| c.clone()).collect();
                    if !cands.is_empty() {
                        distinct_on = Some(self.pick(&cands).clone());
                    }
                }
            }
        }
        let list: Vec<String> = items.iter().enumerate().map(|(i, (e, _))| format!("{e} AS c{i}")).collect();
        let kinds: Vec<K> = items.iter().map(|(_, k)| *k).collect();
        let sortable = kinds.iter().all(|k| k.orderable());
        if !sortable {
            distinct_on = None;
        }
        let on = distinct_on.as_ref().map_or(String::new(), |c| format!("DISTINCT ON ({c}) "));
        let mut sql = format!("SELECT {distinct}{on}{} FROM {from}", list.join(", "));
        if let Some(f) = filter {
            sql.push_str(&format!(" WHERE {f}"));
        }
        sql.push_str(&group);
        sql.push_str(&having);
        let mut ordered = false;
        if let Some(c) = &distinct_on {
            // Which row survives is decided by the rest of the sort order,
            // which covers every output column.
            let keys: Vec<String> = (1..=kinds.len()).map(|i| i.to_string()).collect();
            sql.push_str(&format!(" ORDER BY {c}, {}", keys.join(", ")));
        } else if sortable && self.chance(if top { 0.5 } else { 0.2 }) {
            // Ordering by every output column makes the result a sequence.
            let dir = *self.pick(&["", "", " DESC", " NULLS FIRST", " DESC NULLS LAST"]);
            let keys: Vec<String> = (1..=kinds.len()).map(|i| format!("{i}{dir}")).collect();
            sql.push_str(&format!(" ORDER BY {}", keys.join(", ")));
            ordered = true;
            if self.chance(0.5) {
                sql.push_str(&format!(" LIMIT {}", self.below(6)));
                if self.chance(0.3) {
                    sql.push_str(&format!(" OFFSET {}", self.below(4)));
                }
            }
        } else if sortable && mode < 6 && distinct.is_empty() && self.chance(0.1) {
            // Sorting by an arbitrary expression must not change the rows.
            let k = *self.pick(&[K::Int, K::Text]);
            if let Some(c) = self.col_of(k, &scope) {
                sql.push_str(&format!(" ORDER BY {c}"));
            }
        }
        if !with.is_empty() {
            let recursive = with.iter().any(|w| w.starts_with("RECURSIVE "));
            let defs: Vec<String> = with.iter().map(|w| w.trim_start_matches("RECURSIVE ").to_string()).collect();
            sql = format!("WITH {}{} {sql}", if recursive { "RECURSIVE " } else { "" }, defs.join(", "));
        }
        (sql, kinds, ordered)
    }

    // ------------------------------------------------------------------
    // Data modification, DDL, transactions
    // ------------------------------------------------------------------

    fn table_scope(t: &Table, alias: &str) -> Scope {
        t.cols.iter().map(|c| (format!("{alias}.{}", c.name), c.kind)).collect()
    }

    fn insert(&mut self) -> Stmt {
        let t = self.pick(&self.tables.clone()).clone();
        let returning = if self.chance(0.3) { " RETURNING *" } else { "" };
        if self.chance(0.1) {
            // INSERT ... SELECT from the table itself with shifted keys.
            self.next_id += 1000;
            let list: Vec<String> = t
                .cols
                .iter()
                .map(|c| if c.name == "id" { format!("id + {}", self.next_id) } else { c.name.clone() })
                .collect();
            let sql = format!("INSERT INTO {} SELECT {} FROM {} WHERE id < 6{returning}", t.name, list.join(", "), t.name);
            return Stmt::Dml { sql, table: t.name };
        }
        let nrows = if self.chance(0.2) { 2 } else { 1 };
        let partial = self.chance(0.3);
        let cols: Vec<&Col> = t.cols.iter().enumerate().filter(|(i, _)| t.pk.contains(i) || !partial || self.rng.gen_bool(0.6)).map(|(_, c)| c).collect();
        let mut rows = Vec::new();
        for _ in 0..nrows {
            let mut vals = Vec::new();
            for c in &cols {
                vals.push(if c.name == "id" {
                    // Mostly new keys, sometimes existing ones.
                    if self.chance(0.25) {
                        self.rng.gen_range(1..8).to_string()
                    } else {
                        self.next_id += 1;
                        self.next_id.to_string()
                    }
                } else if c.name == "k" {
                    self.below(3).to_string()
                } else if c.name == "ref" {
                    if self.chance(0.2) { "999".to_string() } else { self.rng.gen_range(1..5).to_string() }
                } else if self.chance(0.08) {
                    "DEFAULT".to_string()
                } else {
                    self.col_value(c)
                });
            }
            rows.push(format!("({})", vals.join(", ")));
        }
        let conflict = match self.below(8) {
            0 => " ON CONFLICT DO NOTHING".to_string(),
            1 => format!(" ON CONFLICT ({}) DO NOTHING", t.pk.iter().map(|&p| t.cols[p].name.clone()).collect::<Vec<_>>().join(", ")),
            2 if t.cols.len() > t.pk.len() => {
                let c = &t.cols[t.pk.len() + self.below(t.cols.len() - t.pk.len())];
                let filter = if self.chance(0.3) { format!(" WHERE {}.id > 2", t.name) } else { String::new() };
                format!(
                    " ON CONFLICT ({}) DO UPDATE SET {} = excluded.{}{filter}",
                    t.pk.iter().map(|&p| t.cols[p].name.clone()).collect::<Vec<_>>().join(", "),
                    c.name,
                    c.name
                )
            }
            _ => String::new(),
        };
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        let sql = format!("INSERT INTO {} ({}) VALUES {}{conflict}{returning}", t.name, names.join(", "), rows.join(", "));
        Stmt::Dml { sql, table: t.name }
    }

    fn update(&mut self) -> Stmt {
        let t = self.pick(&self.tables.clone()).clone();
        let mut scope = Self::table_scope(&t, &t.name);
        let mut from = String::new();
        if self.chance(0.15) {
            let o = self.pick(&self.tables.clone()).clone();
            let a = self.fresh("f");
            // Join on the key so that each target row matches at most once.
            if o.pk.len() == 1 && t.pk.len() == 1 {
                from = format!(" FROM {} {a} WHERE {a}.id = {}.id", o.name, t.name);
                scope.extend(Self::table_scope(&o, &a));
            }
        }
        let settable: Vec<&Col> = t.cols.iter().enumerate().filter(|(i, c)| !t.pk.contains(i) && c.name != "ref").map(|(_, c)| c).collect();
        if settable.is_empty() {
            return self.insert();
        }
        let mut sets = Vec::new();
        for c in settable.choose_multiple(&mut self.rng.clone(), 1 + self.below(2)) {
            let value = if c.unique || self.chance(0.3) {
                self.col_value(c)
            } else if c.decl == "smallint" || c.decl.starts_with("varchar") {
                self.leaf(c.kind, &scope)
            } else {
                self.expr(c.kind, 2, &scope)
            };
            sets.push(format!("{} = {value}", c.name));
        }
        let cond = if self.chance(0.85) { Some(self.expr(K::Bool, 2, &scope)) } else { None };
        let filter = match (from.is_empty(), cond) {
            (true, Some(c)) => format!(" WHERE {c}"),
            (false, Some(c)) => format!(" AND {c}"),
            _ => String::new(),
        };
        let returning = if self.chance(0.3) { format!(" RETURNING {}.*", t.name) } else { String::new() };
        Stmt::Dml { sql: format!("UPDATE {} SET {}{from}{filter}{returning}", t.name, sets.join(", ")), table: t.name }
    }

    fn delete(&mut self) -> Stmt {
        let t = self.pick(&self.tables.clone()).clone();
        let mut scope = Self::table_scope(&t, &t.name);
        let mut using = String::new();
        if self.chance(0.15) {
            let o = self.pick(&self.tables.clone()).clone();
            let a = self.fresh("f");
            using = format!(" USING {} {a} WHERE {a}.id = {}.id", o.name, t.name);
            scope.extend(Self::table_scope(&o, &a));
        }
        let cond = self.expr(K::Bool, 2, &scope);
        let filter = if using.is_empty() { format!(" WHERE {cond}") } else { format!(" AND {cond}") };
        let returning = if self.chance(0.3) { format!(" RETURNING {}.*", t.name) } else { String::new() };
        Stmt::Dml { sql: format!("DELETE FROM {}{using}{filter}{returning}", t.name), table: t.name }
    }

    fn ddl(&mut self) -> Stmt {
        let ti = self.below(self.tables.len());
        let t = self.tables[ti].clone();
        match self.below(4) {
            0 => {
                self.extra += 1;
                let kind = *self.pick(&[K::Int, K::Text, K::Num, K::Bool, K::Date]);
                let decl = self.col_decl(kind);
                let name = format!("x{}", self.extra);
                let col = Col { name: name.clone(), decl: decl.clone(), kind, not_null: false, unique: false };
                let default = if self.chance(0.5) { format!(" DEFAULT {}", self.col_value(&Col { not_null: true, ..col.clone() })) } else { String::new() };
                self.tables[ti].cols.push(col);
                Stmt::Dml { sql: format!("ALTER TABLE {} ADD COLUMN {name} {decl}{default}", t.name), table: t.name }
            }
            1 => {
                let droppable: Vec<usize> =
                    (0..t.cols.len()).filter(|i| !t.pk.contains(i) && t.cols[*i].name.starts_with('x')).collect();
                if droppable.is_empty() {
                    return self.ddl_index(ti);
                }
                let i = *self.pick(&droppable);
                let name = self.tables[ti].cols.remove(i).name;
                Stmt::Dml { sql: format!("ALTER TABLE {} DROP COLUMN {name}", t.name), table: t.name }
            }
            _ => self.ddl_index(ti),
        }
    }

    fn ddl_index(&mut self, ti: usize) -> Stmt {
        let t = self.tables[ti].clone();
        if !t.indexes.is_empty() && self.chance(0.4) {
            let ix = self.tables[ti].indexes.pop().unwrap();
            return Stmt::Other { sql: format!("DROP INDEX {ix}") };
        }
        let cands: Vec<&Col> = t.cols.iter().filter(|c| c.kind.orderable() && !c.name.starts_with('x')).collect();
        let c = (*self.pick(&cands)).clone();
        self.extra += 1;
        let ix = format!("{}_fx{}", t.name, self.extra);
        self.tables[ti].indexes.push(ix.clone());
        let second = if self.chance(0.3) { ", id" } else { "" };
        Stmt::Other { sql: format!("CREATE INDEX {ix} ON {} ({}{second})", t.name, c.name) }
    }

    fn txn(&mut self) -> Stmt {
        let sql = match self.txn {
            TxnState::None => {
                self.txn = TxnState::Open;
                self.saved = vec![self.tables.clone()];
                self.savepoints = 0;
                self.pick(&["BEGIN", "BEGIN", "BEGIN ISOLATION LEVEL REPEATABLE READ"]).to_string()
            }
            TxnState::Open => match self.below(10) {
                0..=2 => {
                    self.savepoints += 1;
                    self.saved.push(self.tables.clone());
                    format!("SAVEPOINT sp{}", self.savepoints)
                }
                3 | 4 if self.savepoints > 0 => {
                    let n = self.rng.gen_range(1..=self.savepoints);
                    self.saved.truncate(n + 1);
                    self.tables = self.saved[n].clone();
                    self.savepoints = n;
                    format!("ROLLBACK TO SAVEPOINT sp{n}")
                }
                5 | 6 => {
                    self.txn = TxnState::None;
                    self.tables = self.saved[0].clone();
                    "ROLLBACK".to_string()
                }
                _ => {
                    self.txn = TxnState::None;
                    "COMMIT".to_string()
                }
            },
        };
        Stmt::Other { sql }
    }

    /// Called when a statement failed on both sides inside a transaction:
    /// the model forgets the DDL of the aborted transaction at its end.
    pub fn note_failure_in_txn(&mut self) {
        if self.txn == TxnState::Open {
            // The transaction is now aborted; end it right away so that the
            // model and both databases agree again.
            self.pending_rollback = true;
        }
    }

    pub fn next(&mut self) -> Stmt {
        if self.pending_rollback {
            self.pending_rollback = false;
            self.txn = TxnState::None;
            self.tables = self.saved[0].clone();
            return Stmt::Other { sql: "ROLLBACK".to_string() };
        }
        let roll = match self.only {
            Only::Select => 0,
            Only::Dml => 60 + self.below(30),
            Only::Ddl => 90 + self.below(5),
            Only::Txn => {
                if self.chance(0.3) {
                    95
                } else {
                    60 + self.below(30)
                }
            }
            Only::All => self.below(100),
        };
        match roll {
            0..=59 => {
                let depth = self.rng.gen_range(0..=3);
                let (sql, _, ordered) = self.select(depth, None, true);
                Stmt::Query { sql, ordered }
            }
            60..=71 => self.insert(),
            72..=81 => self.update(),
            82..=89 => self.delete(),
            90..=94 => self.ddl(),
            _ => self.txn(),
        }
    }
}

fn cast_name(k: K) -> &'static str {
    match k {
        K::Int => "integer",
        K::Num => "numeric",
        K::Float => "float8",
        K::Text => "text",
        K::Bool => "boolean",
        K::Date => "date",
        K::Ts => "timestamp",
        K::Tstz => "timestamptz",
        K::Uuid => "uuid",
        K::Bytea => "bytea",
        K::Jsonb => "jsonb",
        K::IntArr => "integer[]",
        K::TextArr => "text[]",
        K::Enum => "mood",
    }
}
