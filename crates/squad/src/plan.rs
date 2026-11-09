//! Query plans: the tree the executor runs.

use std::sync::Arc;

use crate::catalog::{Index, Table};
use crate::expr::Expr;
use crate::types::Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    /// Emit left rows with at least one match (`EXISTS`, `IN`).
    Semi,
    /// Emit left rows without a match (`NOT EXISTS`).
    Anti,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: Expr,
    pub desc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    BoolAnd,
    BoolOr,
    BitAnd,
    BitOr,
    StringAgg,
    ArrayAgg,
    JsonAgg {
        jsonb: bool,
    },
    JsonObjectAgg {
        jsonb: bool,
    },
    /// Variance / standard deviation; flags: (sample, sqrt).
    Variance {
        sample: bool,
        stddev: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggCall {
    pub func: AggFunc,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub filter: Option<Expr>,
    /// `agg(x ORDER BY ...)`
    pub order_by: Vec<SortKey>,
    /// Result type (decides e.g. whether `sum` yields bigint or numeric).
    pub ret: Type,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    Agg(AggFunc),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameUnit {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(Expr),
    CurrentRow,
    Following(Expr),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub unit: FrameUnit,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowCall {
    pub func: WindowFunc,
    pub args: Vec<Expr>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortKey>,
    pub frame: Frame,
    pub filter: Option<Expr>,
    pub ret: Type,
}

/// Bounds of a scan over a table's primary key or one of its indexes.
#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub table: Arc<Table>,
    /// `None` scans the table in primary key order; otherwise the position
    /// in `table.indexes`.
    pub index: Option<usize>,
    /// Equality constraints on the leading key columns. Evaluated once per
    /// (re)scan against an empty row.
    pub eq: Vec<Expr>,
    /// Range on the key column following the equality prefix:
    /// `(bound, inclusive)`.
    pub lower: Option<(Expr, bool)>,
    pub upper: Option<(Expr, bool)>,
    pub reverse: bool,
    /// Remaining predicate, over the table row.
    pub filter: Option<Expr>,
}

impl ScanPlan {
    pub fn full(table: Arc<Table>) -> ScanPlan {
        ScanPlan { table, index: None, eq: Vec::new(), lower: None, upper: None, reverse: false, filter: None }
    }

    pub fn index_def(&self) -> Option<&Index> {
        self.index.map(|i| &self.table.indexes[i])
    }

    /// Table column positions of the key this scan is ordered by.
    pub fn key_columns(&self) -> &[usize] {
        match self.index_def() {
            Some(ix) => &ix.columns,
            None => &self.table.pk,
        }
    }

    /// True if the scan can return at most one row.
    pub fn is_unique_lookup(&self) -> bool {
        match self.index_def() {
            None => self.eq.len() == self.table.pk.len(),
            Some(ix) => ix.unique && self.eq.len() == ix.columns.len(),
        }
    }
}

/// Set-returning functions usable in FROM.
#[derive(Debug, Clone, PartialEq)]
pub enum Srf {
    GenerateSeries,
    Unnest,
    JsonArrayElements { text: bool },
    JsonEach { text: bool },
    JsonObjectKeys,
    RegexpSplitToTable,
    StringToTable,
    GenerateSubscripts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLock {
    Update,
    Share,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockWait {
    Block,
    Skip,
    Error,
}

#[derive(Debug, Clone)]
pub struct CteDef {
    pub id: usize,
    pub name: String,
    pub plan: Plan,
    /// For `WITH RECURSIVE`: the recursive term, which reads the working
    /// table through `Plan::CteScan(id)`; `union_all` keeps duplicates.
    pub recursive: Option<(Plan, bool)>,
}

#[derive(Debug, Clone)]
pub enum OnConflict {
    Nothing {
        /// Arbiter index position (`None`: any unique index).
        target: Option<Option<usize>>,
    },
    Update {
        /// `None` means the primary key.
        target: Option<usize>,
        /// Assignments `(column, value)` over the row `existing ++ excluded`.
        set: Vec<(usize, Expr)>,
        filter: Option<Expr>,
    },
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub table: Arc<Table>,
    /// Produces rows already shaped like the table (all columns, defaults
    /// filled in, values coerced).
    pub source: Plan,
    pub on_conflict: Option<OnConflict>,
    /// CHECK constraints `(name, predicate over the table row)`.
    pub checks: Vec<(String, Expr)>,
    /// Over the inserted (or updated) table row.
    pub returning: Option<Vec<Expr>>,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table: Arc<Table>,
    /// Produces `table row ++ extra columns` (from `UPDATE ... FROM`).
    pub source: Plan,
    /// `(column, value)` over the source row.
    pub set: Vec<(usize, Expr)>,
    /// The WHERE clause over the source row, re-evaluated when a row was
    /// changed concurrently (Read Committed).
    pub recheck: Option<Expr>,
    pub checks: Vec<(String, Expr)>,
    /// Over `new table row ++ extra columns`.
    pub returning: Option<Vec<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub table: Arc<Table>,
    /// Produces `table row ++ extra columns` (from `DELETE ... USING`).
    pub source: Plan,
    pub recheck: Option<Expr>,
    pub returning: Option<Vec<Expr>>,
}

#[derive(Debug, Clone)]
pub enum Plan {
    /// Exactly one empty row (`SELECT 1`).
    OneRow,
    Values(Vec<Vec<Expr>>),
    Scan(ScanPlan),
    /// Rows of a system catalog relation, generated from the catalog.
    System(String),
    Filter(Box<Plan>, Expr),
    Project(Box<Plan>, Vec<Expr>),
    NestedLoop {
        left: Box<Plan>,
        right: Box<Plan>,
        kind: JoinKind,
        /// Over `left row ++ right row`.
        on: Option<Expr>,
        /// The right side is re-planned per left row and sees it either as
        /// `Outer(1, _)` (`lateral`) or as `NlLeft(_)` (index join).
        lateral: bool,
        index_join: bool,
        right_width: usize,
    },
    HashJoin {
        left: Box<Plan>,
        right: Box<Plan>,
        kind: JoinKind,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        /// Residual condition over `left row ++ right row`.
        filter: Option<Expr>,
        left_width: usize,
        right_width: usize,
    },
    /// Output: group expressions, then aggregates.
    Aggregate {
        input: Box<Plan>,
        groups: Vec<Expr>,
        aggs: Vec<AggCall>,
    },
    /// Output: input columns, then one column per window call.
    Window {
        input: Box<Plan>,
        calls: Vec<WindowCall>,
    },
    Sort {
        input: Box<Plan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<Plan>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    /// `on == None`: whole-row DISTINCT. Otherwise DISTINCT ON over sorted
    /// input: the first row of each run of equal keys is kept.
    Distinct {
        input: Box<Plan>,
        on: Option<Vec<Expr>>,
    },
    SetOp {
        kind: SetOpKind,
        all: bool,
        left: Box<Plan>,
        right: Box<Plan>,
    },
    /// Materializes the CTEs, then runs the body.
    With {
        ctes: Vec<CteDef>,
        body: Box<Plan>,
    },
    CteScan(usize),
    FuncScan {
        func: Srf,
        args: Vec<Expr>,
        ordinality: bool,
    },
    /// Set-returning functions in the select list: output is the input row
    /// followed by one column per function.
    ProjectSet {
        input: Box<Plan>,
        funcs: Vec<(Srf, Vec<Expr>)>,
    },
    /// Locks the rows flowing through (`FOR UPDATE` / `FOR SHARE`). The
    /// table's columns start at `offset` in the input row.
    LockRows {
        input: Box<Plan>,
        table: Arc<Table>,
        offset: usize,
        mode: RowLock,
        wait: LockWait,
        /// The query's WHERE clause, re-evaluated when a row was changed
        /// by a concurrent transaction while waiting for its lock.
        recheck: Option<Expr>,
    },
    Insert(Box<InsertPlan>),
    Update(Box<UpdatePlan>),
    Delete(Box<DeletePlan>),
}
