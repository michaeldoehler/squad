//! Table sources and scans.

use std::sync::Arc;

use crate::predicate::{selection, Predicate};
use crate::segment::Segment;
use crate::types::{Batch, Column, Schema};
use crate::{Error, Result};

/// What a query runs over: immutable segments plus rows that have not
/// been written to a segment yet (the "delta").
#[derive(Clone)]
pub struct TableSource {
    pub schema: Arc<Schema>,
    pub segments: Vec<Arc<Segment>>,
    /// In-memory batches with the table's schema.
    pub delta: Vec<Batch>,
}

impl TableSource {
    pub fn new(schema: Arc<Schema>) -> TableSource {
        TableSource { schema, segments: Vec::new(), delta: Vec::new() }
    }

    pub fn num_rows(&self) -> u64 {
        self.segments.iter().map(|s| s.num_rows()).sum::<u64>()
            + self.delta.iter().map(|b| b.num_rows() as u64).sum::<u64>()
    }

    pub(crate) fn check(&self) -> Result<()> {
        let same = |other: &Schema| {
            other.columns.len() == self.schema.columns.len()
                && other.columns.iter().zip(&self.schema.columns).all(|(a, b)| a.ty == b.ty)
        };
        if self.segments.iter().any(|s| !same(s.schema())) || self.delta.iter().any(|b| !same(&b.schema)) {
            return Err(Error::Type("a segment or delta batch does not have the table's schema".into()));
        }
        Ok(())
    }

    /// The units of work of a query, after dropping whole segments whose
    /// statistics rule them out.
    pub(crate) fn units(&self, predicate: Option<&Predicate>) -> Vec<Unit> {
        let mut units = Vec::new();
        for (s, seg) in self.segments.iter().enumerate() {
            if let Some(p) = predicate {
                if !p.may_match(&|c| Some(seg.column_stats(c))) {
                    continue;
                }
            }
            units.extend((0..seg.num_row_groups()).map(|g| Unit::RowGroup(s, g)));
        }
        units.extend((0..self.delta.len()).map(Unit::Delta));
        units
    }

    /// Loads one unit: applies the predicate (pruning by zone maps and
    /// bloom filters first) and returns the requested columns with the
    /// selected rows, or `None` if no row qualifies.
    pub(crate) fn load(&self, unit: Unit, predicate: Option<&Predicate>, columns: &[usize]) -> Result<Option<Loaded>> {
        let ncols = self.schema.len();
        let mut cols: Vec<Option<Column>> = vec![None; ncols];
        let mut pred_cols = Vec::new();
        if let Some(p) = predicate {
            p.columns(&mut pred_cols);
        }
        if let Some(bad) = pred_cols.iter().chain(columns).find(|c| **c >= ncols) {
            return Err(Error::Invalid(format!("column {bad} does not exist")));
        }
        let rows = match unit {
            Unit::RowGroup(s, g) => {
                let seg = &self.segments[s];
                let meta = seg.row_group(g);
                if let Some(p) = predicate {
                    if !p.may_match(&|c| Some(&meta.columns[c].stats)) {
                        return Ok(None);
                    }
                    let mut required = Vec::new();
                    p.required_values(&mut required);
                    for (c, values) in required {
                        if meta.columns[c].has_bloom() {
                            if let Some(bloom) = seg.bloom(g, c)? {
                                if !values.iter().any(|v| bloom.may_contain(v)) {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
                for &c in &pred_cols {
                    cols[c] = Some(seg.read_column(g, c)?);
                }
                meta.rows as usize
            }
            Unit::Delta(i) => {
                let batch = &self.delta[i];
                for &c in pred_cols.iter().chain(columns) {
                    if cols[c].is_none() {
                        cols[c] = Some(batch.columns[c].clone());
                    }
                }
                batch.num_rows()
            }
        };
        if rows == 0 {
            return Ok(None);
        }
        let sel = match predicate {
            Some(p) => {
                let sel = selection(&p.evaluate(&cols, rows)?);
                if sel.as_ref().is_some_and(Vec::is_empty) {
                    return Ok(None);
                }
                sel
            }
            None => None,
        };
        if let Unit::RowGroup(s, g) = unit {
            for &c in columns {
                if cols[c].is_none() {
                    cols[c] = Some(self.segments[s].read_column(g, c)?);
                }
            }
        }
        Ok(Some(Loaded { columns: cols, rows, selection: sel }))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Unit {
    RowGroup(usize, usize),
    Delta(usize),
}

/// Columns of one unit (indexed by schema position; only the requested
/// ones are present) and the rows that passed the predicate.
pub(crate) struct Loaded {
    pub(crate) columns: Vec<Option<Column>>,
    pub(crate) rows: usize,
    /// `None`: all rows.
    pub(crate) selection: Option<Vec<u32>>,
}

impl Loaded {
    pub(crate) fn selected(&self) -> usize {
        self.selection.as_ref().map_or(self.rows, Vec::len)
    }
}

/// A filtered, projected scan.
#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    /// Columns to return, by schema position, in this order.
    pub projection: Vec<usize>,
    pub predicate: Option<Predicate>,
    /// Stop after this many rows.
    pub limit: Option<usize>,
}

/// Iterator over the batches of a scan. Each batch has the projected
/// columns; row order follows segment order, then the delta.
pub struct Scan<'a> {
    source: &'a TableSource,
    request: &'a ScanRequest,
    schema: Arc<Schema>,
    units: std::vec::IntoIter<Unit>,
    remaining: Option<usize>,
}

/// Starts a scan.
pub fn scan<'a>(source: &'a TableSource, request: &'a ScanRequest) -> Result<Scan<'a>> {
    source.check()?;
    if let Some(bad) = request.projection.iter().find(|c| **c >= source.schema.len()) {
        return Err(Error::Invalid(format!("column {bad} does not exist")));
    }
    let schema = Schema::new(request.projection.iter().map(|&c| source.schema.columns[c].clone()).collect());
    Ok(Scan {
        source,
        request,
        schema: Arc::new(schema),
        units: source.units(request.predicate.as_ref()).into_iter(),
        remaining: request.limit,
    })
}

impl Scan<'_> {
    /// Schema of the batches this scan returns.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

impl Iterator for Scan<'_> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Result<Batch>> {
        loop {
            if self.remaining == Some(0) {
                return None;
            }
            let unit = self.units.next()?;
            let loaded = match self.source.load(unit, self.request.predicate.as_ref(), &self.request.projection) {
                Ok(Some(l)) => l,
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            };
            let mut sel = loaded.selection;
            if let Some(limit) = self.remaining {
                let have = sel.as_ref().map_or(loaded.rows, Vec::len);
                if have > limit {
                    sel = Some(match sel {
                        Some(mut s) => {
                            s.truncate(limit);
                            s
                        }
                        None => (0..limit as u32).collect(),
                    });
                }
                self.remaining = Some(limit - have.min(limit));
            }
            let columns = self
                .request
                .projection
                .iter()
                .map(|&c| {
                    let col = loaded.columns[c].as_ref().expect("projected column was loaded");
                    match &sel {
                        Some(s) => col.take(s),
                        None => col.clone(),
                    }
                })
                .collect();
            return Some(Batch::new(self.schema.clone(), columns));
        }
    }
}
