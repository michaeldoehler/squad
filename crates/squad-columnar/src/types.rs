//! Data model: schemas, single values, columns and batches.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::{Error, Result};

/// Type of a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Bool,
    /// 64-bit integer. Also used for narrower integers, dates, timestamps
    /// in microseconds and enum ordinals.
    Int64,
    Float64,
    /// Fixed-point number: an `i128` unscaled value with this many digits
    /// after the decimal point.
    Decimal {
        scale: u8,
    },
    /// Arbitrary bytes.
    Bytes,
    /// UTF-8 text. Stored and compared like [`ColumnType::Bytes`].
    Utf8,
}

impl ColumnType {
    pub fn is_bytes(&self) -> bool {
        matches!(self, ColumnType::Bytes | ColumnType::Utf8)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, ty: ColumnType, nullable: bool) -> Field {
        Field { name: name.into(), ty, nullable }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Schema {
    pub columns: Vec<Field>,
}

impl Schema {
    pub fn new(columns: Vec<Field>) -> Schema {
        Schema { columns }
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// A single value. `Bytes` serves both byte and text columns; `Decimal`
/// is the unscaled value at the scale of its column.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(i128),
    Bytes(Vec<u8>),
}

impl Datum {
    pub fn text(s: impl AsRef<str>) -> Datum {
        Datum::Bytes(s.as_ref().as_bytes().to_vec())
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Datum::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Datum::Float(v) => Some(*v),
            Datum::Int(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Datum::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }
}

/// Total order of floats as PostgreSQL defines it: NaN equals NaN and is
/// greater than every other value.
pub fn cmp_f64(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => a.partial_cmp(&b).unwrap(),
    }
}

/// Orders two values of the same type. NULL sorts after everything.
pub fn cmp_datum(a: &Datum, b: &Datum) -> Ordering {
    match (a, b) {
        (Datum::Null, Datum::Null) => Ordering::Equal,
        (Datum::Null, _) => Ordering::Greater,
        (_, Datum::Null) => Ordering::Less,
        (Datum::Bool(x), Datum::Bool(y)) => x.cmp(y),
        (Datum::Int(x), Datum::Int(y)) => x.cmp(y),
        (Datum::Float(x), Datum::Float(y)) => cmp_f64(*x, *y),
        (Datum::Int(x), Datum::Float(y)) => cmp_f64(*x as f64, *y),
        (Datum::Float(x), Datum::Int(y)) => cmp_f64(*x, *y as f64),
        (Datum::Decimal(x), Datum::Decimal(y)) => x.cmp(y),
        (Datum::Bytes(x), Datum::Bytes(y)) => x.cmp(y),
        _ => rank(a).cmp(&rank(b)),
    }
}

fn rank(d: &Datum) -> u8 {
    match d {
        Datum::Bool(_) => 0,
        Datum::Int(_) | Datum::Float(_) => 1,
        Datum::Decimal(_) => 2,
        Datum::Bytes(_) => 3,
        Datum::Null => 4,
    }
}

/// Variable-length values stored back to back: value `i` is
/// `data[offsets[i]..offsets[i + 1]]`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BytesData {
    pub offsets: Vec<u32>,
    pub data: Vec<u8>,
}

impl BytesData {
    pub fn new() -> BytesData {
        BytesData { offsets: vec![0], data: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, i: usize) -> &[u8] {
        &self.data[self.offsets[i] as usize..self.offsets[i + 1] as usize]
    }

    pub fn push(&mut self, v: &[u8]) {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        self.data.extend_from_slice(v);
        self.offsets.push(self.data.len() as u32);
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).map(move |i| self.get(i))
    }
}

/// Values of a byte or text column, either directly or as codes into a
/// dictionary of distinct values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BytesColumn {
    Plain(BytesData),
    Dict { codes: Vec<u32>, dict: Arc<BytesData> },
}

impl BytesColumn {
    pub fn len(&self) -> usize {
        match self {
            BytesColumn::Plain(d) => d.len(),
            BytesColumn::Dict { codes, .. } => codes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, i: usize) -> &[u8] {
        match self {
            BytesColumn::Plain(d) => d.get(i),
            BytesColumn::Dict { codes, dict } => dict.get(codes[i] as usize),
        }
    }
}

/// Values of one column. Slots of NULL rows hold an arbitrary value.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Bool(Vec<bool>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Decimal(Vec<i128>),
    Bytes(BytesColumn),
}

/// A column of values with an optional validity mask (`true` = not NULL).
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub data: ColumnData,
    pub validity: Option<Vec<bool>>,
}

impl Column {
    pub fn new(data: ColumnData, validity: Option<Vec<bool>>) -> Column {
        // A mask without any NULL is dropped so "no mask" means "no NULLs".
        let validity = validity.filter(|v| v.iter().any(|x| !x));
        Column { data, validity }
    }

    pub fn len(&self) -> usize {
        match &self.data {
            ColumnData::Bool(v) => v.len(),
            ColumnData::Int64(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::Decimal(v) => v.len(),
            ColumnData::Bytes(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.validity.as_ref().is_some_and(|v| !v[i])
    }

    pub fn null_count(&self) -> usize {
        self.validity.as_ref().map_or(0, |v| v.iter().filter(|x| !**x).count())
    }

    pub fn datum(&self, i: usize) -> Datum {
        if self.is_null(i) {
            return Datum::Null;
        }
        match &self.data {
            ColumnData::Bool(v) => Datum::Bool(v[i]),
            ColumnData::Int64(v) => Datum::Int(v[i]),
            ColumnData::Float64(v) => Datum::Float(v[i]),
            ColumnData::Decimal(v) => Datum::Decimal(v[i]),
            ColumnData::Bytes(v) => Datum::Bytes(v.get(i).to_vec()),
        }
    }

    /// Builds a column of type `ty` from values.
    pub fn from_datums<'a>(ty: ColumnType, values: impl IntoIterator<Item = &'a Datum>) -> Result<Column> {
        let mut b = ColumnBuilder::new(ty);
        for v in values {
            b.push(v)?;
        }
        Ok(b.finish())
    }

    /// The rows at `indices`, in that order.
    pub fn take(&self, indices: &[u32]) -> Column {
        let validity = self.validity.as_ref().map(|v| indices.iter().map(|&i| v[i as usize]).collect());
        let data = match &self.data {
            ColumnData::Bool(v) => ColumnData::Bool(indices.iter().map(|&i| v[i as usize]).collect()),
            ColumnData::Int64(v) => ColumnData::Int64(indices.iter().map(|&i| v[i as usize]).collect()),
            ColumnData::Float64(v) => ColumnData::Float64(indices.iter().map(|&i| v[i as usize]).collect()),
            ColumnData::Decimal(v) => ColumnData::Decimal(indices.iter().map(|&i| v[i as usize]).collect()),
            ColumnData::Bytes(BytesColumn::Dict { codes, dict }) => ColumnData::Bytes(BytesColumn::Dict {
                codes: indices.iter().map(|&i| codes[i as usize]).collect(),
                dict: dict.clone(),
            }),
            ColumnData::Bytes(BytesColumn::Plain(d)) => {
                let mut out = BytesData::new();
                for &i in indices {
                    out.push(d.get(i as usize));
                }
                ColumnData::Bytes(BytesColumn::Plain(out))
            }
        };
        Column::new(data, validity)
    }

    /// Appends all rows of `other`, which must have the same type.
    pub fn append(&mut self, other: &Column) {
        let (n, m) = (self.len(), other.len());
        if self.validity.is_some() || other.validity.is_some() {
            let mut v = self.validity.take().unwrap_or_else(|| vec![true; n]);
            match &other.validity {
                Some(o) => v.extend_from_slice(o),
                None => v.resize(n + m, true),
            }
            self.validity = Some(v);
        }
        match (&mut self.data, &other.data) {
            (ColumnData::Bool(a), ColumnData::Bool(b)) => a.extend_from_slice(b),
            (ColumnData::Int64(a), ColumnData::Int64(b)) => a.extend_from_slice(b),
            (ColumnData::Float64(a), ColumnData::Float64(b)) => a.extend_from_slice(b),
            (ColumnData::Decimal(a), ColumnData::Decimal(b)) => a.extend_from_slice(b),
            (ColumnData::Bytes(a), ColumnData::Bytes(b)) => {
                // Appending turns a dictionary column into a plain one.
                let mut plain = match std::mem::replace(a, BytesColumn::Plain(BytesData::new())) {
                    BytesColumn::Plain(d) => d,
                    dict @ BytesColumn::Dict { .. } => {
                        let mut d = BytesData::new();
                        (0..dict.len()).for_each(|i| d.push(dict.get(i)));
                        d
                    }
                };
                (0..b.len()).for_each(|i| plain.push(b.get(i)));
                *a = BytesColumn::Plain(plain);
            }
            _ => panic!("appending columns of different types"),
        }
    }
}

/// Builds a column value by value.
pub struct ColumnBuilder {
    ty: ColumnType,
    data: ColumnData,
    validity: Vec<bool>,
    has_null: bool,
}

impl ColumnBuilder {
    pub fn new(ty: ColumnType) -> ColumnBuilder {
        let data = match ty {
            ColumnType::Bool => ColumnData::Bool(Vec::new()),
            ColumnType::Int64 => ColumnData::Int64(Vec::new()),
            ColumnType::Float64 => ColumnData::Float64(Vec::new()),
            ColumnType::Decimal { .. } => ColumnData::Decimal(Vec::new()),
            ColumnType::Bytes | ColumnType::Utf8 => ColumnData::Bytes(BytesColumn::Plain(BytesData::new())),
        };
        ColumnBuilder { ty, data, validity: Vec::new(), has_null: false }
    }

    pub fn push(&mut self, v: &Datum) -> Result<()> {
        let null = v.is_null();
        self.validity.push(!null);
        self.has_null |= null;
        match (&mut self.data, v) {
            (ColumnData::Bool(c), Datum::Bool(x)) => c.push(*x),
            (ColumnData::Bool(c), Datum::Null) => c.push(false),
            (ColumnData::Int64(c), Datum::Int(x)) => c.push(*x),
            (ColumnData::Int64(c), Datum::Null) => c.push(0),
            (ColumnData::Float64(c), Datum::Float(x)) => c.push(*x),
            (ColumnData::Float64(c), Datum::Int(x)) => c.push(*x as f64),
            (ColumnData::Float64(c), Datum::Null) => c.push(0.0),
            (ColumnData::Decimal(c), Datum::Decimal(x)) => c.push(*x),
            (ColumnData::Decimal(c), Datum::Null) => c.push(0),
            (ColumnData::Bytes(BytesColumn::Plain(c)), Datum::Bytes(x)) => c.push(x),
            (ColumnData::Bytes(BytesColumn::Plain(c)), Datum::Null) => c.push(&[]),
            (_, other) => {
                self.validity.pop();
                return Err(Error::Type(format!("value {other:?} does not fit a column of type {:?}", self.ty)));
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Column {
        Column::new(self.data, if self.has_null { Some(self.validity) } else { None })
    }
}

/// A set of rows in columnar form.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    pub schema: Arc<Schema>,
    pub columns: Vec<Column>,
}

impl Batch {
    pub fn new(schema: Arc<Schema>, columns: Vec<Column>) -> Result<Batch> {
        if columns.len() != schema.len() {
            return Err(Error::Type("batch has a different number of columns than its schema".into()));
        }
        if let Some(first) = columns.first() {
            if columns.iter().any(|c| c.len() != first.len()) {
                return Err(Error::Type("batch columns differ in length".into()));
            }
        }
        Ok(Batch { schema, columns })
    }

    pub fn empty(schema: Arc<Schema>) -> Batch {
        let columns = schema.columns.iter().map(|f| ColumnBuilder::new(f.ty).finish()).collect();
        Batch { schema, columns }
    }

    pub fn num_rows(&self) -> usize {
        self.columns.first().map_or(0, Column::len)
    }

    /// Converts rows of values into a batch.
    pub fn from_rows(schema: Arc<Schema>, rows: &[Vec<Datum>]) -> Result<Batch> {
        let mut builders: Vec<ColumnBuilder> = schema.columns.iter().map(|f| ColumnBuilder::new(f.ty)).collect();
        for row in rows {
            if row.len() != builders.len() {
                return Err(Error::Type("row has a different number of values than the schema has columns".into()));
            }
            for (b, v) in builders.iter_mut().zip(row) {
                b.push(v)?;
            }
        }
        Batch::new(schema, builders.into_iter().map(ColumnBuilder::finish).collect())
    }

    pub fn row(&self, i: usize) -> Vec<Datum> {
        self.columns.iter().map(|c| c.datum(i)).collect()
    }

    pub fn to_rows(&self) -> Vec<Vec<Datum>> {
        (0..self.num_rows()).map(|i| self.row(i)).collect()
    }

    pub fn take(&self, indices: &[u32]) -> Batch {
        Batch { schema: self.schema.clone(), columns: self.columns.iter().map(|c| c.take(indices)).collect() }
    }

    /// The given columns, as a batch with the matching sub-schema.
    pub fn project(&self, columns: &[usize]) -> Batch {
        let schema = Schema::new(columns.iter().map(|&i| self.schema.columns[i].clone()).collect());
        Batch { schema: Arc::new(schema), columns: columns.iter().map(|&i| self.columns[i].clone()).collect() }
    }

    /// Concatenates batches of the same schema.
    pub fn concat(schema: Arc<Schema>, batches: &[Batch]) -> Batch {
        let mut out = Batch::empty(schema);
        for b in batches {
            for (o, c) in out.columns.iter_mut().zip(&b.columns) {
                o.append(c);
            }
        }
        out
    }
}
