//! Model-based tests: every encoding, predicate and aggregate is compared
//! with a naive row-at-a-time implementation on random data, across
//! several segments plus a delta batch, with and without parallelism.

// Three-valued logic folds over `Option<bool>` without short-circuiting
// on unknown, which `try_fold` would do.
#![allow(clippy::manual_try_fold)]

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use squad_columnar::*;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

const I: usize = 0;
const F: usize = 1;
const D: usize = 2;
const S: usize = 3;
const B: usize = 4;
const T: usize = 5;
const U: usize = 6;
const DAY: i64 = 86_400_000_000;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("i", ColumnType::Int64, true),
        Field::new("f", ColumnType::Float64, true),
        Field::new("d", ColumnType::Decimal { scale: 2 }, true),
        Field::new("s", ColumnType::Utf8, true),
        Field::new("b", ColumnType::Bool, true),
        Field::new("t", ColumnType::Int64, false),
        Field::new("u", ColumnType::Utf8, true),
    ]))
}

fn random_row(rng: &mut Rng, n: usize) -> Vec<Datum> {
    let null = |rng: &mut Rng, d: Datum| if rng.chance(8) { Datum::Null } else { d };
    let i = match rng.below(20) {
        0 => i64::MAX,
        1 => i64::MIN,
        2 => 0,
        _ => rng.below(50) as i64 - 10,
    };
    let f = match rng.below(25) {
        0 => f64::NAN,
        1 => f64::INFINITY,
        2 => -0.0,
        _ => (rng.below(200) as f64 - 100.0) / 2.0,
    };
    let d = match rng.below(30) {
        0 => i128::MAX / 4,
        1 => i128::MIN / 4,
        _ => rng.below(100_000) as i128 - 50_000,
    };
    let s = ["alpha", "beta", "gamma", "", "δelta", "al%pha", "a_b"][rng.below(7) as usize];
    let t = 946_684_800_000_000 + n as i64 * 3_600_000_000 / 7 + rng.below(1_000_000) as i64
        - 400 * DAY * rng.below(2) as i64;
    let b = rng.chance(50);
    let u = format!("user-{}", rng.below(5000));
    vec![
        null(rng, Datum::Int(i)),
        null(rng, Datum::Float(f)),
        null(rng, Datum::Decimal(d)),
        null(rng, Datum::text(s)),
        null(rng, Datum::Bool(b)),
        Datum::Int(t),
        null(rng, Datum::text(u)),
    ]
}

/// Builds a table of `segments` segments and one delta batch from `rows`.
fn table(rows: &[Vec<Datum>], rng: &mut Rng) -> TableSource {
    let schema = schema();
    let mut source = TableSource::new(schema.clone());
    let parts = 3;
    let per = rows.len() / (parts + 1) + 1;
    for (p, chunk) in rows.chunks(per).enumerate() {
        let batch = Batch::from_rows(schema.clone(), chunk).unwrap();
        if p == parts {
            source.delta.push(batch);
            continue;
        }
        let options = WriterOptions {
            row_group_rows: [7, 97, 1000][rng.below(3) as usize],
            sort_key: [vec![], vec![T], vec![S, I]][rng.below(3) as usize].clone(),
            compress: rng.chance(50),
        };
        let mut w = SegmentWriter::new(schema.clone(), options);
        // Feed the writer in pieces to exercise concatenation.
        for piece in chunk.chunks(chunk.len() / 3 + 1) {
            w.write_batch(&Batch::from_rows(schema.clone(), piece).unwrap()).unwrap();
        }
        let bytes = w.finish().unwrap();
        source.segments.push(Arc::new(Segment::open(Arc::new(bytes)).unwrap()));
    }
    source
}

fn same(a: &Datum, b: &Datum) -> bool {
    match (a, b) {
        (Datum::Float(x), Datum::Float(y)) => (x.is_nan() && y.is_nan()) || x == y,
        _ => a == b,
    }
}

fn sorted(mut rows: Vec<Vec<Datum>>) -> Vec<Vec<Datum>> {
    rows.sort_by(|a, b| {
        a.iter().zip(b).map(|(x, y)| cmp_datum(x, y)).find(|o| o.is_ne()).unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

fn assert_rows(got: Vec<Vec<Datum>>, want: Vec<Vec<Datum>>, ctx: &str) {
    let (got, want) = (sorted(got), sorted(want));
    assert_eq!(got.len(), want.len(), "{ctx}: row count; got {got:?}\nwant {want:?}");
    for (g, w) in got.iter().zip(&want) {
        assert!(g.len() == w.len() && g.iter().zip(w).all(|(x, y)| same(x, y)), "{ctx}:\n got {g:?}\nwant {w:?}");
    }
}

#[test]
fn segments_round_trip() {
    for seed in 1..40u64 {
        let mut rng = Rng(seed * 7919);
        let n = [0, 1, 5, 300, 2500][rng.below(5) as usize];
        let rows: Vec<_> = (0..n).map(|k| random_row(&mut rng, k)).collect();
        let source = table(&rows, &mut rng);
        let mut got = Vec::new();
        for seg in &source.segments {
            // Unsorted segments keep row order; all keep the multiset.
            let back = seg.read_all().unwrap();
            assert_eq!(back.num_rows() as u64, seg.num_rows());
            got.extend(back.to_rows());
        }
        got.extend(source.delta.iter().flat_map(Batch::to_rows));
        assert_rows(got, rows, &format!("seed {seed}"));
    }
}

#[test]
fn integer_encodings_cover_their_cases() {
    let cases: Vec<Vec<i64>> = vec![
        vec![],
        vec![42],
        vec![7; 1000],
        (0..1000).collect(),
        (0..1000).map(|i| 1_700_000_000_000_000 + i * 1_000_003).collect(),
        (0..1000).map(|i| (i * 7919) % 13).collect(),
        vec![i64::MIN, i64::MAX, 0, -1, 1],
        (0..1000).map(|i| if i % 100 == 0 { i64::MAX } else { i }).collect(),
        (0..600).map(|i| i / 100).collect(),
    ];
    let schema = Arc::new(Schema::new(vec![Field::new("v", ColumnType::Int64, false)]));
    let mut sizes = Vec::new();
    for case in cases {
        let rows: Vec<_> = case.iter().map(|v| vec![Datum::Int(*v)]).collect();
        let mut w = SegmentWriter::new(schema.clone(), WriterOptions { compress: false, ..Default::default() });
        w.write_batch(&Batch::from_rows(schema.clone(), &rows).unwrap()).unwrap();
        let seg = Segment::open(Arc::new(w.finish().unwrap())).unwrap();
        sizes.push(if rows.is_empty() { 0 } else { seg.row_group(0).columns[0].bytes() });
        assert_eq!(seg.read_all().unwrap().to_rows(), rows);
    }
    // Constant, sequential and small-range data must be far below 8 bytes
    // per value.
    assert!(sizes[2] < 40 && sizes[3] < 40 && sizes[4] < 40, "{sizes:?}");
    assert!(sizes[5] < 600 && sizes[8] < 100, "{sizes:?}");
}

fn cmp_ref(v: &Datum, op: CmpOp, c: &Datum) -> Option<bool> {
    if v.is_null() || c.is_null() {
        return None;
    }
    let o = cmp_datum(v, c);
    Some(match op {
        CmpOp::Eq => o.is_eq(),
        CmpOp::Ne => o.is_ne(),
        CmpOp::Lt => o.is_lt(),
        CmpOp::Le => o.is_le(),
        CmpOp::Gt => o.is_gt(),
        CmpOp::Ge => o.is_ge(),
    })
}

fn like_ref(text: &str, pattern: &str) -> bool {
    // Straightforward recursive matcher over characters.
    fn go(t: &[char], p: &[char]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some((&'%', rest)) => (0..=t.len()).any(|i| go(&t[i..], rest)),
            Some((&'_', rest)) => !t.is_empty() && go(&t[1..], rest),
            Some((&'\\', rest)) if !rest.is_empty() => !t.is_empty() && t[0] == rest[0] && go(&t[1..], &rest[1..]),
            Some((c, rest)) => !t.is_empty() && t[0] == *c && go(&t[1..], rest),
        }
    }
    go(&text.chars().collect::<Vec<_>>(), &pattern.chars().collect::<Vec<_>>())
}

fn and3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn eval_ref(p: &Predicate, row: &[Datum]) -> Option<bool> {
    match p {
        Predicate::Const(c) => *c,
        Predicate::Cmp { column, op, value } => cmp_ref(&row[*column], *op, value),
        Predicate::Between { column, low, high } => {
            and3(cmp_ref(&row[*column], CmpOp::Ge, low), cmp_ref(&row[*column], CmpOp::Le, high))
        }
        Predicate::In { column, values } => {
            let mut out = Some(false);
            for v in values {
                out = eval_ref(
                    &Predicate::Not(Box::new(Predicate::Const(and3(
                        out.map(|b| !b),
                        cmp_ref(&row[*column], CmpOp::Eq, v).map(|b| !b),
                    )))),
                    row,
                );
            }
            if row[*column].is_null() && !values.is_empty() {
                None
            } else {
                out
            }
        }
        Predicate::IsNull { column, negated } => Some(row[*column].is_null() != *negated),
        Predicate::Like { column, pattern, case_insensitive } => row[*column].as_str().map(|t| {
            if *case_insensitive {
                like_ref(&t.to_lowercase(), &pattern.to_lowercase())
            } else {
                like_ref(t, pattern)
            }
        }),
        Predicate::BoolColumn(c) => match &row[*c] {
            Datum::Bool(b) => Some(*b),
            _ => None,
        },
        Predicate::And(ps) => ps.iter().fold(Some(true), |acc, p| and3(acc, eval_ref(p, row))),
        Predicate::Or(ps) => {
            ps.iter().fold(Some(false), |acc, p| and3(acc.map(|b| !b), eval_ref(p, row).map(|b| !b)).map(|b| !b))
        }
        Predicate::Not(p) => eval_ref(p, row).map(|b| !b),
    }
}

fn random_predicate(rng: &mut Rng, rows: &[Vec<Datum>], depth: u32) -> Predicate {
    let sample = |rng: &mut Rng, col: usize| -> Datum {
        if rows.is_empty() || rng.chance(5) {
            return Datum::Null;
        }
        rows[rng.below(rows.len() as u64) as usize][col].clone()
    };
    let col = [I, F, D, S, T, U][rng.below(6) as usize];
    let op = [CmpOp::Eq, CmpOp::Ne, CmpOp::Lt, CmpOp::Le, CmpOp::Gt, CmpOp::Ge][rng.below(6) as usize];
    match rng.below(if depth == 0 { 7 } else { 10 }) {
        0 | 1 => Predicate::Cmp { column: col, op, value: sample(rng, col) },
        2 => Predicate::In { column: col, values: (0..rng.below(4)).map(|_| sample(rng, col)).collect() },
        3 => Predicate::Between { column: col, low: sample(rng, col), high: sample(rng, col) },
        4 => Predicate::IsNull { column: [I, F, D, S, B, U][rng.below(6) as usize], negated: rng.chance(50) },
        5 => Predicate::Like {
            column: [S, U][rng.below(2) as usize],
            pattern: ["al%", "%a", "%et%", "a_b", "user-1%", "%", "", "al\\%pha", "_e%a", "ALPHA", "δ%"]
                [rng.below(11) as usize]
                .to_string(),
            case_insensitive: rng.chance(30),
        },
        6 => {
            if rng.chance(70) {
                Predicate::BoolColumn(B)
            } else {
                Predicate::Const([Some(true), Some(false), None][rng.below(3) as usize])
            }
        }
        7 => Predicate::And((0..rng.below(3) + 1).map(|_| random_predicate(rng, rows, depth - 1)).collect()),
        8 => Predicate::Or((0..rng.below(3) + 1).map(|_| random_predicate(rng, rows, depth - 1)).collect()),
        _ => Predicate::Not(Box::new(random_predicate(rng, rows, depth - 1))),
    }
}

#[test]
fn scans_match_the_reference() {
    for seed in 1..60u64 {
        let mut rng = Rng(seed * 104_729);
        let n = [0, 3, 400, 3000][rng.below(4) as usize];
        let rows: Vec<_> = (0..n).map(|k| random_row(&mut rng, k)).collect();
        let source = table(&rows, &mut rng);
        for q in 0..25 {
            let predicate = random_predicate(&mut rng, &rows, 2);
            let projection: Vec<usize> =
                [vec![I, S], vec![U], vec![T, F, D, B], vec![S, I, U, T]][rng.below(4) as usize].clone();
            let request =
                ScanRequest { projection: projection.clone(), predicate: Some(predicate.clone()), limit: None };
            let got: Vec<Vec<Datum>> = scan(&source, &request).unwrap().flat_map(|b| b.unwrap().to_rows()).collect();
            let want: Vec<Vec<Datum>> = rows
                .iter()
                .filter(|r| eval_ref(&predicate, r) == Some(true))
                .map(|r| projection.iter().map(|c| r[*c].clone()).collect())
                .collect();
            assert_rows(got, want.clone(), &format!("seed {seed} query {q} {predicate:?}"));

            // A limit returns that many of the matching rows.
            let limit = rng.below(20) as usize;
            let limited = ScanRequest { projection, predicate: Some(predicate), limit: Some(limit) };
            let count: usize = scan(&source, &limited).unwrap().map(|b| b.unwrap().num_rows()).sum();
            assert_eq!(count, limit.min(want.len()), "seed {seed} query {q} limit");
        }
    }
}

fn bucket_ref(t: i64, unit: TimeUnit, offset: i64, epoch: i64) -> i64 {
    floor_time(t + epoch + offset, unit) - epoch - offset
}

fn key_ref(key: &GroupKey, row: &[Datum]) -> Datum {
    match key {
        GroupKey::Column(c) => match &row[*c] {
            // -0.0 and 0.0 are one group.
            Datum::Float(f) if *f == 0.0 => Datum::Float(0.0),
            d => d.clone(),
        },
        GroupKey::TimeBucket { column, unit, utc_offset_micros, epoch_unix_micros } => match &row[*column] {
            Datum::Int(t) => Datum::Int(bucket_ref(*t, *unit, *utc_offset_micros, *epoch_unix_micros)),
            _ => Datum::Null,
        },
    }
}

fn agg_ref(agg: &Aggregate, rows: &[&Vec<Datum>]) -> Datum {
    let vals = |c: usize| rows.iter().map(move |r| &r[c]).filter(|d| !d.is_null());
    let key = |d: &Datum| match d {
        Datum::Float(f) if f.is_nan() => "nan".to_string(),
        Datum::Float(f) if *f == 0.0 => "0".to_string(),
        d => format!("{d:?}"),
    };
    match agg {
        Aggregate::CountStar => Datum::Int(rows.len() as i64),
        Aggregate::Count(c) => Datum::Int(vals(*c).count() as i64),
        Aggregate::CountDistinct(c) | Aggregate::ApproxCountDistinct(c) => {
            Datum::Int(vals(*c).map(key).collect::<HashSet<_>>().len() as i64)
        }
        Aggregate::Sum(c) => {
            let mut any = false;
            let (mut wide, mut float, mut is_float) = (0i128, 0f64, false);
            for v in vals(*c) {
                any = true;
                match v {
                    Datum::Int(i) => wide += *i as i128,
                    Datum::Decimal(d) => wide += d,
                    Datum::Float(f) => {
                        float += f;
                        is_float = true;
                    }
                    _ => unreachable!(),
                }
            }
            match (any, is_float) {
                (false, _) => Datum::Null,
                (true, true) => Datum::Float(float),
                (true, false) => Datum::Decimal(wide),
            }
        }
        Aggregate::Avg(c) => {
            let mut n = 0u64;
            let (mut wide, mut float, mut scale) = (0i128, 0f64, 1.0);
            let mut is_float = false;
            for v in vals(*c) {
                n += 1;
                match v {
                    Datum::Int(i) => wide += *i as i128,
                    Datum::Decimal(d) => {
                        wide += d;
                        scale = 100.0;
                    }
                    Datum::Float(f) => {
                        float += f;
                        is_float = true;
                    }
                    _ => unreachable!(),
                }
            }
            match (n, is_float) {
                (0, _) => Datum::Null,
                (_, true) => Datum::Float(float / n as f64),
                _ => Datum::Float(wide as f64 / scale / n as f64),
            }
        }
        Aggregate::Min(c) => vals(*c).min_by(|a, b| cmp_datum(a, b)).cloned().unwrap_or(Datum::Null),
        Aggregate::Max(c) => vals(*c).max_by(|a, b| cmp_datum(a, b)).cloned().unwrap_or(Datum::Null),
        Aggregate::ApproxPercentile { .. } => unreachable!(),
    }
}

fn close(a: &Datum, b: &Datum) -> bool {
    match (a, b) {
        (Datum::Float(x), Datum::Float(y)) => {
            (x.is_nan() && y.is_nan()) || x == y || (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0)
        }
        _ => a == b,
    }
}

#[test]
fn aggregates_match_the_reference() {
    for seed in 1..60u64 {
        let mut rng = Rng(seed * 15_485_863);
        let n = [0, 2, 500, 3000][rng.below(4) as usize];
        let mut rows: Vec<_> = (0..n).map(|k| random_row(&mut rng, k)).collect();
        // Decimal sums must not overflow in the reference.
        for r in &mut rows {
            if let Datum::Decimal(d) = &mut r[D] {
                *d = (*d).clamp(-1_000_000_000_000, 1_000_000_000_000);
            }
        }
        let source = table(&rows, &mut rng);
        for q in 0..20 {
            let predicate = if rng.chance(60) { Some(random_predicate(&mut rng, &rows, 1)) } else { None };
            let units = [
                TimeUnit::Second,
                TimeUnit::Minute,
                TimeUnit::Hour,
                TimeUnit::Day,
                TimeUnit::Week,
                TimeUnit::Month,
                TimeUnit::Quarter,
                TimeUnit::Year,
            ];
            let pick_key = |rng: &mut Rng| match rng.below(8) {
                0 => GroupKey::Column(S),
                1 => GroupKey::Column(I),
                2 => GroupKey::Column(B),
                3 => GroupKey::Column(F),
                4 => GroupKey::Column(D),
                5 => GroupKey::Column(U),
                _ => GroupKey::TimeBucket {
                    column: T,
                    unit: units[rng.below(8) as usize],
                    utc_offset_micros: [0, 3_600_000_000, -5 * 3_600_000_000][rng.below(3) as usize],
                    epoch_unix_micros: [0, 946_684_800_000_000][rng.below(2) as usize],
                },
            };
            let group_by: Vec<GroupKey> = (0..rng.below(4)).map(|_| pick_key(&mut rng)).collect();
            let all = [
                Aggregate::CountStar,
                Aggregate::Count(I),
                Aggregate::Count(U),
                Aggregate::CountDistinct(I),
                Aggregate::CountDistinct(S),
                Aggregate::CountDistinct(F),
                Aggregate::CountDistinct(U),
                Aggregate::Sum(D),
                Aggregate::Avg(D),
                Aggregate::Min(I),
                Aggregate::Max(I),
                Aggregate::Min(F),
                Aggregate::Max(F),
                Aggregate::Min(S),
                Aggregate::Max(U),
                Aggregate::Min(D),
                Aggregate::Max(B),
                Aggregate::Min(T),
                Aggregate::Sum(T),
                Aggregate::Avg(T),
            ];
            let aggregates: Vec<Aggregate> =
                (0..rng.below(5) + 1).map(|_| all[rng.below(all.len() as u64) as usize].clone()).collect();
            let request = AggregateRequest {
                predicate: predicate.clone(),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                parallelism: [0, 1, 4][rng.below(3) as usize],
                ..Default::default()
            };
            let got = aggregate(&source, &request).unwrap().to_rows();

            let mut groups: BTreeMap<String, (Vec<Datum>, Vec<&Vec<Datum>>)> = BTreeMap::new();
            if group_by.is_empty() {
                groups.insert(String::new(), (Vec::new(), Vec::new()));
            }
            for r in rows.iter().filter(|r| predicate.as_ref().is_none_or(|p| eval_ref(p, r) == Some(true))) {
                let key: Vec<Datum> = group_by.iter().map(|k| key_ref(k, r)).collect();
                let name = key
                    .iter()
                    .map(|d| match d {
                        Datum::Float(f) if f.is_nan() => "nan".to_string(),
                        d => format!("{d:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                groups.entry(name).or_insert_with(|| (key, Vec::new())).1.push(r);
            }
            let want: Vec<Vec<Datum>> = groups
                .values()
                .map(|(key, members)| {
                    key.iter().cloned().chain(aggregates.iter().map(|a| agg_ref(a, members))).collect()
                })
                .collect();
            let ctx = format!("seed {seed} query {q}: {request:?}");
            let (got, want) = (sorted(got), sorted(want));
            assert_eq!(got.len(), want.len(), "{ctx}: number of groups\n got {got:?}\nwant {want:?}");
            for (g, w) in got.iter().zip(&want) {
                assert!(
                    g.len() == w.len() && g.iter().zip(w).all(|(x, y)| close(x, y)),
                    "{ctx}:\n got {g:?}\nwant {w:?}"
                );
            }
        }
    }
}

#[test]
fn sum_of_integers_does_not_overflow_and_decimals_report_overflow() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", ColumnType::Int64, false),
        Field::new("d", ColumnType::Decimal { scale: 0 }, false),
    ]));
    let rows = vec![vec![Datum::Int(i64::MAX), Datum::Decimal(i128::MAX)]; 3];
    let mut source = TableSource::new(schema.clone());
    source.delta.push(Batch::from_rows(schema, &rows).unwrap());
    let sum = |agg| aggregate(&source, &AggregateRequest { aggregates: vec![agg], ..Default::default() });
    assert_eq!(sum(Aggregate::Sum(0)).unwrap().row(0), vec![Datum::Decimal(3 * i64::MAX as i128)]);
    assert!(sum(Aggregate::Sum(1)).is_err());
}

#[test]
fn having_order_and_limit() {
    let mut rng = Rng(99);
    let rows: Vec<_> = (0..2000).map(|k| random_row(&mut rng, k)).collect();
    let source = table(&rows, &mut rng);
    let request = AggregateRequest {
        group_by: vec![GroupKey::Column(S)],
        aggregates: vec![Aggregate::CountStar],
        having: Some(Predicate::Cmp { column: 1, op: CmpOp::Gt, value: Datum::Int(10) }),
        order_by: vec![OrderBy { column: 1, descending: true }, OrderBy { column: 0, descending: false }],
        limit: Some(3),
        parallelism: 3,
        ..Default::default()
    };
    let got = aggregate(&source, &request).unwrap().to_rows();
    let mut counts: BTreeMap<String, (Datum, i64)> = BTreeMap::new();
    for r in &rows {
        counts.entry(format!("{:?}", r[S])).or_insert((r[S].clone(), 0)).1 += 1;
    }
    let mut want: Vec<(Datum, i64)> = counts.into_values().filter(|(_, c)| *c > 10).collect();
    want.sort_by(|a, b| b.1.cmp(&a.1).then(cmp_datum(&a.0, &b.0)));
    want.truncate(3);
    assert_eq!(got, want.into_iter().map(|(k, c)| vec![k, Datum::Int(c)]).collect::<Vec<_>>());
}

#[test]
fn approximate_aggregates_are_within_their_error_bounds() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", ColumnType::Int64, false),
        Field::new("user", ColumnType::Utf8, false),
        Field::new("ms", ColumnType::Float64, false),
        Field::new("n", ColumnType::Int64, false),
    ]));
    let mut rng = Rng(4242);
    let rows: Vec<Vec<Datum>> = (0..300_000)
        .map(|i| {
            let g = (i % 3) as i64;
            // Group 0 has few users, group 2 many; durations are skewed.
            let users = [100, 20_000, 150_000][g as usize];
            let ms = (rng.below(1_000_000) as f64 / 1_000_000.0).powi(4) * 60_000.0 + 1.0;
            vec![
                Datum::Int(g),
                Datum::text(format!("u{}", rng.below(users))),
                Datum::Float(ms),
                Datum::Int(rng.below(1000) as i64 - 500),
            ]
        })
        .collect();
    let mut source = TableSource::new(schema.clone());
    for chunk in rows.chunks(70_000) {
        let mut w = SegmentWriter::new(schema.clone(), WriterOptions { row_group_rows: 10_000, ..Default::default() });
        w.write_batch(&Batch::from_rows(schema.clone(), chunk).unwrap()).unwrap();
        source.segments.push(Arc::new(Segment::open(Arc::new(w.finish().unwrap())).unwrap()));
    }
    for parallelism in [1, 4] {
        let result = aggregate(
            &source,
            &AggregateRequest {
                group_by: vec![GroupKey::Column(0)],
                aggregates: vec![
                    Aggregate::CountDistinct(1),
                    Aggregate::ApproxCountDistinct(1),
                    Aggregate::ApproxPercentile { column: 2, percentile: 0.5 },
                    Aggregate::ApproxPercentile { column: 2, percentile: 0.99 },
                    Aggregate::ApproxPercentile { column: 3, percentile: 0.1 },
                    Aggregate::ApproxPercentile { column: 3, percentile: 0.0 },
                    Aggregate::ApproxPercentile { column: 3, percentile: 1.0 },
                ],
                order_by: vec![OrderBy { column: 0, descending: false }],
                parallelism,
                ..Default::default()
            },
        )
        .unwrap()
        .to_rows();
        for (g, row) in result.iter().enumerate() {
            let members: Vec<&Vec<Datum>> = rows.iter().filter(|r| r[0] == Datum::Int(g as i64)).collect();
            let exact = row[1].as_i64().unwrap() as f64;
            let approx = row[2].as_i64().unwrap() as f64;
            assert!((approx - exact).abs() / exact < 0.06, "distinct estimate {approx} vs {exact}");
            let quantile = |col: usize, q: f64| {
                let mut v: Vec<f64> = members.iter().map(|r| r[col].as_f64().unwrap()).collect();
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                v[((v.len() - 1) as f64 * q).round() as usize]
            };
            for (i, (col, q)) in [(2, 0.5), (2, 0.99), (3, 0.1), (3, 0.0), (3, 1.0)].into_iter().enumerate() {
                let (got, want) = (row[3 + i].as_f64().unwrap(), quantile(col, q));
                // 1 % relative error of the sketch plus rank rounding.
                assert!(
                    (got - want).abs() <= 0.025 * want.abs() + 1.0,
                    "quantile {q} of column {col}: {got} vs {want}"
                );
            }
        }
    }
}

#[test]
fn pruning_skips_data_without_changing_results() {
    // Sorted by time: a narrow time range must touch few row groups.
    struct Counting {
        data: Vec<u8>,
        reads: std::sync::atomic::AtomicUsize,
    }
    impl ChunkSource for Counting {
        fn len(&self) -> u64 {
            self.data.len() as u64
        }
        fn read(&self, offset: u64, len: u64) -> std::io::Result<Vec<u8>> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.data.read(offset, len)
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("t", ColumnType::Int64, false),
        Field::new("user", ColumnType::Utf8, false),
        Field::new("v", ColumnType::Int64, false),
    ]));
    let rows: Vec<Vec<Datum>> = (0..100_000)
        .map(|i| vec![Datum::Int(i), Datum::text(format!("user-{}", i % 30_000)), Datum::Int(i % 7)])
        .collect();
    let mut w =
        SegmentWriter::new(schema.clone(), WriterOptions { row_group_rows: 1000, sort_key: vec![0], compress: true });
    w.write_batch(&Batch::from_rows(schema.clone(), &rows).unwrap()).unwrap();
    let counting = Arc::new(Counting { data: w.finish().unwrap(), reads: Default::default() });
    let mut source = TableSource::new(schema);
    source.segments.push(Arc::new(Segment::open(counting.clone()).unwrap()));
    let reads = || counting.reads.swap(0, std::sync::atomic::Ordering::Relaxed);
    reads();

    let count = |predicate: Predicate| -> i64 {
        let r = aggregate(
            &source,
            &AggregateRequest {
                predicate: Some(predicate),
                aggregates: vec![Aggregate::CountStar],
                ..Default::default()
            },
        )
        .unwrap();
        r.row(0)[0].as_i64().unwrap()
    };
    // Zone maps: 2500 rows out of 100 000 lie in three row groups.
    assert_eq!(count(Predicate::Between { column: 0, low: Datum::Int(50_000), high: Datum::Int(52_499) }), 2500);
    assert!(reads() <= 3, "a time range must read only the row groups it overlaps");
    // Bloom filters: one user occurs in a handful of row groups.
    assert_eq!(count(Predicate::Cmp { column: 1, op: CmpOp::Eq, value: Datum::text("user-12345") }), 3);
    let touched = reads();
    assert!(touched < 40, "a point lookup read {touched} chunks; bloom filters should rule most row groups out");
    // A value that does not exist reads no data chunk at all.
    assert_eq!(count(Predicate::Cmp { column: 0, op: CmpOp::Eq, value: Datum::Int(-5) }), 0);
    assert_eq!(reads(), 0);
}

#[test]
fn manifest_compaction_and_retention() {
    let mut rng = Rng(7);
    let schema = schema();
    let rows: Vec<_> = (0..3000).map(|k| random_row(&mut rng, k)).collect();
    let mut manifest = Manifest::new((*schema).clone(), vec![T]);
    let mut segments = Vec::new();
    for (i, chunk) in rows.chunks(500).enumerate() {
        let mut w = SegmentWriter::new(schema.clone(), WriterOptions { sort_key: vec![T], ..Default::default() });
        w.write_batch(&Batch::from_rows(schema.clone(), chunk).unwrap()).unwrap();
        let seg = Arc::new(Segment::open(Arc::new(w.finish().unwrap())).unwrap());
        manifest.segments.push(SegmentMeta::describe(format!("seg-{i}"), &seg));
        segments.push(seg);
    }
    assert_eq!(manifest.num_rows(), 3000);
    // Compared in serialized form: statistics may hold NaN.
    let back = Manifest::from_bytes(&manifest.to_bytes()).unwrap();
    assert_eq!(back.to_bytes(), manifest.to_bytes());
    assert_eq!((back.segments.len(), back.num_rows(), &back.sort_key), (6, 3000, &vec![T]));
    let mut damaged = manifest.to_bytes();
    damaged[20] ^= 1;
    assert!(Manifest::from_bytes(&damaged).is_err());

    // Retention: exactly the segments whose newest row is older than the cutoff.
    let cutoff = rows.iter().map(|r| r[T].as_i64().unwrap()).max().unwrap() - 1;
    let expired = manifest.expired(T, &Datum::Int(cutoff));
    for meta in &manifest.segments {
        let idx: usize = meta.id[4..].parse().unwrap();
        let newest = rows.chunks(500).nth(idx).unwrap().iter().map(|r| r[T].as_i64().unwrap()).max().unwrap();
        assert_eq!(expired.iter().any(|e| e.id == meta.id), newest < cutoff);
    }

    // Compaction: one sorted segment with all rows.
    let candidates = manifest.compaction_candidates(10_000, 2);
    assert_eq!(candidates.len(), 6);
    let merged =
        Segment::open(Arc::new(compact(&segments, WriterOptions { sort_key: vec![T], ..Default::default() }).unwrap()))
            .unwrap();
    let all = merged.read_all().unwrap().to_rows();
    assert!(all.windows(2).all(|w| cmp_datum(&w[0][T], &w[1][T]).is_le()), "compacted segment is sorted");
    assert_rows(all, rows, "compaction");
}

#[test]
fn corruption_is_detected_and_never_panics() {
    let mut rng = Rng(31337);
    let schema = schema();
    let rows: Vec<_> = (0..1200).map(|k| random_row(&mut rng, k)).collect();
    let mut w = SegmentWriter::new(schema.clone(), WriterOptions { row_group_rows: 300, ..Default::default() });
    w.write_batch(&Batch::from_rows(schema.clone(), &rows).unwrap()).unwrap();
    let good = w.finish().unwrap();
    let expected = Segment::open(Arc::new(good.clone())).unwrap().read_all().unwrap();

    let read_everything = |bytes: Vec<u8>| -> Result<Batch> {
        let seg = Segment::open(Arc::new(bytes))?;
        for g in 0..seg.num_row_groups() {
            for c in 0..seg.schema().len() {
                seg.bloom(g, c)?;
            }
        }
        let mut source = TableSource::new(seg.schema().clone());
        let seg = Arc::new(seg);
        source.segments.push(seg.clone());
        aggregate(
            &source,
            &AggregateRequest {
                group_by: vec![GroupKey::Column(S)],
                aggregates: vec![Aggregate::Sum(I)],
                ..Default::default()
            },
        )?;
        seg.read_all()
    };
    let mut detected = 0;
    for round in 0..3000 {
        let mut bad = good.clone();
        match rng.below(10) {
            0 => bad.truncate(rng.below(good.len() as u64) as usize),
            1 => {
                let at = rng.below(good.len() as u64) as usize;
                bad.splice(at..at, [rng.next() as u8; 3]);
            }
            2 => {
                // Damage several bytes at once.
                for _ in 0..4 {
                    let at = rng.below(good.len() as u64) as usize;
                    bad[at] = rng.next() as u8;
                }
            }
            _ => {
                let at = rng.below(good.len() as u64) as usize;
                bad[at] ^= 1 << rng.below(8);
            }
        }
        if bad == good {
            continue;
        }
        match read_everything(bad) {
            Err(_) => detected += 1,
            // Damage outside every checksummed region (the leading magic)
            // must leave the data intact.
            Ok(batch) => assert_eq!(
                batch.num_rows(),
                expected.num_rows(),
                "round {round}: damaged segment read as different data"
            ),
        }
    }
    assert!(detected > 2900, "only {detected} of 3000 corruptions were detected");
}
