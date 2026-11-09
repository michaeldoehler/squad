#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ops::Bound;

use squad_storage::{Engine, WriteBatch};

pub type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// splitmix64 – small, fast and deterministic.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo)
    }
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// A key from a keyspace of `space` distinct keys. A few keys are padded up
/// to the maximum key length.
pub fn gen_key(rng: &mut Rng, space: u64) -> Vec<u8> {
    let id = rng.below(space);
    let mut k = format!("k{id:06}").into_bytes();
    match id % 53 {
        0 => k.resize(squad_storage::MAX_KEY_LEN, b'x'),
        1 => k.resize(700 + (id % 600) as usize, b'y'),
        _ => {}
    }
    k
}

pub fn gen_value(rng: &mut Rng) -> Vec<u8> {
    let len = match rng.below(100) {
        0..=69 => rng.below(120),
        70..=89 => rng.range(120, 2100),
        90..=97 => rng.range(2100, 12_000),
        _ => rng.range(12_000, 60_000),
    } as usize;
    let seed = rng.next();
    (0..len).map(|i| (seed.wrapping_mul(i as u64 + 1) >> 24) as u8).collect()
}

pub fn gen_batch(rng: &mut Rng, space: u64, max_ops: u64) -> WriteBatch {
    let mut b = WriteBatch::new();
    for _ in 0..rng.range(1, max_ops + 1) {
        let k = gen_key(rng, space);
        if rng.chance(30) {
            b.delete(k);
        } else {
            b.put(k, gen_value(rng));
        }
    }
    b
}

pub fn apply(model: &mut Model, batch: &WriteBatch) {
    for (k, v) in &batch.ops {
        match v {
            Some(v) => {
                model.insert(k.clone(), v.clone());
            }
            None => {
                model.remove(k);
            }
        }
    }
}

pub fn dump(engine: &dyn Engine) -> Vec<(Vec<u8>, Vec<u8>)> {
    engine.scan(Bound::Unbounded, Bound::Unbounded, false, usize::MAX).unwrap()
}

pub fn assert_same(engine: &dyn Engine, model: &Model, ctx: &str) {
    let got = dump(engine);
    let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    if got != want {
        let first = got.iter().zip(&want).position(|(a, b)| a != b).unwrap_or(got.len().min(want.len()));
        panic!(
            "{ctx}: engine and model differ: {} vs {} entries, first difference at index {first}",
            got.len(),
            want.len()
        );
    }
}

fn gen_bound(rng: &mut Rng, space: u64) -> Bound<Vec<u8>> {
    match rng.below(5) {
        0 => Bound::Unbounded,
        1 | 2 => Bound::Included(gen_key(rng, space)),
        _ => Bound::Excluded(gen_key(rng, space)),
    }
}

fn as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Compares one random point lookup and one random range scan.
pub fn check_reads(rng: &mut Rng, engine: &dyn Engine, model: &Model, space: u64) {
    let k = gen_key(rng, space);
    assert_eq!(engine.get(&k).unwrap().as_ref(), model.get(&k), "get mismatch");

    let (lo, hi) = (gen_bound(rng, space), gen_bound(rng, space));
    let reverse = rng.chance(50);
    let limit = if rng.chance(20) { usize::MAX } else { rng.below(40) as usize };
    let got = engine.scan(as_ref(&lo), as_ref(&hi), reverse, limit).unwrap();
    let in_range = |k: &Vec<u8>| {
        (match &lo {
            Bound::Included(l) => k >= l,
            Bound::Excluded(l) => k > l,
            Bound::Unbounded => true,
        }) && (match &hi {
            Bound::Included(h) => k <= h,
            Bound::Excluded(h) => k < h,
            Bound::Unbounded => true,
        })
    };
    let all = model.iter().filter(|(k, _)| in_range(k)).map(|(k, v)| (k.clone(), v.clone()));
    let want: Vec<_> = if reverse { all.rev().take(limit).collect() } else { all.take(limit).collect() };
    assert_eq!(got.len(), want.len(), "scan length mismatch (reverse={reverse}, limit={limit})");
    assert!(got == want, "scan content mismatch (reverse={reverse}, limit={limit})");
}

pub fn assert_verify_clean(engine: &dyn Engine, ctx: &str) {
    let report = engine.verify().unwrap();
    assert!(report.is_ok(), "{ctx}: verify reported {:?}", report.errors);
}
