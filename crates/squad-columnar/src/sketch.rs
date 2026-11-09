//! Mergeable sketches for approximate aggregates.

use crate::util::FastSet;

const HLL_P: u32 = 12;
const HLL_M: usize = 1 << HLL_P;
/// Below this many distinct hashes the set is kept exactly.
const HLL_SPARSE_LIMIT: usize = 256;

/// HyperLogLog distinct counter with 4096 registers (standard error about
/// 1.6 %). Small sets are counted exactly.
#[derive(Debug, Clone)]
pub enum Hll {
    Sparse(FastSet<u64>),
    Dense(Box<[u8; HLL_M]>),
}

impl Default for Hll {
    fn default() -> Self {
        Hll::Sparse(FastSet::default())
    }
}

fn register(dense: &mut [u8; HLL_M], hash: u64) {
    let idx = (hash >> (64 - HLL_P)) as usize;
    let rest = hash << HLL_P;
    let rank = (rest.leading_zeros() + 1).min(64 - HLL_P + 1) as u8;
    if rank > dense[idx] {
        dense[idx] = rank;
    }
}

impl Hll {
    /// Adds a value by its 64-bit hash.
    #[inline]
    pub fn add_hash(&mut self, hash: u64) {
        match self {
            Hll::Sparse(set) => {
                set.insert(hash);
                if set.len() > HLL_SPARSE_LIMIT {
                    let mut dense = Box::new([0u8; HLL_M]);
                    set.iter().for_each(|h| register(&mut dense, *h));
                    *self = Hll::Dense(dense);
                }
            }
            Hll::Dense(d) => register(d, hash),
        }
    }

    pub fn merge(&mut self, other: &Hll) {
        match other {
            Hll::Sparse(set) => set.iter().for_each(|h| self.add_hash(*h)),
            Hll::Dense(o) => {
                if let Hll::Sparse(set) = self {
                    let mut dense = Box::new([0u8; HLL_M]);
                    set.iter().for_each(|h| register(&mut dense, *h));
                    *self = Hll::Dense(dense);
                }
                let Hll::Dense(d) = self else { unreachable!() };
                for (a, b) in d.iter_mut().zip(o.iter()) {
                    *a = (*a).max(*b);
                }
            }
        }
    }

    /// Estimated number of distinct values added.
    pub fn estimate(&self) -> u64 {
        match self {
            Hll::Sparse(set) => set.len() as u64,
            Hll::Dense(d) => {
                let m = HLL_M as f64;
                let sum: f64 = d.iter().map(|r| 2f64.powi(-(*r as i32))).sum();
                let alpha = 0.7213 / (1.0 + 1.079 / m);
                let raw = alpha * m * m / sum;
                let zeros = d.iter().filter(|r| **r == 0).count();
                // Linear counting is more accurate while registers are
                // still empty.
                if raw <= 2.5 * m && zeros > 0 {
                    (m * (m / zeros as f64).ln()).round() as u64
                } else {
                    raw.round() as u64
                }
            }
        }
    }
}

/// Relative accuracy of quantile estimates.
const DD_ALPHA: f64 = 0.01;
/// Values closer to zero than this count as zero.
const DD_MIN: f64 = 1e-300;

#[derive(Debug, Clone, Default)]
struct Store {
    offset: i32,
    counts: Vec<u64>,
}

impl Store {
    fn add(&mut self, index: i32, n: u64) {
        if self.counts.is_empty() {
            self.offset = index;
            self.counts.push(n);
            return;
        }
        if index < self.offset {
            let grow = (self.offset - index) as usize;
            let mut counts = vec![0; grow + self.counts.len()];
            counts[grow..].copy_from_slice(&self.counts);
            self.counts = counts;
            self.offset = index;
        }
        let pos = (index - self.offset) as usize;
        if pos >= self.counts.len() {
            self.counts.resize(pos + 1, 0);
        }
        self.counts[pos] += n;
    }

    fn merge(&mut self, other: &Store) {
        for (i, c) in other.counts.iter().enumerate() {
            if *c > 0 {
                self.add(other.offset + i as i32, *c);
            }
        }
    }
}

/// DDSketch: quantiles with a relative error of at most 1 %, mergeable
/// without loss.
#[derive(Debug, Clone, Default)]
pub struct QuantileSketch {
    positive: Store,
    negative: Store,
    zeros: u64,
    nans: u64,
    count: u64,
    min: f64,
    max: f64,
}

fn gamma() -> f64 {
    (1.0 + DD_ALPHA) / (1.0 - DD_ALPHA)
}

fn index_of(v: f64) -> i32 {
    (v.ln() / gamma().ln()).ceil() as i32
}

fn value_of(index: i32) -> f64 {
    2.0 * gamma().powi(index) / (gamma() + 1.0)
}

/// Bucket indices of the integers below 65 536, which covers typical
/// durations in milliseconds without computing a logarithm per value.
fn small_int_index(v: i64) -> Option<i32> {
    static TABLE: std::sync::OnceLock<Vec<i32>> = std::sync::OnceLock::new();
    if !(1..65_536).contains(&v) {
        return None;
    }
    Some(TABLE.get_or_init(|| (0..65_536).map(|i| if i == 0 { 0 } else { index_of(i as f64) }).collect())[v as usize])
}

impl QuantileSketch {
    /// Adds an integer.
    #[inline]
    pub fn add_int(&mut self, v: i64) {
        match small_int_index(v) {
            Some(index) => {
                self.positive.add(index, 1);
                let f = v as f64;
                if self.count == self.nans {
                    self.min = f;
                    self.max = f;
                } else {
                    self.min = self.min.min(f);
                    self.max = self.max.max(f);
                }
                self.count += 1;
            }
            None => self.add(v as f64),
        }
    }

    pub fn add(&mut self, v: f64) {
        if v.is_nan() {
            // NaN sorts above everything, as in PostgreSQL.
            self.nans += 1;
        } else if v.abs() < DD_MIN {
            self.zeros += 1;
        } else if v > 0.0 {
            self.positive.add(index_of(v.min(f64::MAX)), 1);
        } else {
            self.negative.add(index_of((-v).min(f64::MAX)), 1);
        }
        if !v.is_nan() {
            if self.count == self.nans {
                self.min = v;
                self.max = v;
            } else {
                self.min = self.min.min(v);
                self.max = self.max.max(v);
            }
        }
        self.count += 1;
    }

    pub fn merge(&mut self, other: &QuantileSketch) {
        if other.count == 0 {
            return;
        }
        let (mine, theirs) = (self.count - self.nans, other.count - other.nans);
        if theirs > 0 {
            if mine == 0 {
                self.min = other.min;
                self.max = other.max;
            } else {
                self.min = self.min.min(other.min);
                self.max = self.max.max(other.max);
            }
        }
        self.positive.merge(&other.positive);
        self.negative.merge(&other.negative);
        self.zeros += other.zeros;
        self.nans += other.nans;
        self.count += other.count;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    /// The value at quantile `q` (0 to 1), or `None` for an empty sketch.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let rank = (q.clamp(0.0, 1.0) * (self.count - 1) as f64).round() as u64;
        let mut seen = 0u64;
        // Most negative first: large indices of the negative store.
        for (i, c) in self.negative.counts.iter().enumerate().rev() {
            seen += c;
            if *c > 0 && seen > rank {
                return Some((-value_of(self.negative.offset + i as i32)).clamp(self.min, self.max));
            }
        }
        seen += self.zeros;
        if self.zeros > 0 && seen > rank {
            return Some(0f64.clamp(self.min, self.max));
        }
        for (i, c) in self.positive.counts.iter().enumerate() {
            seen += c;
            if *c > 0 && seen > rank {
                return Some(value_of(self.positive.offset + i as i32).clamp(self.min, self.max));
            }
        }
        Some(f64::NAN)
    }
}
