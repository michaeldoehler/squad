use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use squad_kv::{Isolation, KvError, LockMode, Store, WaitPolicy};

const U: Bound<&[u8]> = Bound::Unbounded;

fn all(tx: &squad_kv::Txn) -> Vec<(Vec<u8>, Vec<u8>)> {
    tx.scan(U, U, false, usize::MAX).unwrap()
}

#[test]
fn snapshot_isolation_basics() {
    let store = Store::in_memory();
    let mut a = store.begin(Isolation::RepeatableRead);
    a.put(b"k1", b"v1").unwrap();
    assert_eq!(a.get(b"k1").unwrap().as_deref(), Some(&b"v1"[..]));
    let b = store.begin(Isolation::RepeatableRead);
    assert_eq!(b.get(b"k1").unwrap(), None);
    a.commit().unwrap();
    assert_eq!(b.get(b"k1").unwrap(), None, "old snapshot must not see later commit");
    let c = store.begin(Isolation::RepeatableRead);
    assert_eq!(c.get(b"k1").unwrap().as_deref(), Some(&b"v1"[..]));

    let mut d = store.begin(Isolation::ReadCommitted);
    d.delete(b"k1").unwrap();
    assert_eq!(d.get(b"k1").unwrap(), None);
    assert!(all(&d).is_empty());
    d.commit().unwrap();
    assert_eq!(c.get(b"k1").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(all(&c).len(), 1);
    assert!(all(&store.begin(Isolation::ReadCommitted)).is_empty());
}

#[test]
fn read_committed_refresh() {
    let store = Store::in_memory();
    let mut rc = store.begin(Isolation::ReadCommitted);
    let mut w = store.begin(Isolation::ReadCommitted);
    w.put(b"a", b"1").unwrap();
    w.commit().unwrap();
    assert_eq!(rc.get(b"a").unwrap(), None);
    rc.refresh_snapshot();
    assert_eq!(rc.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
}

#[test]
fn first_committer_wins() {
    let store = Store::in_memory();
    let mut a = store.begin(Isolation::RepeatableRead);
    let mut b = store.begin(Isolation::RepeatableRead);
    a.put(b"k", b"a").unwrap();
    b.put(b"k", b"b").unwrap();
    a.commit().unwrap();
    assert!(matches!(b.commit(), Err(KvError::SerializationFailure)));
    let r = store.begin(Isolation::ReadCommitted);
    assert_eq!(r.get(b"k").unwrap().as_deref(), Some(&b"a"[..]));
}

#[test]
fn savepoints() {
    let store = Store::in_memory();
    let mut t = store.begin(Isolation::ReadCommitted);
    t.put(b"a", b"1").unwrap();
    let sp = t.savepoint();
    t.put(b"a", b"2").unwrap();
    t.put(b"b", b"2").unwrap();
    let sp2 = t.savepoint();
    t.delete(b"a").unwrap();
    t.rollback_to(sp2);
    assert_eq!(t.get(b"a").unwrap().as_deref(), Some(&b"2"[..]));
    t.rollback_to(sp);
    assert_eq!(t.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(t.get(b"b").unwrap(), None);
    t.commit().unwrap();
    assert_eq!(all(&store.begin(Isolation::ReadCommitted)).len(), 1);
}

#[test]
fn write_skew_is_caught_only_by_serializable() {
    for (iso, expect_fail) in [(Isolation::RepeatableRead, false), (Isolation::Serializable, true)] {
        let store = Store::in_memory();
        let mut init = store.begin(Isolation::ReadCommitted);
        init.put(b"doctor/1", b"on").unwrap();
        init.put(b"doctor/2", b"on").unwrap();
        init.commit().unwrap();
        let mut a = store.begin(iso);
        let mut b = store.begin(iso);
        let hi = b"doctor0".to_vec();
        assert_eq!(a.scan(Bound::Included(b"doctor/"), Bound::Excluded(&hi), false, 10).unwrap().len(), 2);
        assert_eq!(b.scan(Bound::Included(b"doctor/"), Bound::Excluded(&hi), false, 10).unwrap().len(), 2);
        a.put(b"doctor/1", b"off").unwrap();
        b.put(b"doctor/2", b"off").unwrap();
        a.commit().unwrap();
        assert_eq!(b.commit().is_err(), expect_fail, "{iso:?}");
    }
}

#[test]
fn locks_block_and_release() {
    let store = Store::in_memory();
    let mut a = store.begin(Isolation::ReadCommitted);
    assert!(a.lock(b"row", LockMode::Exclusive, WaitPolicy::Wait(None)).unwrap());
    let mut b = store.begin(Isolation::ReadCommitted);
    assert!(matches!(b.lock(b"row", LockMode::Exclusive, WaitPolicy::NoWait), Err(KvError::LockNotAvailable)));
    assert!(!b.lock(b"row", LockMode::Shared, WaitPolicy::Skip).unwrap());
    assert!(matches!(
        b.lock(b"row", LockMode::Exclusive, WaitPolicy::Wait(Some(Duration::from_millis(20)))),
        Err(KvError::LockTimeout)
    ));
    let (tx, rx) = mpsc::channel();
    let h = thread::spawn(move || {
        b.lock(b"row", LockMode::Exclusive, WaitPolicy::Wait(None)).unwrap();
        tx.send(()).unwrap();
    });
    assert!(rx.recv_timeout(Duration::from_millis(50)).is_err(), "must still be blocked");
    a.commit().unwrap();
    rx.recv_timeout(Duration::from_secs(5)).expect("lock handed over");
    h.join().unwrap();

    let mut s1 = store.begin(Isolation::ReadCommitted);
    let mut s2 = store.begin(Isolation::ReadCommitted);
    assert!(s1.lock(b"s", LockMode::Shared, WaitPolicy::NoWait).unwrap());
    assert!(s2.lock(b"s", LockMode::Shared, WaitPolicy::NoWait).unwrap());
    assert!(s1.lock(b"s", LockMode::Exclusive, WaitPolicy::NoWait).is_err());
    drop(s2);
    assert!(s1.lock(b"s", LockMode::Exclusive, WaitPolicy::NoWait).unwrap(), "upgrade as sole holder");
}

#[test]
fn deadlock_is_detected() {
    let store = Store::in_memory();
    let barrier = Arc::new(Barrier::new(2));
    let run = |first: &'static [u8], second: &'static [u8]| {
        let store = store.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut t = store.begin(Isolation::ReadCommitted);
            t.lock(first, LockMode::Exclusive, WaitPolicy::Wait(None)).unwrap();
            barrier.wait();
            let r = t.lock(second, LockMode::Exclusive, WaitPolicy::Wait(None));
            if r.is_ok() {
                // Hold on briefly so the other side is certainly waiting.
                thread::sleep(Duration::from_millis(50));
            }
            r.is_err()
        })
    };
    let a = run(b"x", b"y");
    let b = run(b"y", b"x");
    let (fa, fb) = (a.join().unwrap(), b.join().unwrap());
    assert!(fa ^ fb, "exactly one victim, got {fa} {fb}");
}

#[test]
fn gc_removes_dead_versions() {
    let store = Store::in_memory();
    for i in 0..10u8 {
        let mut t = store.begin(Isolation::ReadCommitted);
        t.put(b"k", &[i]).unwrap();
        t.put(b"gone", &[i]).unwrap();
        t.commit().unwrap();
    }
    let mut t = store.begin(Isolation::ReadCommitted);
    t.delete(b"gone").unwrap();
    t.commit().unwrap();
    let old = store.begin(Isolation::RepeatableRead);
    let mut t = store.begin(Isolation::ReadCommitted);
    t.put(b"k", b"new").unwrap();
    t.commit().unwrap();
    store.gc().unwrap();
    assert_eq!(old.get(b"k").unwrap(), Some(vec![9]));
    assert_eq!(old.get(b"gone").unwrap(), None);
    drop(old);
    store.gc().unwrap();
    let raw = store.engine().scan(Bound::Included(b"\x01"), Bound::Excluded(b"\x02"), false, 100).unwrap();
    assert_eq!(raw.len(), 1, "only the live version of k remains");
    assert_eq!(store.begin(Isolation::ReadCommitted).get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
}

/// A transaction with its expected view and its pending writes.
type OpenTxn = (squad_kv::Txn, BTreeMap<Vec<u8>, Vec<u8>>, BTreeMap<Vec<u8>, Option<Vec<u8>>>);

/// Random single-threaded interleaving of transactions, checked against a
/// model that applies each transaction's writes at commit.
#[test]
fn model_check() {
    for seed in 0..40u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let store = Store::in_memory();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        // (txn, view = snapshot of model at begin + own writes)
        let mut open: Vec<OpenTxn> = Vec::new();
        let key = |rng: &mut StdRng| -> Vec<u8> {
            let n = rng.gen_range(0..3);
            (0..n).map(|_| rng.gen_range(0..3u8)).collect()
        };
        for _ in 0..600 {
            match rng.gen_range(0..10) {
                0 if open.len() < 4 => {
                    open.push((store.begin(Isolation::RepeatableRead), model.clone(), BTreeMap::new()))
                }
                1 | 2 if !open.is_empty() => {
                    let i = rng.gen_range(0..open.len());
                    let (tx, _, writes) = open.swap_remove(i);
                    if rng.gen_bool(0.8) {
                        // Blind writes always commit: last committer wins.
                        tx.commit().unwrap();
                        for (k, v) in writes {
                            match v {
                                Some(v) => model.insert(k, v),
                                None => model.remove(&k),
                            };
                        }
                    }
                    if rng.gen_bool(0.1) {
                        store.gc().unwrap();
                    }
                }
                3..=5 if !open.is_empty() => {
                    let i = rng.gen_range(0..open.len());
                    let (tx, view, writes) = &mut open[i];
                    let k = key(&mut rng);
                    if rng.gen_bool(0.7) {
                        let v = vec![rng.gen()];
                        tx.put_blind(&k, &v).unwrap();
                        view.insert(k.clone(), v.clone());
                        writes.insert(k, Some(v));
                    } else {
                        tx.delete_blind(&k).unwrap();
                        view.remove(&k);
                        writes.insert(k, None);
                    }
                }
                _ if !open.is_empty() => {
                    let i = rng.gen_range(0..open.len());
                    let (tx, view, _) = &open[i];
                    let k = key(&mut rng);
                    assert_eq!(tx.get(&k).unwrap(), view.get(&k).cloned(), "seed {seed}");
                    let (a, b) = (key(&mut rng), key(&mut rng));
                    let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
                    let reverse = rng.gen_bool(0.5);
                    let limit = rng.gen_range(1..8);
                    let lo_b = if rng.gen_bool(0.5) { Bound::Included(&lo[..]) } else { Bound::Excluded(&lo[..]) };
                    let hi_b = match rng.gen_range(0..3) {
                        0 => Bound::Included(&hi[..]),
                        1 => Bound::Excluded(&hi[..]),
                        _ => Bound::Unbounded,
                    };
                    let got = tx.scan(lo_b, hi_b, reverse, limit).unwrap();
                    let inside = |k: &Vec<u8>| {
                        (match lo_b {
                            Bound::Included(l) => &k[..] >= l,
                            Bound::Excluded(l) => &k[..] > l,
                            Bound::Unbounded => true,
                        }) && (match hi_b {
                            Bound::Included(h) => &k[..] <= h,
                            Bound::Excluded(h) => &k[..] < h,
                            Bound::Unbounded => true,
                        })
                    };
                    let mut want: Vec<_> =
                        view.iter().filter(|(k, _)| inside(k)).map(|(k, v)| (k.clone(), v.clone())).collect();
                    if reverse {
                        want.reverse();
                    }
                    want.truncate(limit);
                    assert_eq!(got, want, "seed {seed} lo {lo_b:?} hi {hi_b:?} rev {reverse} limit {limit}");
                }
                _ => {}
            }
        }
    }
}

#[test]
fn concurrent_counter_with_locks() {
    let store = Store::in_memory();
    let mut t = store.begin(Isolation::ReadCommitted);
    t.put(b"n", &0u64.to_be_bytes()).unwrap();
    t.commit().unwrap();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            thread::spawn(move || {
                for _ in 0..200 {
                    let mut t = store.begin(Isolation::ReadCommitted);
                    t.lock(b"n", LockMode::Exclusive, WaitPolicy::Wait(None)).unwrap();
                    let (v, _) = t.get_latest(b"n").unwrap().unwrap();
                    let n = u64::from_be_bytes(v.unwrap().try_into().unwrap());
                    t.put_blind(b"n", &(n + 1).to_be_bytes()).unwrap();
                    t.commit().unwrap();
                }
            })
        })
        .collect();
    for h in threads {
        h.join().unwrap();
    }
    let v = store.begin(Isolation::ReadCommitted).get(b"n").unwrap().unwrap();
    assert_eq!(u64::from_be_bytes(v.try_into().unwrap()), 1600);
}

/// Readers must always find a complete, consistent snapshot while writers
/// replace every value and garbage collection removes old versions.
#[test]
fn snapshots_survive_concurrent_gc() {
    use std::sync::atomic::{AtomicBool, Ordering};
    const KEYS: u64 = 40;
    let store = Store::in_memory();
    let mut t = store.begin(Isolation::ReadCommitted);
    for k in 0..KEYS {
        t.put(&k.to_be_bytes(), &100u64.to_be_bytes()).unwrap();
    }
    t.commit().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (store, stop) = (store.clone(), stop.clone());
        thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                // Move one unit between two keys: the total stays put.
                let (a, b) = (i % KEYS, (i * 7 + 3) % KEYS);
                if a == b {
                    continue;
                }
                let mut t = store.begin(Isolation::RepeatableRead);
                let read = |t: &squad_kv::Txn, k: u64| {
                    u64::from_be_bytes(t.get(&k.to_be_bytes()).unwrap().unwrap().try_into().unwrap())
                };
                let (va, vb) = (read(&t, a), read(&t, b));
                if va == 0 {
                    continue;
                }
                t.put_blind(&a.to_be_bytes(), &(va - 1).to_be_bytes()).unwrap();
                t.put_blind(&b.to_be_bytes(), &(vb + 1).to_be_bytes()).unwrap();
                t.commit().unwrap();
                if i.is_multiple_of(16) {
                    store.gc().unwrap();
                }
            }
        })
    };
    let readers: Vec<_> = (0..4)
        .map(|r| {
            let (store, stop) = (store.clone(), stop.clone());
            thread::spawn(move || {
                let mut rounds = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let mut t =
                        store.begin(if r % 2 == 0 { Isolation::RepeatableRead } else { Isolation::ReadCommitted });
                    for _ in 0..3 {
                        let rows = t.scan(U, U, r % 3 == 0, usize::MAX).unwrap();
                        assert_eq!(rows.len() as u64, KEYS, "a key vanished from a snapshot");
                        let total: u64 =
                            rows.iter().map(|(_, v)| u64::from_be_bytes(v.as_slice().try_into().unwrap())).sum();
                        assert_eq!(total, KEYS * 100, "snapshot is not consistent");
                        // Point reads agree with the scan under Repeatable Read.
                        if r % 2 == 0 {
                            let (k, v) = &rows[(rounds % KEYS) as usize];
                            assert_eq!(t.get(k).unwrap().as_ref(), Some(v));
                        }
                        t.refresh_snapshot();
                        rounds += 1;
                    }
                }
                rounds
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(1500));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
}
