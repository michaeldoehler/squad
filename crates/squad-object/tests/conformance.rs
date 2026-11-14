//! One behavioural test suite, run against every store.

use std::sync::Arc;

use std::time::Duration;

use squad_object::emulator::S3Emulator;
use squad_object::{open_url, CachedStore, Error, LocalStore, MemoryStore, ObjectStore, S3Config, S3Store};

fn conformance(store: &dyn ObjectStore) {
    conformance_with(store, true)
}

/// The suite. `conditional` says whether the store is expected to
/// enforce `put_if_absent`; some S3-compatible services do not.
fn conformance_with(store: &dyn ObjectStore, conditional: bool) {
    // Missing objects.
    assert_eq!(store.get("nope").unwrap(), None);
    assert_eq!(store.head("nope").unwrap(), None);
    assert!(store.get_range("nope", 0, 1).is_err());
    store.delete("nope").unwrap();
    assert!(store.list("").unwrap().is_empty());

    // Put, replace, read back.
    store.put("a/one", b"hello").unwrap();
    store.put("a/two", b"").unwrap();
    store.put("b/deep/er/three", &[7u8; 100_000]).unwrap();
    store.put("a/one", b"hello world").unwrap();
    assert_eq!(store.get("a/one").unwrap().as_deref(), Some(&b"hello world"[..]));
    assert_eq!(store.get("a/two").unwrap().as_deref(), Some(&b""[..]));
    assert_eq!(store.head("a/one").unwrap().unwrap().size, 11);
    assert_eq!(store.head("b/deep/er/three").unwrap().unwrap().size, 100_000);
    // A directory-like prefix is not an object.
    assert_eq!(store.head("a").unwrap(), None);
    assert_eq!(store.get("b/deep").unwrap(), None);

    // Ranges.
    assert_eq!(store.get_range("a/one", 0, 5).unwrap(), b"hello");
    assert_eq!(store.get_range("a/one", 6, 5).unwrap(), b"world");
    assert_eq!(store.get_range("a/one", 11, 0).unwrap(), b"");
    assert_eq!(store.get_range("b/deep/er/three", 99_990, 10).unwrap(), [7u8; 10]);
    assert!(store.get_range("a/one", 6, 6).is_err(), "range beyond the end must fail");
    assert!(store.get_range("a/one", 12, 1).is_err());

    // Listing: sorted, by string prefix.
    let keys = |prefix: &str| store.list(prefix).unwrap().into_iter().map(|m| m.key).collect::<Vec<_>>();
    assert_eq!(keys(""), ["a/one", "a/two", "b/deep/er/three"]);
    assert_eq!(keys("a/"), ["a/one", "a/two"]);
    assert_eq!(keys("a/t"), ["a/two"]);
    assert_eq!(keys("b/deep/"), ["b/deep/er/three"]);
    assert!(keys("c").is_empty());
    assert_eq!(store.list("a/one").unwrap()[0].size, 11);
    let after = |prefix: &str, after: &str| {
        store.list_after(prefix, after).unwrap().into_iter().map(|m| m.key).collect::<Vec<_>>()
    };
    assert_eq!(after("", ""), ["a/one", "a/two", "b/deep/er/three"]);
    assert_eq!(after("a/", "a/one"), ["a/two"]);
    assert_eq!(after("a/", "a/o"), ["a/one", "a/two"]);
    assert_eq!(after("", "a/two"), ["b/deep/er/three"]);
    assert_eq!(after("b/", "a/two"), ["b/deep/er/three"]);
    assert!(after("a/", "a/two").is_empty());
    assert!(after("a/", "b").is_empty());
    assert_eq!(store.list_after("a/", "a/o").unwrap(), store.list("a/").unwrap());

    // Conditional creation.
    assert!(store.put_if_absent("lock/1", b"mine").unwrap());
    if conditional {
        assert!(!store.put_if_absent("lock/1", b"yours").unwrap());
        assert_eq!(store.get("lock/1").unwrap().as_deref(), Some(&b"mine"[..]));
    }

    // Keys with characters that need escaping in URLs and XML.
    let odd = "odd/sp ace+plus&amp=eq/ü~é<x>";
    store.put(odd, b"odd").unwrap();
    assert_eq!(store.get(odd).unwrap().as_deref(), Some(&b"odd"[..]));
    assert_eq!(keys("odd/"), [odd]);
    store.delete(odd).unwrap();

    // Many objects: listing must page through all of them.
    for i in 0..57 {
        store.put(&format!("many/{i:03}"), format!("{i}").as_bytes()).unwrap();
    }
    let many = keys("many/");
    assert_eq!(many.len(), 57);
    assert!(many.windows(2).all(|w| w[0] < w[1]));

    // Delete.
    store.delete("a/one").unwrap();
    store.delete("a/one").unwrap();
    assert_eq!(store.get("a/one").unwrap(), None);
    assert_eq!(keys("a/"), ["a/two"]);

    // Malformed keys are rejected.
    for bad in ["", "/abs", "trail/", "a//b", "a/../b", "./a"] {
        assert!(matches!(store.put(bad, b"x"), Err(Error::Invalid(_))), "{bad:?}");
    }
}

#[test]
fn memory_store() {
    conformance(&MemoryStore::new());
}

#[test]
fn local_store() {
    let dir = tempfile::tempdir().unwrap();
    conformance(&LocalStore::new(dir.path().join("objects")).unwrap());
    // No temporary files are left behind or listed.
    let store = LocalStore::new(dir.path().join("objects")).unwrap();
    assert!(store.list("").unwrap().iter().all(|m| !m.key.contains(".tmp-")));
}

#[test]
fn local_store_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    LocalStore::new(dir.path()).unwrap().put("x/y", b"z").unwrap();
    assert_eq!(LocalStore::new(dir.path()).unwrap().get("x/y").unwrap().as_deref(), Some(&b"z"[..]));
}

#[test]
fn s3_store() {
    let emulator = S3Emulator::start();
    emulator.set_page_size(10);
    conformance(&S3Store::new(emulator.config("")).unwrap());
    assert_eq!(emulator.rejected(), 0, "every request must carry a valid signature");
    // With a key prefix, objects land under it and listings hide it.
    let prefixed = S3Store::new(emulator.config("tenant/a")).unwrap();
    conformance(&prefixed);
    prefixed.put("k", b"v").unwrap();
    assert!(emulator.keys().contains(&"tenant/a/k".to_string()));
    assert_eq!(emulator.rejected(), 0);
}

#[test]
fn s3_rejects_wrong_credentials() {
    let emulator = S3Emulator::start();
    let mut config = emulator.config("");
    config.secret_key.push('x');
    let store = S3Store::new(config).unwrap();
    match store.put("k", b"v") {
        Err(Error::Remote { status: 403, .. }) => {}
        other => panic!("expected a signature error, got {other:?}"),
    }
    assert_eq!(emulator.rejected(), 1);
    assert!(emulator.keys().is_empty());
}

#[test]
fn s3_retries_transient_failures() {
    let emulator = S3Emulator::start();
    let store = S3Store::new(emulator.config("")).unwrap();
    emulator.fail_next(2);
    store.put("k", b"v").unwrap();
    emulator.drop_next(2);
    assert_eq!(store.get("k").unwrap().as_deref(), Some(&b"v"[..]));
    emulator.fail_next(1);
    emulator.drop_next(1);
    assert_eq!(store.list("").unwrap().len(), 1);

    // A conditional put whose confirmation is lost is still reported as
    // created when the retry finds its own object.
    emulator.drop_after_next(1);
    assert!(store.put_if_absent("once", b"data").unwrap());
    assert!(!store.put_if_absent("once", b"other").unwrap());

    // Persistent failure surfaces as a transport error.
    let impatient = S3Store::new(emulator.config("")).unwrap().with_retries(2);
    emulator.fail_next(10);
    assert!(matches!(impatient.put("k2", b"v"), Err(Error::Transport(_))));
    emulator.fail_next(0);
    assert_eq!(store.get("k2").unwrap(), None);
}

#[test]
fn cached_store() {
    let inner = MemoryStore::new();
    let cached = CachedStore::new(inner.clone(), 1 << 20);
    conformance(&cached);

    cached.put("c/1", &[1u8; 1000]).unwrap();
    let before = inner.requests();
    for _ in 0..10 {
        assert_eq!(cached.get("c/1").unwrap().unwrap().len(), 1000);
        assert_eq!(cached.get_range("c/1", 10, 5).unwrap(), [1u8; 5]);
    }
    assert_eq!(inner.requests(), before, "cached reads must not reach the store");
    // Replacing and deleting invalidate.
    cached.put("c/1", b"new").unwrap();
    assert_eq!(cached.get("c/1").unwrap().as_deref(), Some(&b"new"[..]));
    cached.delete("c/1").unwrap();
    assert_eq!(cached.get("c/1").unwrap(), None);
    // The cache stays within its budget.
    let small = CachedStore::new(MemoryStore::new(), 4096);
    for i in 0..100 {
        small.put(&format!("k{i}"), &[0u8; 400]).unwrap();
        small.get(&format!("k{i}")).unwrap();
    }
    let (hits, _) = small.stats();
    assert!(hits >= 100);
}

#[test]
fn urls() {
    let dir = tempfile::tempdir().unwrap();
    let file = open_url(&format!("file://{}", dir.path().display())).unwrap();
    file.put("k", b"v").unwrap();
    assert!(dir.path().join("k").exists());

    let memory = open_url("memory://").unwrap();
    memory.put("k", b"v").unwrap();

    let emulator = S3Emulator::start();
    let s3: Arc<dyn ObjectStore> = open_url(&emulator.url("pre/fix")).unwrap();
    s3.put("k", b"v").unwrap();
    assert_eq!(emulator.keys(), ["pre/fix/k"]);
    assert_eq!(emulator.rejected(), 0);

    assert!(open_url("ftp://x").is_err());
    assert!(open_url("nonsense").is_err());
    assert!(open_url("s3://bucket?bogus=1&access_key=a&secret_key=b").is_err());
}

fn big(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8).collect()
}

#[test]
fn s3_multipart_uploads() {
    let emulator = S3Emulator::start();
    let store = S3Store::new(emulator.config("mp")).unwrap().with_multipart(6 << 20, 5 << 20);
    let data = big(13 << 20);
    store.put("large", &data).unwrap();
    assert_eq!(emulator.completed_multipart_uploads(), 1);
    assert_eq!(store.stats().multipart_uploads, 1);
    assert_eq!(store.get("large").unwrap().as_deref(), Some(&data[..]));
    assert_eq!(store.head("large").unwrap().unwrap().size, data.len() as u64);
    // Below the threshold: one PUT.
    store.put("small", &data[..6 << 20]).unwrap();
    assert_eq!(emulator.completed_multipart_uploads(), 1);
    // Transient failures in the middle of an upload are retried.
    emulator.fail_next(2);
    store.put("large2", &data).unwrap();
    assert_eq!(store.get("large2").unwrap().as_deref(), Some(&data[..]));
    // A lost confirmation of the completion is retried and recognised.
    emulator.drop_after_completions(1);
    store.put("large3", &data).unwrap();
    assert_eq!(store.get("large3").unwrap().as_deref(), Some(&data[..]));
    assert_eq!(emulator.open_multipart_uploads(), 0);
    assert_eq!(emulator.rejected(), 0);
}

#[test]
fn s3_permanent_errors_are_not_retried() {
    let emulator = S3Emulator::start();
    let mut config = emulator.config("");
    config.secret_key.push('x');
    let store = S3Store::new(config).unwrap();
    assert!(matches!(store.get("k"), Err(Error::Remote { status: 403, .. })));
    assert_eq!(store.stats(), squad_object::S3Stats { requests: 1, retries: 0, multipart_uploads: 0 });

    // A port nobody listens on: connection errors are transient.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = closed.local_addr().unwrap();
    drop(closed);
    let config = S3Config { endpoint: Some(format!("http://{addr}")), ..emulator.config("") };
    let store = S3Store::new(config)
        .unwrap()
        .with_retries(3)
        .with_backoff(Duration::from_millis(1), Duration::from_millis(2))
        .with_timeouts(Duration::from_millis(500), Duration::from_millis(500));
    assert!(matches!(store.get("k"), Err(Error::Transport(_))));
    assert_eq!(store.stats().retries, 2);
}

/// The URL of the real S3 service to test against, with a key prefix of
/// its own, or `None` (and a visible note) if none is configured.
fn real_s3(what: &str) -> Option<String> {
    let Ok(url) = std::env::var("SQUAD_TEST_S3_URL") else {
        // Written to the handle directly: the test harness captures
        // `eprintln!`, and a skipped test should be noticed.
        use std::io::Write;
        let _ = writeln!(std::io::stderr(), "skipped {what}: SQUAD_TEST_S3_URL not set");
        return None;
    };
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let prefix = format!("squad-object-tests/{what}-{}-{nanos:x}", std::process::id());
    let (location, query) = url.split_once('?').unwrap_or((&url, ""));
    let joined = format!("{}/{prefix}", location.trim_end_matches('/'));
    Some(if query.is_empty() { joined } else { format!("{joined}?{query}") })
}

/// Removes everything a test left under its prefix.
fn clean(store: &dyn ObjectStore) {
    for meta in store.list("").unwrap() {
        store.delete(&meta.key).unwrap();
    }
}

#[test]
fn real_s3_conformance() {
    let Some(url) = real_s3("conformance") else { return };
    let store = S3Store::new(S3Config::from_url(&url).unwrap()).unwrap();
    // Whether the service enforces conditional puts decides what the
    // suite may expect; squad's fencing does not depend on it.
    let conditional = {
        let first = store.put_if_absent("probe", b"1").unwrap();
        let second = store.put_if_absent("probe", b"2").unwrap();
        store.delete("probe").unwrap();
        assert!(first);
        !second
    };
    eprintln!("real S3: conditional writes enforced: {conditional}");
    conformance_with(&store, conditional);
    clean(&store);

    // Multipart upload with the smallest parts S3 accepts.
    let store = S3Store::new(S3Config::from_url(&url).unwrap()).unwrap().with_multipart(5 << 20, 5 << 20);
    let data = big(11 << 20);
    store.put("multipart/object", &data).unwrap();
    assert_eq!(store.stats().multipart_uploads, 1);
    assert_eq!(store.get("multipart/object").unwrap().as_deref(), Some(&data[..]));
    assert_eq!(store.get_range("multipart/object", (5 << 20) - 3, 6).unwrap(), &data[(5 << 20) - 3..(5 << 20) + 3]);
    clean(&store);
    assert!(store.list("").unwrap().is_empty());
}
