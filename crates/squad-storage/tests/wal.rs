//! The write-ahead log on the simulated file system: damage in the
//! middle of the log is refused, a torn tail is not, and a write batch
//! too large for one log record is an error.

use std::path::Path;
use std::sync::Arc;

use squad_object::MemoryStore;
use squad_storage::{
    DiskEngine, DiskOptions, Engine, RemoteConfig, RemoteSync, SimVfs, StorageError, SyncMode, TieredEngine, Vfs,
    WriteBatch,
};

/// Bytes of the segment header and of one record written by `fill`.
const SEG_HEADER: u64 = 16;
const RECORD: u64 = 24 + 4 + (1 + 4 + 5) + (4 + 100);

fn options(vfs: &SimVfs) -> DiskOptions {
    DiskOptions {
        sync_mode: SyncMode::Sync,
        cache_pages: 1024,
        // No checkpoint while the test writes: the log keeps everything.
        checkpoint_wal_bytes: 1 << 30,
        vfs: Arc::new(vfs.clone()),
    }
}

fn batch(i: u64) -> WriteBatch {
    let mut b = WriteBatch::new();
    b.put(format!("k{i:04}").into_bytes(), vec![i as u8; 100]);
    b
}

/// Writes `n` records, each made durable on its own, and dies without a
/// clean shutdown. Returns the path of the log segment.
fn fill(vfs: &SimVfs, n: u64) -> std::path::PathBuf {
    let engine = DiskEngine::open(Path::new("/db"), options(vfs)).unwrap();
    for i in 1..=n {
        let lsn = engine.write(batch(i)).unwrap();
        assert_eq!(lsn, i);
        engine.sync(lsn).unwrap();
    }
    std::mem::forget(engine);
    vfs.crash();
    let segments = vfs.list(Path::new("/db/wal")).unwrap();
    assert_eq!(segments.len(), 1, "{segments:?}");
    Path::new("/db/wal").join(&segments[0])
}

fn keys(engine: &dyn Engine) -> usize {
    engine.scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, false, usize::MAX).unwrap().len()
}

#[test]
fn damage_in_the_middle_of_the_log_is_refused() {
    let vfs = SimVfs::new(1);
    let segment = fill(&vfs, 50);
    // A bit flips in record 20, which was synced long before 21..50.
    assert!(vfs.corrupt_byte(&segment, SEG_HEADER + 19 * RECORD + 60));
    let err = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap_err();
    assert!(matches!(err, StorageError::Corruption(_)), "{err}");
    assert!(err.to_string().contains("LSN 20"), "{err}");
    // Nothing was thrown away: the same open fails again.
    assert!(vfs.exists(&segment).unwrap());
    let err = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap_err();
    assert!(matches!(err, StorageError::Corruption(_)), "{err}");
}

#[test]
fn damage_to_a_record_header_is_refused() {
    let vfs = SimVfs::new(2);
    let segment = fill(&vfs, 30);
    // The length field of record 10: the record boundaries after it are
    // lost and have to be found again.
    assert!(vfs.corrupt_byte(&segment, SEG_HEADER + 9 * RECORD + 1));
    let err = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap_err();
    assert!(err.to_string().contains("LSN 10"), "{err}");
}

#[test]
fn a_torn_last_record_is_tolerated() {
    let vfs = SimVfs::new(3);
    let segment = fill(&vfs, 50);
    // Damage to the last record cannot be told apart from a write that
    // was cut short by a crash: it is dropped, everything before stays.
    assert!(vfs.corrupt_byte(&segment, SEG_HEADER + 49 * RECORD + 60));
    let engine = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap();
    assert_eq!(engine.last_lsn(), 49);
    assert_eq!(keys(&engine), 49);
}

#[test]
fn records_written_together_and_torn_together_are_a_tail() {
    // Records appended between two syncs were never durable on their
    // own: damage among them, followed by intact ones of the same group,
    // is a torn tail.
    let vfs = SimVfs::new(4);
    let engine = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap();
    for i in 1..=10 {
        engine.sync(engine.write(batch(i)).unwrap()).unwrap();
    }
    for i in 11..=20 {
        engine.write(batch(i)).unwrap();
    }
    engine.sync(20).unwrap();
    std::mem::forget(engine);
    vfs.crash();
    let segment = Path::new("/db/wal").join(&vfs.list(Path::new("/db/wal")).unwrap()[0]);
    assert!(vfs.corrupt_byte(&segment, SEG_HEADER + 14 * RECORD + 60));
    let engine = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap();
    assert_eq!(engine.last_lsn(), 14);
}

#[test]
fn an_oversized_batch_is_an_error() {
    // Five values of 1 GiB: more than a log record can describe. The
    // memory is never touched, so it costs no RAM.
    let huge = || {
        let mut b = WriteBatch::new();
        for i in 0..5u8 {
            b.put(vec![b'k', i], vec![0u8; 1 << 30]);
        }
        b
    };
    let vfs = SimVfs::new(5);
    let engine = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap();
    let err = engine.write(huge()).unwrap_err();
    assert!(matches!(err, StorageError::Invalid(_)), "{err}");
    // The engine is not poisoned and nothing was logged.
    assert_eq!(engine.last_lsn(), 0);
    engine.sync(engine.write(batch(1)).unwrap()).unwrap();
    drop(engine);

    let store = MemoryStore::new();
    let config = RemoteConfig::new(Arc::new(store), RemoteSync::Sync);
    let tiered = TieredEngine::open(Path::new("/t"), options(&vfs), config).unwrap();
    let err = tiered.write(huge()).unwrap_err();
    assert!(matches!(err, StorageError::Invalid(_)), "{err}");
    tiered.sync(tiered.write(batch(2)).unwrap()).unwrap();
}

/// Rewrites the magic of a segment to that of log format 1.
fn mark_format_1(vfs: &SimVfs, segment: &Path) {
    let file = vfs.open(segment).unwrap();
    file.write_at(b"SQWAL001", 0).unwrap();
    file.sync().unwrap();
}

#[test]
fn a_log_of_the_earlier_format_with_records_is_refused() {
    let vfs = SimVfs::new(7);
    let segment = fill(&vfs, 5);
    mark_format_1(&vfs, &segment);
    let err = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap_err();
    assert!(err.to_string().contains("log format 1"), "{err}");
}

/// What the earlier version leaves after a checkpoint, an empty segment
/// of its format, opens: there is nothing to replay.
#[test]
fn an_empty_log_segment_of_the_earlier_format_is_replaced() {
    let vfs = SimVfs::new(8);
    let engine = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap();
    let lsn = engine.write(batch(1)).unwrap();
    engine.sync(lsn).unwrap();
    engine.checkpoint().unwrap();
    drop(engine);
    let segments = vfs.list(Path::new("/db/wal")).unwrap();
    assert_eq!(segments.len(), 1, "{segments:?}");
    let segment = Path::new("/db/wal").join(&segments[0]);
    let file = vfs.open(&segment).unwrap();
    let mut rest = vec![0u8; (file.size().unwrap() - SEG_HEADER) as usize];
    file.read_at(&mut rest, SEG_HEADER).unwrap();
    assert!(rest.iter().all(|&b| b == 0), "the segment is not empty after a checkpoint");
    mark_format_1(&vfs, &segment);

    let engine = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap();
    assert_eq!(keys(&engine), 1);
    let lsn = engine.write(batch(2)).unwrap();
    engine.sync(lsn).unwrap();
    drop(engine);
    assert!(!vfs.exists(&segment).unwrap(), "the old segment was not replaced");
    let engine = DiskEngine::open(Path::new("/db"), options(&vfs)).unwrap();
    assert_eq!(keys(&engine), 2);
}
