//! A sort, a hash aggregation and a hash join with a small `work_mem` do
//! not hold their input in memory. Measured with a counting allocator, so
//! this file holds a single test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use squad::Database;

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Bytes allocated at the peak of running `sql`, beyond what was
/// allocated before.
fn peak_during(conn: &squad::Connection, sql: &str) -> usize {
    let base = CURRENT.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let rows = conn.query(sql, &[]).unwrap();
    assert!(!rows.is_empty());
    drop(rows);
    PEAK.load(Ordering::Relaxed) - base
}

#[test]
fn small_work_mem_bounds_operator_memory() {
    let db = Database::open_in_memory().unwrap();
    let conn = db.connect();
    // 200000 rows of about 150 bytes: some 60 MB as rows in memory.
    conn.batch_execute(
        "CREATE TABLE t (id int PRIMARY KEY, g int, s text);
         INSERT INTO t SELECT i, i % 50000, repeat(md5(i::text), 3) FROM generate_series(1, 200000) i",
    )
    .unwrap();
    // Only one row of each result is returned, so the result set itself
    // takes no memory to speak of.
    let queries = [
        "SELECT id, s FROM t ORDER BY s OFFSET 199999",
        "SELECT s, count(*), max(id) FROM t GROUP BY s ORDER BY 3 DESC LIMIT 1",
        "SELECT count(*) FROM t a JOIN t b ON a.s = b.s",
    ];
    for sql in queries {
        conn.batch_execute("SET work_mem = '1GB'").unwrap();
        let large = peak_during(&conn, sql);
        conn.batch_execute("SET work_mem = '256kB'").unwrap();
        let small = peak_during(&conn, sql);
        assert!(large > 24 << 20, "{sql}: in memory it should hold the rows, peak {large}");
        assert!(small < 8 << 20, "{sql}: with 256kB it held {small} bytes at its peak");
    }
}
