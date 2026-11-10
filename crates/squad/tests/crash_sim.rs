//! Deterministic crash simulation at the SQL level.
//!
//! The database runs on a simulated file system that can kill the process
//! at any write, tear unsynced writes and inject I/O errors. Each seed
//! runs a random workload of money transfers and schema changes, crashes
//! at a random point, reopens the database and checks that
//!
//! * every transaction whose COMMIT was acknowledged is present,
//! * every transaction is applied completely or not at all, across tables
//!   and indexes,
//! * the catalog and the storage structures are intact.
//!
//! `SQUAD_SIM_SEEDS` sets the number of seeds (default 6; CI and nightly runs use more),
//! `SQUAD_SIM_START` the first seed, to reproduce a failure.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use squad::{Connection, Database, SyncMode};
use squad_storage::{DiskEngine, DiskOptions, SimVfs};

const ACCOUNTS: i32 = 12;
const INITIAL: i64 = 100;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) ^ (self.0 >> 11)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// What the test knows about the transactions it attempted.
#[derive(Default)]
struct Model {
    /// COMMIT returned success: must be durable.
    acked: BTreeSet<i64>,
    /// The outcome is unknown (an error during or after COMMIT).
    maybe: BTreeSet<i64>,
    next_txid: i64,
    /// The initial rows were committed.
    seeded: bool,
    /// Columns added by acknowledged ALTER TABLE statements.
    extra_columns: BTreeSet<String>,
}

fn open(vfs: &SimVfs, rng: &mut Rng) -> squad::Result<Database> {
    let options = DiskOptions {
        sync_mode: SyncMode::Sync,
        cache_pages: [16, 64, 512][rng.below(3) as usize],
        checkpoint_wal_bytes: [4 << 10, 64 << 10, 1 << 20][rng.below(3) as usize],
        vfs: Arc::new(vfs.clone()),
    };
    let engine = DiskEngine::open(Path::new("/db"), options)?;
    Database::with_engine(Arc::new(engine))
}

fn seed_schema(conn: &Connection, model: &mut Model) -> squad::Result<()> {
    conn.batch_execute(
        "BEGIN;
         CREATE TABLE IF NOT EXISTS acct (id int PRIMARY KEY, balance bigint NOT NULL CHECK (balance >= 0));
         CREATE TABLE IF NOT EXISTS ledger (txid bigint PRIMARY KEY, src int NOT NULL REFERENCES acct (id),
                                           dst int NOT NULL REFERENCES acct (id), amount bigint NOT NULL, note text);
         CREATE INDEX IF NOT EXISTS ledger_src_idx ON ledger (src);
         CREATE UNIQUE INDEX IF NOT EXISTS ledger_note_key ON ledger (note);",
    )?;
    conn.execute(
        "INSERT INTO acct SELECT g, $1 FROM generate_series(1, $2) g ON CONFLICT DO NOTHING",
        &[&INITIAL, &ACCOUNTS],
    )?;
    conn.batch_execute("COMMIT")?;
    model.seeded = true;
    Ok(())
}

/// One random transaction. Returns an error as soon as the simulated
/// process dies or an injected I/O error surfaces.
fn step(conn: &Connection, rng: &mut Rng, model: &mut Model) -> squad::Result<()> {
    match rng.below(20) {
        0 => {
            let name = format!("c{}", rng.below(1000));
            if model.extra_columns.contains(&name) {
                return Ok(());
            }
            conn.batch_execute(&format!("ALTER TABLE ledger ADD COLUMN IF NOT EXISTS {name} int DEFAULT 7"))?;
            model.extra_columns.insert(name);
            Ok(())
        }
        1 => {
            if let Some(name) = model.extra_columns.iter().next().cloned() {
                conn.batch_execute(&format!("ALTER TABLE ledger DROP COLUMN IF EXISTS {name}"))?;
                model.extra_columns.remove(&name);
            }
            Ok(())
        }
        2 => conn.batch_execute("CHECKPOINT"),
        3 => conn.batch_execute("VACUUM"),
        4 => {
            // A transaction that is rolled back must leave no trace.
            conn.batch_execute("BEGIN")?;
            conn.execute("UPDATE acct SET balance = balance + 1000", &[])?;
            conn.execute("DELETE FROM ledger", &[])?;
            conn.batch_execute("ROLLBACK")
        }
        _ => {
            let txid = model.next_txid;
            model.next_txid += 1;
            let src = rng.below(ACCOUNTS as u64) as i32 + 1;
            let dst = rng.below(ACCOUNTS as u64) as i32 + 1;
            let amount = rng.below(30) as i64 + 1;
            // From here on the outcome is unknown until COMMIT answers.
            model.maybe.insert(txid);
            conn.batch_execute("BEGIN")?;
            let balance: i64 = conn.query_one("SELECT balance FROM acct WHERE id = $1 FOR UPDATE", &[&src])?.get(0);
            if balance < amount || src == dst {
                conn.batch_execute("ROLLBACK")?;
                model.maybe.remove(&txid);
                return Ok(());
            }
            conn.execute("UPDATE acct SET balance = balance - $1 WHERE id = $2", &[&amount, &src])?;
            conn.execute("UPDATE acct SET balance = balance + $1 WHERE id = $2", &[&amount, &dst])?;
            conn.execute(
                "INSERT INTO ledger (txid, src, dst, amount, note) VALUES ($1, $2, $3, $4, $5)",
                &[&txid, &src, &dst, &amount, &format!("transfer {txid}")],
            )?;
            conn.batch_execute("COMMIT")?;
            model.maybe.remove(&txid);
            model.acked.insert(txid);
            Ok(())
        }
    }
}

/// Checks every invariant against a freshly opened database.
fn check(db: &Database, model: &Model, ctx: &str) {
    let report = db.verify().unwrap_or_else(|e| panic!("{ctx}: verify failed: {e}"));
    assert!(report.is_ok(), "{ctx}: storage verification: {:?}", report.errors);
    let conn = db.connect();
    let tables: i64 = conn
        .query_one("SELECT count(*) FROM pg_tables WHERE schemaname = 'public'", &[])
        .unwrap_or_else(|e| panic!("{ctx}: catalog unreadable: {e}"))
        .get(0);
    if !model.seeded && tables < 2 {
        // The crash hit before the schema transaction committed: it must
        // be absent as a whole.
        assert_eq!(tables, 0, "{ctx}: schema transaction applied partially");
        return;
    }
    assert_eq!(tables, 2, "{ctx}: tables missing after recovery");

    let rows = conn.query("SELECT id, balance FROM acct ORDER BY id", &[]).unwrap_or_else(|e| panic!("{ctx}: {e}"));
    assert_eq!(rows.len(), ACCOUNTS as usize, "{ctx}: accounts missing");
    let total: i64 = rows.iter().map(|r| r.get::<i64>(1)).sum();
    assert_eq!(total, INITIAL * ACCOUNTS as i64, "{ctx}: money was created or destroyed");

    // Replay the ledger: balances must be exactly what its rows imply.
    let ledger = conn.query("SELECT txid, src, dst, amount, note FROM ledger ORDER BY txid", &[]).unwrap();
    let mut expected: BTreeMap<i32, i64> = (1..=ACCOUNTS).map(|id| (id, INITIAL)).collect();
    let mut present = BTreeSet::new();
    for row in &ledger {
        let (txid, src, dst, amount): (i64, i32, i32, i64) = (row.get(0), row.get(1), row.get(2), row.get(3));
        assert_eq!(row.get::<String>(4), format!("transfer {txid}"), "{ctx}: ledger row damaged");
        *expected.get_mut(&src).unwrap() -= amount;
        *expected.get_mut(&dst).unwrap() += amount;
        present.insert(txid);
    }
    for row in &rows {
        let (id, balance): (i32, i64) = (row.get(0), row.get(1));
        assert_eq!(balance, expected[&id], "{ctx}: balance of account {id} does not match the ledger");
    }
    for txid in &model.acked {
        assert!(present.contains(txid), "{ctx}: acknowledged transaction {txid} was lost");
    }
    for txid in &present {
        assert!(
            model.acked.contains(txid) || model.maybe.contains(txid),
            "{ctx}: transaction {txid} appeared although it was never committed"
        );
    }

    // Indexes must agree with the table.
    for src in 1..=ACCOUNTS {
        let by_index: i64 = conn.query_one("SELECT count(*) FROM ledger WHERE src = $1", &[&src]).unwrap().get(0);
        let by_scan: i64 = conn.query_one("SELECT count(*) FROM ledger WHERE src + 0 = $1", &[&src]).unwrap().get(0);
        assert_eq!(by_index, by_scan, "{ctx}: index ledger_src_idx disagrees with the table for src {src}");
    }
    for txid in &present {
        let hit = conn.query("SELECT txid FROM ledger WHERE note = $1", &[&format!("transfer {txid}")]).unwrap();
        assert_eq!(hit.len(), 1, "{ctx}: unique index lost the entry of transaction {txid}");
    }
    // Acknowledged schema changes are in place.
    for column in &model.extra_columns {
        let n: i64 = conn
            .query_one(
                "SELECT count(*) FROM information_schema.columns WHERE table_name = 'ledger' AND column_name = $1",
                &[column],
            )
            .unwrap()
            .get(0);
        assert_eq!(n, 1, "{ctx}: acknowledged column {column} is missing");
    }
}

fn run_seed(seed: u64) {
    let vfs = SimVfs::new(seed);
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03);
    let mut model = Model::default();
    let rounds = 3 + rng.below(4);
    for round in 0..=rounds {
        let ctx = format!("seed {seed} round {round}");
        let last = round == rounds;
        if !last {
            match rng.below(10) {
                0..=6 => vfs.crash_after(1 + rng.below(1500)),
                7 => vfs.crash_after(1 + rng.below(60)),
                _ => vfs.set_error_rate(200 + rng.below(5000)),
            }
        }
        match open(&vfs, &mut rng) {
            Ok(db) => {
                let conn = db.connect();
                let mut outcome = Ok(());
                if !model.seeded {
                    outcome = seed_schema(&conn, &mut model);
                }
                let steps = if last { 40 } else { 400 };
                for _ in 0..steps {
                    if outcome.is_err() {
                        break;
                    }
                    outcome = step(&conn, &mut rng, &mut model);
                }
                if let Err(e) = &outcome {
                    // Only injected faults may fail the workload.
                    assert!(
                        vfs.is_crashed() || vfs.injected_errors() > 0,
                        "{ctx}: unexpected error without an injected fault: {e}"
                    );
                    // A failed ALTER TABLE has an unknown outcome; forget
                    // what we believed about extra columns.
                    model.extra_columns.clear();
                }
                assert!(!last || outcome.is_ok(), "{ctx}: fault-free round failed: {outcome:?}");
                drop(conn);
                drop(db);
            }
            Err(e) => {
                assert!(
                    !last && (vfs.is_crashed() || vfs.injected_errors() > 0),
                    "{ctx}: open failed without an injected fault: {e}"
                );
            }
        }
        // Power loss, then recovery without faults and a full check.
        vfs.crash();
        vfs.set_error_rate(0);
        let db = open(&vfs, &mut rng).unwrap_or_else(|e| panic!("{ctx}: recovery failed: {e}"));
        check(&db, &model, &ctx);
        // What survived is now known for good.
        let present: BTreeSet<i64> = db
            .connect()
            .query("SELECT txid FROM ledger", &[])
            .map(|r| r.iter().map(|x| x.get(0)).collect())
            .unwrap_or_default();
        model.maybe.retain(|t| present.contains(t));
        model.acked.extend(std::mem::take(&mut model.maybe));
        if !model.seeded {
            model.seeded = db.connect().query("SELECT 1 FROM acct LIMIT 1", &[]).is_ok_and(|r| !r.is_empty());
        }
        drop(db);
        vfs.crash();
    }
}

#[test]
fn sql_crash_simulation() {
    let start = env("SQUAD_SIM_START", 0);
    let seeds = env("SQUAD_SIM_SEEDS", 6);
    for seed in start..start + seeds {
        run_seed(seed);
    }
}
