# Transactions, isolation and locking

squad implements multi-version concurrency control. Each statement or
transaction reads from a consistent snapshot; writers create new row
versions instead of overwriting. The observable behaviour at each
isolation level is meant to be the one PostgreSQL documents, and is
tested as such in `crates/squad/tests/concurrency.rs`.

## Isolation levels

```sql
BEGIN;                                    -- Read Committed (default)
BEGIN ISOLATION LEVEL REPEATABLE READ;
BEGIN ISOLATION LEVEL SERIALIZABLE;
SET default_transaction_isolation = 'serializable';
```

`READ UNCOMMITTED` is accepted and behaves as Read Committed, as in
Postgres.

| Phenomenon | Read Committed | Repeatable Read | Serializable |
| --- | --- | --- | --- |
| Dirty read | never | never | never |
| Non-repeatable read | possible | never | never |
| Phantom read | possible | never | never |
| Lost update | prevented by row locks | fails with `40001` | fails with `40001` |
| Write skew | possible | possible | fails with `40001` |

### Read Committed

Every statement sees everything committed before the statement began.
When an `UPDATE`, `DELETE` or `SELECT … FOR UPDATE` finds its target row
locked by another transaction, it waits. If that transaction commits a
change to the row, the statement continues with the new version and
re-evaluates its `WHERE` clause against it; a row that no longer matches
is skipped. This is what makes patterns such as

```sql
UPDATE jobs SET state = 'running' WHERE id = 7 AND state = 'pending';
```

safe from two workers both succeeding.

### Repeatable Read

The transaction keeps the snapshot taken by its first statement. It never
sees changes committed later, including new rows. If it tries to update
or delete a row that another transaction changed after that snapshot, it
fails with `40001` (`could not serialize access due to concurrent
update`) and should be retried.

### Serializable

Serializable transactions run on a snapshot like Repeatable Read, and in
addition squad records what each transaction read — individual keys and
scanned key ranges. At commit, a transaction that wrote something is
rejected with `40001` if a transaction that committed after its snapshot
changed anything it read. The effect is that committed serializable
transactions are equivalent to running one after another.

This is stricter than PostgreSQL's SSI, which only aborts when it finds
a dangerous structure of two consecutive read-write conflicts: squad can
abort in situations where Postgres would let both transactions commit.
It never allows an anomaly Postgres would prevent. Applications that
already retry on `40001`, as Serializable requires, need no change.

## Row locks

| Statement | Lock |
| --- | --- |
| `UPDATE`, `DELETE`, `INSERT … ON CONFLICT DO UPDATE` | exclusive on each affected row |
| `SELECT … FOR UPDATE`, `FOR NO KEY UPDATE` | exclusive |
| `SELECT … FOR SHARE`, `FOR KEY SHARE` | shared |
| `INSERT` | exclusive on the new primary key and on each unique index key, which is how two inserts of the same key are serialized |
| inserting or updating a row with a foreign key | shared on the referenced row, so it cannot be deleted before commit |

Locks are held until the transaction ends. Plain `SELECT` never takes or
waits for a row lock.

`NOWAIT` fails immediately with `55P03` instead of waiting, `SKIP LOCKED`
skips rows that are locked, and `SET lock_timeout = '2s'` bounds every
lock wait. A queue with competing consumers is therefore written the
usual way:

```sql
BEGIN;
SELECT id, payload FROM jobs
 WHERE state = 'pending'
 ORDER BY id
 LIMIT 1
 FOR UPDATE SKIP LOCKED;
-- … work …
UPDATE jobs SET state = 'done' WHERE id = $1;
COMMIT;
```

### Deadlocks

When transactions wait for each other in a cycle, the one whose wait
would close the cycle fails with `40P01` (`deadlock detected`). Detection
happens at the moment the wait would begin, not after a timeout.

## Schema changes

DDL is transactional and can be mixed with data changes:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN plan text NOT NULL DEFAULT 'free';
UPDATE users SET plan = 'pro' WHERE id IN (SELECT user_id FROM subscriptions);
COMMIT;   -- or ROLLBACK: neither the column nor the updates remain
```

A schema-changing transaction takes an exclusive lock on the tables it
alters, so concurrent writers to those tables wait until it ends, and
only one transaction changes the schema at a time. Readers are not
blocked.

Known gap: a reader that starts in the instant between a schema change
being committed and being published to other sessions may, for that one
statement, combine the old schema with new data. Operations that rewrite
rows (`DROP COLUMN`, `ALTER COLUMN TYPE`) are best run while no long
analytical query on the same table is in flight.

## Savepoints

```sql
BEGIN;
INSERT INTO orders …;
SAVEPOINT items;
INSERT INTO order_items …;      -- fails
ROLLBACK TO SAVEPOINT items;    -- the transaction is usable again
COMMIT;                         -- the order is kept
```

Row locks taken after a savepoint are kept when rolling back to it
(Postgres releases them); this can only make another transaction wait
longer, never produce a wrong result.

## Differences from PostgreSQL to be aware of

- Serializable can report `40001` more often (see above).
- Constraints are always checked immediately, row by row. Foreign key
  actions run as each row is changed rather than at the end of the
  statement, and `DEFERRABLE` / `INITIALLY DEFERRED` have no effect.
- Advisory locks (`pg_advisory_lock` and friends) succeed without
  locking anything. Migration tools that use them to prevent concurrent
  runs are not protected against each other.
- Row locks do not distinguish the `KEY` variants: `FOR NO KEY UPDATE`
  is `FOR UPDATE`, `FOR KEY SHARE` is `FOR SHARE`.
