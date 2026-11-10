//! Model test: a random multi-tenant workload runs on a cluster and on one
//! plain database; every statement must have the same outcome on both.
//!
//! `SQUAD_CLUSTER_SEEDS=first..last` selects other seeds.

use squad::{Database, ToSql, Value};
use squad_cluster::{Cluster, ClusterConfig, ClusterConnection, ShardSpec};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len() as u64) as usize]
    }
}

const SCHEMA: &str = "
    CREATE TABLE plans (id int PRIMARY KEY, label text NOT NULL, price numeric);
    CREATE TABLE users (project_id bigint NOT NULL, id int NOT NULL, email text, plan_id int, score int,
                        PRIMARY KEY (project_id, id));
    CREATE TABLE events (project_id bigint NOT NULL, user_id int, name text, value float8, amount numeric,
                         n int, at timestamptz) USING columnar;
    CREATE INDEX users_plan ON users (plan_id);
";

const DECLARE: &str = "
    SELECT squad_distribute('users', 'project_id');
    SELECT squad_distribute('events', 'project_id');
    SELECT squad_reference('plans');
";

const TENANTS: u64 = 12;
const NAMES: &[&str] = &["pageview", "click", "signup", "purchase", "error"];

struct Pair {
    cluster: ClusterConnection,
    model: squad::Connection,
    context: String,
}

type Outcome = Result<(Vec<String>, Vec<Vec<Value>>, u64), &'static str>;

fn show(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter().map(|r| format!("{r:?}")).collect()
}

impl Pair {
    /// Runs a statement on both sides and compares the outcome. Without
    /// `ordered`, rows are compared as a multiset.
    fn check(&self, sql: &str, params: &[&dyn ToSql], ordered: bool) {
        let cluster: Outcome = match self.cluster.query(sql, params) {
            Ok(rows) => Ok((
                rows.first().map(|r| r.columns().iter().map(|c| format!("{}:{}", c.name, c.ty.name())).collect()).unwrap_or_default(),
                rows.iter().map(|r| r.values().to_vec()).collect(),
                0,
            )),
            Err(e) => Err(e.code()),
        };
        let model: Outcome = match self.model.query(sql, params) {
            Ok(rows) => Ok((
                rows.first().map(|r| r.columns().iter().map(|c| format!("{}:{}", c.name, c.ty.name())).collect()).unwrap_or_default(),
                rows.iter().map(|r| r.values().to_vec()).collect(),
                0,
            )),
            Err(e) => Err(e.code()),
        };
        let params: Vec<Value> = params.iter().map(|p| p.to_value()).collect();
        match (cluster, model) {
            (Ok((ccols, crows, _)), Ok((mcols, mrows, _))) => {
                let (mut c, mut m) = (show(&crows), show(&mrows));
                if !ordered {
                    c.sort();
                    m.sort();
                }
                assert!(c == m, "{}\nstatement: {sql}\nparams: {params:?}\ncluster: {c:#?}\nmodel:   {m:#?}", self.context);
                assert!(ccols == mcols, "{}\nstatement: {sql}\ncluster columns: {ccols:?}\nmodel columns:   {mcols:?}", self.context);
            }
            (c, m) => panic!(
                "{}\nstatement: {sql}\nparams: {params:?}\ncluster: {:?}\nmodel:   {:?}",
                self.context,
                c.map(|o| show(&o.1)),
                m.map(|o| show(&o.1))
            ),
        }
    }

    /// Runs a write on both sides; both must change the same number of rows.
    fn write(&self, sql: &str, params: &[&dyn ToSql]) {
        let c = self.cluster.execute(sql, params).map_err(|e| (e.code(), e.message().to_string()));
        let m = self.model.execute(sql, params).map_err(|e| (e.code(), e.message().to_string()));
        match (&c, &m) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "{}\nrows changed by: {sql}", self.context),
            _ => panic!("{}\nstatement: {sql}\ncluster: {c:?}\nmodel:   {m:?}", self.context),
        }
    }
}

fn event_row(rng: &mut Rng) -> String {
    let user = if rng.below(10) == 0 { "NULL".to_string() } else { rng.below(40).to_string() };
    let value = if rng.below(12) == 0 { "NULL".to_string() } else { format!("{}", rng.below(400) as f64 * 0.25) };
    let amount = if rng.below(12) == 0 { "NULL".to_string() } else { format!("{}.{:02}", rng.below(500), rng.below(4) * 25) };
    let n = if rng.below(15) == 0 { "NULL".to_string() } else { rng.below(60).to_string() };
    format!(
        "({}, {user}, '{}', {value}, {amount}, {n}, '2026-03-{:02} {:02}:{:02}:00+00')",
        rng.below(TENANTS),
        rng.pick(NAMES),
        1 + rng.below(5),
        rng.below(24),
        rng.below(4) * 15
    )
}

fn mutate(p: &Pair, rng: &mut Rng, next_user: &mut i32) {
    let tenant = rng.below(TENANTS) as i64;
    match rng.below(16) {
        0 | 1 => {
            *next_user += 1;
            let plan: Option<i32> = if rng.below(6) == 0 { None } else { Some(1 + rng.below(4) as i32) };
            p.write(
                "INSERT INTO users (project_id, id, email, plan_id, score) VALUES ($1, $2, $3, $4, $5)",
                &[&tenant, &*next_user, &format!("u{}@t{tenant}.example", *next_user), &plan, &(rng.below(100) as i32)],
            );
        }
        2 | 3 => {
            // Several rows for several tenants in one statement.
            let mut rows = Vec::new();
            for _ in 0..1 + rng.below(6) {
                *next_user += 1;
                let plan = if rng.below(6) == 0 { "NULL".to_string() } else { (1 + rng.below(4)).to_string() };
                rows.push(format!("({}, {}, 'm{}@example', {plan}, {})", rng.below(TENANTS), *next_user, *next_user, rng.below(100)));
            }
            p.write(&format!("INSERT INTO users VALUES {}", rows.join(", ")), &[]);
        }
        4..=6 => {
            let rows: Vec<String> = (0..1 + rng.below(12)).map(|_| event_row(rng)).collect();
            p.write(&format!("INSERT INTO events VALUES {}", rows.join(", ")), &[]);
        }
        7 => {
            p.write(
                "INSERT INTO events (project_id, user_id, name, value, n, at) VALUES ($1, $2, $3, $4, $5, '2026-03-02 10:30:00+00')",
                &[&tenant, &(rng.below(40) as i32), &rng.pick(NAMES), &(rng.below(100) as f64 * 0.5), &(rng.below(60) as i32)],
            );
        }
        8 => p.write(
            "UPDATE users SET score = score + $1 WHERE project_id = $2 AND id % 3 = $3",
            &[&(rng.below(5) as i32), &tenant, &(rng.below(3) as i32)],
        ),
        9 => p.write("UPDATE users SET score = score + 1, email = upper(email) WHERE score < $1", &[&(rng.below(30) as i32)]),
        10 => p.write(
            "UPDATE users SET plan_id = $1 WHERE project_id IN ($2, $3) AND score > 50",
            &[&(1 + rng.below(4) as i32), &tenant, &(rng.below(TENANTS) as i64)],
        ),
        11 => p.write("DELETE FROM users WHERE project_id = $1 AND score < $2", &[&tenant, &(rng.below(25) as i32)]),
        12 => p.write("DELETE FROM events WHERE project_id = $1 AND name = $2", &[&tenant, &rng.pick(NAMES)]),
        13 => p.write("DELETE FROM events WHERE n > $1", &[&(50 + rng.below(10) as i32)]),
        14 => {
            let id = 1 + rng.below(6) as i32;
            match rng.below(3) {
                0 => p.write(
                    "INSERT INTO plans VALUES ($1, $2, 9.50) ON CONFLICT (id) DO UPDATE SET label = excluded.label",
                    &[&id, &format!("plan{}", rng.below(4))],
                ),
                1 => p.write("UPDATE plans SET price = price + 1.25 WHERE id = $1", &[&id]),
                _ => p.write("DELETE FROM plans WHERE id = $1", &[&id]),
            }
        }
        _ => p.write("DELETE FROM users WHERE project_id = $1 AND id IN (SELECT id FROM plans)", &[&tenant]),
    }
}

fn queries(p: &Pair, rng: &mut Rng) {
    let tenant = rng.below(TENANTS) as i64;
    let other = rng.below(TENANTS) as i64;
    let small = rng.below(40) as i32;
    let limit = 1 + rng.below(6) as i64;

    // One tenant: routed to its shard.
    p.check("SELECT id, email, score FROM users WHERE project_id = $1 ORDER BY id", &[&tenant], true);
    p.check("SELECT count(*), sum(score), avg(score), min(email), max(score) FROM users WHERE project_id = $1", &[&tenant], true);
    p.check(
        "SELECT name, approx_percentile(value, 0.5), approx_count_distinct(user_id), count(*) FROM events \
         WHERE project_id = $1 GROUP BY name ORDER BY name",
        &[&tenant],
        true,
    );
    p.check(
        "SELECT u.email, e.name, e.n FROM users u JOIN events e ON e.project_id = u.project_id AND e.user_id = u.id \
         WHERE u.project_id = $1 ORDER BY 1, 2, 3",
        &[&tenant],
        true,
    );
    p.check(
        "SELECT u.id, p.label FROM users u LEFT JOIN plans p ON p.id = u.plan_id WHERE u.project_id = $1 AND u.score > $2 ORDER BY u.id",
        &[&tenant, &small],
        true,
    );
    p.check("SELECT project_id, count(*) FROM users WHERE project_id IN ($1, $2, 3) GROUP BY project_id ORDER BY 1", &[&tenant, &other], true);
    p.check("SELECT id, score FROM users WHERE project_id IN ($1, $2) ORDER BY score DESC, id LIMIT $3", &[&tenant, &other, &limit], true);
    p.check("SELECT count(*), avg(score) FROM users WHERE project_id IN ($1, $2)", &[&tenant, &other], true);

    // All tenants, grouped by tenant: complete groups on each shard.
    p.check("SELECT project_id, count(*), sum(score), avg(score) FROM users GROUP BY project_id ORDER BY project_id", &[], true);
    p.check(
        "SELECT project_id, name, count(*), approx_count_distinct(user_id), approx_percentile(value, 0.9) FROM events \
         GROUP BY project_id, name ORDER BY project_id, name",
        &[],
        true,
    );
    p.check(
        "SELECT project_id, count(*) AS c FROM events GROUP BY project_id HAVING count(*) > $1 ORDER BY c DESC, project_id LIMIT $2 OFFSET 1",
        &[&(small as i64 / 4), &limit],
        true,
    );
    p.check(
        "SELECT u.project_id, count(*), sum(e.n) FROM users u JOIN events e ON e.project_id = u.project_id AND e.user_id = u.id \
         GROUP BY u.project_id ORDER BY u.project_id",
        &[],
        true,
    );

    // All tenants, grouped by something else: merged from partial aggregates.
    p.check(
        "SELECT plan_id, count(*) AS n, avg(score), sum(score), count(DISTINCT project_id), min(email) FROM users \
         GROUP BY plan_id HAVING count(*) > $1 ORDER BY n DESC, plan_id NULLS LAST LIMIT $2",
        &[&(small as i64 / 8), &limit],
        true,
    );
    p.check(
        "SELECT name, count(*), sum(value), avg(value), sum(amount), avg(amount), min(at), max(n), count(n), count(DISTINCT user_id) \
         FROM events GROUP BY name ORDER BY name",
        &[],
        true,
    );
    p.check(
        "SELECT date_trunc('hour', at) AS h, count(*), sum(n) FROM events WHERE n >= $1 GROUP BY h ORDER BY h DESC LIMIT 5 OFFSET 1",
        &[&small],
        true,
    );
    p.check(
        "SELECT n % 4 AS bucket, count(*) FILTER (WHERE value > 10) AS big, max(amount), min(value) FROM events \
         GROUP BY n % 4 ORDER BY bucket NULLS FIRST",
        &[],
        true,
    );
    p.check("SELECT count(*), count(DISTINCT name), sum(n), avg(n), max(at) FROM events WHERE name <> $1", &[&rng.pick(NAMES)], true);
    p.check("SELECT count(*) + count(DISTINCT name) AS x, sum(n)::float8 / nullif(count(*), 0) AS y FROM events", &[], true);
    p.check("SELECT bool_and(score > 0), bool_or(score > 90), bit_or(score), bit_and(score), every(id > 0) FROM users", &[], true);
    p.check("SELECT 1 + count(*) AS total FROM users WHERE score > $1 HAVING count(*) > 2", &[&small], true);
    p.check(
        "SELECT p.label, count(*), sum(u.score) FROM users u JOIN plans p ON p.id = u.plan_id GROUP BY p.label ORDER BY p.label",
        &[],
        true,
    );
    p.check(
        "SELECT e.name, count(*), avg(u.score), count(DISTINCT u.id) FROM users u JOIN events e \
         ON u.project_id = e.project_id AND u.id = e.user_id GROUP BY e.name ORDER BY 2 DESC, 1",
        &[],
        true,
    );
    p.check("SELECT count(*) FROM users WHERE plan_id IN (SELECT id FROM plans WHERE price > $1)", &[&9i32], true);
    p.check("SELECT plan_id, score % 5, count(*) FROM users GROUP BY 1, 2 ORDER BY 1 NULLS FIRST, 2", &[], true);
    p.check("SELECT upper(name) AS u, count(*) FROM events GROUP BY upper(name) ORDER BY upper(name) DESC", &[], true);

    // All tenants, no aggregation: concatenated, sorted and limited.
    p.check(
        "SELECT project_id, user_id, name, n FROM events WHERE n > $1 ORDER BY n DESC, project_id, user_id NULLS FIRST, name LIMIT 7 OFFSET 2",
        &[&small],
        true,
    );
    p.check("SELECT email FROM users ORDER BY score DESC, project_id, id LIMIT $1", &[&limit], true);
    p.check("SELECT project_id, id FROM users WHERE score > $1", &[&small], false);
    p.check("SELECT project_id, id, score FROM users ORDER BY 3 DESC, 1, 2 LIMIT 4", &[], true);
    p.check("SELECT DISTINCT name FROM events ORDER BY name", &[], true);
    p.check("SELECT DISTINCT plan_id, score % 3 AS m FROM users ORDER BY 1 NULLS FIRST, m LIMIT 9", &[], true);
    p.check("SELECT u.project_id, u.id, p.price FROM users u JOIN plans p ON p.id = u.plan_id ORDER BY 1, 2 LIMIT 20", &[], true);
    p.check("SELECT id, email FROM users WHERE email LIKE $1 ORDER BY project_id, id", &[&"m%"], true);

    // Reference table.
    p.check("SELECT * FROM plans ORDER BY id", &[], true);
    p.check("SELECT count(*), sum(price) FROM plans", &[], true);
}

fn run(shards: usize, seed: u64, steps: usize) {
    let cluster = Cluster::open(ClusterConfig::new(vec![ShardSpec::Memory; shards])).unwrap();
    let db = cluster.connect();
    db.batch_execute(SCHEMA).unwrap();
    db.batch_execute(DECLARE).unwrap();
    let model_db = Database::open_in_memory().unwrap();
    let model = model_db.connect();
    model.batch_execute(SCHEMA).unwrap();

    let mut pair = Pair { cluster: db, model, context: String::new() };
    let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d) ^ shards as u64);
    pair.write("INSERT INTO plans VALUES (1, 'free', 0), (2, 'team', 19.00), (3, 'business', 99.50), (4, 'enterprise', NULL)", &[]);
    let mut next_user = 0;
    for step in 0..steps {
        pair.context = format!("shards={shards} seed={seed} step={step}");
        mutate(&pair, &mut rng, &mut next_user);
        if step % 8 == 7 || step + 1 == steps {
            queries(&pair, &mut rng);
        }
    }

    // Every row is on the shard its key hashes to, and reference tables
    // are the same everywhere.
    let plans = pair.model.query("SELECT * FROM plans ORDER BY id", &[]).unwrap();
    for shard in cluster.shards() {
        let conn = shard.database().unwrap().connect();
        for table in ["users", "events"] {
            for row in conn.query(&format!("SELECT DISTINCT project_id FROM {table}"), &[]).unwrap() {
                assert_eq!(cluster.shard_for(&row.values()[0]), shard.index(), "{table} row on the wrong shard");
            }
        }
        let here = conn.query("SELECT * FROM plans ORDER BY id", &[]).unwrap();
        assert_eq!(
            here.iter().map(|r| format!("{:?}", r.values())).collect::<Vec<_>>(),
            plans.iter().map(|r| format!("{:?}", r.values())).collect::<Vec<_>>()
        );
    }
}

fn seeds() -> std::ops::Range<u64> {
    match std::env::var("SQUAD_CLUSTER_SEEDS").ok().and_then(|s| {
        let (a, b) = s.split_once("..")?;
        Some(a.parse().ok()?..b.parse().ok()?)
    }) {
        Some(r) => r,
        None => 0..10,
    }
}

fn steps() -> usize {
    std::env::var("SQUAD_CLUSTER_STEPS").ok().and_then(|s| s.parse().ok()).unwrap_or(120)
}

#[test]
fn one_shard_matches_a_single_database() {
    seeds().for_each(|seed| run(1, seed, steps()));
}

#[test]
fn two_shards_match_a_single_database() {
    seeds().for_each(|seed| run(2, seed, steps()));
}

#[test]
fn four_shards_match_a_single_database() {
    seeds().for_each(|seed| run(4, seed, steps()));
}

#[test]
fn seven_shards_match_a_single_database() {
    seeds().for_each(|seed| run(7, seed, steps()));
}
