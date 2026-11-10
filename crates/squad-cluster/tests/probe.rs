use squad::{Database, ToSql};

fn show(db: &squad::Connection, sql: &str, params: &[&dyn ToSql]) {
    match db.query(sql, params) {
        Ok(rows) => println!("OK  {sql} -> {:?} cols {:?}", rows.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>(), rows.first().map(|r| r.columns().iter().map(|c| c.ty.name()).collect::<Vec<_>>())),
        Err(e) => println!("ERR {sql} -> {} {}", e.code(), e.message()),
    }
}

#[test]
fn probe() {
    let db = Database::open_in_memory().unwrap();
    let c = db.connect();
    c.batch_execute("create table t (d int, x int, f float8, n numeric, s text, b bool, u uuid, ts timestamptz); insert into t values (1,1,1.5,2.25,'a',true,null,'2026-01-01 10:00:00+00'),(1,2,2.5,3.5,'b',false,null,'2026-01-02 11:00:00+00'),(2,null,0.25,1,'c',true,null,null)").unwrap();
    show(&c, "select array_agg(distinct x), array_agg(distinct s) filter (where d = 1), count(distinct x) from t", &[]);
    show(&c, "select d, array_agg(distinct x) from t group by d", &[]);
    show(&c, "select bool_and(b), bool_or(b), every(b), bit_and(x), bit_or(x), min(s), max(ts), sum(f), avg(f) from t", &[]);
    show(&c, "select x from t order by x", &[]);
    show(&c, "select x from t order by x desc", &[]);
    show(&c, "create table k (key text primary key, value text not null)", &[]);
    show(&c, "insert into k values ('a','1')", &[]);
    show(&c, "insert into k values ('a','2') on conflict (key) do update set value = excluded.value", &[]);
    show(&c, "select * from k", &[]);
    show(&c, "create table ev (pid bigint, name text, v float8, ts timestamptz) using columnar", &[]);
    show(&c, "insert into ev values (1,'a',1.0,'2026-01-01 10:00:00+00'),(2,'b',2.0,'2026-01-01 10:00:00+00')", &[]);
    show(&c, "select pid, approx_count_distinct(name), approx_percentile(v, 0.5), date_trunc('day', ts) from ev group by pid, date_trunc('day', ts) order by 1", &[]);
    show(&c, "delete from ev where pid = 1", &[]);
    show(&c, "update ev set v = 3 where pid = 2", &[]);
    show(&c, "select count(*) as n, sum(x)::bigint as \"S um\" from t having count(*) > 1", &[]);
    show(&c, "select d, count(*) from t group by 1 order by 2 desc, d limit 1 offset 0", &[]);
    show(&c, "select x + 1 as y from t order by y nulls first", &[]);
    show(&c, "select distinct d from t order by d", &[]);
    show(&c, "select (sum(x))::bigint, coalesce(sum(x),0)::bigint, sum(n)::numeric, pg_typeof(sum(f)) from t where d = 99", &[]);
    show(&c, "select * from t where d = $1 for update", &[&1i64]);
    show(&c, "begin", &[]);
    show(&c, "savepoint a", &[]);
    show(&c, "select 1/0", &[]);
    show(&c, "select 1", &[]);
    show(&c, "rollback", &[]);
    show(&c, "insert into t (d, x) values ($1, $2) returning d, x", &[&7i64, &"5"]);
    show(&c, "show search_path", &[]);
    show(&c, "set timezone = 'UTC'", &[]);
    show(&c, "select 'a'::unknowntype", &[]);
    fn is_clone<T: Clone>() {}
    is_clone::<squad::Config>();
    is_clone::<squad::Error>();
    is_clone::<squad::Database>();
    let s = pg_query::deparse(&pg_query::parse("select a from t limit ($1)::bigint + (3)::bigint", 0i32).unwrap().protobuf, Default::default()).unwrap();
    println!("{s}");
}
