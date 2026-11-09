//! Expected values were produced by PostgreSQL 16.

use super::*;
use crate::{FnCtx, Result, Type, Value};
use Type::*;

type A = (Value, Type);

/// 2024-03-15 12:30:45.123456 UTC, session time zone UTC.
fn ctx() -> FnCtx {
    let t = match parse_value("2024-03-15 12:30:45.123456+00", &TimestampTz, &FnCtx::default()).unwrap() {
        Value::TimestampTz(t) => t,
        _ => unreachable!(),
    };
    FnCtx { txn_time: t, stmt_time: t, tz: chrono_tz::UTC }
}
fn ctx_tz(tz: &str) -> FnCtx {
    FnCtx { tz: tz.parse().unwrap(), ..ctx() }
}

fn p(text: &str, ty: Type) -> A {
    (parse_value(text, &ty, &ctx()).unwrap_or_else(|e| panic!("{text}: {e}")), ty)
}
fn u(text: &str) -> A {
    (Value::Text(text.into()), Unknown)
}
fn i(v: i32) -> A {
    (Value::Int4(v), Int4)
}
fn n(text: &str) -> A {
    p(text, Numeric)
}
fn t(text: &str) -> A {
    (Value::Text(text.into()), Text)
}
fn f(v: f64) -> A {
    (Value::Float8(v), Float8)
}
fn b(v: bool) -> A {
    (Value::Bool(v), Bool)
}
fn nul(ty: Type) -> A {
    (Value::Null, ty)
}
fn ts(text: &str) -> A {
    p(text, Timestamp)
}
fn tstz(text: &str) -> A {
    p(text, TimestampTz)
}
fn date(text: &str) -> A {
    p(text, Date)
}
fn iv(text: &str) -> A {
    p(text, Interval)
}
fn jb(text: &str) -> A {
    p(text, Jsonb)
}
fn arr(text: &str, elem: Type) -> A {
    p(text, elem.array_of())
}

fn show(r: Result<Value>, c: &FnCtx) -> String {
    match r {
        Ok(Value::Null) => "NULL".into(),
        Ok(v) => format_value(&v, c),
        Err(e) => format!("ERR:{}", e.code()),
    }
}
fn call(func: Result<Func>, args: Vec<A>, c: &FnCtx) -> Result<Value> {
    let func = func?;
    let mut vals = Vec::new();
    for ((v, _), ty) in args.into_iter().zip(&func.args) {
        vals.push(cast(v, ty, c)?);
    }
    if func.strict && vals.iter().any(Value::is_null) {
        return Ok(Value::Null);
    }
    (func.eval)(c, &vals)
}
fn op_in(c: &FnCtx, l: A, name: &str, r: A) -> String {
    show(call(resolve_operator(name, Some(&l.1), &r.1), vec![l, r], c), c)
}
fn op(l: A, name: &str, r: A) -> String {
    op_in(&ctx(), l, name, r)
}
fn pre(name: &str, r: A) -> String {
    show(call(resolve_operator(name, None, &r.1), vec![r], &ctx()), &ctx())
}
fn func_in(c: &FnCtx, name: &str, args: Vec<A>) -> String {
    let types: Vec<Type> = args.iter().map(|a| a.1.clone()).collect();
    show(call(resolve_function(name, &types), args, c), c)
}
fn func(name: &str, args: Vec<A>) -> String {
    func_in(&ctx(), name, args)
}
fn op_type(l: Type, name: &str, r: Type) -> String {
    resolve_operator(name, Some(&l), &r).map_or_else(|e| format!("ERR:{}", e.code()), |f| f.ret.name())
}
fn fn_type(name: &str, args: &[Type]) -> String {
    resolve_function(name, args).map_or_else(|e| format!("ERR:{}", e.code()), |f| f.ret.name())
}
/// Parse as `ty` and print again.
fn rt(text: &str, ty: Type) -> String {
    match parse_value(text, &ty, &ctx()) {
        Ok(v) => format_value(&v, &ctx()),
        Err(e) => format!("ERR:{}", e.code()),
    }
}
fn conv(a: A, to: Type) -> String {
    show(cast(a.0, &to, &ctx()), &ctx())
}

#[test]
fn scalar_input_output() {
    assert_eq!(rt("yes", Bool), "t");
    assert_eq!(rt("OFF", Bool), "f");
    assert_eq!(rt(" t ", Bool), "t");
    assert_eq!(rt("x", Bool), "ERR:22P02");
    assert_eq!(rt("0x1F", Int4), "31");
    assert_eq!(rt("1_000", Int4), "1000");
    assert_eq!(rt(" 42 ", Int2), "42");
    assert_eq!(rt("-9223372036854775808", Int8), "-9223372036854775808");
    assert_eq!(rt("abc", Int4), "ERR:22P02");
    assert_eq!(rt("99999999999", Int4), "ERR:22003");
    assert_eq!(rt("40000", Int2), "ERR:22003");
    assert_eq!(rt("1e3", Numeric), "1000");
    assert_eq!(rt("1.5e-3", Numeric), "0.0015");
    assert_eq!(rt(".5", Numeric), "0.5");
    assert_eq!(rt("-0.0", Numeric), "0.0");
    assert_eq!(rt("1.50", Numeric), "1.50");
    assert_eq!(rt("abc", Numeric), "ERR:22P02");
    assert_eq!(rt("1e2", Float8), "100");
    assert_eq!(rt("inf", Float8), "Infinity");
    assert_eq!(rt("-inf", Float8), "-Infinity");
    assert_eq!(rt("nan", Float8), "NaN");
    assert_eq!(rt("1e400", Float8), "ERR:22003");
    assert_eq!(rt("1e14", Float8), "100000000000000");
    assert_eq!(rt("1e15", Float8), "1e+15");
    assert_eq!(rt("123456789012345678", Float8), "1.2345678901234568e+17");
    assert_eq!(rt("0.0001", Float8), "0.0001");
    assert_eq!(rt("0.00001", Float8), "1e-05");
    assert_eq!(rt("1.5e-7", Float8), "1.5e-07");
    assert_eq!(rt("-0.0", Float8), "-0");
    assert_eq!(rt("1e5", Float4), "100000");
    assert_eq!(rt("1e6", Float4), "1e+06");
    assert_eq!(rt("1234567", Float4), "1.234567e+06");
    assert_eq!(rt("1.1", Float4), "1.1");
    assert_eq!(rt("123456789", Float4), "1.2345679e+08");
    assert_eq!(rt("\\x4142", Bytea), "\\x4142");
    assert_eq!(rt("ab\\\\c\\101", Bytea), "\\x61625c6341");
    assert_eq!(rt("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", Uuid), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assert_eq!(rt("{a0eebc999c0b4ef8bb6d6bb9bd380a11}", Uuid), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assert_eq!(rt("zz", Uuid), "ERR:22P02");
}

#[test]
fn array_and_record_io() {
    assert_eq!(rt("{1, 2 ,NULL}", Int4.array_of()), "{1,2,NULL}");
    assert_eq!(rt(r#"{"a b",c,"",NULL,"NULL"}"#, Text.array_of()), r#"{"a b",c,"",NULL,"NULL"}"#);
    assert_eq!(rt("{}", Text.array_of()), "{}");
    assert_eq!(rt("{ }", Int4.array_of()), "{}");
    assert_eq!(rt("[1:2]={5,6}", Int4.array_of()), "{5,6}");
    assert_eq!(rt(r#"{"a\"b",c\,d}"#, Text.array_of()), r#"{"a\"b","c,d"}"#);
    assert_eq!(rt("{1.5,2.25}", Numeric.array_of()), "{1.5,2.25}");
    assert_eq!(rt("{t,f}", Bool.array_of()), "{t,f}");
    assert_eq!(rt("{1,2", Int4.array_of()), "ERR:22P02");
    let c = ctx();
    let texts = ["a", "b c", "", "NULL", "x\"y", "a,b", "{}", "a\\b"];
    let mut items: Vec<Value> = texts.iter().map(|s| Value::text(*s)).collect();
    items.push(Value::Null);
    assert_eq!(format_value(&Value::Array(items), &c), r#"{a,"b c","","NULL","x\"y","a,b","{}","a\\b",NULL}"#);
    let rec = Value::Record(vec![
        Value::Int4(1),
        Value::text("a b"),
        Value::Null,
        Value::text(""),
        Value::text("x,y"),
        Value::text("q\"r"),
    ]);
    assert_eq!(format_value(&rec, &c), r#"(1,"a b",,"","x,y","q""r")"#);
    let tsarr = Value::Array(vec![tstz("2020-01-01 10:00+00").0]);
    assert_eq!(format_value(&tsarr, &c), r#"{"2020-01-01 10:00:00+00"}"#);
    assert_eq!(format_value(&Value::Array(vec![jb(r#"{"a":1}"#).0]), &c), r#"{"{\"a\": 1}"}"#);
}

#[test]
fn json_io() {
    assert_eq!(
        rt(r#"{"b":1,"a":[1,2,{"x":null}],"aa":"s\"t", "d":1e2, "e": true}"#, Jsonb),
        r#"{"a": [1, 2, {"x": null}], "b": 1, "d": 100, "e": true, "aa": "s\"t"}"#
    );
    assert_eq!(rt(r#"{"b":1, "a": [1 ,2]}"#, Json), r#"{"b":1, "a": [1 ,2]}"#);
    assert_eq!(rt("[]", Jsonb), "[]");
    assert_eq!(rt("{}", Jsonb), "{}");
    assert_eq!(rt("\"x\"", Jsonb), "\"x\"");
    assert_eq!(rt("{", Jsonb), "ERR:22P02");
    assert_eq!(rt("{", Json), "ERR:22P02");
}

#[test]
fn datetime_io() {
    assert_eq!(rt("2020-01-01 10:00:00.5", Timestamp), "2020-01-01 10:00:00.5");
    assert_eq!(rt("2020-01-01 10:00:00.123456+05:30", TimestampTz), "2020-01-01 04:30:00.123456+00");
    assert_eq!(rt("0044-03-15 BC", Date), "0044-03-15 BC");
    assert_eq!(rt("12345-01-01", Date), "12345-01-01");
    assert_eq!(rt("infinity", Timestamp), "infinity");
    assert_eq!(rt("-infinity", Date), "-infinity");
    assert_eq!(rt("10:00:00.10", Time), "10:00:00.1");
    assert_eq!(rt("epoch", Timestamp), "1970-01-01 00:00:00");
    assert_eq!(rt("1/2/2020", Date), "2020-01-02");
    assert_eq!(rt("Jan 5 2020", Date), "2020-01-05");
    assert_eq!(rt("January 5, 2020", Date), "2020-01-05");
    assert_eq!(rt("20200105", Date), "2020-01-05");
    assert_eq!(rt("2020-01-05T10:11:12Z", TimestampTz), "2020-01-05 10:11:12+00");
    assert_eq!(rt("2020-01-05 10:11:12 America/New_York", TimestampTz), "2020-01-05 15:11:12+00");
    assert_eq!(rt("2020-01-05 3:04 PM", Timestamp), "2020-01-05 15:04:00");
    assert_eq!(rt("2020-02-30", Date), "ERR:22008");
    assert_eq!(rt("2020-13-01", Date), "ERR:22008");
    assert_eq!(rt("foo", Date), "ERR:22007");
    assert_eq!(rt("2020-01-01 25:00", Timestamp), "ERR:22008");
    assert_eq!(rt("today", Date), "2024-03-15");
    assert_eq!(rt("tomorrow", Timestamp), "2024-03-16 00:00:00");
    assert_eq!(rt("now", TimestampTz), "2024-03-15 12:30:45.123456+00");
    let ny = ctx_tz("America/New_York");
    let show_in = |text: &str| format_value(&parse_value(text, &TimestampTz, &ny).unwrap(), &ny);
    assert_eq!(show_in("2021-11-07 01:30:00"), "2021-11-07 01:30:00-05");
    assert_eq!(show_in("2021-03-14 02:30:00"), "2021-03-14 03:30:00-04");
    assert_eq!(show_in("2021-07-01 12:00:00+00"), "2021-07-01 08:00:00-04");
    let kolkata = ctx_tz("Asia/Kolkata");
    assert_eq!(format_value(&tstz("2024-03-15 12:00+00").0, &kolkata), "2024-03-15 17:30:00+05:30");
}

#[test]
fn interval_io() {
    let cases = [
        ("1 year 2 mons 3 days 04:05:06.7", "1 year 2 mons 3 days 04:05:06.7"),
        ("-1 day +02:00", "-1 days +02:00:00"),
        ("1 mon -1 day", "1 mon -1 days"),
        ("0", "00:00:00"),
        ("25 hours", "25:00:00"),
        ("-1 year -2 mons", "-1 years -2 mons"),
        ("1.5 days", "1 day 12:00:00"),
        ("1.5 months", "1 mon 15 days"),
        ("P1Y2M3DT4H5M6S", "1 year 2 mons 3 days 04:05:06"),
        ("1 day ago", "-1 days"),
        ("1:2:3", "01:02:03"),
        ("90 seconds", "00:01:30"),
        ("1 2:03:04", "1 day 02:03:04"),
        ("-00:00:01", "-00:00:01"),
        ("1 week", "7 days"),
        ("3", "00:00:03"),
        ("1.5 years", "1 year 6 mons"),
        ("2 hours 30 minutes", "02:30:00"),
        ("5min", "00:05:00"),
    ];
    for (input, want) in cases {
        assert_eq!(rt(input, Interval), want, "{input}");
    }
    assert_eq!(rt("xyz", Interval), "ERR:22007");
}

#[test]
fn casts() {
    assert_eq!(conv(n("1.5"), Int4), "2");
    assert_eq!(conv(n("2.5"), Int4), "3");
    assert_eq!(conv(n("-2.5"), Int4), "-3");
    assert_eq!(conv(f(2.5), Int4), "2");
    assert_eq!(conv(f(3.5), Int4), "4");
    assert_eq!(conv(f(1e10), Int8), "10000000000");
    assert_eq!(conv(f(3e9), Int4), "ERR:22003");
    assert_eq!(conv(f(f64::NAN), Int4), "ERR:22003");
    assert_eq!(conv(n("1e28"), Int8), "ERR:22003");
    assert_eq!(conv(f(1.0), Text), "1");
    assert_eq!(conv(i(1), Bool), "t");
    assert_eq!(conv(i(0), Bool), "f");
    assert_eq!(conv(b(true), Int4), "1");
    assert_eq!(conv(b(true), Text), "true");
    assert_eq!(conv(f(1.234_567_890_123_456_8), Numeric), "1.23456789012346");
    assert_eq!(conv(p("1.1", Float4), Float8), "1.100000023841858");
    assert_eq!(conv(p("1.1", Float4), Numeric), "1.1");
    assert_eq!(conv(f(0.1), Numeric), "0.1");
    assert_eq!(conv(f(1e20), Numeric), "100000000000000000000");
    assert_eq!(conv(f(1e-10), Numeric), "0.0000000001");
    assert_eq!(conv(n("12345.678"), Float4), "12345.678");
    assert_eq!(conv(date("2020-01-01"), Timestamp), "2020-01-01 00:00:00");
    assert_eq!(conv(ts("2020-01-01 10:11:12"), Date), "2020-01-01");
    assert_eq!(conv(ts("2020-01-01 10:11:12"), Time), "10:11:12");
    assert_eq!(conv(p("10:11", Time), Interval), "10:11:00");
    assert_eq!(conv(iv("25:00"), Time), "01:00:00");
    assert_eq!(conv(tstz("2020-06-01 12:00+00"), Date), "2020-06-01");
    assert_eq!(conv(jb("\"a\""), Text), "\"a\"");
    assert_eq!(conv(p(r#"{"a":1}"#, Json), Jsonb), r#"{"a": 1}"#);
    assert_eq!(conv(jb(r#"{"a":1}"#), Json), r#"{"a": 1}"#);
    assert_eq!(conv(jb("5"), Int4), "5");
    assert_eq!(conv(jb("5.7"), Int4), "6");
    assert_eq!(conv(jb("true"), Bool), "t");
    assert_eq!(conv(jb("1.5"), Float8), "1.5");
    assert_eq!(conv(jb("\"a\""), Int4), "ERR:22023");
    assert_eq!(conv(arr("{1,2}", Int4), Text), "{1,2}");
    assert_eq!(conv(arr("{1,2}", Int4), Text.array_of()), "{1,2}");
    assert_eq!(conv(arr("{1.7,2.2}", Numeric), Int4.array_of()), "{2,2}");
    assert_eq!(conv(t("abc"), Int4), "ERR:22P02");
    assert_eq!(conv(nul(Int4), Text), "NULL");
    let ny = ctx_tz("America/New_York");
    assert_eq!(show(cast(ts("2024-03-15 12:00").0, &TimestampTz, &ny), &ny), "2024-03-15 12:00:00-04");
    assert_eq!(show(cast(tstz("2024-03-15 12:00+00").0, &Timestamp, &ny), &ny), "2024-03-15 08:00:00");
}

#[test]
fn cast_rules() {
    use CastContext::*;
    assert!(can_cast(&Int4, &Int8, Implicit));
    assert!(can_cast(&Int4, &Numeric, Implicit));
    assert!(can_cast(&Numeric, &Float8, Implicit));
    assert!(!can_cast(&Int8, &Int4, Implicit));
    assert!(can_cast(&Int8, &Int4, Assignment));
    assert!(!can_cast(&Float8, &Numeric, Implicit));
    assert!(can_cast(&Unknown, &Enum("mood".into()), Implicit));
    assert!(!can_cast(&Text, &Int4, Assignment));
    assert!(can_cast(&Text, &Int4, Explicit));
    assert!(can_cast(&Int4, &Text, Assignment));
    assert!(!can_cast(&Int4, &Text, Implicit));
    assert!(can_cast(&Enum("mood".into()), &Text, Assignment));
    assert!(can_cast(&Text, &Enum("mood".into()), Explicit));
    assert!(!can_cast(&Text, &Enum("mood".into()), Assignment));
    assert!(can_cast(&Date, &TimestampTz, Implicit));
    assert!(!can_cast(&Timestamp, &Date, Implicit));
    assert!(can_cast(&Int4.array_of(), &Int8.array_of(), Implicit));
    assert!(!can_cast(&Int4, &Bool, Assignment));
    assert!(can_cast(&Int4, &Bool, Explicit));
    assert!(!can_cast(&Bool, &Float8, Explicit));

    let ct = |types: &[Type]| common_type(types, "CASE").map_or_else(|e| format!("ERR:{}", e.code()), |t| t.name());
    assert_eq!(ct(&[Int2, Int8]), "bigint");
    assert_eq!(ct(&[Int4, Numeric]), "numeric");
    assert_eq!(ct(&[Date, TimestampTz]), "timestamp with time zone");
    assert_eq!(ct(&[Unknown, Unknown]), "text");
    assert_eq!(ct(&[Unknown, Int4]), "integer");
    assert_eq!(ct(&[Float4, Int4]), "real");
    assert_eq!(ct(&[Float4, Numeric]), "real");
    assert_eq!(ct(&[Int4, Float8, Numeric]), "double precision");
    assert_eq!(ct(&[Int4.array_of(), Numeric.array_of()]), "numeric[]");
    assert_eq!(ct(&[Int4, Text]), "ERR:42804");
    assert_eq!(
        common_type(&[Text, Int4], "CASE").unwrap_err().message(),
        "CASE types text and integer cannot be matched"
    );
}

#[test]
fn arithmetic() {
    assert_eq!(op(i(7), "/", i(2)), "3");
    assert_eq!(op(i(-7), "/", i(2)), "-3");
    assert_eq!(op(i(7), "%", i(3)), "1");
    assert_eq!(op(i(-7), "%", i(3)), "-1");
    assert_eq!(op(n("7.0"), "/", i(2)), "3.5000000000000000");
    assert_eq!(op(f(7.0), "/", i(2)), "3.5");
    assert_eq!(op(f(0.1), "+", f(0.2)), "0.30000000000000004");
    assert_eq!(op(i(10), "%", n("3.3")), "0.1");
    assert_eq!(op(n("3.0"), "%", i(2)), "1.0");
    assert_eq!(op(n("1.0"), "*", n("2.50")), "2.500");
    assert_eq!(op(n("1.20"), "+", i(1)), "2.20");
    assert_eq!(op(i(5), "/", n("2.0")), "2.5000000000000000");
    assert_eq!(op(n("1"), "/", i(3)), "0.33333333333333333333");
    assert_eq!(op(n("2"), "/", i(3)), "0.66666666666666666667");
    assert_eq!(op(n("10"), "/", i(4)), "2.5000000000000000");
    assert_eq!(op(n("100000"), "/", i(3)), "33333.333333333333");
    assert_eq!(op(n("0.001"), "/", i(3)), "0.00033333333333333333");
    assert_eq!(op(n("1"), "/", i(7)), "0.14285714285714285714");
    assert_eq!(op(n("22.0"), "/", n("7.000")), "3.1428571428571429");
    assert_eq!(op(n("1.5"), "*", n("2.25")), "3.375");
    assert_eq!(op(i(1), "/", i(0)), "ERR:22012");
    assert_eq!(op(n("1.0"), "/", i(0)), "ERR:22012");
    assert_eq!(op(f(1.0), "/", i(0)), "ERR:22012");
    assert_eq!(op(i(5), "%", i(0)), "ERR:22012");
    assert_eq!(op(i(i32::MAX), "+", i(1)), "ERR:22003");
    assert_eq!(op((Value::Int8(i64::MAX), Int8), "*", i(2)), "ERR:22003");
    assert_eq!(op((Value::Int2(32767), Int2), "+", (Value::Int2(1), Int2)), "ERR:22003");
    assert_eq!(op(f(1e308), "*", i(10)), "ERR:22003");
    assert_eq!(op(i(1), "+", b(true)), "ERR:42883");
    assert_eq!(op(t("a"), "+", i(1)), "ERR:42883");
    assert_eq!(op(u("1"), "+", u("2")), "ERR:42725");
    assert_eq!(
        resolve_operator("+", Some(&Int4), &Bool).unwrap_err().message(),
        "operator does not exist: integer + boolean"
    );
    assert_eq!(op(i(1), "+", u("2")), "3");
    assert_eq!(op(i(2), "^", i(10)), "1024");
    assert_eq!(op(n("10"), "^", i(2)), "100.00000000000000");
    assert_eq!(op(n("2"), "^", i(-2)), "0.2500000000000000");
    assert_eq!(op(n("1.1"), "^", i(2)), "1.2100000000000000");
    assert_eq!(op(n("100.0"), "^", n("0.5")), "10.000000000000000");
    assert_eq!(op(i(3), "^", n("0.5")), "1.7320508075688773");
    assert_eq!(op(n("0.5"), "^", n("0.5")), "0.7071067811865475");
    assert_eq!(pre("-", i(5)), "-5");
    assert_eq!(pre("-", i(i32::MIN)), "ERR:22003");
    assert_eq!(pre("@", i(-5)), "5");
    assert_eq!(pre("|/", n("16.0")), "4");
    assert_eq!(pre("~", i(5)), "-6");
    assert_eq!(op(i(5), "&", i(3)), "1");
    assert_eq!(op(i(5), "|", i(3)), "7");
    assert_eq!(op(i(5), "#", i(3)), "6");
    assert_eq!(op(i(1), "<<", i(4)), "16");
    assert_eq!(op(i(100), ">>", i(2)), "25");

    assert_eq!(op_type(Int4, "+", Numeric), "numeric");
    assert_eq!(op_type(Int2, "+", Int2), "smallint");
    assert_eq!(op_type(Int2, "+", Int4), "integer");
    assert_eq!(op_type(Int8, "+", Int4), "bigint");
    assert_eq!(op_type(Float4, "*", Int4), "double precision");
    assert_eq!(op_type(Float4, "+", Numeric), "double precision");
    assert_eq!(op_type(Float4, "+", Float4), "real");
    assert_eq!(op_type(Int4, "/", Int4), "integer");
    assert_eq!(op_type(Int4, "^", Int4), "double precision");
    assert_eq!(op_type(Date, "-", Date), "integer");
    assert_eq!(op_type(TimestampTz, "-", TimestampTz), "interval");
    assert_eq!(op_type(Date, "+", Interval), "timestamp without time zone");
    assert_eq!(op_type(Numeric.array_of(), "||", Int4), "numeric[]");
    assert_eq!(op_type(Int4, "||", Text), "text");
    assert_eq!(op_type(Int4.array_of(), "@>", Text.array_of()), "ERR:42883");
}

#[test]
fn comparisons() {
    assert_eq!(op(t("b"), ">", t("a")), "t");
    assert_eq!(op(t("B"), "<", t("a")), "t");
    assert_eq!(op(t("a"), "<", t("ab")), "t");
    assert_eq!(op(n("1.0"), "=", i(1)), "t");
    assert_eq!(op((Value::Int2(1), Int2), "=", (Value::Int8(1), Int8)), "t");
    assert_eq!(op(p("1.5", Float4), "=", f(1.5)), "t");
    assert_eq!(op(p("0.1", Float4), "=", f(0.1)), "f");
    assert_eq!(op(f(f64::NAN), ">", f(1e308)), "t");
    assert_eq!(op(f(f64::NAN), "=", f(f64::NAN)), "t");
    assert_eq!(op(b(true), ">", b(false)), "t");
    assert_eq!(op(p("\\x01", Bytea), "<", p("\\x02", Bytea)), "t");
    assert_eq!(
        op(p("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", Uuid), "=", u("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11")),
        "t"
    );
    assert_eq!(op(p("10:00", Time), "<", p("11:00", Time)), "t");
    assert_eq!(op(i(1), "=", u("1")), "t");
    assert_eq!(op(i(1), "=", u("a")), "ERR:22P02");
    assert_eq!(op(i(1), "!=", i(2)), "t");
    assert_eq!(op(u("2020-01-01"), "<", date("2020-01-02")), "t");
    assert_eq!(op(date("2024-03-15"), "<", ts("2024-03-15 00:00:01")), "t");
    assert_eq!(op(iv("1 month"), "=", iv("30 days")), "t");
    assert_eq!(op(iv("1 day"), ">", iv("23 hours")), "t");
    assert_eq!(op(ts("infinity"), ">", ts("2024-01-01")), "t");
    assert_eq!(op(i(1), "=", nul(Int4)), "NULL");
    assert_eq!(op(u("a"), "=", u("b")), "f");
    assert_eq!(op(i(1), "=", t("a")), "ERR:42883");
    // Enums compare by position; an unknown literal adopts the enum type.
    let mood = Enum("mood".into());
    let f = resolve_operator("<", Some(&mood), &mood).unwrap();
    assert_eq!(
        (f.eval)(&ctx(), &[Value::Enum(0, "sad".into()), Value::Enum(1, "ok".into())]).unwrap(),
        Value::Bool(true)
    );
    let f = resolve_operator("=", Some(&mood), &Unknown).unwrap();
    assert_eq!(f.args, vec![mood.clone(), mood.clone()]);
    assert!(resolve_operator("=", Some(&mood), &Text).is_err());
    assert_eq!(op(arr("{1,2}", Int4), "=", arr("{1,2}", Int4)), "t");
    assert_eq!(op(arr("{1,2}", Int4), "<", arr("{1,3}", Int4)), "t");
    assert_eq!(op(arr("{1,2,3}", Int4), "<", arr("{1,3}", Int4)), "t");
    assert_eq!(op(jb(r#"{"a":1}"#), "=", jb(r#"{"a":1.0}"#)), "t");
    assert_eq!(op(jb(r#"{"a":1}"#), "<", jb(r#"{"a":2}"#)), "t");
}

#[test]
fn string_operators() {
    assert_eq!(op(b(true), "||", u("x")), "truex");
    assert_eq!(op(n("1.50"), "||", u("x")), "1.50x");
    assert_eq!(op(u("a"), "||", i(1)), "a1");
    assert_eq!(op(u("x"), "||", date("2020-01-01")), "x2020-01-01");
    assert_eq!(op(f(1.5), "||", u("a")), "1.5a");
    assert_eq!(op(t("a"), "||", nul(Text)), "NULL");
    assert_eq!(op(u("a"), "||", u("b")), "ab");
    assert_eq!(op(t("abc"), "~", u("^a")), "t");
    assert_eq!(op(t("abc"), "~*", u("B")), "t");
    assert_eq!(op(t("abc"), "!~", u("z")), "t");
    assert_eq!(op(t("a"), "~", u("(")), "ERR:2201B");
    assert_eq!(op(t("abc"), "~~", u("a%")), "t");
    assert_eq!(op(t("abc"), "~~", u("_b_")), "t");
    assert_eq!(op(t("abc"), "~~", u("a\\%")), "f");
    assert_eq!(op(t("a%c"), "~~", u("a\\%c")), "t");
    assert_eq!(op(t("ABC"), "~~*", u("a%")), "t");
    assert_eq!(op(t("abc"), "!~~", u("b%")), "t");
    assert_eq!(op(t("abc"), "~~", u("a\\")), "ERR:22025");
    assert_eq!(op(t("héllo"), "~~", u("h_llo")), "t");
    assert_eq!(op(t("abcabc"), "~~", u("%bc%bc")), "t");
    assert_eq!(op(t("abc"), "~~", u("%")), "t");
    assert_eq!(op(t(""), "~~", u("_")), "f");
    // SIMILAR TO and LIKE ... ESCAPE as the parser rewrites them.
    let sim = |s: &str, pat: &str| {
        let re = func("similar_to_escape", vec![t(pat)]);
        op(t(s), "~", t(&re))
    };
    assert_eq!(sim("abc", "(a|b)%"), "t");
    assert_eq!(sim("abc", "ab"), "f");
    assert_eq!(sim("a.c", "a.c"), "t");
    assert_eq!(sim("abc", "a.c"), "f");
    let pat = func("like_escape", vec![t("a!_c"), t("!")]);
    assert_eq!(op(t("a_c"), "~~", t(&pat)), "t");
    assert_eq!(op(t("abc"), "~~", t(&pat)), "f");
}

#[test]
fn string_functions() {
    assert_eq!(func("length", vec![t("héllo")]), "5");
    assert_eq!(func("octet_length", vec![t("héllo")]), "6");
    assert_eq!(func("bit_length", vec![t("ab")]), "16");
    assert_eq!(func("lower", vec![t("ÀbC")]), "àbc");
    assert_eq!(func("initcap", vec![t("hello wORLD foo_bar 3d")]), "Hello World Foo_Bar 3d");
    assert_eq!(func("btrim", vec![t("  x  ")]), "x");
    assert_eq!(func("btrim", vec![t("xxaxx"), t("x")]), "a");
    assert_eq!(func("ltrim", vec![t("  a ")]), "a ");
    assert_eq!(func("rtrim", vec![t("abcc"), t("c")]), "ab");
    assert_eq!(func("substring", vec![t("hello"), i(2), i(3)]), "ell");
    assert_eq!(func("substring", vec![t("hello"), i(0), i(3)]), "he");
    assert_eq!(func("substring", vec![t("hello"), i(-1)]), "hello");
    assert_eq!(func("substr", vec![t("hello"), i(4)]), "lo");
    assert_eq!(func("substring", vec![t("foobar"), u("o(.)b")]), "o");
    assert_eq!(func("substring", vec![t("foobar"), u("o.b")]), "oob");
    assert_eq!(func("substring", vec![t("abc"), i(1), i(-1)]), "ERR:22011");
    assert_eq!(func("position", vec![t("hello"), t("lo")]), "4");
    assert_eq!(func("strpos", vec![t("hello"), t("z")]), "0");
    assert_eq!(func("replace", vec![t("aXbXc"), t("X"), t("--")]), "a--b--c");
    assert_eq!(func("translate", vec![t("12345"), t("143"), t("ax")]), "a2x5");
    assert_eq!(func("concat", vec![u("a"), i(1), nul(Unknown), b(true), n("1.5")]), "a1t1.5");
    assert_eq!(func("concat_ws", vec![u("-"), u("a"), nul(Unknown), u("b"), i(2)]), "a-b-2");
    assert_eq!(func("left", vec![t("hello"), i(2)]), "he");
    assert_eq!(func("left", vec![t("hello"), i(-2)]), "hel");
    assert_eq!(func("right", vec![t("hello"), i(2)]), "lo");
    assert_eq!(func("right", vec![t("hello"), i(-2)]), "llo");
    assert_eq!(func("lpad", vec![t("5"), i(3), t("0")]), "005");
    assert_eq!(func("lpad", vec![t("hello"), i(3)]), "hel");
    assert_eq!(func("rpad", vec![t("ab"), i(5), t("xy")]), "abxyx");
    assert_eq!(func("repeat", vec![t("ab"), i(3)]), "ababab");
    assert_eq!(func("repeat", vec![t("a"), i(-1)]), "");
    assert_eq!(func("reverse", vec![t("héllo")]), "olléh");
    assert_eq!(func("split_part", vec![t("a,b,c"), t(","), i(2)]), "b");
    assert_eq!(func("split_part", vec![t("a,b,c"), t(","), i(-1)]), "c");
    assert_eq!(func("split_part", vec![t("a,b,c"), t(","), i(5)]), "");
    assert_eq!(func("split_part", vec![t("a,b"), t(","), i(0)]), "ERR:22023");
    assert_eq!(func("starts_with", vec![t("hello"), t("he")]), "t");
    assert_eq!(func("ascii", vec![t("A")]), "65");
    assert_eq!(func("chr", vec![i(8364)]), "€");
    assert_eq!(func("chr", vec![i(0)]), "ERR:54000");
    assert_eq!(func("md5", vec![t("hello")]), "5d41402abc4b2a76b9719d911017c592");
    assert_eq!(func("md5", vec![t("")]), "d41d8cd98f00b204e9800998ecf8427e");
    assert_eq!(func("encode", vec![p("\\x00ff41", Bytea), t("hex")]), "00ff41");
    assert_eq!(func("encode", vec![p("hello world", Bytea), t("base64")]), "aGVsbG8gd29ybGQ=");
    assert_eq!(func("encode", vec![p("\\x00ff5c41", Bytea), t("escape")]), "\\000\\377\\\\A");
    assert_eq!(func("decode", vec![t("aGVsbG8="), t("base64")]), "\\x68656c6c6f");
    assert_eq!(func("decode", vec![t("4142"), t("hex")]), "\\x4142");
    assert_eq!(func("decode", vec![t("zz"), t("hex")]), "ERR:22023");
    assert_eq!(func("to_hex", vec![i(255)]), "ff");
    assert_eq!(func("to_hex", vec![i(-1)]), "ffffffff");
    assert_eq!(func("format", vec![u("%s-%s"), u("a"), i(1)]), "a-1");
    assert_eq!(func("format", vec![u("%I %L %%"), u("my col"), u("it's")]), "\"my col\" 'it''s' %");
    assert_eq!(func("format", vec![u("%2$s %1$s"), u("a"), u("b")]), "b a");
    assert_eq!(func("format", vec![u("[%5s] [%-5s]"), u("ab"), u("cd")]), "[   ab] [cd   ]");
    assert_eq!(func("format", vec![u("%L"), nul(Unknown)]), "NULL");
    assert_eq!(func("format", vec![u("%s"), nul(Unknown)]), "");
    assert_eq!(func("format", vec![u("%s")]), "ERR:22023");
    assert_eq!(func("quote_ident", vec![t("abc")]), "abc");
    assert_eq!(func("quote_ident", vec![t("Abc")]), "\"Abc\"");
    assert_eq!(func("quote_ident", vec![t("select")]), "\"select\"");
    assert_eq!(func("quote_literal", vec![t("it's")]), "'it''s'");
    assert_eq!(func("quote_literal", vec![t("a\\b")]), "E'a\\\\b'");
    assert_eq!(func("quote_nullable", vec![nul(Text)]), "NULL");
    assert_eq!(func("quote_literal", vec![i(42)]), "'42'");
    assert_eq!(func("regexp_replace", vec![t("foobarbaz"), t("b.."), t("X")]), "fooXbaz");
    assert_eq!(func("regexp_replace", vec![t("foobarbaz"), t("b.."), t("X"), t("g")]), "fooXX");
    assert_eq!(func("regexp_replace", vec![t("Hello"), t("(l+)"), t("<\\1>")]), "He<ll>o");
    assert_eq!(func("regexp_replace", vec![t("abc"), t("B"), t("x"), t("i")]), "axc");
    assert_eq!(func("regexp_match", vec![t("foobarbequebaz"), t("(bar)(beque)")]), "{bar,beque}");
    assert_eq!(func("regexp_match", vec![t("abc"), t("x")]), "NULL");
    assert_eq!(func("regexp_match", vec![t("abc"), t("b")]), "{b}");
    assert_eq!(func("regexp_like", vec![t("Hello"), t("^h"), t("i")]), "t");
    assert_eq!(func("regexp_count", vec![t("abcabc"), t("b")]), "2");
    assert_eq!(func("regexp_split_to_array", vec![t("a1b22c"), t("[0-9]+")]), "{a,b,c}");
    assert_eq!(func("overlay", vec![t("Txxxxas"), t("hom"), i(2), i(4)]), "Thomas");
    assert_eq!(func("overlay", vec![t("abcdef"), t("XY"), i(3)]), "abXYef");
    assert_eq!(func("string_to_array", vec![t("a,b,,c"), t(",")]), "{a,b,\"\",c}");
    assert_eq!(func("string_to_array", vec![t("a,b"), t(","), t("b")]), "{a,NULL}");
    assert_eq!(func("string_to_array", vec![t("abc"), nul(Text)]), "{a,b,c}");
    assert_eq!(func("string_to_array", vec![t(""), t(",")]), "{}");
    assert_eq!(func("length", vec![i(1)]), "ERR:42883");
    assert_eq!(func("foo", vec![i(1), u("a")]), "ERR:42883");
    assert_eq!(
        resolve_function("foo", &[Int4, Unknown]).unwrap_err().message(),
        "function foo(integer, unknown) does not exist"
    );
    assert!(function_exists("LOWER") && !function_exists("nope"));
}

#[test]
fn math_functions() {
    assert_eq!(func("abs", vec![i(-5)]), "5");
    assert_eq!(func("abs", vec![n("-5.5")]), "5.5");
    assert_eq!(func("ceil", vec![n("1.2")]), "2");
    assert_eq!(func("ceil", vec![n("-1.2")]), "-1");
    assert_eq!(func("floor", vec![n("-1.2")]), "-2");
    assert_eq!(func("floor", vec![f(1.9)]), "1");
    assert_eq!(func("round", vec![n("2.5")]), "3");
    assert_eq!(func("round", vec![n("-2.5")]), "-3");
    assert_eq!(func("round", vec![f(2.5)]), "2");
    assert_eq!(func("round", vec![n("1.2345"), i(2)]), "1.23");
    assert_eq!(func("round", vec![n("1234.5"), i(-2)]), "1200");
    assert_eq!(func("round", vec![n("1.5"), i(3)]), "1.500");
    assert_eq!(func("trunc", vec![n("1.99")]), "1");
    assert_eq!(func("trunc", vec![n("-1.99")]), "-1");
    assert_eq!(func("trunc", vec![n("1.2399"), i(2)]), "1.23");
    assert_eq!(func("trunc", vec![n("1299.9"), i(-2)]), "1200");
    assert_eq!(func("sign", vec![n("-3.2")]), "-1");
    assert_eq!(func("sign", vec![f(0.0)]), "0");
    assert_eq!(func("sqrt", vec![i(16)]), "4");
    assert_eq!(func("sqrt", vec![n("2.0")]), "1.414213562373095");
    assert_eq!(func("sqrt", vec![n("0.0001")]), "0.01000000000000000");
    assert_eq!(func("sqrt", vec![n("123456789.123")]), "11111.11106609056");
    assert_eq!(func("sqrt", vec![i(-1)]), "ERR:2201F");
    assert_eq!(func("power", vec![i(2), i(10)]), "1024");
    assert_eq!(func("power", vec![n("2.0"), i(3)]), "8.0000000000000000");
    assert_eq!(func("power", vec![n("2.5"), n("2.5")]), "9.8821176880261854");
    assert_eq!(func("power", vec![n("10.0"), i(20)]), "100000000000000000000.0");
    assert_eq!(func("power", vec![n("1.5"), i(-3)]), "0.2962962962962963");
    assert_eq!(func("power", vec![n("0"), i(-1)]), "ERR:2201F");
    assert_eq!(func("power", vec![f(-8.0), n("0.5")]), "ERR:2201F");
    assert_eq!(func("exp", vec![i(1)]), "2.718281828459045");
    assert_eq!(func("exp", vec![n("1.0")]), "2.7182818284590452");
    assert_eq!(func("exp", vec![n("2.50")]), "12.182493960703473");
    assert_eq!(func("exp", vec![n("-1.0")]), "0.3678794411714423");
    assert_eq!(func("exp", vec![n("10.0")]), "22026.465794806717");
    assert_eq!(func("ln", vec![i(10)]), "2.302585092994046");
    assert_eq!(func("ln", vec![n("10.0")]), "2.3025850929940457");
    assert_eq!(func("ln", vec![n("0.5")]), "-0.6931471805599453");
    assert_eq!(func("ln", vec![n("123456.789")]), "11.723646487185881");
    assert_eq!(func("ln", vec![i(0)]), "ERR:2201E");
    assert_eq!(func("ln", vec![n("-1")]), "ERR:2201E");
    assert_eq!(func("log", vec![i(100)]), "2");
    assert_eq!(func("log", vec![n("100.0")]), "2.0000000000000000");
    assert_eq!(func("log", vec![n("2.0"), i(8)]), "3.0000000000000000");
    assert_eq!(func("log", vec![i(2), i(1024)]), "10.0000000000000000");
    assert_eq!(func("log10", vec![i(1000)]), "3");
    assert_eq!(func("mod", vec![i(7), i(3)]), "1");
    assert_eq!(func("mod", vec![i(-7), i(3)]), "-1");
    assert_eq!(func("mod", vec![n("7.5"), i(2)]), "1.5");
    assert_eq!(func("div", vec![i(7), i(2)]), "3");
    assert_eq!(func("div", vec![n("-7.5"), i(2)]), "-3");
    assert_eq!(func("gcd", vec![i(12), i(18)]), "6");
    assert_eq!(func("lcm", vec![i(4), i(6)]), "12");
    assert_eq!(func("pi", vec![]), "3.141592653589793");
    assert_eq!(func("degrees", vec![f(std::f64::consts::PI)]), "180");
    assert_eq!(func("atan2", vec![i(1), i(1)]), "0.7853981633974483");
    assert_eq!(func("width_bucket", vec![n("5.35"), n("0.024"), n("10.06"), i(5)]), "3");
    assert_eq!(func("scale", vec![n("1.230")]), "3");
    assert_eq!(func("min_scale", vec![n("1.230")]), "2");
    assert_eq!(func("trim_scale", vec![n("1.230")]), "1.23");
    let r: f64 = func("random", vec![]).parse().unwrap();
    assert!((0.0..1.0).contains(&r));

    assert_eq!(fn_type("round", &[Float8]), "double precision");
    assert_eq!(fn_type("round", &[Int4]), "double precision");
    assert_eq!(fn_type("abs", &[Int2]), "smallint");
    assert_eq!(fn_type("mod", &[Int4, Int4]), "integer");
    assert_eq!(fn_type("power", &[Int4, Int4]), "double precision");
    assert_eq!(fn_type("sqrt", &[Int4]), "double precision");
    assert_eq!(fn_type("floor", &[Numeric]), "numeric");
    assert_eq!(fn_type("ceil", &[Int4]), "double precision");
    assert_eq!(fn_type("trunc", &[Numeric, Int4]), "numeric");
    assert_eq!(fn_type("div", &[Int4, Int4]), "numeric");
    assert_eq!(fn_type("length", &[Unknown]), "integer");
    assert_eq!(fn_type("extract", &[Unknown, TimestampTz]), "numeric");
    assert_eq!(fn_type("date_part", &[Unknown, TimestampTz]), "double precision");
}

#[test]
fn datetime_arithmetic() {
    assert_eq!(op(ts("2024-01-31 10:00"), "+", iv("1 month")), "2024-02-29 10:00:00");
    assert_eq!(op(ts("2024-03-15"), "-", iv("1 year 2 days 3 hours")), "2023-03-12 21:00:00");
    assert_eq!(op(tstz("2024-03-15 12:00+00"), "+", iv("1 day")), "2024-03-16 12:00:00+00");
    assert_eq!(op(ts("2024-03-15 12:00"), "-", ts("2024-03-10 08:30")), "5 days 03:30:00");
    assert_eq!(op(ts("2020-01-01"), "-", ts("2019-12-25 12:00")), "6 days 12:00:00");
    assert_eq!(op(date("2024-03-15"), "+", p("10:30", Time)), "2024-03-15 10:30:00");
    assert_eq!(op(date("2024-03-15"), "-", iv("1 hour")), "2024-03-14 23:00:00");
    assert_eq!(op(date("2020-01-31"), "+", iv("1 month")), "2020-02-29 00:00:00");
    assert_eq!(op(date("2020-03-01"), "-", date("2020-02-01")), "29");
    assert_eq!(op(date("2020-01-01"), "+", i(5)), "2020-01-06");
    assert_eq!(op(date("2024-03-15"), "-", i(10)), "2024-03-05");
    assert_eq!(op(date("2024-03-15"), "-", u("2024-03-01")), "14");
    assert_eq!(op(iv("1 day"), "+", iv("2 hours")), "1 day 02:00:00");
    assert_eq!(op(iv("1 day"), "-", iv("25 hours")), "1 day -25:00:00");
    assert_eq!(op(i(2), "*", iv("1 hour 30 min")), "03:00:00");
    assert_eq!(op(iv("1 day"), "*", n("1.5")), "1 day 12:00:00");
    assert_eq!(op(iv("1 month"), "*", n("1.5")), "1 mon 15 days");
    assert_eq!(op(iv("1 hour"), "/", i(3)), "00:20:00");
    assert_eq!(op(iv("1 mon 10 days"), "/", i(4)), "10 days");
    assert_eq!(op(iv("1 year"), "/", i(2)), "6 mons");
    assert_eq!(op(p("10:00", Time), "+", iv("15 hours")), "01:00:00");
    assert_eq!(op(p("23:00", Time), "+", iv("2 hours")), "01:00:00");
    assert_eq!(op(p("10:00", Time), "-", p("11:30", Time)), "-01:30:00");
    assert_eq!(pre("-", iv("1 day 2 hours")), "-1 days -02:00:00");
    assert_eq!(op(ts("2024-03-15 12:00"), ">", u("2024-03-15 11:59:59")), "t");
    // Month/day steps follow the session zone's wall clock across DST.
    let ny = ctx_tz("America/New_York");
    let at = |text: &str| (parse_value(text, &TimestampTz, &ny).unwrap(), TimestampTz);
    assert_eq!(op_in(&ny, at("2021-03-13 02:30:00"), "+", iv("1 day")), "2021-03-14 03:30:00-04");
    assert_eq!(op_in(&ny, at("2021-03-13 12:00:00"), "+", iv("24 hours")), "2021-03-14 13:00:00-04");
    assert_eq!(func("age", vec![ts("2021-03-01"), ts("2020-01-31")]), "1 year 1 mon 1 day");
    assert_eq!(func("age", vec![ts("2020-01-31"), ts("2021-03-01 12:00")]), "-1 years -1 mons -1 days -12:00:00");
    assert_eq!(func("age", vec![date("2024-03-15"), date("2000-01-01")]), "24 years 2 mons 14 days");
    assert_eq!(func("justify_hours", vec![iv("50 hours")]), "2 days 02:00:00");
    assert_eq!(func("justify_days", vec![iv("65 days")]), "2 mons 5 days");
    assert_eq!(func("justify_interval", vec![iv("1 mon -1 hour")]), "29 days 23:00:00");
}

#[test]
fn datetime_functions() {
    assert_eq!(func("now", vec![]), "2024-03-15 12:30:45.123456+00");
    assert_eq!(func("current_date", vec![]), "2024-03-15");
    assert_eq!(func("localtimestamp", vec![i(0)]), "2024-03-15 12:30:45");
    assert_eq!(func("date_trunc", vec![u("hour"), ts("2024-03-15 12:34:56.789")]), "2024-03-15 12:00:00");
    assert_eq!(func("date_trunc", vec![u("week"), ts("2024-03-15 12:34:56")]), "2024-03-11 00:00:00");
    assert_eq!(func("date_trunc", vec![u("quarter"), ts("2024-05-15")]), "2024-04-01 00:00:00");
    assert_eq!(func("date_trunc", vec![u("month"), tstz("2024-03-15 12:00+00")]), "2024-03-01 00:00:00+00");
    assert_eq!(func("date_trunc", vec![u("decade"), ts("2024-03-15")]), "2020-01-01 00:00:00");
    assert_eq!(func("date_trunc", vec![u("century"), ts("2024-03-15")]), "2001-01-01 00:00:00");
    assert_eq!(func("date_trunc", vec![u("millennium"), ts("2024-03-15")]), "2001-01-01 00:00:00");
    assert_eq!(func("date_trunc", vec![u("hour"), iv("1 day 02:30:00")]), "1 day 02:00:00");
    assert_eq!(
        func("date_trunc", vec![u("day"), tstz("2024-03-15 03:00+00"), u("America/New_York")]),
        "2024-03-14 04:00:00+00"
    );
    assert_eq!(func("date_trunc", vec![u("year"), date("2024-03-15")]), "2024-01-01 00:00:00+00");
    assert_eq!(func("date_trunc", vec![u("foo"), tstz("2024-03-15 03:00+00")]), "ERR:22023");

    let ex = |field: &str, a: A| func("extract", vec![u(field), a]);
    assert_eq!(ex("epoch", date("2020-01-01")), "1577836800");
    assert_eq!(ex("epoch", ts("2020-01-01 10:00:01.5")), "1577872801.500000");
    assert_eq!(ex("second", ts("2020-01-01 10:00:01.5")), "1.500000");
    assert_eq!(ex("year", date("2020-01-01")), "2020");
    assert_eq!(ex("milliseconds", ts("2020-01-01 10:00:01.5")), "1500.000");
    assert_eq!(ex("epoch", iv("1 year 1 day")), "31644000.000000");
    assert_eq!(ex("dow", date("2024-03-03")), "0");
    assert_eq!(ex("week", date("2021-01-03")), "53");
    assert_eq!(ex("century", date("2000-12-31")), "20");
    assert_eq!(ex("decade", date("1999-12-31")), "199");
    assert_eq!(ex("hour", p("10:11:12.5", Time)), "10");
    assert_eq!(ex("epoch", tstz("2020-01-01 00:00+02")), "1577829600.000000");
    assert_eq!(ex("julian", date("2020-01-01")), "2458850");
    assert_eq!(ex("quarter", iv("7 months")), "3");
    assert_eq!(ex("isodow", date("2024-03-17")), "7");
    assert_eq!(ex("isoyear", date("2021-01-03")), "2020");
    assert_eq!(ex("microseconds", p("12:34:56.789", Time)), "56789000");
    assert_eq!(ex("month", iv("14 months")), "2");
    assert_eq!(ex("minute", iv("3 hours 25 min")), "25");
    assert_eq!(ex("millennium", date("2001-01-01")), "3");
    assert_eq!(ex("century", date("0001-01-01 BC")), "-1");
    assert_eq!(ex("year", date("0044-03-15 BC")), "-44");
    assert_eq!(ex("timezone_hour", tstz("2024-03-15 12:00+00")), "0");
    assert_eq!(ex("foo", tstz("2024-03-15 12:00+00")), "ERR:22023");
    assert_eq!(ex("hour", date("2020-01-01")), "ERR:0A000");
    assert_eq!(func("date_part", vec![u("year"), ts("2024-03-15 12:34:56.789")]), "2024");
    assert_eq!(func("date_part", vec![u("second"), ts("2024-03-15 12:34:56.789")]), "56.789");
    assert_eq!(func("date_part", vec![u("dow"), date("2024-03-15")]), "5");
    assert_eq!(func("date_part", vec![u("doy"), date("2024-03-15")]), "75");
    assert_eq!(func("date_part", vec![u("epoch"), iv("1 day")]), "86400");

    assert_eq!(func("make_date", vec![i(2024), i(2), i(29)]), "2024-02-29");
    assert_eq!(func("make_time", vec![i(8), i(15), n("23.5")]), "08:15:23.5");
    assert_eq!(
        func("make_timestamp", vec![i(2024), i(3), i(15), i(12), i(34), n("56.789")]),
        "2024-03-15 12:34:56.789"
    );
    assert_eq!(
        func("make_timestamptz", vec![i(2024), i(3), i(15), i(12), i(0), i(0), u("America/New_York")]),
        "2024-03-15 16:00:00+00"
    );
    assert_eq!(
        func("make_interval", vec![i(1), i(2), i(3), i(4), i(5), i(6), n("7.5")]),
        "1 year 2 mons 25 days 05:06:07.5"
    );
    assert_eq!(func("timezone", vec![u("America/New_York"), ts("2024-03-15 12:00")]), "2024-03-15 16:00:00+00");
    assert_eq!(func("timezone", vec![u("America/New_York"), tstz("2024-03-15 12:00+00")]), "2024-03-15 08:00:00");
    assert_eq!(func("timezone", vec![u("UTC"), ts("2024-03-15 12:00")]), "2024-03-15 12:00:00+00");
    assert_eq!(func("timezone", vec![u("+05"), tstz("2024-03-15 12:00+00")]), "2024-03-15 07:00:00");
    assert_eq!(func("timezone", vec![u("Asia/Kolkata"), tstz("2024-03-15 12:00+00")]), "2024-03-15 17:30:00");
    assert_eq!(func("timezone", vec![u("Nope/Zone"), tstz("2024-03-15 12:00+00")]), "ERR:22023");
    assert_eq!(func("isfinite", vec![ts("infinity")]), "f");
    assert_eq!(func("isfinite", vec![date("2024-01-01")]), "t");
    assert_eq!(
        func("date_bin", vec![u("15 minutes"), ts("2024-03-15 12:34:56"), ts("2001-01-01")]),
        "2024-03-15 12:30:00"
    );
    assert_eq!(func("to_timestamp", vec![i(0)]), "1970-01-01 00:00:00+00");
    assert_eq!(func("to_timestamp", vec![n("1710505845.5")]), "2024-03-15 12:30:45.5+00");
    assert_eq!(
        func("to_timestamp", vec![u("2024-03-15 12:34:56"), u("YYYY-MM-DD HH24:MI:SS")]),
        "2024-03-15 12:34:56+00"
    );
    assert_eq!(
        func("to_timestamp", vec![u("15 Mar 2024 02:05 PM"), u("DD Mon YYYY HH12:MI AM")]),
        "2024-03-15 14:05:00+00"
    );
    assert_eq!(func("to_date", vec![u("20240315"), u("YYYYMMDD")]), "2024-03-15");
    assert_eq!(func("to_date", vec![u("15/03/24"), u("DD/MM/YY")]), "2024-03-15");
    assert_eq!(func("to_date", vec![u("March 5, 2024"), u("Month DD, YYYY")]), "2024-03-05");
    assert_eq!(func("to_date", vec![u("2020-13-45"), u("YYYY-MM-DD")]), "ERR:22008");
    assert_eq!(func("to_number", vec![u("1,234.56"), u("9,999.99")]), "1234.56");
    assert_eq!(func("to_number", vec![u("-12"), u("S99")]), "-12");
}

#[test]
fn to_char() {
    let tc = |a: A, fmt: &str| func("to_char", vec![a, u(fmt)]);
    let d = || ts("2024-03-05 14:07:09.123456");
    assert_eq!(tc(d(), "YYYY-MM-DD HH24:MI:SS.US"), "2024-03-05 14:07:09.123456");
    assert_eq!(tc(d(), "DD Mon YYYY, HH12:MI AM"), "05 Mar 2024, 02:07 PM");
    assert_eq!(tc(d(), "Day, DDth Month YYYY"), "Tuesday  , 05th March     2024");
    assert_eq!(tc(d(), "FMDay, FMDD FMMonth YY"), "Tuesday, 5 March 24");
    assert_eq!(tc(ts("2024-03-05 04:07:09"), "HH:MI:SS a.m. Dy DY dy MON"), "04:07:09 a.m. Tue TUE tue MAR");
    assert_eq!(tc(ts("2024-03-05 14:07:09.5"), "MS \"ms\" Q WW IW DDD D ID J CC"), "500 ms 1 10 10 065 3 2 2460375 21");
    assert_eq!(tc(tstz("2024-03-05 14:07:09+00"), "YYYY-MM-DD\"T\"HH24:MI:SSOF TZ"), "2024-03-05T14:07:09+00 UTC");
    assert_eq!(tc(iv("26 hours 3 minutes"), "HH24:MI:SS"), "26:03:00");
    assert_eq!(tc(ts("2024-03-05 00:07:09"), "HH12 FMHH12 Y,YYY YYY Y RM"), "12 12 2,024 024 4 III ");

    assert_eq!(tc(n("1234.5"), "9999.99"), " 1234.50");
    assert_eq!(tc(n("-1234.5"), "9999.99"), "-1234.50");
    assert_eq!(tc(n("1234.5"), "9,999.99"), " 1,234.50");
    assert_eq!(tc(i(12), "0000"), " 0012");
    assert_eq!(tc(i(12), "FM0000"), "0012");
    assert_eq!(tc(n("12.5"), "FM999.00"), "12.50");
    assert_eq!(tc(n("0.5"), "99.99"), "   .50");
    assert_eq!(tc(i(0), "99"), "  0");
    assert_eq!(tc(i(0), "99.99"), "   .00");
    assert_eq!(tc(i(12345), "999"), " ###");
    assert_eq!(tc(i(-12), "S99"), "-12");
    assert_eq!(tc(i(12), "S99"), "+12");
    assert_eq!(tc(i(-12), "99MI"), "12-");
    assert_eq!(tc(i(-12), "99PR"), "<12>");
    assert_eq!(tc(n("1234567.891"), "FM9,999,999.99"), "1,234,567.89");
    assert_eq!(tc(i(12), "999"), "  12");
    assert_eq!(tc(f(1.5), "9.9"), " 1.5");
    assert_eq!(tc(n("0.5"), "FM0.00"), "0.50");
    assert_eq!(tc(i(5), "FM99"), "5");
    assert_eq!(tc(n("12.3"), "FM999.999"), "12.3");
}

#[test]
fn json_functions() {
    assert_eq!(func("to_json", vec![t("a\"b")]), "\"a\\\"b\"");
    assert_eq!(func("to_json", vec![n("1.50")]), "1.50");
    assert_eq!(func("to_json", vec![arr("{1,2,NULL}", Int4)]), "[1,2,null]");
    assert_eq!(func("to_json", vec![b(true)]), "true");
    assert_eq!(func("to_json", vec![ts("2024-03-15 12:34:56.5")]), "\"2024-03-15T12:34:56.5\"");
    assert_eq!(func("to_json", vec![tstz("2024-03-15 12:34:56+00")]), "\"2024-03-15T12:34:56+00:00\"");
    assert_eq!(func("to_json", vec![date("2024-03-15")]), "\"2024-03-15\"");
    assert_eq!(func("to_json", vec![jb(r#"{"a": 1}"#)]), r#"{"a": 1}"#);
    assert_eq!(func("to_json", vec![nul(Int4)]), "NULL");
    assert_eq!(func("to_json", vec![iv("1 day")]), "\"1 day\"");
    assert_eq!(func("to_json", vec![f(f64::NAN)]), "\"NaN\"");
    assert_eq!(func("to_json", vec![f(1e20)]), "1e+20");
    assert_eq!(func("to_jsonb", vec![t("x")]), "\"x\"");
    assert_eq!(func("to_jsonb", vec![n("1.5")]), "1.5");
    assert_eq!(func("to_jsonb", vec![f(1.0)]), "1");
    assert_eq!(func("to_jsonb", vec![arr("{a,b}", Text)]), r#"["a", "b"]"#);
    let row = (Value::Record(vec![Value::Int4(1), Value::text("a"), Value::Null]), Record);
    assert_eq!(func("row_to_json", vec![row]), r#"{"f1":1,"f2":"a","f3":null}"#);
    assert_eq!(func("array_to_json", vec![arr("{1,2}", Int4)]), "[1,2]");
    assert_eq!(
        func("json_build_object", vec![u("a"), i(1), u("b"), u("x"), i(3), nul(Unknown)]),
        r#"{"a" : 1, "b" : "x", "3" : null}"#
    );
    assert_eq!(func("jsonb_build_object", vec![u("b"), i(1), u("a"), arr("{1,2}", Int4)]), r#"{"a": [1, 2], "b": 1}"#);
    assert_eq!(
        func("json_build_array", vec![i(1), u("a"), nul(Unknown), b(true), n("1.5")]),
        r#"[1, "a", null, true, 1.5]"#
    );
    assert_eq!(func("jsonb_build_array", vec![i(1), u("a"), nul(Unknown)]), r#"[1, "a", null]"#);
    assert_eq!(func("json_build_object", vec![]), "{}");
    assert_eq!(func("jsonb_build_array", vec![]), "[]");
    assert_eq!(func("json_build_object", vec![u("a")]), "ERR:22023");
    assert_eq!(func("json_typeof", vec![u("[1]")]), "array");
    assert_eq!(func("jsonb_typeof", vec![u("1.5")]), "number");
    assert_eq!(func("jsonb_typeof", vec![u("null")]), "null");
    assert_eq!(func("json_array_length", vec![u("[1,2,3]")]), "3");
    assert_eq!(func("jsonb_array_length", vec![u("[]")]), "0");
    assert_eq!(func("jsonb_array_length", vec![jb("{}")]), "ERR:22023");
    assert_eq!(func("json_extract_path", vec![u(r#"{"a":{"b":1}}"#), u("a"), u("b")]), "1");
    assert_eq!(func("jsonb_extract_path_text", vec![u(r#"{"a":{"b":"x"}}"#), u("a"), u("b")]), "x");
    assert_eq!(func("jsonb_set", vec![u(r#"{"a":{"b":1}}"#), u("{a,b}"), u("2")]), r#"{"a": {"b": 2}}"#);
    assert_eq!(func("jsonb_set", vec![u(r#"{"a":1}"#), u("{b}"), u("2")]), r#"{"a": 1, "b": 2}"#);
    assert_eq!(func("jsonb_set", vec![u(r#"{"a":1}"#), u("{b}"), u("2"), b(false)]), r#"{"a": 1}"#);
    assert_eq!(func("jsonb_set", vec![u("[1,2,3]"), u("{1}"), u("\"x\"")]), r#"[1, "x", 3]"#);
    assert_eq!(func("jsonb_set", vec![u("[1,2,3]"), u("{10}"), u("4")]), "[1, 2, 3, 4]");
    assert_eq!(func("jsonb_set", vec![u(r#"{"a":1}"#), u("{x,y}"), u("1")]), r#"{"a": 1}"#);
    assert_eq!(func("jsonb_insert", vec![u("[1,2,3]"), u("{1}"), u("9")]), "[1, 9, 2, 3]");
    assert_eq!(func("jsonb_insert", vec![u("[1,2,3]"), u("{1}"), u("9"), b(true)]), "[1, 2, 9, 3]");
    assert_eq!(func("jsonb_insert", vec![u(r#"{"a":1}"#), u("{b}"), u("2")]), r#"{"a": 1, "b": 2}"#);
    assert_eq!(func("jsonb_insert", vec![u(r#"{"a":1}"#), u("{a}"), u("2")]), "ERR:22023");
    assert_eq!(
        func("jsonb_strip_nulls", vec![u(r#"{"a":null,"b":{"c":null,"d":1},"e":[null]}"#)]),
        r#"{"b": {"d": 1}, "e": [null]}"#
    );
    assert_eq!(func("json_strip_nulls", vec![u(r#"{"a":null,"b":1}"#)]), r#"{"b":1}"#);
    assert_eq!(
        func("jsonb_pretty", vec![u(r#"{"a":1,"b":[1,{"c":2}],"e":[],"o":{}}"#)]),
        "{\n    \"a\": 1,\n    \"b\": [\n        1,\n        {\n            \"c\": 2\n        }\n    ],\n    \"e\": [\n    ],\n    \"o\": {\n    }\n}"
    );
}

#[test]
fn json_operators() {
    let doc = || jb(r#"{"a":{"b":[1,2,{"c":"x"}]},"n":null,"s":"str"}"#);
    assert_eq!(op(doc(), "->", u("a")), r#"{"b": [1, 2, {"c": "x"}]}"#);
    assert_eq!(op(jb("[1,2,3]"), "->", i(0)), "1");
    assert_eq!(op(jb("[1,2,3]"), "->>", i(-1)), "3");
    assert_eq!(op(jb("[1,2,3]"), "->", i(5)), "NULL");
    assert_eq!(op(doc(), "->>", u("s")), "str");
    assert_eq!(op(doc(), "->>", u("n")), "NULL");
    assert_eq!(op(doc(), "->", u("n")), "null");
    assert_eq!(op(doc(), "#>", u("{a,b,2,c}")), "\"x\"");
    assert_eq!(op(doc(), "#>>", u("{a,b,2,c}")), "x");
    assert_eq!(op(doc(), "#>>", u("{a,z}")), "NULL");
    assert_eq!(op(p(r#"{"a":1,"b":2}"#, Json), "->", u("b")), "2");
    assert_eq!(op(jb(r#"{"a":{"x":1}}"#), "->>", u("a")), r#"{"x": 1}"#);
    assert_eq!(op(jb(r#"{"a":1,"b":[1,2,3]}"#), "@>", u(r#"{"b":[2]}"#)), "t");
    assert_eq!(op(jb(r#"{"a":1}"#), "@>", u(r#"{"a":2}"#)), "f");
    assert_eq!(op(jb("[1,2,3]"), "@>", u("2")), "t");
    assert_eq!(op(jb("[1,[2,3]]"), "@>", u("[[3]]")), "t");
    assert_eq!(op(jb("[1,2]"), "@>", u("[1.0]")), "t");
    assert_eq!(op(jb(r#"{"a":1}"#), "<@", u(r#"{"a":1,"b":2}"#)), "t");
    assert_eq!(op(jb(r#"{"a":1}"#), "?", u("a")), "t");
    assert_eq!(op(jb(r#"["x","y"]"#), "?", u("y")), "t");
    assert_eq!(op(jb(r#"{"a":1}"#), "?|", arr("{z,a}", Text)), "t");
    assert_eq!(op(jb(r#"{"a":1}"#), "?&", arr("{z,a}", Text)), "f");
    assert_eq!(op(jb(r#"{"a":1}"#), "||", u(r#"{"b":2,"a":3}"#)), r#"{"a": 3, "b": 2}"#);
    assert_eq!(op(jb("[1]"), "||", u("[2]")), "[1, 2]");
    assert_eq!(op(jb("[1]"), "||", u(r#"{"a":1}"#)), r#"[1, {"a": 1}]"#);
    assert_eq!(op(jb("1"), "||", u("2")), "[1, 2]");
    assert_eq!(op(jb(r#"{"a":1,"b":2}"#), "-", u("a")), r#"{"b": 2}"#);
    assert_eq!(op(jb(r#"["a","b"]"#), "-", t("a")), r#"["b"]"#);
    assert_eq!(op(jb("[1,2,3]"), "-", i(1)), "[1, 3]");
    assert_eq!(op(jb("[1,2,3]"), "-", i(-1)), "[1, 2]");
    assert_eq!(op(jb(r#"{"a":1,"b":2,"c":3}"#), "-", arr("{a,c}", Text)), r#"{"b": 2}"#);
    assert_eq!(op(jb(r#"{"a":{"b":1,"c":2}}"#), "#-", u("{a,b}")), r#"{"a": {"c": 2}}"#);
    assert_eq!(op(jb("1"), "-", t("a")), "ERR:22023");
}

#[test]
fn array_functions() {
    let a123 = || arr("{1,2,3}", Int4);
    assert_eq!(func("array_length", vec![a123(), i(1)]), "3");
    assert_eq!(func("array_length", vec![a123(), i(2)]), "NULL");
    assert_eq!(func("array_length", vec![arr("{}", Int4), i(1)]), "NULL");
    assert_eq!(func("cardinality", vec![arr("{}", Int4)]), "0");
    assert_eq!(func("array_upper", vec![a123(), i(1)]), "3");
    assert_eq!(func("array_lower", vec![a123(), i(1)]), "1");
    assert_eq!(func("array_ndims", vec![a123()]), "1");
    assert_eq!(func("array_dims", vec![a123()]), "[1:3]");
    assert_eq!(func("array_append", vec![arr("{1,2}", Int4), i(3)]), "{1,2,3}");
    assert_eq!(func("array_append", vec![nul(Int4.array_of()), i(1)]), "{1}");
    assert_eq!(func("array_prepend", vec![i(0), arr("{1}", Int4)]), "{0,1}");
    assert_eq!(func("array_cat", vec![arr("{1}", Int4), arr("{2,3}", Int4)]), "{1,2,3}");
    assert_eq!(func("array_position", vec![arr("{a,b,c}", Text), u("b")]), "2");
    assert_eq!(func("array_position", vec![arr("{1,2,1}", Int4), i(1), i(2)]), "3");
    assert_eq!(func("array_position", vec![arr("{1,NULL}", Int4), nul(Int4)]), "2");
    assert_eq!(func("array_positions", vec![arr("{1,2,1}", Int4), i(1)]), "{1,3}");
    assert_eq!(func("array_remove", vec![arr("{1,2,1,NULL}", Int4), i(1)]), "{2,NULL}");
    assert_eq!(func("array_replace", vec![arr("{1,2,1}", Int4), i(1), i(9)]), "{9,2,9}");
    assert_eq!(func("trim_array", vec![a123(), i(2)]), "{1}");
    assert_eq!(func("array_to_string", vec![arr("{1,2,NULL,3}", Int4), u(",")]), "1,2,3");
    assert_eq!(func("array_to_string", vec![arr("{1,NULL}", Int4), u(","), u("*")]), "1,*");
    assert_eq!(op(arr("{1,2}", Int4), "||", i(3)), "{1,2,3}");
    assert_eq!(op(i(0), "||", arr("{1}", Int4)), "{0,1}");
    assert_eq!(op(arr("{1}", Int4), "||", arr("{2}", Int4)), "{1,2}");
    assert_eq!(op(arr("{1}", Int4), "||", u("{2}")), "{1,2}");
    assert_eq!(op(arr("{a}", Text), "||", t("b")), "{a,b}");
    assert_eq!(op(arr("{1.5}", Numeric), "||", i(2)), "{1.5,2}");
    assert_eq!(op(u("a"), "||", arr("{1}", Int4)), "ERR:22P02");
    assert_eq!(op(arr("{1,2}", Int4), "@>", arr("{2}", Int4)), "t");
    assert_eq!(op(arr("{1,2}", Int4), "<@", arr("{2}", Int4)), "f");
    assert_eq!(op(arr("{1,2}", Int4), "&&", arr("{2,5}", Int4)), "t");
    assert_eq!(op(arr("{1,NULL}", Int4), "@>", arr("{NULL}", Int4)), "f");
    assert_eq!(fn_type("array_append", &[Int4.array_of(), Int4]), "integer[]");
    assert_eq!(fn_type("array_append", &[Numeric.array_of(), Int4]), "numeric[]");
    assert_eq!(fn_type("string_to_array", &[Text, Unknown]), "text[]");
}

#[test]
fn binary_round_trip() {
    let c = ctx();
    let cases: Vec<A> = vec![
        b(true),
        (Value::Int2(-7), Int2),
        i(123_456),
        (Value::Int8(-9_000_000_000), Int8),
        p("1.5", Float4),
        f(-2.25),
        n("0"),
        n("1"),
        n("-1234567.000890"),
        n("0.00001234"),
        n("10000"),
        n("123456789012345678901234.5678"),
        n("1.50"),
        t("héllo"),
        p("\\x00ff", Bytea),
        date("2024-03-15"),
        p("12:34:56.789", Time),
        ts("2024-03-15 12:34:56.789"),
        tstz("2024-03-15 12:34:56+00"),
        iv("1 year 2 days 03:04:05"),
        p("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", Uuid),
        p(r#"{"a": [1, 2]}"#, Json),
        jb(r#"{"a": [1, 2]}"#),
        arr("{1,NULL,3}", Int4),
        arr("{}", Text),
        arr(r#"{a,"b c",NULL}"#, Text),
        arr("{1.5,2.25}", Numeric),
    ];
    for (v, ty) in cases {
        let mut buf = Vec::new();
        encode_binary(&v, &ty, &mut buf).unwrap();
        let back = decode_binary(&buf, &ty).unwrap();
        assert_eq!(format_value(&back, &c), format_value(&v, &c), "{ty:?}");
    }
    // Wire images taken from PostgreSQL (numeric_send, array_send, ...).
    let enc = |a: A| {
        let mut buf = Vec::new();
        encode_binary(&a.0, &a.1, &mut buf).unwrap();
        buf.iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    assert_eq!(enc(n("1234.5678")), "000200000000000404d2162e");
    assert_eq!(enc(n("-0.001")), "0001ffff40000003000a");
    assert_eq!(enc(n("0")), "0000000000000000");
    assert_eq!(enc(n("100000000")), "00010002000000000001");
    assert_eq!(enc(n("1.50")), "000200000000000200011388");
    assert_eq!(enc(n("0.00001234")), "0001fffe0000000804d2");
    assert_eq!(enc(i(1)), "00000001");
    assert_eq!(enc(b(true)), "01");
    assert_eq!(enc(f(-2.25)), "c002000000000000");
    assert_eq!(enc(date("2000-01-02")), "00000001");
    assert_eq!(enc(p("00:00:01", Time)), "00000000000f4240");
    assert_eq!(enc(ts("2000-01-01 00:00:01")), "00000000000f4240");
    assert_eq!(enc(iv("1 mon 2 days 00:00:03")), "00000000002dc6c00000000200000001");
    assert_eq!(enc(p("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", Uuid)), "a0eebc999c0b4ef8bb6d6bb9bd380a11");
    assert_eq!(enc(arr("{1,NULL}", Int4)), "00000001000000010000001700000002000000010000000400000001ffffffff");
    assert_eq!(enc(arr("{a,NULL}", Text)), "00000001000000010000001900000002000000010000000161ffffffff");
    assert_eq!(enc(arr("{}", Text)), "000000000000000000000019");
    assert_eq!(enc(jb("[1]")), "015b315d");
}

#[test]
fn float_output_matches_postgres_tie_rules() {
    // Exact halves round to the even digit; strings on the boundary to the
    // neighbouring float are never chosen.
    assert_eq!(rt("-600254479726672.2", Float8), "-600254479726672.2");
    assert_eq!(rt("-5.335842275525186e16", Float8), "-5.3358422755251856e+16");
    assert_eq!(rt("1.5955753716744603e17", Float8), "1.5955753716744602e+17");
    assert_eq!(rt("6.03412327794825e16", Float8), "6.0341232779482496e+16");
    assert_eq!(rt("1.6349317116691822e15", Float8), "1.6349317116691822e+15");
    assert_eq!(rt("1.0674817695195937e8", Float4), "1.06748176e+08");
    assert_eq!(rt("-329600.129945828", Float4), "-329600.12");
    assert_eq!(rt("1149927.2256722257", Float4), "1.1499272e+06");
    assert_eq!(rt("9007199254740993", Float8), "9.007199254740992e+15");
    assert_eq!(rt("0.3", Float8), "0.3");
    assert_eq!(rt("123456.789", Float8), "123456.789");
    assert_eq!(rt("5e-324", Float8), "5e-324");
    assert_eq!(rt("1.7976931348623157e308", Float8), "1.7976931348623157e+308");
}

#[test]
fn to_char_numeric_signs_and_zero() {
    let tc = |a: A, fmt: &str| func("to_char", vec![a, u(fmt)]);
    assert_eq!(tc(n("0.5"), "S999.9"), "   +.5");
    assert_eq!(tc(n("-1.18"), "S999.9"), "  -1.2");
    assert_eq!(tc(i(5), "999S"), "  5+");
    assert_eq!(tc(i(-5), "S999"), "  -5");
    assert_eq!(tc(i(1234), "S9,999"), "+1,234");
    assert_eq!(tc(n("-0.05"), "0000"), " 0000");
    assert_eq!(tc(n("-0.05"), "999"), "   0");
    assert_eq!(tc(n("-0.05"), "999PR"), "   0 ");
    assert_eq!(tc(n("-0.5"), "999.9PR"), "   <.5>");
    assert_eq!(tc(n("-0.04"), "9.9"), "  .0");
    assert_eq!(tc(n("-0.04"), "FM9.9"), "0.");
    assert_eq!(tc(n("0.5"), "FM999.99"), ".5");
    assert_eq!(tc(i(0), "FM999.99"), "0.");
    assert_eq!(tc(i(0), "FM0.00"), "0.00");
    assert_eq!(tc(i(0), "FM9,999,999.0"), ".0");
    assert_eq!(tc(i(100), "FM999.99"), "100.");
    assert_eq!(tc(i(12), "FM999.00"), "12.00");
    assert_eq!(tc(n("0.5"), "0000"), " 0001");
    assert_eq!(tc(i(23007), "FM9,999,999.0"), "23,007.0");
}

#[test]
fn more_datetime_input_forms() {
    assert_eq!(rt("20240315 101112", Timestamp), "2024-03-15 10:11:12");
    assert_eq!(rt("2024.075", Date), "2024-03-15");
    assert_eq!(rt("J2460385", Date), "2024-03-15");
    assert_eq!(rt("2024-3-5", Date), "2024-03-05");
    assert_eq!(rt("03/15/2024", Date), "2024-03-15");
    assert_eq!(rt("15-Mar-2024", Date), "2024-03-15");
    assert_eq!(rt("15 March 2024", Date), "2024-03-15");
    assert_eq!(rt("Fri Mar 15 2024", Date), "2024-03-15");
    assert_eq!(rt("2024-03-15 10:11:12+02", TimestampTz), "2024-03-15 08:11:12+00");
    assert_eq!(rt("2024-03-15 10:11:12 PST", TimestampTz), "2024-03-15 18:11:12+00");
    assert_eq!(rt("2024-03-15 10:11:12 Europe/Berlin", TimestampTz), "2024-03-15 09:11:12+00");
    assert_eq!(rt("2024-03-15 10:11:12-0530", TimestampTz), "2024-03-15 15:41:12+00");
    assert_eq!(rt("2024-03-15 10:11:12.123456789", Timestamp), "2024-03-15 10:11:12.123457");
    assert_eq!(rt("1999-12-31 23:59:59.9999999", Timestamp), "2000-01-01 00:00:00");
    assert_eq!(rt("2024-03-15 24:00:00", Timestamp), "2024-03-16 00:00:00");
    assert_eq!(rt("2024-03-15 12:00 AM", Timestamp), "2024-03-15 00:00:00");
    assert_eq!(rt("2024-03-15 12:00 PM", Timestamp), "2024-03-15 12:00:00");
    assert_eq!(rt("0001-01-01", Timestamp), "0001-01-01 00:00:00");
    assert_eq!(rt("2023-02-29", Date), "ERR:22008");
    assert_eq!(rt("2024-03-15 10:61", Timestamp), "ERR:22008");
    assert_eq!(rt("March 2024", Date), "ERR:22007");
}

/// LIKE against a straightforward recursive reference implementation.
#[test]
fn like_matches_reference() {
    fn reference(t: &[char], p: &[char]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some(('%', rest)) => (0..=t.len()).any(|i| reference(&t[i..], rest)),
            Some(('_', rest)) => !t.is_empty() && reference(&t[1..], rest),
            Some(('\\', rest)) => match (rest.split_first(), t.split_first()) {
                (Some((c, rest)), Some((tc, trest))) => c == tc && reference(trest, rest),
                _ => false,
            },
            Some((c, rest)) => t.first() == Some(c) && reference(&t[1..], rest),
        }
    }
    // Small deterministic generator; no external randomness needed.
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = |n: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % n
    };
    let alphabet = ['a', 'b', 'é', '%', '_'];
    for _ in 0..20_000 {
        let text: String = (0..next(7)).map(|_| alphabet[next(3) as usize]).collect();
        let mut pattern = String::new();
        for _ in 0..next(6) {
            match next(8) {
                0 => pattern.push_str("\\%"),
                1 => pattern.push_str("\\_"),
                k => pattern.push(alphabet[(k % 5) as usize]),
            }
        }
        let want = reference(&text.chars().collect::<Vec<_>>(), &pattern.chars().collect::<Vec<_>>());
        let got = op(t(&text), "~~", t(&pattern));
        assert_eq!(got, if want { "t" } else { "f" }, "text {text:?} pattern {pattern:?}");
    }
}
