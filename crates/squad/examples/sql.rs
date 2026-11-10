//! Runs SQL statements given as arguments (or on stdin, one per line)
//! against an in-memory database and prints the results.
//!
//! ```text
//! cargo run -p squad --example sql -- "SELECT 1 + 1"
//! ```

use std::io::BufRead;

use squad::{Database, FnCtx};

fn main() {
    let db = Database::open_in_memory().expect("open");
    let conn = db.connect();
    let ctx = FnCtx::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let lines: Vec<String> =
        if args.is_empty() { std::io::stdin().lock().lines().map(|l| l.expect("read stdin")).collect() } else { args };
    for sql in lines {
        if sql.trim().is_empty() {
            continue;
        }
        match conn.simple_query(&sql) {
            Ok(results) => {
                for r in results {
                    if !r.columns.is_empty() {
                        println!("{}", r.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(" | "));
                        for row in &r.rows {
                            let cells: Vec<String> =
                                row.iter()
                                    .map(|v| {
                                        if v.is_null() {
                                            "NULL".to_string()
                                        } else {
                                            squad::value::format_value(v, &ctx)
                                        }
                                    })
                                    .collect();
                            println!("{}", cells.join(" | "));
                        }
                    }
                    println!("{}", r.tag);
                }
            }
            Err(e) => println!("{e}"),
        }
    }
}
