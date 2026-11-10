//! Interactive SQL prompt on the embedded API.

use std::io::{self, BufRead, IsTerminal, Write};

use squad::catalog::Table;
use squad::{Database, FnCtx, QueryResult, Session};

/// Renders a result as an aligned table, like psql's default format.
pub fn render(result: &QueryResult, ctx: &FnCtx) -> String {
    let mut out = String::new();
    if !result.columns.is_empty() {
        let cells: Vec<Vec<String>> = result
            .rows
            .iter()
            .map(|r| {
                r.iter().map(|v| if v.is_null() { String::new() } else { squad::value::format_value(v, ctx) }).collect()
            })
            .collect();
        let headers: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        out.push_str(&table(&headers, &cells));
        let n = result.rows.len();
        out.push_str(&format!("({n} row{})\n", if n == 1 { "" } else { "s" }));
    } else if !result.tag.is_empty() {
        out.push_str(&result.tag);
        out.push('\n');
    }
    out
}

fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let width = |s: &str| s.lines().map(|l| l.chars().count()).max().unwrap_or(0);
    let mut widths: Vec<usize> = headers.iter().map(|h| width(h)).collect();
    for row in rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(width(cell));
        }
    }
    let mut out = String::new();
    let line = |cells: Vec<String>, out: &mut String| {
        out.push_str(format!(" {}", cells.join(" | ")).trim_end());
        out.push('\n');
    };
    line(headers.iter().zip(&widths).map(|(h, w)| format!("{h:^w$}", w = *w)).collect(), &mut out);
    out.push_str(&widths.iter().map(|w| "-".repeat(w + 2)).collect::<Vec<_>>().join("+"));
    out.push('\n');
    for row in rows {
        // Multi-line values are shown on one line.
        line(
            row.iter().zip(&widths).map(|(c, w)| format!("{:<w$}", c.replace('\n', "\\n"), w = *w)).collect(),
            &mut out,
        );
    }
    out
}

fn list_tables(session: &Session) -> String {
    let catalog = session.shared().catalog.read().clone();
    let mut rows: Vec<Vec<String>> = Vec::new();
    for t in catalog.tables.values() {
        rows.push(vec![t.schema.clone(), t.name.clone(), "table".into()]);
    }
    for v in catalog.views.values() {
        rows.push(vec![v.schema.clone(), v.name.clone(), "view".into()]);
    }
    for s in catalog.sequences.values() {
        rows.push(vec![s.schema.clone(), s.name.clone(), "sequence".into()]);
    }
    if rows.is_empty() {
        return "Did not find any relations.\n".into();
    }
    rows.sort();
    table(&["Schema", "Name", "Type"], &rows)
}

fn describe_table(t: &Table, catalog: &squad::catalog::Catalog) -> String {
    let mut rows = Vec::new();
    for (_, c) in t.visible_columns() {
        rows.push(vec![
            c.name.clone(),
            c.decl.clone(),
            if c.not_null { "not null".into() } else { String::new() },
            c.default.clone().unwrap_or_default(),
        ]);
    }
    let mut out = format!("Table \"{}.{}\"\n", t.schema, t.name);
    out.push_str(&table(&["Column", "Type", "Nullable", "Default"], &rows));
    let names = |cols: &[usize]| cols.iter().map(|&c| t.columns[c].name.as_str()).collect::<Vec<_>>().join(", ");
    if !t.indexes.is_empty() {
        out.push_str("Indexes:\n");
        for ix in &t.indexes {
            let kind = match (ix.primary, ix.unique) {
                (true, _) => "PRIMARY KEY, ",
                (false, true) => "UNIQUE, ",
                _ => "",
            };
            out.push_str(&format!("    \"{}\" {kind}btree ({})\n", ix.name, names(&ix.columns)));
        }
    }
    if !t.checks.is_empty() {
        out.push_str("Check constraints:\n");
        for c in &t.checks {
            out.push_str(&format!("    \"{}\" CHECK ({})\n", c.name, c.expr));
        }
    }
    if !t.foreign_keys.is_empty() {
        out.push_str("Foreign-key constraints:\n");
        for fk in &t.foreign_keys {
            let target = catalog.table_by_id(fk.ref_table);
            let ref_cols = target
                .map(|p| fk.ref_columns.iter().map(|&c| p.columns[c].name.as_str()).collect::<Vec<_>>().join(", "))
                .unwrap_or_default();
            out.push_str(&format!(
                "    \"{}\" FOREIGN KEY ({}) REFERENCES {}({ref_cols})\n",
                fk.name,
                names(&fk.columns),
                target.map_or("?", |p| p.name.as_str())
            ));
        }
    }
    out
}

fn describe(session: &Session, name: &str) -> String {
    let catalog = session.shared().catalog.read().clone();
    let (schema, name) = match name.split_once('.') {
        Some((s, n)) => (Some(s), n),
        None => (None, name),
    };
    let path = &session.state().search_path;
    if let Some(t) = catalog.table(schema, name, path) {
        return describe_table(t, &catalog);
    }
    if let Some(v) = catalog.view(schema, name, path) {
        return format!("View \"{}.{}\"\nDefinition:\n    {}\n", v.schema, v.name, v.sql);
    }
    format!("Did not find any relation named \"{name}\".\n")
}

const HELP: &str = "\
Statements end with a semicolon.
  \\dt          list tables, views and sequences
  \\d NAME      describe a table or view
  \\q           quit
";

/// True once the text holds a complete statement: it ends with a
/// semicolon outside of string literals, quoted identifiers and comments.
fn is_complete(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let (mut i, mut last_semicolon) = (0, false);
    while i < bytes.len() {
        match bytes[i] {
            q @ (b'\'' | b'"') => {
                i += 1;
                while i < bytes.len() && bytes[i] != q {
                    i += 1;
                }
                if i >= bytes.len() {
                    return false;
                }
                last_semicolon = false;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => match sql[i + 2..].find("*/") {
                Some(end) => i += end + 3,
                None => return false,
            },
            b'$' => {
                // Dollar-quoted string: $tag$ ... $tag$
                if let Some(len) = sql[i + 1..].find('$') {
                    let tag = &sql[i..i + len + 2];
                    if tag[1..tag.len() - 1].chars().all(|c| c.is_alphanumeric() || c == '_') {
                        match sql[i + tag.len()..].find(tag) {
                            Some(end) => i += tag.len() + end + tag.len() - 1,
                            None => return false,
                        }
                    }
                }
                last_semicolon = false;
            }
            b';' => last_semicolon = true,
            c if c.is_ascii_whitespace() => {}
            _ => last_semicolon = false,
        }
        i += 1;
    }
    last_semicolon
}

/// Runs the prompt until end of input or `\q`.
pub fn run(db: &Database, input: impl BufRead, mut output: impl Write) -> io::Result<()> {
    let mut session = db.session();
    let interactive = io::stdin().is_terminal();
    let mut pending = String::new();
    let prompt = |pending: &str, output: &mut dyn Write| -> io::Result<()> {
        if interactive {
            write!(output, "{}", if pending.is_empty() { "squad=> " } else { "squad-> " })?;
            output.flush()?;
        }
        Ok(())
    };
    if interactive {
        writeln!(output, "squad {} - type \\? for help", env!("CARGO_PKG_VERSION"))?;
    }
    prompt(&pending, &mut output)?;
    for line in input.lines() {
        let line = line?;
        let trimmed = line.trim();
        if pending.is_empty() && trimmed.starts_with('\\') {
            let mut words = trimmed.split_whitespace();
            match words.next().unwrap_or_default() {
                "\\q" | "\\quit" => return Ok(()),
                "\\dt" | "\\d" if words.clone().next().is_none() => write!(output, "{}", list_tables(&session))?,
                "\\d" | "\\dt" => write!(output, "{}", describe(&session, words.next().unwrap_or_default()))?,
                "\\?" | "\\h" | "\\help" => write!(output, "{HELP}")?,
                other => writeln!(output, "invalid command {other}; try \\?")?,
            }
        } else {
            pending.push_str(&line);
            pending.push('\n');
            if is_complete(&pending) {
                let sql = std::mem::take(&mut pending);
                let ctx = FnCtx { tz: session.state().tz, ..FnCtx::default() };
                let mut printed = String::new();
                let outcome = session.execute_each(&sql, &mut |r| {
                    printed.push_str(&render(&r, &ctx));
                    Ok(())
                });
                write!(output, "{printed}")?;
                if let Err(e) = outcome {
                    writeln!(output, "{e}")?;
                }
            }
        }
        prompt(&pending, &mut output)?;
    }
    if !pending.trim().is_empty() {
        // Input ended without a final semicolon: run what is there.
        let ctx = FnCtx { tz: session.state().tz, ..FnCtx::default() };
        match session.execute(&pending) {
            Ok(results) => {
                for r in results {
                    write!(output, "{}", render(&r, &ctx))?;
                }
            }
            Err(e) => writeln!(output, "{e}")?,
        }
    }
    if interactive {
        writeln!(output)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_completion() {
        assert!(is_complete("select 1;"));
        assert!(is_complete("select ';' ; -- done\n"));
        assert!(!is_complete("select 1"));
        assert!(!is_complete("select ';"));
        assert!(!is_complete("select 1; /* open"));
        assert!(!is_complete("select $$ ; "));
        assert!(is_complete("select $$ ; $$;"));
        assert!(is_complete("select $a$ ; $a$ ;\n"));
    }

    #[test]
    fn shell_session() {
        let db = Database::open_in_memory().unwrap();
        let script = "CREATE TABLE t (id int PRIMARY KEY,\n name text);\nINSERT INTO t VALUES (1, 'a'), (2, NULL);\nSELECT * FROM t ORDER BY id;\n\\dt\n\\d t\nSELECT nope;\n\\q\nSELECT 1;\n";
        let mut out = Vec::new();
        run(&db, script.as_bytes(), &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("CREATE TABLE\nINSERT 0 2\n id | name\n----+------\n 1  | a\n 2  |\n(2 rows)\n"), "{out}");
        assert!(out.contains(" public | t    | table"), "{out}");
        assert!(out.contains("\"t_pkey\" PRIMARY KEY, btree (id)"), "{out}");
        assert!(out.contains("42703"), "{out}");
        assert!(!out.contains("?column?"), "\\q must stop the shell: {out}");
    }
}
