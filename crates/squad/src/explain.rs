//! Text rendering of query plans for `EXPLAIN`.

use crate::plan::{JoinKind, Plan, ScanPlan};
use crate::session::{Kind, Prepared};

fn scan_line(scan: &ScanPlan) -> String {
    let table = scan.table.qualified_name();
    let dir = if scan.reverse { " Backward" } else { "" };
    let mut line = match scan.index_def() {
        Some(ix) => format!("Index Scan{dir} using {} on {table}", ix.name),
        None if scan.eq.is_empty() && scan.lower.is_none() && scan.upper.is_none() => {
            format!("Seq Scan{dir} on {table}")
        }
        None => match scan.table.pk_name() {
            Some(pk) => format!("Index Scan{dir} using {pk} on {table}"),
            None => format!("Seq Scan{dir} on {table}"),
        },
    };
    let key = scan.key_columns();
    let mut conds = Vec::new();
    for (i, _) in scan.eq.iter().enumerate() {
        conds.push(format!("{} = ?", scan.table.columns[key[i]].name));
    }
    if let Some(next) = key.get(scan.eq.len()) {
        let name = &scan.table.columns[*next].name;
        if let Some((_, inclusive)) = &scan.lower {
            conds.push(format!("{name} {} ?", if *inclusive { ">=" } else { ">" }));
        }
        if let Some((_, inclusive)) = &scan.upper {
            conds.push(format!("{name} {} ?", if *inclusive { "<=" } else { "<" }));
        }
    }
    if !conds.is_empty() {
        line.push_str(&format!("  (Index Cond: {})", conds.join(" AND ")));
    }
    if scan.filter.is_some() {
        line.push_str("  (Filter)");
    }
    line
}

fn join_name(kind: JoinKind) -> &'static str {
    match kind {
        JoinKind::Inner => "Join",
        JoinKind::Left => "Left Join",
        JoinKind::Right => "Right Join",
        JoinKind::Full => "Full Join",
        JoinKind::Semi => "Semi Join",
        JoinKind::Anti => "Anti Join",
    }
}

fn walk(plan: &Plan, depth: usize, out: &mut Vec<String>) {
    let indent = if depth == 0 { String::new() } else { format!("{}->  ", " ".repeat(depth * 6 - 4)) };
    let mut line = |text: String| out.push(format!("{indent}{text}"));
    match plan {
        Plan::OneRow => line("Result".into()),
        Plan::Values(rows) => line(format!("Values Scan  (rows={})", rows.len())),
        Plan::Scan(scan) => line(scan_line(scan)),
        Plan::System(name) => line(format!("System Scan on {name}")),
        Plan::Filter(input, _) => {
            line("Filter".into());
            walk(input, depth + 1, out);
        }
        // Projections are not shown, as in Postgres.
        Plan::Project(input, _) => walk(input, depth, out),
        Plan::NestedLoop { left, right, kind, index_join, lateral, .. } => {
            let detail = if *index_join {
                "  (Index Lookup)"
            } else if *lateral {
                "  (Lateral)"
            } else {
                ""
            };
            line(format!("Nested Loop {}{detail}", join_name(*kind)));
            walk(left, depth + 1, out);
            walk(right, depth + 1, out);
        }
        Plan::HashJoin { left, right, kind, .. } => {
            line(format!("Hash {}", join_name(*kind)));
            walk(left, depth + 1, out);
            walk(right, depth + 1, out);
        }
                Plan::ColumnarAgg { scan, groups, .. } => {
            line(format!(
                "Columnar {} on {}",
                if groups.is_empty() { "Aggregate" } else { "HashAggregate" },
                scan.table.qualified_name()
            ));
        }
        Plan::Aggregate { input, groups, .. } => {
            line(if groups.is_empty() { "Aggregate".into() } else { "HashAggregate".into() });
            walk(input, depth + 1, out);
        }
        Plan::Window { input, .. } => {
            line("WindowAgg".into());
            walk(input, depth + 1, out);
        }
        Plan::Sort { input, .. } => {
            line("Sort".into());
            walk(input, depth + 1, out);
        }
        Plan::Limit { input, .. } => {
            line("Limit".into());
            walk(input, depth + 1, out);
        }
        Plan::Distinct { input, .. } => {
            line("Unique".into());
            walk(input, depth + 1, out);
        }
        Plan::SetOp { kind, all, left, right } => {
            line(format!("{kind:?}{}", if *all { " All" } else { "" }));
            walk(left, depth + 1, out);
            walk(right, depth + 1, out);
        }
        Plan::With { ctes, body } => {
            walk(body, depth, out);
            for cte in ctes {
                out.push(format!("{}CTE {}", " ".repeat(depth * 6 + 2), cte.name));
                walk(&cte.plan, depth + 1, out);
                if let Some((step, _)) = &cte.recursive {
                    walk(step, depth + 1, out);
                }
            }
        }
        Plan::CteScan(_) => line("CTE Scan".into()),
        Plan::FuncScan { func, .. } => line(format!("Function Scan on {func:?}")),
        Plan::ProjectSet { input, .. } => {
            line("ProjectSet".into());
            walk(input, depth + 1, out);
        }
        Plan::LockRows { input, .. } => {
            line("LockRows".into());
            walk(input, depth + 1, out);
        }
        Plan::Insert(p) => {
            line(format!("Insert on {}", p.table.qualified_name()));
            walk(&p.source, depth + 1, out);
        }
        Plan::Update(p) => {
            line(format!("Update on {}", p.table.qualified_name()));
            walk(&p.source, depth + 1, out);
        }
        Plan::Delete(p) => {
            line(format!("Delete on {}", p.table.qualified_name()));
            walk(&p.source, depth + 1, out);
        }
    }
}

/// One line per plan node, indented like Postgres' text format.
pub(crate) fn render(stmt: &Prepared) -> Vec<String> {
    let mut out = Vec::new();
    match &stmt.kind {
        Kind::Plan { plan, .. } => walk(plan, 0, &mut out),
        _ => out.push("Utility Statement".to_string()),
    }
    out
}
