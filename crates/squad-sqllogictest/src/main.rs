//! Command-line sqllogictest runner.
//!
//! ```text
//! cargo run --release -p squad-sqllogictest -- [options] <file or directory>...
//!
//!   --jobs N          run N files in parallel (default: number of CPUs)
//!   --failures N      print the first N failures of every file (default 3)
//!   --group-depth N   also print totals per directory, N levels below
//!                     each argument (default 1)
//!   --dump FILE       write every kept failure to FILE
//!   --verdicts FILE   write `file<TAB>line<TAB>P|F|S` for every record
//! ```
//!
//! Each file runs against its own fresh in-memory database. The exit code
//! is non-zero if any record failed.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use squad_sqllogictest::{collect_files, run_file, FileReport};

fn group_of(root: &Path, file: &Path, depth: usize) -> String {
    let rel = file.strip_prefix(root).unwrap_or(file);
    let parts: Vec<String> = rel.components().map(|c| c.as_os_str().to_string_lossy().into_owned()).collect();
    if parts.len() <= 1 {
        return parts.first().cloned().unwrap_or_default();
    }
    parts[..depth.min(parts.len() - 1)].join("/")
}

fn main() {
    let mut jobs = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut keep = 3usize;
    let mut depth = 1usize;
    let mut dump: Option<PathBuf> = None;
    let mut verdicts: Option<PathBuf> = None;
    let mut roots: Vec<PathBuf> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut number = |name: &str| -> usize {
            args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| {
                eprintln!("{name} needs a number");
                std::process::exit(2)
            })
        };
        match a.as_str() {
            "--jobs" => jobs = number("--jobs").max(1),
            "--failures" => keep = number("--failures"),
            "--group-depth" => depth = number("--group-depth").max(1),
            "--dump" => dump = args.next().map(PathBuf::from),
            "--verdicts" => verdicts = args.next().map(PathBuf::from),
            "-h" | "--help" => {
                println!("usage: squad-sqllogictest [--jobs N] [--failures N] [--group-depth N] [--dump FILE] [--verdicts FILE] <file or directory>...");
                return;
            }
            _ => roots.push(PathBuf::from(a)),
        }
    }
    if roots.is_empty() {
        eprintln!("usage: squad-sqllogictest [--jobs N] [--failures N] [--group-depth N] [--dump FILE] [--verdicts FILE] <file or directory>...");
        std::process::exit(2);
    }

    let mut files: Vec<(PathBuf, PathBuf)> = Vec::new();
    for root in &roots {
        let mut found = Vec::new();
        collect_files(root, &mut found);
        files.extend(found.into_iter().map(|f| (root.clone(), f)));
    }
    let log_verdicts = verdicts.is_some();
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<(FileReport, f64)>>> = Mutex::new(vec![None; files.len()]);
    std::thread::scope(|scope| {
        for _ in 0..jobs.min(files.len().max(1)) {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                let Some((_, file)) = files.get(i) else { break };
                let start = Instant::now();
                // A panic in the engine must not hide the other files.
                let report = std::panic::catch_unwind(|| run_file(file, keep, log_verdicts)).unwrap_or_else(|_| FileReport {
                    parse_error: Some("the engine panicked".to_string()),
                    ..FileReport::default()
                });
                let secs = start.elapsed().as_secs_f64();
                eprintln!(
                    "{:<60} {:>7} passed {:>7} failed {:>6} skipped {:>7.1}s",
                    file.display(),
                    report.passed,
                    report.failed,
                    report.skipped,
                    secs
                );
                results.lock().unwrap()[i] = Some((report, secs));
            });
        }
    });

    let results = results.into_inner().unwrap();
    let mut groups: BTreeMap<String, (usize, usize, usize, usize)> = BTreeMap::new();
    let mut total = (0usize, 0usize, 0usize);
    let mut dump_file = dump.map(|p| std::fs::File::create(p).expect("create dump file"));
    let mut verdict_file = verdicts.map(|p| std::io::BufWriter::new(std::fs::File::create(p).expect("create verdict file")));
    println!();
    for ((root, file), result) in files.iter().zip(&results) {
        let Some((report, _)) = result else { continue };
        let g = groups.entry(group_of(root, file, depth)).or_default();
        g.0 += report.passed;
        g.1 += report.failed;
        g.2 += report.skipped;
        g.3 += 1;
        total.0 += report.passed;
        total.1 += report.failed;
        total.2 += report.skipped;
        if let Some(out) = &mut verdict_file {
            for (line, verdict) in &report.verdicts {
                writeln!(out, "{}\t{line}\t{verdict}", file.display()).expect("write verdict file");
            }
        }
        if let Some(e) = &report.parse_error {
            println!("{}: could not run: {e}", file.display());
            total.1 += 1;
        }
        for f in &report.failures {
            let text = format!("--- {}:{}\n{}\n{}\n", file.display(), f.line, f.sql, f.message);
            match &mut dump_file {
                Some(d) => d.write_all(text.as_bytes()).expect("write dump file"),
                None => println!("{text}"),
            }
        }
    }
    println!("{:<28} {:>6} {:>10} {:>10} {:>9} {:>8}", "group", "files", "passed", "failed", "skipped", "pass %");
    let line = |name: &str, files: usize, p: usize, f: usize, s: usize| {
        let pct = if p + f == 0 { 100.0 } else { p as f64 * 100.0 / (p + f) as f64 };
        println!("{name:<28} {files:>6} {p:>10} {f:>10} {s:>9} {pct:>7.2}%");
    };
    for (name, (p, f, s, n)) in &groups {
        line(name, *n, *p, *f, *s);
    }
    line("total", files.len(), total.0, total.1, total.2);
    if total.1 > 0 {
        std::process::exit(1);
    }
}
