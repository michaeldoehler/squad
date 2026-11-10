//! The `squad` command: run a server, open a SQL shell, back up or verify
//! a database directory.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;

use squad::{Config, Database, RemoteConfig, RemoteSync, SyncMode};
use squad_server::{Auth, Server, ServerConfig};

const USAGE: &str = "\
squad - an embeddable, Postgres-compatible database

USAGE:
    squad serve <dir> [options]     serve a database over the Postgres wire protocol
    squad serve --memory [options]  serve a volatile in-memory database
    squad shell [<dir>]             interactive SQL prompt (in-memory without <dir>)
    squad backup <dir> <dest>       write a consistent copy of the database to <dest>
    squad verify <dir>              check the integrity of the stored data
    squad restore <url> <dir>       download a database from an object store
    squad --version

OPTIONS for serve:
    --listen <addr>           TCP address to listen on (default 127.0.0.1:5432)
    --unix-socket-dir <dir>   also listen on <dir>/.s.PGSQL.<port>
    --user <name> --password <pw>
                              require this user and password (SCRAM-SHA-256);
                              may be repeated; without it every client is trusted
    --cleartext-password      ask for passwords in clear text instead of SCRAM
    --sync <sync|group|async> commit durability (default sync)
    --remote <url>            keep a complete copy in an object store and
                              restore from it if <dir> is empty, e.g.
                              file:///mnt/backup or
                              s3://bucket/prefix?region=eu-central-1
    --remote-sync <async|sync>
                              async (default): ship the log in the background;
                              sync: acknowledge a commit only once it is in
                              the object store
    --remote-restore          replace <dir> with the remote state if they
                              disagree
";

struct Args {
    positional: Vec<String>,
    options: HashMap<String, Vec<String>>,
    flags: Vec<String>,
}

fn parse_args(args: &[String]) -> Result<Args, String> {
    const WITH_VALUE: &[&str] = &["--listen", "--unix-socket-dir", "--user", "--password", "--sync", "--remote", "--remote-sync"];
    const FLAGS: &[&str] = &["--memory", "--cleartext-password", "--remote-restore"];
    let mut parsed = Args { positional: Vec::new(), options: HashMap::new(), flags: Vec::new() };
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if let Some((name, value)) = a.split_once('=').filter(|(n, _)| WITH_VALUE.contains(n)) {
            parsed.options.entry(name.to_string()).or_default().push(value.to_string());
        } else if WITH_VALUE.contains(&a.as_str()) {
            let value = it.next().ok_or_else(|| format!("option {a} requires a value"))?;
            parsed.options.entry(a.clone()).or_default().push(value.clone());
        } else if FLAGS.contains(&a.as_str()) {
            parsed.flags.push(a.clone());
        } else if a.starts_with("--") {
            return Err(format!("unknown option {a}"));
        } else {
            parsed.positional.push(a.clone());
        }
    }
    Ok(parsed)
}

fn open(args: &Args, dir: Option<&String>) -> Result<Database, String> {
    let mut config = Config::default();
    if let Some(mode) = args.options.get("--sync").and_then(|v| v.last()) {
        config.sync_mode = match mode.as_str() {
            "sync" => SyncMode::Sync,
            "group" => SyncMode::Group,
            "async" => SyncMode::Async,
            other => return Err(format!("invalid --sync mode \"{other}\" (expected sync, group or async)")),
        };
    }
    if let Some(url) = args.options.get("--remote").and_then(|v| v.last()) {
        let store = squad::object::open_url(url).map_err(|e| format!("cannot open remote \"{url}\": {e}"))?;
        let mode = match args.options.get("--remote-sync").and_then(|v| v.last()).map_or("async", String::as_str) {
            "async" => RemoteSync::Async { interval: std::time::Duration::from_secs(1) },
            "sync" => RemoteSync::Sync,
            other => return Err(format!("invalid --remote-sync mode \"{other}\" (expected async or sync)")),
        };
        let restore = args.flags.iter().any(|f| f == "--remote-restore");
        config.remote = Some(RemoteConfig { restore, ..RemoteConfig::new(store, mode) });
    }
    match (dir, args.flags.iter().any(|f| f == "--memory")) {
        (None, true) => Database::open_in_memory().map_err(|e| e.to_string()),
        (Some(dir), false) => Database::open(dir, config).map_err(|e| format!("cannot open database \"{dir}\": {e}")),
        (Some(_), true) => Err("give either a directory or --memory, not both".into()),
        (None, false) => Err("missing database directory".into()),
    }
}

fn serve(args: &Args) -> Result<(), String> {
    let db = open(args, args.positional.first())?;
    let users = args.options.get("--user").cloned().unwrap_or_default();
    let passwords = args.options.get("--password").cloned().unwrap_or_default();
    if users.len() != passwords.len() {
        return Err("every --user needs a --password".into());
    }
    let auth = if users.is_empty() {
        Auth::Trust
    } else {
        Auth::Password { users: users.into_iter().zip(passwords).collect() }
    };
    let config = ServerConfig {
        auth,
        allow_cleartext_password: args.flags.iter().any(|f| f == "--cleartext-password"),
        unix_socket: args.options.get("--unix-socket-dir").and_then(|v| v.last()).map(PathBuf::from),
    };
    let listen = args.options.get("--listen").and_then(|v| v.last()).map_or("127.0.0.1:5432", String::as_str);
    let mut server = Server::new(db, config);
    let addr = server.bind(listen).map_err(|e| format!("cannot listen on {listen}: {e}"))?;
    eprintln!("squad {} listening on {addr}", env!("CARGO_PKG_VERSION"));
    if let Some(path) = server.unix_socket_path() {
        eprintln!("listening on Unix socket {}", path.display());
    }
    server.run().map_err(|e| e.to_string())
}

fn run(args: &[String]) -> Result<(), String> {
    let Some(command) = args.first() else { return Err(USAGE.to_string()) };
    let rest = parse_args(&args[1..])?;
    match command.as_str() {
        "--version" | "-V" | "version" => {
            println!("squad {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        "--help" | "-h" | "help" => {
            print!("{USAGE}");
            Ok(())
        }
        "serve" => serve(&rest),
        "shell" => {
            let db = match rest.positional.first() {
                Some(dir) => open(&rest, Some(dir))?,
                None => Database::open_in_memory().map_err(|e| e.to_string())?,
            };
            squad_server::shell::run(&db, std::io::stdin().lock(), std::io::stdout()).map_err(|e| e.to_string())
        }
        "backup" => {
            let [dir, dest] = rest.positional.as_slice() else { return Err("usage: squad backup <dir> <dest>".into()) };
            let db = open(&rest, Some(dir))?;
            db.backup(dest).map_err(|e| format!("backup failed: {e}"))?;
            println!("backup written to {dest}");
            Ok(())
        }
        "restore" => {
            let [url, dir] = rest.positional.as_slice() else { return Err("usage: squad restore <url> <dir>".into()) };
            let store = squad::object::open_url(url).map_err(|e| format!("cannot open \"{url}\": {e}"))?;
            Database::restore_from(&*store, dir).map_err(|e| format!("restore failed: {e}"))?;
            println!("database restored to {dir}");
            Ok(())
        }
        "verify" => {
            let [dir] = rest.positional.as_slice() else { return Err("usage: squad verify <dir>".into()) };
            let db = open(&rest, Some(dir))?;
            let report = db.verify().map_err(|e| format!("verification failed: {e}"))?;
            println!("checked {} pages, {} entries", report.pages_checked, report.entries);
            if report.is_ok() {
                println!("no problems found");
                Ok(())
            } else {
                for e in &report.errors {
                    eprintln!("problem: {e}");
                }
                Err(format!("{} problem(s) found", report.errors.len()))
            }
        }
        other => Err(format!("unknown command \"{other}\"\n\n{USAGE}")),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("{}", msg.trim_end());
            ExitCode::FAILURE
        }
    }
}
