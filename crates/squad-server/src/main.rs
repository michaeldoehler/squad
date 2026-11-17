//! The `squad` command: run a server, open a SQL shell, back up or verify
//! a database directory.

use std::collections::HashMap;
use std::io::{BufRead, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use squad::{Config, Database, RemoteConfig, RemoteSync, ReplicaConfig, SyncMode};
use squad_server::ping::{PingOptions, PingStatus};
use squad_server::scram::Verifier;
use squad_server::{Auth, LoginAttributes, Server, ServerConfig, TlsConfig};

const USAGE: &str = "\
squad - an embeddable, Postgres-compatible database

USAGE:
    squad serve <dir> [options]     serve a database over the Postgres wire protocol
    squad serve --memory [options]  serve a volatile in-memory database
    squad shell [<dir>]             interactive SQL prompt (in-memory without <dir>)
    squad backup <dir> <dest>       write a consistent copy of the database to <dest>
    squad verify <dir>              check the integrity of the stored data
    squad restore <url> <dir>       download a database from an object store
    squad ping [options]            check that a server accepts connections (exit 0/1)
    squad hash-password             read a password from stdin, print a SCRAM verifier
    squad --version

OPTIONS for serve:
    --listen <addr>           TCP address to listen on (default 127.0.0.1:5432)
    --unix-socket-dir <dir>   also listen on <dir>/.s.PGSQL.<port>
    --users-file <file>       logins, one user:password or user:SCRAM-SHA-256$...
                              verifier per line; attributes may follow the
                              name: `user read_only no_bypass_rls:password`
    --read-only <name>        the login may only read (repeatable)
    --no-bypass-rls <name>    row-level security policies apply to the login
                              (repeatable)
    --user <name>             login name for the password in SQUAD_PASSWORD
                              (default: SQUAD_USER, else squad)
    --password <pw>           password for --user (repeatable pairs); visible
                              in the process list, prefer SQUAD_PASSWORD
    --trust                   let every client in without a password even on
                              an address other than loopback
    --cleartext-password      ask for passwords in clear text instead of SCRAM
    --tls-cert <file> --tls-key <file>
                              PEM certificate chain and private key; clients
                              may then switch to TLS
    --require-tls             refuse TCP connections without TLS
    --max-connections <n>     open connections at most (default 100)
    --authentication-timeout <duration>
                              time to log in, e.g. 30s (default 60s)
    --idle-timeout <duration> close connections silent for this long (default off)
    --max-message-size <bytes>
                              largest message after login (default 1073741824)
    --sync <sync|group|async> commit durability (default sync)
    --remote <url>            keep a complete copy in an object store and
                              restore from it if <dir> is empty, e.g.
                              file:///mnt/backup or
                              s3://bucket/prefix?region=eu-central-1
    --remote-file <file>      read the --remote URL from a file
    --remote-sync <async|sync>
                              async (default): ship the log in the background;
                              sync: acknowledge a commit only once it is in
                              the object store
    --remote-restore          replace <dir> with the remote state if they
                              disagree
    --replica-of <url>        serve a read replica of the database whose
                              writer runs with --remote <url>; <dir> holds
                              the replica's local copy. Statements that write
                              fail until `SELECT squad_promote()` makes this
                              node the writer (--remote-sync then says how it
                              ships; default sync)
    --replica-of-file <file>  read the --replica-of URL from a file
    --poll-interval <ms>      how often a replica looks for new commits
                              (default 1000)

OPTIONS for ping:
    --host <host>             host name, IP address or Unix socket directory
                              (default 127.0.0.1)
    --port <port>             (default 5432)
    --user <name>             (default: SQUAD_USER, else squad)
    --database <name>         (default squad)
    --timeout <duration>      (default 5s)

ENVIRONMENT:
    SQUAD_PASSWORD, SQUAD_PASSWORD_FILE
                              password for serve (with --user / SQUAD_USER) and
                              for ping
    SQUAD_USER                login name used with SQUAD_PASSWORD
    SQUAD_LOG                 messages on stderr: off, error, warn, info
                              (default) or debug
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT_URL
                              S3 settings; each can also be read from the file
                              named by the variable with a _FILE suffix
";

struct Args {
    positional: Vec<String>,
    options: HashMap<String, Vec<String>>,
    flags: Vec<String>,
}

impl Args {
    fn last(&self, name: &str) -> Option<&str> {
        self.options.get(name).and_then(|v| v.last()).map(String::as_str)
    }

    fn flag(&self, name: &str) -> bool {
        self.flags.iter().any(|f| f == name)
    }
}

fn parse_args(args: &[String]) -> Result<Args, String> {
    const WITH_VALUE: &[&str] = &[
        "--listen",
        "--unix-socket-dir",
        "--user",
        "--password",
        "--users-file",
        "--read-only",
        "--no-bypass-rls",
        "--tls-cert",
        "--tls-key",
        "--max-connections",
        "--authentication-timeout",
        "--idle-timeout",
        "--max-message-size",
        "--sync",
        "--remote",
        "--remote-file",
        "--remote-sync",
        "--replica-of",
        "--replica-of-file",
        "--poll-interval",
        "--host",
        "--port",
        "--database",
        "--timeout",
    ];
    const FLAGS: &[&str] =
        &["--memory", "--cleartext-password", "--remote-restore", "--trust", "--require-tls", "--help", "-h"];
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

/// Reads a secret from a file, without the trailing line break.
fn read_secret_file(path: &str) -> Result<String, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("cannot read {path}: {e}"))?;
    Ok(text.trim_end_matches(['\n', '\r']).to_string())
}

/// The value of environment variable `name`, or else the contents of the
/// file named by `name_FILE` (the convention of Docker secrets).
fn env_or_file(name: &str) -> Result<Option<String>, String> {
    if let Some(v) = std::env::var(name).ok().filter(|v| !v.is_empty()) {
        return Ok(Some(v));
    }
    match std::env::var(format!("{name}_FILE")).ok().filter(|v| !v.is_empty()) {
        Some(path) => read_secret_file(&path).map(Some).map_err(|e| format!("{name}_FILE: {e}")),
        None => Ok(None),
    }
}

/// Makes `X_FILE` work for the variables the object store reads itself.
fn load_file_env() -> Result<(), String> {
    for name in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_ENDPOINT_URL"] {
        if std::env::var(name).ok().is_none_or(|v| v.is_empty()) {
            if let Some(value) = env_or_file(name)? {
                // Runs before any other thread is started.
                std::env::set_var(name, value);
            }
        }
    }
    Ok(())
}

/// A URL given directly or in a file.
fn url_option(args: &Args, name: &str) -> Result<Option<String>, String> {
    let file_option = format!("{name}-file");
    match (args.last(name), args.last(&file_option)) {
        (Some(_), Some(_)) => Err(format!("give either {name} or {file_option}, not both")),
        (Some(url), None) => Ok(Some(url.to_string())),
        (None, Some(path)) => read_secret_file(path).map(Some).map_err(|e| format!("{file_option}: {e}")),
        (None, None) => Ok(None),
    }
}

/// `30`, `30s`, `500ms`, `5min`, `1h`; a bare number means seconds.
fn parse_duration(option: &str, value: &str) -> Result<Duration, String> {
    let v = value.trim();
    let split = v.find(|c: char| !c.is_ascii_digit()).unwrap_or(v.len());
    let (num, unit) = v.split_at(split);
    let invalid = || format!("invalid {option} \"{value}\" (e.g. 30s, 500ms, 5min)");
    let n: u64 = num.parse().map_err(|_| invalid())?;
    Ok(match unit.trim() {
        "" | "s" => Duration::from_secs(n),
        "ms" => Duration::from_millis(n),
        "min" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        _ => return Err(invalid()),
    })
}

fn parse_number<T: std::str::FromStr>(args: &Args, option: &str) -> Result<Option<T>, String> {
    args.last(option).map(|v| v.parse().map_err(|_| format!("invalid {option} \"{v}\""))).transpose()
}

fn open(args: &Args, dir: Option<&String>) -> Result<Database, String> {
    let mut config = Config::default();
    if let Some(mode) = args.last("--sync") {
        config.sync_mode = match mode {
            "sync" => SyncMode::Sync,
            "group" => SyncMode::Group,
            "async" => SyncMode::Async,
            other => return Err(format!("invalid --sync mode \"{other}\" (expected sync, group or async)")),
        };
    }
    let memory = args.flag("--memory");
    let remote = url_option(args, "--remote")?;
    if let Some(url) = url_option(args, "--replica-of")? {
        if memory || remote.is_some() {
            return Err("--replica-of cannot be combined with --memory or --remote".into());
        }
        let dir = dir.ok_or("missing database directory")?;
        let store = squad::object::open_url(&url).map_err(|e| format!("cannot open the replica's source: {e}"))?;
        let mut replica = ReplicaConfig::new(store);
        replica.sync_mode = config.sync_mode;
        replica.promoted_sync = match args.last("--remote-sync").unwrap_or("sync") {
            "async" => RemoteSync::Async { interval: Duration::from_secs(1) },
            "sync" => RemoteSync::Sync,
            other => return Err(format!("invalid --remote-sync mode \"{other}\" (expected async or sync)")),
        };
        if let Some(ms) = args.last("--poll-interval") {
            let ms: u64 = ms.parse().map_err(|_| format!("invalid --poll-interval \"{ms}\" (milliseconds)"))?;
            replica.poll_interval = Duration::from_millis(ms.max(1));
        }
        return Database::open_replica(dir, replica).map_err(|e| format!("cannot open replica \"{dir}\": {e}"));
    }
    if args.options.contains_key("--poll-interval") {
        return Err("--poll-interval needs --replica-of".into());
    }
    if let Some(url) = remote {
        // The URL may carry credentials: do not echo it.
        let store = squad::object::open_url(&url).map_err(|e| format!("cannot open the remote store: {e}"))?;
        let mode = match args.last("--remote-sync").unwrap_or("async") {
            "async" => RemoteSync::Async { interval: Duration::from_secs(1) },
            "sync" => RemoteSync::Sync,
            other => return Err(format!("invalid --remote-sync mode \"{other}\" (expected async or sync)")),
        };
        let restore = args.flag("--remote-restore");
        config.remote = Some(RemoteConfig { restore, ..RemoteConfig::new(store, mode) });
    }
    match (dir, memory) {
        (None, true) => Database::open_in_memory().map_err(|e| e.to_string()),
        (Some(dir), false) => Database::open(dir, config).map_err(|e| format!("cannot open database \"{dir}\": {e}")),
        (Some(_), true) => Err("give either a directory or --memory, not both".into()),
        (None, false) => Err("missing database directory".into()),
    }
}

/// Collects logins from --user/--password, SQUAD_PASSWORD(_FILE) and
/// --users-file.
fn logins(args: &Args) -> Result<squad_server::users::Users, String> {
    let users = args.options.get("--user").cloned().unwrap_or_default();
    let passwords = args.options.get("--password").cloned().unwrap_or_default();
    let env_password = env_or_file("SQUAD_PASSWORD")?;
    let mut logins: HashMap<String, Verifier> = HashMap::new();
    let mut add = |user: String, verifier: Verifier| -> Result<(), String> {
        if logins.contains_key(&user) {
            return Err(format!("user \"{user}\" is configured twice"));
        }
        logins.insert(user, verifier);
        Ok(())
    };
    if !passwords.is_empty() {
        eprintln!(
            "warning: --password is visible to other users in the process list; \
             prefer SQUAD_PASSWORD, SQUAD_PASSWORD_FILE or --users-file"
        );
        if users.len() != passwords.len() {
            return Err("every --user needs a --password".into());
        }
        for (u, p) in users.iter().zip(&passwords) {
            add(u.clone(), Verifier::from_password(p))?;
        }
    }
    if let Some(pw) = env_password {
        let user = match (users.as_slice(), passwords.is_empty()) {
            ([one], true) => one.clone(),
            _ => std::env::var("SQUAD_USER").ok().filter(|u| !u.is_empty()).unwrap_or_else(|| "squad".into()),
        };
        add(user, Verifier::from_password(&pw))?;
    } else if passwords.is_empty() && !users.is_empty() {
        return Err("--user needs a password: set SQUAD_PASSWORD or SQUAD_PASSWORD_FILE, or use --users-file".into());
    }
    let mut attributes = HashMap::new();
    if let Some(path) = args.last("--users-file") {
        let file = squad_server::users::read_users_file(path).map_err(|e| e.to_string())?;
        for (user, verifier) in file.verifiers {
            add(user, verifier)?;
        }
        attributes = file.logins;
    }
    for user in args.options.get("--read-only").into_iter().flatten() {
        attributes.entry(user.clone()).or_insert_with(LoginAttributes::default).read_only = true;
    }
    for user in args.options.get("--no-bypass-rls").into_iter().flatten() {
        attributes.entry(user.clone()).or_insert_with(LoginAttributes::default).bypass_rls = false;
    }
    Ok(squad_server::users::Users { verifiers: logins, logins: attributes })
}

fn server_config(args: &Args) -> Result<ServerConfig, String> {
    let squad_server::users::Users { verifiers: logins, logins: attributes } = logins(args)?;
    let trust = args.flag("--trust");
    if trust && !logins.is_empty() {
        return Err("--trust cannot be combined with passwords".into());
    }
    let mut config = ServerConfig {
        auth: if logins.is_empty() { Auth::Trust } else { Auth::Verifiers { users: logins } },
        logins: attributes,
        allow_cleartext_password: args.flag("--cleartext-password"),
        trust_remote: trust,
        unix_socket: args.last("--unix-socket-dir").map(PathBuf::from),
        require_tls: args.flag("--require-tls"),
        ..ServerConfig::default()
    };
    match (args.last("--tls-cert"), args.last("--tls-key")) {
        (Some(cert), Some(key)) => {
            config.tls = Some(TlsConfig::from_pem_files(cert, key).map_err(|e| e.to_string())?);
        }
        (None, None) if config.require_tls => return Err("--require-tls needs --tls-cert and --tls-key".into()),
        (None, None) => {}
        _ => return Err("--tls-cert and --tls-key must be given together".into()),
    }
    if let Some(n) = parse_number::<usize>(args, "--max-connections")? {
        if n == 0 {
            return Err("--max-connections must be at least 1".into());
        }
        config.max_connections = n;
    }
    if let Some(v) = args.last("--authentication-timeout") {
        config.authentication_timeout = parse_duration("--authentication-timeout", v)?;
    }
    if let Some(v) = args.last("--idle-timeout") {
        let d = parse_duration("--idle-timeout", v)?;
        config.idle_timeout = (!d.is_zero()).then_some(d);
    }
    if let Some(n) = parse_number::<usize>(args, "--max-message-size")? {
        config.max_message_size = n;
    }
    Ok(config)
}

fn serve(args: &Args) -> Result<(), String> {
    let config = server_config(args)?;
    let tls = config.tls.is_some();
    let db = open(args, args.positional.first())?;
    let replica = db.is_replica();
    let listen = args.last("--listen").unwrap_or("127.0.0.1:5432");
    let mut server = Server::new(db, config);
    let addr = server.bind(listen).map_err(|e| format!("cannot listen on {listen}: {e}"))?;
    if replica {
        eprintln!("serving a read replica; writes fail until SELECT squad_promote() makes this node the writer");
    }
    if tls {
        eprintln!("TLS enabled");
    }
    eprintln!("squad {} listening on {addr}", env!("CARGO_PKG_VERSION"));
    if let Some(path) = server.unix_socket_path() {
        eprintln!("listening on Unix socket {}", path.display());
    }
    run_until_signal(server)
}

/// Serves until SIGTERM or SIGINT, then closes the connections and the
/// database cleanly. Without a handler, a server running as PID 1 in a
/// container would ignore SIGTERM and be killed after the grace period.
#[cfg(unix)]
fn run_until_signal(server: Server) -> Result<(), String> {
    use signal_hook::consts::{SIGINT, SIGTERM};
    let mut signals =
        signal_hook::iterator::Signals::new([SIGTERM, SIGINT]).map_err(|e| format!("cannot handle signals: {e}"))?;
    let handle = server.spawn();
    if let Some(signal) = signals.forever().next() {
        let name = if signal == SIGTERM { "SIGTERM" } else { "SIGINT" };
        // Not eprintln!: stderr may be closed by now.
        let _ = writeln!(std::io::stderr(), "received {name}, shutting down");
    }
    handle.shutdown();
    Ok(())
}

#[cfg(not(unix))]
fn run_until_signal(server: Server) -> Result<(), String> {
    server.run().map_err(|e| e.to_string())
}

fn ping(args: &Args) -> Result<(), String> {
    let mut opts = PingOptions::default();
    if let Some(h) = args.last("--host") {
        opts.host = h.to_string();
    }
    if let Some(p) = parse_number::<u16>(args, "--port")? {
        opts.port = p;
    }
    if let Some(u) = args.last("--user").map(str::to_string).or_else(|| std::env::var("SQUAD_USER").ok()) {
        opts.user = u;
    }
    if let Some(d) = args.last("--database") {
        opts.database = d.to_string();
    }
    if let Some(t) = args.last("--timeout") {
        opts.timeout = parse_duration("--timeout", t)?;
    }
    opts.password = env_or_file("SQUAD_PASSWORD")?;
    match squad_server::ping::ping(&opts)? {
        PingStatus::Ready => println!("{}:{} - accepting connections", opts.host, opts.port),
        PingStatus::Accepting(why) => println!("{}:{} - accepting connections ({why})", opts.host, opts.port),
    }
    Ok(())
}

fn hash_password() -> Result<(), String> {
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).map_err(|e| e.to_string())?;
    let password = line.trim_end_matches(['\n', '\r']);
    if password.is_empty() {
        return Err("usage: echo 'password' | squad hash-password".into());
    }
    println!("{}", Verifier::from_password(password));
    Ok(())
}

fn run(args: &[String]) -> Result<(), String> {
    let Some(command) = args.first() else { return Err(USAGE.to_string()) };
    let rest = parse_args(&args[1..])?;
    if rest.flag("--help") || rest.flag("-h") {
        print!("{USAGE}");
        return Ok(());
    }
    load_file_env()?;
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
        "ping" => ping(&rest),
        "hash-password" => hash_password(),
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
            let store = squad::object::open_url(url).map_err(|e| format!("cannot open the object store: {e}"))?;
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

/// Writes the messages of squad's libraries (`log` crate) to stderr, one
/// line each, at the level `SQUAD_LOG` names.
struct StderrLog {
    level: log::LevelFilter,
}

impl log::Log for StderrLog {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= self.level && metadata.target().starts_with("squad")
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            let level = record.level().as_str().to_ascii_lowercase();
            let _ = writeln!(std::io::stderr(), "{level}: {}", record.args());
        }
    }

    fn flush(&self) {}
}

fn init_log() -> Result<(), String> {
    let level = match std::env::var("SQUAD_LOG").ok().filter(|v| !v.is_empty()).as_deref() {
        None => log::LevelFilter::Info,
        Some(v) => {
            v.parse().map_err(|_| format!("invalid SQUAD_LOG \"{v}\" (expected off, error, warn, info or debug)"))?
        }
    };
    // Fails only if a logger is installed already.
    if log::set_boxed_logger(Box::new(StderrLog { level })).is_ok() {
        log::set_max_level(level);
    }
    Ok(())
}

fn main() -> ExitCode {
    if let Err(msg) = init_log() {
        eprintln!("{msg}");
        return ExitCode::FAILURE;
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("{}", msg.trim_end());
            ExitCode::FAILURE
        }
    }
}
