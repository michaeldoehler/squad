//! The `squad` binary end to end.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};

use common::Client;

fn squad() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_squad"));
    for var in ["SQUAD_PASSWORD", "SQUAD_PASSWORD_FILE", "SQUAD_USER"] {
        cmd.env_remove(var);
    }
    cmd
}

fn output(cmd: &mut Command) -> (bool, String, String) {
    let out = cmd.output().expect("run squad");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Starts `squad serve` on a free port and returns the process and the
/// address it reports.
fn serve(args: &[&str]) -> (Running, SocketAddr) {
    let (child, addr, _) = serve_with(squad(), args);
    (child, addr)
}

/// Like [`serve`] with a prepared command (environment); also returns what
/// the server printed before listening.
fn serve_with(mut cmd: Command, args: &[&str]) -> (Running, SocketAddr, String) {
    let mut child =
        cmd.arg("serve").args(args).args(["--listen", "127.0.0.1:0"]).stderr(Stdio::piped()).spawn().expect("spawn");
    let mut line = String::new();
    let mut printed = String::new();
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    while !line.contains("listening on") {
        line.clear();
        assert!(stderr.read_line(&mut line).unwrap() > 0, "the server exited without listening: {printed}");
        printed.push_str(&line);
    }
    let addr = line.trim().rsplit(' ').next().unwrap().parse().unwrap_or_else(|_| panic!("unexpected output: {line}"));
    (Running(child), addr, printed)
}

/// A server process that is killed when the test ends, also on panic.
struct Running(Child);

impl std::ops::Deref for Running {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for Running {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn version_help_and_usage_errors() {
    let (ok, out, _) = output(squad().arg("--version"));
    assert!(ok);
    assert_eq!(out.trim(), format!("squad {}", env!("CARGO_PKG_VERSION")));
    let (ok, out, _) = output(squad().arg("--help"));
    assert!(ok && out.contains("squad serve <dir>"));
    let (ok, _, err) = output(&mut squad());
    assert!(!ok && err.contains("USAGE"));
    let (ok, _, err) = output(squad().args(["frobnicate"]));
    assert!(!ok && err.contains("unknown command"));
    let (ok, _, err) = output(squad().args(["serve"]));
    assert!(!ok && err.contains("missing database directory"));
    let (ok, _, err) = output(squad().args(["serve", "--memory", "--user", "a"]));
    assert!(!ok && err.contains("--user needs a password"), "{err}");
    let (ok, _, err) = output(squad().args(["serve", "--memory", "--sync", "sometimes"]));
    assert!(!ok && err.contains("invalid --sync mode"), "{err}");
}

#[test]
fn shell_runs_scripts() {
    let mut child = squad().arg("shell").stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"CREATE TABLE t (a int, b text);\nINSERT INTO t VALUES (1, 'x'),\n (2, 'y');\nSELECT * FROM t ORDER BY a;\n\\dt\n")
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(text.contains(" a | b\n---+---\n 1 | x\n 2 | y\n(2 rows)\n"), "{text}");
    assert!(text.contains(" public | t    | table"), "{text}");
}

#[test]
fn serve_persists_across_restart_backup_and_verify() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let data_arg = data.to_str().unwrap();

    let (mut child, addr) = serve(&[data_arg]);
    let mut c = Client::connect(addr);
    c.rows("CREATE TABLE kv (k text PRIMARY KEY, v int); INSERT INTO kv VALUES ('a', 1), ('b', 2)");
    c.rows("UPDATE kv SET v = v + 10 WHERE k = 'b'");
    // Kill the server without a clean shutdown: committed data must
    // survive through the write-ahead log.
    child.kill().unwrap();
    child.wait().unwrap();

    let (mut child, addr) = serve(&[data_arg, "--sync", "group"]);
    let mut c = Client::connect(addr);
    let expected =
        vec![vec![Some("a".to_string()), Some("1".to_string())], vec![Some("b".to_string()), Some("12".to_string())]];
    assert_eq!(c.rows("SELECT k, v FROM kv ORDER BY k"), expected);
    // The directory is locked while the server runs.
    let (ok, _, err) = output(squad().args(["verify", data_arg]));
    assert!(!ok, "second open must fail: {err}");
    child.kill().unwrap();
    child.wait().unwrap();

    let (ok, out, err) = output(squad().args(["verify", data_arg]));
    assert!(ok && out.contains("no problems found"), "{out}{err}");
    let copy = dir.path().join("copy");
    let (ok, out, err) = output(squad().args(["backup", data_arg, copy.to_str().unwrap()]));
    assert!(ok && out.contains("backup written"), "{out}{err}");

    // The backup is a complete database of its own.
    let (mut child, addr) = serve(&[copy.to_str().unwrap()]);
    let mut c = Client::connect(addr);
    assert_eq!(c.rows("SELECT k, v FROM kv ORDER BY k"), expected);
    child.kill().unwrap();
    child.wait().unwrap();
}

#[test]
fn serve_with_password() {
    let (mut child, addr) = serve(&["--memory", "--user", "app", "--password", "pw", "--cleartext-password"]);
    let mut c = Client::startup(addr, &[("user", "app")]);
    assert_eq!(c.recv().body, 3i32.to_be_bytes());
    c.send(b'p', common::Body::new().cstr("pw"));
    assert_eq!(c.recv().body, 0i32.to_be_bytes());
    c.until_ready();
    assert_eq!(c.rows("SELECT 1"), vec![vec![Some("1".to_string())]]);
    child.kill().unwrap();
    child.wait().unwrap();
}

#[test]
fn remote_copy_and_restore() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let remote = format!("file://{}", dir.path().join("remote").display());
    let run = |args: &[&str], script: &str| {
        let mut child =
            squad().args(args).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        child.stdin.take().unwrap().write_all(script.as_bytes()).unwrap();
        let out = child.wait_with_output().unwrap();
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    let (ok, _, err) = run(
        &["shell", data.to_str().unwrap(), "--remote", &remote, "--remote-sync", "sync"],
        "CREATE TABLE t (id int PRIMARY KEY, v text);\nINSERT INTO t SELECT g, 'row ' || g FROM generate_series(1, 500) g;\n",
    );
    assert!(ok, "{err}");

    // The machine is gone; a new one restores from the object store.
    let copy = dir.path().join("copy");
    let (ok, out, err) = output(squad().args(["restore", &remote, copy.to_str().unwrap()]));
    assert!(ok && out.contains("database restored"), "{out}{err}");
    let (ok, out, err) = run(&["shell", copy.to_str().unwrap()], "SELECT count(*), max(v) FROM t;\n");
    assert!(ok && out.contains(" 500 ") && out.contains("row 99"), "{out}{err}");
    let (ok, out, _) = output(squad().args(["verify", copy.to_str().unwrap()]));
    assert!(ok && out.contains("no problems found"), "{out}");

    // Opening an empty directory with --remote restores and continues.
    let fresh = dir.path().join("fresh");
    let (ok, out, err) = run(
        &["shell", fresh.to_str().unwrap(), "--remote", &remote],
        "INSERT INTO t VALUES (501, 'new');\nSELECT count(*) FROM t;\n",
    );
    assert!(ok && out.contains("501"), "{out}{err}");
    // The first directory is now stale and is refused...
    let (ok, _, err) = run(&["shell", data.to_str().unwrap(), "--remote", &remote], "SELECT 1;\n");
    assert!(!ok && err.contains("restore"), "{err}");
    // ...unless told to take the remote state.
    let (ok, out, err) =
        run(&["shell", data.to_str().unwrap(), "--remote", &remote, "--remote-restore"], "SELECT count(*) FROM t;\n");
    assert!(ok && out.contains("501"), "{out}{err}");

    let (ok, _, err) =
        output(squad().args(["serve", "--memory", "--remote-sync", "sometimes", "--remote", "memory://"]));
    assert!(!ok && err.contains("invalid --remote-sync mode"), "{err}");
}

#[test]
fn replica_served_over_the_wire_and_promoted() {
    let dir = tempfile::tempdir().unwrap();
    let path = |name: &str| dir.path().join(name).to_str().unwrap().to_string();
    let remote = format!("file://{}", path("remote"));
    let wait_for = |c: &mut Client, sql: &str, want: &str| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            // Until the replica has applied the change the query may
            // fail (a column that is not there yet) or answer the old state.
            let (msgs, _) = c.query(sql);
            let got: Vec<_> = msgs.iter().filter(|m| m.ty == b'D').map(common::Msg::text_row).collect();
            if got == vec![vec![Some(want.to_string())]] {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "{sql}: still {got:?}");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    };

    let (mut writer, waddr) = serve(&[&path("writer"), "--remote", &remote, "--remote-sync", "sync"]);
    let mut w = Client::connect(waddr);
    w.rows("CREATE TABLE t (id int PRIMARY KEY, v text); INSERT INTO t SELECT g, 'row ' || g FROM generate_series(1, 100) g");
    assert_eq!(w.rows("SELECT pg_is_in_recovery()"), vec![vec![Some("f".to_string())]]);

    let (mut replica, raddr) = serve(&[&path("replica"), "--replica-of", &remote, "--poll-interval", "10"]);
    let mut r = Client::connect(raddr);
    assert_eq!(r.rows("SELECT count(*) FROM t"), vec![vec![Some("100".to_string())]]);
    assert_eq!(r.rows("SELECT pg_is_in_recovery()"), vec![vec![Some("t".to_string())]]);
    assert_eq!(r.rows("SHOW transaction_read_only"), vec![vec![Some("on".to_string())]]);
    assert_eq!(r.rows("SELECT squad_replication_status() ->> 'role'"), vec![vec![Some("replica".to_string())]]);

    // Commits and schema changes arrive without anyone asking.
    w.rows("INSERT INTO t VALUES (101, 'new'); ALTER TABLE t ADD COLUMN n int DEFAULT 7");
    wait_for(&mut r, "SELECT sum(n) FROM t", "707");

    // Writes are refused, in the simple and the extended protocol and
    // for COPY, and the session stays usable.
    for sql in ["INSERT INTO t VALUES (200, 'x')", "CREATE TABLE u (i int)", "COPY t FROM STDIN", "BEGIN READ WRITE"] {
        assert_eq!(r.error(sql), "25006", "{sql}");
    }
    r.send(b'P', common::Body::new().cstr("").cstr("DELETE FROM t WHERE id = $1").i16(0));
    r.send(b'B', common::Body::new().cstr("").cstr("").i16(0).i16(1).param(Some(b"1")).i16(0));
    r.send(b'E', common::Body::new().cstr("").i32(0));
    let (msgs, _) = r.sync();
    let error = msgs.iter().find(|m| m.ty == b'E').expect("the DELETE must fail").fields();
    assert_eq!(error[&'C'], "25006");
    assert_eq!(r.rows("SELECT count(*) FROM t"), vec![vec![Some("101".to_string())]]);

    // Failover: the writer dies, the replica is promoted by a client.
    writer.kill().unwrap();
    writer.wait().unwrap();
    assert_eq!(r.rows("SELECT squad_promote()"), vec![vec![Some("t".to_string())]]);
    assert_eq!(r.rows("SELECT pg_is_in_recovery()"), vec![vec![Some("f".to_string())]]);
    r.rows("INSERT INTO t VALUES (200, 'after failover')");
    assert_eq!(r.rows("SELECT squad_replication_status() ->> 'role'"), vec![vec![Some("writer".to_string())]]);

    // The old writer's directory does not come back as a writer...
    let (ok, _, err) = output(squad().args(["verify", &path("writer"), "--remote", &remote]));
    assert!(!ok, "the fenced writer opened again: {err}");
    // ...but as a replica of the new one, and sees its commits.
    let (mut old, oaddr) = serve(&[&path("writer"), "--replica-of", &remote, "--poll-interval", "10"]);
    let mut o = Client::connect(oaddr);
    wait_for(&mut o, "SELECT v FROM t WHERE id = 200", "after failover");
    old.kill().unwrap();
    old.wait().unwrap();

    // The promoted node restarts as an ordinary writer.
    replica.kill().unwrap();
    replica.wait().unwrap();
    let (mut again, addr) = serve(&[&path("replica"), "--remote", &remote, "--remote-sync", "sync"]);
    let mut c = Client::connect(addr);
    assert_eq!(c.rows("SELECT count(*) FROM t"), vec![vec![Some("102".to_string())]]);
    c.rows("INSERT INTO t VALUES (201, 'restarted')");
    again.kill().unwrap();
    again.wait().unwrap();

    let (ok, _, err) = output(squad().args(["serve", &path("x"), "--replica-of", &remote, "--remote", &remote]));
    assert!(!ok && err.contains("cannot be combined"), "{err}");
    let (ok, _, err) = output(squad().args(["serve", &path("x"), "--poll-interval", "5"]));
    assert!(!ok && err.contains("needs --replica-of"), "{err}");
    let (ok, _, err) =
        output(squad().args(["serve", &path("x"), "--replica-of", &format!("file://{}", path("nothing"))]));
    assert!(!ok && err.contains("cannot open replica"), "{err}");
}

fn ping(args: &[&str]) -> (bool, String, String) {
    output(squad().arg("ping").args(args))
}

fn port(addr: SocketAddr) -> String {
    addr.port().to_string()
}

fn stop(child: Running) {
    drop(child);
}

/// Pings until the server answers or 10 seconds have passed.
fn ping_until_ready(addr: SocketAddr) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let (ok, _, err) = ping(&["--port", &port(addr)]);
        if ok {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "{err}");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

#[test]
fn ping_reports_whether_a_server_accepts_connections() {
    let (child, addr) = serve(&["--memory"]);
    let (ok, out, err) = ping(&["--host", "127.0.0.1", "--port", &port(addr)]);
    assert!(ok, "{err}");
    assert_eq!(out.trim(), format!("127.0.0.1:{} - accepting connections", addr.port()));
    stop(child);
    // Nothing listens there any more.
    let (ok, _, err) = ping(&["--port", &port(addr), "--timeout", "2s"]);
    assert!(!ok && err.contains("cannot connect"), "{err}");
    // Something that is not a PostgreSQL server.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let not_pg = listener.local_addr().unwrap();
    let t = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let _ = s.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n");
    });
    let (ok, _, _) = ping(&["--port", &port(not_pg), "--timeout", "2s"]);
    assert!(!ok);
    t.join().unwrap();
}

#[test]
fn ping_logs_in_with_squad_password() {
    let mut cmd = squad();
    cmd.env("SQUAD_PASSWORD", "s3cret");
    let (child, addr, _) = serve_with(cmd, &["--memory"]);
    // Without the password the server is still reported as up.
    let (ok, out, _) = ping(&["--port", &port(addr)]);
    assert!(ok && out.contains("accepting connections (the server asks for a password)"), "{out}");
    let (ok, out, err) = output(squad().args(["ping", "--port", &port(addr)]).env("SQUAD_PASSWORD", "s3cret"));
    assert!(ok, "{err}");
    assert!(out.trim().ends_with("accepting connections"), "{out}");
    let (ok, out, _) = output(squad().args(["ping", "--port", &port(addr)]).env("SQUAD_PASSWORD", "wrong"));
    assert!(ok && out.contains("login failed"), "{out}");
    stop(child);
}

#[test]
fn trust_needs_loopback_or_explicit_flag() {
    let (ok, _, err) = output(squad().args(["serve", "--memory", "--listen", "0.0.0.0:0"]));
    assert!(!ok && err.contains("refusing to trust every client"), "{err}");
    // With --trust the server starts.
    let mut child = Running(
        squad().args(["serve", "--memory", "--trust", "--listen", "0.0.0.0:0"]).stderr(Stdio::piped()).spawn().unwrap(),
    );
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let mut line = String::new();
    stderr.read_line(&mut line).unwrap();
    assert!(line.contains("listening on 0.0.0.0:"), "{line}");
    stop(child);
    let (ok, _, err) = output(squad().args(["serve", "--memory", "--trust"]).env("SQUAD_PASSWORD", "x"));
    assert!(!ok && err.contains("--trust cannot be combined with passwords"), "{err}");
}

#[test]
fn passwords_from_the_environment_files_and_the_command_line() {
    let dir = tempfile::tempdir().unwrap();
    // On the command line: works, with a warning.
    let (child, addr, printed) = serve_with(squad(), &["--memory", "--user", "app", "--password", "pw"]);
    assert!(printed.contains("warning: --password is visible"), "{printed}");
    let (ok, out, _) =
        output(squad().args(["ping", "--port", &port(addr), "--user", "app"]).env("SQUAD_PASSWORD", "pw"));
    assert!(ok && out.trim().ends_with("accepting connections"), "{out}");
    stop(child);

    // SQUAD_PASSWORD_FILE (Docker secrets) with SQUAD_USER; no warning.
    let secret = dir.path().join("pw");
    std::fs::write(&secret, "from file\n").unwrap();
    let mut cmd = squad();
    cmd.env("SQUAD_PASSWORD_FILE", &secret).env("SQUAD_USER", "svc");
    let (child, addr, printed) = serve_with(cmd, &["--memory"]);
    assert!(!printed.contains("warning"), "{printed}");
    let (ok, out, _) = output(
        squad().args(["ping", "--port", &port(addr)]).env("SQUAD_USER", "svc").env("SQUAD_PASSWORD_FILE", &secret),
    );
    assert!(ok && out.trim().ends_with("accepting connections"), "{out}");
    let (ok, out, _) =
        output(squad().args(["ping", "--port", &port(addr), "--user", "svc"]).env("SQUAD_PASSWORD", "other"));
    assert!(ok && out.contains("login failed"), "{out}");
    stop(child);

    // A users file with a verifier from `squad hash-password`.
    let mut hash = squad().arg("hash-password").stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
    hash.stdin.take().unwrap().write_all(b"hashed pw\n").unwrap();
    let hashed = hash.wait_with_output().unwrap();
    assert!(hashed.status.success());
    let verifier = String::from_utf8(hashed.stdout).unwrap();
    assert!(verifier.starts_with("SCRAM-SHA-256$4096:"), "{verifier}");
    let users = dir.path().join("users");
    std::fs::write(&users, format!("# logins\nreader:{verifier}plain:plain pw\n")).unwrap();
    let (child, addr, _) = serve_with(squad(), &["--memory", "--users-file", users.to_str().unwrap()]);
    for (user, pw) in [("reader", "hashed pw"), ("plain", "plain pw")] {
        let (ok, out, _) =
            output(squad().args(["ping", "--port", &port(addr), "--user", user]).env("SQUAD_PASSWORD", pw));
        assert!(ok && out.trim().ends_with("accepting connections"), "{user}: {out}");
    }
    stop(child);
    std::fs::write(&users, "broken line\n").unwrap();
    let (ok, _, err) = output(squad().args(["serve", "--memory", "--users-file", users.to_str().unwrap()]));
    assert!(!ok && err.contains("line 1"), "{err}");
}

#[test]
fn tls_and_limit_options() {
    let dir = tempfile::tempdir().unwrap();
    let ck = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
    let cert = dir.path().join("server.crt");
    let key = dir.path().join("server.key");
    std::fs::write(&cert, ck.cert.pem()).unwrap();
    std::fs::write(&key, ck.key_pair.serialize_pem()).unwrap();
    let (cert, key) = (cert.to_str().unwrap(), key.to_str().unwrap());
    let (child, addr, printed) = serve_with(
        squad(),
        &["--memory", "--tls-cert", cert, "--tls-key", key, "--require-tls", "--max-connections", "1"],
    );
    assert!(printed.contains("TLS enabled"), "{printed}");
    // Without TLS the login is refused, which still means the server is up.
    let (ok, out, _) = ping(&["--port", &port(addr)]);
    assert!(ok && out.contains("login refused") && out.contains("no encryption"), "{out}");
    let mut c = Client::startup(addr, &[("user", "squad")]);
    assert_eq!(c.recv().fields()[&'C'], "28000");
    stop(child);

    let (child, addr) = serve(&["--memory", "--max-connections", "1", "--idle-timeout", "300ms"]);
    let first = Client::connect(addr);
    let (ok, _, err) = ping(&["--port", &port(addr)]);
    assert!(!ok && err.contains("sorry, too many clients already"), "{err}");
    drop(first);
    ping_until_ready(addr);
    // The idle timeout frees the slot of a silent client.
    let mut silent = Client::connect(addr);
    std::thread::sleep(std::time::Duration::from_millis(600));
    assert_eq!(silent.recv().fields()[&'C'], "57P05");
    ping_until_ready(addr);
    stop(child);

    for (args, msg) in [
        (vec!["--tls-cert", cert], "must be given together"),
        (vec!["--require-tls"], "--require-tls needs --tls-cert"),
        (vec!["--tls-cert", key, "--tls-key", key], "certificate"),
        (vec!["--max-connections", "0"], "at least 1"),
        (vec!["--idle-timeout", "soon"], "invalid --idle-timeout"),
        (vec!["--authentication-timeout", "5parsecs"], "invalid --authentication-timeout"),
    ] {
        let (ok, _, err) = output(squad().args(["serve", "--memory"]).args(&args));
        assert!(!ok && err.contains(msg), "{args:?}: {err}");
    }
}

#[test]
fn remote_url_from_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let url_file = dir.path().join("remote-url");
    std::fs::write(&url_file, format!("file://{}\n", dir.path().join("remote").display())).unwrap();
    let data = dir.path().join("data");
    let (child, addr) =
        serve(&[data.to_str().unwrap(), "--remote-file", url_file.to_str().unwrap(), "--remote-sync", "sync"]);
    let mut c = Client::connect(addr);
    c.rows("CREATE TABLE t (a int); INSERT INTO t VALUES (1)");
    stop(child);
    assert!(dir.path().join("remote").read_dir().unwrap().next().is_some(), "nothing was shipped");
    let (ok, _, err) = output(squad().args([
        "serve",
        data.to_str().unwrap(),
        "--remote",
        "memory://",
        "--remote-file",
        url_file.to_str().unwrap(),
    ]));
    assert!(!ok && err.contains("either --remote or --remote-file"), "{err}");
    // AWS_*_FILE variables are read like Docker secrets.
    let key_file = dir.path().join("key");
    std::fs::write(&key_file, "AKIDFROMFILE\n").unwrap();
    let (ok, _, err) = output(
        squad()
            .args(["restore", "s3://bucket/prefix?endpoint=http://127.0.0.1:1", dir.path().join("x").to_str().unwrap()])
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .env("AWS_ACCESS_KEY_ID_FILE", &key_file),
    );
    // The access key was found; the secret key is still missing.
    assert!(!ok && err.contains("no S3 secret key"), "{err}");
}

#[cfg(unix)]
#[test]
fn sigterm_shuts_the_server_down_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let (mut child, addr) = serve(&[data.to_str().unwrap()]);
    let mut c = Client::connect(addr);
    c.rows("CREATE TABLE t (a int); INSERT INTO t VALUES (1), (2)");
    let status = Command::new("kill").args(["-TERM", &child.id().to_string()]).status().unwrap();
    assert!(status.success());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let exit = loop {
        if let Some(exit) = child.try_wait().unwrap() {
            break exit;
        }
        assert!(std::time::Instant::now() < deadline, "the server ignored SIGTERM");
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    assert!(exit.success(), "{exit:?}");
    // The open connection was closed, and the directory is consistent
    // and unlocked.
    assert!(c.try_recv().is_none());
    let (ok, out, err) = output(squad().args(["verify", data.to_str().unwrap()]));
    assert!(ok && out.contains("no problems found"), "{out}{err}");
}
