//! The `squad` binary end to end.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};

use common::Client;

fn squad() -> Command {
    Command::new(env!("CARGO_BIN_EXE_squad"))
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
fn serve(args: &[&str]) -> (Child, SocketAddr) {
    let mut child = squad()
        .arg("serve")
        .args(args)
        .args(["--listen", "127.0.0.1:0"])
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let mut line = String::new();
    BufReader::new(child.stderr.take().unwrap()).read_line(&mut line).unwrap();
    let addr = line.trim().rsplit(' ').next().unwrap().parse().unwrap_or_else(|_| panic!("unexpected output: {line}"));
    (child, addr)
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
    assert!(!ok && err.contains("requires a value") || err.contains("needs a --password"), "{err}");
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
