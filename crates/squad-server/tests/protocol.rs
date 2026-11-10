//! Protocol-level tests with a hand-written client.

mod common;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use common::{start, types, Body, Client};
use squad_server::{scram, Auth, ServerConfig};

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

#[test]
fn startup_reports_parameters() {
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::startup(addr, &[("user", "alice"), ("database", "db"), ("application_name", "tests")]);
    assert_eq!(c.recv().ty, b'R');
    let (msgs, status) = c.until_ready();
    assert_eq!(status, b'I');
    let params: HashMap<String, String> = msgs
        .iter()
        .filter(|m| m.ty == b'S')
        .map(|m| {
            let mut parts = m.body.split(|b| *b == 0);
            let k = String::from_utf8_lossy(parts.next().unwrap()).into_owned();
            (k, String::from_utf8_lossy(parts.next().unwrap()).into_owned())
        })
        .collect();
    assert_eq!(params["server_version"], squad::SERVER_VERSION);
    assert_eq!(params["client_encoding"], "UTF8");
    assert_eq!(params["standard_conforming_strings"], "on");
    assert_eq!(params["integer_datetimes"], "on");
    assert_eq!(params["application_name"], "tests");
    assert_eq!(params["session_authorization"], "alice");
    assert_eq!(params["TimeZone"], "UTC");
    assert!(msgs.iter().any(|m| m.ty == b'K' && m.body.len() == 8), "BackendKeyData");
    assert_eq!(c.rows("SELECT current_user"), vec![vec![s("alice")]]);

    // A changed time zone is reported back.
    let (msgs, _) = c.query("SET TIME ZONE 'Europe/Berlin'");
    assert!(msgs.iter().any(|m| m.ty == b'S' && m.body.starts_with(b"TimeZone\0Europe/Berlin\0")));
    server.shutdown();
}

#[test]
fn ssl_request_is_declined_then_startup_proceeds() {
    let (server, addr) = start(ServerConfig::default());
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    stream.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).unwrap(); // SSLRequest
    let mut answer = [0u8; 1];
    stream.read_exact(&mut answer).unwrap();
    assert_eq!(answer[0], b'N');
    let mut c = Client { stream };
    let body = Body::new().i32(196608).cstr("user").cstr("squad").u8(0);
    let mut packet = ((body.0.len() + 4) as u32).to_be_bytes().to_vec();
    packet.extend_from_slice(&body.0);
    c.stream.write_all(&packet).unwrap();
    assert_eq!(c.recv().ty, b'R');
    c.until_ready();
    assert_eq!(c.rows("SELECT 1"), vec![vec![s("1")]]);

    // A cancel request is accepted and the connection simply closes.
    let mut cancel = TcpStream::connect(addr).unwrap();
    cancel.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    cancel.write_all(&[0, 0, 0, 16, 4, 210, 22, 46, 0, 0, 0, 1, 0, 0, 0, 2]).unwrap();
    assert_eq!(cancel.read(&mut [0u8; 8]).unwrap(), 0);
    server.shutdown();
}

#[test]
fn simple_query_results_errors_and_status() {
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::connect(addr);

    let (msgs, status) = c.query("CREATE TABLE t (id int PRIMARY KEY, name text); INSERT INTO t VALUES (1, 'a'), (2, NULL); SELECT * FROM t ORDER BY id");
    assert_eq!(types(&msgs), "CCTDDC");
    assert_eq!(status, b'I');
    assert_eq!(msgs[0].text(), "CREATE TABLE");
    assert_eq!(msgs[1].text(), "INSERT 0 2");
    assert_eq!(msgs[2].columns(), vec![("id".to_string(), 23, 0), ("name".to_string(), 25, 0)]);
    assert_eq!(msgs[3].text_row(), vec![s("1"), s("a")]);
    assert_eq!(msgs[4].text_row(), vec![s("2"), None]);
    assert_eq!(msgs[5].text(), "SELECT 2");

    // Empty query strings.
    for sql in ["", "  ", ";"] {
        let (msgs, _) = c.query(sql);
        assert_eq!(types(&msgs), "I", "{sql:?}");
    }

    // Error fields.
    let (msgs, status) = c.query("INSERT INTO t VALUES (1, 'dup')");
    assert_eq!((types(&msgs).as_str(), status), ("E", b'I'));
    let f = msgs[0].fields();
    assert_eq!(f[&'S'], "ERROR");
    assert_eq!(f[&'V'], "ERROR");
    assert_eq!(f[&'C'], "23505");
    assert!(f[&'M'].contains("duplicate key value violates unique constraint"));
    assert_eq!(f[&'D'], "Key (id)=(1) already exists.");
    let (msgs, _) = c.query("SELECT nope FROM t");
    let f = msgs[0].fields();
    assert_eq!((f[&'C'].as_str(), f[&'P'].as_str()), ("42703", "8"));
    assert_eq!(c.error("SELEC 1"), "42601");

    // Results before a failing statement are delivered; the batch is one
    // transaction, so its insert is rolled back.
    let (msgs, _) = c.query("INSERT INTO t VALUES (3, 'c'); SELECT 1/0; SELECT 2");
    assert_eq!(types(&msgs), "CE");
    assert_eq!(c.rows("SELECT count(*) FROM t"), vec![vec![s("2")]]);

    // Transaction status bytes.
    assert_eq!(c.query("BEGIN").1, b'T');
    assert_eq!(c.query("INSERT INTO t VALUES (3, 'c')").1, b'T');
    assert_eq!(c.query("SELECT 1/0").1, b'E');
    assert_eq!(c.error("SELECT 1"), "25P02");
    let (msgs, status) = c.query("COMMIT");
    assert_eq!((msgs[0].text().as_str(), status), ("ROLLBACK", b'I'));
    assert_eq!(c.rows("SELECT count(*) FROM t"), vec![vec![s("2")]]);

    // Clean termination.
    c.send(b'X', Body::new());
    assert!(c.try_recv().is_none());
    server.shutdown();
}

#[test]
fn extended_query_text_and_binary() {
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::connect(addr);
    c.rows("CREATE TABLE t (id int8 PRIMARY KEY, name text, score float8, ok bool, data bytea)");

    // Parse with unspecified parameter types, Describe the statement.
    c.send(b'P', Body::new().cstr("ins").cstr("INSERT INTO t VALUES ($1, $2, $3, $4, $5)").i16(0));
    c.send(b'D', Body::new().u8(b'S').cstr("ins"));
    let (msgs, _) = c.sync();
    assert_eq!(types(&msgs), "1tn");
    let oids: Vec<u32> = msgs[1].body[2..].chunks(4).map(|c| u32::from_be_bytes(c.try_into().unwrap())).collect();
    assert_eq!(oids, vec![20, 25, 701, 16, 17], "inferred parameter types");

    // Bind text parameters, including NULL.
    c.send(
        b'B',
        Body::new()
            .cstr("")
            .cstr("ins")
            .i16(0)
            .i16(5)
            .param(Some(b"1"))
            .param(Some(b"one"))
            .param(Some(b"1.5"))
            .param(Some(b"t"))
            .param(None)
            .i16(0),
    );
    c.send(b'E', Body::new().cstr("").i32(0));
    // Bind binary parameters.
    c.send(
        b'B',
        Body::new()
            .cstr("")
            .cstr("ins")
            .i16(1)
            .i16(1)
            .i16(5)
            .param(Some(&2i64.to_be_bytes()))
            .param(Some("zwei".as_bytes()))
            .param(Some(&2.25f64.to_be_bytes()))
            .param(Some(&[0]))
            .param(Some(&[0, 255, 7]))
            .i16(0),
    );
    c.send(b'E', Body::new().cstr("").i32(0));
    let (msgs, status) = c.sync();
    assert_eq!((types(&msgs).as_str(), status), ("2C2C", b'I'));
    assert_eq!(msgs[1].text(), "INSERT 0 1");

    // Query with binary results for all columns.
    c.send(
        b'P',
        Body::new().cstr("").cstr("SELECT id, name, score, ok, data FROM t WHERE id >= $1 ORDER BY id").i16(1).i32(20),
    );
    c.send(b'B', Body::new().cstr("").cstr("").i16(1).i16(1).i16(1).param(Some(&1i64.to_be_bytes())).i16(1).i16(1));
    c.send(b'D', Body::new().u8(b'P').cstr(""));
    c.send(b'E', Body::new().cstr("").i32(0));
    let (msgs, _) = c.sync();
    assert_eq!(types(&msgs), "12TDDC");
    assert_eq!(
        msgs[2].columns(),
        vec![
            ("id".to_string(), 20, 1),
            ("name".to_string(), 25, 1),
            ("score".to_string(), 701, 1),
            ("ok".to_string(), 16, 1),
            ("data".to_string(), 17, 1)
        ]
    );
    let row = msgs[3].row();
    assert_eq!(row[0].as_deref(), Some(&1i64.to_be_bytes()[..]));
    assert_eq!(row[1].as_deref(), Some(&b"one"[..]));
    assert_eq!(row[2].as_deref(), Some(&1.5f64.to_be_bytes()[..]));
    assert_eq!(row[3].as_deref(), Some(&[1u8][..]));
    assert_eq!(row[4], None);
    let row = msgs[4].row();
    assert_eq!(row[4].as_deref(), Some(&[0u8, 255, 7][..]));
    assert_eq!(msgs[5].text(), "SELECT 2");

    // Mixed result formats: first text, second binary.
    c.send(b'P', Body::new().cstr("").cstr("SELECT 7::int4, 7::int4").i16(0));
    c.send(b'B', Body::new().cstr("").cstr("").i16(0).i16(0).i16(2).i16(0).i16(1));
    c.send(b'E', Body::new().cstr("").i32(0));
    let (msgs, _) = c.sync();
    assert_eq!(msgs[2].row(), vec![Some(b"7".to_vec()), Some(7i32.to_be_bytes().to_vec())]);

    // Close a statement; using it afterwards fails.
    c.send(b'C', Body::new().u8(b'S').cstr("ins"));
    c.send(b'B', Body::new().cstr("").cstr("ins").i16(0).i16(0).i16(0));
    let (msgs, _) = c.sync();
    assert_eq!(types(&msgs), "3E");
    assert_eq!(msgs[1].fields()[&'C'], "26000");
    server.shutdown();
}

#[test]
fn extended_query_errors_skip_until_sync() {
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::connect(addr);
    // The failing Parse makes the server ignore Bind and Execute.
    c.send(b'P', Body::new().cstr("").cstr("SELECT * FROM missing").i16(0));
    c.send(b'B', Body::new().cstr("").cstr("").i16(0).i16(0).i16(0));
    c.send(b'E', Body::new().cstr("").i32(0));
    let (msgs, status) = c.sync();
    assert_eq!((types(&msgs).as_str(), status), ("E", b'I'));
    assert_eq!(msgs[0].fields()[&'C'], "42P01");
    // The connection is usable again.
    c.send(b'P', Body::new().cstr("").cstr("SELECT $1::int + 1").i16(0));
    c.send(b'B', Body::new().cstr("").cstr("").i16(0).i16(1).param(Some(b"41")).i16(0));
    c.send(b'E', Body::new().cstr("").i32(0));
    let (msgs, _) = c.sync();
    assert_eq!(types(&msgs), "12DC");
    assert_eq!(msgs[2].text_row(), vec![s("42")]);

    // Wrong number of parameters, bad parameter text, duplicate names.
    c.send(b'P', Body::new().cstr("p").cstr("SELECT $1::int").i16(0));
    c.send(b'B', Body::new().cstr("").cstr("p").i16(0).i16(0).i16(0));
    assert_eq!(c.sync().0[1].fields()[&'C'], "08P01");
    c.send(b'B', Body::new().cstr("").cstr("p").i16(0).i16(1).param(Some(b"abc")).i16(0));
    assert_eq!(c.sync().0[0].fields()[&'C'], "22P02");
    c.send(b'P', Body::new().cstr("p").cstr("SELECT 1").i16(0));
    assert_eq!(c.sync().0[0].fields()[&'C'], "42P05");
    // Multiple statements cannot be prepared.
    c.send(b'P', Body::new().cstr("").cstr("SELECT 1; SELECT 2").i16(0));
    assert_eq!(c.sync().0[0].fields()[&'C'], "42601");
    // An empty statement.
    c.send(b'P', Body::new().cstr("").cstr("").i16(0));
    c.send(b'B', Body::new().cstr("").cstr("").i16(0).i16(0).i16(0));
    c.send(b'E', Body::new().cstr("").i32(0));
    assert_eq!(types(&c.sync().0), "12I");
    // Flush delivers pending output without ending the cycle.
    c.send(b'P', Body::new().cstr("").cstr("SELECT 1").i16(0));
    c.send(b'H', Body::new());
    assert_eq!(c.recv().ty, b'1');
    c.sync();
    server.shutdown();
}

#[test]
fn portals_with_row_limits() {
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::connect(addr);
    c.query("BEGIN");
    c.send(b'P', Body::new().cstr("").cstr("SELECT g FROM generate_series(1, 5) g").i16(0));
    c.send(b'B', Body::new().cstr("cur").cstr("").i16(0).i16(0).i16(0));
    c.send(b'E', Body::new().cstr("cur").i32(2));
    let (msgs, status) = c.sync();
    assert_eq!((types(&msgs).as_str(), status), ("12DDs", b'T'));
    // The named portal survives the Sync inside the transaction.
    c.send(b'E', Body::new().cstr("cur").i32(2));
    c.send(b'E', Body::new().cstr("cur").i32(2));
    let (msgs, _) = c.sync();
    assert_eq!(types(&msgs), "DDsDC");
    assert_eq!(msgs[3].text_row(), vec![s("5")]);
    assert_eq!(msgs[4].text(), "SELECT 5");
    // Binding the same portal name again is an error; closing works.
    c.send(b'P', Body::new().cstr("").cstr("SELECT 1").i16(0));
    c.send(b'B', Body::new().cstr("cur").cstr("").i16(0).i16(0).i16(0));
    assert_eq!(c.sync().0[1].fields()[&'C'], "42P03");
    assert_eq!(c.query("ROLLBACK").1, b'I');
    // Portals end with their transaction.
    c.send(b'E', Body::new().cstr("cur").i32(0));
    assert_eq!(c.sync().0[0].fields()[&'C'], "34000");
    server.shutdown();
}

#[test]
fn copy_in_and_out() {
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::connect(addr);
    c.rows("CREATE TABLE t (id int PRIMARY KEY, name text, n numeric(6,2))");

    c.send(b'Q', Body::new().cstr("COPY t FROM STDIN"));
    let m = c.recv();
    assert_eq!((m.ty, &m.body[..]), (b'G', &[0, 0, 3, 0, 0, 0, 0, 0, 0][..]));
    // Data split at arbitrary points across CopyData messages.
    c.send(b'd', Body::new().bytes(b"1\tone\t1.5\n2\t\\N\t"));
    c.send(b'd', Body::new().bytes(b"\\N\n3\ttab\\there\t3\n"));
    c.send(b'c', Body::new());
    let (msgs, status) = c.until_ready();
    assert_eq!((msgs[0].text().as_str(), status), ("COPY 3", b'I'));
    assert_eq!(
        c.rows("SELECT * FROM t ORDER BY id"),
        vec![vec![s("1"), s("one"), s("1.50")], vec![s("2"), None, None], vec![s("3"), s("tab\there"), s("3.00")]]
    );

    // COPY TO in text format.
    let (msgs, _) = c.query("COPY t TO STDOUT");
    assert_eq!(types(&msgs), "HdddcC");
    assert_eq!(msgs[1].body, b"1\tone\t1.50\n");
    assert_eq!(msgs[2].body, b"2\t\\N\t\\N\n");
    assert_eq!(msgs[3].body, b"3\ttab\\there\t3.00\n");
    assert_eq!(msgs[5].text(), "COPY 3");

    // CSV with header, a column list and a query source.
    let (msgs, _) =
        c.query("COPY (SELECT name, id FROM t WHERE id < 3 ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER)");
    let lines: Vec<&[u8]> = msgs.iter().filter(|m| m.ty == b'd').map(|m| &m.body[..]).collect();
    assert_eq!(lines, vec![&b"name,id\n"[..], b"one,1\n", b",2\n"]);

    c.send(b'Q', Body::new().cstr("COPY t (name, id) FROM STDIN WITH (FORMAT csv, HEADER true)"));
    assert_eq!(c.recv().ty, b'G');
    c.send(b'd', Body::new().bytes(b"name,id\n\"quoted, \"\"csv\"\"\nline\",10\n,11\n"));
    c.send(b'c', Body::new());
    assert_eq!(c.until_ready().0[0].text(), "COPY 2");
    assert_eq!(
        c.rows("SELECT id, name FROM t WHERE id >= 10 ORDER BY id"),
        vec![vec![s("10"), s("quoted, \"csv\"\nline")], vec![s("11"), None]]
    );

    // A bad row fails the whole COPY; nothing of it is kept.
    c.send(b'Q', Body::new().cstr("COPY t FROM STDIN"));
    assert_eq!(c.recv().ty, b'G');
    c.send(b'd', Body::new().bytes(b"20\tok\t1\n21\tbad\tnot-a-number\n"));
    c.send(b'd', Body::new().bytes(b"22\tlate\t1\n"));
    c.send(b'c', Body::new());
    let (msgs, status) = c.until_ready();
    assert_eq!((msgs[0].fields()[&'C'].as_str(), status), ("22P02", b'I'));
    assert_eq!(c.rows("SELECT count(*) FROM t WHERE id >= 20"), vec![vec![s("0")]]);
    // Duplicate key and wrong column count.
    c.send(b'Q', Body::new().cstr("COPY t FROM STDIN"));
    c.recv();
    c.send(b'd', Body::new().bytes(b"1\tdup\t1\n"));
    c.send(b'c', Body::new());
    assert_eq!(c.until_ready().0[0].fields()[&'C'], "23505");
    c.send(b'Q', Body::new().cstr("COPY t FROM STDIN"));
    c.recv();
    c.send(b'd', Body::new().bytes(b"30\tshort\n"));
    c.send(b'c', Body::new());
    assert_eq!(c.until_ready().0[0].fields()[&'C'], "22P04");

    // CopyFail aborts.
    c.send(b'Q', Body::new().cstr("COPY t FROM STDIN"));
    c.recv();
    c.send(b'd', Body::new().bytes(b"40\tx\t1\n"));
    c.send(b'f', Body::new().cstr("changed my mind"));
    let (msgs, _) = c.until_ready();
    assert_eq!(msgs[0].fields()[&'C'], "57014");
    assert!(msgs[0].fields()[&'M'].contains("changed my mind"));
    assert_eq!(c.rows("SELECT count(*) FROM t WHERE id >= 20"), vec![vec![s("0")]]);

    // Inside a transaction block COPY takes part in it.
    c.query("BEGIN");
    c.send(b'Q', Body::new().cstr("COPY t FROM STDIN"));
    c.recv();
    c.send(b'd', Body::new().bytes(b"50\tx\t1\n\\.\n"));
    c.send(b'c', Body::new());
    assert_eq!(c.until_ready().1, b'T');
    c.query("ROLLBACK");
    assert_eq!(c.rows("SELECT count(*) FROM t WHERE id = 50"), vec![vec![s("0")]]);

    // COPY through the extended protocol.
    c.send(b'P', Body::new().cstr("").cstr("COPY t FROM STDIN").i16(0));
    c.send(b'B', Body::new().cstr("").cstr("").i16(0).i16(0).i16(0));
    c.send(b'E', Body::new().cstr("").i32(0));
    c.send(b'H', Body::new());
    assert_eq!(types(&[c.recv(), c.recv(), c.recv()]), "12G");
    c.send(b'd', Body::new().bytes(b"60\text\t6\n"));
    c.send(b'c', Body::new());
    let (msgs, _) = c.sync();
    assert_eq!(msgs[0].text(), "COPY 1");

    // Unsupported variants are rejected cleanly.
    assert_eq!(c.error("COPY t FROM '/etc/passwd'"), "0A000");
    assert_eq!(c.error("COPY t TO STDOUT (FORMAT binary)"), "0A000");
    assert_eq!(c.error("COPY missing FROM STDIN"), "42P01");
    server.shutdown();
}

/// Runs the client side of SCRAM-SHA-256. Returns true if the server
/// accepted the password (and proved that it knows it, too).
fn scram_login(addr: std::net::SocketAddr, user: &str, password: &str) -> Result<Client, String> {
    let mut c = Client::startup(addr, &[("user", user)]);
    let m = c.recv();
    assert_eq!(m.ty, b'R');
    assert_eq!(&m.body[..4], &10i32.to_be_bytes(), "AuthenticationSASL");
    assert!(m.body[4..].starts_with(b"SCRAM-SHA-256\0"));
    let bare = "n=,r=testclientnonce";
    let first = format!("n,,{bare}");
    c.send(b'p', Body::new().cstr("SCRAM-SHA-256").i32(first.len() as i32).bytes(first.as_bytes()));
    let m = c.recv();
    assert_eq!(&m.body[..4], &11i32.to_be_bytes(), "AuthenticationSASLContinue");
    let server_first = String::from_utf8(m.body[4..].to_vec()).unwrap();
    let attr = |k: &str| server_first.split(',').find_map(|p| p.strip_prefix(k)).unwrap().to_string();
    let (nonce, salt, iterations) = (attr("r="), B64.decode(attr("s=")).unwrap(), attr("i=").parse::<u32>().unwrap());
    assert!(nonce.starts_with("testclientnonce") && nonce.len() > 20);
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{bare},{server_first},{without_proof}");
    let (proof, server_signature) = scram::client_proof(password, &salt, iterations, &auth_message);
    let last = format!("{without_proof},p={}", B64.encode(proof));
    c.send(b'p', Body::new().bytes(last.as_bytes()));
    let m = c.recv();
    if m.ty == b'E' {
        let f = m.fields();
        assert_eq!(f[&'S'], "FATAL");
        return Err(f[&'C'].clone());
    }
    assert_eq!(&m.body[..4], &12i32.to_be_bytes(), "AuthenticationSASLFinal");
    assert_eq!(m.body[4..], *format!("v={}", B64.encode(server_signature)).as_bytes());
    let ok = c.recv();
    assert_eq!((ok.ty, &ok.body[..]), (b'R', &[0, 0, 0, 0][..]));
    c.until_ready();
    Ok(c)
}

#[test]
fn scram_authentication() {
    let users = HashMap::from([("alice".to_string(), "s3cret".to_string())]);
    let (server, addr) = start(ServerConfig { auth: Auth::Password { users }, ..Default::default() });
    let mut c = scram_login(addr, "alice", "s3cret").expect("right password");
    assert_eq!(c.rows("SELECT current_user"), vec![vec![s("alice")]]);
    assert_eq!(scram_login(addr, "alice", "wrong").err().as_deref(), Some("28P01"));
    assert_eq!(scram_login(addr, "mallory", "s3cret").err().as_deref(), Some("28P01"));
    // Nothing can be done before authenticating.
    let mut c = Client::startup(addr, &[("user", "alice")]);
    c.recv();
    c.send(b'Q', Body::new().cstr("SELECT 1"));
    assert_eq!(c.recv().ty, b'E');
    assert!(c.try_recv().is_none());
    server.shutdown();
}

#[test]
fn cleartext_authentication() {
    let users = HashMap::from([("bob".to_string(), "pw".to_string())]);
    let config = ServerConfig { auth: Auth::Password { users }, allow_cleartext_password: true, ..Default::default() };
    let (server, addr) = start(config);
    let mut c = Client::startup(addr, &[("user", "bob")]);
    assert_eq!(c.recv().body, 3i32.to_be_bytes());
    c.send(b'p', Body::new().cstr("pw"));
    assert_eq!(c.recv().body, 0i32.to_be_bytes());
    c.until_ready();
    let mut c = Client::startup(addr, &[("user", "bob")]);
    c.recv();
    c.send(b'p', Body::new().cstr("nope"));
    assert_eq!(c.recv().fields()[&'C'], "28P01");
    server.shutdown();
}

#[test]
fn malformed_input_never_kills_the_server() {
    let (server, addr) = start(ServerConfig::default());
    // An oversized message length after a valid startup.
    let mut c = Client::connect(addr);
    c.stream.write_all(&[b'Q', 0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
    let m = c.recv();
    assert_eq!(m.fields()[&'C'], "08P01");
    assert!(c.try_recv().is_none());
    // An unknown message type and truncated message bodies.
    let mut c = Client::connect(addr);
    c.send(b'?', Body::new());
    assert_eq!(c.recv().fields()[&'C'], "08P01");
    let mut c = Client::connect(addr);
    c.send(b'B', Body::new().cstr("").cstr("").i16(5));
    assert_eq!(c.recv().fields()[&'C'], "08P01");
    let mut c = Client::connect(addr);
    c.send(b'Q', Body::new().bytes(b"SELECT 1 without terminator"));
    assert_eq!(c.recv().fields()[&'C'], "08P01");
    // Bad startup packets.
    for packet in [&[0u8, 0, 0, 3][..], &[0, 0, 0, 8, 0, 2, 0, 0], &[0, 0, 0, 9, 0, 3, 0, 0, 0], &[0xFF; 64]] {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s.write_all(packet).unwrap();
        let mut sink = Vec::new();
        let _ = s.read_to_end(&mut sink);
    }

    // Random bytes, before and after a valid startup. A fixed-seed
    // generator keeps the test reproducible.
    let mut state = 0x9E3779B97F4A7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for round in 0..60 {
        let len = (next() % 200) as usize + 1;
        let mut junk: Vec<u8> = (0..len).map(|_| next() as u8).collect();
        let mut stream = if round % 2 == 0 {
            TcpStream::connect(addr).unwrap()
        } else {
            // Make most garbage look like typed messages of small length.
            let types = b"QPBDECHSXdcfp";
            let mut at = 0;
            while at + 5 <= junk.len() {
                junk[at] = types[(next() % types.len() as u64) as usize];
                let body = (next() % 24) as u32;
                junk[at + 1..at + 5].copy_from_slice(&(body + 4).to_be_bytes());
                at += 5 + body as usize;
            }
            Client::connect(addr).stream
        };
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let _ = stream.write_all(&junk);
        let _ = stream.shutdown(std::net::Shutdown::Write);
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink);
    }
    // Still alive and well.
    let mut c = Client::connect(addr);
    assert_eq!(c.rows("SELECT 1"), vec![vec![s("1")]]);
    server.shutdown();
}

#[test]
fn many_connections_and_shutdown() {
    let (server, addr) = start(ServerConfig::default());
    let mut setup = Client::connect(addr);
    setup.rows("CREATE TABLE counter (id int PRIMARY KEY, n int); INSERT INTO counter VALUES (1, 0)");
    let threads: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(move || {
                let mut c = Client::connect(addr);
                for _ in 0..50 {
                    c.rows("UPDATE counter SET n = n + 1 WHERE id = 1");
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(setup.rows("SELECT n FROM counter"), vec![vec![s("400")]]);
    assert_eq!(server.connections(), 1);
    // Shutdown closes open connections and stops accepting new ones.
    server.shutdown();
    assert!(setup.try_recv().is_none());
    assert!(TcpStream::connect(addr).is_err());
}

#[cfg(unix)]
#[test]
fn unix_socket() {
    use std::os::unix::net::UnixStream;
    let dir = tempfile::tempdir().unwrap();
    let config = ServerConfig { unix_socket: Some(dir.path().to_path_buf()), ..Default::default() };
    let (server, addr) = start(config);
    let path = server.unix_socket_path().expect("socket path");
    assert_eq!(path, dir.path().join(format!(".s.PGSQL.{}", addr.port())));
    let mut s = UnixStream::connect(&path).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let body = Body::new().i32(196608).cstr("user").cstr("squad").u8(0);
    let mut packet = ((body.0.len() + 4) as u32).to_be_bytes().to_vec();
    packet.extend_from_slice(&body.0);
    s.write_all(&packet).unwrap();
    let mut first = [0u8; 9];
    s.read_exact(&mut first).unwrap();
    assert_eq!(first, [b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    server.shutdown();
    assert!(!path.exists(), "socket file is removed on shutdown");
}
