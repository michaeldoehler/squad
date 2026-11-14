//! TLS, cancel requests, connection limits, timeouts, message size limits,
//! the trust policy, users files and notices.

mod common;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{start, start_with, Body, Client};
use squad::Database;
use squad_server::{Auth, Serve, Server, ServerConfig, TlsConfig};
use tokio_postgres::NoTls;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

/// A self-signed certificate for 127.0.0.1 and localhost:
/// `(certificate PEM, key PEM, certificate DER)`.
fn certificate() -> (String, String, Vec<u8>) {
    let ck = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string(), "localhost".to_string()]).unwrap();
    (ck.cert.pem(), ck.key_pair.serialize_pem(), ck.cert.der().to_vec())
}

fn tls_config() -> (TlsConfig, Vec<u8>) {
    let (cert, key, der) = certificate();
    (TlsConfig::from_pem(cert.as_bytes(), key.as_bytes()).unwrap(), der)
}

fn client_tls(root_der: &[u8]) -> tokio_postgres_rustls::MakeRustlsConnect {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(root_der.to_vec())).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tokio_postgres_rustls::MakeRustlsConnect::new(config)
}

fn conninfo(addr: SocketAddr, extra: &str) -> String {
    format!("host=127.0.0.1 port={} user=squad dbname=squad {extra}", addr.port())
}

/// Sends an untyped request (SSLRequest, CancelRequest) on a new socket.
fn raw_request(addr: SocketAddr, code: u32, payload: &[u8]) -> TcpStream {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let mut packet = ((payload.len() + 8) as u32).to_be_bytes().to_vec();
    packet.extend_from_slice(&code.to_be_bytes());
    packet.extend_from_slice(payload);
    s.write_all(&packet).unwrap();
    s
}

const SSL_REQUEST: u32 = 80877103;
const CANCEL_REQUEST: u32 = 80877102;

/// True once the peer has closed the connection (reads end or fail).
fn closed_within(s: &mut TcpStream, limit: Duration) -> bool {
    s.set_read_timeout(Some(limit)).unwrap();
    let mut buf = [0u8; 1024];
    loop {
        match s.read(&mut buf) {
            Ok(0) => return true,
            Ok(_) => continue,
            Err(e) => return e.kind() == std::io::ErrorKind::ConnectionReset,
        }
    }
}

#[test]
fn tls_with_tokio_postgres() {
    let (tls, root) = tls_config();
    let (server, addr) = start(ServerConfig { tls: Some(tls), ..Default::default() });
    runtime().block_on(async {
        let (client, connection) =
            tokio_postgres::connect(&conninfo(addr, "sslmode=require"), client_tls(&root)).await.expect("TLS connect");
        tokio::spawn(connection);
        let row = client.query_one("SELECT 40 + 2", &[]).await.unwrap();
        assert_eq!(row.get::<_, i32>(0), 42);
        // Large results cross the encrypted stream intact.
        let row = client.query_one("SELECT sum(length(repeat('x', g))) FROM generate_series(1, 2000) g", &[]).await;
        assert_eq!(row.unwrap().get::<_, i64>(0), 2001000);
        let rows = client.query("SELECT repeat('y', 100000) FROM generate_series(1, 5)", &[]).await.unwrap();
        assert!(rows.iter().all(|r| r.get::<_, &str>(0).len() == 100000));
        // Without TLS required, plain connections still work.
        let (plain, connection) = tokio_postgres::connect(&conninfo(addr, "sslmode=disable"), NoTls).await.unwrap();
        tokio::spawn(connection);
        assert_eq!(plain.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
    });
    server.shutdown();
}

#[test]
fn tls_rejects_a_client_that_does_not_trust_the_certificate() {
    let (tls, _) = tls_config();
    let (_, _, other_root) = certificate();
    let (server, addr) = start(ServerConfig { tls: Some(tls), ..Default::default() });
    runtime().block_on(async {
        let result = tokio_postgres::connect(&conninfo(addr, "sslmode=require"), client_tls(&other_root)).await;
        assert!(result.is_err());
    });
    // The server keeps serving.
    let mut c = Client::connect(addr);
    assert_eq!(c.rows("SELECT 1"), vec![vec![Some("1".to_string())]]);
    server.shutdown();
}

#[test]
fn ssl_request_is_answered_by_configuration() {
    // Without a certificate: 'N', and the client continues in clear text.
    let (server, addr) = start(ServerConfig::default());
    let mut s = raw_request(addr, SSL_REQUEST, &[]);
    let mut answer = [0u8; 1];
    s.read_exact(&mut answer).unwrap();
    assert_eq!(&answer, b"N");
    server.shutdown();

    // With one: 'S'. Plain-text data sent along with the request is a
    // protocol violation (CVE-2021-23214).
    let (tls, _) = tls_config();
    let (server, addr) = start(ServerConfig { tls: Some(tls), ..Default::default() });
    let mut s = raw_request(addr, SSL_REQUEST, &[]);
    s.read_exact(&mut answer).unwrap();
    assert_eq!(&answer, b"S");
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let mut packet = 8u32.to_be_bytes().to_vec();
    packet.extend_from_slice(&SSL_REQUEST.to_be_bytes());
    packet.extend_from_slice(&[0, 0, 0, 8, 0, 3, 0, 0]);
    s.write_all(&packet).unwrap();
    s.read_exact(&mut answer).unwrap();
    assert_eq!(&answer, b"S");
    let mut c = Client { stream: s };
    let e = c.recv();
    assert_eq!((e.ty, e.fields()[&'C'].as_str()), (b'E', "08P01"));
    assert!(e.fields()[&'M'].contains("unencrypted data after SSL request"));
    server.shutdown();
}

#[test]
fn require_tls_refuses_plain_tcp() {
    let (tls, root) = tls_config();
    let config = ServerConfig { tls: Some(tls), require_tls: true, ..Default::default() };
    let (server, addr) = start(config);
    let mut c = Client::startup(addr, &[("user", "squad")]);
    let e = c.recv();
    assert_eq!((e.ty, e.fields()[&'S'].as_str(), e.fields()[&'C'].as_str()), (b'E', "FATAL", "28000"));
    assert!(e.fields()[&'M'].contains("no encryption"), "{:?}", e.fields());
    assert!(c.try_recv().is_none());
    runtime().block_on(async {
        let err = tokio_postgres::connect(&conninfo(addr, "sslmode=disable"), NoTls).await.err().unwrap();
        assert_eq!(err.code().map(|c| c.code()), Some("28000"));
        let (client, connection) =
            tokio_postgres::connect(&conninfo(addr, "sslmode=require"), client_tls(&root)).await.unwrap();
        tokio::spawn(connection);
        assert_eq!(client.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
    });
    server.shutdown();

    // Requiring TLS without a certificate is a configuration error.
    let mut server =
        Server::new(Database::open_in_memory().unwrap(), ServerConfig { require_tls: true, ..Default::default() });
    assert_eq!(server.bind("127.0.0.1:0").unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
}

#[cfg(unix)]
#[test]
fn require_tls_does_not_apply_to_unix_sockets() {
    let dir = tempfile::tempdir().unwrap();
    let (tls, _) = tls_config();
    let config = ServerConfig {
        tls: Some(tls),
        require_tls: true,
        unix_socket: Some(dir.path().to_path_buf()),
        ..Default::default()
    };
    let mut server = Server::new(Database::open_in_memory().unwrap(), config);
    server.bind("127.0.0.1:0").unwrap();
    let path = server.unix_socket_path().unwrap();
    let handle = server.spawn();
    let mut s = std::os::unix::net::UnixStream::connect(&path).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    // An SSL request over the socket is declined; the login succeeds.
    s.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).unwrap();
    let mut answer = [0u8; 1];
    s.read_exact(&mut answer).unwrap();
    assert_eq!(&answer, b"N");
    let mut body = Body::new().i32(196608).cstr("user").cstr("squad").u8(0).0;
    let mut packet = ((body.len() + 4) as u32).to_be_bytes().to_vec();
    packet.append(&mut body);
    s.write_all(&packet).unwrap();
    let mut header = [0u8; 9];
    s.read_exact(&mut header).unwrap();
    assert_eq!(header, [b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    handle.shutdown();
}

const ENDLESS: &str = "SELECT count(*) FROM generate_series(1, 100000) a, generate_series(1, 100000) b";

#[test]
fn cancel_request_from_tokio_postgres() {
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let (client, connection) = tokio_postgres::connect(&conninfo(addr, ""), NoTls).await.unwrap();
        tokio::spawn(connection);
        let token = client.cancel_token();
        let canceller = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            token.cancel_query(NoTls).await.unwrap();
        });
        let started = Instant::now();
        let err = client.query_one(ENDLESS, &[]).await.unwrap_err();
        canceller.await.unwrap();
        assert_eq!(err.code().map(|c| c.code()), Some("57014"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(30));
        // The connection stays usable.
        assert_eq!(client.query_one("SELECT 7", &[]).await.unwrap().get::<_, i32>(0), 7);
    });
    server.shutdown();
}

#[test]
fn cancel_request_needs_the_right_key() {
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::startup(addr, &[("user", "squad")]);
    let (msgs, _) = c.until_ready();
    let key = msgs.iter().find(|m| m.ty == b'K').expect("BackendKeyData");
    let (pid, secret) = (&key.body[0..4], &key.body[4..8]);
    c.send(b'Q', Body::new().cstr(ENDLESS));
    std::thread::sleep(Duration::from_millis(200));
    // A wrong secret is ignored; the requester gets no answer either way.
    let wrong: Vec<u8> = pid.iter().chain(&[secret[0] ^ 1, secret[1], secret[2], secret[3]]).copied().collect();
    let mut s = raw_request(addr, CANCEL_REQUEST, &wrong);
    assert!(closed_within(&mut s, Duration::from_secs(10)));
    c.stream.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    assert!(c.try_recv().is_none(), "a wrong key must not cancel");
    c.stream.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let right: Vec<u8> = pid.iter().chain(secret).copied().collect();
    let mut s = raw_request(addr, CANCEL_REQUEST, &right);
    assert!(closed_within(&mut s, Duration::from_secs(10)));
    let (msgs, status) = c.until_ready();
    let e = msgs.iter().find(|m| m.ty == b'E').expect("error");
    assert_eq!(e.fields()[&'C'], "57014");
    assert_eq!(e.fields()[&'M'], "canceling statement due to user request");
    assert_eq!(status, b'I');
    // Another connection's key differs.
    let mut other = Client::startup(addr, &[("user", "squad")]);
    let (msgs, _) = other.until_ready();
    let other_key = msgs.iter().find(|m| m.ty == b'K').unwrap();
    assert_ne!(other_key.body, key.body);
    server.shutdown();
}

#[test]
fn statement_timeout_over_the_wire() {
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::startup(addr, &[("user", "squad"), ("options", "-c statement_timeout=200")]);
    c.until_ready();
    let (msgs, _) = c.query(ENDLESS);
    let e = msgs.iter().find(|m| m.ty == b'E').expect("error");
    assert_eq!(e.fields()[&'C'], "57014");
    assert_eq!(e.fields()[&'M'], "canceling statement due to statement timeout");
    server.shutdown();
}

#[test]
fn max_connections_refuses_with_53300() {
    let (server, addr) = start(ServerConfig { max_connections: 2, ..Default::default() });
    let a = Client::connect(addr);
    let mut b = Client::connect(addr);
    let mut c = Client::startup(addr, &[("user", "squad")]);
    let e = c.recv();
    assert_eq!((e.ty, e.fields()[&'S'].as_str(), e.fields()[&'C'].as_str()), (b'E', "FATAL", "53300"));
    assert_eq!(e.fields()[&'M'], "sorry, too many clients already");
    assert!(c.try_recv().is_none());
    assert_eq!(server.connections(), 2);
    // Cancel requests are still served while the server is full.
    let mut s = raw_request(addr, CANCEL_REQUEST, &[0, 0, 0, 1, 0, 0, 0, 1]);
    assert!(closed_within(&mut s, Duration::from_secs(10)));
    // Once a client leaves, the next one gets in.
    drop(a);
    let deadline = Instant::now() + Duration::from_secs(10);
    while server.connections() > 1 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut d = Client::connect(addr);
    assert_eq!(d.rows("SELECT 1").len(), 1);
    assert_eq!(b.rows("SELECT 2").len(), 1);
    server.shutdown();
}

#[test]
fn authentication_timeout_closes_silent_and_slow_clients() {
    let config = ServerConfig { authentication_timeout: Duration::from_millis(300), ..Default::default() };
    let users = HashMap::from([("u".to_string(), "pw".to_string())]);
    let (server, addr) = start(ServerConfig { auth: Auth::Password { users }, ..config });
    // Says nothing at all.
    let mut s = TcpStream::connect(addr).unwrap();
    let started = Instant::now();
    assert!(closed_within(&mut s, Duration::from_secs(10)));
    assert!(started.elapsed() < Duration::from_secs(5));
    // Trickles the startup packet byte by byte.
    let mut s = TcpStream::connect(addr).unwrap();
    let started = Instant::now();
    for b in [0u8, 0, 0, 40, 0, 3, 0, 0] {
        if s.write_all(&[b]).is_err() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(closed_within(&mut s, Duration::from_secs(10)));
    assert!(started.elapsed() < Duration::from_secs(5));
    // Never answers the password request.
    let mut c = Client::startup(addr, &[("user", "u")]);
    assert_eq!(c.recv().ty, b'R');
    assert!(closed_within(&mut c.stream, Duration::from_secs(10)));
    server.shutdown();
    // After logging in, the timeout no longer applies.
    let (server, addr) = start(config_with_timeout());
    let mut c = Client::connect(addr);
    std::thread::sleep(Duration::from_millis(700));
    assert_eq!(c.rows("SELECT 1").len(), 1);
    server.shutdown();
}

fn config_with_timeout() -> ServerConfig {
    ServerConfig { authentication_timeout: Duration::from_millis(300), ..Default::default() }
}

#[test]
fn idle_timeout_ends_silent_sessions() {
    let (server, addr) = start(ServerConfig { idle_timeout: Some(Duration::from_millis(300)), ..Default::default() });
    let mut c = Client::connect(addr);
    // Busy clients are unaffected.
    for _ in 0..5 {
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(c.rows("SELECT 1").len(), 1);
    }
    std::thread::sleep(Duration::from_millis(600));
    let e = c.recv();
    assert_eq!((e.fields()[&'S'].as_str(), e.fields()[&'C'].as_str()), ("FATAL", "57P05"));
    assert!(c.try_recv().is_none());
    // Inside a transaction block the code is 25P03.
    let mut c = Client::connect(addr);
    c.query("BEGIN");
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(c.recv().fields()[&'C'], "25P03");
    server.shutdown();
}

#[test]
fn message_size_limits() {
    let (server, addr) = start(ServerConfig { max_message_size: 20_000, ..Default::default() });
    // Before authentication: at most 10,000 bytes, as PostgreSQL's
    // startup packet.
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    s.write_all(&[0, 0, 0x30, 0, 0, 3, 0, 0]).unwrap();
    let mut c = Client { stream: s };
    assert_eq!(c.recv().fields()[&'C'], "08P01");
    // A SASL response may not be large either.
    let users = HashMap::from([("u".to_string(), "pw".to_string())]);
    let (server2, addr2) = start(ServerConfig { auth: Auth::Password { users }, ..Default::default() });
    let mut c = Client::startup(addr2, &[("user", "u")]);
    assert_eq!(c.recv().ty, b'R');
    c.send(b'p', Body::new().bytes(&vec![b'x'; 50_000]));
    let e = c.recv();
    assert_eq!(e.fields()[&'C'], "08P01");
    assert!(e.fields()[&'M'].contains("exceeds the limit of 10000 bytes"), "{:?}", e.fields());
    server2.shutdown();
    // After authentication: the configured limit.
    let mut c = Client::connect(addr);
    let sql = format!("SELECT '{}'", "x".repeat(15_000));
    assert_eq!(c.rows(&sql).len(), 1);
    c.send(b'Q', Body::new().cstr(&format!("SELECT '{}'", "x".repeat(30_000))));
    let e = c.recv();
    assert_eq!((e.fields()[&'S'].as_str(), e.fields()[&'C'].as_str()), ("FATAL", "08P01"));
    assert!(c.try_recv().is_none());
    server.shutdown();
}

#[test]
fn trust_only_on_loopback_unless_allowed() {
    let db = Database::open_in_memory().unwrap();
    let err = db.serve("0.0.0.0:0").err().expect("trust on all interfaces must be refused");
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(err.to_string().contains("not a loopback address"), "{err}");
    db.serve("127.0.0.1:0").unwrap().shutdown();
    let mut server = Server::new(db.clone(), ServerConfig { trust_remote: true, ..Default::default() });
    server.bind("0.0.0.0:0").unwrap();
    server.spawn().shutdown();
    let users = HashMap::from([("u".to_string(), "pw".to_string())]);
    let mut server = Server::new(db, ServerConfig { auth: Auth::Password { users }, ..Default::default() });
    server.bind("0.0.0.0:0").unwrap();
    server.spawn().shutdown();
}

#[test]
fn verifiers_from_a_users_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("users");
    // `pg` was produced by PostgreSQL 16 for the password "secret".
    let pg = "SCRAM-SHA-256$4096:2p69YhUMsuJ9UY9GhURkFg==$btRVJP5LmOQTVAAf2sXJeDPPa4dj3mYOSahIsXWl8SU=:VQGgrY+lvmyvr0xiP+b7VjMzf4JJlB+lf1XRYBAA4Yc=";
    std::fs::write(&path, format!("# test logins\nalice:{pg}\nbob:plain pw\n")).unwrap();
    let users = squad_server::users::read_users_file(&path).unwrap();
    let (server, addr) = start(ServerConfig { auth: Auth::Verifiers { users: users.clone() }, ..Default::default() });
    runtime().block_on(async {
        for (user, pw, ok) in [("alice", "secret", true), ("bob", "plain pw", true), ("alice", "wrong", false)] {
            let info = format!("host=127.0.0.1 port={} user={user} password='{pw}' dbname=squad", addr.port());
            match tokio_postgres::connect(&info, NoTls).await {
                Ok((client, connection)) => {
                    assert!(ok, "{user}/{pw} should fail");
                    tokio::spawn(connection);
                    let row = client.query_one("SELECT current_user", &[]).await.unwrap();
                    assert_eq!(row.get::<_, String>(0), user);
                }
                Err(e) => {
                    assert!(!ok, "{user}/{pw}: {e}");
                    assert_eq!(e.code().map(|c| c.code()), Some("28P01"));
                }
            }
        }
    });
    server.shutdown();
    // Clear-text password authentication checks against the verifiers.
    let config = ServerConfig { auth: Auth::Verifiers { users }, allow_cleartext_password: true, ..Default::default() };
    let (server, addr) = start(config);
    for (pw, code) in [("secret", None), ("nope", Some("28P01"))] {
        let mut c = Client::startup(addr, &[("user", "alice")]);
        assert_eq!(c.recv().body, 3i32.to_be_bytes());
        c.send(b'p', Body::new().cstr(pw));
        let m = c.recv();
        match code {
            None => assert_eq!(m.body, 0i32.to_be_bytes()),
            Some(code) => assert_eq!(m.fields()[&'C'], code),
        }
    }
    server.shutdown();
}

#[test]
fn auth_debug_output_hides_passwords() {
    let users = HashMap::from([("u".to_string(), "hunter2".to_string())]);
    let config = ServerConfig { auth: Auth::Password { users }, ..Default::default() };
    let text = format!("{config:?}");
    assert!(text.contains("\"u\"") && !text.contains("hunter2"), "{text}");
}

#[test]
fn notices_reach_the_client() {
    let (server, addr) = start(ServerConfig::default());
    let mut c = Client::connect(addr);
    c.rows("CREATE TABLE t (a int)");
    let (msgs, _) = c.query("GRANT SELECT ON t TO PUBLIC; COMMENT ON TABLE t IS 'x'");
    assert_eq!(common::types(&msgs), "NCNC");
    let f = msgs[0].fields();
    assert_eq!((f[&'S'].as_str(), f[&'C'].as_str()), ("WARNING", "01000"));
    assert_eq!(f[&'M'], "squad has no privileges; GRANT has no effect");
    assert!(f.contains_key(&'H'));
    assert_eq!((msgs[2].fields()[&'S'].as_str(), msgs[2].fields()[&'C'].as_str()), ("NOTICE", "00000"));
    // Extended protocol: the notice comes before CommandComplete.
    c.send(b'P', Body::new().cstr("").cstr("REVOKE ALL ON t FROM PUBLIC").i16(0));
    c.send(b'B', Body::new().cstr("").cstr("").i16(0).i16(0).i16(0));
    c.send(b'E', Body::new().cstr("").i32(0));
    let (msgs, _) = c.sync();
    assert_eq!(common::types(&msgs), "12NC");
    assert_eq!(msgs[2].fields()[&'M'], "squad has no privileges; REVOKE has no effect");
    server.shutdown();
}

#[test]
fn notices_with_tokio_postgres() {
    let (server, addr) = start(ServerConfig::default());
    runtime().block_on(async {
        let (client, mut connection) = tokio_postgres::connect(&conninfo(addr, ""), NoTls).await.unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut messages = futures_util::stream::poll_fn(move |cx| connection.poll_message(cx));
            while let Some(Ok(m)) = futures_util::StreamExt::next(&mut messages).await {
                if let tokio_postgres::AsyncMessage::Notice(n) = m {
                    let _ = tx.send((n.severity().to_string(), n.code().code().to_string(), n.message().to_string()));
                }
            }
        });
        client.batch_execute("CREATE TABLE t (a int); CREATE EXTENSION IF NOT EXISTS pgcrypto").await.unwrap();
        let (severity, code, message) = rx.recv().await.unwrap();
        assert_eq!((severity.as_str(), code.as_str()), ("NOTICE", "00000"));
        assert_eq!(message, "squad has no extensions; CREATE EXTENSION \"pgcrypto\" has no effect");
    });
    server.shutdown();
}

#[test]
fn lock_table_over_the_wire() {
    let (server, addr) = start_with(Database::open_in_memory().unwrap(), ServerConfig::default());
    let mut c = Client::connect(addr);
    c.rows("CREATE TABLE t (a int)");
    assert_eq!(c.error("LOCK TABLE t IN ROW EXCLUSIVE MODE"), "25P01");
    c.rows("BEGIN");
    assert_eq!(c.error("LOCK TABLE t"), "0A000");
    c.rows("ROLLBACK");
    server.shutdown();
}
