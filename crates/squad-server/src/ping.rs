//! A minimal client that checks whether a server accepts connections, for
//! `squad ping` and container health checks.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use rand::RngCore;

use crate::scram;

/// Where and as whom to connect.
#[derive(Debug, Clone)]
pub struct PingOptions {
    /// Host name or IP address; a value starting with `/` is the directory
    /// of a Unix domain socket.
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    /// Used if the server asks for a password.
    pub password: Option<String>,
    /// Limit for connecting and for each answer of the server.
    pub timeout: Duration,
}

impl Default for PingOptions {
    fn default() -> PingOptions {
        PingOptions {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "squad".into(),
            database: "squad".into(),
            password: None,
            timeout: Duration::from_secs(5),
        }
    }
}

/// What the server answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PingStatus {
    /// Logged in and ran `SELECT 1`.
    Ready,
    /// The server answered the startup packet properly but the login was
    /// not completed: it wants a password that was not given, or it
    /// refused the login (wrong password, TLS required). The server is up.
    Accepting(String),
}

trait Conn: Read + Write {}
impl<T: Read + Write> Conn for T {}

fn connect(opts: &PingOptions) -> io::Result<Box<dyn Conn>> {
    if opts.host.starts_with('/') {
        #[cfg(unix)]
        {
            let path = std::path::Path::new(&opts.host).join(format!(".s.PGSQL.{}", opts.port));
            let s = std::os::unix::net::UnixStream::connect(&path)?;
            s.set_read_timeout(Some(opts.timeout))?;
            s.set_write_timeout(Some(opts.timeout))?;
            return Ok(Box::new(s));
        }
        #[cfg(not(unix))]
        return Err(io::Error::new(io::ErrorKind::Unsupported, "Unix sockets are not available"));
    }
    let mut last = io::Error::new(io::ErrorKind::NotFound, format!("cannot resolve {}", opts.host));
    for addr in (opts.host.as_str(), opts.port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&addr, opts.timeout) {
            Ok(s) => {
                s.set_read_timeout(Some(opts.timeout))?;
                s.set_write_timeout(Some(opts.timeout))?;
                s.set_nodelay(true)?;
                return Ok(Box::new(s));
            }
            Err(e) => last = e,
        }
    }
    Err(last)
}

fn send(conn: &mut dyn Conn, ty: Option<u8>, body: &[u8]) -> io::Result<()> {
    let mut packet = Vec::with_capacity(body.len() + 5);
    packet.extend(ty);
    packet.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    packet.extend_from_slice(body);
    conn.write_all(&packet)
}

fn recv(conn: &mut dyn Conn) -> Result<(u8, Vec<u8>), String> {
    let mut header = [0u8; 5];
    conn.read_exact(&mut header).map_err(|e| format!("no answer from the server: {e}"))?;
    let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    if !(4..=1 << 20).contains(&len) {
        return Err("the server does not speak the PostgreSQL protocol".into());
    }
    let mut body = vec![0u8; len - 4];
    conn.read_exact(&mut body).map_err(|e| format!("connection lost: {e}"))?;
    Ok((header[0], body))
}

/// `(SQLSTATE, message)` of an ErrorResponse.
fn error_fields(body: &[u8]) -> (String, String) {
    let mut code = String::new();
    let mut message = String::new();
    for field in body.split(|b| *b == 0).filter(|f| !f.is_empty()) {
        let value = String::from_utf8_lossy(&field[1..]).into_owned();
        match field[0] {
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }
    }
    (code, message)
}

fn cstr(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn auth_code(body: &[u8]) -> Result<i32, String> {
    body.get(..4).map(|b| i32::from_be_bytes(b.try_into().unwrap())).ok_or_else(|| "malformed message".to_string())
}

/// SCRAM-SHA-256 as a client, after the server sent AuthenticationSASL.
fn scram_login(conn: &mut dyn Conn, password: &str) -> Result<(), String> {
    let mut raw = [0u8; 18];
    rand::thread_rng().fill_bytes(&mut raw);
    let client_nonce = B64.encode(raw);
    let bare = format!("n=,r={client_nonce}");
    let first = format!("n,,{bare}");
    let mut body = Vec::new();
    cstr(&mut body, scram::MECHANISM);
    body.extend_from_slice(&(first.len() as i32).to_be_bytes());
    body.extend_from_slice(first.as_bytes());
    send(conn, Some(b'p'), &body).map_err(|e| e.to_string())?;
    let server_first = match recv(conn)? {
        (b'R', b) if auth_code(&b)? == 11 => String::from_utf8_lossy(&b[4..]).into_owned(),
        (b'E', b) => return Err(error_fields(&b).1),
        _ => return Err("unexpected answer during SCRAM authentication".into()),
    };
    let attr = |key: &str| {
        server_first.split(',').find_map(|p| p.strip_prefix(key)).map(str::to_string).ok_or("malformed SCRAM message")
    };
    let nonce = attr("r=")?;
    let salt = B64.decode(attr("s=")?).map_err(|_| "malformed SCRAM salt")?;
    let iterations: u32 = attr("i=")?.parse().map_err(|_| "malformed SCRAM iteration count")?;
    if !nonce.starts_with(&client_nonce) {
        return Err("SCRAM nonce mismatch".into());
    }
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{bare},{server_first},{without_proof}");
    let (proof, server_signature) = scram::client_proof(password, &salt, iterations, &auth_message);
    let last = format!("{without_proof},p={}", B64.encode(proof));
    send(conn, Some(b'p'), last.as_bytes()).map_err(|e| e.to_string())?;
    match recv(conn)? {
        (b'R', b) if auth_code(&b)? == 12 => {
            if String::from_utf8_lossy(&b[4..]) != format!("v={}", B64.encode(server_signature)) {
                return Err("the server's SCRAM signature is wrong".into());
            }
            Ok(())
        }
        (b'E', b) => Err(error_fields(&b).1),
        _ => Err("unexpected answer during SCRAM authentication".into()),
    }
}

/// Connects, logs in if possible, and runs `SELECT 1`.
///
/// `Ok` means the server is up: [`PingStatus::Ready`] after a successful
/// query, [`PingStatus::Accepting`] if it answered properly but the login
/// could not be completed (no password given, wrong password, TLS
/// required). `Err` means it is not usable: no connection, no or a
/// malformed answer, too many clients, or a failing query.
pub fn ping(opts: &PingOptions) -> Result<PingStatus, String> {
    let mut conn = connect(opts).map_err(|e| format!("cannot connect to {}:{}: {e}", opts.host, opts.port))?;
    let conn = conn.as_mut();
    let mut startup = 196608i32.to_be_bytes().to_vec();
    for (k, v) in
        [("user", opts.user.as_str()), ("database", opts.database.as_str()), ("application_name", "squad ping")]
    {
        cstr(&mut startup, k);
        cstr(&mut startup, v);
    }
    startup.push(0);
    send(conn, None, &startup).map_err(|e| format!("cannot send to the server: {e}"))?;
    loop {
        let (ty, body) = recv(conn)?;
        match ty {
            b'R' => match auth_code(&body)? {
                0 => {}
                3 => {
                    let Some(pw) = &opts.password else {
                        return Ok(PingStatus::Accepting("the server asks for a password".into()));
                    };
                    let mut b = Vec::new();
                    cstr(&mut b, pw);
                    send(conn, Some(b'p'), &b).map_err(|e| e.to_string())?;
                }
                10 => {
                    let Some(pw) = &opts.password else {
                        return Ok(PingStatus::Accepting("the server asks for a password".into()));
                    };
                    if let Err(e) = scram_login(conn, pw) {
                        return Ok(PingStatus::Accepting(format!("login failed: {e}")));
                    }
                }
                other => return Ok(PingStatus::Accepting(format!("unsupported authentication method {other}"))),
            },
            b'E' => {
                let (code, message) = error_fields(&body);
                // Class 28: the server works but does not let this login
                // in (password, TLS policy).
                if code.starts_with("28") {
                    return Ok(PingStatus::Accepting(format!("login refused: {message}")));
                }
                return Err(format!("{message} (SQLSTATE {code})"));
            }
            b'Z' => break,
            _ => {}
        }
    }
    let mut q = Vec::new();
    cstr(&mut q, "SELECT 1");
    send(conn, Some(b'Q'), &q).map_err(|e| e.to_string())?;
    let mut ok = false;
    loop {
        let (ty, body) = recv(conn)?;
        match ty {
            b'D' => ok = body.ends_with(b"1"),
            b'E' => {
                let (code, message) = error_fields(&body);
                return Err(format!("SELECT 1 failed: {message} (SQLSTATE {code})"));
            }
            b'Z' => break,
            _ => {}
        }
    }
    let _ = send(conn, Some(b'X'), &[]);
    if ok {
        Ok(PingStatus::Ready)
    } else {
        Err("SELECT 1 returned no row".into())
    }
}
