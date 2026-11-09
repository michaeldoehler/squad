//! Shared helpers: a server on a free port and a minimal protocol client.
#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use squad::Database;
use squad_server::{Server, ServerConfig, ServerHandle};

pub fn start(config: ServerConfig) -> (ServerHandle, SocketAddr) {
    start_with(Database::open_in_memory().expect("open database"), config)
}

pub fn start_with(db: Database, config: ServerConfig) -> (ServerHandle, SocketAddr) {
    let mut server = Server::new(db, config);
    let addr = server.bind("127.0.0.1:0").expect("bind");
    (server.spawn(), addr)
}

/// True if a program can be run (used to skip tests that need the
/// Postgres client tools).
pub fn have(program: &str) -> bool {
    Command::new(program).arg("--version").output().is_ok_and(|o| o.status.success())
}

/// Runs psql against the server; returns stdout and stderr.
pub fn psql(addr: SocketAddr, args: &[&str]) -> (String, String) {
    let out = Command::new("psql")
        .args(["-h", "127.0.0.1", "-p", &addr.port().to_string(), "-U", "squad", "-d", "squad", "-X", "-A", "-t"])
        .args(args)
        .env("PGPASSWORD", "unused")
        .env("PGCONNECT_TIMEOUT", "10")
        .output()
        .expect("run psql");
    (String::from_utf8_lossy(&out.stdout).into_owned(), String::from_utf8_lossy(&out.stderr).into_owned())
}

pub fn psql_file(addr: SocketAddr, file: &Path) -> (String, String) {
    psql(addr, &["-q", "-f", file.to_str().expect("utf-8 path")])
}

/// One backend message.
#[derive(Debug, Clone, PartialEq)]
pub struct Msg {
    pub ty: u8,
    pub body: Vec<u8>,
}

impl Msg {
    /// Fields of an ErrorResponse / NoticeResponse by their code.
    pub fn fields(&self) -> HashMap<char, String> {
        self.body
            .split(|b| *b == 0)
            .filter(|f| !f.is_empty())
            .map(|f| (f[0] as char, String::from_utf8_lossy(&f[1..]).into_owned()))
            .collect()
    }

    /// Column values of a DataRow; `None` is NULL.
    pub fn row(&self) -> Vec<Option<Vec<u8>>> {
        assert_eq!(self.ty, b'D');
        let n = i16::from_be_bytes([self.body[0], self.body[1]]) as usize;
        let mut at = 2;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let len = i32::from_be_bytes(self.body[at..at + 4].try_into().unwrap());
            at += 4;
            if len < 0 {
                out.push(None);
            } else {
                out.push(Some(self.body[at..at + len as usize].to_vec()));
                at += len as usize;
            }
        }
        out
    }

    pub fn text_row(&self) -> Vec<Option<String>> {
        self.row().into_iter().map(|c| c.map(|b| String::from_utf8(b).expect("utf-8 text"))).collect()
    }

    /// Text of a CommandComplete tag or any single C-string body.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(self.body.split(|b| *b == 0).next().unwrap_or_default()).into_owned()
    }

    /// `(name, type oid, format)` of each RowDescription field.
    pub fn columns(&self) -> Vec<(String, u32, i16)> {
        assert_eq!(self.ty, b'T');
        let n = i16::from_be_bytes([self.body[0], self.body[1]]) as usize;
        let mut at = 2;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let end = at + self.body[at..].iter().position(|b| *b == 0).unwrap();
            let name = String::from_utf8_lossy(&self.body[at..end]).into_owned();
            at = end + 1;
            let oid = u32::from_be_bytes(self.body[at + 6..at + 10].try_into().unwrap());
            let format = i16::from_be_bytes(self.body[at + 16..at + 18].try_into().unwrap());
            at += 18;
            out.push((name, oid, format));
        }
        out
    }
}

/// Builder for frontend message bodies.
#[derive(Default)]
pub struct Body(pub Vec<u8>);

impl Body {
    pub fn new() -> Body {
        Body::default()
    }
    pub fn cstr(mut self, s: &str) -> Body {
        self.0.extend_from_slice(s.as_bytes());
        self.0.push(0);
        self
    }
    pub fn i16(mut self, v: i16) -> Body {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    pub fn i32(mut self, v: i32) -> Body {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    pub fn u8(mut self, v: u8) -> Body {
        self.0.push(v);
        self
    }
    pub fn bytes(mut self, b: &[u8]) -> Body {
        self.0.extend_from_slice(b);
        self
    }
    /// A length-prefixed parameter value; `None` is NULL.
    pub fn param(self, v: Option<&[u8]>) -> Body {
        match v {
            Some(b) => self.i32(b.len() as i32).bytes(b),
            None => self.i32(-1),
        }
    }
}

pub struct Client {
    pub stream: TcpStream,
}

impl Client {
    /// Opens a socket and sends the startup message; authentication is
    /// left to the caller.
    pub fn startup(addr: SocketAddr, params: &[(&str, &str)]) -> Client {
        let stream = TcpStream::connect(addr).expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        stream.set_nodelay(true).unwrap();
        let mut c = Client { stream };
        let mut body = Body::new().i32(196608);
        for (k, v) in params {
            body = body.cstr(k).cstr(v);
        }
        body = body.u8(0);
        let mut packet = ((body.0.len() + 4) as u32).to_be_bytes().to_vec();
        packet.extend_from_slice(&body.0);
        c.stream.write_all(&packet).unwrap();
        c
    }

    /// Connects to a server with trust authentication.
    pub fn connect(addr: SocketAddr) -> Client {
        let mut c = Client::startup(addr, &[("user", "squad"), ("database", "squad")]);
        let auth = c.recv();
        assert_eq!((auth.ty, &auth.body[..]), (b'R', &[0, 0, 0, 0][..]), "expected AuthenticationOk");
        c.until_ready();
        c
    }

    pub fn send(&mut self, ty: u8, body: Body) {
        let mut packet = vec![ty];
        packet.extend_from_slice(&((body.0.len() + 4) as u32).to_be_bytes());
        packet.extend_from_slice(&body.0);
        self.stream.write_all(&packet).unwrap();
    }

    pub fn try_recv(&mut self) -> Option<Msg> {
        let mut header = [0u8; 5];
        self.stream.read_exact(&mut header).ok()?;
        let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
        let mut body = vec![0u8; len - 4];
        self.stream.read_exact(&mut body).ok()?;
        Some(Msg { ty: header[0], body })
    }

    pub fn recv(&mut self) -> Msg {
        self.try_recv().expect("server closed the connection")
    }

    /// Reads up to and including ReadyForQuery. Returns the messages
    /// before it and the transaction status byte.
    pub fn until_ready(&mut self) -> (Vec<Msg>, u8) {
        let mut out = Vec::new();
        loop {
            let m = self.recv();
            if m.ty == b'Z' {
                return (out, m.body[0]);
            }
            out.push(m);
        }
    }

    /// Simple query; returns all response messages and the status byte.
    pub fn query(&mut self, sql: &str) -> (Vec<Msg>, u8) {
        self.send(b'Q', Body::new().cstr(sql));
        self.until_ready()
    }

    /// Simple query expected to succeed: the text rows of all results.
    pub fn rows(&mut self, sql: &str) -> Vec<Vec<Option<String>>> {
        let (msgs, _) = self.query(sql);
        if let Some(e) = msgs.iter().find(|m| m.ty == b'E') {
            panic!("query failed: {sql}: {:?}", e.fields());
        }
        msgs.iter().filter(|m| m.ty == b'D').map(Msg::text_row).collect()
    }

    /// Simple query expected to fail: the SQLSTATE.
    pub fn error(&mut self, sql: &str) -> String {
        let (msgs, _) = self.query(sql);
        let e = msgs.iter().find(|m| m.ty == b'E').unwrap_or_else(|| panic!("expected an error from: {sql}"));
        e.fields()[&'C'].clone()
    }

    pub fn sync(&mut self) -> (Vec<Msg>, u8) {
        self.send(b'S', Body::new());
        self.until_ready()
    }
}

/// Message type bytes of a response, for compact assertions.
pub fn types(msgs: &[Msg]) -> String {
    msgs.iter().map(|m| m.ty as char).collect()
}
