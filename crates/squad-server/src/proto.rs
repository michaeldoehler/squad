//! Framing of the Postgres frontend/backend protocol, version 3.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Largest message a client may send before it has authenticated. The
/// startup packet has the same limit in PostgreSQL.
pub const PRE_AUTH_MAX_MESSAGE_LEN: usize = 10_000;

type TlsStream = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;

/// A client connection over TCP, TLS over TCP, or a Unix domain socket.
pub enum Stream {
    Tcp(TcpStream),
    Tls(Box<TlsStream>),
    #[cfg(unix)]
    Unix(UnixStream),
    /// Only while the connection is being switched to TLS.
    Detached,
}

fn detached() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "connection is being switched to TLS")
}

impl Stream {
    pub fn try_clone(&self) -> io::Result<Stream> {
        Ok(match self {
            Stream::Tcp(s) => Stream::Tcp(s.try_clone()?),
            Stream::Tls(s) => Stream::Tcp(s.sock.try_clone()?),
            #[cfg(unix)]
            Stream::Unix(s) => Stream::Unix(s.try_clone()?),
            Stream::Detached => return Err(detached()),
        })
    }

    pub fn shutdown(&self) {
        let _ = match self {
            Stream::Tcp(s) => s.shutdown(Shutdown::Both),
            Stream::Tls(s) => s.sock.shutdown(Shutdown::Both),
            #[cfg(unix)]
            Stream::Unix(s) => s.shutdown(Shutdown::Both),
            Stream::Detached => Ok(()),
        };
    }

    /// True for TCP connections, encrypted or not.
    pub fn is_tcp(&self) -> bool {
        matches!(self, Stream::Tcp(_) | Stream::Tls(_))
    }

    pub fn is_tls(&self) -> bool {
        matches!(self, Stream::Tls(_))
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_read_timeout(timeout),
            Stream::Tls(s) => s.sock.set_read_timeout(timeout),
            #[cfg(unix)]
            Stream::Unix(s) => s.set_read_timeout(timeout),
            Stream::Detached => Err(detached()),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            Stream::Tls(s) => s.read(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.read(buf),
            Stream::Detached => Err(detached()),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.write(buf),
            Stream::Detached => Err(detached()),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.flush(),
            Stream::Tls(s) => s.flush(),
            #[cfg(unix)]
            Stream::Unix(s) => s.flush(),
            Stream::Detached => Err(detached()),
        }
    }
}

/// A read that ran into the socket's timeout reports `WouldBlock` on Unix.
fn timed_out(e: io::Error) -> io::Error {
    match e.kind() {
        io::ErrorKind::WouldBlock => io::ErrorKind::TimedOut.into(),
        _ => e,
    }
}

fn protocol_error(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Buffered message I/O. Output is collected and written out whenever the
/// server is about to wait for the client, so a pipeline of messages is
/// answered with as few packets as possible.
pub struct Io {
    stream: Stream,
    inbuf: Vec<u8>,
    inpos: usize,
    pub out: Vec<u8>,
    /// Reads fail with [`io::ErrorKind::TimedOut`] after this instant.
    pub deadline: Option<Instant>,
    /// A read timeout is set on the socket.
    timeout_set: bool,
    /// Largest typed message accepted.
    pub max_message: usize,
}

impl Io {
    pub fn new(stream: Stream) -> Io {
        Io {
            stream,
            inbuf: Vec::with_capacity(8192),
            inpos: 0,
            out: Vec::with_capacity(8192),
            deadline: None,
            timeout_set: false,
            max_message: PRE_AUTH_MAX_MESSAGE_LEN,
        }
    }

    pub fn stream(&self) -> &Stream {
        &self.stream
    }

    /// Switches the connection to TLS after the server answered an
    /// SSLRequest with 'S', and completes the handshake.
    pub fn start_tls(&mut self, config: Arc<rustls::ServerConfig>) -> io::Result<()> {
        if self.inbuf.len() > self.inpos {
            // The client sent data the encryption would not cover
            // (CVE-2021-23214).
            return Err(protocol_error("received unencrypted data after SSL request"));
        }
        self.flush()?;
        let Stream::Tcp(sock) = std::mem::replace(&mut self.stream, Stream::Detached) else {
            return Err(protocol_error("TLS is only available over TCP"));
        };
        if let Some(remaining) = self.remaining()? {
            sock.set_read_timeout(Some(remaining))?;
            self.timeout_set = true;
        }
        let conn = rustls::ServerConnection::new(config).map_err(io::Error::other)?;
        let mut tls = rustls::StreamOwned::new(conn, sock);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).map_err(timed_out)?;
        }
        self.stream = Stream::Tls(Box::new(tls));
        Ok(())
    }

    /// Time left until the deadline; an error once it has passed.
    fn remaining(&self) -> io::Result<Option<Duration>> {
        match self.deadline {
            None => Ok(None),
            Some(d) => match d.checked_duration_since(Instant::now()) {
                Some(left) if !left.is_zero() => Ok(Some(left)),
                _ => Err(io::ErrorKind::TimedOut.into()),
            },
        }
    }

    /// Bounds the next socket read by the deadline.
    fn arm_read_timeout(&mut self) -> io::Result<()> {
        match self.remaining()? {
            Some(left) => {
                self.stream.set_read_timeout(Some(left))?;
                self.timeout_set = true;
            }
            None if self.timeout_set => {
                self.stream.set_read_timeout(None)?;
                self.timeout_set = false;
            }
            None => {}
        }
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if !self.out.is_empty() {
            self.stream.write_all(&self.out)?;
            self.stream.flush()?;
            self.out.clear();
            if self.out.capacity() > 1 << 20 {
                self.out.shrink_to(8192);
            }
        }
        Ok(())
    }

    /// Makes at least `n` unread bytes available.
    fn fill(&mut self, n: usize) -> io::Result<()> {
        if self.inbuf.len() - self.inpos >= n {
            return Ok(());
        }
        if self.inpos > 0 {
            self.inbuf.drain(..self.inpos);
            self.inpos = 0;
        }
        // About to block on the client: send what we have.
        self.flush()?;
        let mut chunk = [0u8; 16384];
        while self.inbuf.len() < n {
            self.arm_read_timeout()?;
            let got = self.stream.read(&mut chunk).map_err(timed_out)?;
            if got == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.inbuf.extend_from_slice(&chunk[..got]);
        }
        Ok(())
    }

    fn take(&mut self, n: usize) -> io::Result<&[u8]> {
        self.fill(n)?;
        self.inpos += n;
        Ok(&self.inbuf[self.inpos - n..self.inpos])
    }

    /// Reads the untyped first message of a connection (startup, SSL or
    /// cancel request): `(code, payload)`.
    pub fn read_startup(&mut self) -> io::Result<(u32, Vec<u8>)> {
        let len = u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as usize;
        if !(8..=PRE_AUTH_MAX_MESSAGE_LEN).contains(&len) {
            return Err(protocol_error("invalid length of startup packet"));
        }
        let body = self.take(len - 4)?;
        let code = u32::from_be_bytes(body[..4].try_into().unwrap());
        Ok((code, body[4..].to_vec()))
    }

    /// Reads a typed message: `(type byte, payload)`. `Ok(None)` on a
    /// clean end of the connection.
    pub fn read_message(&mut self) -> io::Result<Option<(u8, Vec<u8>)>> {
        match self.fill(1) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && self.inbuf.len() == self.inpos => return Ok(None),
            Err(e) => return Err(e),
        }
        let header = self.take(5)?;
        let ty = header[0];
        let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        if len < 4 {
            return Err(protocol_error("invalid message length"));
        }
        if len > self.max_message {
            return Err(protocol_error(format!(
                "message of {len} bytes exceeds the limit of {} bytes",
                self.max_message
            )));
        }
        Ok(Some((ty, self.take(len - 4)?.to_vec())))
    }

    /// Starts a backend message; finish it with [`Io::end`].
    pub fn begin(&mut self, ty: u8) -> usize {
        self.out.push(ty);
        self.out.extend_from_slice(&[0; 4]);
        self.out.len() - 4
    }

    pub fn end(&mut self, start: usize) {
        let len = (self.out.len() - start) as u32;
        self.out[start..start + 4].copy_from_slice(&len.to_be_bytes());
    }

    pub fn message(&mut self, ty: u8, body: &[u8]) {
        let start = self.begin(ty);
        self.out.extend_from_slice(body);
        self.end(start);
    }

    pub fn cstr(&mut self, s: &str) {
        // Embedded NULs cannot be represented; cut there.
        let s = s.split('\0').next().unwrap_or_default();
        self.out.extend_from_slice(s.as_bytes());
        self.out.push(0);
    }

    pub fn i16(&mut self, v: i16) {
        self.out.extend_from_slice(&v.to_be_bytes());
    }

    pub fn i32(&mut self, v: i32) {
        self.out.extend_from_slice(&v.to_be_bytes());
    }
}

/// Cursor over the payload of a frontend message.
pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf }
    }

    pub fn bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.buf.len() < n {
            return Err(protocol_error("message is shorter than its contents require"));
        }
        let (a, b) = self.buf.split_at(n);
        self.buf = b;
        Ok(a)
    }

    pub fn u8(&mut self) -> io::Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    pub fn i16(&mut self) -> io::Result<i16> {
        Ok(i16::from_be_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    pub fn i32(&mut self) -> io::Result<i32> {
        Ok(i32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    pub fn cstr(&mut self) -> io::Result<&'a str> {
        let end =
            self.buf.iter().position(|b| *b == 0).ok_or_else(|| protocol_error("unterminated string in message"))?;
        let s = std::str::from_utf8(&self.buf[..end]).map_err(|_| protocol_error("invalid UTF-8 in message"))?;
        self.buf = &self.buf[end + 1..];
        Ok(s)
    }
}
