//! Framing of the Postgres frontend/backend protocol, version 3.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
#[cfg(unix)]
use std::os::unix::net::UnixStream;

/// Largest message the server accepts from a client.
pub const MAX_MESSAGE_LEN: usize = 1 << 30;

/// A client connection over TCP or a Unix domain socket.
pub enum Stream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Stream {
    pub fn try_clone(&self) -> io::Result<Stream> {
        Ok(match self {
            Stream::Tcp(s) => Stream::Tcp(s.try_clone()?),
            #[cfg(unix)]
            Stream::Unix(s) => Stream::Unix(s.try_clone()?),
        })
    }

    pub fn shutdown(&self) {
        let _ = match self {
            Stream::Tcp(s) => s.shutdown(Shutdown::Both),
            #[cfg(unix)]
            Stream::Unix(s) => s.shutdown(Shutdown::Both),
        };
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.flush(),
            #[cfg(unix)]
            Stream::Unix(s) => s.flush(),
        }
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
}

impl Io {
    pub fn new(stream: Stream) -> Io {
        Io { stream, inbuf: Vec::with_capacity(8192), inpos: 0, out: Vec::with_capacity(8192) }
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if !self.out.is_empty() {
            self.stream.write_all(&self.out)?;
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
            let got = self.stream.read(&mut chunk)?;
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
        if !(8..=10_000).contains(&len) {
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
        if !(4..=MAX_MESSAGE_LEN).contains(&len) {
            return Err(protocol_error("invalid message length"));
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
