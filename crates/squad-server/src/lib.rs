//! A server that speaks the PostgreSQL wire protocol (version 3) in front
//! of a squad [`Database`], so that `psql`, drivers and tools written for
//! Postgres can connect to it.
//!
//! ```no_run
//! use squad::Database;
//! use squad_server::{Server, ServerConfig};
//!
//! let db = Database::open_in_memory()?;
//! let mut server = Server::new(db, ServerConfig::default());
//! server.bind("127.0.0.1:5432")?;
//! server.run()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! The server uses one thread per connection and no async runtime.
//! Connections are not encrypted: a client's SSL request is declined, so
//! clients must not require TLS (`sslmode=prefer` or `disable`).

mod conn;
mod proto;
pub mod scram;
pub mod shell;

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use squad::Database;

use proto::{Io, Stream};

/// How clients prove who they are.
#[derive(Debug, Clone, Default)]
pub enum Auth {
    /// Anyone who can connect is let in.
    #[default]
    Trust,
    /// Clients must know the password of the user they connect as.
    Password {
        /// User name to password.
        users: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct ServerConfig {
    pub auth: Auth,
    /// With [`Auth::Password`]: ask for the password in clear text instead
    /// of SCRAM-SHA-256, for clients that do not implement SCRAM. The
    /// password then crosses the network unprotected.
    pub allow_cleartext_password: bool,
    /// Directory in which to create a Unix domain socket named
    /// `.s.PGSQL.<port>`, the convention `psql -h <dir>` expects.
    pub unix_socket: Option<PathBuf>,
}

pub(crate) enum AuthData {
    Trust,
    Cleartext(HashMap<String, String>),
    Scram(HashMap<String, scram::Verifier>),
}

pub(crate) struct Inner {
    db: Database,
    pub(crate) auth: AuthData,
    shutdown: AtomicBool,
    /// Open connections, so that shutdown can close them.
    conns: Mutex<HashMap<u64, Stream>>,
    next_conn: AtomicU64,
    idle: Condvar,
}

impl Inner {
    fn serve(self: &Arc<Self>, stream: Stream) {
        let id = self.next_conn.fetch_add(1, Ordering::Relaxed);
        let Ok(handle) = stream.try_clone() else { return };
        self.conns.lock().insert(id, handle);
        let inner = self.clone();
        let spawned = thread::Builder::new().name(format!("squad-conn-{id}")).spawn(move || {
            let session = inner.db.session();
            // I/O errors mean the client went away; nothing to report.
            let _ = conn::Conn::new(Io::new(stream), session, inner.clone()).run();
            let mut conns = inner.conns.lock();
            if let Some(s) = conns.remove(&id) {
                s.shutdown();
            }
            inner.idle.notify_all();
        });
        if spawned.is_err() {
            if let Some(s) = self.conns.lock().remove(&id) {
                s.shutdown();
            }
        }
    }
}

/// A wire-protocol server for one database.
pub struct Server {
    inner: Arc<Inner>,
    unix_dir: Option<PathBuf>,
    tcp: Option<TcpListener>,
    #[cfg(unix)]
    unix: Option<(UnixListener, PathBuf)>,
}

impl Server {
    pub fn new(db: Database, config: ServerConfig) -> Server {
        let auth = match config.auth {
            Auth::Trust => AuthData::Trust,
            Auth::Password { users } if config.allow_cleartext_password => AuthData::Cleartext(users),
            Auth::Password { users } => {
                AuthData::Scram(users.into_iter().map(|(u, pw)| (u, scram::Verifier::from_password(&pw))).collect())
            }
        };
        Server {
            inner: Arc::new(Inner {
                db,
                auth,
                shutdown: AtomicBool::new(false),
                conns: Mutex::new(HashMap::new()),
                next_conn: AtomicU64::new(1),
                idle: Condvar::new(),
            }),
            unix_dir: config.unix_socket,
            tcp: None,
            #[cfg(unix)]
            unix: None,
        }
    }

    /// Starts listening on a TCP address (port 0 picks a free port) and,
    /// if configured, on the Unix domain socket. Returns the TCP address
    /// actually bound.
    pub fn bind(&mut self, addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
        let listener = TcpListener::bind(addr)?;
        let local = listener.local_addr()?;
        #[cfg(unix)]
        if let Some(dir) = &self.unix_dir {
            let path = dir.join(format!(".s.PGSQL.{}", local.port()));
            // A leftover socket file of a dead server would block bind.
            let _ = std::fs::remove_file(&path);
            self.unix = Some((UnixListener::bind(&path)?, path));
        }
        self.tcp = Some(listener);
        Ok(local)
    }

    /// The TCP address the server listens on, once bound.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.tcp.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// Path of the Unix domain socket, once bound.
    pub fn unix_socket_path(&self) -> Option<PathBuf> {
        #[cfg(unix)]
        return self.unix.as_ref().map(|(_, p)| p.clone());
        #[cfg(not(unix))]
        None
    }

    /// Accepts connections until the server is shut down. Blocks.
    pub fn run(self) -> io::Result<()> {
        self.spawn().wait();
        Ok(())
    }

    /// Accepts connections on background threads and returns a handle to
    /// stop the server.
    pub fn spawn(self) -> ServerHandle {
        let addr = self.local_addr();
        let mut threads = Vec::new();
        if let Some(listener) = self.tcp {
            let inner = self.inner.clone();
            threads.push(thread::spawn(move || {
                for stream in listener.incoming() {
                    if inner.shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    match stream {
                        Ok(s) => {
                            let _ = s.set_nodelay(true);
                            inner.serve(Stream::Tcp(s));
                        }
                        // Out of file descriptors and the like: back off.
                        Err(_) => thread::sleep(Duration::from_millis(50)),
                    }
                }
            }));
        }
        #[cfg(unix)]
        let unix_path = self.unix.as_ref().map(|(_, p)| p.clone());
        #[cfg(unix)]
        if let Some((listener, _)) = self.unix {
            let inner = self.inner.clone();
            threads.push(thread::spawn(move || {
                for stream in listener.incoming() {
                    if inner.shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    match stream {
                        Ok(s) => inner.serve(Stream::Unix(s)),
                        Err(_) => thread::sleep(Duration::from_millis(50)),
                    }
                }
            }));
        }
        ServerHandle {
            inner: self.inner,
            addr,
            #[cfg(unix)]
            unix_path,
            threads,
        }
    }
}

/// A running server. Dropping the handle does not stop the server; call
/// [`shutdown`](ServerHandle::shutdown).
pub struct ServerHandle {
    inner: Arc<Inner>,
    addr: Option<SocketAddr>,
    #[cfg(unix)]
    unix_path: Option<PathBuf>,
    threads: Vec<JoinHandle<()>>,
}

impl ServerHandle {
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.addr
    }

    pub fn unix_socket_path(&self) -> Option<PathBuf> {
        #[cfg(unix)]
        return self.unix_path.clone();
        #[cfg(not(unix))]
        None
    }

    /// Number of client connections currently open.
    pub fn connections(&self) -> usize {
        self.inner.conns.lock().len()
    }

    /// Blocks until the accept loops end.
    fn wait(mut self) {
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }

    /// Stops accepting connections, closes the open ones and waits for
    /// their threads to finish.
    pub fn shutdown(mut self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        // Wake the accept loops with a throwaway connection.
        if let Some(mut addr) = self.addr {
            if addr.ip().is_unspecified() {
                addr.set_ip(if addr.is_ipv4() {
                    std::net::Ipv4Addr::LOCALHOST.into()
                } else {
                    std::net::Ipv6Addr::LOCALHOST.into()
                });
            }
            let _ = TcpStream::connect_timeout(&addr, Duration::from_secs(1));
        }
        #[cfg(unix)]
        if let Some(path) = &self.unix_path {
            let _ = UnixStream::connect(path);
            let _ = std::fs::remove_file(path);
        }
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
        let mut conns = self.inner.conns.lock();
        for s in conns.values() {
            s.shutdown();
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !conns.is_empty() {
            if self.inner.idle.wait_until(&mut conns, deadline).timed_out() {
                break;
            }
        }
    }
}

/// Adds [`serve`](Serve::serve) to [`Database`]: one call makes an
/// embedded database reachable for other processes and for tools and
/// drivers that speak the Postgres protocol.
///
/// ```
/// use squad::Database;
/// use squad_server::Serve;
///
/// let db = Database::open_in_memory()?;
/// let server = db.serve("127.0.0.1:0")?;
/// // e.g. postgres://squad@127.0.0.1:54321/squad
/// println!("{}", server.url().unwrap());
/// server.shutdown();
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait Serve {
    /// Serves the database on `addr` in background threads, trusting every
    /// client. Use [`Server`] directly to configure authentication or a
    /// Unix socket. The database stays usable through the embedded API.
    fn serve(&self, addr: impl ToSocketAddrs) -> io::Result<ServerHandle>;
}

impl Serve for Database {
    fn serve(&self, addr: impl ToSocketAddrs) -> io::Result<ServerHandle> {
        let mut server = Server::new(self.clone(), ServerConfig::default());
        server.bind(addr)?;
        Ok(server.spawn())
    }
}

impl ServerHandle {
    /// Connection URL for clients on this machine, as accepted by libpq,
    /// sqlx, tokio-postgres and other drivers.
    pub fn url(&self) -> Option<String> {
        self.local_addr().map(|a| format!("postgres://squad@{a}/squad"))
    }
}
