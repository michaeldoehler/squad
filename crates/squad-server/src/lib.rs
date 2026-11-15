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
//! The server uses one thread per connection and no async runtime. It
//! offers TLS when [`ServerConfig::tls`] is set, limits the number of
//! connections, the time a client may take to authenticate and the size of
//! the messages it may send, and supports cancel requests.

mod conn;
pub mod ping;
mod proto;
pub mod scram;
pub mod shell;
mod tls;
pub mod users;

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use squad::{CancelHandle, Database};

use proto::{Io, Stream};
pub use tls::TlsConfig;

/// How clients prove who they are.
#[derive(Clone, Default)]
pub enum Auth {
    /// Anyone who can connect is let in. Only allowed on loopback
    /// addresses and Unix sockets unless [`ServerConfig::trust_remote`]
    /// is set.
    #[default]
    Trust,
    /// Clients must know the password of the user they connect as.
    Password {
        /// User name to password.
        users: HashMap<String, String>,
    },
    /// Like [`Auth::Password`], with SCRAM-SHA-256 verifiers instead of
    /// passwords, e.g. from [`users::read_users_file`].
    Verifiers {
        /// User name to verifier.
        users: HashMap<String, scram::Verifier>,
    },
}

/// Never prints passwords.
impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn sorted<'a>(names: impl Iterator<Item = &'a String>) -> Vec<&'a String> {
            let mut names: Vec<&String> = names.collect();
            names.sort();
            names
        }
        match self {
            Auth::Trust => f.write_str("Trust"),
            Auth::Password { users } => f.debug_struct("Password").field("users", &sorted(users.keys())).finish(),
            Auth::Verifiers { users } => f.debug_struct("Verifiers").field("users", &sorted(users.keys())).finish(),
        }
    }
}

/// What a login may do, beyond logging in. Logins without an entry in
/// [`ServerConfig::logins`] get [`LoginAttributes::default`]: they bypass
/// row-level security and may write, as every login did before these
/// attributes existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoginAttributes {
    /// Exempt from row-level security policies, like a table's owner in
    /// PostgreSQL: policies apply to such a login only on tables with
    /// `FORCE ROW LEVEL SECURITY`. Without it, the policies of every table
    /// with row-level security apply, and the login may not change those
    /// tables or their policies. Default true.
    pub bypass_rls: bool,
    /// Every transaction of the login is read-only: writes and DDL fail
    /// with SQLSTATE 25006, and `BEGIN READ WRITE` is refused. Default
    /// false.
    pub read_only: bool,
}

impl Default for LoginAttributes {
    fn default() -> LoginAttributes {
        LoginAttributes { bypass_rls: true, read_only: false }
    }
}

/// Settings of a [`Server`]. Start from [`ServerConfig::default`] and
/// change what you need.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub auth: Auth,
    /// Attributes of logins by user name, e.g. from
    /// [`users::read_users_file`]. Applies with every kind of `auth`,
    /// trust included.
    pub logins: HashMap<String, LoginAttributes>,
    /// With password authentication: ask for the password in clear text
    /// instead of SCRAM-SHA-256, for clients that do not implement SCRAM.
    /// Without TLS the password then crosses the network unprotected.
    pub allow_cleartext_password: bool,
    /// Allow [`Auth::Trust`] on a TCP address other than loopback, where
    /// every host that can reach the port gets full access. Without it,
    /// [`Server::bind`] refuses such an address.
    pub trust_remote: bool,
    /// Directory in which to create a Unix domain socket named
    /// `.s.PGSQL.<port>`, the convention `psql -h <dir>` expects.
    pub unix_socket: Option<PathBuf>,
    /// Certificate and key for TLS. Without it, a client's request for
    /// TLS is declined and the connection continues unencrypted.
    pub tls: Option<TlsConfig>,
    /// Refuse TCP connections that did not switch to TLS (SQLSTATE
    /// 28000). Unix socket connections are not affected. Needs `tls`.
    pub require_tls: bool,
    /// Open client connections at most; further clients are refused with
    /// SQLSTATE 53300 ("sorry, too many clients already"). Default 100.
    pub max_connections: usize,
    /// Time a client has from connecting until it is authenticated,
    /// including the TLS handshake. Default 60 seconds, as PostgreSQL's
    /// `authentication_timeout`.
    pub authentication_timeout: Duration,
    /// Close connections on which the client sends nothing for this
    /// long while the server waits for it (SQLSTATE 57P05, or 25P03
    /// inside a transaction). Default: no limit.
    pub idle_timeout: Option<Duration>,
    /// Largest message an authenticated client may send, in bytes.
    /// Before authentication the limit is 10,000 bytes. Default 1 GiB, as
    /// in PostgreSQL.
    pub max_message_size: usize,
}

impl Default for ServerConfig {
    fn default() -> ServerConfig {
        ServerConfig {
            auth: Auth::Trust,
            logins: HashMap::new(),
            allow_cleartext_password: false,
            trust_remote: false,
            unix_socket: None,
            tls: None,
            require_tls: false,
            max_connections: 100,
            authentication_timeout: Duration::from_secs(60),
            idle_timeout: None,
            max_message_size: 1 << 30,
        }
    }
}

pub(crate) enum AuthData {
    Trust,
    Cleartext(HashMap<String, scram::Verifier>),
    Scram(HashMap<String, scram::Verifier>),
}

/// Connections that are refused for lack of capacity are answered on
/// threads of their own, but only this many at a time; beyond that the
/// socket is closed without an answer.
const MAX_REFUSING: usize = 16;
/// Time a refused client has to send its startup packet.
const REFUSE_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct Inner {
    db: Database,
    pub(crate) auth: AuthData,
    pub(crate) logins: HashMap<String, LoginAttributes>,
    pub(crate) tls: Option<Arc<rustls::ServerConfig>>,
    pub(crate) require_tls: bool,
    pub(crate) authentication_timeout: Duration,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) max_message_size: usize,
    max_connections: usize,
    shutdown: AtomicBool,
    /// Open connections, so that shutdown can close them.
    conns: Mutex<HashMap<u64, Stream>>,
    next_conn: AtomicU64,
    idle: Condvar,
    refusing: AtomicUsize,
    /// Process id to secret key and cancel handle, for cancel requests.
    pub(crate) backends: Mutex<HashMap<i32, (i32, CancelHandle)>>,
}

impl Inner {
    fn serve(self: &Arc<Self>, stream: Stream) {
        let id = self.next_conn.fetch_add(1, Ordering::Relaxed);
        let Ok(handle) = stream.try_clone() else { return };
        {
            let mut conns = self.conns.lock();
            if conns.len() >= self.max_connections {
                drop(conns);
                self.refuse(stream);
                return;
            }
            conns.insert(id, handle);
        }
        let inner = self.clone();
        let spawned = thread::Builder::new().name(format!("squad-conn-{id}")).spawn(move || {
            let session = inner.db.session();
            // I/O errors mean the client went away; nothing to report.
            let _ = conn::Conn::new(Io::new(stream), session, inner.clone(), false).run();
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

    /// Answers a client beyond `max_connections` with "too many clients"
    /// once it has sent its startup packet (cancel requests are still
    /// served).
    fn refuse(self: &Arc<Self>, stream: Stream) {
        if self.refusing.fetch_add(1, Ordering::SeqCst) >= MAX_REFUSING {
            self.refusing.fetch_sub(1, Ordering::SeqCst);
            stream.shutdown();
            return;
        }
        let inner = self.clone();
        let spawned = thread::Builder::new().name("squad-refuse".into()).spawn(move || {
            let mut io = Io::new(stream);
            io.deadline = Some(std::time::Instant::now() + REFUSE_TIMEOUT);
            let session = inner.db.session();
            let _ = conn::Conn::new(io, session, inner.clone(), true).run();
            inner.refusing.fetch_sub(1, Ordering::SeqCst);
        });
        if spawned.is_err() {
            self.refusing.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// A wire-protocol server for one database.
pub struct Server {
    inner: Arc<Inner>,
    unix_dir: Option<PathBuf>,
    trust_remote: bool,
    tcp: Option<TcpListener>,
    #[cfg(unix)]
    unix: Option<(UnixListener, PathBuf)>,
}

impl Server {
    pub fn new(db: Database, config: ServerConfig) -> Server {
        let verifiers = |users: HashMap<String, String>| -> HashMap<String, scram::Verifier> {
            users.into_iter().map(|(u, pw)| (u, scram::Verifier::from_password(&pw))).collect()
        };
        let auth = match config.auth {
            Auth::Trust => AuthData::Trust,
            Auth::Password { users } if config.allow_cleartext_password => AuthData::Cleartext(verifiers(users)),
            Auth::Password { users } => AuthData::Scram(verifiers(users)),
            Auth::Verifiers { users } if config.allow_cleartext_password => AuthData::Cleartext(users),
            Auth::Verifiers { users } => AuthData::Scram(users),
        };
        Server {
            inner: Arc::new(Inner {
                db,
                auth,
                logins: config.logins,
                tls: config.tls.map(|t| t.config),
                require_tls: config.require_tls,
                authentication_timeout: config.authentication_timeout,
                idle_timeout: config.idle_timeout,
                max_message_size: config.max_message_size.max(proto::PRE_AUTH_MAX_MESSAGE_LEN),
                max_connections: config.max_connections,
                shutdown: AtomicBool::new(false),
                conns: Mutex::new(HashMap::new()),
                next_conn: AtomicU64::new(1),
                idle: Condvar::new(),
                refusing: AtomicUsize::new(0),
                backends: Mutex::new(HashMap::new()),
            }),
            unix_dir: config.unix_socket,
            trust_remote: config.trust_remote,
            tcp: None,
            #[cfg(unix)]
            unix: None,
        }
    }

    /// Starts listening on a TCP address (port 0 picks a free port) and,
    /// if configured, on the Unix domain socket. Returns the TCP address
    /// actually bound.
    ///
    /// Fails with [`io::ErrorKind::PermissionDenied`] if the server
    /// trusts every client and the address is not a loopback address,
    /// unless [`ServerConfig::trust_remote`] is set; and with
    /// [`io::ErrorKind::InvalidInput`] if TLS is required but not
    /// configured.
    pub fn bind(&mut self, addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
        if self.inner.require_tls && self.inner.tls.is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "require_tls needs a TLS certificate and key"));
        }
        let listener = TcpListener::bind(addr)?;
        let local = listener.local_addr()?;
        if matches!(self.inner.auth, AuthData::Trust) && !self.trust_remote && !local.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "refusing to trust every client on {local}, which is not a loopback address: \
                     configure password authentication or allow it explicitly (trust_remote, --trust)"
                ),
            ));
        }
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
    /// client. Because of that, `addr` must be a loopback address such as
    /// `127.0.0.1` or `[::1]`; anything else fails with
    /// [`io::ErrorKind::PermissionDenied`]. Use [`Server`] directly to
    /// configure authentication, TLS, limits or a Unix socket. The
    /// database stays usable through the embedded API.
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
