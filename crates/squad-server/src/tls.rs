//! TLS configuration (rustls with the ring provider).

use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Certificate and private key with which the server accepts TLS
/// connections. Clients ask for TLS with PostgreSQL's SSLRequest, as
/// `sslmode=prefer`, `require`, `verify-ca` and `verify-full` do.
#[derive(Clone)]
pub struct TlsConfig {
    pub(crate) config: Arc<rustls::ServerConfig>,
}

impl TlsConfig {
    /// Builds a configuration from a PEM certificate chain (server
    /// certificate first) and a PEM private key (PKCS#8, PKCS#1 or SEC1).
    pub fn from_pem(cert_chain: &[u8], key: &[u8]) -> io::Result<TlsConfig> {
        let invalid = |what: String| io::Error::new(io::ErrorKind::InvalidInput, what);
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_chain)
            .collect::<Result<_, _>>()
            .map_err(|e| invalid(format!("cannot read TLS certificate: {e}")))?;
        if certs.is_empty() {
            return Err(invalid("no certificate found in the TLS certificate file".into()));
        }
        let key =
            PrivateKeyDer::from_pem_slice(key).map_err(|e| invalid(format!("cannot read TLS private key: {e}")))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| invalid(format!("TLS certificate and key do not fit together: {e}")))?;
        // PostgreSQL 17 clients offer this protocol name.
        config.alpn_protocols = vec![b"postgresql".to_vec()];
        Ok(TlsConfig { config: Arc::new(config) })
    }

    /// Reads the certificate chain and the private key from PEM files.
    pub fn from_pem_files(cert_chain: impl AsRef<Path>, key: impl AsRef<Path>) -> io::Result<TlsConfig> {
        let read = |p: &Path, what: &str| {
            std::fs::read(p)
                .map_err(|e| io::Error::new(e.kind(), format!("cannot read TLS {what} {}: {e}", p.display())))
        };
        TlsConfig::from_pem(&read(cert_chain.as_ref(), "certificate")?, &read(key.as_ref(), "key")?)
    }

    /// Uses a rustls configuration built by the application, e.g. with
    /// client certificate verification or a certificate resolver that
    /// reloads certificates.
    pub fn from_rustls(config: Arc<rustls::ServerConfig>) -> TlsConfig {
        TlsConfig { config }
    }
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfig").finish_non_exhaustive()
    }
}
