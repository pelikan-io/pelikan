use crate::*;
use std::io::Result;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::path::PathBuf;

#[cfg(feature = "boringssl")]
mod boringssl;

#[cfg(feature = "openssl")]
mod openssl;

pub enum Implementation {
    #[cfg(feature = "boringssl")]
    Boringssl,
    #[cfg(feature = "openssl")]
    Openssl,
}

impl Default for Implementation {
    fn default() -> Self {
        #[cfg(all(not(feature = "boringssl"), feature = "openssl"))]
        {
            return Self::Openssl;
        }

        Self::Boringssl
    }
}

#[derive(PartialEq, Debug, Copy, Clone)]
pub enum ShutdownResult {
    Sent,
    Received,
}

#[cfg(feature = "openssl")]
impl From<::openssl::ssl::ShutdownResult> for ShutdownResult {
    fn from(other: ::openssl::ssl::ShutdownResult) -> Self {
        match other {
            ::openssl::ssl::ShutdownResult::Sent => Self::Sent,
            ::openssl::ssl::ShutdownResult::Received => Self::Received,
        }
    }
}

#[cfg(feature = "boringssl")]
impl From<::boring::ssl::ShutdownResult> for ShutdownResult {
    fn from(other: ::boring::ssl::ShutdownResult) -> Self {
        match other {
            ::boring::ssl::ShutdownResult::Sent => Self::Sent,
            ::boring::ssl::ShutdownResult::Received => Self::Received,
        }
    }
}

#[derive(Debug)]
pub struct TlsTcpStream {
    inner: TlsTcpStreamImpl,
}

#[derive(Debug)]
enum TlsTcpStreamImpl {
    #[cfg(feature = "boringssl")]
    Boringssl(boringssl::TlsTcpStream),
    #[cfg(feature = "openssl")]
    Openssl(openssl::TlsTcpStream),
}

impl AsRawFd for TlsTcpStream {
    fn as_raw_fd(&self) -> i32 {
        match &self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.as_raw_fd(),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.as_raw_fd(),
        }
    }
}

impl TlsTcpStream {
    pub fn set_nodelay(&mut self, nodelay: bool) -> Result<()> {
        match &mut self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.set_nodelay(nodelay),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.set_nodelay(nodelay),
        }
    }

    pub fn is_handshaking(&self) -> bool {
        match &self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.is_handshaking(),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.is_handshaking(),
        }
    }

    pub fn interest(&self) -> Interest {
        if self.is_handshaking() {
            Interest::READABLE.add(Interest::WRITABLE)
        } else {
            Interest::READABLE
        }
    }

    /// Attempts to drive the TLS/SSL handshake to completion. If the return
    /// variant is `Ok` it indiates that the handshake is complete. An error
    /// result of `WouldBlock` indicates that the handshake may complete in the
    /// future. Other error types indiate a handshake failure with no possible
    /// recovery and that the connection should be closed.
    pub fn do_handshake(&mut self) -> Result<()> {
        match &mut self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.do_handshake(),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.do_handshake(),
        }
    }

    pub fn shutdown(&mut self) -> Result<ShutdownResult> {
        match &mut self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.shutdown().map(|v| v.into()),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.shutdown().map(|v| v.into()),
        }
    }
}

impl Read for TlsTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match &mut self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.read(buf),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.read(buf),
        }
    }
}

impl Write for TlsTcpStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        match &mut self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.write(buf),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> Result<()> {
        match &mut self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.flush(),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.flush(),
        }
    }
}

impl event::Source for TlsTcpStream {
    fn register(&mut self, registry: &Registry, token: Token, interest: Interest) -> Result<()> {
        match &mut self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.register(registry, token, interest),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.register(registry, token, interest),
        }
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interest: mio::Interest,
    ) -> Result<()> {
        match &mut self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.reregister(registry, token, interest),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.reregister(registry, token, interest),
        }
    }

    fn deregister(&mut self, registry: &mio::Registry) -> Result<()> {
        match &mut self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpStreamImpl::Boringssl(s) => s.deregister(registry),
            #[cfg(feature = "openssl")]
            TlsTcpStreamImpl::Openssl(s) => s.deregister(registry),
        }
    }
}

pub struct TlsTcpAcceptor {
    inner: TlsTcpAcceptorImpl,
}

enum TlsTcpAcceptorImpl {
    Boringssl(boringssl::TlsTcpAcceptor),
    #[cfg(feature = "openssl")]
    Openssl(openssl::TlsTcpAcceptor),
}

impl TlsTcpAcceptor {
    pub fn builder() -> TlsTcpAcceptorBuilder {
        TlsTcpAcceptorBuilder::default()
    }

    pub fn accept(&self, stream: TcpStream) -> Result<TlsTcpStream> {
        match &self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpAcceptorImpl::Boringssl(s) => s.accept(stream).map(|s| TlsTcpStream {
                inner: TlsTcpStreamImpl::Boringssl(s),
            }),
            #[cfg(feature = "openssl")]
            TlsTcpAcceptorImpl::Openssl(s) => s.accept(stream).map(|s| TlsTcpStream {
                inner: TlsTcpStreamImpl::Openssl(s),
            }),
        }
    }
}

/// Provides a wrapped builder for producing a `TlsAcceptor`. This has some
/// minor differences from the `boring::ssl::SslAcceptorBuilder` to provide
/// improved ergonomics.
#[derive(Default)]
pub struct TlsTcpAcceptorBuilder {
    implementation: Implementation,
    ca_file: Option<PathBuf>,
    certificate_file: Option<PathBuf>,
    certificate_chain_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
}

impl TlsTcpAcceptorBuilder {
    pub fn build(self) -> Result<TlsTcpAcceptor> {
        match self.implementation {
            #[cfg(feature = "boringssl")]
            Implementation::Boringssl => Ok(TlsTcpAcceptor {
                inner: TlsTcpAcceptorImpl::Boringssl(boringssl::TlsTcpAcceptor::build(self)?),
            }),
            #[cfg(feature = "openssl")]
            Implementation::Openssl => Ok(TlsTcpAcceptor {
                inner: TlsTcpAcceptorImpl::Openssl(openssl::TlsTcpAcceptor::build(self)?),
            }),
        }
    }

    /// Allows selection of the TLS/SSL implementation if this crate is built
    /// with multiple implementations enabled.
    pub fn implementation(mut self, implementation: Implementation) -> Self {
        self.implementation = implementation;
        self
    }

    /// Load trusted root certificates from a file.
    ///
    /// The file should contain a sequence of PEM-formatted CA certificates.
    pub fn ca_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.ca_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Load a leaf certificate from a file.
    ///
    /// This loads only a single PEM-formatted certificate from the file which
    /// will be used as the leaf certifcate.
    ///
    /// Use `set_certificate_chain_file` to provide a complete certificate
    /// chain. Use this with the `set_certifcate_chain_file` if the leaf
    /// certifcate and remainder of the certificate chain are split across two
    /// files.
    pub fn certificate_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.certificate_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Load a certificate chain from a file.
    ///
    /// The file should contain a sequence of PEM-formatted certificates. If
    /// used without `set_certificate_file` the provided file must contain the
    /// leaf certificate and the complete chain of certificates up to and
    /// including the trusted root certificate. If used with
    /// `set_certificate_file`, this file must not contain the leaf certifcate
    /// and will be treated as the complete chain of certificates up to and
    /// including the trusted root certificate.
    pub fn certificate_chain_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.certificate_chain_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Loads the private key from a PEM-formatted file.
    pub fn private_key_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.private_key_file = Some(file.as_ref().to_path_buf());
        self
    }
}

pub struct TlsTcpConnector {
    inner: TlsTcpConnectorImpl,
}

enum TlsTcpConnectorImpl {
    #[cfg(feature = "boringssl")]
    Boringssl(boringssl::TlsTcpConnector),
    #[cfg(feature = "openssl")]
    Openssl(openssl::TlsTcpConnector),
}

impl TlsTcpConnector {
    pub fn builder() -> TlsTcpConnectorBuilder {
        TlsTcpConnectorBuilder::default()
    }

    pub fn connect<A: ToSocketAddrs>(&self, addr: A) -> Result<TlsTcpStream> {
        match &self.inner {
            #[cfg(feature = "boringssl")]
            TlsTcpConnectorImpl::Boringssl(s) => s.connect(addr).map(|s| TlsTcpStream {
                inner: TlsTcpStreamImpl::Boringssl(s),
            }),
            #[cfg(feature = "openssl")]
            TlsTcpConnectorImpl::Openssl(s) => s.connect(addr).map(|s| TlsTcpStream {
                inner: TlsTcpStreamImpl::Openssl(s),
            }),
        }
    }
}

#[derive(Default)]
pub struct TlsTcpConnectorBuilder {
    implementation: Implementation,
    ca_file: Option<PathBuf>,
    certificate_file: Option<PathBuf>,
    certificate_chain_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
}

impl TlsTcpConnectorBuilder {
    pub fn build(self) -> Result<TlsTcpConnector> {
        match self.implementation {
            #[cfg(feature = "boringssl")]
            Implementation::Boringssl => Ok(TlsTcpConnector {
                inner: TlsTcpConnectorImpl::Boringssl(boringssl::TlsTcpConnector::build(self)?),
            }),
            #[cfg(feature = "openssl")]
            Implementation::Openssl => Ok(TlsTcpConnector {
                inner: TlsTcpConnectorImpl::Openssl(openssl::TlsTcpConnector::build(self)?),
            }),
        }
    }

    /// Allows selection of the TLS/SSL implementation if this crate is built
    /// with multiple implementations enabled.
    pub fn implementation(mut self, implementation: Implementation) -> Self {
        self.implementation = implementation;
        self
    }

    /// Load trusted root certificates from a file.
    ///
    /// The file should contain a sequence of PEM-formatted CA certificates.
    pub fn ca_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.ca_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Load a leaf certificate from a file.
    ///
    /// This loads only a single PEM-formatted certificate from the file which
    /// will be used as the leaf certifcate.
    ///
    /// Use `set_certificate_chain_file` to provide a complete certificate
    /// chain. Use this with the `set_certifcate_chain_file` if the leaf
    /// certifcate and remainder of the certificate chain are split across two
    /// files.
    pub fn certificate_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.certificate_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Load a certificate chain from a file.
    ///
    /// The file should contain a sequence of PEM-formatted certificates. If
    /// used without `set_certificate_file` the provided file must contain the
    /// leaf certificate and the complete chain of certificates up to and
    /// including the trusted root certificate. If used with
    /// `set_certificate_file`, this file must not contain the leaf certifcate
    /// and will be treated as the complete chain of certificates up to and
    /// including the trusted root certificate.
    pub fn certificate_chain_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.certificate_chain_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Loads the private key from a PEM-formatted file.
    pub fn private_key_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.private_key_file = Some(file.as_ref().to_path_buf());
        self
    }
}
