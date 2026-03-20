// Copyright 2024 Pelikan Foundation
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::{BufReader, ErrorKind};
use std::os::unix::prelude::AsRawFd;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConnection, ServerConfig, ServerConnection, StreamOwned};

use crate::*;

#[derive(PartialEq, Copy, Clone)]
pub enum ShutdownResult {
    Sent,
    Received,
}

#[derive(PartialEq)]
enum TlsState {
    Handshaking,
    Negotiated,
}

enum ConnectionType {
    Server(StreamOwned<ServerConnection, TcpStream>),
    Client(StreamOwned<ClientConnection, TcpStream>),
}

/// Wraps a TLS/SSL stream so that negotiated and handshaking sessions have a
/// uniform type.
pub struct TlsTcpStream {
    inner: ConnectionType,
    state: TlsState,
}

impl AsRawFd for TlsTcpStream {
    fn as_raw_fd(&self) -> i32 {
        match &self.inner {
            ConnectionType::Server(s) => s.sock.as_raw_fd(),
            ConnectionType::Client(s) => s.sock.as_raw_fd(),
        }
    }
}

impl TlsTcpStream {
    pub fn set_nodelay(&mut self, nodelay: bool) -> Result<()> {
        match &mut self.inner {
            ConnectionType::Server(s) => s.sock.set_nodelay(nodelay),
            ConnectionType::Client(s) => s.sock.set_nodelay(nodelay),
        }
    }

    pub fn is_handshaking(&self) -> bool {
        self.state == TlsState::Handshaking
    }

    pub fn interest(&self) -> Interest {
        if self.is_handshaking() {
            Interest::READABLE.add(Interest::WRITABLE)
        } else {
            Interest::READABLE
        }
    }

    /// Attempts to drive the TLS/SSL handshake to completion. If the return
    /// variant is `Ok` it indicates that the handshake is complete. An error
    /// result of `WouldBlock` indicates that the handshake may complete in the
    /// future. Other error types indicate a handshake failure with no possible
    /// recovery and that the connection should be closed.
    pub fn do_handshake(&mut self) -> Result<()> {
        if self.state != TlsState::Handshaking {
            return Ok(());
        }

        let result = match &mut self.inner {
            ConnectionType::Server(s) => s.conn.complete_io(&mut s.sock),
            ConnectionType::Client(s) => s.conn.complete_io(&mut s.sock),
        };

        let still_handshaking = match &self.inner {
            ConnectionType::Server(s) => s.conn.is_handshaking(),
            ConnectionType::Client(s) => s.conn.is_handshaking(),
        };

        if !still_handshaking {
            metric! {
                STREAM_HANDSHAKE.increment();
            }
            self.state = TlsState::Negotiated;
            return Ok(());
        }

        match result {
            Ok(_) => Err(Error::from(ErrorKind::WouldBlock)),
            Err(e) if e.kind() == ErrorKind::WouldBlock => Err(Error::from(ErrorKind::WouldBlock)),
            Err(e) => {
                metric! {
                    STREAM_HANDSHAKE.increment();
                    STREAM_HANDSHAKE_EX.increment();
                }
                Err(Error::other(format!("handshake failed: {}", e)))
            }
        }
    }

    pub fn shutdown(&mut self) -> Result<ShutdownResult> {
        match &mut self.inner {
            ConnectionType::Server(s) => {
                s.conn.send_close_notify();
                match s.conn.complete_io(&mut s.sock) {
                    Ok(_) => {
                        metric! {
                            STREAM_SHUTDOWN.increment();
                        }
                        // Try to receive peer's close_notify
                        match s.conn.complete_io(&mut s.sock) {
                            Ok(_) => Ok(ShutdownResult::Received),
                            _ => Ok(ShutdownResult::Sent),
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(ShutdownResult::Sent),
                    Err(e) => Err(Error::other(e.to_string())),
                }
            }
            ConnectionType::Client(s) => {
                s.conn.send_close_notify();
                match s.conn.complete_io(&mut s.sock) {
                    Ok(_) => {
                        metric! {
                            STREAM_SHUTDOWN.increment();
                        }
                        // Try to receive peer's close_notify
                        match s.conn.complete_io(&mut s.sock) {
                            Ok(_) => Ok(ShutdownResult::Received),
                            _ => Ok(ShutdownResult::Sent),
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(ShutdownResult::Sent),
                    Err(e) => Err(Error::other(e.to_string())),
                }
            }
        }
    }
}

impl Debug for TlsTcpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match &self.inner {
            ConnectionType::Server(s) => write!(f, "{:?}", s.sock),
            ConnectionType::Client(s) => write!(f, "{:?}", s.sock),
        }
    }
}

impl Read for TlsTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.is_handshaking() {
            Err(Error::new(
                ErrorKind::WouldBlock,
                "read on handshaking session would block",
            ))
        } else {
            match &mut self.inner {
                ConnectionType::Server(s) => s.read(buf),
                ConnectionType::Client(s) => s.read(buf),
            }
        }
    }
}

impl Write for TlsTcpStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.is_handshaking() {
            Err(Error::new(
                ErrorKind::WouldBlock,
                "write on handshaking session would block",
            ))
        } else {
            match &mut self.inner {
                ConnectionType::Server(s) => s.write(buf),
                ConnectionType::Client(s) => s.write(buf),
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.is_handshaking() {
            Err(Error::new(
                ErrorKind::WouldBlock,
                "flush on handshaking session would block",
            ))
        } else {
            match &mut self.inner {
                ConnectionType::Server(s) => s.flush(),
                ConnectionType::Client(s) => s.flush(),
            }
        }
    }
}

impl event::Source for TlsTcpStream {
    fn register(&mut self, registry: &Registry, token: Token, interest: Interest) -> Result<()> {
        match &mut self.inner {
            ConnectionType::Server(s) => s.sock.register(registry, token, interest),
            ConnectionType::Client(s) => s.sock.register(registry, token, interest),
        }
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interest: mio::Interest,
    ) -> Result<()> {
        match &mut self.inner {
            ConnectionType::Server(s) => s.sock.reregister(registry, token, interest),
            ConnectionType::Client(s) => s.sock.reregister(registry, token, interest),
        }
    }

    fn deregister(&mut self, registry: &mio::Registry) -> Result<()> {
        match &mut self.inner {
            ConnectionType::Server(s) => s.sock.deregister(registry),
            ConnectionType::Client(s) => s.sock.deregister(registry),
        }
    }
}

// Builder for TlsTcpAcceptor

#[derive(Default)]
pub struct TlsTcpAcceptorBuilder {
    ca_file: Option<PathBuf>,
    certificate_file: Option<PathBuf>,
    certificate_chain_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
}

impl TlsTcpAcceptorBuilder {
    pub fn build(self) -> Result<TlsTcpAcceptor> {
        TlsTcpAcceptor::build(self)
    }

    /// Load trusted root certificates from a file.
    pub fn ca_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.ca_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Load a leaf certificate from a file.
    pub fn certificate_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.certificate_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Load a certificate chain from a file.
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

/// Provides a wrapped acceptor for server-side TLS.
pub struct TlsTcpAcceptor {
    config: Arc<ServerConfig>,
}

impl TlsTcpAcceptor {
    pub fn builder() -> TlsTcpAcceptorBuilder {
        TlsTcpAcceptorBuilder::default()
    }

    fn build(builder: TlsTcpAcceptorBuilder) -> Result<TlsTcpAcceptor> {
        let certs = load_certs(&builder.certificate_chain_file, &builder.certificate_file)?;
        let key = load_private_key(&builder.private_key_file)?;

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| Error::other(format!("failed to build TLS config: {}", e)))?;

        Ok(TlsTcpAcceptor {
            config: Arc::new(config),
        })
    }

    pub fn accept(&self, stream: TcpStream) -> Result<TlsTcpStream> {
        let conn = ServerConnection::new(Arc::clone(&self.config))
            .map_err(|e| Error::other(format!("failed to create server connection: {}", e)))?;

        let mut tls_stream = StreamOwned::new(conn, stream);

        match tls_stream.conn.complete_io(&mut tls_stream.sock) {
            Ok(_) => {
                let state = if tls_stream.conn.is_handshaking() {
                    TlsState::Handshaking
                } else {
                    TlsState::Negotiated
                };
                Ok(TlsTcpStream {
                    inner: ConnectionType::Server(tls_stream),
                    state,
                })
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(TlsTcpStream {
                inner: ConnectionType::Server(tls_stream),
                state: TlsState::Handshaking,
            }),
            Err(e) => Err(Error::other(format!("handshake failed: {}", e))),
        }
    }
}

// Builder for TlsTcpConnector

#[derive(Default)]
pub struct TlsTcpConnectorBuilder {
    server_name: Option<String>,
    ca_file: Option<PathBuf>,
    certificate_file: Option<PathBuf>,
    certificate_chain_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
}

impl TlsTcpConnectorBuilder {
    pub fn build(self) -> Result<TlsTcpConnector> {
        TlsTcpConnector::build(self)
    }

    /// Set the server name for SNI and certificate verification.
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Load trusted root certificates from a file.
    pub fn ca_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.ca_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Load a leaf certificate from a file.
    pub fn certificate_file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.certificate_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Load a certificate chain from a file.
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

/// Provides a wrapped connector for client-side TLS.
pub struct TlsTcpConnector {
    config: Arc<rustls::ClientConfig>,
    server_name: Option<String>,
}

impl TlsTcpConnector {
    pub fn builder() -> TlsTcpConnectorBuilder {
        TlsTcpConnectorBuilder::default()
    }

    fn build(builder: TlsTcpConnectorBuilder) -> Result<TlsTcpConnector> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        if let Some(f) = &builder.ca_file {
            let ca_file = std::fs::File::open(f).map_err(|e| {
                Error::other(format!("failed to open CA file: {}: {}", f.display(), e))
            })?;
            let mut ca_reader = BufReader::new(ca_file);
            let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    Error::other(format!("failed to parse CA file: {}: {}", f.display(), e))
                })?;
            for cert in ca_certs {
                root_store
                    .add(cert)
                    .map_err(|e| Error::other(format!("failed to add CA certificate: {}", e)))?;
            }
        }

        let config_builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

        let config = if builder.private_key_file.is_some() {
            let certs = load_certs(&builder.certificate_chain_file, &builder.certificate_file)?;
            let key = load_private_key(&builder.private_key_file)?;

            config_builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| Error::other(format!("failed to build TLS config: {}", e)))?
        } else {
            config_builder.with_no_client_auth()
        };

        Ok(TlsTcpConnector {
            config: Arc::new(config),
            server_name: builder.server_name,
        })
    }

    pub fn connect<A: ToSocketAddrs>(&self, addr: A) -> Result<TlsTcpStream> {
        let addrs: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();
        let mut s = Err(Error::other("failed to resolve"));
        for addr in &addrs {
            s = TcpStream::connect(*addr);
            if s.is_ok() {
                break;
            }
        }
        let stream = s?;

        let server_name = if let Some(name) = &self.server_name {
            ServerName::try_from(name.as_str())
                .map_err(|e| Error::other(format!("invalid server name: {}", e)))?
                .to_owned()
        } else {
            let ip = addrs
                .first()
                .ok_or_else(|| Error::other("no addresses resolved"))?
                .ip();
            ServerName::IpAddress(ip.into())
        };

        let conn = ClientConnection::new(Arc::clone(&self.config), server_name)
            .map_err(|e| Error::other(format!("failed to create client connection: {}", e)))?;

        let mut tls_stream = StreamOwned::new(conn, stream);

        match tls_stream.conn.complete_io(&mut tls_stream.sock) {
            Ok(_) => {
                let state = if tls_stream.conn.is_handshaking() {
                    TlsState::Handshaking
                } else {
                    TlsState::Negotiated
                };
                Ok(TlsTcpStream {
                    inner: ConnectionType::Client(tls_stream),
                    state,
                })
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(TlsTcpStream {
                inner: ConnectionType::Client(tls_stream),
                state: TlsState::Handshaking,
            }),
            Err(e) => Err(Error::other(format!("handshake failed: {}", e))),
        }
    }
}

// Shared helpers

fn load_certs(
    chain_file: &Option<PathBuf>,
    cert_file: &Option<PathBuf>,
) -> Result<Vec<CertificateDer<'static>>> {
    match (chain_file, cert_file) {
        (Some(chain), Some(cert)) => {
            let mut certs = read_certs_from_file(cert)?;
            certs.extend(read_certs_from_file(chain)?);
            Ok(certs)
        }
        (Some(chain), None) => read_certs_from_file(chain),
        (None, Some(cert)) => read_certs_from_file(cert),
        (None, None) => Err(Error::other(
            "no certificate file or certificate chain file provided",
        )),
    }
}

fn read_certs_from_file(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path).map_err(|e| {
        Error::other(format!(
            "failed to open certificate file: {}: {}",
            path.display(),
            e
        ))
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            Error::other(format!(
                "failed to parse certificate file: {}: {}",
                path.display(),
                e
            ))
        })
}

fn load_private_key(key_file: &Option<PathBuf>) -> Result<PrivateKeyDer<'static>> {
    let f = key_file
        .as_ref()
        .ok_or_else(|| Error::other("no private key file provided"))?;

    let file = std::fs::File::open(f).map_err(|e| {
        Error::other(format!(
            "failed to open private key file: {}: {}",
            f.display(),
            e
        ))
    })?;
    let mut reader = BufReader::new(file);

    let mut keys = Vec::new();
    loop {
        match rustls_pemfile::read_one(&mut reader) {
            Ok(Some(rustls_pemfile::Item::Pkcs1Key(key))) => {
                keys.push(PrivateKeyDer::Pkcs1(key));
            }
            Ok(Some(rustls_pemfile::Item::Pkcs8Key(key))) => {
                keys.push(PrivateKeyDer::Pkcs8(key));
            }
            Ok(Some(rustls_pemfile::Item::Sec1Key(key))) => {
                keys.push(PrivateKeyDer::Sec1(key));
            }
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(e) => {
                return Err(Error::other(format!(
                    "failed to parse private key file: {}: {}",
                    f.display(),
                    e
                )));
            }
        }
    }

    keys.into_iter()
        .next()
        .ok_or_else(|| Error::other(format!("no private key found in file: {}", f.display())))
}
