// Copyright 2024 Pelikan Foundation
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::BufReader;
use std::os::unix::prelude::AsRawFd;
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

    /// Attempts to drive the TLS/SSL handshake to completion. If the return
    /// variant is `Ok` it indicates that the handshake is complete. An error
    /// result of `WouldBlock` indicates that the handshake may complete in the
    /// future. Other error types indicate a handshake failure with no possible
    /// recovery and that the connection should be closed.
    pub fn do_handshake(&mut self) -> Result<()> {
        if self.is_handshaking() {
            let (is_handshaking, complete_io_result) = match &mut self.inner {
                ConnectionType::Server(s) => {
                    let result = s.conn.complete_io(&mut s.sock);
                    (s.conn.is_handshaking(), result)
                }
                ConnectionType::Client(s) => {
                    let result = s.conn.complete_io(&mut s.sock);
                    (s.conn.is_handshaking(), result)
                }
            };

            if is_handshaking {
                match complete_io_result {
                    Ok(_) => {
                        let still_handshaking = match &self.inner {
                            ConnectionType::Server(s) => s.conn.is_handshaking(),
                            ConnectionType::Client(s) => s.conn.is_handshaking(),
                        };
                        if !still_handshaking {
                            metric! {
                                STREAM_HANDSHAKE.increment();
                            }
                            self.state = TlsState::Negotiated;
                            Ok(())
                        } else {
                            Err(Error::from(ErrorKind::WouldBlock))
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        Err(Error::from(ErrorKind::WouldBlock))
                    }
                    Err(e) => {
                        metric! {
                            STREAM_HANDSHAKE.increment();
                            STREAM_HANDSHAKE_EX.increment();
                        }
                        Err(Error::new(
                            ErrorKind::Other,
                            format!("handshake failed: {}", e),
                        ))
                    }
                }
            } else {
                metric! {
                    STREAM_HANDSHAKE.increment();
                }
                self.state = TlsState::Negotiated;
                Ok(())
            }
        } else {
            Ok(())
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
                        Ok(ShutdownResult::Sent)
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(ShutdownResult::Sent),
                    Err(e) => Err(Error::new(ErrorKind::Other, e.to_string())),
                }
            }
            ConnectionType::Client(s) => {
                s.conn.send_close_notify();
                match s.conn.complete_io(&mut s.sock) {
                    Ok(_) => {
                        metric! {
                            STREAM_SHUTDOWN.increment();
                        }
                        Ok(ShutdownResult::Sent)
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(ShutdownResult::Sent),
                    Err(e) => Err(Error::new(ErrorKind::Other, e.to_string())),
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

/// Provides a wrapped acceptor for server-side TLS. This returns our wrapped
/// `TlsStream` type so that clients can store negotiated and handshaking
/// streams in a structure with a uniform type.
pub struct TlsTcpAcceptor {
    config: Arc<ServerConfig>,
}

impl TlsTcpAcceptor {
    pub fn build(builder: TlsTcpAcceptorBuilder) -> Result<TlsTcpAcceptor> {
        // Load certificates
        let certs = match (&builder.certificate_chain_file, &builder.certificate_file) {
            (Some(chain), Some(cert)) => {
                // Load leaf certificate first
                let cert_file = std::fs::File::open(cert).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to open certificate file: {}: {}", cert.display(), e),
                    )
                })?;
                let mut cert_reader = BufReader::new(cert_file);
                let mut certs: Vec<CertificateDer<'static>> =
                    rustls_pemfile::certs(&mut cert_reader)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::Other,
                                format!(
                                    "failed to parse certificate file: {}: {}",
                                    cert.display(),
                                    e
                                ),
                            )
                        })?;

                // Then append chain certificates
                let chain_file = std::fs::File::open(chain).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!(
                            "failed to open certificate chain file: {}: {}",
                            chain.display(),
                            e
                        ),
                    )
                })?;
                let mut chain_reader = BufReader::new(chain_file);
                let chain_certs: Vec<CertificateDer<'static>> =
                    rustls_pemfile::certs(&mut chain_reader)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::Other,
                                format!(
                                    "failed to parse certificate chain file: {}: {}",
                                    chain.display(),
                                    e
                                ),
                            )
                        })?;
                certs.extend(chain_certs);
                certs
            }
            (Some(chain), None) => {
                // Load complete chain from one file
                let chain_file = std::fs::File::open(chain).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!(
                            "failed to open certificate chain file: {}: {}",
                            chain.display(),
                            e
                        ),
                    )
                })?;
                let mut chain_reader = BufReader::new(chain_file);
                rustls_pemfile::certs(&mut chain_reader)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!(
                                "failed to parse certificate chain file: {}: {}",
                                chain.display(),
                                e
                            ),
                        )
                    })?
            }
            (None, Some(cert)) => {
                // Load just the leaf certificate
                let cert_file = std::fs::File::open(cert).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to open certificate file: {}: {}", cert.display(), e),
                    )
                })?;
                let mut cert_reader = BufReader::new(cert_file);
                rustls_pemfile::certs(&mut cert_reader)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!(
                                "failed to parse certificate file: {}: {}",
                                cert.display(),
                                e
                            ),
                        )
                    })?
            }
            (None, None) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "no certificate file or certificate chain file provided",
                ));
            }
        };

        // Load private key
        let key = if let Some(f) = &builder.private_key_file {
            let key_file = std::fs::File::open(f).map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("failed to open private key file: {}: {}", f.display(), e),
                )
            })?;
            let mut key_reader = BufReader::new(key_file);

            // Try to read any supported private key format
            let mut keys = Vec::new();
            loop {
                match rustls_pemfile::read_one(&mut key_reader) {
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
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("failed to parse private key file: {}: {}", f.display(), e),
                        ));
                    }
                }
            }

            keys.into_iter().next().ok_or_else(|| {
                Error::new(
                    ErrorKind::Other,
                    format!("no private key found in file: {}", f.display()),
                )
            })?
        } else {
            return Err(Error::new(ErrorKind::Other, "no private key file provided"));
        };

        // Build server config
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("failed to build TLS config: {}", e),
                )
            })?;

        Ok(TlsTcpAcceptor {
            config: Arc::new(config),
        })
    }

    pub fn accept(&self, stream: TcpStream) -> Result<TlsTcpStream> {
        let conn = ServerConnection::new(Arc::clone(&self.config)).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("failed to create server connection: {}", e),
            )
        })?;

        let mut tls_stream = StreamOwned::new(conn, stream);

        // Try to perform initial handshake
        match tls_stream.conn.complete_io(&mut tls_stream.sock) {
            Ok(_) => {
                if tls_stream.conn.is_handshaking() {
                    Ok(TlsTcpStream {
                        inner: ConnectionType::Server(tls_stream),
                        state: TlsState::Handshaking,
                    })
                } else {
                    Ok(TlsTcpStream {
                        inner: ConnectionType::Server(tls_stream),
                        state: TlsState::Negotiated,
                    })
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(TlsTcpStream {
                inner: ConnectionType::Server(tls_stream),
                state: TlsState::Handshaking,
            }),
            Err(e) => Err(Error::new(
                ErrorKind::Other,
                format!("handshake failed: {}", e),
            )),
        }
    }
}

/// Provides a wrapped connector for client-side TLS. This returns our wrapped
/// `TlsStream` type so that clients can store negotiated and handshaking
/// streams in a structure with a uniform type.
#[allow(dead_code)]
pub struct TlsTcpConnector {
    config: Arc<rustls::ClientConfig>,
}

impl TlsTcpConnector {
    pub fn build(builder: TlsTcpConnectorBuilder) -> Result<TlsTcpConnector> {
        // Start with root certificates
        let mut root_store = rustls::RootCertStore::empty();

        // Add webpki roots as default
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        // Load CA file if provided
        if let Some(f) = &builder.ca_file {
            let ca_file = std::fs::File::open(f).map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("failed to open CA file: {}: {}", f.display(), e),
                )
            })?;
            let mut ca_reader = BufReader::new(ca_file);
            let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to parse CA file: {}: {}", f.display(), e),
                    )
                })?;
            for cert in ca_certs {
                root_store.add(cert).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to add CA certificate: {}", e),
                    )
                })?;
            }
        }

        // Build client config
        let config_builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

        // Add client certificate if provided
        let config = if builder.private_key_file.is_some() {
            // Load certificates
            let certs = match (&builder.certificate_chain_file, &builder.certificate_file) {
                (Some(chain), Some(cert)) => {
                    let cert_file = std::fs::File::open(cert).map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("failed to open certificate file: {}: {}", cert.display(), e),
                        )
                    })?;
                    let mut cert_reader = BufReader::new(cert_file);
                    let mut certs: Vec<CertificateDer<'static>> =
                        rustls_pemfile::certs(&mut cert_reader)
                            .collect::<std::result::Result<Vec<_>, _>>()
                            .map_err(|e| {
                                Error::new(
                                    ErrorKind::Other,
                                    format!(
                                        "failed to parse certificate file: {}: {}",
                                        cert.display(),
                                        e
                                    ),
                                )
                            })?;

                    let chain_file = std::fs::File::open(chain).map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!(
                                "failed to open certificate chain file: {}: {}",
                                chain.display(),
                                e
                            ),
                        )
                    })?;
                    let mut chain_reader = BufReader::new(chain_file);
                    let chain_certs: Vec<CertificateDer<'static>> =
                        rustls_pemfile::certs(&mut chain_reader)
                            .collect::<std::result::Result<Vec<_>, _>>()
                            .map_err(|e| {
                                Error::new(
                                    ErrorKind::Other,
                                    format!(
                                        "failed to parse certificate chain file: {}: {}",
                                        chain.display(),
                                        e
                                    ),
                                )
                            })?;
                    certs.extend(chain_certs);
                    certs
                }
                (Some(chain), None) => {
                    let chain_file = std::fs::File::open(chain).map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!(
                                "failed to open certificate chain file: {}: {}",
                                chain.display(),
                                e
                            ),
                        )
                    })?;
                    let mut chain_reader = BufReader::new(chain_file);
                    rustls_pemfile::certs(&mut chain_reader)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::Other,
                                format!(
                                    "failed to parse certificate chain file: {}: {}",
                                    chain.display(),
                                    e
                                ),
                            )
                        })?
                }
                (None, Some(cert)) => {
                    let cert_file = std::fs::File::open(cert).map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("failed to open certificate file: {}: {}", cert.display(), e),
                        )
                    })?;
                    let mut cert_reader = BufReader::new(cert_file);
                    rustls_pemfile::certs(&mut cert_reader)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::Other,
                                format!(
                                    "failed to parse certificate file: {}: {}",
                                    cert.display(),
                                    e
                                ),
                            )
                        })?
                }
                (None, None) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "no certificate file or certificate chain file provided",
                    ));
                }
            };

            // Load private key
            let key = if let Some(f) = &builder.private_key_file {
                let key_file = std::fs::File::open(f).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to open private key file: {}: {}", f.display(), e),
                    )
                })?;
                let mut key_reader = BufReader::new(key_file);

                let mut keys = Vec::new();
                loop {
                    match rustls_pemfile::read_one(&mut key_reader) {
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
                            return Err(Error::new(
                                ErrorKind::Other,
                                format!("failed to parse private key file: {}: {}", f.display(), e),
                            ));
                        }
                    }
                }

                keys.into_iter().next().ok_or_else(|| {
                    Error::new(
                        ErrorKind::Other,
                        format!("no private key found in file: {}", f.display()),
                    )
                })?
            } else {
                unreachable!()
            };

            config_builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to build TLS config: {}", e),
                    )
                })?
        } else {
            config_builder.with_no_client_auth()
        };

        Ok(TlsTcpConnector {
            config: Arc::new(config),
        })
    }

    pub fn connect<A: ToSocketAddrs>(&self, addr: A) -> Result<TlsTcpStream> {
        let addrs: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();
        let mut s = Err(Error::new(ErrorKind::Other, "failed to resolve"));
        for addr in addrs {
            s = TcpStream::connect(addr);
            if s.is_ok() {
                break;
            }
        }
        let stream = s?;

        // For client connections, we need a server name
        // Use a placeholder since we're connecting by IP
        let server_name = ServerName::try_from("localhost")
            .map_err(|e| Error::new(ErrorKind::Other, format!("invalid server name: {}", e)))?;

        let conn = ClientConnection::new(Arc::clone(&self.config), server_name.to_owned())
            .map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("failed to create client connection: {}", e),
                )
            })?;

        let mut tls_stream = StreamOwned::new(conn, stream);

        // Try to perform initial handshake
        match tls_stream.conn.complete_io(&mut tls_stream.sock) {
            Ok(_) => {
                if tls_stream.conn.is_handshaking() {
                    Ok(TlsTcpStream {
                        inner: ConnectionType::Client(tls_stream),
                        state: TlsState::Handshaking,
                    })
                } else {
                    Ok(TlsTcpStream {
                        inner: ConnectionType::Client(tls_stream),
                        state: TlsState::Negotiated,
                    })
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(TlsTcpStream {
                inner: ConnectionType::Client(tls_stream),
                state: TlsState::Handshaking,
            }),
            Err(e) => Err(Error::new(
                ErrorKind::Other,
                format!("handshake failed: {}", e),
            )),
        }
    }
}
