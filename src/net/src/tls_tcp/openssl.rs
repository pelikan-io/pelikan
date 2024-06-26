// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

pub use ::openssl::ssl::ShutdownResult;

use std::os::unix::prelude::AsRawFd;

use ::openssl::ssl::{ErrorCode, Ssl, SslFiletype, SslMethod, SslStream};
use ::openssl::x509::X509;
use foreign_types_shared_01::ForeignTypeRef;

use crate::*;

#[derive(PartialEq)]
enum TlsState {
    Handshaking,
    Negotiated,
}

/// Wraps a TLS/SSL stream so that negotiated and handshaking sessions have a
/// uniform type.
pub struct TlsTcpStream {
    inner: SslStream<TcpStream>,
    state: TlsState,
}

impl AsRawFd for TlsTcpStream {
    fn as_raw_fd(&self) -> i32 {
        self.inner.get_ref().as_raw_fd()
    }
}

impl TlsTcpStream {
    pub fn set_nodelay(&mut self, nodelay: bool) -> Result<()> {
        self.inner.get_mut().set_nodelay(nodelay)
    }

    pub fn is_handshaking(&self) -> bool {
        self.state == TlsState::Handshaking
    }

    /// Attempts to drive the TLS/SSL handshake to completion. If the return
    /// variant is `Ok` it indiates that the handshake is complete. An error
    /// result of `WouldBlock` indicates that the handshake may complete in the
    /// future. Other error types indiate a handshake failure with no possible
    /// recovery and that the connection should be closed.
    pub fn do_handshake(&mut self) -> Result<()> {
        if self.is_handshaking() {
            let ptr = self.inner.ssl().as_ptr();
            let ret = unsafe { openssl_sys::SSL_do_handshake(ptr) };
            if ret > 0 {
                metrics! {
                    STREAM_HANDSHAKE.increment();
                }

                self.state = TlsState::Negotiated;

                Ok(())
            } else {
                let code = unsafe { ErrorCode::from_raw(openssl_sys::SSL_get_error(ptr, ret)) };
                match code {
                    ErrorCode::WANT_READ | ErrorCode::WANT_WRITE => {
                        Err(Error::from(ErrorKind::WouldBlock))
                    }
                    _ => {
                        metrics! {
                            STREAM_HANDSHAKE.increment();
                            STREAM_HANDSHAKE_EX.increment();
                        }

                        Err(Error::new(ErrorKind::Other, "handshake failed"))
                    }
                }
            }
        } else {
            Ok(())
        }
    }

    pub fn shutdown(&mut self) -> Result<ShutdownResult> {
        self.inner
            .shutdown()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
    }
}

impl Debug for TlsTcpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.inner.get_ref())
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
            self.inner.read(buf)
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
            self.inner.write(buf)
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.is_handshaking() {
            Err(Error::new(
                ErrorKind::WouldBlock,
                "flush on handshaking session would block",
            ))
        } else {
            self.inner.flush()
        }
    }
}

impl event::Source for TlsTcpStream {
    fn register(&mut self, registry: &Registry, token: Token, interest: Interest) -> Result<()> {
        self.inner.get_mut().register(registry, token, interest)
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interest: mio::Interest,
    ) -> Result<()> {
        self.inner.get_mut().reregister(registry, token, interest)
    }

    fn deregister(&mut self, registry: &mio::Registry) -> Result<()> {
        self.inner.get_mut().deregister(registry)
    }
}

/// Provides a wrapped acceptor for server-side TLS. This returns our wrapped
/// `TlsStream` type so that clients can store negotiated and handshaking
/// streams in a structure with a uniform type.
pub struct TlsTcpAcceptor {
    inner: ::openssl::ssl::SslContext,
}

impl TlsTcpAcceptor {
    pub fn build(builder: TlsTcpAcceptorBuilder) -> Result<TlsTcpAcceptor> {
        let mut acceptor =
            ::openssl::ssl::SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_client())
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

        // load the CA file, if provided
        if let Some(f) = builder.ca_file {
            acceptor.set_ca_file(f.clone()).map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("failed to load CA file: {}\n{}", f.display(), e),
                )
            })?;
        }

        // load the private key from file
        if let Some(f) = builder.private_key_file {
            acceptor
                .set_private_key_file(f.clone(), SslFiletype::PEM)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to load private key file: {}\n{}", f.display(), e),
                    )
                })?;
        } else {
            return Err(Error::new(ErrorKind::Other, "no private key file provided"));
        }

        // load the certificate chain, certificate file, or both
        match (builder.certificate_chain_file, builder.certificate_file) {
            (Some(chain), Some(cert)) => {
                // assume we have the leaf in a standalone file, and the
                // intermediates + root in another file

                // first load the leaf
                acceptor
                    .set_certificate_file(cert.clone(), SslFiletype::PEM)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("failed to load certificate file: {}\n{}", cert.display(), e),
                        )
                    })?;

                // append the rest of the chain
                let pem = std::fs::read(chain.clone()).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!(
                            "failed to load certificate chain file: {}\n{}",
                            chain.display(),
                            e
                        ),
                    )
                })?;
                let cert_chain = X509::stack_from_pem(&pem).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!(
                            "failed to load certificate chain file: {}\n{}",
                            chain.display(),
                            e
                        ),
                    )
                })?;
                for cert in cert_chain {
                    acceptor.add_extra_chain_cert(cert).map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!(
                                "bad certificate in certificate chain file: {}\n{}",
                                chain.display(),
                                e
                            ),
                        )
                    })?;
                }
            }
            (Some(chain), None) => {
                // assume we have a complete chain: leaf + intermediates + root in
                // one file

                // load the entire chain
                acceptor
                    .set_certificate_chain_file(chain.clone())
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!(
                                "failed to load certificate chain file: {}\n{}",
                                chain.display(),
                                e
                            ),
                        )
                    })?;
            }
            (None, Some(cert)) => {
                // this will just load the leaf certificate from the file
                acceptor
                    .set_certificate_file(cert.clone(), SslFiletype::PEM)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("failed to load certificate file: {}\n{}", cert.display(), e),
                        )
                    })?;
            }
            (None, None) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "no certificate file or certificate chain file provided",
                ));
            }
        }

        let inner = acceptor.build().into_context();

        Ok(TlsTcpAcceptor { inner })
    }

    pub fn accept(&self, stream: TcpStream) -> Result<TlsTcpStream> {
        let ssl = Ssl::new(&self.inner)?;

        let stream = SslStream::new(ssl, stream)?;

        let ret = unsafe { openssl_sys::SSL_accept(stream.ssl().as_ptr()) };

        if ret > 0 {
            Ok(TlsTcpStream {
                inner: stream,
                state: TlsState::Negotiated,
            })
        } else {
            let code = unsafe {
                ErrorCode::from_raw(openssl_sys::SSL_get_error(stream.ssl().as_ptr(), ret))
            };
            match code {
                ErrorCode::WANT_READ | ErrorCode::WANT_WRITE => Ok(TlsTcpStream {
                    inner: stream,
                    state: TlsState::Handshaking,
                }),
                _ => Err(Error::new(ErrorKind::Other, "handshake failed")),
            }
        }
    }
}

/// Provides a wrapped connector for client-side TLS. This returns our wrapped
/// `TlsStream` type so that clients can store negotiated and handshaking
/// streams in a structure with a uniform type.
#[allow(dead_code)]
pub struct TlsTcpConnector {
    inner: ::openssl::ssl::SslContext,
}

impl TlsTcpConnector {
    pub fn build(builder: TlsTcpConnectorBuilder) -> Result<TlsTcpConnector> {
        let mut connector = ::openssl::ssl::SslConnector::builder(SslMethod::tls_client())
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

        // load the CA file, if provided
        if let Some(f) = builder.ca_file {
            connector.set_ca_file(f).map_err(|e| {
                Error::new(ErrorKind::Other, format!("failed to load CA file: {e}"))
            })?;
        }

        // load the private key from file
        if let Some(f) = builder.private_key_file {
            connector
                .set_private_key_file(f, SslFiletype::PEM)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to load private key file: {e}"),
                    )
                })?;
        } else {
            return Err(Error::new(ErrorKind::Other, "no private key file provided"));
        }

        // load the certificate chain, certificate file, or both
        match (builder.certificate_chain_file, builder.certificate_file) {
            (Some(chain), Some(cert)) => {
                // assume we have the leaf in a standalone file, and the
                // intermediates + root in another file

                // first load the leaf
                connector
                    .set_certificate_file(cert, SslFiletype::PEM)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("failed to load certificate file: {e}"),
                        )
                    })?;

                // append the rest of the chain
                let pem = std::fs::read(chain).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to load certificate chain file: {e}"),
                    )
                })?;
                let chain = X509::stack_from_pem(&pem).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to load certificate chain file: {e}"),
                    )
                })?;
                for cert in chain {
                    connector.add_extra_chain_cert(cert).map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("bad certificate in certificate chain file: {e}"),
                        )
                    })?;
                }
            }
            (Some(chain), None) => {
                // assume we have a complete chain: leaf + intermediates + root in
                // one file

                // load the entire chain
                connector.set_certificate_chain_file(chain).map_err(|e| {
                    Error::new(
                        ErrorKind::Other,
                        format!("failed to load certificate chain file: {e}"),
                    )
                })?;
            }
            (None, Some(cert)) => {
                // this will just load the leaf certificate from the file
                connector
                    .set_certificate_file(cert, SslFiletype::PEM)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("failed to load certificate file: {e}"),
                        )
                    })?;
            }
            (None, None) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "no certificate file or certificate chain file provided",
                ));
            }
        }

        let inner = connector.build().into_context();

        Ok(TlsTcpConnector { inner })
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

        let ssl = Ssl::new(&self.inner)?;

        let stream = SslStream::new(ssl, s?)?;

        let ret = unsafe { openssl_sys::SSL_connect(stream.ssl().as_ptr()) };

        if ret > 0 {
            Ok(TlsTcpStream {
                inner: stream,
                state: TlsState::Negotiated,
            })
        } else {
            let code = unsafe {
                ErrorCode::from_raw(openssl_sys::SSL_get_error(stream.ssl().as_ptr(), ret))
            };
            match code {
                ErrorCode::WANT_READ | ErrorCode::WANT_WRITE => Ok(TlsTcpStream {
                    inner: stream,
                    state: TlsState::Handshaking,
                }),
                _ => Err(Error::new(ErrorKind::Other, "handshake failed")),
            }
        }
    }
}

// NOTE: these tests only work if there's a `test` folder within this crate that
// contains the necessary keys and certs. They are left here for reference and
// in the future we should automate creation of self-signed keys and certs for
// use for testing during local development and in CI.

// #[cfg(test)]
// mod tests {
//     use super::*;

//     fn gen_keys() -> Result<(), ()> {

//     }

//     fn create_connector() -> Connector {
//         let tls_connector = TlsTcpConnector::builder()
//             .expect("failed to create builder")
//             .ca_file("test/root.crt")
//             .certificate_chain_file("test/client.crt")
//             .private_key_file("test/client.key")
//             .build()
//             .expect("failed to initialize tls connector");

//         Connector::from(tls_connector)
//     }

//     fn create_listener(addr: &'static str) -> Listener {
//         let tcp_listener = TcpListener::bind(addr).expect("failed to bind");
//         let tls_acceptor = TlsTcpAcceptor::mozilla_intermediate_v5()
//             .expect("failed to create builder")
//             .ca_file("test/root.crt")
//             .certificate_chain_file("test/server.crt")
//             .private_key_file("test/server.key")
//             .build()
//             .expect("failed to initialize tls acceptor");

//         Listener::from((tcp_listener, tls_acceptor))
//     }

//     #[test]
//     fn listener() {
//         let _ = create_listener("127.0.0.1:0");
//     }

//     #[test]
//     fn connector() {
//         let _ = create_connector();
//     }

//     #[test]
//     fn ping_pong() {
//         let connector = create_connector();
//         let listener = create_listener("127.0.0.1:0");

//         let addr = listener.local_addr().expect("listener has no local addr");

//         let mut client_stream = connector.connect(addr).expect("failed to connect");
//         std::thread::sleep(std::time::Duration::from_millis(100));
//         let mut server_stream = listener.accept().expect("failed to accept");

//         let mut server_handshake_complete = false;
//         let mut client_handshake_complete = false;

//         while !(server_handshake_complete && client_handshake_complete) {
//             if !server_handshake_complete {
//                 std::thread::sleep(std::time::Duration::from_millis(100));
//                 if server_stream.do_handshake().is_ok() {
//                     server_handshake_complete = true;
//                 }
//             }

//             if !client_handshake_complete {
//                 std::thread::sleep(std::time::Duration::from_millis(100));
//                 if client_stream.do_handshake().is_ok() {
//                     client_handshake_complete = true;
//                 }
//             }
//         }

//         std::thread::sleep(std::time::Duration::from_millis(100));

//         client_stream
//             .write_all(b"PING\r\n")
//             .expect("failed to write");
//         client_stream.flush().expect("failed to flush");

//         std::thread::sleep(std::time::Duration::from_millis(100));

//         let mut buf = [0; 4096];

//         match server_stream.read(&mut buf) {
//             Ok(6) => {
//                 assert_eq!(&buf[0..6], b"PING\r\n");
//                 server_stream
//                     .write_all(b"PONG\r\n")
//                     .expect("failed to write");
//             }
//             Ok(n) => {
//                 panic!("read: {} bytes but expected 6", n);
//             }
//             Err(e) => {
//                 panic!("error reading: {}", e);
//             }
//         }

//         std::thread::sleep(std::time::Duration::from_millis(100));

//         match client_stream.read(&mut buf) {
//             Ok(6) => {
//                 assert_eq!(&buf[0..6], b"PONG\r\n");
//             }
//             Ok(n) => {
//                 panic!("read: {} bytes but expected 6", n);
//             }
//             Err(e) => {
//                 panic!("error reading: {}", e);
//             }
//         }
//     }
// }
