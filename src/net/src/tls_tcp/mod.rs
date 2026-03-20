mod rustls_impl;

pub use rustls_impl::{
    ShutdownResult, TlsTcpAcceptor, TlsTcpAcceptorBuilder, TlsTcpConnector,
    TlsTcpConnectorBuilder, TlsTcpStream,
};
