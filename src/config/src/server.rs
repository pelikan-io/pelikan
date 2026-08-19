// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use serde::{Deserialize, Serialize};

use std::net::{AddrParseError, SocketAddr};

// constants to define default values
const SERVER_HOST: &str = "0.0.0.0";
const SERVER_PORT: &str = "12321";
const SERVER_TIMEOUT: usize = 100;
const SERVER_NEVENT: usize = 1024;
const SERVER_IO_BACKEND: &str = "mio";

// helper functions
fn host() -> String {
    SERVER_HOST.to_string()
}

fn port() -> String {
    SERVER_PORT.to_string()
}

fn timeout() -> usize {
    SERVER_TIMEOUT
}

fn nevent() -> usize {
    SERVER_NEVENT
}

fn io_backend() -> String {
    SERVER_IO_BACKEND.to_string()
}

// definitions
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Server {
    #[serde(default = "host")]
    host: String,
    #[serde(default = "port")]
    port: String,
    #[serde(default = "timeout")]
    timeout: usize,
    #[serde(default = "nevent")]
    nevent: usize,
    #[serde(default = "io_backend")]
    io_backend: String,
}

// implementation
impl Server {
    /// Host address to listen on
    pub fn host(&self) -> String {
        self.host.clone()
    }

    /// Port to listen on
    pub fn port(&self) -> String {
        self.port.clone()
    }

    pub fn set_host(&mut self, host: impl Into<String>) {
        self.host = host.into();
    }

    pub fn set_port(&mut self, port: impl Into<String>) {
        self.port = port.into();
    }

    /// Return the result of parsing the host and port
    pub fn socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        format!("{}:{}", self.host(), self.port()).parse()
    }

    /// The poll timeout in milliseconds
    pub fn timeout(&self) -> usize {
        self.timeout
    }

    /// Maximum events to accept in one poll
    pub fn nevent(&self) -> usize {
        self.nevent
    }

    /// I/O backend requested for the cache server
    pub fn io_backend(&self) -> &str {
        &self.io_backend
    }

    /// Select the cache-server I/O backend before startup.
    pub fn set_io_backend(&mut self, backend: impl Into<String>) {
        self.io_backend = backend.into();
    }
}

// trait implementations
impl Default for Server {
    fn default() -> Self {
        Self {
            host: host(),
            port: port(),
            timeout: timeout(),
            nevent: nevent(),
            io_backend: io_backend(),
        }
    }
}

// trait definitions
pub trait ServerConfig {
    fn server(&self) -> &Server;

    fn server_mut(&mut self) -> &mut Server;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_backend_defaults_to_mio() {
        let server: Server = toml::from_str("").unwrap();
        assert_eq!(server.io_backend(), "mio");
    }

    #[test]
    fn io_backend_reads_ringline() {
        let server: Server = toml::from_str("io_backend = 'ringline'").unwrap();
        assert_eq!(server.io_backend(), "ringline");
    }

    #[test]
    fn io_backend_retains_unknown_value_for_network_validation() {
        let server: Server = toml::from_str("io_backend = 'other'").unwrap();
        assert_eq!(server.io_backend(), "other");
    }

    #[test]
    fn io_backend_can_be_selected_programmatically() {
        let mut server = Server::default();
        server.set_io_backend("ringline");
        assert_eq!(server.io_backend(), "ringline");
    }

    #[test]
    fn listener_address_can_be_reserved_programmatically() {
        let mut server = Server::default();
        server.set_host("127.0.0.1");
        server.set_port("43210");
        assert_eq!(
            server.socket_addr().unwrap(),
            "127.0.0.1:43210".parse().unwrap()
        );
    }
}
