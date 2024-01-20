// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use serde::{Deserialize, Serialize};

use std::net::{AddrParseError, SocketAddr, ToSocketAddrs};

// constants to define default values
const LISTEN_ADDRESS: &str = "0.0.0.0:12322";
const TIMEOUT_MS: usize = 100;
const NEVENT_MAX: usize = 1024;
const FRONTEND_THREADS: usize = 1;
const BACKEND_THREADS: usize = 1;
const BACKEND_POOLSIZE: usize = 1;

// helper functions
fn address() -> String {
    LISTEN_ADDRESS.to_string()
}

fn timeout() -> usize {
    TIMEOUT_MS
}

fn nevent() -> usize {
    NEVENT_MAX
}

fn frontend_threads() -> usize {
    FRONTEND_THREADS
}

fn backend_threads() -> usize {
    BACKEND_THREADS
}

fn backend_poolsize() -> usize {
    BACKEND_POOLSIZE
}

// definitions
#[derive(Serialize, Deserialize, Debug)]
pub struct Listener {
    #[serde(default = "address")]
    address: String,
    #[serde(default = "timeout")]
    timeout: usize,
    #[serde(default = "nevent")]
    nevent: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Frontend {
    #[serde(default = "timeout")]
    timeout: usize,
    #[serde(default = "nevent")]
    nevent: usize,
    #[serde(default = "frontend_threads")]
    threads: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Backend {
    #[serde(default = "timeout")]
    timeout: usize,
    #[serde(default = "nevent")]
    nevent: usize,
    #[serde(default = "backend_threads")]
    threads: usize,
    #[serde(default = "backend_poolsize")]
    poolsize: usize,
    endpoints: Vec<String>,
}

// implementation
impl Listener {
    /// Return the result of parsing the host and port
    pub fn socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        self.address.parse()
    }

    /// The poll timeout in milliseconds
    pub fn timeout(&self) -> usize {
        self.timeout
    }

    /// Maximum events to accept in one poll
    pub fn nevent(&self) -> usize {
        self.nevent
    }
}

impl Frontend {
    /// Number of frontend threads to launch
    pub fn threads(&self) -> usize {
        self.threads
    }

    /// The poll timeout in milliseconds
    pub fn timeout(&self) -> usize {
        self.timeout
    }

    /// Maximum events to accept in one poll
    pub fn nevent(&self) -> usize {
        self.nevent
    }
}

impl Backend {
    /// Number of backend threads to launch
    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Number of connections to each server endpoint from each backend thread
    pub fn poolsize(&self) -> usize {
        self.poolsize
    }

    /// The poll timeout in milliseconds
    pub fn timeout(&self) -> usize {
        self.timeout
    }

    /// Maximum events to accept in one poll
    pub fn nevent(&self) -> usize {
        self.nevent
    }

    // TODO(bmartin): the handling of ZK service discovery is based on how
    // Aurora serversets work and needs to be factored out into some more
    // general way of handling service discovery. We may want to allow for
    // sending topology information to the admin port so that a sidecar can be
    // used to handle service discovery.
    pub fn socket_addrs(&self) -> Result<Vec<SocketAddr>, std::io::Error> {
        if !self.endpoints.is_empty() {
            let mut endpoints = Vec::new();
            for endpoint in &self.endpoints {
                match endpoint.to_socket_addrs() {
                    Ok(mut addrs) => {
                        if let Some(addr) = addrs.next() {
                            endpoints.push(addr)
                        } else {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "failed to resolve endpoint address",
                            ));
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Ok(endpoints)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no endpoints provided",
            ))
        }
    }
}

// trait implementations
impl Default for Listener {
    fn default() -> Self {
        Self {
            address: address(),
            timeout: timeout(),
            nevent: nevent(),
        }
    }
}

impl Default for Frontend {
    fn default() -> Self {
        Self {
            timeout: timeout(),
            nevent: nevent(),
            threads: frontend_threads(),
        }
    }
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            timeout: timeout(),
            nevent: nevent(),
            threads: backend_threads(),
            endpoints: Vec::new(),
            poolsize: backend_poolsize(),
        }
    }
}

// trait definitions
pub trait ListenerConfig {
    fn listener(&self) -> &Listener;
}

pub trait FrontendConfig {
    fn frontend(&self) -> &Frontend;
}

pub trait BackendConfig {
    fn backend(&self) -> &Backend;
}
