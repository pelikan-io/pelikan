// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Pingserver process construction.

use config::PingserverConfig;
use entrystore::Noop;
use logger::configure_logging;
use protocol_ping::{PingProtocol, Request, Response};
use server::{Process, ProcessBuilder};

/// A running Pingserver process.
pub struct Pingserver {
    process: Process,
}

impl Pingserver {
    /// Creates and starts a Pingserver process.
    pub fn new(config: PingserverConfig) -> Result<Self, std::io::Error> {
        let log = configure_logging(&config);
        common::metrics::init();
        let storage = Noop::new();
        let protocol = PingProtocol::default();
        let process = ProcessBuilder::<PingProtocol, Request, Response, Noop>::new(
            &config, log, protocol, storage,
        )?
        .version(env!("CARGO_PKG_VERSION"))
        .spawn();
        Ok(Self { process })
    }

    /// Returns the bound data-listener address.
    pub fn data_addr(&self) -> std::net::SocketAddr {
        self.process.data_addr()
    }

    /// Returns the bound administrative-listener address.
    pub fn admin_addr(&self) -> std::net::SocketAddr {
        self.process.admin_addr()
    }

    /// Waits for all process threads to terminate.
    pub fn wait(self) {
        self.process.wait()
    }

    /// Shuts down the process and waits for termination.
    pub fn shutdown(self) {
        self.process.shutdown()
    }
}

common::metrics::test_no_duplicates!();
