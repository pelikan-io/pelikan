// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use config::PingproxyConfig;
use logger::configure_logging;
use protocol_ping::*;
use proxy::{Process, ProcessBuilder};

type BackendProtocol = PingProtocol;
type BackendRequest = Request;
type BackendResponse = Response;

type FrontendProtocol = PingProtocol;
type FrontendRequest = Request;
type FrontendResponse = Response;

#[allow(dead_code)]
pub struct Pingproxy {
    process: Process,
}

impl Default for Pingproxy {
    fn default() -> Self {
        Self::new(PingproxyConfig::default())
    }
}

impl Pingproxy {
    /// Creates a new `Pingproxy` process from the given `PingproxyConfig`.
    pub fn new(config: PingproxyConfig) -> Self {
        // initialize logging
        let log_drain = configure_logging(&config);

        // initialize metrics
        common::metrics::init();

        // initialize parsers
        let frontend_protocol = FrontendProtocol::default();
        let backend_protocol = BackendProtocol::default();

        // initialize process
        let process_builder =
            ProcessBuilder::<
                BackendProtocol,
                BackendRequest,
                BackendResponse,
                FrontendProtocol,
                FrontendRequest,
                FrontendResponse,
            >::new(&config, log_drain, frontend_protocol, backend_protocol)
            .expect("failed to launch");
        let process = process_builder.spawn();

        Self { process }
    }

    /// Wait for all threads to complete. Blocks until the process has fully
    /// terminated. Under normal conditions, this will block indefinitely.
    pub fn wait(self) {
        self.process.wait()
    }

    /// Triggers a shutdown of the process and blocks until the process has
    /// fully terminated. This is more likely to be used for running integration
    /// tests or other automated testing.
    pub fn shutdown(self) {
        self.process.shutdown()
    }
}

common::metrics::test_no_duplicates!();
