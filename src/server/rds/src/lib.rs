// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! RDS is a work-in-progress RESP protocol server.

use config::*;
use entrystore::Seg;
use logger::*;
use protocol_resp::{Request, RequestParser, Response};
use server::{Process, ProcessBuilder};

type Parser = RequestParser;
type Storage = Seg;

/// This structure represents a running `Rds` process.
#[allow(dead_code)]
pub struct Rds {
    process: Process,
}

impl Rds {
    /// Creates a new [Rds] process from the given [SegcacheConfig].
    pub fn new(config: SegcacheConfig) -> Result<Self, std::io::Error> {
        // initialize logging
        let log_drain = configure_logging(&config);

        // initialize metrics
        common::metrics::init();

        // initialize storage
        let storage = Storage::new(&config)?;

        // initialize parser
        let parser = Parser::new();

        // initialize process
        let process_builder = ProcessBuilder::<Parser, Request, Response, Storage>::new(
            &config, log_drain, parser, storage,
        )?
        .version(env!("CARGO_PKG_VERSION"));

        // spawn threads
        let process = process_builder.spawn();

        Ok(Self { process })
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
