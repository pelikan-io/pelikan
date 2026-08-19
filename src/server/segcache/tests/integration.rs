// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This test module runs the integration test suite against a single-threaded
//! instance of Segcache.

mod common;

#[macro_use]
extern crate logger;

use crate::common::*;

use config::{SegcacheConfig, ServerConfig};
use pelikan_segcache::Segcache;
use server::{SERVER_IO_BACKEND_ACTIVE, SERVER_IO_BACKEND_FALLBACK};

use std::time::Duration;

fn run_backend(backend: &str) {
    debug!("launching {backend} server");
    let fallback_before = SERVER_IO_BACKEND_FALLBACK.value();
    let mut config = SegcacheConfig::default();
    config.server_mut().set_io_backend(backend);
    let server = Segcache::new(config).expect("failed to launch segcache");
    std::thread::sleep(Duration::from_secs(1));
    if backend == "ringline" && SERVER_IO_BACKEND_ACTIVE.value() == 0 {
        assert!(SERVER_IO_BACKEND_FALLBACK.value() > fallback_before);
        println!("Ringline capability unavailable; exact fallback_cause is printed in the structured startup event above; exercising the Mio fallback");
    }
    tests();
    conformance_tests();
    admin_tests();
    let started = std::time::Instant::now();
    server.shutdown();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(std::net::TcpStream::connect("127.0.0.1:12321").is_err());
}

#[cfg(target_os = "linux")]
fn repeated_ringline_startup_releases_resources() {
    for _ in 0..8 {
        let mut config = SegcacheConfig::default();
        config.server_mut().set_io_backend("ringline");
        let server = Segcache::new(config).expect("repeated Ringline startup failed");
        std::thread::sleep(Duration::from_millis(100));
        smoke_exchange();
        server.shutdown();
    }
}

fn main() {
    run_backend("mio");
    #[cfg(target_os = "linux")]
    {
        run_backend("ringline");
        repeated_ringline_startup_releases_resources();
    }
    info!("passed!");
}
