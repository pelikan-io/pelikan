// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Runs the RDS conformance suite with two workers.

mod common;

#[macro_use]
extern crate logger;

use crate::common::*;
use config::{RdsConfig, ServerConfig, WorkerConfig};
use pelikan_rds::Rds;
use server::{backend_resolution, FallbackReason, IoBackend, SERVER_IO_BACKEND_FALLBACK};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

fn reserved_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn configure(backend: &str) -> (RdsConfig, SocketAddr, SocketAddr) {
    let data = reserved_address();
    let admin = reserved_address();
    assert_ne!(data, admin);
    let mut config = RdsConfig::default();
    config.server_mut().set_host("127.0.0.1");
    config.server_mut().set_port(data.port().to_string());
    config.server_mut().set_io_backend(backend);
    config.worker_mut().set_threads(2);
    config.admin_mut().set_host("127.0.0.1");
    config.admin_mut().set_port(admin.port().to_string());
    set_test_addresses(data, admin);
    (config, data, admin)
}

fn wait_until_listening(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while TcpStream::connect(addr).is_err() {
        assert!(Instant::now() < deadline, "listener {addr} did not start");
        std::thread::yield_now();
    }
}

fn assert_ringline_resolution() {
    let resolution = backend_resolution();
    assert_eq!(resolution.requested, IoBackend::Ringline);
    match (resolution.active, resolution.fallback) {
        (IoBackend::Ringline, None) => {}
        (IoBackend::Mio, Some(FallbackReason::UnsupportedCapability(cause))) => {
            assert!(!cause.is_empty());
            println!("Ringline unsupported capability: {cause}");
        }
        other => panic!("Ringline startup produced a non-capability fallback: {other:?}"),
    }
}

fn run_backend(backend: &str) {
    let fallback_before = SERVER_IO_BACKEND_FALLBACK.value();
    let (config, data, admin) = configure(backend);
    let server = Rds::new(config).expect("failed to launch rds");
    wait_until_listening(data);
    wait_until_listening(admin);
    if backend == "ringline" {
        assert_ringline_resolution();
        if backend_resolution().active == IoBackend::Mio {
            assert_eq!(SERVER_IO_BACKEND_FALLBACK.value(), fallback_before + 1);
        }
    }
    tests();
    conformance_tests();
    admin_tests();
    let started = Instant::now();
    server.shutdown();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(TcpStream::connect(data).is_err(), "data listener leaked");
    assert!(TcpStream::connect(admin).is_err(), "admin listener leaked");
}

#[cfg(target_os = "linux")]
fn repeated_ringline_startup_releases_resources() {
    for _ in 0..8 {
        let (config, data, admin) = configure("ringline");
        let server = Rds::new(config).expect("repeated Ringline startup failed");
        wait_until_listening(data);
        wait_until_listening(admin);
        assert_ringline_resolution();
        smoke_exchange();
        server.shutdown();
        assert!(TcpStream::connect(data).is_err(), "data listener leaked");
        assert!(TcpStream::connect(admin).is_err(), "admin listener leaked");
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
