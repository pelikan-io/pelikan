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
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

fn configure(backend: &str) -> RdsConfig {
    let mut config = RdsConfig::default();
    config.server_mut().set_host("127.0.0.1");
    config.server_mut().set_port("0");
    config.server_mut().set_io_backend(backend);
    config.worker_mut().set_threads(2);
    config.admin_mut().set_host("127.0.0.1");
    config.admin_mut().set_port("0");
    config
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
    if cfg!(feature = "ringline-force-mio") {
        assert_eq!(resolution.active, IoBackend::Ringline);
        assert_eq!(resolution.fallback, None);
        return;
    }
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
    let config = configure(backend);
    let server = Rds::new(config).expect("failed to launch rds");
    let data = server.data_addr();
    let admin = server.admin_addr();
    assert_ne!(data, admin);
    assert_ne!(data.port(), 0);
    assert_ne!(admin.port(), 0);
    set_test_addresses(data, admin);
    if !cfg!(feature = "ringline-force-mio") {
        wait_until_listening(data);
    }
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
    let resolution = backend_resolution();
    let fallback = resolution
        .fallback
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "none".to_string());
    assert_admin_backend_info(
        &resolution.requested.to_string(),
        &resolution.active.to_string(),
        &fallback,
    );
    let started = Instant::now();
    server.shutdown();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(TcpStream::connect(data).is_err(), "data listener leaked");
    assert!(TcpStream::connect(admin).is_err(), "admin listener leaked");
}

#[cfg(all(feature = "ringline", target_os = "linux"))]
fn repeated_ringline_startup_releases_resources() {
    for _ in 0..8 {
        let config = configure("ringline");
        let server = Rds::new(config).expect("repeated Ringline startup failed");
        let data = server.data_addr();
        let admin = server.admin_addr();
        assert_ne!(data, admin);
        set_test_addresses(data, admin);
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
    #[cfg(not(feature = "ringline-force-mio"))]
    run_backend("mio");
    #[cfg(all(feature = "ringline", target_os = "linux"))]
    {
        run_backend("ringline");
        repeated_ringline_startup_releases_resources();
    }
    info!("passed!");
}
