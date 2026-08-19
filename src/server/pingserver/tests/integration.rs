// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use config::{PingserverConfig, ServerConfig};
use pelikan_pingserver::Pingserver;
use server::{backend_resolution, FallbackReason, IoBackend};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

fn configure(backend: &str) -> PingserverConfig {
    let mut config = PingserverConfig::default();
    config.server_mut().set_host("127.0.0.1");
    config.server_mut().set_port("0");
    config.server_mut().set_io_backend(backend);
    config.admin_mut().set_host("127.0.0.1");
    config.admin_mut().set_port("0");
    config
}

fn connect(addr: std::net::SocketAddr) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match TcpStream::connect(addr) {
            Ok(stream) => return stream,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                std::thread::yield_now();
            }
            Err(error) => panic!("listener {addr} did not start: {error}"),
        }
    }
}

fn run_backend(backend: &str) {
    let server = Pingserver::new(configure(backend)).expect("failed to launch pingserver");
    let data = server.data_addr();
    let admin = server.admin_addr();
    assert_ne!(data, admin);

    if backend == "ringline" {
        let resolution = backend_resolution();
        assert_eq!(resolution.requested, IoBackend::Ringline);
        if cfg!(feature = "ringline-force-mio") {
            assert_eq!(resolution.active, IoBackend::Ringline);
            assert_eq!(resolution.fallback, None);
        } else {
            match (resolution.active, resolution.fallback) {
                (IoBackend::Ringline, None) => {}
                (IoBackend::Mio, Some(FallbackReason::UnsupportedCapability(cause))) => {
                    assert!(!cause.is_empty())
                }
                other => panic!("unexpected Ringline resolution: {other:?}"),
            }
        }
    }

    let mut client = connect(data);
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    client.write_all(b"PING\r\n").unwrap();
    let mut pong = [0_u8; 6];
    client.read_exact(&mut pong).unwrap();
    assert_eq!(&pong, b"PONG\r\n");
    drop(client);

    let resolution = backend_resolution();
    let fallback = resolution
        .fallback
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "none".to_string());
    let mut admin_client = connect(admin);
    admin_client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    admin_client.write_all(b"stats\r\n").unwrap();
    let mut output = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let count = admin_client.read(&mut chunk).unwrap();
        assert_ne!(count, 0, "admin closed before END");
        output.extend_from_slice(&chunk[..count]);
        if output.windows(5).any(|window| window == b"END\r\n") {
            break;
        }
    }
    let output = String::from_utf8(output).unwrap();
    assert!(
        output.contains(&format!(
            "STAT server_io_backend_requested {}\r\n",
            resolution.requested
        )),
        "missing requested backend in {output:?}"
    );
    assert!(
        output.contains(&format!(
            "STAT server_io_backend_active_name {}\r\n",
            resolution.active
        )),
        "missing active backend in {output:?}"
    );
    assert!(
        output.contains(&format!(
            "STAT server_io_backend_fallback_cause {fallback}\r\n"
        )),
        "missing fallback cause in {output:?}"
    );
    drop(admin_client);

    let started = Instant::now();
    server.shutdown();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(TcpStream::connect(data).is_err(), "data listener leaked");
    assert!(TcpStream::connect(admin).is_err(), "admin listener leaked");
}

fn main() {
    #[cfg(not(feature = "ringline"))]
    run_backend("mio");
    #[cfg(all(feature = "ringline", target_os = "linux"))]
    run_backend("ringline");
}
