// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This module provides a set of integration tests and a function to run the
//! tests against an Rds instance. This allows us to run the same test suite
//! for multiple server configurations.

use logger::*;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Mutex;

static TEST_ADDRESSES: Mutex<Option<(SocketAddr, SocketAddr)>> = Mutex::new(None);

pub fn set_test_addresses(data: SocketAddr, admin: SocketAddr) {
    *TEST_ADDRESSES.lock().unwrap() = Some((data, admin));
}

fn data_addr() -> SocketAddr {
    TEST_ADDRESSES
        .lock()
        .unwrap()
        .expect("test addresses not set")
        .0
}

fn admin_addr() -> SocketAddr {
    TEST_ADDRESSES
        .lock()
        .unwrap()
        .expect("test addresses not set")
        .1
}
use std::time::Duration;

pub fn tests() {
    debug!("beginning tests");
    println!();

    // get and gets on a key that is not in the cache results in a miss
    test("get miss", &[("get 0\r\n", Some(RESP_NIL))]);

    // check that we can store and retrieve a key
    test(
        "set and get",
        &[
            // store the key
            ("set foo bar\r\n", Some(RESP_OK)),
            // retrieve the key
            ("get foo\r\n", Some(&bulk_string("bar"))),
        ],
    );

    std::thread::sleep(Duration::from_millis(500));
}

// opens a new connection, operating on request + response pairs from the
// provided data.
fn test(name: &str, data: &[(&str, Option<&str>)]) {
    info!("testing: {name}");
    debug!("connecting to server");
    let mut stream = connected_client();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("failed to set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .expect("failed to set write timeout");

    debug!("sending request");
    for (request, response) in data {
        match stream.write(request.as_bytes()) {
            Ok(bytes) => {
                if bytes == request.len() {
                    debug!("full request sent");
                } else {
                    error!("incomplete write");
                    panic!("status: failed\n");
                }
            }
            Err(_) => {
                error!("error sending request");
                panic!("status: failed\n");
            }
        }

        std::thread::sleep(Duration::from_millis(10));
        let mut buf = vec![0; 4096];

        if let Some(response) = response {
            if let Err(error) = stream.read_exact(&mut buf[..response.len()]) {
                panic!("error reading response: {error:?}");
            } else if response.as_bytes() != &buf[0..response.len()] {
                error!("sent (UTF-8): {request:?}");
                error!("sent (bytes): {:?}", request.as_bytes());
                error!("expected (bytes): {:?}", response.as_bytes());
                error!("received (bytes): {:?}", &buf[0..response.len()]);
                error!("expected (UTF-8): {response:?}");
                let resp = std::str::from_utf8(&buf[0..response.len()])
                    .expect("received invalid UTF-8 from Rds");
                error!("received (UTF-8): {resp}");
                std::thread::sleep(Duration::from_millis(500));
                panic!("status: failed\n");
            } else {
                debug!("correct response");
            }
            assert_eq!(response.as_bytes(), &buf[0..response.len()]);
        } else if let Err(e) = stream.read(&mut buf) {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                debug!("got no response");
            } else {
                error!("error reading response");
                std::thread::sleep(Duration::from_millis(500));
                panic!("status: failed\n");
            }
        } else {
            error!("expected no response");
            std::thread::sleep(Duration::from_millis(500));
            panic!("status: failed\n");
        }

        if data.len() > 1 {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    info!("status: passed\n");
}

#[cfg(feature = "ringline")]
pub fn smoke_exchange() {
    let mut stream = connected_client();
    exchange(&mut stream, b"get task7-smoke\r\n", b"$-1\r\n");
}

pub fn conformance_tests() {
    partial_request_is_completed_after_second_write();
    pipelined_requests_preserve_response_order();
    partial_request_disconnect_cancels_mutation();
    connection_burst_does_not_block_existing_clients();
    deep_pipeline_preserves_every_response();
    large_request_and_response_round_trip();
    flush_all_clears_storage();
    invalid_input_closes_only_that_connection();
}

fn connected_client() -> TcpStream {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let stream = loop {
        match TcpStream::connect(data_addr()) {
            Ok(stream) => break stream,
            Err(_) if std::time::Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("failed to connect: {error}"),
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
}

fn exchange(stream: &mut TcpStream, request: &[u8], expected: &[u8]) {
    stream.write_all(request).expect("request write failed");
    let mut response = vec![0; expected.len()];
    stream
        .read_exact(&mut response)
        .expect("response read failed");
    assert_eq!(response, expected);
}

fn partial_request_is_completed_after_second_write() {
    let mut stream = connected_client();
    stream.write_all(b"get ").unwrap();
    std::thread::sleep(Duration::from_millis(10));
    exchange(&mut stream, b"missing\r\n", b"$-1\r\n");
}

fn pipelined_requests_preserve_response_order() {
    let mut stream = connected_client();
    exchange(
        &mut stream,
        b"set first one\r\nset second two\r\nget first\r\nget second\r\n",
        b"+OK\r\n+OK\r\n$3\r\none\r\n$3\r\ntwo\r\n",
    );
}

fn partial_request_disconnect_cancels_mutation() {
    let mut abandoned = connected_client();
    abandoned.write_all(b"set abandoned").unwrap();
    drop(abandoned);
    let mut existing = connected_client();
    exchange(&mut existing, b"get abandoned\r\n", b"$-1\r\n");
}

fn connection_burst_does_not_block_existing_clients() {
    let mut existing = connected_client();
    let burst: Vec<_> = (0..128).map(|_| connected_client()).collect();
    exchange(&mut existing, b"get abandoned\r\n", b"$-1\r\n");
    drop(burst);
}

fn deep_pipeline_preserves_every_response() {
    let mut request = Vec::new();
    let mut expected = Vec::new();
    for _ in 0..256 {
        request.extend_from_slice(b"get task7-pressure\r\n");
        expected.extend_from_slice(b"$-1\r\n");
    }
    let mut stream = connected_client();
    exchange(&mut stream, &request, &expected);
}

fn large_request_and_response_round_trip() {
    let value = "x".repeat(64 * 1024);
    let request = format!("set task7-large {value}\r\nget task7-large\r\n");
    let expected = format!("+OK\r\n${}\r\n{value}\r\n", value.len());
    let mut stream = connected_client();
    exchange(&mut stream, request.as_bytes(), expected.as_bytes());
}

fn flush_all_clears_storage() {
    let mut data = connected_client();
    exchange(&mut data, b"set task7-flush value\r\n", b"+OK\r\n");
    let mut admin = TcpStream::connect(admin_addr()).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    exchange(&mut admin, b"flush_all\r\n", b"OK\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let mut data = connected_client();
        data.write_all(b"get task7-flush\r\n").unwrap();
        let mut response = [0_u8; 64];
        let count = data.read(&mut response).unwrap();
        if response[..count].starts_with(b"$-1\r\n") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "FlushAll did not clear RDS storage"
        );
        std::thread::yield_now();
    }
}

fn invalid_input_closes_only_that_connection() {
    let mut invalid = connected_client();
    invalid.write_all(b"!not-resp\r\n").unwrap();
    let mut byte = [0_u8; 1];
    match invalid.read(&mut byte) {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        outcome => panic!("invalid RESP input did not close its connection: {outcome:?}"),
    }
    let mut valid = connected_client();

    exchange(&mut valid, b"get task7-after-invalid\r\n", b"$-1\r\n");
}

pub fn assert_admin_backend_info(requested: &str, active: &str, fallback: &str) {
    let mut stream = TcpStream::connect(admin_addr()).expect("failed to connect to admin");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream.write_all(b"stats\r\n").unwrap();
    let mut buffer = vec![0_u8; 64 * 1024];
    let count = stream.read(&mut buffer).unwrap();
    let output = String::from_utf8(buffer[..count].to_vec()).unwrap();
    assert!(
        output.contains(&format!("STAT server_io_backend_requested {requested}\r\n")),
        "missing requested backend in {output:?}"
    );
    assert!(
        output.contains(&format!("STAT server_io_backend_active_name {active}\r\n")),
        "missing active backend in {output:?}"
    );
    assert!(
        output.contains(&format!(
            "STAT server_io_backend_fallback_cause {fallback}\r\n"
        )),
        "missing fallback cause in {output:?}"
    );
}

pub fn admin_tests() {
    debug!("beginning admin tests");
    println!();

    admin_test(
        "version",
        &[(
            "version\r\n",
            Some(&format!("VERSION {}\r\n", env!("CARGO_PKG_VERSION"))),
        )],
    );
}

// opens a new connection to the admin port, sends a request, and checks the response.
fn admin_test(name: &str, data: &[(&str, Option<&str>)]) {
    info!("testing: {name}");
    debug!("connecting to server");
    let mut stream = TcpStream::connect(admin_addr()).expect("failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("failed to set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .expect("failed to set write timeout");

    debug!("sending request");
    for (request, response) in data {
        match stream.write(request.as_bytes()) {
            Ok(bytes) => {
                if bytes == request.len() {
                    debug!("full request sent");
                } else {
                    error!("incomplete write");
                    panic!("status: failed\n");
                }
            }
            Err(_) => {
                error!("error sending request");
                panic!("status: failed\n");
            }
        }

        std::thread::sleep(Duration::from_millis(10));
        let mut buf = vec![0; 4096];

        if let Some(response) = response {
            if stream.read(&mut buf).is_err() {
                std::thread::sleep(Duration::from_millis(500));
                panic!("error reading response");
            } else if response.as_bytes() != &buf[0..response.len()] {
                error!("expected: {:?}", response.as_bytes());
                error!("received: {:?}", &buf[0..response.len()]);
                std::thread::sleep(Duration::from_millis(500));
                panic!("status: failed\n");
            } else {
                debug!("correct response");
            }
            assert_eq!(response.as_bytes(), &buf[0..response.len()]);
        } else if let Err(e) = stream.read(&mut buf) {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                debug!("got no response");
            } else {
                error!("error reading response");
                std::thread::sleep(Duration::from_millis(500));
                panic!("status: failed\n");
            }
        } else {
            error!("expected no response");
            std::thread::sleep(Duration::from_millis(500));
            panic!("status: failed\n");
        }

        if data.len() > 1 {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    info!("status: passed\n");
}
const RESP_NIL: &str = "$-1\r\n";
const RESP_OK: &str = "+OK\r\n";

fn bulk_string(str: &str) -> String {
    let length = str.len();
    format!("${length}\r\n{str}\r\n")
}
