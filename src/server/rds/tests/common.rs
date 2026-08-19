// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This module provides a set of integration tests and a function to run the
//! tests against an Rds instance. This allows us to run the same test suite
//! for multiple server configurations.

use logger::*;

use std::io::{Read, Write};
use std::net::TcpStream;
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
    let mut stream = TcpStream::connect("127.0.0.1:12321").expect("failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .expect("failed to set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_millis(250)))
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

pub fn smoke_exchange() {
    let mut stream = connected_client();
    exchange(&mut stream, b"get task7-smoke\r\n", b"$-1\r\n");
}

pub fn conformance_tests() {
    partial_request_is_completed_after_second_write();
    pipelined_requests_preserve_response_order();
    client_disconnect_does_not_poison_runtime();
    connection_burst_does_not_block_existing_clients();
}

fn connected_client() -> TcpStream {
    let stream = TcpStream::connect("127.0.0.1:12321").expect("failed to connect");
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

fn client_disconnect_does_not_poison_runtime() {
    let mut abandoned = connected_client();
    abandoned.write_all(b"set abandoned").unwrap();
    drop(abandoned);
    let mut existing = connected_client();
    exchange(&mut existing, b"get missing\r\n", b"$-1\r\n");
}

fn connection_burst_does_not_block_existing_clients() {
    let mut existing = connected_client();
    let burst: Vec<_> = (0..128).map(|_| connected_client()).collect();
    exchange(&mut existing, b"get missing\r\n", b"$-1\r\n");
    drop(burst);
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
    let mut stream = TcpStream::connect("127.0.0.1:9999").expect("failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .expect("failed to set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_millis(250)))
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
