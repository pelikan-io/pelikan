// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This module provides a set of integration tests and a function to run the
//! tests against a Segcache instance. This allows us to run the same test suite
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
    test("get miss", &[("get 0\r\n", Some("END\r\n"))]);
    test("gets miss", &[("gets 0\r\n", Some("END\r\n"))]);

    // check that we can store and retrieve a key
    test(
        "set and get",
        &[
            // store the key
            ("set 1 0 0 1\r\n1\r\n", Some("STORED\r\n")),
            // retrieve the key
            ("get 1\r\n", Some("VALUE 1 0 1\r\n1\r\nEND\r\n")),
        ],
    );

    test(
        "cas not_found",
        &[
            // try to cas on key that is not in the cache
            ("cas 2 0 0 1 0\r\n0\r\n", Some("NOT_FOUND\r\n")),
            // confirm that the key is still not in the cache
            ("get 2\r\n", Some("END\r\n")),
        ],
    );

    test(
        "cas exists",
        &[
            // store the key
            ("set 3 0 0 1\r\n3\r\n", Some("STORED\r\n")),
            // try to cas with a bad cas value
            ("cas 3 0 0 1 0\r\n0\r\n", Some("EXISTS\r\n")),
            // check that it was not updated
            ("get 3\r\n", Some("VALUE 3 0 1\r\n3\r\nEND\r\n")),
        ],
    );

    // the cas token is opaque to the client and must be retrieved with a
    // `gets` before issuing the `cas` command
    test_cas_stored();

    test(
        "add not_stored",
        &[
            // store the key
            ("set 5 0 0 1\r\n5\r\n", Some("STORED\r\n")),
            // try to add a key that exists
            ("add 5 0 0 1\r\n0\r\n", Some("NOT_STORED\r\n")),
            // check that the value was not updated
            ("get 5\r\n", Some("VALUE 5 0 1\r\n5\r\nEND\r\n")),
        ],
    );

    test(
        "add stored",
        &[
            // try to add a new key
            ("add 6 0 0 1\r\n6\r\n", Some("STORED\r\n")),
            // check that the key exists now
            ("get 6\r\n", Some("VALUE 6 0 1\r\n6\r\nEND\r\n")),
        ],
    );

    test(
        "replace not_stored",
        &[
            // try to replace a key that does not exist
            ("replace 7 0 0 1\r\n7\r\n", Some("NOT_STORED\r\n")),
            // check that the value was not stored
            ("get 7\r\n", Some("END\r\n")),
        ],
    );

    test(
        "replace stored",
        &[
            // store the key
            ("set 8 0 0 1\r\n8\r\n", Some("STORED\r\n")),
            // replace a key that does exist
            ("replace 8 0 0 1\r\n0\r\n", Some("STORED\r\n")),
            // check that the value was updated
            ("get 8\r\n", Some("VALUE 8 0 1\r\n0\r\nEND\r\n")),
        ],
    );

    test(
        "set flags",
        &[
            // store the key
            ("set 9 42 0 1\r\n1\r\n", Some("STORED\r\n")),
            // retrieve with correct flags
            ("get 9\r\n", Some("VALUE 9 42 1\r\n1\r\nEND\r\n")),
        ],
    );

    // test pipelined commands
    test(
        "pipelined get (key: 4 depth: 2)",
        &[("get 10\r\nget 10\r\n", Some("END\r\nEND\r\n"))],
    );
    test(
        "pipelined get and invalid (key 4, depth 2)",
        &[("get 11\r\n ", Some("END\r\n"))],
    );
    test(
        "pipelined get and add (key 4, depth 2)",
        &[(
            "get 12 \r\nadd 12 0 0 1\r\n1\r\n",
            Some("END\r\nSTORED\r\n"),
        )],
    );
    test(
        "pipelined get and set (key 5, depth 2)",
        &[(
            "get 13 \r\nset 13 0 0 1 \r\n1\r\n",
            Some("END\r\nSTORED\r\n"),
        )],
    );
    test(
        "pipelined set and get (key 6, depth 3)",
        &[(
            "set 14 0 0 2 \r\nhi\r\nset 14 0 0 6\r\nhello!\r\nget 14 \r\n",
            Some("STORED\r\nSTORED\r\nVALUE 14 0 6\r\nhello!\r\nEND\r\n"),
        )],
    );

    // test increment
    test(
        "incr not_found",
        &[("incr 15 1\r\n", Some("NOT_FOUND\r\n"))],
    );
    test(
        "incr stored",
        &[
            // set the key
            ("set 15 0 0 1\r\n0\r\n", Some("STORED\r\n")),
            // increment it
            ("incr 15 1\r\n", Some("1\r\n")),
            // increment it again
            ("incr 15 2\r\n", Some("3\r\n")),
        ],
    );
    test(
        "incr error",
        &[
            // set the key
            ("set 16 0 0 1\r\na\r\n", Some("STORED\r\n")),
            // increment non-numeric value is an error
            ("incr 16 1\r\n", Some("ERROR\r\n")),
        ],
    );

    // test decrement
    test(
        "decr not_found",
        &[("decr 17 1\r\n", Some("NOT_FOUND\r\n"))],
    );
    test(
        "decr stored",
        &[
            // set the key
            ("set 18 0 0 2\r\n10\r\n", Some("STORED\r\n")),
            // decrement it
            ("decr 18 1\r\n", Some("9\r\n")),
            // decrement it again
            ("decr 18 2\r\n", Some("7\r\n")),
            // decrement it again, saturates at zero
            ("decr 18 255\r\n", Some("0\r\n")),
        ],
    );

    // test unsupported commands
    test("append", &[("append 7 0 0 1\r\n0\r\n", Some("ERROR\r\n"))]);
    test(
        "prepend",
        &[("prepend 8 0 0 1\r\n0\r\n", Some("ERROR\r\n"))],
    );

    // version command on the data listener
    test(
        "version",
        &[(
            "version\r\n",
            Some(&format!("VERSION {}\r\n", env!("CARGO_PKG_VERSION"))),
        )],
    );

    std::thread::sleep(Duration::from_millis(500));
}

// stores a key, retrieves its cas token with `gets`, and checks that a `cas`
// with that token succeeds and updates the value
fn test_cas_stored() {
    info!("testing: cas stored");
    debug!("connecting to server");
    let mut stream = TcpStream::connect(data_addr()).expect("failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .expect("failed to set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_millis(250)))
        .expect("failed to set write timeout");

    let exchange = |stream: &mut TcpStream, request: &str| -> String {
        debug!("sending request");
        if !matches!(stream.write(request.as_bytes()), Ok(n) if n == request.len()) {
            error!("error sending request");
            panic!("status: failed\n");
        }
        std::thread::sleep(Duration::from_millis(10));
        let mut buf = vec![0; 4096];
        let bytes = stream.read(&mut buf).unwrap_or_else(|_| {
            std::thread::sleep(Duration::from_millis(500));
            panic!("error reading response");
        });
        String::from_utf8_lossy(&buf[0..bytes]).into_owned()
    };

    // store the key
    let response = exchange(&mut stream, "set 4 0 0 1\r\n4\r\n");
    if response != "STORED\r\n" {
        error!("expected: {:?}", "STORED\r\n");
        error!("received: {response:?}");
        std::thread::sleep(Duration::from_millis(500));
        panic!("status: failed\n");
    }

    // retrieve the cas token with gets: `VALUE <key> <flags> <bytes> <cas>`
    let response = exchange(&mut stream, "gets 4\r\n");
    let cas = response
        .strip_prefix("VALUE 4 0 1 ")
        .and_then(|r| r.split_whitespace().next())
        .unwrap_or_else(|| {
            error!("expected gets response with cas token");
            error!("received: {response:?}");
            std::thread::sleep(Duration::from_millis(500));
            panic!("status: failed\n");
        })
        .to_owned();

    // cas with the correct cas value
    let response = exchange(&mut stream, &format!("cas 4 0 0 1 {cas}\r\n0\r\n"));
    if response != "STORED\r\n" {
        error!("expected: {:?}", "STORED\r\n");
        error!("received: {response:?}");
        std::thread::sleep(Duration::from_millis(500));
        panic!("status: failed\n");
    }

    // check that the value was updated
    let response = exchange(&mut stream, "get 4\r\n");
    if response != "VALUE 4 0 1\r\n0\r\nEND\r\n" {
        error!("expected: {:?}", "VALUE 4 0 1\r\n0\r\nEND\r\n");
        error!("received: {response:?}");
        std::thread::sleep(Duration::from_millis(500));
        panic!("status: failed\n");
    }

    info!("status: passed\n");
}

// opens a new connection, operating on request + response pairs from the
// provided data.
fn test(name: &str, data: &[(&str, Option<&str>)]) {
    info!("testing: {name}");
    debug!("connecting to server");
    let mut stream = TcpStream::connect(data_addr()).expect("failed to connect");
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

pub fn smoke_exchange() {
    let mut stream = connected_client();
    exchange(&mut stream, b"get task7-smoke\r\n", b"END\r\n");
}

pub fn conformance_tests() {
    partial_request_is_completed_after_second_write();
    pipelined_requests_preserve_response_order();
    partial_request_disconnect_cancels_mutation();
    connection_burst_does_not_block_existing_clients();
    deep_pipeline_preserves_every_response();
    large_request_and_response_round_trip();
    flush_all_clears_storage();
}

fn connected_client() -> TcpStream {
    let stream = TcpStream::connect(data_addr()).expect("failed to connect");
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
    exchange(&mut stream, b"missing\r\n", b"END\r\n");
}

fn pipelined_requests_preserve_response_order() {
    let mut stream = connected_client();
    exchange(
        &mut stream,
        b"set first 0 0 3\r\none\r\nset second 0 0 3\r\ntwo\r\nget first\r\nget second\r\n",
        b"STORED\r\nSTORED\r\nVALUE first 0 3\r\none\r\nEND\r\nVALUE second 0 3\r\ntwo\r\nEND\r\n",
    );
}

fn partial_request_disconnect_cancels_mutation() {
    let mut abandoned = connected_client();
    abandoned.write_all(b"set abandoned 0 0 4\r\npar").unwrap();
    drop(abandoned);
    let mut existing = connected_client();
    exchange(&mut existing, b"get abandoned\r\n", b"END\r\n");
}

fn connection_burst_does_not_block_existing_clients() {
    let mut existing = connected_client();
    let burst: Vec<_> = (0..128).map(|_| connected_client()).collect();
    exchange(&mut existing, b"get missing\r\n", b"END\r\n");
    drop(burst);
}

fn deep_pipeline_preserves_every_response() {
    let mut request = Vec::new();
    let mut expected = Vec::new();
    for _ in 0..256 {
        request.extend_from_slice(b"get task7-pressure\r\n");
        expected.extend_from_slice(b"END\r\n");
    }
    let mut stream = connected_client();
    exchange(&mut stream, &request, &expected);
}

fn large_request_and_response_round_trip() {
    let value = "x".repeat(64 * 1024);
    let request = format!(
        "set task7-large 0 0 {}\r\n{value}\r\nget task7-large\r\n",
        value.len()
    );
    let expected = format!(
        "STORED\r\nVALUE task7-large 0 {}\r\n{value}\r\nEND\r\n",
        value.len()
    );
    let mut stream = connected_client();
    exchange(&mut stream, request.as_bytes(), expected.as_bytes());
}

fn flush_all_clears_storage() {
    let mut data = connected_client();
    exchange(
        &mut data,
        b"set task7-flush 0 0 5\r\nvalue\r\n",
        b"STORED\r\n",
    );
    let mut admin = TcpStream::connect(admin_addr()).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    exchange(&mut admin, b"flush_all\r\n", b"OK\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let mut data = connected_client();
        data.write_all(b"get task7-flush\r\n").unwrap();
        let mut response = [0_u8; 128];
        let count = data.read(&mut response).unwrap();
        if response[..count] == *b"END\r\n" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "FlushAll did not clear Segcache storage"
        );
        std::thread::yield_now();
    }
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
