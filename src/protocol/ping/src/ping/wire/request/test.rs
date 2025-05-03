// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Tests for the `Ping` protocol implementation.

use crate::*;
use std::io::ErrorKind;

#[test]
fn ping() {
    let protocol = PingProtocol::default();

    assert!(protocol.parse_request(b"ping\r\n").is_ok());
    assert!(protocol.parse_request(b"PING\r\n").is_ok());
}

#[test]
fn incomplete() {
    let protocol = PingProtocol::default();

    if let Err(e) = protocol.parse_request(b"ping") {
        if e.kind() != ErrorKind::WouldBlock {
            panic!("invalid parse result");
        }
    } else {
        panic!("invalid parse result");
    }
}

#[test]
fn trailing_whitespace() {
    let protocol = PingProtocol::default();

    assert!(protocol.parse_request(b"ping \r\n").is_ok())
}

#[test]
fn unknown() {
    let protocol = PingProtocol::default();

    for request in &["unknown\r\n"] {
        if let Err(e) = protocol.parse_request(request.as_bytes()) {
            if e.kind() != ErrorKind::InvalidInput {
                panic!("invalid parse result");
            }
        } else {
            panic!("invalid parse result");
        }
    }
}

#[test]
fn pipelined() {
    let protocol = PingProtocol::default();

    assert!(protocol.parse_request(b"ping\r\nping\r\n").is_ok());
}
