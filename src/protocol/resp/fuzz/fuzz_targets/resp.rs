// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A fuzz target which makes sure that the `RESP` protocol implementation will
//! handle arbitrary data without panicking.

#![no_main]
use libfuzzer_sys::fuzz_target;

use protocol_resp::*;

// TODO(bmartin): we should be able to do some validation like we do in the
// memcache protocol fuzzing. For now, this just makes sure the parser will
// not panic on unanticipated inputs.

fuzz_target!(|data: &[u8]| {
    let protocol = Protocol::default();

    let _ = protocol.parse_request(data);
});
