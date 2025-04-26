// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A collection of protocol implementations which implement a set of common
//! traits so that the a server implementation can easily switch between
//! protocol implementations.

// TODO(bmartin): this crate should probably be split into one crate per
// protocol to help separate the metrics namespaces.

#[macro_use]
extern crate logger;

use crate::wire::parse::parse_keyword;
use crate::wire::parse::parse_ping;
use crate::wire::Keyword;
pub use protocol_common::*;

mod ping;

pub use ping::*;

#[derive(Default, Clone)]
pub struct PingProtocol {
    _unusued: (),
}

impl Protocol<Request, Response> for PingProtocol {
    fn parse_request(
        &self,
        buffer: &[u8],
    ) -> std::result::Result<protocol_common::ParseOk<Request>, std::io::Error> {
        match parse_keyword(buffer)? {
            Keyword::Ping => parse_ping(buffer),
        }
    }

    fn compose_request(
        &self,
        _: &Request,
        _: &mut dyn protocol_common::BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        todo!()
    }
    fn parse_response(
        &self,
        _: &Request,
        _: &[u8],
    ) -> std::result::Result<protocol_common::ParseOk<Response>, std::io::Error> {
        todo!()
    }
    fn compose_response(
        &self,
        _: &Request,
        _: &Response,
        _: &mut dyn protocol_common::BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        todo!()
    }
}

#[cfg(feature = "stats")]
use stats::*;

#[cfg(feature = "stats")]
mod stats {
    use metriken::*;

    #[cfg(feature = "server")]
    #[metric(name = "ping", description = "the number of ping requests")]
    pub static PING: Counter = Counter::new();

    #[cfg(feature = "client")]
    #[metric(name = "pong", description = "the number of pong responses")]
    pub static PONG: Counter = Counter::new();
}

common::metrics::test_no_duplicates!();
