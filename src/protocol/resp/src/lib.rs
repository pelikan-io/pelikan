// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

#[macro_use]
extern crate logger;

mod message;
mod request;
mod response;
mod storage;
mod util;

pub mod parse;

pub use protocol_common::*;

pub(crate) use crate::util::*;

pub use crate::request::*;
pub use crate::response::*;
pub use crate::storage::*;

use metriken::*;

#[derive(Default, Clone)]
pub struct Protocol {
    request: RequestParser,
    response: ResponseParser,
}

impl protocol_common::Protocol<Request, Response> for Protocol {
    fn parse_request(
        &self,
        buffer: &[u8],
    ) -> std::result::Result<protocol_common::ParseOk<request::Request>, std::io::Error> {
        self.request.parse(buffer)
    }

    fn compose_request(
        &self,
        request: &request::Request,
        buffer: &mut dyn protocol_common::BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        Ok(request.compose(buffer))
    }

    fn parse_response(
        &self,
        _: &request::Request,
        buffer: &[u8],
    ) -> std::result::Result<protocol_common::ParseOk<message::Message>, std::io::Error> {
        self.response.parse(buffer)
    }

    fn compose_response(
        &self,
        _: &request::Request,
        response: &message::Message,
        buffer: &mut dyn protocol_common::BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        Ok(response.compose(buffer))
    }
}

common::metrics::test_no_duplicates!();
