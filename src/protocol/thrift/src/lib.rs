// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A protocol crate for Thrift binary protocol.

use metriken::*;
use protocol_common::BufMut;
use protocol_common::Compose;
use protocol_common::ParseOk;

const THRIFT_HEADER_LEN: usize = std::mem::size_of::<u32>();

// Stats
#[metric(name = "messages_parsed")]
pub static MESSAGES_PARSED: Counter = Counter::new();

#[metric(name = "messages_composed")]
pub static MESSAGES_COMPOSED: Counter = Counter::new();

#[derive(Default, Clone)]
pub struct Protocol {
    max_size: usize,
}

impl Protocol {
    pub fn new(max_size: usize) -> Self {
        Self { max_size }
    }
}

impl protocol_common::Protocol<Message, Message> for Protocol {
    fn parse_request(
        &self,
        buffer: &[u8],
    ) -> std::result::Result<ParseOk<Message>, std::io::Error> {
        if buffer.len() < THRIFT_HEADER_LEN {
            return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
        }

        let data_len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

        let framed_len = THRIFT_HEADER_LEN + data_len as usize;

        if framed_len == 0 || framed_len > self.max_size {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
        }

        if buffer.len() < framed_len {
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        } else {
            MESSAGES_PARSED.increment();
            let data = buffer[THRIFT_HEADER_LEN..framed_len]
                .to_vec()
                .into_boxed_slice();
            let message = Message { data };
            Ok(ParseOk::new(message, framed_len))
        }
    }

    fn compose_request(
        &self,
        request: &Message,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        Ok(request.compose(buffer))
    }

    fn parse_response(
        &self,
        _: &Message,
        buffer: &[u8],
    ) -> std::result::Result<protocol_common::ParseOk<Message>, std::io::Error> {
        if buffer.len() < THRIFT_HEADER_LEN {
            return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
        }

        let data_len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

        let framed_len = THRIFT_HEADER_LEN + data_len as usize;

        if framed_len == 0 || framed_len > self.max_size {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
        }

        if buffer.len() < framed_len {
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        } else {
            MESSAGES_PARSED.increment();
            let data = buffer[THRIFT_HEADER_LEN..framed_len]
                .to_vec()
                .into_boxed_slice();
            let message = Message { data };
            Ok(ParseOk::new(message, framed_len))
        }
    }

    fn compose_response(
        &self,
        _: &Message,
        response: &Message,
        buffer: &mut dyn protocol_common::BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        Ok(response.compose(buffer))
    }
}

/// An opaque Thrift message
pub struct Message {
    data: Box<[u8]>,
}

#[allow(clippy::len_without_is_empty)]
impl Message {
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl Compose for Message {
    fn compose(&self, session: &mut dyn BufMut) -> usize {
        MESSAGES_COMPOSED.increment();
        session.put_slice(&(self.data.len() as u32).to_be_bytes());
        session.put_slice(&self.data);
        std::mem::size_of::<u32>() + self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol_common::Protocol as ProtocolTrait;

    #[test]
    fn parse() {
        let body = b"COFFEE".to_vec();
        let len = (body.len() as u32).to_be_bytes();

        let mut message: Vec<u8> = len.to_vec();
        message.extend_from_slice(&body);

        let protocol = Protocol::new(1024);

        let parsed = protocol.parse_request(&message).expect("failed to parse");
        let consumed = parsed.consumed();
        let parsed = parsed.into_inner();

        assert_eq!(consumed, body.len() + THRIFT_HEADER_LEN);
        assert_eq!(*parsed.data, body);
    }
}

common::metrics::test_no_duplicates!();
