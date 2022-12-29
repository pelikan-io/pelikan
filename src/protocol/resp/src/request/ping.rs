// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::redundant_allocation)]
pub struct PingRequest {}

impl TryFrom<Message> for PingRequest {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        if let Message::Array(array) = other {
            if array.inner.is_none() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }
            Ok(Self {})
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl PingRequest {
    pub fn new() -> Self {
        Self {}
    }
}

impl From<&PingRequest> for Message {
    fn from(_: &PingRequest) -> Message {
        Message::Array(Array {
            inner: Some(vec![Message::BulkString(BulkString::new(b"Ping"))]),
        })
    }
}

impl Compose for PingRequest {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        let message = Message::from(self);
        message.compose(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser() {
        let parser = RequestParser::new();
        assert_eq!(
            parser.parse(b"PING\r\n").unwrap().into_inner(),
            Request::Ping(PingRequest::new())
        );
    }
}
