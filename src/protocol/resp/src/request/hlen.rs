// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};

counter!(HLEN);
counter!(HLEN_EX);
counter!(HLEN_HIT);
counter!(HLEN_MISS);

#[derive(Debug, PartialEq, Eq)]
pub struct HashLengthRequest {
    key: Arc<[u8]>,
}

impl TryFrom<Message> for HashLengthRequest {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        if let Message::Array(array) = other {
            if array.inner.is_none() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let mut array = array.inner.unwrap();

            if array.len() < 2 {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let _command = take_bulk_string(&mut array)?;

            let key = take_bulk_string(&mut array)?
                .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

            if key.is_empty() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            Ok(Self { key })
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl HashLengthRequest {
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.into() }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

impl From<&HashLengthRequest> for Message {
    fn from(other: &HashLengthRequest) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"HLEN")),
                Message::BulkString(BulkString::from(other.key.clone())),
            ]),
        })
    }
}

impl Compose for HashLengthRequest {
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
            parser.parse(b"hlen 0\r\n").unwrap().into_inner(),
            Request::HashLength(HashLengthRequest::new(b"0"))
        );

        assert_eq!(
            parser
                .parse(b"*2\r\n$4\r\nhlen\r\n$1\r\n0\r\n")
                .unwrap()
                .into_inner(),
            Request::HashLength(HashLengthRequest::new(b"0"))
        );
    }
}
