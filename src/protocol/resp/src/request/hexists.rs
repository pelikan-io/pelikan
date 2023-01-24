// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};

counter!(HEXISTS);
counter!(HEXISTS_EX);
counter!(HEXISTS_HIT);
counter!(HEXISTS_MISS);

#[derive(Debug, PartialEq, Eq)]
pub struct HashExistsRequest {
    key: Arc<[u8]>,
    field: Arc<[u8]>,
}

impl TryFrom<Message> for HashExistsRequest {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        if let Message::Array(array) = other {
            if array.inner.is_none() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let mut array = array.inner.unwrap();

            if array.len() != 3 {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let _command = take_bulk_string(&mut array)?;

            let key = take_bulk_string(&mut array)?
                .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

            if key.is_empty() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let field = take_bulk_string(&mut array)?
                .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

            if field.is_empty() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            Ok(Self { key, field })
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl HashExistsRequest {
    pub fn new(key: &[u8], field: &[u8]) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn field(&self) -> &[u8] {
        &self.field
    }
}

impl From<&HashExistsRequest> for Message {
    fn from(other: &HashExistsRequest) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"HEXISTS")),
                Message::BulkString(BulkString::from(other.key.clone())),
                Message::BulkString(BulkString::from(other.field.clone())),
            ]),
        })
    }
}

impl Compose for HashExistsRequest {
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
            parser.parse(b"hexists 0 1\r\n").unwrap().into_inner(),
            Request::HashExists(HashExistsRequest::new(b"0", b"1"))
        );

        assert_eq!(
            parser
                .parse(b"*3\r\n$7\r\nhexists\r\n$1\r\n0\r\n$1\r\n1\r\n")
                .unwrap()
                .into_inner(),
            Request::HashExists(HashExistsRequest::new(b"0", b"1"))
        );
    }
}
