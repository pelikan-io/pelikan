// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

counter!(HGET);
counter!(HGET_EX);
counter!(HGET_HIT);
counter!(HGET_MISS);

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::redundant_allocation)]
pub struct HashGetRequest {
    key: ArcByteSlice,
    field: ArcByteSlice,
}

impl TryFrom<Message> for HashGetRequest {
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

impl HashGetRequest {
    pub fn new(key: &[u8], field: &[u8]) -> Self {
        Self {
            key: Arc::new(key.to_owned().into_boxed_slice()),
            field: Arc::new(field.to_owned().into_boxed_slice()),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn field(&self) -> &[u8] {
        &self.field
    }
}

impl From<&HashGetRequest> for Message {
    fn from(other: &HashGetRequest) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"HGET")),
                Message::BulkString(BulkString::from(other.key.clone())),
                Message::BulkString(BulkString::from(other.field.clone())),
            ]),
        })
    }
}

impl Compose for HashGetRequest {
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
            parser.parse(b"hget 0 1\r\n").unwrap().into_inner(),
            Request::HashGet(HashGetRequest::new(b"0", b"1"))
        );

        assert_eq!(
            parser
                .parse(b"*3\r\n$4\r\nhget\r\n$1\r\n0\r\n$1\r\n1\r\n")
                .unwrap()
                .into_inner(),
            Request::HashGet(HashGetRequest::new(b"0", b"1"))
        );
    }
}
