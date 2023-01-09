// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

counter!(HKEYS);
counter!(HKEYS_EX);
counter!(HKEYS_FOUND);
counter!(HKEYS_NOT_FOUND);

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::redundant_allocation)]
pub struct HashValuesRequest {
    key: ArcByteSlice,
}

impl TryFrom<Message> for HashValuesRequest {
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
                .ok_or(Error::new(ErrorKind::Other, "malformed command"))?;

            if key.is_empty() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            Ok(Self { key })
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl HashValuesRequest {
    pub fn new(key: &[u8]) -> Self {
        Self {
            key: Arc::new(key.to_owned().into_boxed_slice()),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

impl From<&HashValuesRequest> for Message {
    fn from(other: &HashValuesRequest) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"HVALS")),
                Message::BulkString(BulkString::from(other.key.clone())),
            ]),
        })
    }
}

impl Compose for HashValuesRequest {
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
            parser.parse(b"hvals 0\r\n").unwrap().into_inner(),
            Request::HashValues(HashValuesRequest::new(b"0"))
        );

        assert_eq!(
            parser
                .parse(b"*2\r\n$5\r\nhvals\r\n$1\r\n0\r\n")
                .unwrap()
                .into_inner(),
            Request::HashValues(HashValuesRequest::new(b"0"))
        );
    }
}
