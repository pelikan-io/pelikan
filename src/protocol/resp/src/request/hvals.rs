// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};

#[metric(name = "hvals")]
pub static HVALS: Counter = Counter::new();

#[metric(name = "hvals_ex")]
pub static HVALS_EX: Counter = Counter::new();

#[metric(name = "hvals_hit")]
pub static HVALS_HIT: Counter = Counter::new();

#[metric(name = "hvals_miss")]
pub static HVALS_MISS: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub struct HashValues {
    key: Arc<[u8]>,
}

impl TryFrom<Message> for HashValues {
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

impl HashValues {
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.into() }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

impl From<&HashValues> for Message {
    fn from(other: &HashValues) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"HVALS")),
                Message::BulkString(BulkString::from(other.key.clone())),
            ]),
        })
    }
}

impl Compose for HashValues {
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
            Request::HashValues(HashValues::new(b"0"))
        );

        assert_eq!(
            parser
                .parse(b"*2\r\n$5\r\nhvals\r\n$1\r\n0\r\n")
                .unwrap()
                .into_inner(),
            Request::HashValues(HashValues::new(b"0"))
        );
    }
}
