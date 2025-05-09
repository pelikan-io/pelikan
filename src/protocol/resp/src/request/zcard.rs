// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "zcard")]
pub static ZCARD: Counter = Counter::new();

#[metric(name = "zcard_ex")]
pub static ZCARD_EX: Counter = Counter::new();

#[metric(name = "zcard_hit")]
pub static ZCARD_HIT: Counter = Counter::new();

#[metric(name = "zcard_miss")]
pub static ZCARD_MISS: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub struct SortedSetCardinality {
    key: Arc<[u8]>,
}

impl TryFrom<Message> for SortedSetCardinality {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        if let Message::Array(array) = other {
            if array.inner.is_none() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let mut array = array.inner.unwrap();

            if array.len() != 2 {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let _command = take_bulk_string(&mut array)?;
            let key = take_bulk_string(&mut array)?
                .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

            Ok(Self { key })
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl SortedSetCardinality {
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.into() }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

impl From<&SortedSetCardinality> for Message {
    fn from(value: &SortedSetCardinality) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"ZCARD")),
                Message::BulkString(BulkString::new(value.key())),
            ]),
        })
    }
}

impl Compose for SortedSetCardinality {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        Message::from(self).compose(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser() {
        let parser = RequestParser::new();
        assert_eq!(
            parser.parse(b"ZCARD 0\r\n").unwrap().into_inner(),
            Request::SortedSetCardinality(SortedSetCardinality::new(b"0"))
        );

        assert_eq!(
            parser
                .parse(b"ZCARD \"\0\r\n key\"\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetCardinality(SortedSetCardinality::new(b"\0\r\n key"))
        );

        assert_eq!(
            parser
                .parse(b"*2\r\n$5\r\nZCARD\r\n$1\r\n0\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetCardinality(SortedSetCardinality::new(b"0"))
        );
    }
}
