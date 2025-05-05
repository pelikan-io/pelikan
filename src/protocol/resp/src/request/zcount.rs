// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "zcount")]
pub static ZCOUNT: Counter = Counter::new();

#[metric(name = "zcount_ex")]
pub static ZCOUNT_EX: Counter = Counter::new();

#[metric(name = "zcount_hit")]
pub static ZCOUNT_HIT: Counter = Counter::new();

#[metric(name = "zcount_miss")]
pub static ZCOUNT_MISS: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub struct SortedSetCount {
    key: Arc<[u8]>,
    min_score: Arc<[u8]>,
    max_score: Arc<[u8]>,
}

impl TryFrom<Message> for SortedSetCount {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        let array = match other {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        if array.inner.is_none() {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let mut array = array.inner.unwrap();

        if array.len() != 4 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array)?;
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let min_score = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let max_score = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed content"))?;

        Ok(Self {
            key,
            min_score,
            max_score,
        })
    }
}

impl SortedSetCount {
    pub fn new(key: &[u8], min_score: &[u8], max_score: &[u8]) -> Self {
        Self {
            key: key.into(),
            min_score: min_score.into(),
            max_score: max_score.into(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn min_score(&self) -> &[u8] {
        &self.min_score
    }

    pub fn max_score(&self) -> &[u8] {
        &self.max_score
    }
}

impl From<&SortedSetCount> for Message {
    fn from(value: &SortedSetCount) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"ZCOUNT")),
                Message::BulkString(BulkString::new(value.key())),
                Message::BulkString(BulkString::new(value.min_score())),
                Message::BulkString(BulkString::new(value.max_score())),
            ]),
        })
    }
}

impl Compose for SortedSetCount {
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
            parser
                .parse(b"ZCOUNT z -inf +inf\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetCount(SortedSetCount::new(b"z", b"-inf", b"+inf"))
        );

        assert_eq!(
            parser.parse(b"ZCOUNT z (1 3\r\n").unwrap().into_inner(),
            Request::SortedSetCount(SortedSetCount::new(b"z", b"(1", b"3"))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$6\r\nZCOUNT\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\n3\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetCount(SortedSetCount::new(b"z", b"1", b"3"))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$6\r\nZCOUNT\r\n$1\r\nz\r\n$4\r\n-inf\r\n$4\r\n+inf\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetCount(SortedSetCount::new(b"z", b"-inf", b"+inf"))
        );
    }
}
