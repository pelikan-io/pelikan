// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "zscore")]
pub static ZSCORE: Counter = Counter::new();

#[metric(name = "zscore_ex")]
pub static ZSCORE_EX: Counter = Counter::new();

#[metric(name = "zscore_hit")]
pub static ZSCORE_HIT: Counter = Counter::new();

#[metric(name = "zscore_miss")]
pub static ZSCORE_MISS: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub struct SortedSetScore {
    key: Arc<[u8]>,
    member: Arc<[u8]>,
}

impl TryFrom<Message> for SortedSetScore {
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

        if array.len() != 3 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array)?;
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let member = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed content"))?;

        Ok(Self { key, member })
    }
}

impl SortedSetScore {
    pub fn new(key: &[u8], member: &[u8]) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn member(&self) -> &[u8] {
        &self.member
    }
}

impl From<&SortedSetScore> for Message {
    fn from(value: &SortedSetScore) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"ZSCORE")),
                Message::BulkString(BulkString::new(value.key())),
                Message::BulkString(BulkString::new(value.member())),
            ]),
        })
    }
}

impl Compose for SortedSetScore {
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
            parser.parse(b"ZSCORE z a\r\n").unwrap().into_inner(),
            Request::SortedSetScore(SortedSetScore::new(b"z", b"a"))
        );

        assert_eq!(
            parser.parse(b"ZSCORE z a\r\n").unwrap().into_inner(),
            Request::SortedSetScore(SortedSetScore::new(b"z", b"a"))
        );

        assert_eq!(
            parser
                .parse(b"*3\r\n$6\r\nZSCORE\r\n$1\r\nz\r\n$1\r\na\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetScore(SortedSetScore::new(b"z", b"a"))
        );
    }
}
