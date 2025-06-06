// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "zincrby")]
pub static ZINCRBY: Counter = Counter::new();

#[metric(name = "zincrby_ex")]
pub static ZINCRBY_EX: Counter = Counter::new();

#[derive(Debug, PartialEq)]
pub struct SortedSetIncrement {
    key: Arc<[u8]>,
    increment: f64,
    member: Arc<[u8]>,
}

impl TryFrom<Message> for SortedSetIncrement {
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
        let increment_string = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let member = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed content"))?;

        // Parse the increment string into a float
        let increment = parse_sorted_set_score(&increment_string)?;

        Ok(Self {
            key,
            increment,
            member,
        })
    }
}

impl SortedSetIncrement {
    pub fn new(key: &[u8], increment: f64, member: &[u8]) -> Self {
        Self {
            key: key.into(),
            increment,
            member: member.into(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn increment(&self) -> f64 {
        self.increment
    }

    pub fn member(&self) -> &[u8] {
        &self.member
    }
}

impl From<&SortedSetIncrement> for Message {
    fn from(value: &SortedSetIncrement) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"ZINCRBY")),
                Message::BulkString(BulkString::new(value.key())),
                Message::BulkString(BulkString::new(value.increment().to_string().as_bytes())),
                Message::BulkString(BulkString::new(value.member())),
            ]),
        })
    }
}

impl Compose for SortedSetIncrement {
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
            parser.parse(b"ZINCRBY z 1 a\r\n").unwrap().into_inner(),
            Request::SortedSetIncrement(SortedSetIncrement::new(b"z", 1.0, b"a"))
        );

        assert_eq!(
            parser.parse(b"ZINCRBY z +inf a\r\n").unwrap().into_inner(),
            Request::SortedSetIncrement(SortedSetIncrement::new(b"z", f64::INFINITY, b"a"))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$7\r\nZINCRBY\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\na\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetIncrement(SortedSetIncrement::new(b"z", 1.0, b"a"))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$7\r\nZINCRBY\r\n$1\r\nz\r\n$4\r\n-inf\r\n$1\r\na\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetIncrement(SortedSetIncrement::new(b"z", f64::NEG_INFINITY, b"a"))
        );
    }
}
