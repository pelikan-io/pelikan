// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

counter!(HINCRBY);
counter!(HINCRBY_EX);
counter!(HINCRBY_HIT);
counter!(HINCRBY_MISS);

#[derive(Debug, PartialEq, Eq)]
pub struct HashIncrBy {
    key: Arc<[u8]>,
    field: Arc<[u8]>,
    increment: i64,
}

impl TryFrom<Message> for HashIncrBy {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        let array = match other {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        let mut array = array.inner.unwrap();
        if array.len() != 4 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array)?;
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let field = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed content"))?;
        let increment = take_bulk_string_as_i64(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        Ok(Self {
            key,
            field,
            increment,
        })
    }
}

impl HashIncrBy {
    pub fn new(key: &[u8], field: &[u8], increment: i64) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            increment,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn field(&self) -> &[u8] {
        &self.field
    }

    pub fn increment(&self) -> i64 {
        self.increment
    }
}

impl From<&HashIncrBy> for Message {
    fn from(value: &HashIncrBy) -> Self {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"HINCRBY")),
                Message::BulkString(BulkString::new(value.key())),
                Message::BulkString(BulkString::new(value.field())),
                Message::BulkString(BulkString::new(value.increment().to_string().as_bytes())),
            ]),
        })
    }
}

impl Compose for HashIncrBy {
    fn compose(&self, dst: &mut dyn BufMut) -> usize {
        Message::from(self).compose(dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser() {
        let parser = RequestParser::new();
        assert_eq!(
            parser.parse(b"hincrby a b 10\r\n").unwrap().into_inner(),
            Request::HashIncrBy(HashIncrBy::new(b"a", b"b", 10))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$7\r\nhincrby\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\n10\r\n")
                .unwrap()
                .into_inner(),
            Request::HashIncrBy(HashIncrBy::new(b"a", b"b", 10))
        );
    }
}
