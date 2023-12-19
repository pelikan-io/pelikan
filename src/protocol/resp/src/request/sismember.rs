// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};

#[metric(name = "sismember")]
pub static SISMEMBER: Counter = Counter::new();

#[metric(name = "sismember_ex")]
pub static SISMEMBER_EX: Counter = Counter::new();

#[metric(name = "sismember_hit")]
pub static SISMEMBER_HIT: Counter = Counter::new();

#[metric(name = "sismember_miss")]
pub static SISMEMBER_MISS: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub struct SetIsMember {
    key: Arc<[u8]>,
    field: Arc<[u8]>,
}

impl TryFrom<Message> for SetIsMember {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Error> {
        let mut array = match value {
            Message::Array(array) => array.inner.unwrap(),
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        if array.len() != 3 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array)?;

        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let field = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        Ok(Self { key, field })
    }
}

impl SetIsMember {
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

impl From<&SetIsMember> for Message {
    fn from(other: &SetIsMember) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"SISMEMBER")),
                Message::BulkString(BulkString::new(other.key())),
                Message::BulkString(BulkString::new(other.field())),
            ]),
        })
    }
}

impl Compose for SetIsMember {
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
            parser.parse(b"sismember 0 1\r\n").unwrap().into_inner(),
            Request::SetIsMember(SetIsMember::new(b"0", b"1"))
        );

        assert_eq!(
            parser
                .parse(b"*3\r\n$9\r\nsismember\r\n$1\r\n0\r\n$1\r\n1\r\n")
                .unwrap()
                .into_inner(),
            Request::SetIsMember(SetIsMember::new(b"0", b"1"))
        );
    }
}
