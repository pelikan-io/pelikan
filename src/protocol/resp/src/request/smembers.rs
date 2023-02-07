// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

counter!(SMEMBERS);
counter!(SMEMBERS_EX);

#[derive(Debug, PartialEq, Eq)]
pub struct SetMembers {
    key: Arc<[u8]>,
}

impl TryFrom<Message> for SetMembers {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Error> {
        let array = match value {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        let mut array = array.inner.unwrap();
        if array.len() < 2 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array)?;
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        Ok(Self { key })
    }
}

impl SetMembers {
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.into() }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

impl From<&SetMembers> for Message {
    fn from(value: &SetMembers) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"GET")),
                Message::BulkString(BulkString::new(value.key())),
            ]),
        })
    }
}

impl Compose for SetMembers {
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
            parser.parse(b"smembers 0\r\n").unwrap().into_inner(),
            Request::SetMembers(SetMembers::new(b"0"))
        );

        assert_eq!(
            parser
                .parse(b"smembers \"\0\r\n key\"\r\n")
                .unwrap()
                .into_inner(),
            Request::SetMembers(SetMembers::new(b"\0\r\n key"))
        );

        assert_eq!(
            parser
                .parse(b"*2\r\n$8\r\nsmembers\r\n$1\r\n0\r\n")
                .unwrap()
                .into_inner(),
            Request::SetMembers(SetMembers::new(b"0"))
        );
    }
}
