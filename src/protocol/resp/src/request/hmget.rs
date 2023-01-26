// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

counter!(HMGET);
counter!(HMGET_EX);
counter!(HMGET_FIELD);
counter!(HMGET_FIELD_HIT);
counter!(HMGET_FIELD_MISS);

#[derive(Debug, PartialEq, Eq)]
pub struct HashMultiGet {
    key: Arc<[u8]>,
    fields: Box<[Arc<[u8]>]>,
}

impl TryFrom<Message> for HashMultiGet {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        if let Message::Array(array) = other {
            if array.inner.is_none() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let mut array = array.inner.unwrap();

            if array.len() < 3 {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let _command = take_bulk_string(&mut array)?;

            let key = take_bulk_string(&mut array)?
                .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

            if key.is_empty() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let mut fields = Vec::with_capacity(array.len());

            while let Some(field) = take_bulk_string(&mut array)? {
                if field.is_empty() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }
                fields.push(field);
            }

            Ok(Self {
                key,
                fields: fields.into_boxed_slice(),
            })
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl HashMultiGet {
    pub fn new(key: &[u8], fields: &[&[u8]]) -> Self {
        let fields: Vec<Arc<[u8]>> = fields.iter().map(|f| (*f).into()).collect();

        Self {
            key: key.into(),
            fields: fields.into(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn fields(&self) -> &[Arc<[u8]>] {
        &self.fields
    }
}

impl From<&HashMultiGet> for Message {
    fn from(other: &HashMultiGet) -> Message {
        let mut data = vec![
            Message::BulkString(BulkString::new(b"HMGET")),
            Message::BulkString(BulkString::from(other.key.clone())),
        ];

        for field in other.fields.iter() {
            data.push(Message::BulkString(BulkString::from(field.clone())));
        }

        Message::Array(Array { inner: Some(data) })
    }
}

impl Compose for HashMultiGet {
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
            parser.parse(b"hmget 0 1 2\r\n").unwrap().into_inner(),
            Request::HashMultiGet(HashMultiGet::new(b"0", &[b"1", b"2"]))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$5\r\nhmget\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n")
                .unwrap()
                .into_inner(),
            Request::HashMultiGet(HashMultiGet::new(b"0", &[b"1", b"2"]))
        );
    }
}
