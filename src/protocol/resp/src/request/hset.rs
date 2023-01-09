// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

counter!(HSET);
counter!(HSET_EX);
counter!(HSET_STORED);
counter!(HSET_NOT_STORED);

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::redundant_allocation)]
pub struct HashSetRequest {
    key: ArcByteSlice,
    data: Arc<Box<[ArcFieldValuePair]>>
}

impl TryFrom<Message> for HashSetRequest {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        if let Message::Array(array) = other {
            if array.inner.is_none() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let mut array = array.inner.unwrap();

            if array.len() < 4 {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            if array.len() % 2 == 1 {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let _command = take_bulk_string(&mut array)?;

            let key = take_bulk_string(&mut array)?
                .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

            if key.is_empty() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            // loop as long as we have at least 2 arguments after the command
            let mut data = Vec::with_capacity(array.len() / 2);

            while array.len() >= 2 {
                let field = take_bulk_string(&mut array)?
                    .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

                if field.is_empty() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }

                let value = take_bulk_string(&mut array)?
                    .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

                if value.is_empty() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }

                data.push((field, value));
            }

            Ok(Self {
                key,
                data: Arc::new(Box::<[ArcKeyValuePair]>::from(data)),
            })
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl HashSetRequest {
    pub fn new(key: &[u8], data: &[(&[u8], &[u8])]) -> Self {
        let mut d = Vec::with_capacity(data.len());
        for (field, value) in data.iter() {
            let field = Arc::new((*field).to_owned().into_boxed_slice());
            let value = Arc::new((*value).to_owned().into_boxed_slice());
            d.push((field, value));
        }

        let d = Arc::new(d.into_boxed_slice());

        Self {
            key: Arc::new(key.to_owned().into_boxed_slice()),
            data: d,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn data(&self) -> &[ArcFieldValuePair] {
        &self.data
    }
}

impl From<&HashSetRequest> for Message {
    fn from(other: &HashSetRequest) -> Message {
        let mut data = vec![
            Message::BulkString(BulkString::new(b"HSET")),
            Message::BulkString(BulkString::from(other.key.clone()))];

        for (field, value) in other.data.iter() {
            data.push(Message::BulkString(BulkString::from(field.clone())));
            data.push(Message::BulkString(BulkString::from(value.clone())));
        }

        Message::Array(Array {
            inner: Some(data),
        })
    }
}

impl Compose for HashSetRequest {
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
            parser.parse(b"hset 0 1 2\r\n").unwrap().into_inner(),
            Request::HashSet(HashSetRequest::new(b"0", &[(b"1", b"2")]))
        );

        assert_eq!(
            parser.parse(b"hset 0 1 2 3 4\r\n").unwrap().into_inner(),
            Request::HashSet(HashSetRequest::new(b"0", &[(b"1", b"2"), (b"3", b"4")]))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$4\r\nhset\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n")
                .unwrap()
                .into_inner(),
            Request::HashSet(HashSetRequest::new(b"0", &[(b"1", b"2")]))
        );

        assert_eq!(
            parser
                .parse(b"*6\r\n$4\r\nhset\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n")
                .unwrap()
                .into_inner(),
            Request::HashSet(HashSetRequest::new(b"0", &[(b"1", b"2"), (b"3", b"4")]))
        );
    }
}
