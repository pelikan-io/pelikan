// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

/// Represents the btree add command which was added to Twitter's internal
/// version of redis32.
/// format is: badd outer_key (inner_key value)+
#[derive(Debug, PartialEq, Eq)]
pub struct BtreeAdd {
    outer_key: Arc<[u8]>,
    inner_key_value_pairs: Box<[FieldValuePair]>,
}

impl BtreeAdd {
    pub fn new(outer_key: Arc<[u8]>, inner_key_value_pairs: Box<[FieldValuePair]>) -> Self {
        Self {
            outer_key,
            inner_key_value_pairs,
        }
    }

    pub fn outer_key(&self) -> &[u8] {
        &self.outer_key
    }

    pub fn inner_key_value_pairs(&self) -> Box<[(&[u8], &[u8])]> {
        self.inner_key_value_pairs
            .iter()
            .map(|(k, v)| (&**k, &**v))
            .collect::<Vec<(&[u8], &[u8])>>()
            .into_boxed_slice()
    }
}

impl TryFrom<Message> for BtreeAdd {
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

            let outer_key = take_bulk_string(&mut array)?;

            if outer_key.is_none() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let outer_key = outer_key.unwrap();

            if outer_key.is_empty() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            //loop as long as we have at least 2 arguments after the command
            let mut pairs = Vec::with_capacity(array.len() / 2);
            while array.len() >= 2 {
                let inner_key = take_bulk_string(&mut array)?;
                if inner_key.is_none() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }
                let inner_key = inner_key.unwrap();

                if inner_key.is_empty() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }

                let value = take_bulk_string(&mut array)?;

                if value.is_none() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }

                let value = value.unwrap();

                if value.is_empty() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }

                pairs.push((inner_key, value));
            }

            Ok(Self {
                outer_key,
                inner_key_value_pairs: pairs.into(),
            })
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl From<&BtreeAdd> for Message {
    fn from(other: &BtreeAdd) -> Message {
        let mut v = vec![
            Message::bulk_string(b"BADD"),
            Message::BulkString(BulkString::from(other.outer_key.clone())),
        ];
        for kv in (*other.inner_key_value_pairs).iter() {
            v.push(Message::BulkString(BulkString::from(kv.0.clone())));
            v.push(Message::BulkString(BulkString::from(kv.1.clone())));
        }

        Message::Array(Array { inner: Some(v) })
    }
}

impl Compose for BtreeAdd {
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

        //1 key value pair
        if let Request::BtreeAdd(request) = parser
            .parse(b"badd outer inner 42\r\n")
            .unwrap()
            .into_inner()
        {
            assert_eq!(request.outer_key(), b"outer");
            assert_eq!(request.inner_key_value_pairs.len(), 1);
            assert_eq!(request.inner_key_value_pairs()[0].0, b"inner");
            assert_eq!(request.inner_key_value_pairs()[0].1, b"42");
        } else {
            panic!("invalid parse result");
        }

        //> 1 key value pairs
        if let Request::BtreeAdd(request) = parser
            .parse(b"badd outer inner 42 inner2 7*6\r\n")
            .unwrap()
            .into_inner()
        {
            assert_eq!(request.outer_key(), b"outer");
            assert_eq!(request.inner_key_value_pairs.len(), 2);
            assert_eq!(request.inner_key_value_pairs()[0].0, b"inner");
            assert_eq!(request.inner_key_value_pairs()[0].1, b"42");
            assert_eq!(request.inner_key_value_pairs()[1].0, b"inner2");
            assert_eq!(request.inner_key_value_pairs()[1].1, b"7*6");
        } else {
            panic!("invalid parse result");
        }
    }
}
