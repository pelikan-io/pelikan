// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Del {
    keys: Vec<Arc<[u8]>>,
}

impl TryFrom<Message> for Del {
    type Error = Error;

    fn try_from(value: Message) -> std::io::Result<Self> {
        let array = match value {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        let mut array = array.inner.unwrap();
        if array.len() < 3 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array)?;

        let mut keys = Vec::with_capacity(array.len());
        while !array.is_empty() {
            keys.push(
                take_bulk_string(&mut array)?
                    .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?,
            );
        }

        Ok(Self { keys })
    }
}

impl Del {
    pub fn new(keys: &[&[u8]]) -> Self {
        Self {
            keys: keys.iter().copied().map(From::from).collect(),
        }
    }

    pub fn keys(&self) -> &[Arc<[u8]>] {
        &self.keys
    }
}

impl From<&Del> for Message {
    fn from(value: &Del) -> Self {
        let mut vals = Vec::with_capacity(value.keys.len() + 1);

        vals.push(Message::BulkString(BulkString::new(b"DEL")));
        vals.extend(
            value
                .keys()
                .iter()
                .map(|v| Message::BulkString(BulkString::new(v))),
        );

        Message::Array(Array { inner: Some(vals) })
    }
}

impl Compose for Del {
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
            parser.parse(b"del k1 k2 k3 k4\r\n").unwrap().into_inner(),
            Request::Del(Del::new(&[b"k1", b"k2", b"k3", b"k4"]))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$3\r\ndel\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n")
                .unwrap()
                .into_inner(),
            Request::Del(Del::new(&[b"k1", b"k2", b"k3"]))
        );
    }
}
