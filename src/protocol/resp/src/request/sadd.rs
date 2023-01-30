// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

counter!(SADD);
counter!(SADD_EX);

#[derive(Debug, PartialEq, Eq)]
pub struct SetAdd {
    key: Arc<[u8]>,
    members: Vec<Arc<[u8]>>,
}

impl TryFrom<Message> for SetAdd {
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
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        let mut members = Vec::with_capacity(array.len());
        while !array.is_empty() {
            members.push(
                take_bulk_string(&mut array)?
                    .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?,
            );
        }

        Ok(Self { key, members })
    }
}

impl SetAdd {
    pub fn new(key: &[u8], members: &[&[u8]]) -> Self {
        Self {
            key: key.into(),
            members: members.iter().copied().map(From::from).collect(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn members(&self) -> &[Arc<[u8]>] {
        &self.members
    }
}

impl From<&SetAdd> for Message {
    fn from(value: &SetAdd) -> Self {
        let mut vals = Vec::with_capacity(value.members.len() + 2);

        vals.push(Message::BulkString(BulkString::new(b"SADD")));
        vals.push(Message::BulkString(BulkString::new(value.key())));
        vals.extend(
            value
                .members()
                .iter()
                .map(|v| Message::BulkString(BulkString::new(&**v))),
        );

        Message::Array(Array { inner: Some(vals) })
    }
}

impl Compose for SetAdd {
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
            parser
                .parse(b"sadd key m1 m2 m3 m4\r\n")
                .unwrap()
                .into_inner(),
            Request::SetAdd(SetAdd::new(b"key", &[b"m1", b"m2", b"m3", b"m4"]))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$4\r\nsadd\r\n$3\r\nkey\r\n$2\r\nm1\r\n$2\r\nm2\r\n")
                .unwrap()
                .into_inner(),
            Request::SetAdd(SetAdd::new(b"key", &[b"m1", b"m2"]))
        );
    }
}
