// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

counter!(SREM);
counter!(SREM_EX);

#[derive(Debug, PartialEq, Eq)]
pub struct SetRem {
    key: Arc<[u8]>,
    members: Vec<Arc<[u8]>>,
}

impl TryFrom<Message> for SetRem {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let array = match value {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        let mut array = array.inner.unwrap();
        if array.len() != 3 {
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

impl SetRem {
    pub fn new(key: &[u8], members: &[&[u8]]) -> Self {
        Self {
            key: key.into(),
            members: members.iter().map(|&x| x.into()).collect(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn members(&self) -> &[Arc<[u8]>] {
        &self.members
    }
}

impl From<&SetRem> for Message {
    fn from(value: &SetRem) -> Self {
        let mut vals = Vec::with_capacity(value.members.len() + 2);

        vals.push(Message::BulkString(BulkString::new(b"SREM")));
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

impl Compose for SetRem {
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
            parser.parse(b"srem test member\r\n").unwrap().into_inner(),
            Request::SetRem(SetRem::new(b"test", &[b"member"]))
        );

        assert_eq!(
            parser
                .parse(b"*3\r\n$4\r\nsrem\r\n$4\r\ntest\r\n$6\r\nmember\r\n")
                .unwrap()
                .into_inner(),
            Request::SetRem(SetRem::new(b"test", &[b"member"]))
        );
    }
}
