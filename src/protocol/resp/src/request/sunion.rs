// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "sunion")]
pub static SUNION: Counter = Counter::new();

#[metric(name = "sunion_ex")]
pub static SUNION_EX: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub struct SetUnion {
    keys: Vec<Arc<[u8]>>,
}

impl TryFrom<Message> for SetUnion {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let array = match value {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        let mut array = array.inner.unwrap();
        if array.len() < 2 {
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

impl SetUnion {
    pub fn new(keys: &[&[u8]]) -> Self {
        Self {
            keys: keys.iter().copied().map(From::from).collect(),
        }
    }

    pub fn keys(&self) -> &[Arc<[u8]>] {
        &self.keys
    }
}

impl From<&SetUnion> for Message {
    fn from(value: &SetUnion) -> Self {
        let mut vals = Vec::with_capacity(value.keys().len() + 1);
        vals.push(Message::bulk_string(b"SUNION"));
        vals.extend(value.keys().iter().map(|v| Message::bulk_string(v)));

        Message::Array(Array { inner: Some(vals) })
    }
}

impl Compose for SetUnion {
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
            parser.parse(b"sunion k1 k2 k3\r\n").unwrap().into_inner(),
            Request::SetUnion(SetUnion::new(&[b"k1", b"k2", b"k3"]))
        );

        assert_eq!(
            parser
                .parse(b"*3\r\n$6\r\nsunion\r\n$2\r\nk1\r\n$2\r\nk2\r\n")
                .unwrap()
                .into_inner(),
            Request::SetUnion(SetUnion::new(&[b"k1", b"k2"]))
        );
    }
}
