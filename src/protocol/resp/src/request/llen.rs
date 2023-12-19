// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[metric(name = "llen")]
pub static LLEN: Counter = Counter::new();

#[metric(name = "llen_ex")]
pub static LLEN_EX: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub struct ListLen {
    key: Arc<[u8]>,
}

impl TryFrom<Message> for ListLen {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let array = match value {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        let mut array = array.inner.unwrap();
        if array.len() != 2 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array);
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        Ok(Self { key })
    }
}

impl ListLen {
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.into() }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

impl From<&ListLen> for Message {
    fn from(value: &ListLen) -> Self {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"LLEN")),
                Message::BulkString(BulkString::new(value.key())),
            ]),
        })
    }
}

impl Compose for ListLen {
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
            parser.parse(b"llen a\r\n").unwrap().into_inner(),
            Request::ListLen(ListLen::new(b"a"))
        );

        assert_eq!(
            parser
                .parse(b"*2\r\n$4\r\nllen\r\n$1\r\nb\r\n")
                .unwrap()
                .into_inner(),
            Request::ListLen(ListLen::new(b"b"))
        );
    }
}
