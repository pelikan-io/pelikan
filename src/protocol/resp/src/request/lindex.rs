// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

counter!(LINDEX);
counter!(LINDEX_EX);
counter!(LINDEX_HIT);
counter!(LINDEX_MISS);

#[derive(Debug, PartialEq, Eq)]
pub struct ListIndex {
    key: Arc<[u8]>,
    index: i64,
}

impl TryFrom<Message> for ListIndex {
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
        let index = take_bulk_string_as_i64(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        Ok(Self { key, index })
    }
}

impl ListIndex {
    pub fn new(key: &[u8], index: i64) -> Self {
        Self {
            key: key.into(),
            index,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn index(&self) -> i64 {
        self.index
    }
}

impl From<&ListIndex> for Message {
    fn from(value: &ListIndex) -> Self {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"LINDEX")),
                Message::BulkString(BulkString::new(value.key())),
                Message::BulkString(BulkString::new(value.index().to_string().as_bytes())),
            ]),
        })
    }
}

impl Compose for ListIndex {
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
            parser.parse(b"lindex test 10\r\n").unwrap().into_inner(),
            Request::ListIndex(ListIndex::new(b"test", 10))
        );

        assert_eq!(
            parser
                .parse(b"*3\r\n$6\r\nlindex\r\n$3\r\naaa\r\n$1\r\n5\r\n")
                .unwrap()
                .into_inner(),
            Request::ListIndex(ListIndex::new(b"aaa", 5))
        );
    }
}
