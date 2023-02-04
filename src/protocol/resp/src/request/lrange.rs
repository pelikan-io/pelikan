// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

counter!(LRANGE);
counter!(LRANGE_EX);

#[derive(Debug, PartialEq, Eq)]
pub struct ListRange {
    key: Arc<[u8]>,
    start: i64,
    stop: i64,
}

impl TryFrom<Message> for ListRange {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let mut array = match value {
            Message::Array(array) => array.inner.unwrap(),
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        if array.len() != 4 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array);
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        let start = take_bulk_string_as_i64(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let stop = take_bulk_string_as_i64(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        Ok(Self { key, start, stop })
    }
}

impl ListRange {
    pub fn new(key: &[u8], start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn start(&self) -> i64 {
        self.start
    }

    pub fn stop(&self) -> i64 {
        self.stop
    }
}

impl From<&ListRange> for Message {
    fn from(value: &ListRange) -> Self {
        Message::Array(Array {
            inner: Some(vec![
                Message::bulk_string(b"LRANGE"),
                Message::bulk_string(value.key()),
                Message::bulk_string(value.start().to_string().as_bytes()),
                Message::bulk_string(value.stop().to_string().as_bytes()),
            ]),
        })
    }
}

impl Compose for ListRange {
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
            parser.parse(b"lrange key 0 1\r\n").unwrap().into_inner(),
            Request::ListRange(ListRange::new(b"key", 0, 1))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$6\r\nlrange\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\n1\r\n")
                .unwrap()
                .into_inner(),
            Request::ListRange(ListRange::new(b"key", 0, 1))
        );
    }
}
