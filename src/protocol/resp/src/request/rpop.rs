// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

counter!(RPOP);
counter!(RPOP_EX);

#[derive(Debug, PartialEq, Eq)]
pub struct ListPopBack {
    key: Arc<[u8]>,
    count: Option<u64>,
}

impl TryFrom<Message> for ListPopBack {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let array = match value {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        let mut array = array.inner.unwrap();
        if !(2..=3).contains(&array.len()) {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array);
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        let count = take_bulk_string_as_u64(&mut array)?;

        Ok(Self { key, count })
    }
}

impl ListPopBack {
    pub fn new(key: &[u8], count: Option<u64>) -> Self {
        Self {
            key: key.into(),
            count,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn count(&self) -> Option<u64> {
        self.count
    }
}

impl From<&ListPopBack> for Message {
    fn from(value: &ListPopBack) -> Self {
        let mut vals = Vec::with_capacity(3);
        vals.push(Message::bulk_string(b"LPOP"));
        vals.push(Message::bulk_string(value.key()));

        if let Some(count) = value.count() {
            vals.push(Message::bulk_string(count.to_string().as_bytes()));
        }

        Message::Array(Array { inner: Some(vals) })
    }
}

impl Compose for ListPopBack {
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
            parser.parse(b"rpop key\r\n").unwrap().into_inner(),
            Request::ListPopBack(ListPopBack::new(b"key", None))
        );

        assert_eq!(
            parser.parse(b"rpop key 4\r\n").unwrap().into_inner(),
            Request::ListPopBack(ListPopBack::new(b"key", Some(4)))
        );

        assert_eq!(
            parser
                .parse(b"*2\r\n$4\r\nrpop\r\n$1\r\nb\r\n")
                .unwrap()
                .into_inner(),
            Request::ListPopBack(ListPopBack::new(b"b", None))
        );
    }
}
