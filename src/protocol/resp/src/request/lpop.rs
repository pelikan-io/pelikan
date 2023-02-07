// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

counter!(LPOP);
counter!(LPOP_EX);

#[derive(Debug, PartialEq, Eq)]
pub struct ListPop {
    key: Arc<[u8]>,
    count: Option<u64>,
}

impl TryFrom<Message> for ListPop {
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

impl ListPop {
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

impl From<&ListPop> for Message {
    fn from(value: &ListPop) -> Self {
        let mut vals = Vec::with_capacity(3);
        vals.push(Message::bulk_string(b"LPOP"));
        vals.push(Message::bulk_string(value.key()));

        if let Some(count) = value.count() {
            vals.push(Message::bulk_string(count.to_string().as_bytes()));
        }

        Message::Array(Array { inner: Some(vals) })
    }
}

impl Compose for ListPop {
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
            parser.parse(b"lpop key\r\n").unwrap().into_inner(),
            Request::ListPop(ListPop::new(b"key", None))
        );

        assert_eq!(
            parser.parse(b"lpop key 4\r\n").unwrap().into_inner(),
            Request::ListPop(ListPop::new(b"key", Some(4)))
        );

        assert_eq!(
            parser
                .parse(b"*2\r\n$4\r\nlpop\r\n$1\r\nb\r\n")
                .unwrap()
                .into_inner(),
            Request::ListPop(ListPop::new(b"b", None))
        );
    }
}
