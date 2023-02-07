// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use logger::klog;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub struct Incr {
    key: Arc<[u8]>,
}

impl TryFrom<Message> for Incr {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        if let Message::Array(array) = other {
            if array.inner.is_none() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let mut array = array.inner.unwrap();

            if array.len() != 2 {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let key = if let Message::BulkString(key) = array.remove(1) {
                if key.inner.is_none() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }

                let key = key.inner.unwrap();

                if key.len() == 0 {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }

                key
            } else {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            };

            Ok(Self { key })
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl Incr {
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.into() }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

impl From<&Incr> for Message {
    fn from(other: &Incr) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"INCR")),
                Message::BulkString(BulkString::from(other.key.clone())),
            ]),
        })
    }
}

impl Compose for Incr {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        let message = Message::from(self);
        message.compose(buf)
    }
}

impl Klog for Incr {
    type Response = Response;

    fn klog(&self, response: &Self::Response) {
        let (code, len) = match response {
            Message::BulkString(_) if *response == Response::null() => (ResponseCode::Miss, 0),
            Message::BulkString(s) => (ResponseCode::Hit, s.len()),
            _ => (ResponseCode::Miss, 0),
        };

        klog!(
            "\"incr {}\" {} {}",
            string_key(self.key()),
            code as u32,
            len
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser() {
        let parser = RequestParser::new();
        assert_eq!(
            parser.parse(b"incr 0\r\n").unwrap().into_inner(),
            Request::Incr(Incr::new(b"0"))
        );

        assert_eq!(
            parser
                .parse(b"incr \"\0\r\n key\"\r\n")
                .unwrap()
                .into_inner(),
            Request::Incr(Incr::new(b"\0\r\n key"))
        );

        assert_eq!(
            parser
                .parse(b"*2\r\n$3\r\nincr\r\n$1\r\n0\r\n")
                .unwrap()
                .into_inner(),
            Request::Incr(Incr::new(b"0"))
        );
    }
}
