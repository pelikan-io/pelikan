// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::sync::Arc;

use std::io::{Error, ErrorKind};

#[derive(Debug, PartialEq, Eq)]
pub struct BulkString {
    pub(crate) inner: Option<Arc<[u8]>>,
}

impl BulkString {
    pub fn new(bytes: &[u8]) -> Self {
        Self {
            inner: Some(bytes.into()),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.as_ref().map(|i| i.len()).unwrap_or(0)
    }

    pub fn null() -> Self {
        Self { inner: None }
    }

    pub fn bytes(&self) -> Option<&[u8]> {
        self.inner.as_ref().map(|v| v.as_ref())
    }
}

impl From<Arc<[u8]>> for BulkString {
    fn from(other: Arc<[u8]>) -> Self {
        Self { inner: Some(other) }
    }
}

impl TryInto<u64> for BulkString {
    type Error = Error;

    fn try_into(self) -> std::result::Result<u64, Error> {
        if self.inner.is_none() {
            return Err(Error::new(ErrorKind::Other, "null bulk string"));
        }

        std::str::from_utf8(self.inner.as_ref().unwrap())
            .map_err(|_| Error::new(ErrorKind::Other, "bulk string is not valid utf8"))?
            .parse::<u64>()
            .map_err(|_| Error::new(ErrorKind::Other, "bulk string is not a valid u64"))
    }
}

impl Compose for BulkString {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        if let Some(value) = &self.inner {
            let header = format!("${}\r\n", value.len());
            buf.put_slice(header.as_bytes());
            buf.put_slice(value);
            buf.put_slice(b"\r\n");
            header.len() + value.len() + 2
        } else {
            // A null bulk string is serialized as `$-1\r\n`.
            buf.put_slice(b"$-1\r\n");
            5
        }
    }
}

pub fn parse(input: &[u8]) -> IResult<&[u8], BulkString> {
    match input.first() {
        Some(b'-') => {
            let (input, _) = take(1usize)(input)?;
            let (input, len) = digit1(input)?;
            if len != b"1" {
                return Err(Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
            let (input, _) = crlf(input)?;
            Ok((input, BulkString { inner: None }))
        }
        Some(_) => {
            let (input, len) = digit1(input)?;
            let len = unsafe { std::str::from_utf8_unchecked(len).to_owned() };
            let len = len.parse::<usize>().map_err(|_| {
                Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
            })?;
            let (input, _) = crlf(input)?;
            let (input, value) = take(len)(input)?;
            let (input, _) = crlf(input)?;
            Ok((
                input,
                BulkString {
                    inner: Some(value.into()),
                },
            ))
        }
        None => Err(Err::Incomplete(Needed::new(1))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(message(b"$-1\r\n"), Ok((&b""[..], Message::null(),)));

        assert_eq!(
            message(b"$0\r\n\r\n"),
            Ok((&b""[..], Message::bulk_string(&[])))
        );

        assert_eq!(
            message(b"$1\r\n\0\r\n"),
            Ok((&b""[..], Message::bulk_string(&[0])))
        );

        assert_eq!(
            message(b"$11\r\nHELLO WORLD\r\n"),
            Ok((&b""[..], Message::bulk_string("HELLO WORLD".as_bytes())))
        );
    }
}
