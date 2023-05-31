// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_common::Compose;

#[derive(Debug, PartialEq, Eq)]
pub struct Array {
    pub(crate) inner: Option<Vec<Message>>,
}

impl Array {
    /// Create a null array.
    ///
    /// This serializes to `*-1\r\n` on the wire.
    pub fn null() -> Self {
        Self { inner: None }
    }

    /// Get the number of items in the array. The `None` variant indicates a
    /// null array.
    pub fn len(&self) -> Option<usize> {
        self.inner.as_ref().map(|a| a.len())
    }
}

impl Compose for Array {
    fn compose(&self, session: &mut dyn BufMut) -> usize {
        let mut len = 0;
        if let Some(values) = &self.inner {
            let header = format!("*{}\r\n", values.len());
            session.put_slice(header.as_bytes());
            len += header.as_bytes().len();
            for value in values {
                len += value.compose(session);
            }
            session.put_slice(b"\r\n");
            len += 2;
        } else {
            // A null array is serialized as `*-1\r\n`.
            session.put_slice(b"*-1\r\n");
            len += 5;
        }
        len
    }
}

pub fn parse(input: &[u8]) -> IResult<&[u8], Array> {
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
            Ok((input, Array { inner: None }))
        }
        Some(_) => {
            let (input, len) = digit1(input)?;
            let len = unsafe { std::str::from_utf8_unchecked(len).to_owned() };
            let len = len.parse::<usize>().map_err(|_| {
                Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
            })?;
            let (mut input, _) = crlf(input)?;
            let mut values = Vec::new();
            for _ in 0..len {
                let (i, value) = message(input)?;
                values.push(value);
                input = i;
            }
            Ok((
                input,
                Array {
                    inner: Some(values),
                },
            ))
        }
        None => Err(Err::Incomplete(Needed::new(1))),
    }
}

pub struct Iter<'a> {
    array: &'a Array,
    position: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Message;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &self.array.inner {
            let next = inner.get(self.position);
            self.position += 1;
            next
        } else {
            None
        }
    }
}

impl<'a> IntoIterator for &'a Array {
    type IntoIter = Iter<'a>;
    type Item = &'a Message;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            array: self,
            position: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(
            message(b"*-1\r\n"),
            Ok((&b""[..], Message::Array(Array::null()),))
        );

        assert_eq!(
            message(b"*1\r\n$5\r\nHELLO\r\n"),
            Ok((
                &b""[..],
                Message::Array(Array {
                    inner: Some(vec![Message::bulk_string(b"HELLO")])
                })
            ))
        );
    }

    #[test]
    fn iter() {
        let message = Array::null();
        assert_eq!(message.into_iter().next(), None);

        let message = Array {
            inner: Some(vec![Message::bulk_string(b"HELLO")])
        };
        assert_eq!(message.into_iter().next(), Some(&Message::bulk_string(b"HELLO")));

    }
}
