// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Integer {
    pub(crate) inner: i64,
}

impl Integer {
    pub fn new(value: i64) -> Self {
        Self { inner: value }
    }

    pub fn value(self) -> i64 {
        self.inner
    }
}

impl Compose for Integer {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        let data = format!(":{}\r\n", self.inner);
        buf.put_slice(data.as_bytes());
        data.len()
    }
}

pub fn parse(input: &[u8]) -> IResult<&[u8], Integer> {
    let (input, string) = digit1(input)?;
    let (input, _) = crlf(input)?;

    let string = unsafe { std::str::from_utf8_unchecked(string).to_owned() };
    let value = string
        .parse::<i64>()
        .map_err(|_| Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;
    Ok((input, Integer { inner: value }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(message(b":0\r\n"), Ok((&b""[..], Message::integer(0),)));

        assert_eq!(
            message(b":1000\r\n"),
            Ok((&b""[..], Message::integer(1000),))
        );
    }
}
