// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::str::FromStr;

use crate::parse::{ParseError, ParseResult};
#[derive(Clone)]
pub struct Parser<'a> {
    data: &'a [u8],
}

impl<'a> Parser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn remaining(&self) -> &'a [u8] {
        self.data
    }

    /// Fetch the next byte without advancing the parser.
    pub fn peek(&self) -> Option<u8> {
        self.data.first().copied()
    }

    /// Helper function that ensures a parse operation is applied in its
    /// entirety or not at all.
    pub fn try_parse<F, R>(&mut self, func: F) -> ParseResult<'a, R>
    where
        F: FnOnce(&mut Self) -> ParseResult<'a, R>,
    {
        let mut copy = self.clone();
        match func(&mut copy) {
            Ok(ret) => {
                *self = copy;
                Ok(ret)
            }
            Err(e) => Err(e),
        }
    }

    fn parse_literal(&mut self, lit: &'static [u8]) -> ParseResult<'a, &'a [u8]> {
        if self.data.len() < lit.len() {
            return Err(ParseError::Incomplete);
        }

        let (head, rest) = self.data.split_at(lit.len());

        if head != lit {
            return Err(ParseError::invalid_literal(lit, head));
        }

        self.data = rest;
        Ok(head)
    }

    fn parse_bytes(&mut self, len: usize) -> ParseResult<'a, &'a [u8]> {
        if self.data.len() < len {
            return Err(ParseError::Incomplete);
        }

        let (head, rest) = self.data.split_at(len);
        self.data = rest;
        Ok(head)
    }

    fn parse_crlf(&mut self) -> ParseResult<'a, ()> {
        self.parse_literal(b"\r\n").map(|_| ())
    }

    fn parse_len(&mut self) -> ParseResult<'a, usize> {
        self.try_parse(|p| {
            let text = p.parse_delimited_text()?;
            let text = std::str::from_utf8(text).map_err(|_| ParseError::invalid_number(text))?;
            text.parse()
                .map_err(|_| ParseError::invalid_number(text.as_bytes()))
        })
    }

    /// Parses text not containing CR or LF followed by CRLF
    pub(crate) fn parse_delimited_text(&mut self) -> ParseResult<'a, &'a [u8]> {
        self.try_parse(|p| {
            let text = match memchr::memchr2(b'\r', b'\n', p.data) {
                Some(offset) => p.parse_bytes(offset)?,
                None => return Err(ParseError::Incomplete),
            };

            p.parse_crlf()?;

            Ok(text)
        })
    }

    /// Helper for [`CommandParser`].
    ///
    /// The inline format allows for quoting strings so we need to handle that.
    pub(crate) fn parse_command_line(&mut self) -> ParseResult<'a, &'a [u8]> {
        self.try_parse(|p| {
            let mut copy = p.clone();
            copy.try_parse(|p| {
                loop {
                    match memchr::memchr3(b'\r', b'\n', b'"', p.data) {
                        Some(index) => p.parse_bytes(index)?,
                        None => return Err(ParseError::Incomplete),
                    };

                    match p.peek() {
                        Some(b'\r' | b'\n') => break,
                        Some(b'"') => p.parse_literal(b"\"")?,
                        None => return Err(ParseError::Incomplete),
                        _ => unreachable!(),
                    };

                    // Found an escaped section, parse to the escape
                    loop {
                        match memchr::memchr2(b'\\', b'"', p.data) {
                            Some(index) => p.parse_bytes(index)?,
                            None => return Err(ParseError::Incomplete),
                        };

                        if p.parse_literal(b"\\\"").is_err() {
                            p.parse_literal(b"\"")?;
                            break;
                        }
                    }
                }

                Ok(())
            })?;

            let len = (copy.remaining().as_ptr() as usize) - (p.remaining().as_ptr() as usize);
            let text = p.parse_bytes(len)?;
            p.parse_crlf()?;
            Ok(text)
        })
    }

    pub fn parse_simple_string(&mut self) -> ParseResult<'a, &'a [u8]> {
        self.try_parse(|p| {
            p.parse_literal(b"+")?;
            p.parse_delimited_text()
        })
    }

    pub fn parse_bulk_string(&mut self) -> ParseResult<'a, Option<&'a [u8]>> {
        self.try_parse(|p| {
            p.parse_literal(b"$")?;

            let result = if matches!(p.peek(), Some(b'-')) {
                p.parse_literal(b"-1")?;
                None
            } else {
                let len = p.parse_len()?;
                let data = p.parse_bytes(len)?;
                Some(data)
            };

            p.parse_crlf()?;
            Ok(result)
        })
    }

    pub fn parse_string(&mut self) -> ParseResult<'a, Option<&'a [u8]>> {
        match self.peek() {
            Some(b'+') => self.parse_simple_string().map(Some),
            Some(b'$') => self.parse_bulk_string(),
            Some(c) => Err(ParseError::UnexpectedCharacter {
                expected: "\"+\" or \"$\"",
                found: c,
            }),
            None => Err(ParseError::Incomplete),
        }
    }

    pub fn parse_error(&mut self) -> ParseResult<'a, &'a [u8]> {
        self.try_parse(|p| {
            p.parse_literal(b"-")?;
            p.parse_delimited_text()
        })
    }

    pub fn parse_literal_u64(&mut self) -> ParseResult<'a, u64> {
        self.try_parse(|p| {
            p.parse_literal(b":")?;
            let text = p.parse_delimited_text()?;
            let text = std::str::from_utf8(text).map_err(|_| ParseError::invalid_number(text))?;
            text.parse()
                .map_err(|_| match <i64 as FromStr>::from_str(text) {
                    Ok(_) => ParseError::unexpected_negative_number(text.as_bytes()),
                    Err(_) => ParseError::invalid_number(text.as_bytes()),
                })
        })
    }

    pub fn parse_literal_i64(&mut self) -> ParseResult<'a, i64> {
        self.try_parse(|p| {
            p.parse_literal(b":")?;
            let text = p.parse_delimited_text()?;
            std::str::from_utf8(text)
                .map_err(|_| ParseError::invalid_number(text))?
                .parse()
                .map_err(|_| ParseError::invalid_number(text))
        })
    }

    pub fn parse_array<'p>(&'p mut self) -> ParseResult<'a, Option<ArrayParser<'a, 'p>>> {
        let len = self.try_parse(|p| {
            p.parse_literal(b"*")?;

            Ok(match p.peek() {
                Some(b'-') => {
                    p.parse_literal(b"-1")?;
                    p.parse_crlf()?;
                    None
                }
                _ => Some(p.parse_len()?),
            })
        })?;

        Ok(len.map(|len| ArrayParser {
            parser: self,
            remaining: len,
        }))
    }

    pub fn parse_any<V: Visitor<'a>>(&mut self, visitor: V) -> ParseResult<'a, V::Output> {
        match self.peek() {
            Some(b'+') => visitor.visit_simple_string(self.parse_simple_string()?),
            Some(b'-') => visitor.visit_error_string(self.parse_error()?),
            Some(b':') => visitor.visit_integer(self.parse_literal_i64()?),
            Some(b'$') => visitor.visit_bulk_string(self.parse_bulk_string()?),
            Some(b'*') => {
                let mut av = self.parse_array()?;
                let output = visitor.visit_array(av.as_mut())?;

                if let Some(av) = av {
                    av.finish()?;
                }

                Ok(output)
            }
            Some(c) => Err(ParseError::UnexpectedCharacter {
                expected: "a valid data type identifier",
                found: c,
            }),
            None => Err(ParseError::Incomplete),
        }
    }
}

#[allow(unused_variables)]
pub trait Visitor<'a>: Sized {
    type Output: 'a;

    fn expected(&self) -> &'static str;

    fn visit_simple_string(self, value: &'a [u8]) -> ParseResult<'a, Self::Output> {
        Err(ParseError::Custom {
            expected: self.expected(),
            found: "a simple string",
        })
    }

    fn visit_bulk_string(self, value: Option<&'a [u8]>) -> ParseResult<'a, Self::Output> {
        Err(ParseError::Custom {
            expected: self.expected(),
            found: "a bulk string",
        })
    }

    fn visit_error_string(self, value: &'a [u8]) -> ParseResult<'a, Self::Output> {
        Err(ParseError::Custom {
            expected: self.expected(),
            found: "an error string",
        })
    }

    fn visit_integer(self, value: i64) -> ParseResult<'a, Self::Output> {
        Err(ParseError::Custom {
            expected: self.expected(),
            found: "an integer",
        })
    }

    fn visit_array(self, value: Option<&mut ArrayParser<'a, '_>>) -> ParseResult<'a, Self::Output> {
        Err(ParseError::Custom {
            expected: self.expected(),
            found: "an array",
        })
    }
}

pub struct ArrayParser<'a, 'p> {
    parser: &'p mut Parser<'a>,
    remaining: usize,
}

impl<'a, 'p> ArrayParser<'a, 'p> {
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    pub fn parse_simple_string(&mut self) -> ParseResult<'a, &'a [u8]> {
        self.try_parse(|p| p.parse_simple_string())
    }

    pub fn parse_bulk_string(&mut self) -> ParseResult<'a, Option<&'a [u8]>> {
        self.try_parse(|p| p.parse_bulk_string())
    }

    pub fn parse_string(&mut self) -> ParseResult<'a, Option<&'a [u8]>> {
        self.try_parse(|p| p.parse_string())
    }

    pub fn parse_error(&mut self) -> ParseResult<'a, &'a [u8]> {
        self.try_parse(|p| p.parse_error())
    }

    pub fn parse_literal_u64(&mut self) -> ParseResult<'a, u64> {
        self.try_parse(|p| p.parse_literal_u64())
    }

    pub fn parse_literal_i64(&mut self) -> ParseResult<'a, i64> {
        self.try_parse(|p| p.parse_literal_i64())
    }

    pub fn finish(self) -> ParseResult<'a, ()> {
        match self.remaining {
            0 => Ok(()),
            _ => Err(ParseError::UnexpectedArrayElement),
        }
    }

    fn check_remaining(&self) -> ParseResult<'a, ()> {
        match self.remaining() {
            0 => Err(ParseError::ExpectedArrayElement),
            _ => Ok(()),
        }
    }

    fn try_parse<F, R>(&mut self, func: F) -> ParseResult<'a, R>
    where
        F: FnOnce(&mut Parser<'a>) -> ParseResult<'a, R>,
        R: 'a,
    {
        self.check_remaining()?;
        let value = self.parser.try_parse(func)?;
        self.remaining -= 1;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use bstr::BStr;

    use super::*;

    use std::borrow::Cow;
    use std::fmt;

    struct Test<T>(T);

    impl<T: TestFmt> fmt::Debug for Test<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            <T as TestFmt>::fmt(&self.0, f)
        }
    }

    macro_rules! parse_test {
        ($name:ident, $method:ident ( $data:expr ), $result:pat) => {
            #[test]
            fn $name() {
                let data = $data;
                let mut parser = Parser::new(data.as_ref());

                let result = parser.$method();

                assert!(
                    matches!(result, $result),
                    "found {:?}, expected {}",
                    result.as_ref().map(Test),
                    stringify!($result)
                );
            }
        };
    }

    parse_test!(simple_string, parse_simple_string("+TEST\r\n"), Ok(b"TEST"));

    parse_test!(
        simple_string_incomplete,
        parse_simple_string("+TEST"),
        Err(ParseError::Incomplete)
    );

    parse_test!(
        simple_string_invalid,
        parse_simple_string("+A\rB\r\n"),
        Err(ParseError::InvalidLiteral {
            expected: b"\r\n",
            found: Cow::Borrowed(b"\rB")
        })
    );

    parse_test!(
        bulk_string,
        parse_bulk_string("$6\r\nTE\r\nST\r\n"),
        Ok(Some(b"TE\r\nST"))
    );

    parse_test!(bulk_string_nil, parse_bulk_string("$-1\r\n"), Ok(None));

    parse_test!(
        bulk_string_incomplete,
        parse_bulk_string("$77\r\nTEST\r\n"),
        Err(ParseError::Incomplete)
    );

    parse_test!(
        bulk_string_invalid_len,
        parse_bulk_string("$aaa\r\nTEST\r\n"),
        Err(ParseError::InvalidNumber(Cow::Borrowed(b"aaa")))
    );

    parse_test!(
        bulk_string_invalid_nil,
        parse_bulk_string("$-2\r\n"),
        Err(ParseError::InvalidLiteral {
            expected: b"-1",
            found: Cow::Borrowed(b"-2")
        })
    );

    parse_test!(
        command_line_basic,
        parse_command_line("test a b\r\nababab"),
        Ok(b"test a b")
    );

    parse_test!(
        command_line_escaped,
        parse_command_line("test \"a\r\nb\" c\r\n"),
        Ok(b"test \"a\r\nb\" c")
    );

    parse_test!(
        command_line_escaped_quote,
        parse_command_line("test \"a\\\" \r\nb\"\r\n"),
        Ok(b"test \"a\\\" \r\nb\"")
    );

    // Helper trait for formatting byte strings in a reasonable manner.
    trait TestFmt {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result;
    }

    impl<'a, T> TestFmt for &'a T
    where
        T: TestFmt + ?Sized,
    {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            <T as TestFmt>::fmt(self, fmt)
        }
    }

    impl TestFmt for [u8] {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(fmt, "{:?}", BStr::new(self))
        }
    }

    impl<T> TestFmt for Option<T>
    where
        T: TestFmt,
    {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(fmt, "{:?}", self.as_ref().map(Test))
        }
    }

    impl TestFmt for u64 {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(fmt, "{self}")
        }
    }

    impl TestFmt for i64 {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(fmt, "{self}")
        }
    }
}
