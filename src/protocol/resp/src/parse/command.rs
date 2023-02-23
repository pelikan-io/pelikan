// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::borrow::Cow;
use std::str::FromStr;

use crate::message::Message;

use super::{ArrayParser, ParseError, ParseResult, Parser};

enum Detail<'a, 'p> {
    Inline(Vec<Cow<'a, [u8]>>),
    Resp(ArrayParser<'a, 'p>),
}

/// Parser for redis commands, whether inline or using the RESP protocol.
///
/// See <https://redis.io/docs/reference/protocol-spec/> for the specification.
pub struct CommandParser<'a, 'p> {
    detail: Detail<'a, 'p>,
}

impl<'a, 'p> CommandParser<'a, 'p> {
    pub fn new(parser: &'p mut Parser<'a>) -> ParseResult<'a, Self> {
        match parser.peek() {
            // If the buffer starts with one of these then we have a RESP
            // object so we should parse it as such. This will give an error if
            // it is not an array but that is what we want here.
            Some(b'*' | b'+' | b'-' | b':' | b'$') => Ok(Self {
                detail: Detail::Resp(parser.parse_array()?.ok_or(ParseError::Custom {
                    expected: "a non-null array",
                    found: "a null array",
                })?),
            }),
            // Otherwise parse using the inline command format.
            Some(_) => {
                let mut parser = InlineParser::new(parser.parse_command_line()?);
                let mut items = Vec::new();

                while let Some(item) = parser.parse_next() {
                    items.push(item);
                }

                items.reverse();

                Ok(Self {
                    detail: Detail::Inline(items),
                })
            }
            None => Err(ParseError::Incomplete),
        }
    }

    pub fn remaining(&self) -> usize {
        match &self.detail {
            Detail::Resp(ap) => ap.remaining(),
            Detail::Inline(vals) => vals.len(),
        }
    }

    pub fn parse_string(&mut self) -> ParseResult<'a, Option<Cow<'a, [u8]>>> {
        match &mut self.detail {
            Detail::Resp(p) => Ok(p.parse_string()?.map(Cow::from)),
            Detail::Inline(values) => match values.pop() {
                Some(value) => Ok(Some(value)),
                None => Err(ParseError::ExpectedArrayElement),
            },
        }
    }

    pub fn parse_string_nonnil(&mut self) -> ParseResult<'a, Cow<'a, [u8]>> {
        self.parse_string()?.ok_or(ParseError::UnexpectedNilString)
    }

    pub fn parse_u64(&mut self) -> ParseResult<'a, u64> {
        let bytes = self.parse_string_nonnil()?;
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(_) => return Err(ParseError::invalid_number(bytes)),
        };

        match text.parse() {
            Ok(value) => Ok(value),
            Err(_) => Err(match <i64 as FromStr>::from_str(text) {
                Ok(_) => ParseError::unexpected_negative_number(bytes),
                Err(_) => ParseError::unexpected_negative_number(bytes),
            }),
        }
    }

    pub fn parse_i64(&mut self) -> ParseResult<'a, u64> {
        let bytes = self.parse_string_nonnil()?;
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(_) => return Err(ParseError::invalid_number(bytes)),
        };

        match text.parse() {
            Ok(value) => Ok(value),
            Err(_) => Err(ParseError::invalid_number(bytes)),
        }
    }

    pub fn finish(self) -> ParseResult<'a, ()> {
        match self.detail {
            Detail::Resp(ap) => ap.finish(),
            Detail::Inline(values) if values.is_empty() => Ok(()),
            Detail::Inline(_) => Err(ParseError::UnexpectedArrayElement),
        }
    }

    pub fn parse_message(mut self) -> ParseResult<'a, Message> {
        use crate::message::Array;

        // To avoid DOS attacks we should only pre-allocate capacity up to a certain size.
        const MAX_MESSAGE_PREALLOC: usize = 64;

        let mut elements = Vec::with_capacity(self.remaining().min(MAX_MESSAGE_PREALLOC));

        for _ in 0..self.remaining() {
            elements.push(Message::bulk_string(&self.parse_string_nonnil()?));
        }

        self.finish()?;

        Ok(Message::Array(Array {
            inner: Some(elements),
        }))
    }
}

struct InlineParser<'a> {
    data: &'a [u8],
}

impl<'a> InlineParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    fn advance(&mut self, len: usize) {
        self.data = &self.data[len..];
    }

    fn parse_bytes(&mut self, len: usize) -> &'a [u8] {
        let (head, rest) = self.data.split_at(len);
        self.data = rest;
        head
    }

    fn parse_bytes_escaped(&mut self) -> Cow<'a, [u8]> {
        let index = match memchr::memchr3(b'\\', b'"', b' ', self.data) {
            Some(index) => match self.data.get(index).copied() {
                Some(b' ') => {
                    let bytes = self.parse_bytes(index);
                    self.advance(1);
                    return Cow::Borrowed(bytes);
                }
                Some(b'\\' | b'"') => index,
                _ => unreachable!(),
            },
            None => return Cow::Borrowed(self.parse_bytes(self.data.len())),
        };

        #[derive(Copy, Clone, Debug, Eq, PartialEq)]
        enum State {
            Unquoted,
            Quoted,
        }

        let mut state = State::Unquoted;
        let mut output = Vec::new();
        output.extend_from_slice(self.parse_bytes(index));

        'outer: loop {
            if state == State::Unquoted {
                let index =
                    memchr::memchr3(b'\\', b'"', b' ', self.data).unwrap_or(self.data.len());
                output.extend_from_slice(self.parse_bytes(index));

                match *self.data {
                    [] => break 'outer,
                    [b' ', ..] => break 'outer,
                    [b'"', ..] => state = State::Quoted,
                    [b'\\'] => output.push(b'\\'),
                    [b'\\', c, ..] => {
                        output.push(c);
                        self.advance(1);
                    }
                    _ => unreachable!(),
                }

                self.advance(1);
            } else {
                let index = memchr::memchr2(b'\\', b'"', self.data).unwrap_or(self.data.len());
                output.extend_from_slice(self.parse_bytes(index));

                match *self.data {
                    [] => break 'outer,
                    [b'"', ..] => state = State::Unquoted,
                    [b'\\'] => output.push(b'\\'),
                    [b'\\', c, ..] => {
                        output.push(c);
                        self.advance(1);
                    }
                    _ => unreachable!(),
                }
                self.advance(1);
            }
        }

        if !self.data.is_empty() {
            debug_assert_eq!(self.data[0], b' ');
            self.advance(1);
        }

        Cow::Owned(output)
    }

    pub fn parse_next(&mut self) -> Option<Cow<'a, [u8]>> {
        if self.data.is_empty() {
            return None;
        }

        Some(self.parse_bytes_escaped())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bstr::BStr;

    macro_rules! parse {
        ($text:expr, $expected:expr) => {{
            let mut parser = InlineParser::new($text.as_ref());
            let result = parser.parse_next();
            let expected: Option<&[u8]> = $expected;

            assert_eq!(
                result.as_deref(),
                expected,
                "{:?} != {:?}",
                result.as_deref().map(BStr::new),
                expected.map(BStr::new)
            );
        }};
    }

    #[test]
    fn inline_parser_test() {
        parse!("aaaaaa", Some(b"aaaaaa"));
        parse!(" aa", Some(b""));
        parse!("\"aa \\\" b\"ccd a", Some(b"aa \" bccd"));
    }

    #[test]
    fn parse_quoted() {
        parse!("\"a \r\n\" b", Some(b"a \r\n"));
    }

    #[test]
    fn quote_escaped() {
        parse!("\"\\\"\" a", Some(b"\""));
        parse!("\\\"", Some(b"\""));
    }
}
