use std::str::FromStr;

use super::{ArrayParser, ParseError, ParseResult, Parser};

enum Detail<'a, 'p> {
    Inline { data: &'a [u8], remaining: usize },
    Resp(ArrayParser<'a, 'p>),
}

pub struct CommandParser<'a, 'p> {
    detail: Detail<'a, 'p>,
}

impl<'a, 'p> CommandParser<'a, 'p> {
    pub fn new(parser: &'p mut Parser<'a>) -> ParseResult<'a, Self> {
        match parser.peek() {
            Some(b'+' | b'-' | b':' | b'$' | b'*') => Ok(Self {
                detail: Detail::Resp(parser.parse_array()?.ok_or_else(|| ParseError::Custom {
                    expected: "a non-nil array",
                    found: "a nil array",
                })?),
            }),
            Some(_) => {
                let data = parser.parse_delimited_text()?;
                let count = memchr::memchr_iter(b' ', data).count() + 1;

                Ok(Self {
                    detail: Detail::Inline {
                        data,
                        remaining: count,
                    },
                })
            }
            None => Err(ParseError::Incomplete),
        }
    }

    pub fn remaining(&self) -> usize {
        match &self.detail {
            Detail::Resp(ap) => ap.remaining(),
            Detail::Inline { remaining, .. } => *remaining,
        }
    }

    pub fn parse_string(&mut self) -> ParseResult<'a, Option<&'a [u8]>> {
        match &mut self.detail {
            Detail::Resp(p) => p.parse_string(),
            Detail::Inline { data, remaining } => {
                if data.is_empty() {
                    debug_assert_eq!(*remaining, 0);
                    return Err(ParseError::ExpectedArrayElement);
                }

                let (text, rest) = match memchr::memchr(b' ', *data) {
                    Some(offset) => {
                        let (head, rest) = data.split_at(offset);
                        assert!(!rest.is_empty());
                        (head, &rest[1..])
                    }
                    // Note: We use &data[data.len()..] to ensure that data.as_ptr() still works
                    //       properly for computing offsets and the like.
                    None => (*data, &data[data.len()..]),
                };

                *data = rest;
                *remaining -= 1;
                Ok(Some(text))
            }
        }
    }

    pub fn parse_string_nonnil(&mut self) -> ParseResult<'a, &'a [u8]> {
        self.parse_string()?.ok_or(ParseError::UnexpectedNilString)
    }

    pub fn parse_u64(&mut self) -> ParseResult<'a, u64> {
        let text = self.parse_string_nonnil()?;

        let text = std::str::from_utf8(text).map_err(|_| ParseError::InvalidNumber(text))?;
        text.parse()
            .map_err(|_| match <i64 as FromStr>::from_str(text) {
                Ok(_) => ParseError::UnexpectedNegativeNumber(text.as_bytes()),
                Err(_) => ParseError::InvalidNumber(text.as_bytes()),
            })
    }

    pub fn parse_i64(&mut self) -> ParseResult<'a, u64> {
        let text = self.parse_string_nonnil()?;

        std::str::from_utf8(text)
            .map_err(|_| ParseError::InvalidNumber(text))?
            .parse()
            .map_err(|_| ParseError::InvalidNumber(text))
    }

    pub fn finish(self) -> ParseResult<'a, ()> {
        match self.detail {
            Detail::Resp(ap) => ap.finish(),
            Detail::Inline { remaining: 0, data } => {
                debug_assert!(data.is_empty());
                Ok(())
            }
            Detail::Inline { .. } => Err(ParseError::UnexpectedArrayElement),
        }
    }
}
