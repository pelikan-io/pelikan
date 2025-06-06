// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This module handles parsing of the wire representation of a `Ping` request
//! into a request object.

use super::super::*;
use crate::*;

use core::convert::TryFrom;
use core::slice::Windows;

struct ParseState<'a> {
    single_byte: Windows<'a, u8>,
    double_byte: Windows<'a, u8>,
}

impl<'a> ParseState<'a> {
    fn new(buffer: &'a [u8]) -> Self {
        let single_byte = buffer.windows(1);
        let double_byte = buffer.windows(2);
        Self {
            single_byte,
            double_byte,
        }
    }

    fn next_space(&mut self) -> Option<usize> {
        self.single_byte.position(|w| w == b" ")
    }

    fn next_crlf(&mut self) -> Option<usize> {
        self.double_byte.position(|w| w == CRLF.as_bytes())
    }
}

pub(crate) fn parse_keyword(buffer: &[u8]) -> Result<Keyword, std::io::Error> {
    let command;
    {
        let mut parse_state = ParseState::new(buffer);
        if let Some(line_end) = parse_state.next_crlf() {
            if let Some(cmd_end) = parse_state.next_space() {
                command = Keyword::try_from(&buffer[0..cmd_end])?;
            } else {
                command = Keyword::try_from(&buffer[0..line_end])?;
            }
        } else {
            return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
        }
    }
    Ok(command)
}

#[allow(clippy::unnecessary_wraps)]
pub(crate) fn parse_ping(buffer: &[u8]) -> Result<ParseOk<Request>, std::io::Error> {
    let mut parse_state = ParseState::new(buffer);

    // this was already checked for when determining the command
    let line_end = parse_state.next_crlf().unwrap();

    let consumed = line_end + CRLF.len();

    let message = Request::Ping;

    #[cfg(feature = "server")]
    PING.increment();

    Ok(ParseOk::new(message, consumed))
}
