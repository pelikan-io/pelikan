// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::binary::Opcode;
use crate::binary::response::header::{ResponseStatus};

use super::*;

const BINARY_MESSAGE: &[u8] = b"ERROR";
const TEXT_MESSAGE: &[u8] = b"ERROR\r\n";

#[derive(Debug, PartialEq, Eq)]
pub struct Error {}

impl Default for Error {
    fn default() -> Self {
        Self::new()
    }
}

impl Error {
    pub fn new() -> Self {
        Self {}
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn len(&self) -> usize {
        TEXT_MESSAGE.len()
    }

    pub fn write_binary_response(&self, opcode: Opcode, buffer: &mut dyn BufMut) -> usize {
        let mut header = ResponseStatus::InternalError.as_empty_response(opcode);
        header.total_body_len = BINARY_MESSAGE.len() as u32;
        header.write_to(buffer);
        buffer.put_slice(BINARY_MESSAGE);
        24 + BINARY_MESSAGE.len()
    }
}

impl Compose for Error {
    fn compose(&self, session: &mut dyn BufMut) -> usize {
        session.put_slice(TEXT_MESSAGE);
        TEXT_MESSAGE.len()
    }
}

pub fn parse(input: &[u8]) -> IResult<&[u8], Error> {
    let (input, _) = space0(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Error {}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(response(b"ERROR\r\n"), Ok((&b""[..], Response::error(),)));

        assert_eq!(response(b"ERROR \r\n"), Ok((&b""[..], Response::error(),)));
    }
}
