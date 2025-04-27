// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_common::BufMut;

impl TextProtocol {
    // this is to be called after parsing the command, so we do not match the verb
    pub fn parse_quit_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Quit> {
        let (input, _) = space0(input)?;
        let (input, _) = crlf(input)?;

        #[cfg(feature = "metrics")]
        QUIT.increment();

        Ok((input, Quit {}))
    }

    pub(crate) fn _compose_quit_request(&self, session: &mut dyn BufMut) -> usize {
        session.put_slice(b"quit\r\n");
        6
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let protocol = TextProtocol::new();

        // quit command
        assert_eq!(
            protocol._parse_request(b"quit\r\n"),
            Ok((&b""[..], Request::Quit(Quit {})))
        );
    }
}
