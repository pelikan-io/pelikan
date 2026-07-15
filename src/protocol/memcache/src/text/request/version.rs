// Copyright 2026 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_common::BufMut;

impl TextProtocol {
    // this is to be called after parsing the command, so we do not match the verb
    pub fn parse_version_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Version> {
        let (input, _) = space0(input)?;
        let (input, _) = crlf(input)?;

        #[cfg(feature = "metrics")]
        VERSION.increment();

        Ok((input, Version { opaque: None }))
    }

    pub(crate) fn _compose_version_request(&self, session: &mut dyn BufMut) -> usize {
        session.put_slice(b"version\r\n");
        9
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let protocol = TextProtocol::new();

        // version command
        assert_eq!(
            protocol._parse_request(b"version\r\n"),
            Ok((&b""[..], Request::Version(Version { opaque: None })))
        );
    }
}
