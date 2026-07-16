// Copyright 2026 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

impl TextProtocol {
    pub(crate) fn parse_version_response<'a>(
        &self,
        _request: &Version,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[allow(unused_variables)]
    pub(crate) fn compose_version_response(
        &self,
        request: &Version,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        Ok(response.compose(buffer))
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use protocol_common::Protocol;

    #[test]
    fn compose() {
        let protocol = TextProtocol::new();
        let request = Request::Version(Version { opaque: None });
        let response = Response::version("0.3.2");

        let mut buffer = Vec::new();
        let _ = protocol.compose_response(&request, &response, &mut buffer);

        assert_eq!(&buffer[..], b"VERSION 0.3.2\r\n");
    }

    #[test]
    fn parse() {
        assert_eq!(
            crate::response(b"VERSION 0.3.2\r\n"),
            Ok((&b""[..], Response::version("0.3.2")))
        );
    }
}
