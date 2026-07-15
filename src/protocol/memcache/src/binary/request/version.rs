use super::*;

impl BinaryProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_version_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Version> {
        VERSION.increment();
        self._parse_version_request(input, header)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_version_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Version> {
        self._parse_version_request(input, header)
    }

    fn _parse_version_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Version> {
        // version takes no key, value, or extras
        if header.key_len != 0 || header.extras_len != 0 || header.total_body_len != 0 {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        Ok((
            input,
            Version {
                opaque: Some(header.opaque),
            },
        ))
    }

    pub(crate) fn compose_version_request(
        &self,
        request: &Version,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        let mut header = RequestHeader::version();
        header.opaque = request.opaque.unwrap_or(0);
        header.write_to(buffer);
        Ok(header.request_len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn parse() {
        let protocol = BinaryProtocol::default();

        // magic=0x80 request, opcode=0x0b version, opaque=0x01020304, rest zero
        let buffer = [
            0x80, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
            0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_eq!(
            protocol._parse_request(&buffer),
            Ok((
                &b""[..],
                Request::Version(Version {
                    opaque: Some(0x01020304)
                })
            ))
        );
    }

    #[test]
    fn compose_request() {
        let protocol = BinaryProtocol::default();
        let request = Request::Version(Version {
            opaque: Some(0x01020304),
        });

        let mut buffer = BytesMut::new();
        let _ = protocol.compose_request(&request, &mut buffer);

        assert_eq!(
            &*buffer,
            &[
                0x80, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
                0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ]
        );
    }
}
