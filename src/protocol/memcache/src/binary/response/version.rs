use super::{header::ResponseStatus, *};

impl BinaryProtocol {
    pub(crate) fn parse_version_response<'a>(
        &self,
        _request: &Version,
        input: &'a [u8],
        header: ResponseHeader,
    ) -> IResult<&'a [u8], Response> {
        match header.status {
            ResponseStatus::NoError => {
                if header.key_len != 0 || header.extras_len != 0 {
                    return Err(nom::Err::Failure(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Tag,
                    )));
                }

                let (input, version) = take(header.total_body_len as usize)(input)?;

                Ok((input, Response::version(String::from_utf8_lossy(version))))
            }
            _ => Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            ))),
        }
    }

    pub(crate) fn compose_version_response(
        &self,
        request: &Version,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        match response {
            Response::Version(version) => {
                let body = version.inner.as_bytes();

                ResponseHeader {
                    magic: MagicValue::Response,
                    opcode: Opcode::Version,
                    key_len: 0,
                    extras_len: 0,
                    data_type: 0x00,
                    status: ResponseStatus::NoError,
                    total_body_len: body.len() as u32,
                    opaque: request.opaque.unwrap_or(0),
                    cas: 0,
                }
                .write_to(buffer);

                buffer.put_slice(body);

                Ok(24 + body.len())
            }
            other => Ok(response::ServerError {
                inner: format!("unknown response: {other}"),
            }
            .write_binary_response(Opcode::Version, buffer)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn compose_response() {
        let protocol = BinaryProtocol::default();
        let request = Request::Version(Version {
            opaque: Some(0x00000001),
        });
        let response = Response::version("0.3.2");

        let mut buffer = BytesMut::new();
        let _ = protocol.compose_response(&request, &response, &mut buffer);

        assert_eq!(
            &*buffer,
            &[
                // header
                0x81, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
                0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // body: "0.3.2"
                0x30, 0x2e, 0x33, 0x2e, 0x32,
            ]
        );
    }

    #[test]
    fn round_trip() {
        let protocol = BinaryProtocol::default();
        let request = Request::Version(Version {
            opaque: Some(0x00000001),
        });
        let response = Response::version("0.3.2");

        let mut buffer = BytesMut::new();
        let _ = protocol.compose_response(&request, &response, &mut buffer);

        let parsed = protocol.parse_response(&request, &buffer).unwrap();
        assert_eq!(parsed.into_inner(), Response::version("0.3.2"));
    }
}
