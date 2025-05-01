use crate::binary::response::header::ResponseStatus;

use super::*;

impl BinaryProtocol {
    pub(crate) fn parse_get_response<'a>(
        &self,
        request: &Get,
        input: &'a [u8],
        header: ResponseHeader,
    ) -> IResult<&'a [u8], Response> {
        self._parse_get_response(request, input, header)
    }

    fn _parse_get_response<'a>(
        &self,
        request: &Get,
        input: &'a [u8],
        header: ResponseHeader,
    ) -> IResult<&'a [u8], Response> {
        match header.status {
            ResponseStatus::NoError => {
                if header.total_body_len > 0 {
                    Err(nom::Err::Failure(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Tag,
                    )))
                } else {
                    Ok((input, Response::not_found(false)))
                }
            }
            ResponseStatus::KeyNotFound => {
                if header.total_body_len < 5 || header.key_len != 0 {
                    Err(nom::Err::Failure(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Tag,
                    )))
                } else {
                    let (input, flags) = take(4usize)(input)?;
                    let flags = u32::from_be_bytes([flags[0], flags[1], flags[2], flags[3]]);

                    let (input, value) = take(header.total_body_len as usize - 4)(input)?;

                    Ok((input, Response::found(&request.keys[0], flags, None, value)))
                }
            }
            _ => Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            ))),
        }
    }

    pub(crate) fn compose_get_response(
        &self,
        request: &Get,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_get_response(request, response, buffer)
    }

    fn _compose_get_response(
        &self,
        request: &Get,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        match response {
            Response::Values(values) => {
                const EXTRAS_LEN: u8 = 4;
                let key_len = if !request.key {
                    0
                } else {
                    values.values[0].key().len() as u16
                };
                let total_body_len = EXTRAS_LEN as usize
                    + key_len as usize
                    + values.values[0].value().map(|v| v.len()).unwrap_or(0);

                ResponseHeader {
                    magic: MagicValue::Response,
                    opcode: Opcode::Get,
                    key_len,
                    extras_len: EXTRAS_LEN,
                    data_type: 0x00,
                    status: ResponseStatus::NoError,
                    total_body_len: total_body_len as u32,
                    opaque: request.opaque.unwrap_or(0),
                    cas: values.values[0].cas.unwrap_or(0),
                }
                .write_to(buffer);

                // EXTRAS_LEN
                buffer.put_u32(values.values[0].flags);

                if request.key {
                    buffer.put_slice(values.values[0].key());
                }

                if let Some(value) = values.values[0].value() {
                    buffer.put_slice(value);
                }

                Ok(24 + total_body_len)
            }
            Response::NotFound(_) => {
                ResponseStatus::KeyNotFound
                    .as_empty_response(Opcode::Get)
                    .write_to(buffer);
                Ok(24)
            }
            Response::Error(error) => Ok(error.write_binary_response(Opcode::Get, buffer)),
            Response::ClientError(client_error) => {
                Ok(client_error.write_binary_response(Opcode::Get, buffer))
            }
            Response::ServerError(server_error) => {
                Ok(server_error.write_binary_response(Opcode::Get, buffer))
            }
            Response::Stored(_stored) => Ok(response::ServerError {
                inner: "unknown response: STORED".to_string(),
            }
            .write_binary_response(Opcode::Get, buffer)),
            Response::NotStored(_not_stored) => Ok(response::ServerError {
                inner: "unknown response: NOT_STORED".to_string(),
            }
            .write_binary_response(Opcode::Get, buffer)),
            Response::Exists(_exists) => Ok(response::ServerError {
                inner: "unknown response: EXISTS".to_string(),
            }
            .write_binary_response(Opcode::Get, buffer)),
            Response::Numeric(_numeric) => Ok(response::ServerError {
                inner: "unknown response: NUMERIC".to_string(),
            }
            .write_binary_response(Opcode::Get, buffer)),
            Response::Deleted(_deleted) => Ok(response::ServerError {
                inner: "unknown response: DELETED".to_string(),
            }
            .write_binary_response(Opcode::Get, buffer)),
            Response::Hangup => Ok(response::ServerError {
                inner: "HANGUP".to_string(),
            }
            .write_binary_response(Opcode::Get, buffer)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn compose_response_hit() {
        let request = Request::Get(Get {
            keys: vec!["Hello".as_bytes().into()].into(),
            opaque: Some(0),
            cas: true,
            key: false,
        });
        let response = Response::found("Hello".as_bytes(), 0, Some(0), "World".as_bytes());

        let mut buffer = BytesMut::new();

        let protocol = BinaryProtocol::default();

        let _ = protocol.compose_response(&request, &response, &mut buffer);

        assert_eq!(
            &*buffer,
            &[
                0x81, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x57, 0x6f, 0x72, 0x6c, 0x64
            ]
        );
    }
}
