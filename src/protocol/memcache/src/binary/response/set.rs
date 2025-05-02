use super::{header::ResponseStatus, *};

impl BinaryProtocol {
    pub(crate) fn parse_set_response<'a>(
        &self,
        request: &Set,
        input: &'a [u8],
        header: ResponseHeader,
    ) -> IResult<&'a [u8], Response> {
        self._parse_set_response(request, input, header)
    }

    fn _parse_set_response<'a>(
        &self,
        request: &Set,
        input: &'a [u8],
        header: ResponseHeader,
    ) -> IResult<&'a [u8], Response> {
        match header.status {
            ResponseStatus::NoError => Ok((input, Response::stored(request.noreply))),
            ResponseStatus::ItemNotStored => Ok((input, Response::not_stored(request.noreply))),
            _ => Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            ))),
        }
    }

    pub(crate) fn compose_set_response(
        &self,
        request: &Set,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_set_response(request, response, buffer)
    }

    fn _compose_set_response(
        &self,
        request: &Set,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        match response {
            Response::Stored(_) => {
                ResponseHeader {
                    magic: MagicValue::Response,
                    opcode: Opcode::Set,
                    key_len: 0,
                    extras_len: 0,
                    data_type: 0x00,
                    status: ResponseStatus::NoError,
                    total_body_len: 0,
                    opaque: request.opaque.unwrap_or(0),
                    cas: 0,
                }
                .write_to(buffer);

                Ok(24)
            }
            Response::NotStored(_) => {
                ResponseHeader {
                    magic: MagicValue::Response,
                    opcode: Opcode::Set,
                    key_len: 0,
                    extras_len: 0,
                    data_type: 0x00,
                    status: ResponseStatus::ItemNotStored,
                    total_body_len: 0,
                    opaque: request.opaque.unwrap_or(0),
                    cas: 0,
                }
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
            other => Ok(response::ServerError {
                inner: format!("unknown response: {other}"),
            }
            .write_binary_response(Opcode::Get, buffer))
        }
    }
}
