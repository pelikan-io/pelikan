use super::{header::ResponseStatus, *};

impl BinaryProtocol {
    pub(crate) fn parse_delete_response<'a>(
        &self,
        request: &Delete,
        input: &'a [u8],
        header: ResponseHeader,
    ) -> IResult<&'a [u8], Response> {
        self._parse_delete_response(request, input, header)
    }

    fn _parse_delete_response<'a>(
        &self,
        request: &Delete,
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

    pub(crate) fn compose_delete_response(
        &self,
        request: &Delete,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_delete_response(request, response, buffer)
    }

    fn _compose_delete_response(
        &self,
        request: &Delete,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        match response {
            Response::Deleted(_) => {
                ResponseHeader {
                    magic: MagicValue::Response,
                    opcode: Opcode::Delete,
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
            Response::NotFound(_) => {
                ResponseHeader {
                    magic: MagicValue::Response,
                    opcode: Opcode::Delete,
                    key_len: 0,
                    extras_len: 0,
                    data_type: 0x00,
                    status: ResponseStatus::KeyNotFound,
                    total_body_len: 0,
                    opaque: request.opaque.unwrap_or(0),
                    cas: 0,
                }
                .write_to(buffer);

                Ok(24)
            }
            other => Ok(response::ServerError {
                inner: format!("unknown response: {other}"),
            }
            .write_binary_response(Opcode::Get, buffer))
        }
    }
}
