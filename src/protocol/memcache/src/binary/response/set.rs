use super::*;

impl BinaryProtocol {
	pub(crate) fn parse_set_response<'a>(&self, request: &Set, input: &'a [u8], header: ResponseHeader) -> IResult<&'a [u8], Response> {
        self._parse_set_response(request, input, header)
    }

	fn _parse_set_response<'a>(&self, request: &Set, input: &'a [u8], header: ResponseHeader) -> IResult<&'a [u8], Response> {
        match header.status {
            0 => {
                Ok((input, Response::stored(request.noreply)))
            }
            5 => {
                Ok((input, Response::not_stored(request.noreply)))
            }
            _ => {
                Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )))
            }
        }
    }

    pub(crate) fn compose_set_response(&self, request: &Set, response: &Response, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
    	self._compose_set_response(request, response, buffer)
    }

    fn _compose_set_response(&self, _request: &Set, response: &Response, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
        match response {
            Response::Stored(_) => {
                buffer.put_slice(&[0x81, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
            }
            Response::NotStored(_) => {
                buffer.put_slice(&[0x81, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
            }
            _ => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "unexpected response"));
            }
        }

		Ok(24)
    }
}