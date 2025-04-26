use super::*;

impl BinaryProtocol {
	pub(crate) fn parse_delete_response<'a>(&self, request: &Delete, input: &'a [u8], header: ResponseHeader) -> IResult<&'a [u8], Response> {
        self._parse_delete_response(request, input, header)
    }

	fn _parse_delete_response<'a>(&self, request: &Delete, input: &'a [u8], header: ResponseHeader) -> IResult<&'a [u8], Response> {
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

    pub(crate) fn compose_delete_response(&self, request: &Delete, response: &Response, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
    	self._compose_delete_response(request, response, buffer)
    }

    fn _compose_delete_response(&self, _request: &Delete, response: &Response, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
        match response {
            Response::Deleted(_) => {
                buffer.put_slice(&[0x81, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
            }
            Response::NotFound(_) => {
                buffer.put_slice(&[0x81, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
            }
            _ => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "unexpected response"));
            }
        }

		Ok(24)
        
    }
}