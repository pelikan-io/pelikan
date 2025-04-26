use super::*;

impl BinaryProtocol {
	pub(crate) fn parse_get_response<'a>(&self, request: &Get, input: &'a [u8], header: ResponseHeader) -> IResult<&'a [u8], Response> {
        self._parse_get_response(request, input, header)
    }

	fn _parse_get_response<'a>(&self, request: &Get, input: &'a [u8], header: ResponseHeader) -> IResult<&'a [u8], Response> {
         match header.status {
            0 => {
                if header.total_body_len > 0 {
                    Err(nom::Err::Failure(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Tag,
                    )))
                } else {
                    Ok((input, Response::not_found(false)))
                }
            }
            1 => {
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
            _ => {
                Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )))
            }
        }
    }

    pub(crate) fn compose_get_response(&self, request: &Get, response: &Response, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
    	self._compose_get_response(request, response, buffer)
    }

    fn _compose_get_response(&self, request: &Get, response: &Response, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
        match response {
            Response::Values(values) => {
                buffer.put_slice(&[0x81, 0x00]);
                if request.key {
                    buffer.put_slice(&[0x00, 0x00]);
                } else {
                    buffer.put_u16(values.values[0].key().len() as u16);
                }
                buffer.put_slice(&[0x04, 0x00, 0x00, 0x00]);

                let total_body_len = values.values[0].key().len() + values.values[0].value().map(|v| v.len()).unwrap_or(0) + 4;

                buffer.put_u32(total_body_len as _);
                buffer.put_u32(request.opaque.unwrap_or(0));
                buffer.put_u64(values.values[0].cas.unwrap_or(0));
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
                buffer.put_slice(&[0x81, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

                Ok(24)
            }
            _ => {
                Err(std::io::Error::new(std::io::ErrorKind::Other, "unexpected response"))
            }
        }
    }
}