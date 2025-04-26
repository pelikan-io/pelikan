//! A submodule containing the relevant parts for the Memcache binary protocol.
//! Since it's just a different encoding of the same logical requests and
//! responses, we reuse many of the components from the text-bases protocol,
//! such as the concrete request and response types.

use crate::binary::response::ResponseHeader;
use crate::binary::request::RequestHeader;
use protocol_common::BufMut;
use protocol_common::Protocol;
use crate::*;

pub mod request;
pub mod response;

pub struct BinaryProtocol {
	max_value_size: u32,
    max_key_len: u16,
}

impl Default for BinaryProtocol {
	fn default() -> Self {
		Self {
			max_value_size: u32::MAX,
			max_key_len: u16::MAX,
		}
	}
}

impl BinaryProtocol {
	fn _parse_request<'a>(&self, buffer: &'a [u8]) -> IResult<&'a [u8], Request> {
		let (input, header) = RequestHeader::parse(buffer)?;

    	// impose a constraint on the total body length based on the max
    	// sizes for key, value, and extra data
    	if header.total_body_len as usize > self.max_key_len as usize + self.max_value_size as usize + 32 {
    		return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
    	}

    	match header.opcode {
    		0x00 => {
    			let (input, request) = self.parse_get_request(input, header)?;
    			Ok((input, Request::Get(request)))
    		}
    		0x01 => {
    			let (input, request) = self.parse_set_request(input, header)?;
    			Ok((input, Request::Set(request)))
    		}
    		0x04 => {
    			let (input, request) = self.parse_delete_request(input, header)?;
    			Ok((input, Request::Delete(request)))
    		}
    		_ => {
    			Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )))
    		}
    	}
	}

	fn _compose_request(&self, request: &Request, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
		match request {
			Request::Delete(r) => {
				self.compose_delete_request(r, buffer)
			}
			Request::Get(r) => {
				self.compose_get_request(r, buffer)
			}
			Request::Set(r) => {
				self.compose_set_request(r, buffer)
			}
			_ => {
				Err(std::io::Error::new(std::io::ErrorKind::Other, "request unsupported for binary protocol"))
			}
		}
	}

	fn _parse_response<'a>(&self, request: &Request, buffer: &'a [u8]) -> IResult<&'a [u8], Response> {
		let (input, header) = ResponseHeader::parse(buffer)?;

		if header.magic != 0x81 {
			return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
		}

    	// impose a constraint on the total body length based on the max
    	// sizes for key, value, and extra data
    	if header.total_body_len as usize > self.max_key_len as usize + self.max_value_size as usize + 32 {
    		return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
    	}

    	match request {
    		Request::Delete(request) => {
    			if header.opcode == 0x05 {
    				let (input, response) = self.parse_delete_response(request, input, header)?;
    				return Ok((input, response));
    			}
    		}
    		Request::Get(request) => {
    			if header.opcode == 0x00 {
    				let (input, response) = self.parse_get_response(request, input, header)?;
    				return Ok((input, response));
    			}
    		}
    		Request::Set(request) => {
    			if header.opcode == 0x01 {
    				let (input, response) = self.parse_set_response(request, input, header)?;
    				return Ok((input, response));
    			}
    		}
    		_ => {}
    	}

    	Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )))
	}

	fn _compose_response(&self, request: &Request, response: &Response, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
		match request {
			Request::Delete(request) => self.compose_delete_response(request, response, buffer),
			Request::Get(request) => self.compose_get_response(request, response, buffer),
			Request::Set(request) => self.compose_set_response(request, response, buffer),
			_ => {
				unimplemented!()
			}
		}
	}
}

impl Protocol<Request, Response> for BinaryProtocol {
	fn parse_request(&self, buffer: &[u8]) -> std::result::Result<ParseOk<Request>, std::io::Error> {
		match self._parse_request(buffer) {
            Ok((input, request)) => Ok(ParseOk::new(request, buffer.len() - input.len())),
            Err(Err::Incomplete(_)) => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
            Err(_) => Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
        }
	}

	fn compose_request(&self, request: &Request, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
		self._compose_request(request, buffer)
	}

	fn parse_response(&self, request: &Request, buffer: &[u8]) -> std::result::Result<ParseOk<Response>, std::io::Error> {
		match self._parse_response(request, buffer) {
            Ok((input, request)) => Ok(ParseOk::new(request, buffer.len() - input.len())),
            Err(Err::Incomplete(_)) => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
            Err(_) => Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
        }
	}

	fn compose_response(&self, request: &Request, response: &Response, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
		self._compose_response(request, response, buffer)
	}
}

pub(crate) fn is_key_valid(key: &[u8]) -> bool {
	for i in 0..key.len() {
    	if key[i] == 32 {
    		return false;
    	}

    	if key[i] == 13 {
    		if let Some(10) = key.get(i + 1) {
    			return false;
    		}
    	}
    }

    true
}

#[cfg(test)]
mod tests {
	use protocol_common::Protocol;
	use crate::Request;
	use crate::binary::BinaryProtocol;

	#[test]
	fn fuzz_1() {
		let protocol = BinaryProtocol::default();

		let buffer = [128, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 1, 0, 249, 0, 0, 245, 138, 121, 120, 255, 65, 255, 255];

		match protocol.parse_request(&buffer).map(|v| (v.consumed(), v.into_inner())) {
			Ok((consumed, Request::Get(get))) => {
				assert_eq!(consumed, buffer.len());
				assert_eq!(get.keys.len(), 1);
				assert_eq!(&*get.keys[0], &[255]);
			}
			_ => {
				panic!("wrong request type");
			}
		}
	}
}