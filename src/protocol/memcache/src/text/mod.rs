//! A submodule containing the relevant parts for the Memcache binary protocol.
//! Since it's just a different encoding of the same logical requests and
//! responses, we reuse many of the components from the text-bases protocol,
//! such as the concrete request and response types.

use crate::text::request::TextProtocolRequest;
use protocol_common::BufMut;
use protocol_common::Protocol;
use crate::*;

pub mod request;
pub mod response;

#[repr(C)]
pub(crate) struct Header {
	magic: u8,
	opcode: u8,
	key_len: u16,
	extras_len: u8,
	data_type: u8,
	_reserved: u16,
	total_body_len: u32,
	opaque: u32,
	cas: u64,
}

impl Header {
	pub(crate) fn parse(input: &[u8]) -> IResult<&[u8], Self> {
    	let (remaining, h) = take(24usize)(input)?;

    	let header = Header {
    		magic: h[0],
    		opcode: h[1],
    		key_len: u16::from_be_bytes([h[2], h[3]]),
    		extras_len: h[4],
    		data_type: h[5],
    		_reserved: u16::from_be_bytes([h[6], h[7]]),
    		total_body_len: u32::from_be_bytes([h[8], h[9], h[10], h[11]]),
    		opaque: u32::from_be_bytes([h[12], h[13], h[14], h[15]]),
    		cas: u64::from_be_bytes([h[16], h[17], h[18], h[19], h[20], h[21], h[22], h[23]]),
    	};

    	if header.magic != 0x80 {
    		return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
    	}

    	if header.data_type != 0x00 {
    		return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
    	}

        Ok((remaining, header))
    }
}

pub struct BinaryProtocol {
	request: BinaryProtocolRequest,
	response: (),
}

impl Protocol<Request, Response> for BinaryProtocol {
	fn parse_request(&self, buffer: &[u8]) -> std::result::Result<ParseOk<Request>, std::io::Error> {
		self.request.parse(buffer)
	}

	fn compose_request(&self, request: &Request, buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> {
		self.request.compose(request, buffer)
	}

	fn parse_response(&self, _request: &Request, _buffer: &[u8]) -> std::result::Result<ParseOk<Response>, std::io::Error> { todo!() }

	fn compose_response(&self, _request: &Request, _response: &Response, _buffer: &mut dyn BufMut) -> std::result::Result<usize, std::io::Error> { todo!() }
}