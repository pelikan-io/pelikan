//! A submodule containing the relevant parts for the Memcache binary protocol.
//! Since it's just a different encoding of the same logical requests and
//! responses, we reuse many of the components from the text-bases protocol,
//! such as the concrete request and response types.

use crate::binary::request::RequestHeader;
use crate::*;
use protocol_common::BufMut;
use protocol_common::Protocol;
use response::header::ResponseHeader;

pub mod request;
pub mod response;

#[derive(Clone)]
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
        if header.total_body_len as usize
            > self.max_key_len as usize + self.max_value_size as usize + 32
        {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        match header.opcode {
            Opcode::Get => {
                let (input, request) = self.parse_get_request(input, header)?;
                Ok((input, Request::Get(request)))
            }
            Opcode::Set => {
                let (input, request) = self.parse_set_request(input, header)?;
                Ok((input, Request::Set(request)))
            }
            Opcode::Delete => {
                let (input, request) = self.parse_delete_request(input, header)?;
                Ok((input, Request::Delete(request)))
            }
            _ => Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            ))),
        }
    }

    fn _compose_request(
        &self,
        request: &Request,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        match request {
            Request::Delete(r) => self.compose_delete_request(r, buffer),
            Request::Get(r) => self.compose_get_request(r, buffer),
            Request::Set(r) => self.compose_set_request(r, buffer),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "request unsupported for binary protocol",
            )),
        }
    }

    fn _parse_response<'a>(
        &self,
        request: &Request,
        buffer: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        let (input, header) = ResponseHeader::parse(buffer)?;

        if header.magic != MagicValue::Response {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        // impose a constraint on the total body length based on the max
        // sizes for key, value, and extra data
        if header.total_body_len as usize
            > self.max_key_len as usize + self.max_value_size as usize + 32
        {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        match request {
            Request::Delete(request) => {
                if header.opcode == Opcode::Delete {
                    let (input, response) = self.parse_delete_response(request, input, header)?;
                    return Ok((input, response));
                }
            }
            Request::Get(request) => {
                if header.opcode == Opcode::Get {
                    let (input, response) = self.parse_get_response(request, input, header)?;
                    return Ok((input, response));
                }
            }
            Request::Set(request) => {
                if header.opcode == Opcode::Set {
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

    fn _compose_response(
        &self,
        request: &Request,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
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
    fn parse_request(
        &self,
        buffer: &[u8],
    ) -> std::result::Result<ParseOk<Request>, std::io::Error> {
        match self._parse_request(buffer) {
            Ok((input, request)) => Ok(ParseOk::new(request, buffer.len() - input.len())),
            Err(Err::Incomplete(_)) => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
            Err(_) => Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
        }
    }

    fn compose_request(
        &self,
        request: &Request,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_request(request, buffer)
    }

    fn parse_response(
        &self,
        request: &Request,
        buffer: &[u8],
    ) -> std::result::Result<ParseOk<Response>, std::io::Error> {
        match self._parse_response(request, buffer) {
            Ok((input, request)) => Ok(ParseOk::new(request, buffer.len() - input.len())),
            Err(Err::Incomplete(_)) => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
            Err(_) => Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
        }
    }

    fn compose_response(
        &self,
        request: &Request,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MagicValue {
    Unknown(u8),
    Request,
    Response,
}

impl MagicValue {
    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            0x80 => MagicValue::Request,
            0x81 => MagicValue::Response,
            other => MagicValue::Unknown(other),
        }
    }

    pub(crate) fn to_u8(self) -> u8 {
        match self {
            MagicValue::Unknown(other) => other,
            MagicValue::Request => 0x80,
            MagicValue::Response => 0x81,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opcode {
    Unknown(u8),
    Get,
    Set,
    Delete,
}

impl Opcode {
    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            0x00 => Opcode::Get,
            0x01 => Opcode::Set,
            0x04 => Opcode::Delete,
            other => Opcode::Unknown(other),
        }
    }

    pub(crate) fn to_u8(self) -> u8 {
        match self {
            Opcode::Unknown(other) => other,
            Opcode::Get => 0x00,
            Opcode::Set => 0x01,
            Opcode::Delete => 0x04,
        }
    }
}

