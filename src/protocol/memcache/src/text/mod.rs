//! A submodule containing the relevant parts for the Memcache binary protocol.
//! Since it's just a different encoding of the same logical requests and
//! responses, we reuse many of the components from the text-bases protocol,
//! such as the concrete request and response types.

use crate::*;
use protocol_common::BufMut;

pub mod request;
pub mod response;

#[derive(Clone)]
pub struct TextProtocol {
    max_value_size: usize,
    max_batch_size: usize,
    max_key_len: usize,
    time_type: TimeType,
}

impl Default for TextProtocol {
    fn default() -> Self {
        Self {
            max_value_size: DEFAULT_MAX_VALUE_SIZE,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_key_len: DEFAULT_MAX_KEY_LEN,
            time_type: TimeType::Memcache,
        }
    }
}

impl TextProtocol {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn time_type(mut self, time_type: TimeType) -> Self {
        self.time_type = time_type;
        self
    }

    pub fn max_value_size(mut self, bytes: usize) -> Self {
        self.max_value_size = bytes;
        self
    }

    pub fn max_key_len(mut self, bytes: usize) -> Self {
        self.max_key_len = bytes;
        self
    }

    pub fn max_batch_size(mut self, count: usize) -> Self {
        self.max_batch_size = count;
        self
    }

    fn parse_command<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Command> {
        let (remaining, command_bytes) = take_till(|b| (b == b' ' || b == b'\r'))(input)?;
        let command = match command_bytes {
            b"add" | b"ADD" => Command::Add,
            b"append" | b"APPEND" => Command::Append,
            b"cas" | b"CAS" => Command::Cas,
            b"decr" | b"DECR" => Command::Decr,
            b"delete" | b"DELETE" => Command::Delete,
            b"flush_all" | b"FLUSH_ALL" => Command::FlushAll,
            b"incr" | b"INCR" => Command::Incr,
            b"get" | b"GET" => Command::Get,
            b"gets" | b"GETS" => Command::Gets,
            b"prepend" | b"PREPEND" => Command::Prepend,
            b"quit" | b"QUIT" => Command::Quit,
            b"replace" | b"REPLACE" => Command::Replace,
            b"set" | b"SET" => Command::Set,
            _ => {
                // TODO(bmartin): we can return an unknown command error here
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        };
        Ok((remaining, command))
    }

    fn _parse_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Request> {
        match self.parse_command(input)? {
            (input, Command::Add) => {
                let (input, request) = self.parse_add_request(input)?;
                Ok((input, Request::Add(request)))
            }
            (input, Command::Append) => {
                let (input, request) = self.parse_append_request(input)?;
                Ok((input, Request::Append(request)))
            }
            (input, Command::Cas) => {
                let (input, request) = self.parse_cas_request(input)?;
                Ok((input, Request::Cas(request)))
            }
            (input, Command::Decr) => {
                let (input, request) = self.parse_decr_request(input)?;
                Ok((input, Request::Decr(request)))
            }
            (input, Command::Delete) => {
                let (input, request) = self.parse_delete_request(input)?;
                Ok((input, Request::Delete(request)))
            }
            (input, Command::FlushAll) => {
                let (input, request) = self.parse_flush_all_request(input)?;
                Ok((input, Request::FlushAll(request)))
            }
            (input, Command::Incr) => {
                let (input, request) = self.parse_incr_request(input)?;
                Ok((input, Request::Incr(request)))
            }
            (input, Command::Get) => {
                let (input, request) = self.parse_get_request(input)?;
                Ok((input, Request::Get(request)))
            }
            (input, Command::Gets) => {
                let (input, request) = self.parse_gets_request(input)?;
                Ok((input, Request::Get(request)))
            }
            (input, Command::Prepend) => {
                let (input, request) = self.parse_prepend_request(input)?;
                Ok((input, Request::Prepend(request)))
            }
            (input, Command::Quit) => {
                let (input, request) = self.parse_quit_request(input)?;
                Ok((input, Request::Quit(request)))
            }
            (input, Command::Replace) => {
                let (input, request) = self.parse_replace_request(input)?;
                Ok((input, Request::Replace(request)))
            }
            (input, Command::Set) => {
                let (input, request) = self.parse_set_request(input)?;
                Ok((input, Request::Set(request)))
            }
        }
    }

    fn _compose_request(
        &self,
        request: &Request,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        let len = match request {
            Request::Add(r) => self._compose_add_request(r, buffer),
            Request::Append(r) => self._compose_append_request(r, buffer),
            Request::Cas(r) => self._compose_cas_request(r, buffer),
            Request::Decr(r) => self._compose_decr_request(r, buffer),
            Request::Delete(r) => self._compose_delete_request(r, buffer),
            Request::FlushAll(r) => self._compose_flush_all_request(r, buffer),
            Request::Get(r) => self._compose_get_request(r, buffer),
            Request::Incr(r) => self._compose_incr_request(r, buffer),
            Request::Prepend(r) => self._compose_prepend_request(r, buffer),
            Request::Quit(_) => self._compose_quit_request(buffer),
            Request::Replace(r) => self._compose_replace_request(r, buffer),
            Request::Set(r) => self._compose_set_request(r, buffer),
        };

        Ok(len)
    }

    fn _parse_response<'a>(
        &self,
        request: &Request,
        buffer: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        match request {
            Request::Add(r) => self.parse_add_response(r, buffer),
            Request::Append(r) => self.parse_append_response(r, buffer),
            Request::Cas(r) => self.parse_cas_response(r, buffer),
            Request::Decr(r) => self.parse_decr_response(r, buffer),
            Request::Delete(r) => self.parse_delete_response(r, buffer),
            Request::FlushAll(r) => self.parse_flush_all_response(r, buffer),
            Request::Get(r) => self.parse_get_response(r, buffer),
            Request::Incr(r) => self.parse_incr_response(r, buffer),
            Request::Prepend(r) => self.parse_prepend_response(r, buffer),
            Request::Replace(r) => self.parse_replace_response(r, buffer),
            Request::Set(r) => self.parse_set_response(r, buffer),
            _ => todo!(),
        }
    }

    fn _compose_response(
        &self,
        request: &Request,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        match request {
            Request::Add(request) => self.compose_add_response(request, response, buffer),
            Request::Append(request) => self.compose_append_response(request, response, buffer),
            Request::Cas(request) => self.compose_cas_response(request, response, buffer),
            Request::Decr(request) => self.compose_decr_response(request, response, buffer),
            Request::Delete(request) => self.compose_delete_response(request, response, buffer),
            Request::FlushAll(request) => {
                self.compose_flush_all_response(request, response, buffer)
            }
            Request::Get(request) => self.compose_get_response(request, response, buffer),
            Request::Incr(request) => self.compose_incr_response(request, response, buffer),
            Request::Prepend(request) => self.compose_prepend_response(request, response, buffer),
            // Request::Quit(request) => self.compose_quit_response(request, response, buffer),
            Request::Replace(request) => self.compose_replace_response(request, response, buffer),
            Request::Set(request) => self.compose_set_response(request, response, buffer),
            _ => todo!(),
        }

        // Ok(len)
    }
}

impl Protocol<Request, Response> for TextProtocol {
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
            Ok((input, response)) => Ok(ParseOk::new(response, buffer.len() - input.len())),
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
