// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_common::BufMut;

impl TextProtocol {
    // this is to be called after parsing the command, so we do not match the verb
    pub fn parse_decr_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Decr> {
        // we can use the incr parser here and convert the request
        match self._parse_incr_request(input) {
            Ok((input, request)) => {
                DECR.increment();
                Ok((
                    input,
                    Decr {
                        key: request.key,
                        value: request.value,
                        noreply: request.noreply,
                    },
                ))
            }
            Err(e) => {
                if !e.is_incomplete() {
                    DECR.increment();
                    DECR_EX.increment();
                }
                Err(e)
            }
        }
    }

    pub(crate) fn _compose_decr_request(&self, request: &Decr, session: &mut dyn BufMut) -> usize {
        let verb = b"decr ";
        let value = format!(" {}", request.value).into_bytes();
        let header_end = if request.noreply {
            " noreply\r\n".as_bytes()
        } else {
            "\r\n".as_bytes()
        };

        let size = verb.len() + request.key.len() + value.len() + header_end.len();

        session.put_slice(verb);
        session.put_slice(&request.key);
        session.put_slice(&value);
        session.put_slice(header_end);

        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let protocol = TextProtocol::new();

        // basic decr command
        assert_eq!(
            protocol._parse_request(b"decr 0 1\r\n"),
            Ok((
                &b""[..],
                Request::Decr(Decr {
                    key: b"0".to_vec().into_boxed_slice(),
                    value: 1,
                    noreply: false,
                })
            ))
        );
    }
}
