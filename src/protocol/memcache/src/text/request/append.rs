// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_common::BufMut;

impl TextProtocol {
    // this is to be called after parsing the command, so we do not match the verb
    pub fn parse_append_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Append> {
        // we can use the set parser here and convert the request
        match self._parse_set_request(input) {
            Ok((input, request)) => {
                #[cfg(feature = "metrics")]
                APPEND.increment();

                Ok((
                    input,
                    Append {
                        key: request.key,
                        value: request.value,
                        ttl: request.ttl,
                        flags: request.flags,
                        noreply: request.noreply,
                    },
                ))
            }
            Err(e) => {
                #[cfg(feature = "metrics")]
                if !e.is_incomplete() {
                    APPEND.increment();
                    APPEND_EX.increment();
                }

                Err(e)
            }
        }
    }

    pub(crate) fn _compose_append_request(
        &self,
        request: &Append,
        session: &mut dyn BufMut,
    ) -> usize {
        let verb = b"append ";
        let flags = format!(" {}", request.flags).into_bytes();
        let ttl = format!(" {}", request.ttl.get().unwrap_or(0)).into_bytes();
        let vlen = format!(" {}", request.value.len());
        let header_end = if request.noreply {
            " noreply\r\n".as_bytes()
        } else {
            "\r\n".as_bytes()
        };

        let size = verb.len()
            + request.key.len()
            + flags.len()
            + ttl.len()
            + vlen.len()
            + header_end.len()
            + request.value.len()
            + CRLF.len();

        session.put_slice(verb);
        session.put_slice(&request.key);
        session.put_slice(&flags);
        session.put_slice(&ttl);
        session.put_slice(vlen.as_bytes());
        session.put_slice(header_end);
        session.put_slice(&request.value);
        session.put_slice(CRLF);

        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let protocol = TextProtocol::new();

        // basic append command
        assert_eq!(
            protocol._parse_request(b"append 0 0 0 1\r\n0\r\n"),
            Ok((
                &b""[..],
                Request::Append(Append {
                    key: b"0".to_vec().into_boxed_slice(),
                    value: b"0".to_vec().into_boxed_slice(),
                    flags: 0,
                    ttl: Ttl::none(),
                    noreply: false,
                })
            ))
        );

        // noreply
        assert_eq!(
            protocol._parse_request(b"append 0 0 0 1 noreply\r\n0\r\n"),
            Ok((
                &b""[..],
                Request::Append(Append {
                    key: b"0".to_vec().into_boxed_slice(),
                    value: b"0".to_vec().into_boxed_slice(),
                    flags: 0,
                    ttl: Ttl::none(),
                    noreply: true,
                })
            ))
        );
    }
}
