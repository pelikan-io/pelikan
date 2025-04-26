// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_common::BufMut;

impl TextProtocol {
    // this is to be called after parsing the command, so we do not match the verb
    pub(crate) fn _parse_incr_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Incr> {
        let mut noreply = false;

        let (input, _) = space1(input)?;
        let (input, key) = key(input, self.max_key_len)?;

        let key = match key {
            Some(k) => k,
            None => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        };

        let (input, _) = space1(input)?;
        let (mut input, value) = parse_u64(input)?;

        // if we have a space, we might have a noreply
        if let Ok((i, _)) = space1(input) {
            if i.len() > 7 && &i[0..7] == b"noreply" {
                input = &i[7..];
                noreply = true;
            }
        }

        let (input, _) = space0(input)?;
        let (input, _) = crlf(input)?;

        Ok((
            input,
            Incr {
                key: key.to_owned().into_boxed_slice(),
                value,
                noreply,
            },
        ))
    }

    pub fn parse_incr_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Incr> {
        match self._parse_incr_request(input) {
            Ok((input, request)) => {
                INCR.increment();
                Ok((input, request))
            }
            Err(e) => {
                if !e.is_incomplete() {
                    INCR.increment();
                    INCR_EX.increment();
                }
                Err(e)
            }
        }
    }

    pub(crate) fn _compose_incr_request(&self, request: &Incr, session: &mut dyn BufMut) -> usize {
        let verb = b"incr ";
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

        // basic command
        assert_eq!(
            protocol._parse_request(b"incr 0 1\r\n"),
            Ok((
                &b""[..],
                Request::Incr(Incr {
                    key: b"0".to_vec().into_boxed_slice(),
                    value: 1,
                    noreply: false,
                })
            ))
        );

        // noreply
        assert_eq!(
            protocol._parse_request(b"incr 0 1 noreply\r\n"),
            Ok((
                &b""[..],
                Request::Incr(Incr {
                    key: b"0".to_vec().into_boxed_slice(),
                    value: 1,
                    noreply: true,
                })
            ))
        );

        // alternate value
        assert_eq!(
            protocol._parse_request(b"incr 0 42\r\n"),
            Ok((
                &b""[..],
                Request::Incr(Incr {
                    key: b"0".to_vec().into_boxed_slice(),
                    value: 42,
                    noreply: false,
                })
            ))
        );

        // trailing space doesn't matter
        assert_eq!(
            protocol._parse_request(b"incr 0 1\r\n"),
            protocol._parse_request(b"incr 0 1 \r\n"),
        );
        assert_eq!(
            protocol._parse_request(b"incr 0 1 noreply\r\n"),
            protocol._parse_request(b"incr 0 1 noreply \r\n"),
        );
    }
}
