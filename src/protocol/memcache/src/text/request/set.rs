// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_common::BufMut;

impl TextProtocol {
    // this is to be called after parsing the command, so we do not match the verb
    pub(crate) fn _parse_set_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Set> {
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
        let (input, flags) = parse_u32(input)?;
        let (input, _) = space1(input)?;
        let (input, ttl) = parse_ttl(input, self.time_type)?;
        let (input, _) = space1(input)?;
        let (mut input, bytes) = parse_usize(input)?;

        if bytes > self.max_value_size {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        // if we have a space, we might have a noreply
        if let Ok((i, _)) = space1(input) {
            if i.len() > 7 && &i[0..7] == b"noreply" {
                input = &i[7..];
                noreply = true;
            }
        }

        let (input, _) = space0(input)?;
        let (input, _) = crlf(input)?;
        let (input, value) = take(bytes)(input)?;
        let (input, _) = crlf(input)?;

        Ok((
            input,
            Set {
                key: key.to_owned().into_boxed_slice(),
                value: value.to_owned().into_boxed_slice(),
                ttl,
                flags,
                noreply,
                opaque: None,
            },
        ))
    }

    pub fn parse_set_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Set> {
        match self._parse_set_request(input) {
            Ok((input, request)) => {
                #[cfg(feature = "metrics")]
                SET.increment();

                Ok((input, request))
            }
            Err(e) => {
                #[cfg(feature = "metrics")]
                if !e.is_incomplete() {
                    SET.increment();
                    SET_EX.increment();
                }

                Err(e)
            }
        }
    }

    pub(crate) fn _compose_set_request(&self, request: &Set, session: &mut dyn BufMut) -> usize {
        let verb = b"set ";
        let flags = format!(" {}", request.flags).into_bytes();
        let ttl = format!(" {}", request.ttl.get().unwrap_or(0)).into_bytes();
        let vlen = format!(" {}", request.value.len()).into_bytes();
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
        session.put_slice(&vlen);
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

        // basic set command
        assert_eq!(
            protocol._parse_request(b"set 0 0 0 1\r\n0\r\n"),
            Ok((
                &b""[..],
                Request::Set(Set {
                    key: b"0".to_vec().into_boxed_slice(),
                    value: b"0".to_vec().into_boxed_slice(),
                    flags: 0,
                    ttl: Ttl::none(),
                    noreply: false,
                    opaque: None,
                })
            ))
        );

        // noreply
        assert_eq!(
            protocol._parse_request(b"set 0 0 0 1 noreply\r\n0\r\n"),
            Ok((
                &b""[..],
                Request::Set(Set {
                    key: b"0".to_vec().into_boxed_slice(),
                    value: b"0".to_vec().into_boxed_slice(),
                    flags: 0,
                    ttl: Ttl::none(),
                    noreply: true,
                    opaque: None,
                })
            ))
        );
    }
}
