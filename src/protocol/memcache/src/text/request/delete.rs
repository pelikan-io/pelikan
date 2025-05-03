// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_common::BufMut;

impl TextProtocol {
    // this is to be called after parsing the command, so we do not match the verb
    pub(crate) fn _parse_delete_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Delete> {
        let (input, _) = space1(input)?;

        let (mut input, key) = key(input, self.max_key_len)?;

        let key = match key {
            Some(k) => k,
            None => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        };

        let mut noreply = false;

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
            Delete {
                key: key.to_owned().into_boxed_slice(),
                noreply,
                opaque: None,
            },
        ))
    }

    // this is to be called after parsing the command, so we do not match the verb
    pub fn parse_delete_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Delete> {
        match self._parse_delete_request(input) {
            Ok((input, request)) => {
                #[cfg(feature = "metrics")]
                DELETE.increment();

                Ok((input, request))
            }
            Err(e) => {
                #[cfg(feature = "metrics")]
                if !e.is_incomplete() {
                    DELETE.increment();
                    DELETE_EX.increment();
                }

                Err(e)
            }
        }
    }

    pub(crate) fn _compose_delete_request(
        &self,
        request: &Delete,
        session: &mut dyn BufMut,
    ) -> usize {
        let verb = b"delete ";
        let header_end = if request.noreply {
            " noreply\r\n".as_bytes()
        } else {
            "\r\n".as_bytes()
        };

        let size = verb.len() + request.key.len() + header_end.len();

        session.put_slice(verb);
        session.put_slice(&request.key);
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

        // basic delete command
        assert_eq!(
            protocol._parse_request(b"delete 0\r\n"),
            Ok((
                &b""[..],
                Request::Delete(Delete {
                    key: b"0".to_vec().into_boxed_slice(),
                    noreply: false,
                    opaque: None,
                })
            ))
        );
    }
}
