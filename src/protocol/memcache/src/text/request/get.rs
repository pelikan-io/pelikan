// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_common::BufMut;

impl TextProtocol {
    // this is to be called after parsing the command, so we do not match the verb
    pub(crate) fn _parse_get_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Get> {
        let mut keys = Vec::new();

        let (mut input, _) = space1(input)?;

        loop {
            let (i, key) = key(input, self.max_key_len)?;

            match key {
                Some(k) => {
                    keys.push(k.to_owned().into_boxed_slice());
                }
                None => {
                    break;
                }
            };

            if let Ok((i, _)) = space1(i) {
                input = i;
            } else {
                input = i;
                break;
            }

            if keys.len() >= self.max_batch_size {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        }

        if keys.is_empty() {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        let (input, _) = space0(input)?;
        let (input, _) = crlf(input)?;
        Ok((
            input,
            Get {
                keys: keys.to_owned().into_boxed_slice(),
                cas: false,
                key: true,
                opaque: None,
            },
        ))
    }

    // this is to be called after parsing the command, so we do not match the verb
    pub fn parse_get_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Get> {
        match self._parse_get_request(input) {
            Ok((input, request)) => {
                #[cfg(feature = "metrics")]
                {
                    GET.increment();
                    let keys = request.keys.len() as u64;
                    GET_KEY.add(keys);
                    let _ = GET_CARDINALITY.increment(keys);
                }

                Ok((input, request))
            }
            Err(e) => {
                #[cfg(feature = "metrics")]
                if !e.is_incomplete() {
                    GET.increment();
                    GET_EX.increment();
                }

                Err(e)
            }
        }
    }

    pub(crate) fn _compose_get_request(&self, request: &Get, session: &mut dyn BufMut) -> usize {
        let verb = b"get";

        let mut size = verb.len() + CRLF.len();

        session.put_slice(verb);
        for key in request.keys.iter() {
            session.put_slice(b" ");
            session.put_slice(key);
            size += 1 + key.len();
        }
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

        // basic get command
        assert_eq!(
            protocol._parse_request(b"get key\r\n"),
            Ok((
                &b""[..],
                Request::Get(Get {
                    keys: vec![b"key".to_vec().into_boxed_slice()].into_boxed_slice(),
                    cas: false,
                    key: true,
                    opaque: None,
                })
            ))
        );

        // command name is not case sensitive
        assert_eq!(
            protocol._parse_request(b"get key \r\n"),
            protocol._parse_request(b"GET key \r\n"),
        );

        // trailing spaces don't matter
        assert_eq!(
            protocol._parse_request(b"get key\r\n"),
            protocol._parse_request(b"get key \r\n"),
        );

        // multiple trailing spaces is fine too
        assert_eq!(
            protocol._parse_request(b"get key\r\n"),
            protocol._parse_request(b"get key      \r\n"),
        );

        // request can have multiple keys
        assert_eq!(
            protocol._parse_request(b"get a b c\r\n"),
            Ok((
                &b""[..],
                Request::Get(Get {
                    keys: vec![
                        b"a".to_vec().into_boxed_slice(),
                        b"b".to_vec().into_boxed_slice(),
                        b"c".to_vec().into_boxed_slice(),
                    ]
                    .into_boxed_slice(),
                    cas: false,
                    key: true,
                    opaque: None,
                })
            ))
        );

        // key is binary safe
        assert_eq!(
            protocol._parse_request(b"get evil\0key \r\n"),
            Ok((
                &b""[..],
                Request::Get(Get {
                    keys: vec![b"evil\0key".to_vec().into_boxed_slice(),].into_boxed_slice(),
                    cas: false,
                    key: true,
                    opaque: None,
                })
            ))
        );
    }
}
