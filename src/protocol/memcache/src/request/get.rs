// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Get {
    pub(crate) key: bool,
    pub(crate) cas: bool,
    pub(crate) opaque: Option<u32>,
    pub(crate) keys: Box<[Box<[u8]>]>,
}

impl Get {
    pub fn cas(&self) -> bool {
        self.cas
    }

    pub fn keys(&self) -> &[Box<[u8]>] {
        self.keys.as_ref()
    }
}

impl RequestParser {
    // this is to be called after parsing the command, so we do not match the verb
    pub(crate) fn parse_get_no_stats<'a>(&self, cas: bool, input: &'a [u8]) -> IResult<&'a [u8], Get> {
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
                key: true,
                cas,
                opaque: None,
                keys: keys.to_owned().into_boxed_slice(),
            },
        ))
    }

    // this is to be called after parsing the command, so we do not match the verb
    pub fn parse_get<'a>(&self, cas: bool, input: &'a [u8]) -> IResult<&'a [u8], Get> {
        match self.parse_get_no_stats(cas, input) {
            Ok((input, request)) => {
                GET.increment();
                let keys = request.keys.len() as u64;
                GET_KEY.add(keys);
                let _ = GET_CARDINALITY.increment(keys);
                Ok((input, request))
            }
            Err(e) => {
                if !e.is_incomplete() {
                    GET.increment();
                    GET_EX.increment();
                }
                Err(e)
            }
        }
    }
}

impl Compose for Get {
    fn compose(&self, session: &mut dyn BufMut) -> usize {
        let verb = if self.cas {
            "gets".as_bytes()
        } else {
            "get".as_bytes()
        };

        let mut size = verb.len() + CRLF.len();

        session.put_slice(verb);
        for key in self.keys.iter() {
            session.put_slice(b" ");
            session.put_slice(key);
            size += 1 + key.len();
        }
        session.put_slice(CRLF);

        size
    }
}

impl Klog for Get {
    type Response = Response;

    fn klog(&self, response: &Self::Response) {
        if let Response::Values(ref res) = response {
            let mut hit_keys = 0;
            let mut miss_keys = 0;

            let verb = if self.cas {
                "gets"
            } else {
                "get"
            };

            for value in res.values() {
                if value.len().is_none() {
                    miss_keys += 1;

                    klog!(
                        "\"{verb} {}\" {} 0",
                        String::from_utf8_lossy(value.key()),
                        MISS
                    );
                } else {
                    hit_keys += 1;

                    klog!(
                        "\"{verb} {}\" {} {}",
                        String::from_utf8_lossy(value.key()),
                        HIT,
                        value.len().unwrap(),
                    );
                }
            }

            GET_KEY_HIT.add(hit_keys as _);
            GET_KEY_MISS.add(miss_keys as _);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let parser = RequestParser::new();

        // basic get command
        assert_eq!(
            parser.parse_request(b"get key\r\n"),
            Ok((
                &b""[..],
                Request::Get(Get {
                    key: true,
                    cas: false,
                    opaque: None,
                    keys: vec![b"key".to_vec().into_boxed_slice()].into_boxed_slice(),
                })
            ))
        );

        // command name is not case sensitive
        assert_eq!(
            parser.parse_request(b"get key \r\n"),
            parser.parse_request(b"GET key \r\n"),
        );

        // trailing spaces don't matter
        assert_eq!(
            parser.parse_request(b"get key\r\n"),
            parser.parse_request(b"get key \r\n"),
        );

        // multiple trailing spaces is fine too
        assert_eq!(
            parser.parse_request(b"get key\r\n"),
            parser.parse_request(b"get key      \r\n"),
        );

        // request can have multiple keys
        assert_eq!(
            parser.parse_request(b"get a b c\r\n"),
            Ok((
                &b""[..],
                Request::Get(Get {
                    key: true,
                    cas: false,
                    opaque: None,
                    keys: vec![
                        b"a".to_vec().into_boxed_slice(),
                        b"b".to_vec().into_boxed_slice(),
                        b"c".to_vec().into_boxed_slice(),
                    ]
                    .into_boxed_slice(),
                })
            ))
        );

        // key is binary safe
        assert_eq!(
            parser.parse_request(b"get evil\0key \r\n"),
            Ok((
                &b""[..],
                Request::Get(Get {
                    key: true,
                    cas: false,
                    opaque: None,
                    keys: vec![b"evil\0key".to_vec().into_boxed_slice(),].into_boxed_slice()
                })
            ))
        );
    }
}
