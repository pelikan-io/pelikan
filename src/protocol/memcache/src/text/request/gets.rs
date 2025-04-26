// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

impl TextProtocol {
    // this is to be called after parsing the command, so we do not match the verb
    pub(crate) fn parse_gets_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Get> {
        // we can use the get parser here and convert the request
        match self._parse_get_request(input) {
            Ok((input, request)) => {
                GETS.increment();
                let keys = request.keys.len() as u64;
                GETS_KEY.add(keys);
                Ok((
                    input,
                    Get {
                        keys: request.keys,
                        cas: true,
                        key: true,
                        opaque: None,
                    },
                ))
            }
            Err(e) => {
                if !e.is_incomplete() {
                    GETS.increment();
                    GETS_EX.increment();
                }
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let protocol = TextProtocol::new();

        // test parsing a simple request
        assert_eq!(
            protocol._parse_request(b"gets key \r\n"),
            Ok((
                &b""[..],
                Request::Get(Get {
                    keys: vec![b"key".to_vec().into_boxed_slice()].into_boxed_slice(),
                    cas: true,
                    key: true,
                    opaque: None,
                })
            ))
        );

        // command name is not case sensitive
        assert_eq!(
            protocol._parse_request(b"gets key \r\n"),
            protocol._parse_request(b"GETS key \r\n"),
        );

        // trailing spaces don't matter
        assert_eq!(
            protocol._parse_request(b"gets key\r\n"),
            protocol._parse_request(b"gets key \r\n"),
        );

        // multiple trailing spaces is fine too
        assert_eq!(
            protocol._parse_request(b"gets key\r\n"),
            protocol._parse_request(b"gets key      \r\n"),
        );

        // request can have multiple keys
        assert_eq!(
            protocol._parse_request(b"gets a b c\r\n"),
            Ok((
                &b""[..],
                Request::Get(Get {
                    keys: vec![
                        b"a".to_vec().into_boxed_slice(),
                        b"b".to_vec().into_boxed_slice(),
                        b"c".to_vec().into_boxed_slice(),
                    ]
                    .into_boxed_slice(),
                    cas: true,
                    key: true,
                    opaque: None,
                })
            ))
        );

        // key is binary safe
        assert_eq!(
            protocol._parse_request(b"gets evil\0key \r\n"),
            Ok((
                &b""[..],
                Request::Get(Get {
                    keys: vec![b"evil\0key".to_vec().into_boxed_slice(),].into_boxed_slice(),
                    cas: true,
                    key: true,
                    opaque: None,
                })
            ))
        );
    }
}
