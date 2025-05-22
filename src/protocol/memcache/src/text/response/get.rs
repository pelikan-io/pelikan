use super::*;

impl TextProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_get_response<'a>(
        &self,
        _request: &Get,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_get_response<'a>(
        &self,
        _request: &Get,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[allow(unused_variables)]
    pub(crate) fn compose_get_response(
        &self,
        request: &Get,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        #[cfg(feature = "metrics")]
        {
            match response {
                Response::Values(v) => {
                    let hit = v.values.len();
                    let miss = request.keys.len() - hit;

                    if request.cas {
                        GETS_KEY_HIT.add(hit as _);
                        GETS_KEY_MISS.add(miss as _);
                    } else {
                        GET_KEY_HIT.add(hit as _);
                        GET_KEY_MISS.add(miss as _);
                    }
                }
                Response::NotFound(_) => {
                    if request.cas {
                        GETS_KEY_MISS.add(request.keys.len() as _);
                    } else {
                        GET_KEY_MISS.add(request.keys.len() as _);
                    }
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "unexpected response",
                    ));
                }
            }
        }

        // For text GETs where all keys miss, emit just "END\r\n" by
        // composing an empty Values; otherwise forward the original
        // response.
        // The binary protocol uses the `NotFound` directly.
        if let Response::NotFound(_) = response {
            let empty = Values {
                values: Vec::new().into(),
            };
            return Ok(Response::Values(empty).compose(buffer));
        }
        Ok(response.compose(buffer))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn text_get_with_some_hit_doesnt_emit_end() {
        // Simulate a get request and response with some keys that hit and some that miss.
        let request = Get {
            key: true,
            opaque: None,
            keys: vec![
                b"key".to_vec().into_boxed_slice(),
                b"missing_key".to_vec().into_boxed_slice(),
            ]
            .into_boxed_slice(),
            cas: false,
        };

        let resp = Response::values(vec![Value::new(b"key", 0, None, b"val")].into_boxed_slice());

        // Verify that the response is composed correctly.
        let mut buffer = BytesMut::new();
        let text_protocol = TextProtocol::default();
        text_protocol
            .compose_get_response(&request, &resp, &mut buffer)
            .unwrap();

        assert_eq!(&buffer[..], b"VALUE key 0 3\r\nval\r\nEND\r\n");
    }

    #[test]
    fn text_get_miss_emits_end() {
        // Simulate a get request and response with all keys missing.
        let request = Get {
            key: true,
            opaque: None,
            keys: vec![b"missing".to_vec().into_boxed_slice()].into_boxed_slice(),
            cas: false,
        };
        let response = Response::not_found(false);

        // Verify that the response is composed correctly.
        let mut buffer = BytesMut::new();
        let text_protocol = TextProtocol::default();
        text_protocol
            .compose_get_response(&request, &response, &mut buffer)
            .unwrap();

        assert_eq!(&buffer[..], b"END\r\n");
    }
}
