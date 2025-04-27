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

                    if get.cas {
                        GETS_KEY_HIT.add(hit);
                        GETS_KEY_MISS.add(miss);
                    } else {
                        GET_KEY_HIT.add(hit);
                        GET_KEY_MISS.add(miss);
                    }
                }
                Response::NotFound(_) => {
                    if get.cas {
                        GETS_KEY_MISS.add(request.keys.len());
                    } else {
                        GET_KEY_MISS.add(request.keys.len());
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

        Ok(response.compose(buffer))
    }
}
