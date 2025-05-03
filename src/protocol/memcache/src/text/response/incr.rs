use super::*;

impl TextProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_incr_response<'a>(
        &self,
        _request: &Incr,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_incr_response<'a>(
        &self,
        _request: &Incr,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[allow(unused_variables)]
    pub(crate) fn compose_incr_response(
        &self,
        request: &Incr,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        #[cfg(feature = "metrics")]
        {
            match response {
                Response::Numeric(_) => {
                    INCR_STORED.increment();
                }
                Response::NotFound(_) => {
                    INCR_NOT_FOUND.increment();
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
