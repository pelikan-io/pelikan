use super::*;

impl TextProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_set_response<'a>(
        &self,
        _request: &Set,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_set_response<'a>(
        &self,
        _request: &Set,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[allow(unused_variables)]
    pub(crate) fn compose_set_response(
        &self,
        request: &Set,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        #[cfg(feature = "metrics")]
        {
            match response {
                Response::Stored(_) => {
                    SET_STORED.increment();
                }
                Response::NotStored(_) => {
                    SET_NOT_STORED.increment();
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
