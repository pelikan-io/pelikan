use super::*;

impl TextProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_append_response<'a>(
        &self,
        _request: &Append,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_append_response<'a>(
        &self,
        _request: &Append,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[allow(unused_variables)]
    pub(crate) fn compose_append_response(
        &self,
        request: &Append,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        #[cfg(feature = "metrics")]
        {
            match response {
                Response::Stored(ref res) => {
                    APPEND_STORED.increment();
                }
                Response::NotStored(ref res) => {
                    APPEND_NOT_STORED.increment();
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
