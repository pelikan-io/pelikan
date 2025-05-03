use super::*;

impl TextProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_replace_response<'a>(
        &self,
        _request: &Replace,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_replace_response<'a>(
        &self,
        _request: &Replace,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[allow(unused_variables)]
    pub(crate) fn compose_replace_response(
        &self,
        request: &Replace,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        #[cfg(feature = "metrics")]
        {
            match response {
                Response::Stored(_) => {
                    REPLACE_STORED.increment();
                }
                Response::NotStored(_) => {
                    REPLACE_NOT_STORED.increment();
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
