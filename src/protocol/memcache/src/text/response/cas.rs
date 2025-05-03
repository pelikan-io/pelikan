use super::*;

impl TextProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_cas_response<'a>(
        &self,
        _request: &Cas,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_cas_response<'a>(
        &self,
        _request: &Cas,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[allow(unused_variables)]
    pub(crate) fn compose_cas_response(
        &self,
        request: &Cas,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        #[cfg(feature = "metrics")]
        {
            match response {
                Response::Stored(_) => {
                    CAS_STORED.increment();
                }
                Response::Exists(_) => {
                    CAS_EXISTS.increment();
                }
                Response::NotFound(_) => {
                    CAS_NOT_FOUND.increment();
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
