use super::*;

impl TextProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_add_response<'a>(
        &self,
        _request: &Add,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_add_response<'a>(
        &self,
        _request: &Add,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    pub(crate) fn compose_add_response(
        &self,
        request: &Add,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_add_response(request, response, buffer)
    }

    fn _compose_add_response(
        &self,
        _request: &Add,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        Ok(response.compose(buffer))
    }
}
