use super::*;

impl TextProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_flush_all_response<'a>(
        &self,
        _request: &FlushAll,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_flush_all_response<'a>(
        &self,
        _request: &FlushAll,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    pub(crate) fn compose_flush_all_response(
        &self,
        request: &FlushAll,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_flush_all_response(request, response, buffer)
    }

    fn _compose_flush_all_response(
        &self,
        _request: &FlushAll,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        Ok(response.compose(buffer))
    }
}
