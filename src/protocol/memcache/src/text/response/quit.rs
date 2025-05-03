use super::*;

impl TextProtocol {
    pub(crate) fn compose_quit_response(
        &self,
        request: &Quit,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_quit_response(request, response, buffer)
    }

    fn _compose_quit_response(
        &self,
        _request: &Quit,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        Ok(response.compose(buffer))
    }
}
