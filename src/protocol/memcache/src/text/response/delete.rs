use super::*;

impl TextProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_delete_response<'a>(
        &self,
        _request: &Delete,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        let response = crate::response(input);

        match crate::response(input) {
            Ok((_, Response::Deleted(_))) => {
                DELETE_DELETED.increment();
            }
            Ok((_, Response::NotFound(_))) => {
                DELETE_NOT_FOUND.increment();
            }
            _ => {}
        }

        response
        // self._parse_delete_response(request, input, header)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_delete_response<'a>(
        &self,
        _request: &Delete,
        input: &'a [u8],
    ) -> IResult<&'a [u8], Response> {
        crate::response(input)
    }

    pub(crate) fn compose_delete_response(
        &self,
        request: &Delete,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_delete_response(request, response, buffer)
    }

    fn _compose_delete_response(
        &self,
        _request: &Delete,
        response: &Response,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        match response {
            Response::Deleted(_) => {
                buffer.put_slice(&[
                    0x81, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                ]);
            }
            Response::NotFound(_) => {
                buffer.put_slice(&[
                    0x81, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                ]);
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "unexpected response",
                ));
            }
        }

        Ok(24)
    }
}
