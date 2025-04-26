use super::*;

impl BinaryProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_delete_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Delete> {
        DELETE.increment();
        match self._parse_delete_request(input, header) {
            Ok((input, request)) => Ok((input, request)),
            Err(e) => {
                if !e.is_incomplete() {
                    DELETE_EX.increment();
                }
                Err(e)
            }
        }
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_delete_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Delete> {
        self._parse_delete_request(input, header)
    }

    fn _parse_delete_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Delete> {
        // validation

        if header.key_len == 0 || header.key_len as usize > self.max_key_len as usize {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        if header.extras_len != 0 {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        if header.total_body_len > header.key_len.into() {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        let (input, key) = take(header.key_len as usize)(input)?;

        if !is_key_valid(key) {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        Ok((
            input,
            Delete {
                noreply: false,
                opaque: Some(header.opaque),
                key: key.to_owned().into_boxed_slice(),
            },
        ))
    }

    pub(crate) fn compose_delete_request(
        &self,
        request: &Delete,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_delete_request(request, buffer)
    }

    fn _compose_delete_request(
        &self,
        request: &Delete,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        if request.key.len() > u16::MAX as _ {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "request key too large for binary protocol",
            ));
        }

        let klen = request.key.len() as u16;

        buffer.put_slice(&[0x80, 0x00]);
        buffer.put_u16(klen);
        buffer.put_slice(&[0x00, 0x00, 0x00, 0x00]);
        buffer.put_u32(klen as _);
        buffer.put_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        buffer.put_slice(&request.key);

        Ok(24 + klen as usize)
    }
}
