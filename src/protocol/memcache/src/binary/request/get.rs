use super::*;

impl BinaryProtocol {
    // NOTE: we increment metrics for GETS not GET because all binary protocol
    // get requests return with CAS value populated.
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_get_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Get> {
        GETS.increment();

        match self._parse_get_request(input, header) {
            Ok((input, request)) => {
                let keys = request.keys.len() as u64;
                GETS_KEY.add(keys);
                let _ = GETS_CARDINALITY.increment(keys);

                Ok((input, request))
            }
            Err(e) => {
                if !e.is_incomplete() {
                    GETS_EX.increment();
                }
                Err(e)
            }
        }
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_get_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Get> {
        self._parse_get_request(input, header)
    }

    fn _parse_get_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Get> {
        let mut keys = Vec::new();

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

        keys.push(key.into());

        if keys.is_empty() {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        Ok((
            input,
            Get {
                key: false,
                cas: true,
                opaque: Some(header.opaque),
                keys: keys.to_owned().into_boxed_slice(),
            },
        ))
    }

    pub(crate) fn compose_get_request(
        &self,
        request: &Get,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_get_request(request, buffer)
    }

    fn _compose_get_request(
        &self,
        request: &Get,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        if request.keys.len() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "get request has multiple keys for binary protocol",
            ));
        }

        if request.keys.len() > u16::MAX as _ {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "request key too large for binary protocol",
            ));
        }

        let klen = request.keys[0].len() as u16;

        buffer.put_slice(&[0x80, 0x00]);
        buffer.put_u16(klen);
        buffer.put_slice(&[0x00, 0x00, 0x00, 0x00]);
        buffer.put_u32(klen as _);
        buffer.put_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        buffer.put_slice(&request.keys[0]);

        Ok(24 + klen as usize)
    }
}
