use super::*;

impl BinaryProtocol {
    #[cfg(feature = "metrics")]
    pub(crate) fn parse_set_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Set> {
        SET.increment();

        match self._parse_set_request(input, header) {
            Ok((input, request)) => Ok((input, request)),
            Err(e) => {
                if !e.is_incomplete() {
                    SET_EX.increment();
                }
                Err(e)
            }
        }
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn parse_set_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Set> {
        self._parse_set_request(input, header)
    }

    fn _parse_set_request<'a>(
        &self,
        input: &'a [u8],
        header: RequestHeader,
    ) -> IResult<&'a [u8], Set> {
        // validation

        if header.key_len == 0 || header.key_len as usize > self.max_key_len as usize {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        if header.extras_len != 8 {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        if header.total_body_len < (header.key_len as u32 + header.extras_len as u32) {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        let value_len =
            header.total_body_len as usize - header.key_len as usize - header.extras_len as usize;

        if value_len == 0 || value_len > self.max_value_size as usize {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        let (input, flags) = take(4usize)(input)?;
        let (input, expiry) = take(4usize)(input)?;
        let (input, key) = take(header.key_len as usize)(input)?;

        if !is_key_valid(key) {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        let (input, value) = take(value_len)(input)?;

        let flags = u32::from_be_bytes([flags[0], flags[1], flags[2], flags[3]]);

        let expiry = i32::from_be_bytes([expiry[0], expiry[1], expiry[2], expiry[3]]);
        let ttl = Ttl::new(expiry.into(), TimeType::Memcache);

        Ok((
            input,
            Set {
                key: key.to_owned().into_boxed_slice(),
                flags,
                noreply: false,
                ttl,
                value: value.to_owned().into_boxed_slice(),
                opaque: Some(header.opaque),
            },
        ))
    }

    pub(crate) fn compose_set_request(
        &self,
        request: &Set,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        self._compose_set_request(request, buffer)
    }

    fn _compose_set_request(
        &self,
        request: &Set,
        buffer: &mut dyn BufMut,
    ) -> std::result::Result<usize, std::io::Error> {
        if request.key.len() > u16::MAX as _ {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "request key too large for binary protocol",
            ));
        }

        if request.value.len() > u32::MAX as _ {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "request value too large for binary protocol",
            ));
        }

        let header = RequestHeader::set(request.key.len() as _, request.value.len() as _)?;
        header.write_to(buffer);
        buffer.put_u32(request.flags);
        buffer.put_i32(request.ttl.get().unwrap_or(0));
        buffer.put_slice(&request.key);
        buffer.put_slice(&request.value);

        Ok(header.request_len())
    }
}
