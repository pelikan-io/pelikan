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

#[cfg(test)]
mod tests {
    use crate::binary::BinaryProtocol;
    use crate::Request;
    use protocol_common::Protocol;

    #[test]
    fn get() {
        let protocol = BinaryProtocol::default();

        let buffer = [
            0x80, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x48, 0x65, 0x6C, 0x6C, 0x6F
        ];

        match protocol
            .parse_request(&buffer)
            .map(|v| (v.consumed(), v.into_inner()))
        {
            Ok((consumed, Request::Get(get))) => {
                assert_eq!(consumed, buffer.len());
                assert_eq!(get.keys.len(), 1);
                assert_eq!(&*get.keys[0], "Hello".as_bytes());

                assert!(get.cas);
                assert!(!get.key);
                assert_eq!(get.opaque, Some(0));
            }
            Ok((_consumed, request)) => {
                panic!("wrong request type: {:?}", request);
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    panic!("incomplete");
                } else {
                    panic!("corrupt");
                }
            }
        }
    }

    #[test]
    fn get_with_opaque() {
        let protocol = BinaryProtocol::default();

        let buffer = [
            0x80, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xDE, 0xCA, 0xFB, 0xAD,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x48, 0x65, 0x6C, 0x6C, 0x6F
        ];

        match protocol
            .parse_request(&buffer)
            .map(|v| (v.consumed(), v.into_inner()))
        {
            Ok((consumed, Request::Get(get))) => {
                assert_eq!(consumed, buffer.len());
                assert_eq!(get.keys.len(), 1);
                assert_eq!(&*get.keys[0], "Hello".as_bytes());

                assert!(get.cas);
                assert!(!get.key);
                assert_eq!(get.opaque, Some(0xDECAFBAD));
            }
            Ok((_consumed, request)) => {
                panic!("wrong request type: {:?}", request);
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    panic!("incomplete");
                } else {
                    panic!("corrupt");
                }
            }
        }
    }

    #[test]
    fn incomplete() {
        let protocol = BinaryProtocol::default();

        let buffer = [
            0x80, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xDE, 0xCA, 0xFB, 0xAD,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x48, 0x65, 0x6C, 0x6C
        ];

        match protocol
            .parse_request(&buffer)
            .map(|v| (v.consumed(), v.into_inner()))
        {
            Ok(_) => {
                panic!("should be incomplete");
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::WouldBlock {
                    panic!("corrupt");
                }
            }
        }
    }

    #[test]
    fn get_with_extra_bytes() {
        let protocol = BinaryProtocol::default();

        let buffer = [
            0x80, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x48, 0x65, 0x6C, 0x6C, 0x6F, 0xC0, 0xFF, 0xEE
        ];

        match protocol
            .parse_request(&buffer)
            .map(|v| (v.consumed(), v.into_inner()))
        {
            Ok((consumed, Request::Get(get))) => {
                assert_eq!(consumed, buffer.len() - 3);
                assert_eq!(get.keys.len(), 1);
                assert_eq!(&*get.keys[0], "Hello".as_bytes());

                assert!(get.cas);
                assert!(!get.key);
                assert_eq!(get.opaque, Some(0));
            }
            Ok((_consumed, request)) => {
                panic!("wrong request type: {:?}", request);
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    panic!("incomplete");
                } else {
                    panic!("corrupt");
                }
            }
        }
    }
}

