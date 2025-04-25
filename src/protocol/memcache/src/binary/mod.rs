//! A submodule containing the relevant parts for the Memcache binary protocol.
//! Since it's just a different encoding of the same logical requests and
//! responses, we reuse many of the components from the text-bases protocol,
//! such as the concrete request and response types.

use crate::*;

pub mod request {
	use super::*;

	#[repr(C)]
	pub struct Header {
		magic: u8,
		opcode: u8,
		key_len: u16,
		extras_len: u8,
		data_type: u8,
		_reserved: u16,
		total_body_len: u32,
		opaque: u32,
		cas: u64,
	}

	#[derive(Copy, Clone)]
	pub struct Parser {
	    max_value_size: usize,
	    max_batch_size: usize,
	    max_key_len: usize,
	    time_type: TimeType,
	}

	impl Parser {
	    pub fn new() -> Self {
	        Default::default()
	    }

	    pub fn time_type(mut self, time_type: TimeType) -> Self {
	        self.time_type = time_type;
	        self
	    }

	    pub fn max_value_size(mut self, bytes: usize) -> Self {
	        self.max_value_size = bytes;
	        self
	    }

	    pub fn max_key_len(mut self, bytes: usize) -> Self {
	        self.max_key_len = bytes;
	        self
	    }

	    pub fn max_batch_size(mut self, count: usize) -> Self {
	        self.max_batch_size = count;
	        self
	    }

	    fn parse_header<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Header> {
	    	let (remaining, h) = take(24usize)(input)?;

	    	let header = Header {
	    		magic: h[0],
	    		opcode: h[1],
	    		key_len: u16::from_be_bytes([h[2], h[3]]),
	    		extras_len: h[4],
	    		data_type: h[5],
	    		_reserved: u16::from_be_bytes([h[6], h[7]]),
	    		total_body_len: u32::from_be_bytes([h[8], h[9], h[10], h[11]]),
	    		opaque: u32::from_be_bytes([h[12], h[13], h[14], h[15]]),
	    		cas: u64::from_be_bytes([h[16], h[17], h[18], h[19], h[20], h[21], h[22], h[23]]),
	    	};

	    	if header.magic != 0x80 {
	    		return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
	    	}

	    	if header.data_type != 0x00 {
	    		return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
	    	}

	    	// impose a constraint on the total body length based on the max
	    	// sizes for key, value, and extra data
	    	if header.total_body_len as usize > self.max_key_len + self.max_value_size + 32 {
	    		return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
	    	}

	        Ok((remaining, header))
	    }

	    fn parse_get_no_stats<'a>(&self, input: &'a [u8], header: Header) -> IResult<&'a [u8], Get> {
	        let mut keys = Vec::new();

	        // validation

	        if header.key_len == 0 || header.key_len as usize > self.max_key_len {
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
	                keys: keys.to_owned().into_boxed_slice(),
	            },
	        ))
	    }

	    // this is to be called after parsing the command, so we do not match the verb
	    fn parse_set<'a>(&self, input: &'a [u8], header: Header) -> IResult<&'a [u8], Set> {
	        match self.parse_set_no_stats(input, header) {
	            Ok((input, request)) => {
	                SET.increment();
	                Ok((input, request))
	            }
	            Err(e) => {
	                if !e.is_incomplete() {
	                    SET.increment();
	                    SET_EX.increment();
	                }
	                Err(e)
	            }
	        }
	    }

	    fn parse_set_no_stats<'a>(&self, input: &'a [u8], header: Header) -> IResult<&'a [u8], Set> {
	        // validation

	        if header.key_len == 0 || header.key_len as usize > self.max_key_len {
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

	        let value_len = header.total_body_len as usize - header.key_len as usize - header.extras_len as usize;

	        if value_len == 0 || value_len > self.max_value_size {
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
	            },
	        ))
	    }

	    // this is to be called after parsing the command, so we do not match the verb
	    fn parse_get<'a>(&self, input: &'a [u8], header: Header) -> IResult<&'a [u8], Get> {
	        match self.parse_get_no_stats(input, header) {
	            Ok((input, request)) => {
	                GET.increment();
	                let keys = request.keys.len() as u64;
	                GET_KEY.add(keys);
	                let _ = GET_CARDINALITY.increment(keys);
	                Ok((input, request))
	            }
	            Err(e) => {
	                if !e.is_incomplete() {
	                    GET.increment();
	                    GET_EX.increment();
	                }
	                Err(e)
	            }
	        }
	    }

	    pub fn parse_request<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Request> {
	    	let (input, header) = self.parse_header(input)?;

	    	match header.opcode {
	    		0x00 => {
	    			let (input, request) = self.parse_get(input, header)?;
	    			Ok((input, Request::Get(request)))
	    		}
	    		0x01 => {
	    			let (input, request) = self.parse_set(input, header)?;
	    			Ok((input, Request::Set(request)))
	    		}
	    		_ => {
	    			Err(nom::Err::Failure(nom::error::Error::new(
	                    input,
	                    nom::error::ErrorKind::Tag,
	                )))
	    		}
	    	}
	    }
	}

	fn is_key_valid(key: &[u8]) -> bool {
		for i in 0..key.len() {
        	if key[i] == 32 {
        		return false;
        	}

        	if key[i] == 13 {
        		if let Some(10) = key.get(i + 1) {
        			return false;
        		}
        	}
        }

        true
	}

	impl Default for Parser {
	    fn default() -> Self {
	        Self {
	            max_value_size: DEFAULT_MAX_VALUE_SIZE,
	            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
	            max_key_len: DEFAULT_MAX_KEY_LEN,
	            time_type: TimeType::Memcache,
	        }
	    }
	}

	impl Parse<Request> for Parser {
	    fn parse(&self, buffer: &[u8]) -> Result<ParseOk<Request>, std::io::Error> {
	        match self.parse_request(buffer) {
	            Ok((input, request)) => Ok(ParseOk::new(request, buffer.len() - input.len())),
	            Err(Err::Incomplete(_)) => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
	            Err(_) => Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
	        }
	    }
	}

	#[cfg(test)]
	mod tests {
		use crate::Request;
		use crate::binary::request::Parser;

		#[test]
		fn fuzz_1() {
			let parser = Parser::default();

			let buffer = [128, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 1, 0, 249, 0, 0, 245, 138, 121, 120, 255, 65, 255, 255];

			match parser.parse_request(&buffer) {
				Ok((_, Request::Get(get))) => {
					assert_eq!(get.keys.len(), 1);
					assert_eq!(&*get.keys[0], &[255]);
				}
				_ => {
					panic!("wrong request type");
				}
			}
		}
	}
}

pub mod response {

}
