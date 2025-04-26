use super::*;

mod delete;
mod get;
mod set;

#[repr(C)]
pub(crate) struct ResponseHeader {
	pub(crate) magic: u8,
	pub(crate) opcode: u8,
	pub(crate) key_len: u16,
	pub(crate) extras_len: u8,
	pub(crate) data_type: u8,
	pub(crate) status: u16,
	pub(crate) total_body_len: u32,
	pub(crate) opaque: u32,
	pub(crate) cas: u64,
}

impl ResponseHeader {
	pub(crate) fn parse(input: &[u8]) -> IResult<&[u8], Self> {
    	let (remaining, h) = take(24usize)(input)?;

    	let header = Self {
    		magic: h[0],
    		opcode: h[1],
    		key_len: u16::from_be_bytes([h[2], h[3]]),
    		extras_len: h[4],
    		data_type: h[5],
    		status: u16::from_be_bytes([h[6], h[7]]),
    		total_body_len: u32::from_be_bytes([h[8], h[9], h[10], h[11]]),
    		opaque: u32::from_be_bytes([h[12], h[13], h[14], h[15]]),
    		cas: u64::from_be_bytes([h[16], h[17], h[18], h[19], h[20], h[21], h[22], h[23]]),
    	};

    	if header.magic != 0x81 {
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

        Ok((remaining, header))
    }
}
