use crate::binary::MagicValue;
use crate::binary::Opcode;
use bytes::BufMut;
use nom::{bytes::streaming::take, IResult};

#[repr(C)]
pub(crate) struct RequestHeader {
    pub(crate) magic: MagicValue,
    pub(crate) opcode: Opcode,
    pub(crate) key_len: u16,
    pub(crate) extras_len: u8,
    pub(crate) data_type: u8,
    pub(crate) _reserved: u16,
    pub(crate) total_body_len: u32,
    pub(crate) opaque: u32,
    pub(crate) cas: u64,
}

impl RequestHeader {
    pub(crate) fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (remaining, h) = take(24usize)(input)?;

        let header = Self {
            magic: MagicValue::from_u8(h[0]),
            opcode: Opcode::from_u8(h[1]),
            key_len: u16::from_be_bytes([h[2], h[3]]),
            extras_len: h[4],
            data_type: h[5],
            _reserved: u16::from_be_bytes([h[6], h[7]]),
            total_body_len: u32::from_be_bytes([h[8], h[9], h[10], h[11]]),
            opaque: u32::from_be_bytes([h[12], h[13], h[14], h[15]]),
            cas: u64::from_be_bytes([h[16], h[17], h[18], h[19], h[20], h[21], h[22], h[23]]),
        };

        if header.magic != MagicValue::Request {
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

        if header._reserved != 0x0000 {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        Ok((remaining, header))
    }

    /// Writes 24 bytes to the buffer
    pub fn write_to(&self, buffer: &mut dyn BufMut) {
        buffer.put_u8(self.magic.to_u8());
        buffer.put_u8(self.opcode.to_u8());
        buffer.put_u16(self.key_len);
        buffer.put_u8(self.extras_len);
        buffer.put_u8(self.data_type);
        buffer.put_u16(self._reserved);
        buffer.put_u32(self.total_body_len);
        buffer.put_u32(self.opaque);
        buffer.put_u64(self.cas);
    }

    fn with_opcode(opcode: Opcode) -> Self {
        Self {
            magic: MagicValue::Request,
            opcode,
            key_len: 0,
            extras_len: 0,
            data_type: 0,
            _reserved: 0,
            total_body_len: 0,
            opaque: 0,
            cas: 0,
        }
    }

    /// Returns the total request length which is the header length plus the
    /// request body length.
    pub fn request_len(&self) -> usize {
        24 + self.total_body_len as usize
    }

    /// Create a header for a `get` request.
    pub fn get(key_len: u16) -> Self {
        let mut header = Self::with_opcode(Opcode::Get);
        header.key_len = key_len;
        header.total_body_len = key_len as u32;

        header
    }

    /// Try to create a header for a `set` request. Returns an error if the
    /// key, value, and extras exceed the max request body size.
    pub fn set(key_len: u16, value_len: u32) -> Result<Self, std::io::Error> {
        const EXTRAS_LEN: u8 = 8;

        let total_body_len: u32 = (key_len as u64 + value_len as u64 + EXTRAS_LEN as u64)
            .try_into()
            .map_err(|_e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "request body too large for binary protocol",
                )
            })?;

        let mut header = Self::with_opcode(Opcode::Set);
        header.key_len = key_len;
        header.extras_len = EXTRAS_LEN;
        header.total_body_len = total_body_len;

        Ok(header)
    }

    /// Create a header for a `delete` request.
    pub fn delete(key_len: u16) -> Self {
        let mut header = Self::with_opcode(Opcode::Delete);
        header.key_len = key_len;
        header.total_body_len = key_len as u32;

        header
    }
}
