use crate::binary::{MagicValue, Opcode};
use bytes::BufMut;
use nom::{bytes::streaming::take, IResult};

pub(crate) struct ResponseHeader {
    pub(crate) magic: MagicValue,
    pub(crate) opcode: Opcode,
    pub(crate) key_len: u16,
    pub(crate) extras_len: u8,
    pub(crate) data_type: u8,
    pub(crate) status: ResponseStatus,
    pub(crate) total_body_len: u32,
    pub(crate) opaque: u32,
    pub(crate) cas: u64,
}

impl ResponseHeader {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (remaining, h) = take(24usize)(input)?;

        let header = Self {
            magic: MagicValue::from_u8(h[0]),
            opcode: Opcode::from_u8(h[1]),
            key_len: u16::from_be_bytes([h[2], h[3]]),
            extras_len: h[4],
            data_type: h[5],
            status: ResponseStatus::from_u16(u16::from_be_bytes([h[6], h[7]])),
            total_body_len: u32::from_be_bytes([h[8], h[9], h[10], h[11]]),
            opaque: u32::from_be_bytes([h[12], h[13], h[14], h[15]]),
            cas: u64::from_be_bytes([h[16], h[17], h[18], h[19], h[20], h[21], h[22], h[23]]),
        };

        if header.magic != MagicValue::Response {
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

    /// Writes 24 bytes to the buffer
    pub fn write_to(&self, buffer: &mut dyn BufMut) {
        buffer.put_u8(self.magic.to_u8());
        buffer.put_u8(self.opcode.to_u8());
        buffer.put_u16(self.key_len);
        buffer.put_u8(self.extras_len);
        buffer.put_u8(self.data_type);
        buffer.put_u16(self.status.to_u16());
        buffer.put_u32(self.total_body_len);
        buffer.put_u32(self.opaque);
        buffer.put_u64(self.cas);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseStatus {
    Unknown(u16),
    NoError,
    KeyNotFound,
    KeyExists,
    ValueTooLarge,
    InvalidArguments,
    ItemNotStored,
    IncrDecrOnNonNumericValue,
    VBucketBelongsToAnotherServer,
    AuthenticationError,
    AuthenticationContinue,
    UnknownCommand,
    OutOfMemory,
    NotSupported,
    InternalError,
    Busy,
    TemporaryFailure,
}

impl ResponseStatus {
    pub fn from_u16(value: u16) -> Self {
        match value {
            0x0000 => ResponseStatus::NoError,
            0x0001 => ResponseStatus::KeyNotFound,
            0x0002 => ResponseStatus::KeyExists,
            0x0003 => ResponseStatus::ValueTooLarge,
            0x0004 => ResponseStatus::InvalidArguments,
            0x0005 => ResponseStatus::ItemNotStored,
            0x0006 => ResponseStatus::IncrDecrOnNonNumericValue,
            0x0007 => ResponseStatus::VBucketBelongsToAnotherServer,
            0x0008 => ResponseStatus::AuthenticationError,
            0x0009 => ResponseStatus::AuthenticationContinue,
            0x0081 => ResponseStatus::UnknownCommand,
            0x0082 => ResponseStatus::OutOfMemory,
            0x0083 => ResponseStatus::NotSupported,
            0x0084 => ResponseStatus::InternalError,
            0x0085 => ResponseStatus::Busy,
            0x0086 => ResponseStatus::TemporaryFailure,
            other => ResponseStatus::Unknown(other),
        }
    }

    pub fn to_u16(self) -> u16 {
        match self {
            ResponseStatus::Unknown(other) => other,
            ResponseStatus::NoError => 0x0000,
            ResponseStatus::KeyNotFound => 0x0001,
            ResponseStatus::KeyExists => 0x0002,
            ResponseStatus::ValueTooLarge => 0x0003,
            ResponseStatus::InvalidArguments => 0x0004,
            ResponseStatus::ItemNotStored => 0x0005,
            ResponseStatus::IncrDecrOnNonNumericValue => 0x0006,
            ResponseStatus::VBucketBelongsToAnotherServer => 0x0007,
            ResponseStatus::AuthenticationError => 0x0008,
            ResponseStatus::AuthenticationContinue => 0x0009,
            ResponseStatus::UnknownCommand => 0x0081,
            ResponseStatus::OutOfMemory => 0x0082,
            ResponseStatus::NotSupported => 0x0083,
            ResponseStatus::InternalError => 0x0084,
            ResponseStatus::Busy => 0x0085,
            ResponseStatus::TemporaryFailure => 0x0086,
        }
    }

    pub fn as_empty_response(&self, opcode: Opcode) -> ResponseHeader {
        ResponseHeader {
            magic: MagicValue::Response,
            opcode,
            key_len: 0,
            extras_len: 0,
            data_type: 0,
            status: *self,
            total_body_len: 0,
            opaque: 0,
            cas: 0,
        }
    }
}
