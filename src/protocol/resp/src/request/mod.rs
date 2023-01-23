// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::message::*;
use crate::*;
use protocol_common::BufMut;
use protocol_common::Parse;
use protocol_common::ParseOk;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

mod badd;
mod get;
mod hdel;
mod hexists;
mod hget;
mod hgetall;
mod hkeys;
mod hlen;
mod hmget;
mod hset;
mod hvals;
mod set;

pub use badd::*;
pub use get::*;
pub use hdel::*;
pub use hexists::*;
pub use hget::*;
pub use hgetall::*;
pub use hkeys::*;
pub use hlen::*;
pub use hmget::*;
pub use hset::*;
pub use hvals::*;
pub use set::*;


type ArcKeyValuePair = (Arc<[u8]>, Arc<[u8]>);
type ArcFieldValuePair = (Arc<[u8]>, Arc<[u8]>);

#[derive(Default)]
pub struct RequestParser {
    message_parser: MessageParser,
}

impl RequestParser {
    pub fn new() -> Self {
        Self {
            message_parser: MessageParser {},
        }
    }
}

impl Parse<Request> for RequestParser {
    fn parse(&self, buffer: &[u8]) -> Result<ParseOk<Request>, Error> {
        // we have two different parsers, one for RESP and one for inline
        // both require that there's at least one character in the buffer
        if buffer.is_empty() {
            return Err(Error::from(ErrorKind::WouldBlock));
        }

        let (message, consumed) = if matches!(buffer[0], b'*' | b'+' | b'-' | b':' | b'$') {
            self.message_parser.parse(buffer).map(|v| {
                let c = v.consumed();
                (v.into_inner(), c)
            })?
        } else {
            let mut remaining = buffer;

            let mut message = Vec::new();

            while let Ok((r, string)) = string(remaining) {
                message.push(Message::BulkString(BulkString {
                    inner: Some(string.into()),
                }));
                remaining = r;

                if let Ok((r, _)) = space1(remaining) {
                    remaining = r;
                } else {
                    break;
                }
            }

            if &remaining[0..2] != b"\r\n" {
                return Err(Error::from(ErrorKind::WouldBlock));
            }

            let message = Message::Array(Array {
                inner: Some(message),
            });

            let consumed = (buffer.len() - remaining.len()) + 2;

            (message, consumed)
        };

        match &message {
            Message::Array(array) => {
                if array.inner.is_none() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }

                let array = array.inner.as_ref().unwrap();

                if array.is_empty() {
                    return Err(Error::new(ErrorKind::Other, "malformed command"));
                }

                match &array[0] {
                    Message::BulkString(c) => match c.inner.as_ref().map(|v| v.as_ref().as_ref()) {
                        Some(b"badd") | Some(b"BADD") => {
                            BAddRequest::try_from(message).map(Request::from)
                        }
                        Some(b"get") | Some(b"GET") => {
                            GetRequest::try_from(message).map(Request::from)
                        }
                        Some(b"hdel") | Some(b"HDEL") => {
                            HashDeleteRequest::try_from(message).map(Request::from)
                        }
                        Some(b"hexists") | Some(b"HEXISTS") => {
                            HashExistsRequest::try_from(message).map(Request::from)
                        }
                        Some(b"hget") | Some(b"HGET") => {
                            HashGetRequest::try_from(message).map(Request::from)
                        }
                        Some(b"hgetall") | Some(b"HGETALL") => {
                            HashGetAllRequest::try_from(message).map(Request::from)
                        }
                        Some(b"hkeys") | Some(b"HKEYS") => {
                            HashKeysRequest::try_from(message).map(Request::from)
                        }
                        Some(b"hlen") | Some(b"HLEN") => {
                            HashLengthRequest::try_from(message).map(Request::from)
                        }
                        Some(b"hmget") | Some(b"HMGET") => {
                            HashMultiGetRequest::try_from(message).map(Request::from)
                        }
                        Some(b"hset") | Some(b"HSET") => {
                            HashSetRequest::try_from(message).map(Request::from)
                        }
                        Some(b"hvals") | Some(b"HVALS") => {
                            HashValuesRequest::try_from(message).map(Request::from)
                        }
                        Some(b"set") | Some(b"SET") => {
                            SetRequest::try_from(message).map(Request::from)
                        }
                        _ => Err(Error::new(ErrorKind::Other, "unknown command")),
                    },
                    _ => {
                        // all valid commands are encoded as a bulk string
                        Err(Error::new(ErrorKind::Other, "malformed command"))
                    }
                }
            }
            _ => {
                // all valid requests are arrays
                Err(Error::new(ErrorKind::Other, "malformed command"))
            }
        }
        .map(|v| ParseOk::new(v, consumed))
    }
}

impl Compose for Request {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        match self {
            Self::BAdd(r) => r.compose(buf),
            Self::Get(r) => r.compose(buf),
            Self::HashDelete(r) => r.compose(buf),
            Self::HashExists(r) => r.compose(buf),
            Self::HashGet(r) => r.compose(buf),
            Self::HashGetAll(r) => r.compose(buf),
            Self::HashKeys(r) => r.compose(buf),
            Self::HashLength(r) => r.compose(buf),
            Self::HashMultiGet(r) => r.compose(buf),
            Self::HashSet(r) => r.compose(buf),
            Self::HashValues(r) => r.compose(buf),
            Self::Set(r) => r.compose(buf),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Request {
    BAdd(BAddRequest),
    Get(GetRequest),
    HashDelete(HashDeleteRequest),
    HashExists(HashExistsRequest),
    HashGet(HashGetRequest),
    HashGetAll(HashGetAllRequest),
    HashKeys(HashKeysRequest),
    HashLength(HashLengthRequest),
    HashMultiGet(HashMultiGetRequest),
    HashSet(HashSetRequest),
    HashValues(HashValuesRequest),
    Set(SetRequest),
}

impl From<BAddRequest> for Request {
    fn from(other: BAddRequest) -> Self {
        Self::BAdd(other)
    }
}

impl From<GetRequest> for Request {
    fn from(other: GetRequest) -> Self {
        Self::Get(other)
    }
}

impl From<HashDeleteRequest> for Request {
    fn from(other: HashDeleteRequest) -> Self {
        Self::HashDelete(other)
    }
}

impl From<HashExistsRequest> for Request {
    fn from(other: HashExistsRequest) -> Self {
        Self::HashExists(other)
    }
}

impl From<HashGetRequest> for Request {
    fn from(other: HashGetRequest) -> Self {
        Self::HashGet(other)
    }
}

impl From<HashGetAllRequest> for Request {
    fn from(other: HashGetAllRequest) -> Self {
        Self::HashGetAll(other)
    }
}

impl From<HashKeysRequest> for Request {
    fn from(other: HashKeysRequest) -> Self {
        Self::HashKeys(other)
    }
}

impl From<HashLengthRequest> for Request {
    fn from(other: HashLengthRequest) -> Self {
        Self::HashLength(other)
    }
}

impl From<HashMultiGetRequest> for Request {
    fn from(other: HashMultiGetRequest) -> Self {
        Self::HashMultiGet(other)
    }
}

impl From<HashSetRequest> for Request {
    fn from(other: HashSetRequest) -> Self {
        Self::HashSet(other)
    }
}

impl From<HashValuesRequest> for Request {
    fn from(other: HashValuesRequest) -> Self {
        Self::HashValues(other)
    }
}

impl From<SetRequest> for Request {
    fn from(other: SetRequest) -> Self {
        Self::Set(other)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    BAdd,
    Get,
    HashDelete,
    HashExists,
    HashGet,
    HashGetAll,
    HashKeys,
    HashLength,
    HashMultiGet,
    HashSet,
    HashValues,
    Set,
}

impl TryFrom<&[u8]> for Command {
    type Error = ();

    fn try_from(other: &[u8]) -> Result<Self, ()> {
        match other {
            b"badd" | b"BADD" => Ok(Command::BAdd),
            b"get" | b"GET" => Ok(Command::Get),
            b"hdel" | b"HDEL" => Ok(Command::HashDelete),
            b"hexists" | b"HEXISTS" => Ok(Command::HashExists),
            b"hget" | b"HGET" => Ok(Command::HashGet),
            b"hgetall" | b"HGETALL" => Ok(Command::HashGetAll),
            b"hkeys" | b"HKEYS" => Ok(Command::HashKeys),
            b"hlen" | b"HLEN" => Ok(Command::HashLength),
            b"hmget" | b"HMGET" => Ok(Command::HashMultiGet),
            b"hset" | b"HSET" => Ok(Command::HashSet),
            b"hvals" | b"HVALS" => Ok(Command::HashValues),
            b"set" | b"SET" => Ok(Command::Set),
            _ => Err(()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum ExpireTime {
    Seconds(u64),
    Milliseconds(u64),
    UnixSeconds(u64),
    UnixMilliseconds(u64),
    KeepTtl,
}
