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
mod hincrby;
mod hkeys;
mod hlen;
mod hmget;
mod hset;
mod hvals;
mod lindex;
mod llen;
mod sadd;
mod sdiff;
mod set;
mod srem;

pub use self::lindex::*;
pub use self::llen::*;
pub use self::sdiff::*;
pub use self::srem::*;
pub use badd::*;
pub use get::*;
pub use hdel::*;
pub use hexists::*;
pub use hget::*;
pub use hgetall::*;
pub use hincrby::*;
pub use hkeys::*;
pub use hlen::*;
pub use hmget::*;
pub use hset::*;
pub use hvals::*;
pub use sadd::*;
pub use set::*;

pub type FieldValuePair = (Arc<[u8]>, Arc<[u8]>);

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

            if !remaining.starts_with(b"\r\n") {
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
                    Message::BulkString(c) => match c.inner.as_ref().map(|v| v.as_ref()) {
                        Some(b"badd") | Some(b"BADD") => {
                            BtreeAdd::try_from(message).map(Request::from)
                        }
                        Some(b"get") | Some(b"GET") => Get::try_from(message).map(Request::from),
                        Some(b"hdel") | Some(b"HDEL") => {
                            HashDelete::try_from(message).map(Request::from)
                        }
                        Some(b"hexists") | Some(b"HEXISTS") => {
                            HashExists::try_from(message).map(Request::from)
                        }
                        Some(b"hget") | Some(b"HGET") => {
                            HashGet::try_from(message).map(Request::from)
                        }
                        Some(b"hgetall") | Some(b"HGETALL") => {
                            HashGetAll::try_from(message).map(Request::from)
                        }
                        Some(b"hkeys") | Some(b"HKEYS") => {
                            HashKeys::try_from(message).map(Request::from)
                        }
                        Some(b"hlen") | Some(b"HLEN") => {
                            HashLength::try_from(message).map(Request::from)
                        }
                        Some(b"hmget") | Some(b"HMGET") => {
                            HashMultiGet::try_from(message).map(Request::from)
                        }
                        Some(b"hset") | Some(b"HSET") => {
                            HashSet::try_from(message).map(Request::from)
                        }
                        Some(b"hvals") | Some(b"HVALS") => {
                            HashValues::try_from(message).map(Request::from)
                        }
                        Some(b"hincrby") | Some(b"HINCRBY") => {
                            HashIncrBy::try_from(message).map(Request::from)
                        }
                        Some(b"lindex") | Some(b"LINDEX") => {
                            ListIndex::try_from(message).map(Request::from)
                        }
                        Some(b"llen") | Some(b"LLEN") => ListLen::try_from(message).map(From::from),
                        Some(b"set") | Some(b"SET") => Set::try_from(message).map(Request::from),
                        Some(b"sadd") | Some(b"SADD") => {
                            SetAdd::try_from(message).map(Request::from)
                        }
                        Some(b"srem") | Some(b"SREM") => SetRem::try_from(message).map(From::from),
                        Some(b"sdiff") | Some(b"SDIFF") => {
                            SetDiff::try_from(message).map(From::from)
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
            Self::BtreeAdd(r) => r.compose(buf),
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
            Self::HashIncrBy(r) => r.compose(buf),
            Self::ListIndex(r) => r.compose(buf),
            Self::ListLen(r) => r.compose(buf),
            Self::Set(r) => r.compose(buf),
            Self::SetAdd(r) => r.compose(buf),
            Self::SetRem(r) => r.compose(buf),
            Self::SetDiff(r) => r.compose(buf),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Request {
    BtreeAdd(BtreeAdd),
    Get(Get),
    HashDelete(HashDelete),
    HashExists(HashExists),
    HashGet(HashGet),
    HashGetAll(HashGetAll),
    HashKeys(HashKeys),
    HashLength(HashLength),
    HashMultiGet(HashMultiGet),
    HashSet(HashSet),
    HashValues(HashValues),
    HashIncrBy(HashIncrBy),
    ListIndex(ListIndex),
    ListLen(ListLen),
    Set(Set),
    SetAdd(SetAdd),
    SetRem(SetRem),
    SetDiff(SetDiff),
}

impl Request {
    pub fn get(key: &[u8]) -> Self {
        Self::Get(Get::new(key))
    }

    pub fn hash_delete(key: &[u8], fields: &[&[u8]]) -> Self {
        Self::HashDelete(HashDelete::new(key, fields))
    }

    pub fn hash_exists(key: &[u8], field: &[u8]) -> Self {
        Self::HashExists(HashExists::new(key, field))
    }

    pub fn hash_get(key: &[u8], field: &[u8]) -> Self {
        Self::HashGet(HashGet::new(key, field))
    }

    pub fn hash_get_all(key: &[u8]) -> Self {
        Self::HashGetAll(HashGetAll::new(key))
    }

    pub fn hash_keys(key: &[u8]) -> Self {
        Self::HashKeys(HashKeys::new(key))
    }

    pub fn hash_length(key: &[u8]) -> Self {
        Self::HashLength(HashLength::new(key))
    }

    pub fn hash_multi_get(key: &[u8], fields: &[&[u8]]) -> Self {
        Self::HashMultiGet(HashMultiGet::new(key, fields))
    }

    pub fn hash_set(key: &[u8], data: &[(&[u8], &[u8])]) -> Self {
        Self::HashSet(HashSet::new(key, data))
    }

    pub fn hash_values(key: &[u8]) -> Self {
        Self::HashValues(HashValues::new(key))
    }

    pub fn hash_incrby(key: &[u8], field: &[u8], increment: i64) -> Self {
        Self::HashIncrBy(HashIncrBy::new(key, field, increment))
    }

    pub fn set(
        key: &[u8],
        value: &[u8],
        expire_time: Option<ExpireTime>,
        mode: SetMode,
        get_old: bool,
    ) -> Self {
        Self::Set(Set::new(key, value, expire_time, mode, get_old))
    }

    pub fn command(&self) -> &'static str {
        match self {
            Self::BtreeAdd(_) => "badd",
            Self::Get(_) => "get",
            Self::HashDelete(_) => "hdel",
            Self::HashExists(_) => "hexists",
            Self::HashGet(_) => "hget",
            Self::HashGetAll(_) => "hgetall",
            Self::HashKeys(_) => "hkeys",
            Self::HashLength(_) => "hlen",
            Self::HashMultiGet(_) => "hmget",
            Self::HashSet(_) => "hset",
            Self::HashValues(_) => "hvals",
            Self::HashIncrBy(_) => "hincrby",
            Self::ListIndex(_) => "lindex",
            Self::ListLen(_) => "llen",
            Self::Set(_) => "set",
            Self::SetAdd(_) => "sadd",
            Self::SetRem(_) => "srem",
            Self::SetDiff(_) => "sdiff",
        }
    }
}

impl From<BtreeAdd> for Request {
    fn from(other: BtreeAdd) -> Self {
        Self::BtreeAdd(other)
    }
}

impl From<Get> for Request {
    fn from(other: Get) -> Self {
        Self::Get(other)
    }
}

impl From<HashDelete> for Request {
    fn from(other: HashDelete) -> Self {
        Self::HashDelete(other)
    }
}

impl From<HashExists> for Request {
    fn from(other: HashExists) -> Self {
        Self::HashExists(other)
    }
}

impl From<HashGet> for Request {
    fn from(other: HashGet) -> Self {
        Self::HashGet(other)
    }
}

impl From<HashGetAll> for Request {
    fn from(other: HashGetAll) -> Self {
        Self::HashGetAll(other)
    }
}

impl From<HashKeys> for Request {
    fn from(other: HashKeys) -> Self {
        Self::HashKeys(other)
    }
}

impl From<HashLength> for Request {
    fn from(other: HashLength) -> Self {
        Self::HashLength(other)
    }
}

impl From<HashMultiGet> for Request {
    fn from(other: HashMultiGet) -> Self {
        Self::HashMultiGet(other)
    }
}

impl From<HashSet> for Request {
    fn from(other: HashSet) -> Self {
        Self::HashSet(other)
    }
}

impl From<HashValues> for Request {
    fn from(other: HashValues) -> Self {
        Self::HashValues(other)
    }
}

impl From<HashIncrBy> for Request {
    fn from(value: HashIncrBy) -> Self {
        Self::HashIncrBy(value)
    }
}

impl From<ListIndex> for Request {
    fn from(value: ListIndex) -> Self {
        Self::ListIndex(value)
    }
}

impl From<ListLen> for Request {
    fn from(value: ListLen) -> Self {
        Self::ListLen(value)
    }
}

impl From<Set> for Request {
    fn from(other: Set) -> Self {
        Self::Set(other)
    }
}

impl From<SetAdd> for Request {
    fn from(value: SetAdd) -> Self {
        Self::SetAdd(value)
    }
}

impl From<SetRem> for Request {
    fn from(value: SetRem) -> Self {
        Self::SetRem(value)
    }
}

impl From<SetDiff> for Request {
    fn from(value: SetDiff) -> Self {
        Self::SetDiff(value)
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

#[cfg(test)]
mod tests {
    use crate::RequestParser;
    use protocol_common::Parse;

    #[test]
    fn it_should_not_panic_on_newline_delimited_get_key() {
        let parser = RequestParser::new();
        assert!(parser.parse(b"GET test\n").is_err());
    }
}
