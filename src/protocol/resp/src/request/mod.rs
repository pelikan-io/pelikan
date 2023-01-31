// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::message::*;
use crate::*;
use logger::Klog;
use protocol_common::BufMut;
use protocol_common::Parse;
use protocol_common::ParseOk;
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
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
mod set;

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
pub use set::*;
// response codes for klog
const MISS: u8 = 0;
const HIT: u8 = 4;
const STORED: u8 = 5;
const NOT_STORED: u8 = 9;

pub type FieldValuePair = (Arc<[u8]>, Arc<[u8]>);

#[derive(Default, Clone)]
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
                        Some(b"set") | Some(b"SET") => Set::try_from(message).map(Request::from),
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
            Self::Set(r) => r.compose(buf),
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
    Set(Set),
}

impl Klog for Request {
    type Response = Response;

    fn klog(&self, response: &Self::Response) {
        match self {
            Request::Get(r) => r.klog(response),
            Request::Set(r) => r.klog(response),
            _ => (),
        }
    }
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

impl From<Set> for Request {
    fn from(other: Set) -> Self {
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
impl Default for ExpireTime {
    fn default() -> Self {
        ExpireTime::Seconds(0)
    }
}
impl Display for ExpireTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpireTime::Seconds(s) => write!(f, "{}s", s),
            ExpireTime::Milliseconds(ms) => write!(f, "{}ms", ms),
            ExpireTime::UnixSeconds(s) => write!(f, "{}unix_secs", s),
            ExpireTime::UnixMilliseconds(ms) => write!(f, "{}unix_ms", ms),
            ExpireTime::KeepTtl => write!(f, "keep_ttl"),
        }
    }
}

fn string_key(key: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(key)
}
