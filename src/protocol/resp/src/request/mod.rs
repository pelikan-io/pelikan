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
mod del;
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
mod lpop;
mod lpush;
mod lrange;
mod ltrim;
mod rpop;
mod rpush;
mod sadd;
mod sdiff;
mod set;
mod sinter;
mod sismember;
mod smembers;
mod srem;
mod sunion;
mod zadd;
mod zcard;
mod zincrby;
mod zmscore;
mod zrange;
mod zrank;
mod zrem;
mod zscore;
pub use self::lindex::*;
pub use self::llen::*;
pub use self::lpop::*;
pub use self::lpush::*;
pub use self::lrange::*;
pub use self::ltrim::*;
pub use self::rpop::*;
pub use self::rpush::*;
pub use self::sdiff::*;
pub use self::sinter::*;
pub use self::sismember::*;
pub use self::smembers::*;
pub use self::srem::*;
pub use self::sunion::*;
pub use badd::*;
pub use del::*;
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
pub use zadd::*;
pub use zcard::*;
pub use zincrby::*;
pub use zmscore::*;
pub use zrange::*;
pub use zrank::*;
pub use zrem::*;
pub use zscore::*;

/// response codes for klog
/// matches Memcache protocol response codes for compatibility with existing tools
/// [crate::memcache::MISS]
#[allow(dead_code)]
enum ResponseCode {
    Miss = 0,
    Hit = 4,
    Stored = 5,
    Exists = 6,
    Deleted = 7,
    NotFound = 8,
    NotStored = 9,
}

pub type FieldValuePair = (Arc<[u8]>, Arc<[u8]>);
pub type ScoreMemberPair = (Arc<[u8]>, Arc<[u8]>);

/// Macro to deal with the boilerplate around the Request enum.
macro_rules! decl_request {
    {
        $vis:vis enum $name:ident {
            $(
                $variant:ident($type:ty) => $command:literal
            ),* $(,)?
        }
    } => {
        #[derive(Debug, PartialEq, Eq)]
        $vis enum $name {
            $( $variant($type), )*
        }

        impl Parse<$name> for RequestParser {
            fn parse(&self, buffer: &[u8]) -> Result<ParseOk<$name>, Error> {
                use crate::parse::*;

                let mut parser = Parser::new(buffer);
                let result = CommandParser::new(&mut parser)
                    .and_then(|command| command.parse_message());

                let message = match result {
                    Ok(message) => message,
                    Err(ParseError::Incomplete) => return Err(Error::from(ErrorKind::WouldBlock)),
                    Err(e) => return Err(Error::new(ErrorKind::Other, e.to_string())),
                };
                let consumed = (parser.remaining().as_ptr() as usize) - (buffer.as_ptr() as usize);

                let array = match &message {
                    Message::Array(Array { inner: Some(array)}) if !array.is_empty() => array,
                    _ => return Err(Error::new(ErrorKind::Other, "malformed command"))
                };

                let command = match &array[0] {
                    Message::BulkString(BulkString { inner: Some(command) }) => command,
                    // all valid commands are encoded as a bulk string
                    _ => return Err(Error::new(ErrorKind::Other, "malformed command"))
                };

                let response = match command {
                    $( _ if command.eq_ignore_ascii_case($command.as_bytes()) => <$type>::try_from(message)?.into(), )*
                    _ => return Err(Error::new(ErrorKind::Other, "unknown command"))
                };

                Ok(ParseOk::new(response, consumed))
            }
        }

        impl $name {
            pub fn command(&self) -> &'static str {
                match self {
                    $( Self::$variant(_) => $command, )*
                }
            }
        }

        impl Compose for $name {
            fn compose(&self, buf: &mut dyn BufMut) -> usize {
                match self {
                    $( Self::$variant(v) => v.compose(buf), )*
                }
            }
        }

        $(
            impl From<$type> for $name {
                fn from(value: $type) -> Self {
                    Self::$variant(value)
                }
            }
        )*
    }
}

decl_request! {
    pub enum Request {
        BtreeAdd(BtreeAdd) => "badd",
        Del(Del) => "del",
        Get(Get) => "get",
        HashDelete(HashDelete) => "hdel",
        HashExists(HashExists) => "hexists",
        HashGet(HashGet) => "hget",
        HashGetAll(HashGetAll) => "hgetall",
        HashKeys(HashKeys) => "hkeys",
        HashLength(HashLength) => "hlen",
        HashMultiGet(HashMultiGet) => "hmget",
        HashSet(HashSet) => "hset",
        HashValues(HashValues) => "hvals",
        HashIncrBy(HashIncrBy) => "hincrby",
        ListIndex(ListIndex) => "lindex",
        ListLen(ListLen) => "llen",
        ListPop(ListPop) => "lpop",
        ListPopBack(ListPopBack) => "rpop",
        ListRange(ListRange) => "lrange",
        ListPush(ListPush) => "lpush",
        ListPushBack(ListPushBack) => "rpush",
        ListTrim(ListTrim) => "ltrim",
        Set(Set) => "set",
        SetAdd(SetAdd) => "sadd",
        SetRem(SetRem) => "srem",
        SetDiff(SetDiff) => "sdiff",
        SetUnion(SetUnion) => "sunion",
        SetIntersect(SetIntersect) => "sinter",
        SetMembers(SetMembers) => "smembers",
        SetIsMember(SetIsMember) => "sismember",
        SortedSetCardinality(SortedSetCardinality) => "zcard",
        SortedSetIncrement(SortedSetIncrement) => "zincrby",
        SortedSetScore(SortedSetScore) => "zscore",
        SortedSetMultiScore(SortedSetMultiScore) => "zmscore",
        SortedSetRemove(SortedSetRemove) => "zrem",
        SortedSetRank(SortedSetRank) => "zrank",
        SortedSetRange(SortedSetRange) => "zrange",
        SortedSetAdd(SortedSetAdd) => "zadd",
    }
}

#[derive(Clone, Default)]
pub struct RequestParser {}

impl RequestParser {
    pub fn new() -> Self {
        Self {}
    }
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
    pub fn del(keys: &[&[u8]]) -> Self {
        Self::Del(Del::new(keys))
    }

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

    pub fn sorted_set_cardinality(key: &[u8]) -> Self {
        Self::SortedSetCardinality(SortedSetCardinality::new(key))
    }

    pub fn sorted_set_increment(key: &[u8], increment: &[u8], member: &[u8]) -> Self {
        Self::SortedSetIncrement(SortedSetIncrement::new(key, increment, member))
    }

    pub fn sorted_set_score(key: &[u8], member: &[u8]) -> Self {
        Self::SortedSetScore(SortedSetScore::new(key, member))
    }

    pub fn sorted_set_multi_score(key: &[u8], members: &[&[u8]]) -> Self {
        Self::SortedSetMultiScore(SortedSetMultiScore::new(key, members))
    }

    pub fn sorted_set_remove(key: &[u8], members: &[&[u8]]) -> Self {
        Self::SortedSetRemove(SortedSetRemove::new(key, members))
    }

    pub fn sorted_set_rank(key: &[u8], member: &[u8], with_score: bool) -> Self {
        Self::SortedSetRank(SortedSetRank::new(key, member, with_score))
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
            ExpireTime::Seconds(s) => write!(f, "{s}s"),
            ExpireTime::Milliseconds(ms) => write!(f, "{ms}ms"),
            ExpireTime::UnixSeconds(s) => write!(f, "{s}unix_secs"),
            ExpireTime::UnixMilliseconds(ms) => write!(f, "{ms}unix_ms"),
            ExpireTime::KeepTtl => write!(f, "keep_ttl"),
        }
    }
}

fn string_key(key: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(key)
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
