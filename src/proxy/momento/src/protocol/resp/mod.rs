// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use momento::MomentoError;
pub use protocol_resp::{Request, RequestParser};
use std::future::Future;

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
mod zcount;
mod zincrby;
mod zmscore;
mod zrange;
mod zrank;
mod zrem;
mod zrevrank;
mod zscore;
mod zunionstore;
use crate::error::ProxyError;

pub use self::lindex::*;
pub use self::llen::*;
pub use self::lpop::*;
pub use self::lpush::*;
pub use self::lrange::*;
pub use self::rpop::*;
pub use self::rpush::*;
pub use self::sdiff::*;
pub use self::sinter::*;
pub use self::sismember::*;
pub use self::smembers::*;
pub use self::srem::*;
pub use self::sunion::*;
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
pub use zcount::*;
pub use zincrby::*;
pub use zmscore::*;
pub use zrange::*;
pub use zrank::*;
pub use zrem::*;
pub use zrevrank::*;
pub use zscore::*;
pub use zunionstore::*;

pub(crate) fn momento_error_to_resp_error(buf: &mut Vec<u8>, command: &str, error: MomentoError) {
    use crate::BACKEND_EX;

    BACKEND_EX.increment();

    error!("backend error for {command}: {error}");
    buf.extend_from_slice(format!("-ERR backend error: {error}\r\n").as_bytes());
}

async fn update_method_metrics<T, E>(
    count: &metriken::Counter,
    count_ex: &metriken::Counter,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    count.increment();
    future.await.inspect_err(|_| {
        count_ex.increment();
    })
}

fn parse_sorted_set_score(score: &[u8]) -> Result<f64, std::io::Error> {
    // Momento calls cannot accept f64::INFINITY, so using f64::MAX instead
    if score == "-inf".as_bytes() {
        Ok(f64::MIN)
    } else if score == "+inf".as_bytes() {
        Ok(f64::MAX)
    } else if let Some(float) = std::str::from_utf8(score)
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "score string is not valid utf8")
        })?
        .parse::<f64>()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "score string is not a f64"))
        .map(Some)?
    {
        return Ok(float);
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "score string is not a valid f64",
        ));
    }
}

fn parse_score_boundary_as_integer(value: &[u8]) -> Result<i32, ProxyError> {
    let index = std::str::from_utf8(value)
        .map_err(|_| {
            ProxyError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "ZRANGE index is not valid utf8",
            ))
        })?
        .parse::<i32>()
        .map_err(|_| {
            ProxyError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "ZRANGE index is not an integer",
            ))
        })?;
    Ok(index)
}

// Returns a tuple of (value, is_exclusive)
fn parse_score_boundary_as_float(value: &[u8]) -> Result<(f64, bool), ProxyError> {
    // First check if the value is +inf or -inf
    if value == b"+inf" {
        return Ok((f64::INFINITY, false));
    }
    if value == b"-inf" {
        return Ok((f64::NEG_INFINITY, false));
    }

    // Otherwise, split apart '(' and the value if present
    let (inclusive_symbol, number) = if value[0] == b'(' {
        (true, &value[1..])
    } else {
        (false, value)
    };

    let score = std::str::from_utf8(number)
        .map_err(|_| {
            ProxyError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "ZRANGE score is not valid utf8",
            ))
        })?
        .parse::<f64>()
        .map_err(|_| {
            ProxyError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "ZRANGE score is not a float",
            ))
        })?;

    if inclusive_symbol {
        Ok((score, true))
    } else {
        Ok((score, false))
    }
}
