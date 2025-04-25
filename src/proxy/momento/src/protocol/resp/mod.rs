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
mod zcard;
mod zincrby;
mod zscore;
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
pub use zcard::*;
pub use zincrby::*;
pub use zscore::*;
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
