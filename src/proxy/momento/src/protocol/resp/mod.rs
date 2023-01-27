// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use momento::response::MomentoError;
pub use protocol_resp::{Request, RequestParser};

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

fn momento_error_to_resp_error(buf: &mut Vec<u8>, command: &str, error: MomentoError) {
    use crate::{BACKEND_EX, BACKEND_EX_RATE_LIMITED, BACKEND_EX_TIMEOUT};

    BACKEND_EX.increment();

    match error {
        MomentoError::LimitExceeded(_) => {
            BACKEND_EX_RATE_LIMITED.increment();
            buf.extend_from_slice(b"-ERR ratelimit exceeded\r\n");
        }
        MomentoError::Timeout(_) => {
            BACKEND_EX_TIMEOUT.increment();
            buf.extend_from_slice(b"-ERR backend timeout\r\n");
        }
        e => {
            error!("error for {}: {}", command, e);
            buf.extend_from_slice(b"-ERR backend error\r\n");
        }
    }
}
