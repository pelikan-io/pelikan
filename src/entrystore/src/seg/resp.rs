// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This module defines how `Seg` storage will be used to execute `Redis`
//! storage commands.

use super::*;
use protocol_common::*;

use protocol_resp::*;

use std::time::Duration;

impl Execute<Request, Response> for Seg {
    fn execute(&mut self, request: &Request) -> Response {
        match request {
            Request::Get(get) => self.get(get),
            Request::Set(set) => self.set(set),
            _ => Response::error("not supported"),
        }
    }
}

impl Storage for Seg {
    fn get(&mut self, get: &Get) -> Response {
        if let Some(item) = self.data.get(get.key()) {
            match item.value() {
                seg::Value::Bytes(b) => Response::bulk_string(b),
                seg::Value::U64(v) => Response::bulk_string(format!("{v}").as_bytes()),
            }
        } else {
            Response::null()
        }
    }

    fn set(&mut self, set: &Set) -> Response {
        let ttl = match set.expire_time().unwrap_or(ExpireTime::default()) {
            ExpireTime::Seconds(n) => n,
            _ => 0,
        };

        if self
            .data
            .insert(set.key(), set.value(), None, Duration::from_secs(ttl))
            .is_ok()
        {
            Response::simple_string("OK")
        } else {
            Response::error("not stored")
        }
    }
}
