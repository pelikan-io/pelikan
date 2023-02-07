// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This module defines how `Seg` storage will be used to execute `Redis`
//! storage commands.

use super::*;
use protocol_common::*;

use protocol_resp::*;

use seg::Value::{Bytes, U64};
use std::time::Duration;
use storage_types::parse_signed_redis;

impl Execute<Request, Response> for Seg {
    fn execute(&mut self, request: &Request) -> Response {
        match request {
            Request::Get(get) => self.get(get),
            Request::Set(set) => self.set(set),
            Request::Incr(incr) => self.incr(incr),
            _ => Response::error("not supported"),
        }
    }
}

impl Storage for Seg {
    fn get(&mut self, get: &Get) -> Response {
        if let Some(item) = self.data.get(get.key()) {
            match item.value() {
                seg::Value::Bytes(b) => Response::bulk_string(b),
                seg::Value::U64(v) => Response::bulk_string(format!("{}", v).as_bytes()),
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
        let value = match parse_signed_redis(set.value()) {
            Some(integer) => U64(integer as u64),
            None => Bytes(set.value()),
        };

        if self
            .data
            .insert(set.key(), value, None, Duration::from_secs(ttl))
            .is_ok()
        {
            Response::simple_string("OK")
        } else {
            Response::error("not stored")
        }
    }

    fn incr(&mut self, incr: &Incr) -> Response {
        if let Some(mut item) = self.data.get(incr.key()) {
            match item.value() {
                seg::Value::Bytes(b) => Response::error("wrong data type"),
                seg::Value::U64(uint) => {
                    if let Some(incremented) = (uint as i64).checked_add(1) {
                        item.wrapping_add(1);
                        Response::integer(incremented)
                    } else {
                        Response::error("increment or decrement would overflow")
                    }
                }
            }
        } else {
            if self
                .data
                .insert(incr.key(), 1 as u64, None, Duration::from_secs(0))
                .is_ok()
            {
                Response::integer(1)
            } else {
                Response::error("not stored")
            }
        }
    }
}
