// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This module defines how `Seg` storage will be used to execute `Redis`
//! storage commands.

use super::*;
use protocol_common::*;

use protocol_resp::*;

use protocol_common::parsing::parse_signed_redis;
use seg::Value::{Bytes, U64};
use std::time::Duration;

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
                seg::Value::Bytes(_) => Response::error("value is not an integer or out of range"),
                seg::Value::U64(uint) => {
                    if let Some(incremented) = (uint as i64).checked_add(1) {
                        item.wrapping_add(1)
                            .expect("we already checked that type is numeric, how can this fail?");
                        Response::integer(incremented)
                    } else {
                        Response::error("increment or decrement would overflow")
                    }
                }
            }
        } else if self
            .data
            .insert(incr.key(), 1_u64, None, Duration::ZERO)
            .is_ok()
        {
            Response::integer(1)
        } else {
            Response::error("not stored")
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Seg;
    use config::RdsConfig;
    use protocol_resp::*;

    #[test]
    fn it_should_set() {
        let config = RdsConfig::default();
        let mut seg = Seg::new(&config).expect("could not initialize seg");
        let set_missing = Set::new(b"missing", b"value", None, SetMode::Set, false);
        let response = seg.set(&set_missing);
        assert_eq!(response, Response::ok());
    }

    #[test]
    fn it_should_get() {
        //setup
        let config = RdsConfig::default();
        let mut seg = Seg::new(&config).expect("could not initialize seg");

        //missing
        let get_missing = Get::new(b"missing");
        let response = seg.get(&get_missing);
        assert_eq!(response, Response::null());

        //get something set
        let key = b"foo";
        let value = b"bar";
        let set = Set::new(key, value, None, SetMode::Set, false);
        let response = seg.set(&set);
        assert_eq!(response, Response::ok());

        let get = Get::new(key);
        let response = seg.get(&get);
        assert_eq!(response, Response::bulk_string(value));
    }

    #[test]
    fn it_should_incr() {
        //setup
        let config = RdsConfig::default();
        let mut seg = Seg::new(&config).expect("could not initialize seg");

        // incr missing
        let incr_missing = Incr::new(b"missing");
        let response = seg.incr(&incr_missing);
        assert_eq!(response, Response::integer(1));

        // incr numeric set
        let key = b"number";
        let number = 123456_i64;
        let set = Set::new(
            key,
            number.to_string().as_bytes(),
            None,
            SetMode::Set,
            false,
        );
        seg.set(&set);

        let incr_numeric = Incr::new(key);
        let response = seg.incr(&incr_numeric);
        assert_eq!(response, Response::integer(number + 1));

        // incr string set
        let key = b"string";
        let set = Set::new(key, b"value", None, SetMode::Set, false);
        seg.set(&set);

        let incr_missing = Incr::new(key);
        let response = seg.incr(&incr_missing);
        assert_eq!(
            response,
            Response::error("value is not an integer or out of range")
        );

        // incr overflow
        let key = b"string";
        let value = b"9223372036854775807";
        let set = Set::new(key, value, None, SetMode::Set, false);
        seg.set(&set);

        let incr_overflow = Incr::new(key);
        let response = seg.incr(&incr_overflow);
        assert_eq!(
            response,
            Response::error("increment or decrement would overflow")
        );
    }
}
