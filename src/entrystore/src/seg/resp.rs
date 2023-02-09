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
            Request::Del(del) => self.del(del),
            Request::Get(get) => self.get(get),
            Request::Set(set) => self.set(set),
            _ => Response::error("not supported"),
        }
    }
}

impl Storage for Seg {
    fn del(&mut self, delete: &Del) -> Response {
        let count = delete
            .keys()
            .iter()
            .filter(|key| self.data.delete(key))
            .count();
        // according to https://stackoverflow.com/questions/42911672/is-there-any-limit-on-the-number-of-arguments-that-redis-commands-such-as-zadd-o#:~:text=The%20maximum%20number%20of%20arguments,long%20meaning%20up%20to%202%2C147%2C483%2C647.
        // the max number of arguments to a Redis command is the max value of an i32, so this cast should be safe
        Response::integer(count as i64)
    }
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

#[cfg(test)]
mod tests {
    use crate::Seg;
    use config::RdsConfig;
    use protocol_resp::*;

    #[test]
    fn it_should_del() {
        let config = RdsConfig::default();
        let mut seg = Seg::new(&config).expect("could not initialize seg");

        //ignores missing
        let del_missing = Del::new(&[b"missing"]);
        let response = seg.del(&del_missing);
        assert_eq!(response, Response::integer(0));

        //deletes multiple keys, returning the number deleted
        let mut keys = vec![];
        for i in 1..5 {
            let key_string = format!("k{i}");
            let key = key_string.as_bytes();
            let set = Set::new(key, format!("v{i}").as_bytes(), None, SetMode::Set, false);
            seg.set(&set);
            keys.push(key_string);
        }

        let del = Del::new(
            keys.iter()
                .map(|s| s.as_bytes())
                .collect::<Vec<&[u8]>>()
                .as_slice(),
        );

        let response = seg.del(&del);
        assert_eq!(response, Response::integer(keys.len() as i64));

        for key in keys {
            let bytes = key.as_bytes();

            let get = Get::new(bytes);
            let response = seg.get(&get);
            assert_eq!(response, Response::null())
        }
    }
}
