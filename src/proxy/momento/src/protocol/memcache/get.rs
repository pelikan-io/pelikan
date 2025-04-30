// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::{klog_1, Status};
use crate::{Error, *};
use momento::cache::GetResponse;
use protocol_memcache::GET_EX;
use protocol_memcache::GET_KEY_HIT;
use protocol_memcache::GET_KEY_MISS;
use protocol_memcache::{Get, Response, Value};

pub async fn get(
    client: &mut CacheClient,
    cache_name: &str,
    request: &Get,
    flags: bool,
) -> Result<Response, Error> {
    // check if any of the keys are invalid before
    // sending the requests to the backend
    for key in request.keys().iter() {
        if std::str::from_utf8(key).is_err() {
            GET_EX.increment();

            // invalid key
            return Ok(Response::client_error("invalid key"));
        }
    }

    let mut values = Vec::new();

    for key in request.keys() {
        BACKEND_REQUEST.increment();

        // we don't have a strict guarantee this function was called with memcache
        // safe keys. This matters mostly for writing the response back to the client
        // in a protocol compliant way.
        let str_key = std::str::from_utf8(key);

        // invalid keys will be treated as a miss
        if str_key.is_err() {
            continue;
        }

        // unwrap is safe now, rebind for convenience
        let str_key = str_key.unwrap();

        match timeout(Duration::from_millis(200), client.get(cache_name, str_key)).await {
            Ok(Ok(response)) => match response {
                GetResponse::Hit { value } => {
                    GET_KEY_HIT.increment();

                    let value: Vec<u8> = value.into();

                    if flags && value.len() < 5 {
                        klog_1(&"get", &key, Status::Miss, 0);
                    } else if flags {
                        let flags: u32 =
                            u32::from_be_bytes([value[0], value[1], value[2], value[3]]);
                        let value: Vec<u8> = value[4..].into();
                        let length = value.len();

                        values.push(Value::new(key, flags, None, &value));

                        klog_1(&"get", &key, Status::Hit, length);
                    } else {
                        let length = value.len();
                        values.push(Value::new(key, 0, None, &value));

                        klog_1(&"get", &key, Status::Hit, length);
                    }
                }
                GetResponse::Miss => {
                    GET_KEY_MISS.increment();

                    klog_1(&"get", &key, Status::Miss, 0);
                }
            },
            Ok(Err(e)) => {
                // we got some error from the momento client
                // log and incr stats and move on treating it
                // as a miss
                error!("backend error for get: {}", e);
                BACKEND_EX.increment();

                klog_1(&"get", &key, Status::ServerError, 0);
            }
            Err(_) => {
                // we had a timeout, incr stats and move on
                // treating it as a miss
                BACKEND_EX.increment();
                BACKEND_EX_TIMEOUT.increment();

                klog_1(&"get", &key, Status::Timeout, 0);
            }
        }
    }

    if !values.is_empty() {
        Ok(Response::values(values.into()))
    } else {
        Ok(Response::not_found(false))
    }
}
