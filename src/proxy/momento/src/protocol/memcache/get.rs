// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::{klog_1, Status};
use crate::{Error, *};
use futures::StreamExt;
use momento::cache::GetResponse;
use protocol_memcache::*;

pub async fn get(
    client: &CacheClient,
    cache_name: &str,
    request: &Get,
    flags: bool,
) -> Result<Response, Error> {
    let mut tasks = futures::stream::FuturesOrdered::new();
    for key in request.keys() {
        BACKEND_REQUEST.increment();

        tasks.push_back(run_get(client, cache_name, flags, key));
    }
    let values: Vec<Option<protocol_memcache::Value>> = tasks.collect().await;
    let values: Vec<protocol_memcache::Value> = values.into_iter().flatten().collect();

    if !values.is_empty() {
        Ok(Response::values(values.into()))
    } else {
        Ok(Response::not_found(false))
    }
}

async fn run_get(
    client: &CacheClient,
    cache_name: &str,
    flags: bool,
    key: &[u8],
) -> Option<protocol_memcache::Value> {
    match timeout(Duration::from_millis(200), client.get(cache_name, key)).await {
        Ok(Ok(response)) => match response {
            GetResponse::Hit { value } => {
                GET_KEY_HIT.increment();

                let value: Vec<u8> = value.into();

                if flags && value.len() < 5 {
                    klog_1(&"get", &key, Status::Miss, 0);
                    None
                } else if flags {
                    let flags: u32 = u32::from_be_bytes([value[0], value[1], value[2], value[3]]);
                    let value: Vec<u8> = value[4..].into();
                    let length = value.len();

                    klog_1(&"get", &key, Status::Hit, length);
                    Some(protocol_memcache::Value::new(key, flags, None, &value))
                } else {
                    let length = value.len();

                    klog_1(&"get", &key, Status::Hit, length);
                    Some(protocol_memcache::Value::new(key, 0, None, &value))
                }
            }
            GetResponse::Miss => {
                GET_KEY_MISS.increment();

                klog_1(&"get", &key, Status::Miss, 0);
                None
            }
        },
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and incr stats and move on treating it
            // as a miss
            error!("backend error for get: {}", e);
            BACKEND_EX.increment();

            klog_1(&"get", &key, Status::ServerError, 0);
            None
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as a miss
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();

            klog_1(&"get", &key, Status::Timeout, 0);
            None
        }
    }
}
