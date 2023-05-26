// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::{klog_1, Status};
use crate::*;
use protocol_memcache::*;

use super::update_method_metrics;

pub async fn get(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    key: &[u8],
) -> ProxyResult {
    update_method_metrics(&GET, &GET_EX, async move {
        GET_KEY.increment();

        let response = match timeout(Duration::from_millis(200), client.get(cache_name, key)).await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                GET_EX.increment();
                klog_1(&"get", &key, Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                GET_EX.increment();
                klog_1(&"get", &key, Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response.result {
            MomentoGetStatus::ERROR => {
                // we got some error from
                // the backend.
                BACKEND_EX.increment();
                GET_EX.increment();
                response_buf.extend_from_slice(b"-ERR backend error\r\n");

                klog_1(&"get", &key, Status::ServerError, 0);
            }
            MomentoGetStatus::HIT => {
                GET_KEY_HIT.increment();

                let item_header = format!("${}\r\n", response.value.len());

                response_buf.extend_from_slice(item_header.as_bytes());
                response_buf.extend_from_slice(&response.value);
                response_buf.extend_from_slice(b"\r\n");

                klog_1(&"get", &key, Status::Hit, response.value.len());
            }
            MomentoGetStatus::MISS => {
                GET_KEY_MISS.increment();

                response_buf.extend_from_slice(b"$-1\r\n");

                klog_1(&"get", &key, Status::Miss, 0);
            }
        }

        Ok(())
    })
    .await
}
