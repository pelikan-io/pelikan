// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::time::Duration;

use momento::response::DictionaryGet;
use momento::SimpleCacheClient;
use protocol_resp::{HashExists, HEXISTS, HEXISTS_EX, HEXISTS_HIT, HEXISTS_MISS};
use tokio::time::timeout;

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn hexists(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashExists,
) -> ProxyResult {
    update_method_metrics(&HEXISTS, &HEXISTS_EX, async move {
        let response = match timeout(
            Duration::from_millis(200),
            client.dictionary_get(cache_name, req.key(), vec![req.field()]),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_2(&"hexists", &req.key(), &req.field(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_2(&"hexists", &req.key(), &req.field(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            DictionaryGet::Hit { value } => {
                let map: HashMap<Vec<u8>, Vec<u8>> = value.collect_into();

                if let Some(_value) = map.get(req.field()) {
                    HEXISTS_HIT.increment();
                    response_buf.extend_from_slice(b":1\r\n");
                    klog_2(&"hexists", &req.key(), &req.field(), Status::Hit, 1);
                } else {
                    HEXISTS_MISS.increment();
                    response_buf.extend_from_slice(b":0\r\n");
                    klog_2(&"hexists", &req.key(), &req.field(), Status::Miss, 0);
                }
            }
            DictionaryGet::Miss => {
                HEXISTS_MISS.increment();
                response_buf.extend_from_slice(b":0\r\n");
                klog_2(&"hexists", &req.key(), &req.field(), Status::Miss, 0);
            }
        }

        Ok(())
    })
    .await
}
