// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use momento::cache::DictionaryGetFieldResponse;
use momento::CacheClient;
use protocol_resp::{HashExists, HEXISTS, HEXISTS_EX, HEXISTS_HIT, HEXISTS_MISS};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn hexists(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashExists,
) -> ProxyResult {
    update_method_metrics(&HEXISTS, &HEXISTS_EX, async move {
        let response = match timeout(
            Duration::from_millis(200),
            client.dictionary_get_field(cache_name, req.key(), req.field()),
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
            DictionaryGetFieldResponse::Hit { value: _ } => {
                HEXISTS_HIT.increment();
                response_buf.extend_from_slice(b":1\r\n");
                klog_2(&"hexists", &req.key(), &req.field(), Status::Hit, 1);
            }
            DictionaryGetFieldResponse::Miss => {
                HEXISTS_MISS.increment();
                response_buf.extend_from_slice(b":0\r\n");
                klog_2(&"hexists", &req.key(), &req.field(), Status::Miss, 0);
            }
        }

        Ok(())
    })
    .await
}
