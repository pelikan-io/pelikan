// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::response::MomentoDictionaryGetStatus;
use momento::SimpleCacheClient;
use protocol_resp::{HashExists, HEXISTS, HEXISTS_EX, HEXISTS_HIT, HEXISTS_MISS};
use tokio::time::timeout;

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};
use crate::BACKEND_EX;

use super::update_method_metrics;

pub async fn hexists(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashExists,
) -> ProxyResult {
    update_method_metrics(&HEXISTS, &HEXISTS_EX, async move {
        let response = timeout(
            Duration::from_millis(200),
            client.dictionary_get(cache_name, req.key(), vec![req.field()]),
        )
        .await??;

        match response.result {
            MomentoDictionaryGetStatus::ERROR => {
                // we got some error from
                // the backend.
                BACKEND_EX.increment();
                HEXISTS_EX.increment();
                response_buf.extend_from_slice(b"-ERR backend error\r\n");
            }
            MomentoDictionaryGetStatus::FOUND => {
                if response.dictionary.is_none() {
                    error!("error for hget: dictionary found but not set in response");
                    BACKEND_EX.increment();
                    HEXISTS_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                } else if let Some(_value) = response.dictionary.unwrap().get(req.field()) {
                    HEXISTS_HIT.increment();
                    response_buf.extend_from_slice(b":1\r\n");
                    klog_2(&"hexists", &req.key(), &req.field(), Status::Hit, 1);
                } else {
                    HEXISTS_MISS.increment();
                    response_buf.extend_from_slice(b":0\r\n");
                    klog_2(&"hexists", &req.key(), &req.field(), Status::Miss, 0);
                }
            }
            MomentoDictionaryGetStatus::MISSING => {
                HEXISTS_MISS.increment();
                response_buf.extend_from_slice(b":0\r\n");
                klog_2(&"hexists", &req.key(), &req.field(), Status::Miss, 0);
            }
        }

        Ok(())
    })
    .await
}
