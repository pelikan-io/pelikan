// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::response::MomentoDictionaryGetStatus;
use momento::SimpleCacheClient;
use protocol_resp::{HashGet, HGET, HGET_EX, HGET_HIT, HGET_MISS};

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};
use crate::BACKEND_EX;

use super::update_method_metrics;

pub async fn hget(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashGet,
) -> ProxyResult {
    update_method_metrics(&HGET, &HGET_EX, async move {
        let response = tokio::time::timeout(
            Duration::from_millis(200),
            client.dictionary_get(cache_name, req.key(), vec![req.field()]),
        )
        .await??;

        match response.result {
            MomentoDictionaryGetStatus::ERROR => {
                // we got some error from
                // the backend.
                BACKEND_EX.increment();
                HGET_EX.increment();
                response_buf.extend_from_slice(b"-ERR backend error\r\n");
            }
            MomentoDictionaryGetStatus::FOUND => {
                if response.dictionary.is_none() {
                    error!("error for hget: dictionary found but not set in response");
                    BACKEND_EX.increment();
                    HGET_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                } else if let Some(value) = response.dictionary.unwrap().get(req.field()) {
                    HGET_HIT.increment();

                    let item_header = format!("${}\r\n", value.len());

                    response_buf.extend_from_slice(item_header.as_bytes());
                    response_buf.extend_from_slice(value);
                    response_buf.extend_from_slice(b"\r\n");

                    klog_2(&"hget", &req.key(), &req.field(), Status::Hit, value.len());
                } else {
                    HGET_MISS.increment();
                    response_buf.extend_from_slice(b"$-1\r\n");
                    klog_2(&"hget", &req.key(), &req.field(), Status::Miss, 0);
                }
            }
            MomentoDictionaryGetStatus::MISSING => {
                HGET_MISS.increment();
                response_buf.extend_from_slice(b"$-1\r\n");
                klog_2(&"hget", &req.key(), &req.field(), Status::Miss, 0);
            }
        }

        Ok(())
    })
    .await
}
