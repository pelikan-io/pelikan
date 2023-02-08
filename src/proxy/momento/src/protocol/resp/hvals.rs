// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::response::MomentoDictionaryFetchStatus;
use momento::SimpleCacheClient;
use protocol_resp::{HashValues, HVALS, HVALS_EX, HVALS_HIT, HVALS_MISS};

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::BACKEND_EX;

use super::update_method_metrics;

pub async fn hvals(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashValues,
) -> ProxyResult {
    update_method_metrics(&HVALS, &HVALS_EX, async move {
        let response = tokio::time::timeout(
            Duration::from_millis(200),
            client.dictionary_fetch(cache_name, req.key()),
        )
        .await??;

        match response.result {
            MomentoDictionaryFetchStatus::ERROR => {
                // we got some error from
                // the backend.
                BACKEND_EX.increment();
                HVALS_EX.increment();
                response_buf.extend_from_slice(b"-ERR backend error\r\n");
            }
            MomentoDictionaryFetchStatus::FOUND => {
                if response.dictionary.is_none() {
                    error!("error for hgetall: dictionary found but not provided in response");
                    BACKEND_EX.increment();
                    HVALS_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                } else {
                    HVALS_HIT.increment();
                    let dictionary = response.dictionary.as_ref().unwrap();

                    response_buf.extend_from_slice(format!("*{}\r\n", dictionary.len()).as_bytes());

                    for value in dictionary.values() {
                        let value_header = format!("${}\r\n", value.len());

                        response_buf.extend_from_slice(value_header.as_bytes());
                        response_buf.extend_from_slice(value);
                        response_buf.extend_from_slice(b"\r\n");
                    }

                    klog_1(&"hvals", &req.key(), Status::Hit, response_buf.len());
                }
            }
            MomentoDictionaryFetchStatus::MISSING => {
                HVALS_MISS.increment();
                // per command reference, return an empty list
                response_buf.extend_from_slice(b"*0\r\n");
                klog_1(&"hvals", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
