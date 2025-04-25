// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use momento::cache::DictionaryLengthResponse;
use momento::CacheClient;
use protocol_resp::{HashLength, HLEN, HLEN_EX, HLEN_HIT, HLEN_MISS};
use std::time::Duration;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn hlen(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashLength,
) -> ProxyResult {
    update_method_metrics(&HLEN, &HLEN_EX, async move {
        let response = match tokio::time::timeout(
            Duration::from_millis(200),
            client.dictionary_length(cache_name, req.key()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"hlen", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"hlen", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            DictionaryLengthResponse::Hit { length } => {
                HLEN_HIT.increment();

                let response = format!(":{}\r\n", length).into_bytes();

                response_buf.extend_from_slice(&response);

                klog_1(&"hlen", &req.key(), Status::Hit, response_buf.len());
            }
            DictionaryLengthResponse::Miss => {
                HLEN_MISS.increment();
                response_buf.extend_from_slice(b":0\r\n");
                klog_1(&"hlen", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
