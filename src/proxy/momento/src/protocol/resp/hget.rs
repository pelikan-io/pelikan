// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use momento::cache::DictionaryGetFieldResponse;
use momento::CacheClient;
use protocol_resp::{HashGet, HGET, HGET_EX, HGET_HIT, HGET_MISS};
use std::time::Duration;

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn hget(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashGet,
) -> ProxyResult {
    update_method_metrics(&HGET, &HGET_EX, async move {
        let response = match tokio::time::timeout(
            Duration::from_millis(200),
            client.dictionary_get_field(cache_name, req.key(), req.field()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_2(&"hget", &req.key(), &req.field(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_2(&"hget", &req.key(), &req.field(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            DictionaryGetFieldResponse::Hit { value } => {
                HGET_HIT.increment();

                let value_bytes: Vec<u8> = value.into();
                let item_header = format!("${}\r\n", value_bytes.len());

                response_buf.extend_from_slice(item_header.as_bytes());
                response_buf.extend_from_slice(value_bytes.as_slice());
                response_buf.extend_from_slice(b"\r\n");

                klog_2(
                    &"hget",
                    &req.key(),
                    &req.field(),
                    Status::Hit,
                    value_bytes.len(),
                );
            }
            DictionaryGetFieldResponse::Miss => {
                HGET_MISS.increment();
                response_buf.extend_from_slice(b"$-1\r\n");
                klog_2(&"hget", &req.key(), &req.field(), Status::Miss, 0);
            }
        }

        Ok(())
    })
    .await
}
