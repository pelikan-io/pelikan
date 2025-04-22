// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;
use momento::cache::DictionaryFetchResponse;
use momento::CacheClient;
use protocol_resp::{HashGetAll, HGETALL, HGETALL_EX, HGETALL_HIT, HGETALL_MISS};

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn hgetall(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashGetAll,
) -> ProxyResult {
    update_method_metrics(&HGETALL, &HGETALL_EX, async move {
        let response = match tokio::time::timeout(
            Duration::from_millis(200),
            client.dictionary_fetch(cache_name, req.key()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"hgetall", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"hgetall", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            DictionaryFetchResponse::Hit { value } => {
                HGETALL_HIT.increment();
                let map: Vec<(Vec<u8>, Vec<u8>)> = value.collect_into();

                response_buf.extend_from_slice(format!("*{}\r\n", map.len() * 2).as_bytes());

                for (field, value) in map {
                    let field_header = format!("${}\r\n", field.len());
                    let value_header = format!("${}\r\n", value.len());

                    response_buf.extend_from_slice(field_header.as_bytes());
                    response_buf.extend_from_slice(&field);
                    response_buf.extend_from_slice(b"\r\n");
                    response_buf.extend_from_slice(value_header.as_bytes());
                    response_buf.extend_from_slice(&value);
                    response_buf.extend_from_slice(b"\r\n");
                }

                klog_1(&"hgetall", &req.key(), Status::Hit, response_buf.len());
            }
            DictionaryFetchResponse::Miss => {
                HGETALL_MISS.increment();
                response_buf.extend_from_slice(b"*0\r\n");
                klog_1(&"hgetall", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
