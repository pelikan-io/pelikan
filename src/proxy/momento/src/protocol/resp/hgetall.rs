// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

// use crate::klog::*;
// use crate::{Error, *};
// use ::net::*;
// use protocol_resp::*;

use std::time::Duration;

use momento::response::MomentoDictionaryFetchStatus;
use momento::SimpleCacheClient;
use protocol_resp::{HashGetAll, HGETALL, HGETALL_EX, HGETALL_HIT, HGETALL_MISS};

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::BACKEND_EX;

use super::update_method_metrics;

pub async fn hgetall(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashGetAll,
) -> ProxyResult {
    update_method_metrics(&HGETALL, &HGETALL_EX, async move {
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
                HGETALL_EX.increment();
                response_buf.extend_from_slice(b"-ERR backend error\r\n");
            }
            MomentoDictionaryFetchStatus::FOUND => {
                if response.dictionary.is_none() {
                    error!("error for hgetall: dictionary found but not provided in response");
                    BACKEND_EX.increment();
                    HGETALL_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                } else {
                    HGETALL_HIT.increment();
                    let dictionary = response.dictionary.as_ref().unwrap();

                    response_buf
                        .extend_from_slice(format!("*{}\r\n", dictionary.len() * 2).as_bytes());

                    for (field, value) in dictionary {
                        let field_header = format!("${}\r\n", field.len());
                        let value_header = format!("${}\r\n", value.len());

                        response_buf.extend_from_slice(field_header.as_bytes());
                        response_buf.extend_from_slice(field);
                        response_buf.extend_from_slice(b"\r\n");
                        response_buf.extend_from_slice(value_header.as_bytes());
                        response_buf.extend_from_slice(value);
                        response_buf.extend_from_slice(b"\r\n");
                    }

                    klog_1(&"hgetall", &req.key(), Status::Hit, response_buf.len());
                }
            }
            MomentoDictionaryFetchStatus::MISSING => {
                HGETALL_MISS.increment();
                response_buf.extend_from_slice(b"*0\r\n");
                klog_1(&"hgetall", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
