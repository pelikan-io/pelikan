// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::cache::SetFetchResponse;
use momento::CacheClient;
use protocol_resp::{SetIsMember, SISMEMBER, SISMEMBER_EX, SISMEMBER_HIT, SISMEMBER_MISS};
use std::collections::HashSet;
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn sismember(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetIsMember,
) -> ProxyResult {
    update_method_metrics(&SISMEMBER, &SISMEMBER_EX, async move {
        let response = match time::timeout(
            Duration::from_millis(200),
            client.set_fetch(cache_name, req.key()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_2(
                    &"sismember",
                    &req.key(),
                    &req.field(),
                    Status::ServerError,
                    0,
                );
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_2(
                    &"sismember",
                    &req.key(),
                    &req.field(),
                    Status::ServerError,
                    0,
                );
                return Err(ProxyError::from(e));
            }
        };

        let status = match response {
            SetFetchResponse::Hit { values } => {
                let values: Vec<Vec<u8>> = values.into();
                let values: HashSet<Vec<u8>> = values.into_iter().collect();
                if values.contains(req.field()) {
                    SISMEMBER_HIT.increment();
                    response_buf.extend_from_slice(b":1\r\n");
                    Status::Hit
                } else {
                    SISMEMBER_MISS.increment();
                    response_buf.extend_from_slice(b":0\r\n");
                    Status::Miss
                }
            }
            SetFetchResponse::Miss => {
                SISMEMBER_MISS.increment();
                response_buf.extend_from_slice(b":0\r\n");
                Status::Miss
            }
        };

        klog_2(
            &"sismember",
            &req.key(),
            &req.field(),
            status,
            response_buf.len(),
        );

        Ok(())
    })
    .await
}
