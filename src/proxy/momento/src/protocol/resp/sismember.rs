// Copyright 2023 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::SimpleCacheClient;
use protocol_resp::{SetIsMember, SISMEMBER, SISMEMBER_EX, SISMEMBER_HIT, SISMEMBER_MISS};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};

use super::update_method_metrics;

pub async fn sismember(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetIsMember,
) -> ProxyResult {
    update_method_metrics(&SISMEMBER, &SISMEMBER_EX, async move {
        let response = time::timeout(
            Duration::from_millis(200),
            client.set_fetch(cache_name, req.key()),
        )
        .await??;

        let status = match response.value {
            Some(set) if set.contains(req.field()) => {
                SISMEMBER_HIT.increment();
                response_buf.extend_from_slice(b":1\r\n");
                Status::Hit
            }
            _ => {
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
