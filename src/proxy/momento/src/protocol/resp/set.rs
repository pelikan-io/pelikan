// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::CacheClient;
use protocol_resp::Set;

use crate::error::{ProxyError, ProxyResult};
use crate::klog::{klog_set, Status};
use crate::*;

use super::update_method_metrics;

pub async fn set(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &Set,
) -> ProxyResult {
    update_method_metrics(&SET, &SET_EX, async move {
        let ttl = match req.expire_time() {
            Some(protocol_resp::ExpireTime::Seconds(v)) => Some(Duration::from_secs(v)),
            Some(protocol_resp::ExpireTime::Milliseconds(v)) => {
                Some(Duration::from_millis((v / 1000).max(1)))
            }
            Some(_) => return Err(ProxyError::custom("expire time")),
            None => None,
        };

        let _response = match tokio::time::timeout(
            Duration::from_millis(200),
            client.set(cache_name, req.key(), req.value()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_set(
                    &req.key(),
                    0,
                    ttl.map(|v| v.as_millis()).unwrap_or(0) as i32,
                    req.value().len(),
                    Status::ServerError,
                    0,
                );
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_set(
                    &req.key(),
                    0,
                    ttl.map(|v| v.as_millis()).unwrap_or(0) as i32,
                    req.value().len(),
                    Status::Timeout,
                    0,
                );
                return Err(ProxyError::from(e));
            }
        };

        SET_STORED.increment();
        klog_set(
            &req.key(),
            0,
            ttl.map(|v| v.as_millis()).unwrap_or(0) as i32,
            req.value().len(),
            Status::Stored,
            8,
        );

        response_buf.extend_from_slice(b"+OK\r\n");

        Ok(())
    })
    .await
}
