// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::response::MomentoSetStatus;
use momento::SimpleCacheClient;
use protocol_memcache::{SET, SET_EX, SET_NOT_STORED, SET_STORED};
use protocol_resp::Set;

use crate::error::{ProxyError, ProxyResult};
use crate::klog::klog_set;

use super::update_method_metrics;

pub async fn set(
    client: &mut SimpleCacheClient,
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

        let response = tokio::time::timeout(
            Duration::from_millis(200),
            client.set(cache_name, req.key(), req.value(), ttl),
        )
        .await??;

        match response.result {
            MomentoSetStatus::OK => {
                SET_STORED.increment();
                klog_set(
                    &req.key(),
                    0,
                    ttl.map(|v| v.as_millis()).unwrap_or(0) as i32,
                    req.value().len(),
                    5,
                    8,
                );

                response_buf.extend_from_slice(b"+OK\r\n");
            }
            MomentoSetStatus::ERROR => {
                SET_NOT_STORED.increment();
                klog_set(
                    &req.key(),
                    0,
                    ttl.map(|v| v.as_millis()).unwrap_or(0) as i32,
                    req.value().len(),
                    9,
                    12,
                );

                return Err(ProxyError::custom("backend error"));
            }
        }

        Ok(())
    })
    .await
}
