// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;
use std::io::Write;
use std::time::Duration;

use momento::cache::SetFetchResponse;
use momento::CacheClient;
use protocol_resp::{SetMembers, SMEMBERS, SMEMBERS_EX};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn smembers(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetMembers,
) -> ProxyResult {
    update_method_metrics(&SMEMBERS, &SMEMBERS_EX, async move {
        let response = match time::timeout(
            Duration::from_millis(200),
            client.set_fetch(cache_name, req.key()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"sismember", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"sismember", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        let (set, status) = match response {
            SetFetchResponse::Hit { values } => {
                let values: Vec<Vec<u8>> = values.into();
                let set: HashSet<Vec<u8>> = values.into_iter().collect();
                (set, Status::Hit)
            }
            SetFetchResponse::Miss => {
                let set: HashSet<Vec<u8>> = HashSet::default();
                (set, Status::Miss)
            }
        };

        write!(response_buf, "*{}\r\n", set.len())?;

        for entry in &set {
            write!(response_buf, "${}\r\n", entry.len())?;
            response_buf.extend_from_slice(entry);
        }

        klog_1(&"sismember", &req.key(), status, response_buf.len());

        Ok(())
    })
    .await
}
