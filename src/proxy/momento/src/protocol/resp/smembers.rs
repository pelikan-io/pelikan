// Copyright 2023 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;
use std::io::Write;
use std::time::Duration;

use momento::SimpleCacheClient;
use protocol_resp::{SetMembers, SMEMBERS, SMEMBERS_EX};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};

use super::update_method_metrics;

pub async fn smembers(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetMembers,
) -> ProxyResult {
    update_method_metrics(&SMEMBERS, &SMEMBERS_EX, async move {
        let response = time::timeout(
            Duration::from_millis(200),
            client.set_fetch(cache_name, req.key()),
        )
        .await??;

        let (set, status) = match response.value {
            Some(set) => (set, Status::Hit),
            None => (HashSet::default(), Status::Miss),
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
