// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::SimpleCacheClient;
use protocol_resp::{ListLen, LLEN, LLEN_EX};

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};

use super::update_method_metrics;

pub async fn llen(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListLen,
) -> ProxyResult {
    update_method_metrics(&LLEN, &LLEN_EX, async move {
        let len = tokio::time::timeout(
            Duration::from_millis(200),
            client.list_length(cache_name, req.key()),
        )
        .await??;

        write!(response_buf, ":{}\r\n", len.unwrap_or(0))?;
        klog_1(&"llen", &req.key(), Status::Hit, response_buf.len());

        Ok(())
    })
    .await
}
