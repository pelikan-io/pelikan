// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::SortedSetLengthResponse;
use momento::CacheClient;
use protocol_resp::{SortedSetCardinality, ZCARD, ZCARD_EX, ZCARD_HIT, ZCARD_MISS};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zcard(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetCardinality,
) -> ProxyResult {
    update_method_metrics(&ZCARD, &ZCARD_EX, async move {
        let response = match time::timeout(
            Duration::from_millis(200),
            client.sorted_set_length(cache_name, req.key()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"zcard", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"zcard", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            SortedSetLengthResponse::Hit { length } => {
                ZCARD_HIT.increment();
                write!(response_buf, ":{}\r\n", length)?;
                klog_1(&"zcard", &req.key(), Status::Hit, response_buf.len());
            }
            SortedSetLengthResponse::Miss => {
                ZCARD_MISS.increment();
                write!(response_buf, ":0\r\n")?;
                klog_1(&"zcard", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
