// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::SortedSetGetScoreResponse;
use momento::CacheClient;
use protocol_resp::{SortedSetScore, ZSCORE, ZSCORE_EX};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zscore(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetScore,
) -> ProxyResult {
    update_method_metrics(&ZSCORE, &ZSCORE_EX, async move {
        let response = match time::timeout(
            Duration::from_millis(200),
            client.sorted_set_get_score(cache_name, req.key(), req.member()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"zscore", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"zscore", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            SortedSetGetScoreResponse::Hit { score } => {
                write!(response_buf, ":{}\r\n", score)?;
                klog_1(&"zscore", &req.key(), Status::Hit, response_buf.len());
            }
            SortedSetGetScoreResponse::Miss => {
                write!(response_buf, ":0\r\n")?;
                klog_1(&"zscore", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
