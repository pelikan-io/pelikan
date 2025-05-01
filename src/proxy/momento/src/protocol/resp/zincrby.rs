// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::CacheClient;
use protocol_resp::{SortedSetIncrement, ZINCRBY, ZINCRBY_EX};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::{parse_sorted_set_score, update_method_metrics};

pub async fn zincrby(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetIncrement,
) -> ProxyResult {
    update_method_metrics(&ZINCRBY, &ZINCRBY_EX, async move {
        let response = match time::timeout(
            Duration::from_millis(200),
            client.sorted_set_increment_score(
                cache_name,
                req.key(),
                req.member(),
                parse_sorted_set_score(req.increment())?,
            ),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"zincrby", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"zincrby", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        // Return string representation of the floating-point score
        let score_str = response.score.to_string();
        write!(response_buf, "${}\r\n{}\r\n", score_str.len(), score_str)?;
        klog_1(&"zincrby", &req.key(), Status::Hit, response_buf.len());

        Ok(())
    })
    .await
}
