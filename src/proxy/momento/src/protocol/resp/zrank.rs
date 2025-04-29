// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::SortedSetGetRankResponse;
use momento::CacheClient;
use protocol_resp::{SortedSetRank, ZRANK, ZRANK_EX};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zrank(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetRank,
) -> ProxyResult {
    update_method_metrics(&ZRANK, &ZRANK_EX, async move {
        // sorted_set_get_rank uses ascending order (scores sorted from lowest to highest) by default
        let response = match time::timeout(
            Duration::from_millis(200),
            client.sorted_set_get_rank(cache_name, req.key(), req.member()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"zrank", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"zrank", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            SortedSetGetRankResponse::Hit { rank } => {
                if req.with_score() {
                    write!(response_buf, "*2\r\n:{}\r\n${}\r\n", rank, req.member().len())?;
                    response_buf.extend_from_slice(req.member());
                    response_buf.extend_from_slice(b"\r\n");
                } else {
                    write!(response_buf, ":{}\r\n", rank)?;
                }
                klog_1(&"zrank", &req.key(), Status::Hit, response_buf.len());
            }
            SortedSetGetRankResponse::Miss => {
                write!(response_buf, "_\r\n")?;
                klog_1(&"zrank", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
