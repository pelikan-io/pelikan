// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::{SortedSetGetRankRequest, SortedSetGetRankResponse, SortedSetOrder};
use momento::CacheClient;
use protocol_resp::{SortedSetReverseRank, ZREVRANK, ZREVRANK_EX, ZREVRANK_HIT, ZREVRANK_MISS};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zrevrank(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetReverseRank,
) -> ProxyResult {
    update_method_metrics(&ZREVRANK, &ZREVRANK_EX, async move {
        // sorted_set_get_rank uses ascending order (scores sorted from lowest to highest) by default,
        // must specify descending order to get reverse rank
        let get_rank_request = SortedSetGetRankRequest::new(cache_name, req.key(), req.member())
            .order(SortedSetOrder::Descending);
        let response = match time::timeout(
            Duration::from_millis(200),
            client.send_request(get_rank_request),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"zrevrank", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"zrevrank", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            SortedSetGetRankResponse::Hit { rank } => {
                ZREVRANK_HIT.increment();
                if req.with_score() {
                    write!(
                        response_buf,
                        "*2\r\n:{}\r\n${}\r\n",
                        rank,
                        req.member().len()
                    )?;
                    response_buf.extend_from_slice(req.member());
                    response_buf.extend_from_slice(b"\r\n");
                } else {
                    write!(response_buf, ":{}\r\n", rank)?;
                }
                klog_1(&"zrevrank", &req.key(), Status::Hit, response_buf.len());
            }
            SortedSetGetRankResponse::Miss => {
                ZREVRANK_MISS.increment();
                write!(response_buf, "_\r\n")?;
                klog_1(&"zrevrank", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
