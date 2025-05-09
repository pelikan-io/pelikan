// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::{SortedSetLengthByScoreRequest, SortedSetLengthByScoreResponse};
use momento::CacheClient;
use protocol_resp::{SortedSetCount, ZCOUNT, ZCOUNT_EX, ZCOUNT_HIT, ZCOUNT_MISS};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zcount(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetCount,
) -> ProxyResult {
    update_method_metrics(&ZCOUNT, &ZCOUNT_EX, async move {
        let min_score = match (req.min_score(), req.min_score_exclusive()) {
            (f64::NEG_INFINITY, _) => None,
            (f64::INFINITY, _) => Some(f64::MAX), // Momento does not accept +inf, but accepts max f64
            (score, true) => Some(score + 1.0),
            (score, false) => Some(score),
        };

        let max_score = match (req.max_score(), req.max_score_exclusive()) {
            (f64::INFINITY, _) => None,
            (f64::NEG_INFINITY, _) => Some(f64::MIN), // Momento does not accept -inf, but accepts min f64
            (score, true) => Some(score - 1.0),
            (score, false) => Some(score),
        };

        let request = SortedSetLengthByScoreRequest::new(cache_name, req.key())
            .min_score(min_score)
            .max_score(max_score);

        let response =
            match time::timeout(Duration::from_millis(200), client.send_request(request)).await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    klog_1(&"zcount", &req.key(), Status::ServerError, 0);
                    return Err(ProxyError::from(e));
                }
                Err(e) => {
                    klog_1(&"zcount", &req.key(), Status::Timeout, 0);
                    return Err(ProxyError::from(e));
                }
            };

        match response {
            SortedSetLengthByScoreResponse::Hit { length } => {
                ZCOUNT_HIT.increment();
                write!(response_buf, ":{}\r\n", length)?;
                klog_1(&"zcount", &req.key(), Status::Hit, response_buf.len());
            }
            SortedSetLengthByScoreResponse::Miss => {
                ZCOUNT_MISS.increment();
                write!(response_buf, ":0\r\n")?;
                klog_1(&"zcount", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
