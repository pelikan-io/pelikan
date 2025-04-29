// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::{SortedSetFetchByScoreRequest, SortedSetFetchResponse, SortedSetOrder};
use momento::CacheClient;
use protocol_resp::{
    MomentoSortedSetFetchArgs, SortedSetRange, StartStopValue, ZRANGE, ZRANGE_EX, ZRANGE_HIT,
    ZRANGE_MISS,
};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zrange(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetRange,
) -> ProxyResult {
    update_method_metrics(&ZRANGE, &ZRANGE_EX, async move {
        let response = match req.args() {
            MomentoSortedSetFetchArgs::ByRank(start, stop) => {
                let order = if req.reversed() {
                    SortedSetOrder::Descending
                } else {
                    SortedSetOrder::Ascending
                };
                match time::timeout(
                    Duration::from_millis(200),
                    client.sorted_set_fetch_by_rank(
                        cache_name,
                        req.key(),
                        order,
                        Some(*start as i32),
                        Some(*stop as i32),
                    ),
                )
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        klog_1(&"zrange", &req.key(), Status::ServerError, 0);
                        return Err(ProxyError::from(e));
                    }
                    Err(e) => {
                        klog_1(&"zrange", &req.key(), Status::Timeout, 0);
                        return Err(ProxyError::from(e));
                    }
                }
            }
            MomentoSortedSetFetchArgs::ByScore(start, stop, offset, count) => {
                let order = if req.reversed() {
                    SortedSetOrder::Descending
                } else {
                    SortedSetOrder::Ascending
                };

                let start = match start {
                    StartStopValue::Inclusive(s) => *s as f64,
                    StartStopValue::Exclusive(s) => (*s + 1) as f64,
                    StartStopValue::PositiveInfinity => f64::INFINITY,
                    StartStopValue::NegativeInfinity => f64::NEG_INFINITY,
                };

                let stop = match stop {
                    StartStopValue::Inclusive(s) => *s as f64,
                    StartStopValue::Exclusive(s) => (*s - 1) as f64,
                    StartStopValue::PositiveInfinity => f64::INFINITY,
                    StartStopValue::NegativeInfinity => f64::NEG_INFINITY,
                };

                let fetch_request = SortedSetFetchByScoreRequest::new(cache_name, req.key())
                    .order(order)
                    .min_score(start)
                    .max_score(stop)
                    .offset(offset.map(|o| o as u32))
                    .count(count.map(|c| c as i32));

                match time::timeout(
                    Duration::from_millis(200),
                    client.send_request(fetch_request),
                )
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        klog_1(&"zrange", &req.key(), Status::ServerError, 0);
                        return Err(ProxyError::from(e));
                    }
                    Err(e) => {
                        klog_1(&"zrange", &req.key(), Status::Timeout, 0);
                        return Err(ProxyError::from(e));
                    }
                }
            }
        };

        match response {
            SortedSetFetchResponse::Hit { value } => {
                ZRANGE_HIT.increment();
                if req.with_scores() {
                    // Return elements and scores
                    response_buf
                        .extend_from_slice(format!("*{}\r\n", value.elements.len() * 2).as_bytes());
                } else {
                    // Return elements only
                    response_buf
                        .extend_from_slice(format!("*{}\r\n", value.elements.len()).as_bytes());
                }

                for (element, score) in value.elements {
                    // write the element header
                    response_buf.extend_from_slice(format!("${}\r\n", element.len()).as_bytes());
                    // write the element
                    response_buf.extend_from_slice(&element);
                    response_buf.extend_from_slice(b"\r\n");

                    if req.with_scores() {
                        // write the score header
                        response_buf.extend_from_slice(
                            format!("${}\r\n", score.to_string().len()).as_bytes(),
                        );
                        // write the score
                        response_buf.extend_from_slice(score.to_string().as_bytes());
                        response_buf.extend_from_slice(b"\r\n");
                    }
                }
                klog_1(&"zrange", &req.key(), Status::Hit, response_buf.len());
            }
            SortedSetFetchResponse::Miss => {
                ZRANGE_MISS.increment();
                // return empty list on miss
                write!(response_buf, "*0\r\n")?;
                klog_1(&"zrange", &req.key(), Status::Miss, response_buf.len());
            }
        }
        Ok(())
    })
    .await
}
