// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::cache::{SortedSetGetScoreResponse, SortedSetGetScoresResponse};
use momento::CacheClient;
use protocol_resp::{SortedSetMultiScore, ZMSCORE, ZMSCORE_EX};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zmscore(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetMultiScore,
) -> ProxyResult {
    update_method_metrics(&ZMSCORE, &ZMSCORE_EX, async move {
        let members: Vec<_> = req.members().iter().map(|x| &**x).collect();
        let response: SortedSetGetScoresResponse<_> = match time::timeout(
            Duration::from_millis(200),
            client.sorted_set_get_scores(cache_name, req.key(), members),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"zmscore", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"zmscore", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            SortedSetGetScoresResponse::Hit {
                responses,
                values: _,
            } => {
                response_buf.extend_from_slice(format!("*{}\r\n", responses.len()).as_bytes());

                for response in responses {
                    let (score_header, score) = match response {
                        SortedSetGetScoreResponse::Hit { score } => {
                            let score_str = score.to_string();
                            (format!("${}\r\n", score_str.len()), score_str)
                        }
                        SortedSetGetScoreResponse::Miss => {
                            let nil_str = "nil".to_string();
                            (format!("${}\r\n", nil_str.len()), nil_str)
                        }
                    };

                    response_buf.extend_from_slice(score_header.as_bytes());
                    response_buf.extend_from_slice(score.as_bytes());
                    response_buf.extend_from_slice(b"\r\n");
                }
                klog_1(&"zmscore", &req.key(), Status::Hit, response_buf.len());
            }
            SortedSetGetScoresResponse::Miss => {
                // Return nil for missing sorted set
                response_buf.extend_from_slice(b"*1\r\n$3\r\nnil\r\n");
                klog_1(&"zmscore", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
