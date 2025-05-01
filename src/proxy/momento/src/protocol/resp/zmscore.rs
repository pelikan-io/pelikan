// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::cache::{SortedSetGetScoreResponse, SortedSetGetScoresResponse};
use momento::CacheClient;
use protocol_resp::{SortedSetMultiScore, ZMSCORE, ZMSCORE_EX, ZMSCORE_HIT, ZMSCORE_MISS};
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
        let num_members = members.len();
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
                    match response {
                        SortedSetGetScoreResponse::Hit { score } => {
                            ZMSCORE_HIT.increment();
                            let score_str = score.to_string();
                            response_buf.extend_from_slice(
                                format!("${}\r\n{}\r\n", score_str.len(), score_str).as_bytes(),
                            );
                        }
                        SortedSetGetScoreResponse::Miss => {
                            ZMSCORE_MISS.increment();
                            // Add nil to list if the element was not found
                            response_buf.extend_from_slice(b"_\r\n");
                        }
                    };
                }
                klog_1(&"zmscore", &req.key(), Status::Hit, response_buf.len());
            }
            SortedSetGetScoresResponse::Miss => {
                // Return list of nil for each missing element
                ZMSCORE_MISS.increment();
                response_buf.extend_from_slice(format!("*{}\r\n", num_members).as_bytes());
                for _ in 0..num_members {
                    response_buf.extend_from_slice(b"_\r\n");
                }
                klog_1(&"zmscore", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
