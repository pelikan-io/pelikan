// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::SortedSetElement;
use momento::CacheClient;
use protocol_resp::{SortedSetAdd, SortedSetIncrement, ZADD, ZADD_EX};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::{parse_sorted_set_score, update_method_metrics, zincrby};

pub async fn zadd(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetAdd,
) -> ProxyResult {
    update_method_metrics(&ZADD, &ZADD_EX, async move {
        let number_of_elements_added = req.members().len();

        // If INCR is set, then ZADD should behave like ZINCRBY (as per the docs), which accepts only a single score-member pair
        if req.optional_args().incr {
            if req.members().len() != 1 {
                return Err(ProxyError::from(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "INCR option requires exactly one score-member pair",
                )));
            }
            let zincry_request =
                SortedSetIncrement::new(req.key(), &req.members()[0].0, &req.members()[0].1);
            zincrby(client, cache_name, response_buf, &zincry_request).await?;
        }

        // Otherwise it's a regular ZADD call, and we should convert scores to f64 values before making Momento call
        let mut converted_members: Vec<SortedSetElement<Vec<u8>>> = Vec::new();
        for (score, member) in req.members() {
            match parse_sorted_set_score(score) {
                Ok(float_score) => converted_members.push(SortedSetElement {
                    value: (**member).into(),
                    score: float_score,
                }),
                Err(e) => return Err(ProxyError::from(e)),
            }
        }

        match time::timeout(
            Duration::from_millis(200),
            client.sorted_set_put_elements(cache_name, req.key(), converted_members),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"zadd", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"zadd", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        // If there was no error, we assume all the elements were added and return the number of elements added
        write!(response_buf, ":{}\r\n", number_of_elements_added)?;
        klog_1(&"zadd", &req.key(), Status::Hit, response_buf.len());

        Ok(())
    })
    .await
}
