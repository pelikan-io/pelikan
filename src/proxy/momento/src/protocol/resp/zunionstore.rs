// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::{SortedSetAggregateFunction, SortedSetUnionStoreRequest};
use momento::CacheClient;
use protocol_resp::AggregateFunction;
use protocol_resp::{SortedSetUnionStore, ZUNIONSTORE, ZUNIONSTORE_EX};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zunionstore(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetUnionStore,
) -> ProxyResult {
    update_method_metrics(&ZUNIONSTORE, &ZUNIONSTORE_EX, async move {
        let aggregate_function = if let Some(aggregate_function) = req.aggregate_function() {
            match aggregate_function {
                AggregateFunction::Sum => SortedSetAggregateFunction::Sum,
                AggregateFunction::Min => SortedSetAggregateFunction::Min,
                AggregateFunction::Max => SortedSetAggregateFunction::Max,
            }
        } else {
            SortedSetAggregateFunction::Sum
        };

        // If weights are provided, use them. Otherwise, provide only the sorted set source names.
        let sources: Vec<(Vec<u8>, f32)> = if let Some(weights) = req.weights() {
            req.source_keys()
                .iter()
                .zip(weights.iter())
                .map(|(source, weight)| {
                    let weight_float_str = std::str::from_utf8(weight)
                        .map_err(|_| ProxyError::custom("Invalid UTF-8 from weight"))?;
                    let weight_float = weight_float_str
                        .parse::<f32>()
                        .map_err(|_| ProxyError::custom("Invalid float from weight"))?;
                    Ok((source.to_vec(), weight_float))
                })
                .collect::<Result<Vec<(Vec<u8>, f32)>, ProxyError>>()?
        } else {
            // Default weight of 1.0
            req.source_keys()
                .iter()
                .map(|key| (key.to_vec(), 1.0))
                .collect()
        };

        let request = SortedSetUnionStoreRequest::new(cache_name, req.destination_key(), sources)
            .aggregate(aggregate_function);

        let response =
            match time::timeout(Duration::from_millis(200), client.send_request(request)).await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    klog_1(
                        &"zunionstore",
                        &req.destination_key(),
                        Status::ServerError,
                        0,
                    );
                    return Err(ProxyError::from(e));
                }
                Err(e) => {
                    klog_1(&"zunionstore", &req.destination_key(), Status::Timeout, 0);
                    return Err(ProxyError::from(e));
                }
            };

        // Return the number of elements in the destination sorted set
        write!(response_buf, ":{}\r\n", response.length)?;
        klog_1(
            &"zunionstore",
            &req.destination_key(),
            Status::Stored,
            response_buf.len(),
        );

        Ok(())
    })
    .await
}
