// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::CacheClient;
use protocol_resp::{SortedSetRemove, ZREM, ZREM_EX};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zrem(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetRemove,
) -> ProxyResult {
    update_method_metrics(&ZREM, &ZREM_EX, async move {
        let members: Vec<_> = req.members().iter().map(|x| &**x).collect();
        let number_of_elements_removed = members.len();
        match time::timeout(
            Duration::from_millis(200),
            client.sorted_set_remove_elements(cache_name, req.key(), members),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"zrem", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"zrem", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        // If there was no error, we assume all the elements were removed and return the number of elements removed
        write!(response_buf, ":{}\r\n", number_of_elements_removed)?;
        klog_1(&"zrem", &req.key(), Status::Hit, response_buf.len());

        Ok(())
    })
    .await
}
