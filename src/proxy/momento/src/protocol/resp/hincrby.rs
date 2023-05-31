// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::SimpleCacheClient;
use protocol_resp::{HashIncrBy, HINCRBY, HINCRBY_EX};

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;
use crate::COLLECTION_TTL;

use super::update_method_metrics;

pub async fn hincrby(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashIncrBy,
) -> ProxyResult {
    update_method_metrics(&HINCRBY, &HINCRBY_EX, async move {
        let response = match tokio::time::timeout(
            Duration::from_millis(200),
            client.dictionary_increment(
                cache_name,
                req.key(),
                req.field(),
                req.increment(),
                COLLECTION_TTL,
            ),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"hincrby", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"hincrby", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        write!(response_buf, ":{}\r\n", response.value)?;
        klog_1(&"hincrby", &req.key(), Status::Hit, response_buf.len());

        Ok(())
    })
    .await
}
