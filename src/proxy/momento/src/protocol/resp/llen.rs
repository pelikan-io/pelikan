// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::ListLengthResponse;
use momento::CacheClient;
use protocol_resp::{ListLen, LLEN, LLEN_EX};

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn llen(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListLen,
) -> ProxyResult {
    update_method_metrics(&LLEN, &LLEN_EX, async move {
        let length_response = match tokio::time::timeout(
            Duration::from_millis(200),
            client.list_length(cache_name, req.key()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"llen", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"llen", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
        };

        match length_response {
            ListLengthResponse::Hit { length } => {
                write!(response_buf, ":{}\r\n", length)?;
                klog_1(&"llen", &req.key(), Status::Hit, response_buf.len());
            }
            ListLengthResponse::Miss => {
                write!(response_buf, ":0\r\n")?;
                klog_1(&"llen", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
