// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::ListFetchResponse;
use momento::CacheClient;
use protocol_resp::{ListIndex, LINDEX, LINDEX_EX, LINDEX_HIT, LINDEX_MISS};

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn lindex(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListIndex,
) -> ProxyResult {
    update_method_metrics(&LINDEX, &LINDEX_EX, async move {
        let entry = match tokio::time::timeout(
            Duration::from_millis(200),
            client.list_fetch(cache_name, req.key()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                let index = format!("{}", req.index());
                klog_2(&"lindex", &req.key(), &index, Status::ServerError, 0);

                return Err(ProxyError::from(e));
            }
            Err(e) => {
                let index = format!("{}", req.index());
                klog_2(&"lindex", &req.key(), &index, Status::Timeout, 0);

                return Err(ProxyError::from(e));
            }
        };

        match entry {
            ListFetchResponse::Hit { values } => {
                // TODO need to impl from here
                let list: Vec<Vec<u8>> = values.into();
                let index: Option<usize> = match req.index() {
                    index @ 0.. => index.try_into().ok(),
                    index => (-index)
                        .try_into()
                        .map(|index: usize| list.len() - index)
                        .ok(),
                };

                let status = match index.and_then(|index| list.get(index)).map(|x| &**x) {
                    Some(element) => {
                        write!(response_buf, "${}\r\n", element.len())?;
                        response_buf.extend_from_slice(element);
                        response_buf.extend_from_slice(b"\r\n");

                        LINDEX_HIT.increment();
                        Status::Hit
                    }
                    None => {
                        write!(response_buf, "$-1\r\n")?;

                        LINDEX_MISS.increment();
                        Status::Miss
                    }
                };

                let index = format!("{}", req.index());
                klog_2(&"lindex", &req.key(), &index, status, response_buf.len())
            }
            ListFetchResponse::Miss => {
                write!(response_buf, "$-1\r\n")?;

                LINDEX_MISS.increment();

                let index = format!("{}", req.index());
                klog_2(
                    &"lindex",
                    &req.key(),
                    &index,
                    Status::Miss,
                    response_buf.len(),
                )
            }
        }
        Ok(())
    })
    .await
}
