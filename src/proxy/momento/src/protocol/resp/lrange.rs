// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::{cache::ListFetchResponse, CacheClient};
use protocol_resp::{ListRange, LRANGE, LRANGE_EX};
use tokio::time::timeout;

use crate::error::ProxyResult;

use super::update_method_metrics;

pub async fn lrange(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListRange,
) -> ProxyResult {
    update_method_metrics(&LRANGE, &LRANGE_EX, async move {
        let list_fetch_response = timeout(
            Duration::from_millis(200),
            client.list_fetch(cache_name, req.key()),
        )
        .await??;

        match list_fetch_response {
            ListFetchResponse::Hit { values } => {
                let list: Vec<Vec<u8>> = values.into();

                let start: usize = match req.start() {
                    start @ 0.. => start.try_into().unwrap_or(usize::MAX),
                    start @ ..=-1 => {
                        let inv = (-start).try_into().unwrap_or(usize::MAX);
                        list.len().saturating_sub(inv)
                    }
                };
                let end: usize = match req.stop() {
                    end @ 0.. => end.try_into().unwrap_or(usize::MAX),
                    end @ ..=-1 => {
                        let inv = (-end).try_into().unwrap_or(usize::MAX);

                        match list.len().checked_sub(inv) {
                            Some(end) => end,
                            None => {
                                response_buf.extend_from_slice(b"*0\r\n");
                                return Ok(());
                            }
                        }
                    }
                };

                if start >= list.len() || start > end {
                    response_buf.extend_from_slice(b"*0\r\n");
                    return Ok(());
                }

                let start = start.min(list.len());
                let end = end.min(list.len().saturating_sub(1));

                let elems = match list.get(start..=end) {
                    Some(elems) => elems,
                    None => {
                        response_buf.extend_from_slice(b"*0\r\n");
                        return Ok(());
                    }
                };

                write!(response_buf, "*{}\r\n", elems.len())?;

                for elem in elems {
                    write!(response_buf, "${}\r\n", elem.len())?;
                    response_buf.extend_from_slice(elem);
                }
            }
            ListFetchResponse::Miss => {
                response_buf.extend_from_slice(b"*0\r\n");
            }
        }

        Ok(())
    })
    .await
}
