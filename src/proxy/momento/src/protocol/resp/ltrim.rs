// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::ops::Bound;
use std::time::Duration;

use momento::CacheClient;
use protocol_resp::{ListTrim, LTRIM, LTRIM_EX};
use tokio::time::timeout;

use crate::error::ProxyResult;

use super::update_method_metrics;

pub async fn ltrim(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListTrim,
) -> ProxyResult {
    update_method_metrics(&LTRIM, &LTRIM_EX, async move {
        let tout = Duration::from_millis(200);
        let len = timeout(tout, client.list_length(cache_name, req.key()))
            .await??
            .unwrap_or(0);

        let start: Bound<u32> = match req.start() {
            start @ 0.. => start
                .try_into()
                .map(Bound::Excluded)
                .unwrap_or(Bound::Unbounded),
            start @ ..=-1 => Bound::Excluded(
                (-start)
                    .try_into()
                    .map(|inv| len.saturating_sub(inv))
                    .unwrap_or(0),
            ),
        };
        let end: Bound<u32> = match req.stop() {
            end @ 0.. => end
                .try_into()
                .map(Bound::Excluded)
                .unwrap_or(Bound::Excluded(u32::MAX)),
            end @ ..=-1 => (-end)
                .try_into()
                .map(|inv| len.checked_sub(inv).map(Bound::Excluded))
                .unwrap_or(Some(Bound::Unbounded))
                .unwrap_or(Bound::Unbounded),
        };

        timeout(
            tout,
            client.list_erase_many(
                cache_name,
                req.key(),
                [(Bound::Unbounded, start), (end, Bound::Unbounded)],
            ),
        )
        .await??;

        response_buf.extend_from_slice(b"+OK\r\n");

        Ok(())
    })
    .await
}
