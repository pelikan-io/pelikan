// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;

use crate::*;
use protocol_resp::{ListPush, LPUSH, LPUSH_EX};

use super::update_method_metrics;

pub async fn lpush(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListPush,
) -> ProxyResult {
    update_method_metrics(&LPUSH, &LPUSH_EX, async move {
        let count = timeout(
            Duration::from_millis(200),
            client.list_concatenate_front(
                cache_name,
                req.key(),
                req.elements().iter().map(|e| &e[..]),
            ),
        )
        .await??;
        Ok(())
    })
    .await
}
