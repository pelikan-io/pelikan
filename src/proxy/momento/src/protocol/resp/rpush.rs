// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use protocol_resp::{ListPushBack, RPUSH, RPUSH_EX};

use super::update_method_metrics;

pub async fn rpush(
    client: &mut CacheClient,
    cache_name: &str,
    _: &mut Vec<u8>,
    req: &ListPushBack,
) -> ProxyResult {
    update_method_metrics(&RPUSH, &RPUSH_EX, async move {
        timeout(
            Duration::from_millis(200),
            client.list_concatenate_back(
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
