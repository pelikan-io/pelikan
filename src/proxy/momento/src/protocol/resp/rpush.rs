// Copyright 2023 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;

use crate::*;
use protocol_resp::{ListPushBack, RPUSH, RPUSH_EX};

use super::update_method_metrics;

pub async fn rpush(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListPushBack,
) -> ProxyResult {
    update_method_metrics(&RPUSH, &RPUSH_EX, async move {
        let count = timeout(
            Duration::from_millis(200),
            client.list_concat_back(
                cache_name,
                req.key(),
                req.elements().iter().map(|e| &e[..]),
                None,
                COLLECTION_TTL,
            ),
        )
        .await??;

        write!(response_buf, ":{count}\r\n")?;

        Ok(())
    })
    .await
}
