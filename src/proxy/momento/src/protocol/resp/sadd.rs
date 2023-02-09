// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::SimpleCacheClient;
use protocol_resp::{SetAdd, SADD, SADD_EX};

use crate::error::ProxyResult;
use crate::COLLECTION_TTL;

use super::update_method_metrics;

pub async fn sadd(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetAdd,
) -> ProxyResult {
    update_method_metrics(&SADD, &SADD_EX, async move {
        let elements = req.members().iter().map(|e| &**e).collect();

        tokio::time::timeout(
            Duration::from_millis(200),
            client.set_union(cache_name, req.key(), elements, COLLECTION_TTL),
        )
        .await??;

        // Momento doesn't return the info we need here so we pretend that
        // all the elements were added to the set.
        write!(response_buf, ":{}\r\n", req.members().len())?;

        Ok(())
    })
    .await
}
