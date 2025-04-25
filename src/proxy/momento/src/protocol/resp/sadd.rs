// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::CacheClient;
use protocol_resp::{SetAdd, SADD, SADD_EX};

use crate::error::ProxyResult;

use super::update_method_metrics;

pub async fn sadd(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetAdd,
) -> ProxyResult {
    update_method_metrics(&SADD, &SADD_EX, async move {
        let elements: Vec<&[u8]> = req.members().iter().map(|e| &**e).collect();

        tokio::time::timeout(
            Duration::from_millis(200),
            client.set_add_elements(cache_name, req.key(), elements),
        )
        .await??;

        // Momento doesn't return the info we need here so we pretend that
        // all the elements were added to the set.
        write!(response_buf, ":{}\r\n", req.members().len())?;

        Ok(())
    })
    .await
}
