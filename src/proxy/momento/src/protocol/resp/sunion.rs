// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;
use std::io::Write;
use std::time::Duration;

use momento::SimpleCacheClient;
use protocol_resp::{SetUnion, SUNION, SUNION_EX};
use tokio::time;

use crate::ProxyResult;

use super::update_method_metrics;

pub async fn sunion(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetUnion,
) -> ProxyResult {
    update_method_metrics(&SUNION, &SUNION_EX, async move {
        let timeout = Duration::from_millis(200);
        let mut set = HashSet::new();

        for key in req.keys() {
            let key = &**key;

            let response = time::timeout(timeout, client.set_fetch(cache_name, key)).await??;
            if let Some(value) = response.value {
                for entry in value {
                    set.insert(entry);
                }
            }
        }

        write!(response_buf, "*{}\r\n", set.len())?;

        for entry in &set {
            write!(response_buf, "${}\r\n", entry.len())?;
            response_buf.extend_from_slice(entry);
        }

        Ok(())
    })
    .await
}
