// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;
use std::fmt::Debug;
use std::io::Write;
use std::time::Duration;

use momento::cache::SetFetchResponse;
use momento::{CacheClient, MomentoError};
use protocol_resp::{SetUnion, SUNION, SUNION_EX};
use tokio::time;

use crate::ProxyResult;

use super::update_method_metrics;

pub async fn sunion(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetUnion,
) -> ProxyResult {
    update_method_metrics(&SUNION, &SUNION_EX, async move {
        let timeout = Duration::from_millis(200);
        let mut set = HashSet::new();

        for key in req.keys() {
            let key = &**key;

            let response: Result<SetFetchResponse, MomentoError> =
                time::timeout(timeout, client.set_fetch(cache_name, key)).await?;

            match response {
                Ok(response) => {
                    match response {
                        SetFetchResponse::Hit { values } => {
                            for entry in values.into() {
                                set.insert(entry);
                            }
                        }
                        SetFetchResponse::Miss {} => {}
                    }
                }
                Err(e) => {}
            }
        }

        write!(response_buf, "*{}\r\n", set.len())?;

        for entry in &set {
            write!(response_buf, "${}\r\n", entry.len())?;
            response_buf.extend_from_slice(entry.concat().as_bytes());
        }

        Ok(())
    })
    .await
}
