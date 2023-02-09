// Copyright 2022 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::time::Duration;

use momento::response::MomentoDictionarySetStatus;
use momento::SimpleCacheClient;
use protocol_resp::{HashSet, HSET, HSET_EX, HSET_STORED};

use crate::error::ProxyResult;
use crate::klog::{klog_7, Status};
use crate::{BACKEND_EX, COLLECTION_TTL};

use super::update_method_metrics;

pub async fn hset(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashSet,
) -> ProxyResult {
    update_method_metrics(&HSET, &HSET_EX, async move {
        let mut map: HashMap<&[u8], &[u8]> = std::collections::HashMap::new();
        for (field, value) in req.data() {
            map.insert(&**field, &**value);
        }

        let response = tokio::time::timeout(
            Duration::from_millis(200),
            client.dictionary_set(cache_name, req.key(), map.clone(), COLLECTION_TTL),
        )
        .await??;

        match response.result {
            MomentoDictionarySetStatus::ERROR => {
                // we got some error from
                // the backend.
                BACKEND_EX.increment();
                HSET_EX.increment();
                response_buf.extend_from_slice(b"-ERR backend error\r\n");
            }
            MomentoDictionarySetStatus::OK => {
                HSET_STORED.increment();
                for (field, value) in map.iter() {
                    klog_7(
                        &"hset",
                        &req.key(),
                        field,
                        0,
                        value.len(),
                        Status::Stored,
                        0,
                    );
                }
                response_buf.extend_from_slice(format!(":{}\r\n", req.data().len()).as_bytes());
            }
        }

        Ok(())
    })
    .await
}
