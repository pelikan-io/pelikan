// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::response::DictionaryFetch;
use momento::SimpleCacheClient;
use protocol_resp::{HashKeys, HKEYS, HKEYS_EX, HKEYS_HIT, HKEYS_MISS};

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn hkeys(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashKeys,
) -> ProxyResult {
    update_method_metrics(&HKEYS, &HKEYS_EX, async move {
        let response = match tokio::time::timeout(
            Duration::from_millis(200),
            client.dictionary_fetch(cache_name, req.key()),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                klog_1(&"hkeys", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                klog_1(&"hkeys", &req.key(), Status::Timeout, 0);
                return Err(ProxyError::from(e));
            }
        };

        match response {
            DictionaryFetch::Hit { value } => {
                HKEYS_HIT.increment();
                let map: Vec<(Vec<u8>, Vec<u8>)> = value.collect_into();

                response_buf.extend_from_slice(format!("*{}\r\n", map.len()).as_bytes());

                for (field, _value) in map.iter() {
                    let field_header = format!("${}\r\n", field.len());

                    response_buf.extend_from_slice(field_header.as_bytes());
                    response_buf.extend_from_slice(field);
                    response_buf.extend_from_slice(b"\r\n");
                }

                klog_1(&"hkeys", &req.key(), Status::Hit, response_buf.len());
            }
            DictionaryFetch::Miss => {
                HKEYS_MISS.increment();
                klog_1(&"hkeys", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}
