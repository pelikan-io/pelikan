// Copyright 2022 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::cache::DictionarySetFieldsRequest;
use momento::CacheClient;
use protocol_resp::{HashSet, HSET, HSET_EX, HSET_STORED};

use crate::error::ProxyResult;
use crate::klog::{klog_7, Status};
use crate::ProxyError;
use crate::COLLECTION_TTL;

use super::update_method_metrics;

pub async fn hset(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashSet,
) -> ProxyResult {
    update_method_metrics(&HSET, &HSET_EX, async move {
        let elements: Vec<(Vec<u8>, Vec<u8>)> = req
            .data()
            .iter()
            .map(|(field, value)| (field.as_ref().to_vec(), value.as_ref().to_vec()))
            .collect();
        let _response = match tokio::time::timeout(
            Duration::from_millis(200),
            client.send_request(
                DictionarySetFieldsRequest::new(cache_name, req.key(), elements.clone())
                    .ttl(COLLECTION_TTL),
            ),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                for (field, value) in elements.iter() {
                    klog_7(
                        &"hset",
                        &req.key(),
                        field,
                        0,
                        value.len(),
                        Status::ServerError,
                        0,
                    );
                }
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                for (field, value) in elements.iter() {
                    klog_7(
                        &"hset",
                        &req.key(),
                        field,
                        0,
                        value.len(),
                        Status::Timeout,
                        0,
                    );
                }
                return Err(ProxyError::from(e));
            }
        };

        HSET_STORED.increment();
        for (field, value) in elements.iter() {
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

        Ok(())
    })
    .await
}
