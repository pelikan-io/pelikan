// Copyright 2022 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::response::MomentoDictionaryGetStatus;
use momento::SimpleCacheClient;
use protocol_resp::{
    HashMultiGet, HMGET, HMGET_EX, HMGET_FIELD, HMGET_FIELD_HIT, HMGET_FIELD_MISS,
};

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};
use crate::ProxyError;
use crate::BACKEND_EX;

use super::update_method_metrics;

pub async fn hmget(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashMultiGet,
) -> ProxyResult {
    update_method_metrics(&HMGET, &HMGET_EX, async move {
        let fields: Vec<_> = req.fields().iter().map(|x| &**x).collect();
        let response = match tokio::time::timeout(
            Duration::from_millis(200),
            client.dictionary_get(cache_name, req.key(), fields),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                for field in req.fields() {
                    klog_2(&"hmget", &req.key(), field, Status::ServerError, 0);
                }
                return Err(ProxyError::from(e));
            }
            Err(e) => {
                for field in req.fields() {
                    klog_2(&"hmget", &req.key(), field, Status::Timeout, 0);
                }
                return Err(ProxyError::from(e));
            }
        };

        match response.result {
            MomentoDictionaryGetStatus::ERROR => {
                // we got some error from
                // the backend.
                BACKEND_EX.increment();
                HMGET_EX.increment();
                response_buf.extend_from_slice(b"-ERR backend error\r\n");
            }
            MomentoDictionaryGetStatus::FOUND => {
                if response.dictionary.is_none() {
                    error!("error for hmget: dictionary found but not provided in response");
                    BACKEND_EX.increment();
                    HMGET_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                } else {
                    let dictionary = response.dictionary.as_ref().unwrap();

                    response_buf
                        .extend_from_slice(format!("*{}\r\n", req.fields().len()).as_bytes());

                    let mut hit = 0;
                    let mut miss = 0;

                    for field in req.fields() {
                        if let Some(value) = dictionary.get(&**field) {
                            hit += 1;
                            klog_2(&"hmget", &req.key(), field, Status::Hit, value.len());

                            let item_header = format!("${}\r\n", value.len());

                            response_buf.extend_from_slice(item_header.as_bytes());
                            response_buf.extend_from_slice(value);
                            response_buf.extend_from_slice(b"\r\n");
                        } else {
                            miss += 1;
                            klog_2(&"hmget", &req.key(), field, Status::Miss, 0);
                            response_buf.extend_from_slice(b"$-1\r\n");
                        }
                    }

                    HMGET_FIELD.add(req.fields().len() as u64);
                    HMGET_FIELD_HIT.add(hit);
                    HMGET_FIELD_MISS.add(miss);
                }
            }
            MomentoDictionaryGetStatus::MISSING => {
                // treat every requested field as a miss
                response_buf.extend_from_slice(format!("*{}\r\n", req.fields().len()).as_bytes());

                for field in req.fields() {
                    klog_2(&"hmget", &req.key(), field, Status::Miss, 0);
                    response_buf.extend_from_slice(b"$-1\r\n");
                }

                HMGET_FIELD_MISS.add(req.fields().len() as u64);
            }
        }

        Ok(())
    })
    .await
}
