// Copyright 2022 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::*;
use crate::*;
use protocol_resp::*;
use std::io::Write;

use super::update_method_metrics;

pub async fn hdel(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &HashDelete,
) -> ProxyResult {
    update_method_metrics(&HDEL, &HDEL_EX, async move {
        HDEL.increment();

        let fields: Vec<&[u8]> = req.fields().iter().map(|f| &**f).collect();
        timeout(
            Duration::from_millis(200),
            client.dictionary_delete(cache_name, req.key(), Fields::Some(fields)),
        )
        .await??;

        // NOTE: the Momento protocol does not inform us of how many fields are
        // deleted. We lie to the client and say that they all were deleted.
        write!(response_buf, ":{}\r\n", req.fields().len())?;

        for field in req.fields() {
            klog_2(&"hdel", &req.key(), field, Status::Deleted, 0);
        }

        Ok(())
    })
    .await
}
