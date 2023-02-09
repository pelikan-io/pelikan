// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;

use protocol_resp::{ListPopBack, RPOP, RPOP_EX};

use crate::*;

use super::update_method_metrics;

pub async fn rpop(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListPopBack,
) -> ProxyResult {
    update_method_metrics(&RPOP, &RPOP_EX, async move {
        let tout = Duration::from_millis(200);

        match req.count() {
            None => match timeout(tout, client.list_pop_back(cache_name, req.key())).await?? {
                Some(item) => {
                    write!(response_buf, "${}\r\n", item.len())?;
                    response_buf.extend_from_slice(&item);
                    response_buf.extend_from_slice(b"\r\n");
                }
                None => {
                    response_buf.extend_from_slice(b"$-1\r\n");
                }
            },
            Some(0) => match timeout(tout, client.list_length(cache_name, req.key())).await?? {
                Some(_) => response_buf.extend_from_slice(b"*0\r\n"),
                None => response_buf.extend_from_slice(b"*-1\r\n"),
            },
            Some(count) => {
                let mut items = Vec::with_capacity(count.min(64) as usize);

                // Momento doesn't provide a single operation to do what we want here. To make it
                // work there are two options for emulating things here:
                // 1. list_fetch + list_erase
                // 2. a series of list_pop_back calls
                //
                // Both have their own disadvantages. #1 has the potential lose elements or return
                // the same element in concurrent RPOP commands. #2 may lose the ordering but at
                // least removing each individual element is atomic.
                //
                // We use #2 here since I think re-ordering list elements is less bad then
                // potentialy losing or duplicating elements.
                for _ in 0..count {
                    match timeout(tout, client.list_pop_back(cache_name, req.key())).await?? {
                        Some(item) => items.push(item),
                        None => break,
                    }
                }

                // We got no elements, the list does not exist.
                if items.is_empty() {
                    response_buf.extend_from_slice(b"*-1\r\n");
                } else {
                    write!(response_buf, "*{}\r\n", items.len())?;

                    for element in items {
                        write!(response_buf, "${}\r\n", element.len())?;
                        response_buf.extend_from_slice(&element);
                        response_buf.extend_from_slice(b"\r\n");
                    }
                }
            }
        }

        Ok(())
    })
    .await
}
