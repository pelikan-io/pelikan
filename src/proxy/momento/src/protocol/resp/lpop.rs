// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;

use crate::*;
use momento::cache::{ListLengthResponse, ListPopFrontResponse};
use protocol_resp::{ListPop, LPOP, LPOP_EX};

use super::update_method_metrics;

pub async fn lpop(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListPop,
) -> ProxyResult {
    update_method_metrics(&LPOP, &LPOP_EX, async move {
        let tout = Duration::from_millis(200);

        match req.count() {
            None => match timeout(tout, client.list_pop_front(cache_name, req.key())).await?? {
                ListPopFrontResponse::Hit { value } => {
                    let value: Vec<u8> = value.try_into()?;
                    write!(response_buf, "${}\r\n", value.len())?;
                    response_buf.extend_from_slice(&value);
                    response_buf.extend_from_slice(b"\r\n");
                }
                ListPopFrontResponse::Miss => {
                    response_buf.extend_from_slice(b"$-1\r\n");
                }
            },
            Some(0) => match timeout(tout, client.list_length(cache_name, req.key())).await?? {
                ListLengthResponse::Hit { length: _ } => response_buf.extend_from_slice(b"*0\r\n"),
                ListLengthResponse::Miss => response_buf.extend_from_slice(b"*-1\r\n"),
            },
            Some(count) => {
                let mut items = Vec::with_capacity(count.min(64) as usize);

                // Momento doesn't provide a single operation to do what we want here. To make it
                // work there are two options for emulating things here:
                // 1. list_fetch + list_erase
                // 2. a series of list_pop_front_calls
                //
                // Both have their own disadvantages. #1 has the potential lose elements or return
                // the same element in concurrent LPOP commands. #2 may lose the ordering but at
                // least removing each individual element is atomic.
                //
                // We use #2 here since I think re-ordering list elements is less bad then
                // potentialy losing or duplicating elements.
                for _ in 0..count {
                    match timeout(tout, client.list_pop_front(cache_name, req.key())).await?? {
                        ListPopFrontResponse::Hit { value } => {
                            let value: Vec<u8> = value.try_into()?;
                            items.push(value);
                        }
                        ListPopFrontResponse::Miss => break,
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
