// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::SimpleCacheClient;
use net::TCP_SEND_BYTE;
use protocol_resp::{ListIndex, LINDEX, LINDEX_EX, LINDEX_HIT, LINDEX_MISS};
use session::{SESSION_SEND, SESSION_SEND_BYTE, SESSION_SEND_EX};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::klog::{klog_2, Status};
use crate::{BACKEND_EX, BACKEND_EX_TIMEOUT, BACKEND_REQUEST};

use super::momento_error_to_resp_error;

pub async fn lindex(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut TcpStream,
    req: &ListIndex,
) -> std::io::Result<()> {
    LINDEX.increment();
    BACKEND_REQUEST.increment();

    let mut response_buf = Vec::new();

    match timeout(
        Duration::from_millis(200),
        client.list_fetch(cache_name, req.key()),
    )
    .await
    {
        Ok(Ok(Some(entry))) => {
            let list = entry.value();
            let index: Option<usize> = match req.index() {
                index @ 0.. => index.try_into().ok(),
                index => (-index)
                    .try_into()
                    .map(|index: usize| list.len() - index)
                    .ok(),
            };

            let status = match index.and_then(|index| list.get(index)).map(|x| &**x) {
                Some(element) => {
                    write!(&mut response_buf, "${}\r\n", element.len())?;
                    response_buf.extend_from_slice(element);
                    response_buf.extend_from_slice(b"\r\n");

                    LINDEX_HIT.increment();
                    Status::Hit
                }
                None => {
                    write!(&mut response_buf, "$-1\r\n")?;

                    LINDEX_MISS.increment();
                    Status::Miss
                }
            };

            let index = format!("{}", req.index());
            klog_2(&"lindex", &req.key(), &index, status, response_buf.len())
        }
        Ok(Ok(None)) => {
            write!(&mut response_buf, "$-1\r\n")?;

            LINDEX_MISS.increment();

            let index = format!("{}", req.index());
            klog_2(
                &"lindex",
                &req.key(),
                &index,
                Status::Miss,
                response_buf.len(),
            )
        }
        Ok(Err(error)) => {
            LINDEX_EX.increment();
            momento_error_to_resp_error(&mut response_buf, "lindex", error);
        }
        Err(_) => {
            LINDEX_EX.increment();
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();
            response_buf.extend_from_slice(b"-ERR backend timeout\r\n");
        }
    }

    SESSION_SEND.increment();
    SESSION_SEND_BYTE.add(response_buf.len() as _);
    TCP_SEND_BYTE.add(response_buf.len() as _);

    if let Err(e) = socket.write_all(&response_buf).await {
        SESSION_SEND_EX.increment();
        return Err(e);
    }

    Ok(())
}
