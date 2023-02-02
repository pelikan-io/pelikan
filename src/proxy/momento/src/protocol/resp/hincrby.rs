// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::SimpleCacheClient;
use net::TCP_SEND_BYTE;
use protocol_resp::*;
use session::{SESSION_SEND, SESSION_SEND_BYTE, SESSION_SEND_EX};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::klog::{klog_1, Status};
use crate::{BACKEND_EX, BACKEND_EX_TIMEOUT, BACKEND_REQUEST, COLLECTION_TTL};

use super::momento_error_to_resp_error;

pub async fn hincrby(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut TcpStream,
    req: &HashIncrBy,
) -> std::io::Result<()> {
    HINCRBY.increment();
    BACKEND_REQUEST.increment();

    let mut response_buf = Vec::new();

    match timeout(
        Duration::from_millis(200),
        client.dictionary_increment(
            cache_name,
            req.key(),
            req.field(),
            req.increment(),
            COLLECTION_TTL,
        ),
    )
    .await
    {
        Ok(Ok(response)) => {
            write!(&mut response_buf, ":{}\r\n", response.value).unwrap();
            klog_1(&"hincrby", &req.key(), Status::Hit, response_buf.len());
        }
        Ok(Err(error)) => {
            HINCRBY_EX.increment();
            momento_error_to_resp_error(&mut response_buf, "hincrby", error);
        }
        Err(_) => {
            HINCRBY_EX.increment();
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
