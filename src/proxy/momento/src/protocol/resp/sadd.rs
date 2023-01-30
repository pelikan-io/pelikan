// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::SimpleCacheClient;
use net::TCP_SEND_BYTE;
use protocol_resp::{SetAdd, SADD, SADD_EX};
use session::{SESSION_SEND, SESSION_SEND_BYTE, SESSION_SEND_EX};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::{BACKEND_EX, BACKEND_EX_TIMEOUT, BACKEND_REQUEST, COLLECTION_TTL};

use super::momento_error_to_resp_error;

pub async fn sadd(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut TcpStream,
    req: &SetAdd,
) -> std::io::Result<()> {
    SADD.increment();
    BACKEND_REQUEST.increment();

    let mut response_buf = Vec::new();
    let elements = req.members().into_iter().map(|e| &**e).collect();

    match timeout(
        Duration::from_millis(200),
        client.set_union(cache_name, req.key(), elements, COLLECTION_TTL),
    )
    .await
    {
        Ok(Ok(())) => {
            // Momento doesn't return the info we need here so we pretend that
            // all the elements were added to the set.
            write!(&mut response_buf, ":{}\r\n", req.members().len())?;
        }
        Ok(Err(error)) => {
            SADD_EX.increment();
            momento_error_to_resp_error(&mut response_buf, "sadd", error)
        }
        Err(_) => {
            SADD_EX.increment();
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();
            response_buf.extend_from_slice(b"-ERR backend timeout\r\n");
        }
    }

    SESSION_SEND.increment();
    SESSION_SEND_BYTE.add(response_buf.len() as _);
    TCP_SEND_BYTE.add(response_buf.len() as _);

    socket.write_all(&response_buf).await.map_err(|e| {
        SESSION_SEND_EX.increment();
        e
    })
}
