// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::{klog_set, Status};
use crate::{Error, *};
use momento::cache::SetRequest;
use pelikan_net::*;
use protocol_memcache::*;

pub async fn set(
    client: &mut CacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    request: &protocol_memcache::Set,
) -> Result<(), Error> {
    SET.increment();

    let key = request.key();
    let value = request.value();

    if value.is_empty() {
        error!("empty values are not supported by momento");
        let _ = socket.write_all(b"ERROR\r\n").await;

        return Err(Error::from(ErrorKind::InvalidInput));
    }

    BACKEND_REQUEST.increment();

    let ttl = request
        .ttl()
        .get()
        .map(|ttl| Duration::from_secs(ttl.max(1) as u64));

    match timeout(
        Duration::from_millis(200),
        client.send_request(SetRequest::new(cache_name, key, value).ttl(ttl)),
    )
    .await
    {
        Ok(Ok(_result)) => {
            SET_STORED.increment();
            if request.noreply() {
                klog_set(
                    &key,
                    request.flags(),
                    request.ttl().get().unwrap_or(0),
                    value.len(),
                    Status::Stored,
                    0,
                );
            } else {
                klog_set(
                    &key,
                    request.flags(),
                    request.ttl().get().unwrap_or(0),
                    value.len(),
                    Status::Stored,
                    8,
                );
                SESSION_SEND.increment();
                SESSION_SEND_BYTE.add(8);
                TCP_SEND_BYTE.add(8);
                if let Err(e) = socket.write_all(b"STORED\r\n").await {
                    SESSION_SEND_EX.increment();
                    // hangup if we can't send a response back
                    return Err(e);
                }
            }
        }
        Ok(Err(e)) => {
            BACKEND_EX.increment();

            SET_EX.increment();
            SESSION_SEND.increment();

            klog_set(
                &key,
                request.flags(),
                request.ttl().get().unwrap_or(0),
                value.len(),
                Status::ServerError,
                0,
            );

            let message = format!("SERVER_ERROR {e}\r\n");

            SESSION_SEND_BYTE.add(message.len() as _);
            TCP_SEND_BYTE.add(message.len() as _);

            if let Err(e) = socket.write_all(message.as_bytes()).await {
                SESSION_SEND_EX.increment();
                // hangup if we can't send a response back
                return Err(e);
            }
        }
        Err(_) => {
            // timeout
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();

            SET_EX.increment();
            SESSION_SEND.increment();

            klog_set(
                &key,
                request.flags(),
                request.ttl().get().unwrap_or(0),
                value.len(),
                Status::Timeout,
                0,
            );

            let message = "SERVER_ERROR backend timeout\r\n";

            SESSION_SEND_BYTE.add(message.len() as _);
            TCP_SEND_BYTE.add(message.len() as _);

            if let Err(e) = socket.write_all(message.as_bytes()).await {
                SESSION_SEND_EX.increment();
                // hangup if we can't send a response back
                return Err(e);
            }
        }
    }

    Ok(())
}
