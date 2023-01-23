// Copyright 2022 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::*;
use crate::{Error, *};
use ::net::*;
use protocol_resp::*;
use std::sync::Arc;

pub async fn hdel(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    key: &[u8],
    fields: &[Arc<Box<[u8]>>],
) -> Result<(), Error> {
    HDEL.increment();

    // check if the key is valid
    if std::str::from_utf8(key).is_err() {
        HDEL_EX.increment();

        // invalid key
        let _ = socket.write_all(b"-ERR invalid key\r\n").await;
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    // check if the fields are valid before
    // sending the request to the backend
    for field in fields.iter() {
        if std::str::from_utf8(field).is_err() {
            HMGET_EX.increment();

            // invalid field
            let _ = socket.write_all(b"-ERR invalid field\r\n").await;
            return Err(Error::from(ErrorKind::InvalidInput));
        }
    }

    let mut response_buf = Vec::new();

    BACKEND_REQUEST.increment();

    // already checked the key and field, so we know these unwraps are safe
    let key = std::str::from_utf8(key).unwrap().to_owned();

    let fields: Vec<String> = fields
        .iter()
        .map(|f| std::str::from_utf8(f).unwrap().to_owned())
        .collect();

    match timeout(
        Duration::from_millis(200),
        client.dictionary_delete(cache_name, &key, Fields::Some(fields.clone())),
    )
    .await
    {
        Ok(Ok(_)) => {
            // NOTE: the Momento protocol does not inform us of how many fields are
            // deleted. We lie to the client and say that they all were deleted.
            response_buf.extend_from_slice(format!(":{}\r\n", fields.len()).as_bytes());

            for field in &fields {
                klog_2(&"hdel", &key, field, Status::Deleted, 0);
            }
        }
        Ok(Err(MomentoError::LimitExceeded(_))) => {
            BACKEND_EX.increment();
            BACKEND_EX_RATE_LIMITED.increment();
            HDEL_EX.increment();
            response_buf.extend_from_slice(b"-ERR ratelimit exceed\r\n");
        }
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and incr stats and move on treating it
            // as an error
            error!("error for hdel: {}", e);
            BACKEND_EX.increment();
            HDEL_EX.increment();
            response_buf.extend_from_slice(b"-ERR backend error\r\n");
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as an error
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();
            HDEL_EX.increment();
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
