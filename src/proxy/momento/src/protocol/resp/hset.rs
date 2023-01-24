// Copyright 2022 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::*;
use crate::{Error, *};
use ::net::*;
use protocol_resp::*;
use std::sync::Arc;

pub async fn hset(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    key: &[u8],
    data: &[(Arc<[u8]>, Arc<[u8]>)],
) -> Result<(), Error> {
    HSET.increment();

    // check if the key is valid
    if std::str::from_utf8(key).is_err() {
        HSET_EX.increment();

        // invalid key
        let _ = socket.write_all(b"-ERR invalid key\r\n").await;
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    // check if all of the data is valid before
    // sending the requests to the backend
    for (field, value) in data.iter() {
        if std::str::from_utf8(field).is_err() || std::str::from_utf8(value).is_err() {
            HSET_EX.increment();

            // invalid field
            let _ = socket.write_all(b"ERROR\r\n").await;
            return Err(Error::from(ErrorKind::InvalidInput));
        }

        if value.is_empty() {
            HSET_EX.increment();
            error!("empty values are not supported by momento");
            SESSION_SEND.increment();
            SESSION_SEND_BYTE.add(7);
            TCP_SEND_BYTE.add(7);

            if socket.write_all(b"ERROR\r\n").await.is_err() {
                SESSION_SEND_EX.increment();
            }
            return Err(Error::from(ErrorKind::InvalidInput));
        }
    }

    let mut response_buf = Vec::new();

    BACKEND_REQUEST.increment();

    let mut map = std::collections::HashMap::new();
    for (field, value) in data.iter() {
        map.insert(field.as_ref().to_owned(), value.as_ref().to_owned());
    }

    match timeout(
        Duration::from_millis(200),
        client.dictionary_set(cache_name, key, map.clone(), None, false),
    )
    .await
    {
        Ok(Ok(response)) => {
            match response.result {
                MomentoDictionarySetStatus::ERROR => {
                    // we got some error from
                    // the backend.
                    BACKEND_EX.increment();
                    HSET_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                }
                MomentoDictionarySetStatus::OK => {
                    HSET_STORED.increment();
                    for (field, value) in map.iter() {
                        klog_7(&"hset", &key, field, 0, value.len(), Status::Stored, 0);
                    }
                    response_buf.extend_from_slice(format!(":{}\r\n", data.len()).as_bytes());
                }
            }
        }
        Ok(Err(MomentoError::LimitExceeded(_))) => {
            BACKEND_EX.increment();
            BACKEND_EX_RATE_LIMITED.increment();
            HSET_EX.increment();
            response_buf.extend_from_slice(b"-ERR ratelimit exceed\r\n");
        }
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and incr stats and move on treating it
            // as an error
            error!("error for hset: {}", e);
            BACKEND_EX.increment();
            HSET_EX.increment();
            response_buf.extend_from_slice(b"-ERR backend error\r\n");
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as an error
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();
            HSET_EX.increment();
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
