// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::*;
use crate::{Error, *};
use ::net::*;
use protocol_resp::*;

pub async fn hget(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    key: &[u8],
    field: &[u8],
) -> Result<(), Error> {
    HGET.increment();

    // check if the key is valid
    if std::str::from_utf8(key).is_err() {
        HGET_EX.increment();

        // invalid key
        let _ = socket.write_all(b"-ERR invalid key\r\n").await;
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    // check if the field is valid
    if std::str::from_utf8(field).is_err() {
        HGET_EX.increment();

        // invalid field
        let _ = socket.write_all(b"-ERR invalid field\r\n").await;
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    let mut response_buf = Vec::new();

    BACKEND_REQUEST.increment();

    // already checked the key and field, so we know these unwraps are safe
    let key = std::str::from_utf8(key).unwrap().to_owned();
    let field = std::str::from_utf8(field).unwrap().to_owned();

    match timeout(
        Duration::from_millis(200),
        client.dictionary_get(cache_name, &key, vec![field.clone()]),
    )
    .await
    {
        Ok(Ok(response)) => {
            match response.result {
                MomentoDictionaryGetStatus::ERROR => {
                    // we got some error from
                    // the backend.
                    BACKEND_EX.increment();
                    HGET_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                }
                MomentoDictionaryGetStatus::FOUND => {
                    if response.dictionary.is_none() {
                        error!("error for hget: dictionary found but not set in response");
                        BACKEND_EX.increment();
                        HGET_EX.increment();
                        response_buf.extend_from_slice(b"-ERR backend error\r\n");
                    } else if let Some(value) = response
                        .dictionary
                        .unwrap()
                        .get(&field.clone().into_bytes())
                    {
                        HGET_HIT.increment();

                        let item_header = format!("${}\r\n", value.len());

                        response_buf.extend_from_slice(item_header.as_bytes());
                        response_buf.extend_from_slice(value);
                        response_buf.extend_from_slice(b"\r\n");

                        klog_2(&"hget", &key, &field, Status::Hit, value.len());
                    } else {
                        HGET_MISS.increment();
                        response_buf.extend_from_slice(b"$-1\r\n");
                        klog_2(&"hget", &key, &field, Status::Miss, 0);
                    }
                }
                MomentoDictionaryGetStatus::MISSING => {
                    HGET_MISS.increment();
                    response_buf.extend_from_slice(b"$-1\r\n");
                    klog_2(&"hget", &key, &field, Status::Miss, 0);
                }
            }
        }
        Ok(Err(MomentoError::LimitExceeded(_))) => {
            BACKEND_EX.increment();
            BACKEND_EX_RATE_LIMITED.increment();
            HGET_EX.increment();
            response_buf.extend_from_slice(b"-ERR ratelimit exceed\r\n");
        }
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and incr stats and move on treating it
            // as an error
            error!("error for hget: {}", e);
            BACKEND_EX.increment();
            HGET_EX.increment();
            response_buf.extend_from_slice(b"-ERR backend error\r\n");
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as an error
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();
            HGET_EX.increment();
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
