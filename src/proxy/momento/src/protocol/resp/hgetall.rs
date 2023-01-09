// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::*;
use crate::{Error, *};
use ::net::*;
use protocol_resp::*;

pub async fn hgetall(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    key: &[u8],
) -> Result<(), Error> {
    HGETALL.increment();

    // check if the key is valid
    if std::str::from_utf8(key).is_err() {
        HGETALL_EX.increment();

        // invalid key
        let _ = socket.write_all(b"-ERR invalid key\r\n").await;
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    let mut response_buf = Vec::new();

    BACKEND_REQUEST.increment();

    // already checked the key so we know this unwraps is safe
    let key = std::str::from_utf8(key).unwrap().to_owned();

    match timeout(
        Duration::from_millis(200),
        client.dictionary_fetch(cache_name, &key),
    )
    .await
    {
        Ok(Ok(mut response)) => {
            match response.result {
                MomentoDictionaryFetchStatus::ERROR => {
                    // we got some error from
                    // the backend.
                    BACKEND_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                }
                MomentoDictionaryFetchStatus::FOUND => {
                    if response.dictionary.is_none() {
                        error!("error for hgetall: dictionary found but not provided in response");
                        BACKEND_EX.increment();
                        response_buf.extend_from_slice(b"-ERR backend error\r\n");
                    } else {
                        let dictionary = response.dictionary.as_mut().unwrap();

                        response_buf
                            .extend_from_slice(format!("*{}\r\n", dictionary.len() * 2).as_bytes());

                        for (field, value) in dictionary {
                            let field_header = format!("${}\r\n", field.len());
                            let value_header = format!("${}\r\n", value.len());

                            response_buf.extend_from_slice(field_header.as_bytes());
                            response_buf.extend_from_slice(field);
                            response_buf.extend_from_slice(b"\r\n");
                            response_buf.extend_from_slice(value_header.as_bytes());
                            response_buf.extend_from_slice(value);
                            response_buf.extend_from_slice(b"\r\n");
                        }

                        klog_1(&"hgetall", &key, Status::Hit, response_buf.len());
                    }
                }
                MomentoDictionaryFetchStatus::MISSING => {
                    response_buf.extend_from_slice(b"*0\r\n");
                    klog_1(&"hgetall", &key, Status::Miss, response_buf.len());
                }
            }
        }
        Ok(Err(MomentoError::LimitExceeded(_))) => {
            BACKEND_EX.increment();
            BACKEND_EX_RATE_LIMITED.increment();
            response_buf.extend_from_slice(b"-ERR ratelimit exceed\r\n");
        }
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and incr stats and move on treating it
            // as a miss
            error!("error for hgetall: {}", e);
            BACKEND_EX.increment();
            response_buf.extend_from_slice(b"-ERR backend error\r\n");
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as a miss
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
