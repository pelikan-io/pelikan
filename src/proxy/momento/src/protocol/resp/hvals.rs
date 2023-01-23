// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::*;
use crate::{Error, *};
use ::net::*;
use protocol_resp::*;

pub async fn hvals(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    key: &[u8],
) -> Result<(), Error> {
    HVALS.increment();

    // check if the key is valid
    if std::str::from_utf8(key).is_err() {
        HVALS_EX.increment();

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
                    HVALS_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                }
                MomentoDictionaryFetchStatus::FOUND => {
                    if response.dictionary.is_none() {
                        error!("error for hgetall: dictionary found but not provided in response");
                        BACKEND_EX.increment();
                        HVALS_EX.increment();
                        response_buf.extend_from_slice(b"-ERR backend error\r\n");
                    } else {
                        HVALS_HIT.increment();
                        let dictionary = response.dictionary.as_mut().unwrap();

                        response_buf
                            .extend_from_slice(format!("*{}\r\n", dictionary.len()).as_bytes());

                        for value in dictionary.values() {
                            let value_header = format!("${}\r\n", value.len());

                            response_buf.extend_from_slice(value_header.as_bytes());
                            response_buf.extend_from_slice(value);
                            response_buf.extend_from_slice(b"\r\n");
                        }

                        klog_1(&"hvals", &key, Status::Hit, response_buf.len());
                    }
                }
                MomentoDictionaryFetchStatus::MISSING => {
                    HVALS_MISS.increment();
                    // per command reference, return an empty list
                    response_buf.extend_from_slice(b"*0\r\n");
                    klog_1(&"hvals", &key, Status::Miss, response_buf.len());
                }
            }
        }
        Ok(Err(MomentoError::LimitExceeded(_))) => {
            BACKEND_EX.increment();
            BACKEND_EX_RATE_LIMITED.increment();
            HVALS_EX.increment();
            response_buf.extend_from_slice(b"-ERR ratelimit exceed\r\n");
        }
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and incr stats and move on treating it
            // as an error
            error!("error for hvals: {}", e);
            BACKEND_EX.increment();
            HVALS_EX.increment();
            response_buf.extend_from_slice(b"-ERR backend error\r\n");
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as an error
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();
            HVALS_EX.increment();
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
