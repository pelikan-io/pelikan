// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::*;
use crate::{Error, *};
use ::net::*;
use protocol_resp::*;

pub async fn hlen(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    key: &[u8],
) -> Result<(), Error> {
    HLEN.increment();

    let mut response_buf = Vec::new();

    BACKEND_REQUEST.increment();

    match timeout(
        Duration::from_millis(200),
        client.dictionary_fetch(cache_name, key),
    )
    .await
    {
        Ok(Ok(mut response)) => match response.result {
            MomentoDictionaryFetchStatus::ERROR => {
                BACKEND_EX.increment();
                HLEN_EX.increment();
                response_buf.extend_from_slice(b"-ERR backend error\r\n");
            }
            MomentoDictionaryFetchStatus::FOUND => {
                if response.dictionary.is_none() {
                    BACKEND_EX.increment();
                    HLEN_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                } else {
                    HLEN_HIT.increment();

                    let dictionary = response.dictionary.as_mut().unwrap();
                    let response = format!(":{}\r\n", dictionary.len()).into_bytes();

                    response_buf.extend_from_slice(&response);

                    klog_1(&"hlen", &key, Status::Hit, response_buf.len());
                }
            }
            MomentoDictionaryFetchStatus::MISSING => {
                HLEN_MISS.increment();
                response_buf.extend_from_slice(b":0\r\n");
                klog_1(&"hlen", &key, Status::Miss, response_buf.len());
            }
        },
        Ok(Err(MomentoError::LimitExceeded(_))) => {
            BACKEND_EX.increment();
            BACKEND_EX_RATE_LIMITED.increment();
            HLEN_EX.increment();
            response_buf.extend_from_slice(b"-ERR ratelimit exceed\r\n");
        }
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and return a generic error to the client
            error!("error for hlen: {}", e);
            BACKEND_EX.increment();
            HLEN_EX.increment();
            response_buf.extend_from_slice(b"-ERR backend error");
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as an error
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();
            HLEN_EX.increment();
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
