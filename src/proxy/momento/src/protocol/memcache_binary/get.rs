// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::{klog_1, Status};
use crate::{Error, *};
use bytes::BytesMut;
use momento::cache::GetResponse;
use pelikan_net::*;
use protocol_memcache::*;

pub async fn get(
    client: &mut CacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    request: Get,
) -> Result<(), Error> {
    GET.increment();

    if request.keys().len() != 1 {
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    let key = request.keys().first().cloned().unwrap();
    let key_str = std::str::from_utf8(&key);
    if key_str.is_err() {
        GET_EX.increment();

        // invalid key
        return Err(Error::from(ErrorKind::InvalidInput));
    }
    let key_str = key_str.unwrap();

    let mut response_buf = BytesMut::new();

    // for key in keys {
    BACKEND_REQUEST.increment();

    let protocol = protocol_memcache::binary::BinaryProtocol::default();

    match timeout(Duration::from_millis(200), client.get(cache_name, key_str)).await {
        Ok(Ok(response)) => match response {
            GetResponse::Hit { value } => {
                GET_KEY_HIT.increment();

                let value: Vec<u8> = value.into();
                let length = value.len();

                let response = Response::found(&key, 0, None, &value);
                let _ =
                    protocol.compose_response(&Request::Get(request), &response, &mut response_buf);

                klog_1(&"get", &key, Status::Hit, length);
            }
            GetResponse::Miss => {
                GET_KEY_MISS.increment();

                let response = Response::not_found(false);
                let _ =
                    protocol.compose_response(&Request::Get(request), &response, &mut response_buf);

                klog_1(&"get", &key, Status::Miss, 0);
            }
        },
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and incr stats and move on treating it
            // as a miss
            error!("backend error for get: {}", e);
            BACKEND_EX.increment();

            let response = Response::not_found(false);
            let _ = protocol.compose_response(&Request::Get(request), &response, &mut response_buf);

            klog_1(&"get", &key, Status::ServerError, 0);
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as a miss
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();

            let response = Response::not_found(false);
            let _ = protocol.compose_response(&Request::Get(request), &response, &mut response_buf);

            klog_1(&"get", &key, Status::Timeout, 0);
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
