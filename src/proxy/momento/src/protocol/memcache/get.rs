// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::{klog_1, Status};
use crate::{Error, *};
use momento::cache::GetResponse;
use pelikan_net::*;
use protocol_memcache::*;

pub async fn get(
    client: &mut CacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    keys: &[Box<[u8]>],
) -> Result<(), Error> {
    // check if any of the keys are invalid before
    // sending the requests to the backend
    for key in keys.iter() {
        if std::str::from_utf8(key).is_err() {
            GET_EX.increment();

            // invalid key
            let _ = socket.write_all(b"ERROR\r\n").await;
            return Err(Error::from(ErrorKind::InvalidInput));
        }
    }

    let mut response_buf = Vec::new();

    for key in keys {
        BACKEND_REQUEST.increment();

        // we don't have a strict guarantee this function was called with memcache
        // safe keys. This matters mostly for writing the response back to the client
        // in a protocol compliant way.
        let key = std::str::from_utf8(key);

        // invalid keys will be treated as a miss
        if key.is_err() {
            continue;
        }

        // unwrap is safe now, rebind for convenience
        let key = key.unwrap();

        match timeout(Duration::from_millis(200), client.get(cache_name, key)).await {
            Ok(Ok(response)) => {
                match response {
                    GetResponse::Hit { value } => {
                        GET_KEY_HIT.increment();

                        let value: Vec<u8> = value.into();

                        let length = value.len();

                        let item_header = format!("VALUE {key} 0 {length}\r\n");

                        klog_1(&"get", &key, Status::Hit, length);

                        response_buf.extend_from_slice(item_header.as_bytes());
                        response_buf.extend_from_slice(&value);
                        response_buf.extend_from_slice(b"\r\n");
                    }
                    GetResponse::Miss => {
                        GET_KEY_MISS.increment();

                        // we don't write anything for a miss

                        klog_1(&"get", &key, Status::Miss, 0);
                    }
                }
            }
            Ok(Err(e)) => {
                // we got some error from the momento client
                // log and incr stats and move on treating it
                // as a miss
                error!("backend error for get: {}", e);
                BACKEND_EX.increment();

                klog_1(&"get", &key, Status::ServerError, 0);
            }
            Err(_) => {
                // we had a timeout, incr stats and move on
                // treating it as a miss
                BACKEND_EX.increment();
                BACKEND_EX_TIMEOUT.increment();

                klog_1(&"get", &key, Status::Timeout, 0);
            }
        }
    }
    response_buf.extend_from_slice(b"END\r\n");

    SESSION_SEND.increment();
    SESSION_SEND_BYTE.add(response_buf.len() as _);
    TCP_SEND_BYTE.add(response_buf.len() as _);
    if let Err(e) = socket.write_all(&response_buf).await {
        SESSION_SEND_EX.increment();
        return Err(e);
    }
    Ok(())
}
