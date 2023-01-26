// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::klog_set;
use crate::{Error, *};
use ::net::*;
use protocol_memcache::*;
use protocol_resp::Set;

pub async fn set(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    request: &Set,
) -> Result<(), Error> {
    SET.increment();

    let key = request.key();
    let value = request.value();

    if value.is_empty() {
        error!("empty values are not supported by momento");
        SESSION_SEND.increment();
        SESSION_SEND_BYTE.add(7);
        TCP_SEND_BYTE.add(7);

        if socket.write_all(b"ERROR\r\n").await.is_err() {
            SESSION_SEND_EX.increment();
        }
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    BACKEND_REQUEST.increment();

    let ttl = match request.expire_time() {
        Some(protocol_resp::ExpireTime::Seconds(v)) => NonZeroU64::new(v),
        Some(protocol_resp::ExpireTime::Milliseconds(v)) => {
            NonZeroU64::new(std::cmp::min(1, v / 1000))
        }
        Some(_) => {
            if socket.write_all(b"-ERR expire time\r\n").await.is_err() {
                SESSION_SEND_EX.increment();
            }
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        None => None,
    };

    match timeout(
        Duration::from_millis(200),
        client.set(cache_name, key, value, ttl),
    )
    .await
    {
        Ok(Ok(result)) => {
            match result.result {
                MomentoSetStatus::OK => {
                    SET_STORED.increment();
                    klog_set(
                        &key,
                        0,
                        ttl.map(|v| v.get()).unwrap_or(0) as i32,
                        value.len(),
                        5,
                        8,
                    );
                    SESSION_SEND.increment();
                    SESSION_SEND_BYTE.add(8);
                    TCP_SEND_BYTE.add(8);

                    if let Err(e) = socket.write_all(b"+OK\r\n").await {
                        SESSION_SEND_EX.increment();
                        // hangup if we can't send a response back
                        return Err(e);
                    }
                }
                MomentoSetStatus::ERROR => {
                    SET_NOT_STORED.increment();
                    klog_set(
                        &key,
                        0,
                        ttl.map(|v| v.get()).unwrap_or(0) as i32,
                        value.len(),
                        9,
                        12,
                    );
                    SESSION_SEND.increment();
                    SESSION_SEND_BYTE.add(12);
                    TCP_SEND_BYTE.add(12);

                    // let client know this wasn't stored
                    if let Err(e) = socket.write_all(b"-ERR backend error\r\n").await {
                        SESSION_SEND_EX.increment();
                        // hangup if we can't send a response back
                        return Err(e);
                    }
                }
            }
        }
        Ok(Err(MomentoError::LimitExceeded(_))) => {
            BACKEND_EX.increment();
            BACKEND_EX_RATE_LIMITED.increment();

            SET_EX.increment();
            SET_NOT_STORED.increment();
            SESSION_SEND.increment();
            SESSION_SEND_BYTE.add(12);
            TCP_SEND_BYTE.add(12);

            // let client know this wasn't stored
            if let Err(e) = socket.write_all(b"-ERR ratelimit exceeded\r\n").await {
                SESSION_SEND_EX.increment();
                // hangup if we can't send a response back
                return Err(e);
            }
        }
        Ok(Err(e)) => {
            error!("error for set: {}", e);

            BACKEND_EX.increment();
            SET_EX.increment();
            SET_NOT_STORED.increment();
            SESSION_SEND.increment();
            SESSION_SEND_BYTE.add(12);
            TCP_SEND_BYTE.add(12);

            // let client know this wasn't stored
            if let Err(e) = socket.write_all(b"-ERR backend error\r\n").await {
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
            SET_NOT_STORED.increment();
            SESSION_SEND.increment();
            SESSION_SEND_BYTE.add(12);
            TCP_SEND_BYTE.add(12);

            // let client know the request timed out
            if let Err(e) = socket.write_all(b"-ERR backend timeout\r\n").await {
                SESSION_SEND_EX.increment();
                // hangup if we can't send a response back
                return Err(e);
            }
        }
    }

    Ok(())
}
