// Copyright 2022 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::*;
use crate::protocol::resp::ArcByteSlice;
use crate::{Error, *};
use ::net::*;
use protocol_resp::*;

pub async fn hmget(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    key: &[u8],
    fields: &[ArcByteSlice],
) -> Result<(), Error> {
    HMGET.increment();

    // check if the key is valid
    if std::str::from_utf8(key).is_err() {
        HMGET_EX.increment();

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
        client.dictionary_get(cache_name, &key, fields.clone()),
    )
    .await
    {
        Ok(Ok(mut response)) => {
            match response.result {
                MomentoDictionaryGetStatus::ERROR => {
                    // we got some error from
                    // the backend.
                    BACKEND_EX.increment();
                    HMGET_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                }
                MomentoDictionaryGetStatus::FOUND => {
                    if response.dictionary.is_none() {
                        error!("error for hmget: dictionary found but not provided in response");
                        BACKEND_EX.increment();
                        HMGET_EX.increment();
                        response_buf.extend_from_slice(b"-ERR backend error\r\n");
                    } else {
                        let dictionary = response.dictionary.as_mut().unwrap();

                        response_buf.extend_from_slice(format!("*{}\r\n", fields.len()).as_bytes());

                        let mut hit = 0;
                        let mut miss = 0;

                        for field in &fields {
                            if let Some(value) = dictionary.get(field.as_bytes()) {
                                hit += 1;
                                klog_2(&"hmget", &key, field, Status::Hit, value.len());

                                let item_header = format!("${}\r\n", value.len());

                                response_buf.extend_from_slice(item_header.as_bytes());
                                response_buf.extend_from_slice(value);
                                response_buf.extend_from_slice(b"\r\n");
                            } else {
                                miss += 1;
                                klog_2(&"hmget", &key, field, Status::Miss, 0);
                                response_buf.extend_from_slice(b"$-1\r\n");
                            }
                        }

                        HMGET_FIELD.add(fields.len() as u64);
                        HMGET_FIELD_HIT.add(hit);
                        HMGET_FIELD_MISS.add(miss);
                    }
                }
                MomentoDictionaryGetStatus::MISSING => {
                    // treat every requested field as a miss
                    response_buf.extend_from_slice(format!("*{}\r\n", fields.len()).as_bytes());

                    for field in &fields {
                        klog_2(&"hmget", &key, field, Status::Miss, 0);
                        response_buf.extend_from_slice(b"$-1\r\n");
                    }

                    HMGET_FIELD_MISS.add(fields.len() as u64);
                }
            }
        }
        Ok(Err(MomentoError::LimitExceeded(_))) => {
            BACKEND_EX.increment();
            BACKEND_EX_RATE_LIMITED.increment();
            HMGET_EX.increment();
            response_buf.extend_from_slice(b"-ERR ratelimit exceed\r\n");
        }
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and incr stats and move on treating it
            // as an error
            error!("error for hmget: {}", e);
            BACKEND_EX.increment();
            HMGET_EX.increment();
            response_buf.extend_from_slice(b"-ERR backend error\r\n");
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as an error
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();
            HMGET_EX.increment();
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
