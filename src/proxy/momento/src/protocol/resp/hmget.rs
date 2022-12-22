// Copyright 2022 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;
use crate::klog::*;
use crate::{Error, *};
use ::net::*;
use protocol_resp::*;

pub async fn hmget(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    key: &[u8],
    fields: &[Arc<Box<[u8]>>],
) -> Result<(), Error> {
    HGET.increment();

    // check if the key is valid
    if std::str::from_utf8(key).is_err() {
        HGET_EX.increment();

        // invalid key
        let _ = socket.write_all(b"-ERR invalid key\r\n").await;
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    // check if the fields are valied
    // sending the request to the backend
    for field in fields.iter() {
        if std::str::from_utf8(field).is_err() {
            // GET_EX.increment();

            // invalid field
            let _ = socket.write_all(b"ERROR\r\n").await;
            return Err(Error::from(ErrorKind::InvalidInput));
        }
    }

    let mut response_buf = Vec::new();

    BACKEND_REQUEST.increment();

    // already checked the key and field, so we know these unwraps are safe
    let key = std::str::from_utf8(key).unwrap().to_owned();

    let mut fields: Vec<String> = fields.iter().map(|f| std::str::from_utf8(f).unwrap().to_owned()).collect();

    match timeout(Duration::from_millis(200), client.dictionary_get(cache_name, &key, fields.clone())).await {
        Ok(Ok(mut response)) => {
            match response.result {
                MomentoDictionaryGetStatus::ERROR => {
                    // we got some error from
                    // the backend.
                    BACKEND_EX.increment();

                    // TODO: what is the right
                    // way to handle this?
                    //
                    // currently ignoring and
                    // moving on to the next key
                }
                MomentoDictionaryGetStatus::FOUND => {
                    if response.dictionary.is_none() {
                        error!("error for hmget: dictionary found but not provided in response");
                        BACKEND_EX.increment();
                        response_buf.extend_from_slice(b"-ERR backend error\r\n");
                    }

                    let dictionary = response.dictionary.as_mut().unwrap();

                    response_buf.extend_from_slice(format!("*{}\r\n", fields.len()).as_bytes());

                    for field in fields {
                        println!("field: {}", field);
                        if let Some(value) = dictionary.get(field.as_bytes()) {
                            let item_header = format!("${}\r\n", value.len());
                            let response_len = 2 + item_header.len() + value.len();

                            klog_hget(&key, &field, response_len);

                            response_buf.extend_from_slice(item_header.as_bytes());
                            response_buf.extend_from_slice(&value);
                            response_buf.extend_from_slice(b"\r\n");
                        } else {
                            response_buf.extend_from_slice(b"$-1\r\n");
                        }
                    }
                }
                MomentoDictionaryGetStatus::MISSING => {
                    // treat every requested field as a miss
                    response_buf.extend_from_slice(format!("*{}\r\n", fields.len()).as_bytes());

                    for field in fields {
                        HGET_MISS.increment();
                        response_buf.extend_from_slice(b"$-1\r\n");
                        klog_hget(&key, &field, 0);
                    }
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
            error!("error for hget: {}", e);
            BACKEND_EX.increment();

            // treat every requested field as a miss
            response_buf.extend_from_slice(format!("*{}\r\n", fields.len()).as_bytes());

            while let Some(field) = fields.drain(..).next() {
                HGET_MISS.increment();
                response_buf.extend_from_slice(b"$-1\r\n");
                klog_hget(&key, &field, 0);
            }
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
