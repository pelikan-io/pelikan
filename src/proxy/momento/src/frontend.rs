// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::protocol::*;
use crate::*;
use pelikan_net::TCP_SEND_BYTE;
use session::Buf;

pub(crate) async fn handle_memcache_client(
    mut socket: tokio::net::TcpStream,
    mut client: CacheClient,
    cache_name: String,
) {
    // initialize a buffer for incoming bytes from the client
    let mut buf = Buffer::new(INITIAL_BUFFER_SIZE);

    // initialize the request parser
    let parser = memcache::RequestParser::new();

    // handle incoming data from the client
    loop {
        if do_read(&mut socket, &mut buf).await.is_err() {
            break;
        }

        let borrowed_buf = buf.borrow();

        match parser.parse(borrowed_buf) {
            Ok(request) => {
                let consumed = request.consumed();
                let request = request.into_inner();

                match request {
                    memcache::Request::Delete(r) => {
                        if memcache::delete(&mut client, &cache_name, &mut socket, &r)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    memcache::Request::Get(r) => {
                        if memcache::get(&mut client, &cache_name, &mut socket, r.keys())
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    memcache::Request::Set(r) => {
                        if memcache::set(&mut client, &cache_name, &mut socket, &r)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    _ => {
                        debug!("unsupported command: {}", request);
                    }
                }
                buf.advance(consumed);
            }
            Err(e) => match e.kind() {
                ErrorKind::WouldBlock => {}
                _ => {
                    // invalid request
                    trace!("malformed request: {:?}", borrowed_buf);
                    let _ = socket
                        .write_all(b"CLIENT_ERROR malformed request\r\n")
                        .await;
                    break;
                }
            },
        }
    }
}

pub(crate) async fn handle_resp_client(
    mut socket: tokio::net::TcpStream,
    mut client: CacheClient,
    cache_name: String,
) {
    // initialize a buffer for incoming bytes from the client
    let mut buf = Buffer::new(INITIAL_BUFFER_SIZE);

    // initialize the request parser
    let parser = resp::RequestParser::new();

    // handle incoming data from the client
    loop {
        if do_read(&mut socket, &mut buf).await.is_err() {
            break;
        }

        let borrowed_buf = buf.borrow();

        let request = match parser.parse(borrowed_buf) {
            Ok(request) => request,
            Err(e) => match e.kind() {
                ErrorKind::WouldBlock => continue,
                _ => {
                    trace!("malformed request: {:?}", borrowed_buf);
                    let _ = socket.write_all(b"-ERR malformed request\r\n").await;
                    break;
                }
            },
        };

        let consumed = request.consumed();
        let request = request.into_inner();
        let command = request.command();

        let mut response_buf = Vec::<u8>::new();

        let result: ProxyResult = async {
            match &request {
                resp::Request::Del(r) => {
                    resp::del(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::Get(r) => {
                    resp::get(&mut client, &cache_name, &mut response_buf, r.key()).await?
                }
                resp::Request::HashDelete(r) => {
                    resp::hdel(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::HashExists(r) => {
                    resp::hexists(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::HashGet(r) => {
                    resp::hget(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::HashGetAll(r) => {
                    resp::hgetall(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::HashIncrBy(r) => {
                    resp::hincrby(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::HashKeys(r) => {
                    resp::hkeys(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::HashLength(r) => {
                    resp::hlen(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::HashMultiGet(r) => {
                    resp::hmget(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::HashSet(r) => {
                    resp::hset(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::HashValues(r) => {
                    resp::hvals(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::ListIndex(r) => {
                    resp::lindex(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::ListLen(r) => {
                    resp::llen(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::ListPop(r) => {
                    resp::lpop(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::ListRange(r) => {
                    resp::lrange(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::ListPush(r) => {
                    resp::lpush(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::ListPushBack(r) => {
                    resp::rpush(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::ListPopBack(r) => {
                    resp::rpop(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::Set(r) => {
                    resp::set(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::SetAdd(r) => {
                    resp::sadd(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::SetRem(r) => {
                    resp::srem(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::SetDiff(r) => {
                    resp::sdiff(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::SetUnion(r) => {
                    resp::sunion(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::SetIntersect(r) => {
                    resp::sinter(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::SetMembers(r) => {
                    resp::smembers(&mut client, &cache_name, &mut response_buf, r).await?
                }
                resp::Request::SetIsMember(r) => {
                    resp::sismember(&mut client, &cache_name, &mut response_buf, r).await?
                }
                _ => return Err(ProxyError::UnsupportedCommand(request.command())),
            }

            Ok(())
        }
        .await;

        let fatal = match result {
            Ok(()) => false,
            Err(e) => {
                response_buf.clear();

                match e {
                    ProxyError::Momento(error) => {
                        SESSION_SEND.increment();
                        crate::protocol::resp::momento_error_to_resp_error(
                            &mut response_buf,
                            command,
                            error,
                        );

                        false
                    }
                    ProxyError::Timeout(_) => {
                        SESSION_SEND.increment();
                        BACKEND_EX.increment();
                        BACKEND_EX_TIMEOUT.increment();
                        response_buf.extend_from_slice(b"-ERR backend timeout\r\n");

                        false
                    }
                    ProxyError::Io(_) => true,
                    ProxyError::UnsupportedCommand(command) => {
                        debug!("unsupported resp command: {command}");
                        response_buf.extend_from_slice(
                            format!("-ERR unsupported command: {command}\r\n").as_bytes(),
                        );
                        true
                    }
                    ProxyError::Custom(message) => {
                        SESSION_SEND.increment();
                        BACKEND_EX.increment();
                        response_buf.extend_from_slice(b"-ERR ");
                        response_buf.extend_from_slice(message.as_bytes());
                        response_buf.extend_from_slice(b"\r\n");

                        true
                    }
                }
            }
        };

        // Temporary workaround
        // ====================
        // There are a few metrics that are incremented on every request. Before the
        // refactor, these were incremented within each call. Now, they should be
        // handled in this function. As an intermediate, we increment only if the request
        // method put data into response_buf.
        if !response_buf.is_empty() {
            BACKEND_REQUEST.increment();
            SESSION_SEND.increment();
        }

        SESSION_SEND_BYTE.add(response_buf.len() as _);
        TCP_SEND_BYTE.add(response_buf.len() as _);

        if socket.write_all(&response_buf).await.is_err() {
            SESSION_SEND_EX.increment();
            break;
        }

        if fatal {
            break;
        }

        buf.advance(consumed);
    }
}
