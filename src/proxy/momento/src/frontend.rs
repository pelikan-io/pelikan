// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::protocol::*;
use crate::*;
use pelikan_net::TCP_SEND_BYTE;
use protocol_memcache::binary::BinaryProtocol;
use protocol_memcache::text::TextProtocol;
use protocol_memcache::Protocol;
use session::Buf;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) async fn handle_memcache_client(
    mut socket: tokio::net::TcpStream,
    mut client: CacheClient,
    cache_name: String,
) {
    debug!("accepted memcache text client");

    // initialize a buffer for incoming bytes from the client
    let mut buf = Buffer::new(INITIAL_BUFFER_SIZE);

    // initialize the request parser
    let protocol = TextProtocol::default();

    // handle incoming data from the client
    loop {
        if do_read(&mut socket, &mut buf).await.is_err() {
            break;
        }

        let borrowed_buf = buf.borrow();

        match protocol.parse_request(borrowed_buf) {
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

pub(crate) async fn handle_memcache_binary_client(
    socket: tokio::net::TcpStream,
    client: CacheClient,
    cache_name: String,
) {
    debug!("accepted memcache binary client");

    // initialize a buffer for incoming bytes from the client
    let mut read_buffer = Buffer::new(INITIAL_BUFFER_SIZE);
    let mut write_buffer = Buffer::new(INITIAL_BUFFER_SIZE);

    // initialize the protocol
    let protocol = BinaryProtocol::default();

    // queue for response passing back from tasks
    let (sender, mut receiver) = mpsc::channel::<
        std::io::Result<(u64, protocol_memcache::Request, protocol_memcache::Response)>,
    >(1024);

    let (mut read_half, mut write_half) = socket.into_split();

    let sequence = Arc::new(AtomicU64::new(0));
    let sequence2 = sequence.clone();

    let read_alive = Arc::new(AtomicBool::new(true));
    let read_alive2 = read_alive.clone();

    let write_alive = Arc::new(AtomicBool::new(true));
    let write_alive2 = write_alive.clone();

    tokio::spawn(async move {
        let mut next_sequence: u64 = 0;
        let mut backlog = BTreeMap::new();

        let protocol = BinaryProtocol::default();

        while write_alive2.load(Ordering::Relaxed) {
            if !read_alive2.load(Ordering::Relaxed)
                && next_sequence == sequence2.load(Ordering::Relaxed)
            {
                write_alive2.store(false, Ordering::Relaxed);
                return;
            }

            debug!("writer loop");
            if let Some(result) = receiver.recv().await {
                match result {
                    Ok((sequence, request, response)) => {
                        if sequence == next_sequence {
                            debug!("sending next: {next_sequence}");
                            next_sequence += 1;
                            if protocol
                                .compose_response(&request, &response, &mut write_buffer)
                                .is_err()
                            {
                                read_alive2.store(false, Ordering::Relaxed);
                                write_alive2.store(false, Ordering::Relaxed);
                                return;
                            }

                            'backlog: while !backlog.is_empty() {
                                if let Some((request, response)) = backlog.remove(&next_sequence) {
                                    debug!("sending next: {next_sequence}");
                                    next_sequence += 1;
                                    if protocol
                                        .compose_response(&request, &response, &mut write_buffer)
                                        .is_err()
                                    {
                                        read_alive2.store(false, Ordering::Relaxed);
                                        write_alive2.store(false, Ordering::Relaxed);
                                        return;
                                    }
                                } else {
                                    break 'backlog;
                                }
                            }
                        } else {
                            debug!("queueing seq: {sequence}");
                            backlog.insert(sequence, (request, response));
                        }
                    }
                    Err(_e) => {
                        read_alive2.store(false, Ordering::Relaxed);
                        write_alive2.store(false, Ordering::Relaxed);
                        return;
                    }
                }
            }

            while write_buffer.remaining() > 0 {
                debug!("non-blocking write");
                if do_write2(&mut write_half, &mut write_buffer).await.is_err() {
                    read_alive2.store(false, Ordering::Relaxed);
                    write_alive2.store(false, Ordering::Relaxed);
                    return;
                }
            }
        }
    });

    // loop to handle the connection
    while read_alive.load(Ordering::Relaxed) {
        // read data from the tcp stream into the buffer
        if do_read2(&mut read_half, &mut read_buffer).await.is_err() {
            // any read errors result in hangup
            read_alive.store(false, Ordering::Relaxed);
        }

        // dispatch all complete requests in the socket buffer as async tasks
        //
        // NOTE: errors in the request handlers typically indicate write errors.
        //       To eliminate possibility for desync, we hangup if there is an
        //       error. The request handlers should implement graceful handling
        //       of backend errors.
        'requests: loop {
            let borrowed_buf = read_buffer.borrow();

            match protocol.parse_request(borrowed_buf) {
                Ok(request) => {
                    debug!("read request");

                    let consumed = request.consumed();
                    let request = request.into_inner();

                    read_buffer.advance(consumed);

                    let sender = sender.clone();
                    let client = client.clone();
                    let cache_name = cache_name.clone();

                    let sequence = sequence.fetch_add(1, Ordering::Relaxed);

                    tokio::spawn(async move {
                        handle_memcache_binary_request(
                            sender, client, cache_name, sequence, request,
                        )
                        .await;
                    });
                }
                Err(e) => match e.kind() {
                    ErrorKind::WouldBlock => {
                        // more data needs to be read from the stream, so stop
                        // processing requests
                        break 'requests;
                    }
                    _ => {
                        // invalid request
                        trace!("malformed request: {:?}", borrowed_buf);
                        read_alive.store(false, Ordering::Relaxed);
                        return;
                    }
                },
            }
        }
    }

    for _ in 0..60 {
        if write_alive.load(Ordering::Relaxed) {
            // time delay for write half to complete
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            break;
        }
    }

    // shutdown write half
    write_alive.store(false, Ordering::Relaxed);
}

async fn handle_memcache_binary_request(
    channel: mpsc::Sender<
        std::result::Result<
            (u64, protocol_memcache::Request, protocol_memcache::Response),
            std::io::Error,
        >,
    >,
    mut client: CacheClient,
    cache_name: String,
    sequence: u64,
    request: protocol_memcache::Request,
) {
    // info!("handling request");
    let result = match request {
        // memcache::Request::Delete(r) => {
        //     if memcache_binary::delete(&mut client, &cache_name, &mut socket, r)
        //         .await
        //         .is_err()
        //     {
        //         break 'connection;
        //     }
        // }
        memcache::Request::Get(ref r) => memcache_binary::get(&mut client, &cache_name, r).await,
        memcache::Request::Set(ref r) => memcache_binary::set(&mut client, &cache_name, r).await,
        _ => {
            debug!("unsupported command: {}", request);
            Err(Error::new(ErrorKind::Other, "unsupported"))
        }
    };

    match result {
        Ok(response) => {
            // info!("returning response");
            let _ = channel.send(Ok((sequence, request, response))).await;
        }
        Err(e) => {
            // info!("returning error");
            let _ = channel.send(Err(e)).await;
        }
    }
}

pub(crate) async fn handle_resp_client(
    mut socket: tokio::net::TcpStream,
    mut client: CacheClient,
    cache_name: String,
) {
    debug!("accepted resp client");

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
