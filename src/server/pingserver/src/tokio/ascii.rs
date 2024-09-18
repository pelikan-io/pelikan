use crate::Config;
use ::config::BufConfig;
use protocol_ping::{Compose, Parse, Request, Response};
use session::{Buf, BufMut, Buffer};
use std::borrow::{Borrow, BorrowMut};
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub async fn run(config: Arc<Config>) {
    let listener = std::net::TcpListener::bind(config.listen()).unwrap();
    let listener = TcpListener::from_std(listener).unwrap();

    loop {
        if let Ok((mut socket, _)) = listener.accept().await {
            if socket.set_nodelay(true).is_err() {
                continue;
            }

            let buf_size = config.buf().size();

            tokio::spawn(async move {
                // initialize parser and the read and write bufs
                let parser = protocol_ping::RequestParser::new();
                let mut read_buffer = Buffer::new(buf_size);
                let mut write_buffer = Buffer::new(buf_size);

                loop {
                    // read from the socket
                    match socket.read(read_buffer.borrow_mut()).await {
                        Ok(0) => {
                            // socket was closed, return to close
                            return;
                        }
                        Ok(n) => {
                            // bytes received, advance read buffer
                            // to make them available for parsing
                            unsafe {
                                read_buffer.advance_mut(n);
                            }
                        }
                        Err(_) => {
                            // some other error occurred, return to
                            // close
                            return;
                        }
                    };

                    // parse the read buffer
                    let request = match parser.parse(read_buffer.borrow()) {
                        Ok(request) => {
                            // got a complete request, consume the
                            // bytes for the request by advancing
                            // the read buffer
                            let consumed = request.consumed();
                            read_buffer.advance(consumed);

                            request
                        }
                        Err(e) => match e.kind() {
                            ErrorKind::WouldBlock => {
                                // incomplete request, loop to read
                                // again
                                continue;
                            }
                            _ => {
                                // some parse error, return to close
                                return;
                            }
                        },
                    };

                    // compose a response into the write buffer
                    match request.into_inner() {
                        Request::Ping => {
                            Response::Pong.compose(&mut write_buffer);
                        }
                    }

                    // flush the write buffer, return to close on
                    // error
                    if socket.write_all(write_buffer.borrow()).await.is_err() {
                        return;
                    }

                    // clear the write buffer
                    write_buffer.clear();
                }
            });
        }
    }
}
