use crate::Config;
use crate::Parser;
use crate::Storage;

use ::config::BufConfig;
use protocol_common::Execute;
use protocol_memcache::{Compose, Parse};
use session::{Buf, BufMut, Buffer};

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use std::borrow::{Borrow, BorrowMut};
use std::io::ErrorKind;
use std::sync::Arc;

pub async fn run(config: Arc<Config>, storage: Storage, parser: Parser) {
    let listener = std::net::TcpListener::bind(config.listen()).unwrap();
    let listener = TcpListener::from_std(listener).unwrap();

    let storage = Arc::new(Mutex::new(storage));

    loop {
        if let Ok((mut socket, _)) = listener.accept().await {
            if socket.set_nodelay(true).is_err() {
                continue;
            }

            let buf_size = config.buf().size();
            let storage = storage.clone();

            tokio::spawn(async move {
                // initialize parser and the read and write bufs
                // let parser = protocol_memcache::RequestParser::new();
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

                    // execute the request
                    let response = {
                        let mut storage = storage.lock();
                        (*storage).execute(&request.into_inner())
                    };

                    // write the response into the buffer
                    response.compose(&mut write_buffer);

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
