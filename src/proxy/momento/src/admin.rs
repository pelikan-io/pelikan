// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use session::Buf;

#[metric(name = "admin_conn_curr")]
pub static ADMIN_CONN_CURR: Gauge = Gauge::new();

#[metric(name = "admin_conn_accept")]
pub static ADMIN_CONN_ACCEPT: Counter = Counter::new();

#[metric(name = "admin_conn_close")]
pub static ADMIN_CONN_CLOSE: Counter = Counter::new();

pub(crate) async fn admin(admin_listener: TcpListener) {
    loop {
        // accept a new client
        if let Ok(Ok((socket, _))) =
            timeout(Duration::from_millis(1), admin_listener.accept()).await
        {
            ADMIN_CONN_CURR.increment();
            ADMIN_CONN_ACCEPT.increment();
            tokio::spawn(async move {
                admin::handle_admin_client(socket).await;
                ADMIN_CONN_CLOSE.increment();
                ADMIN_CONN_CURR.decrement();
            });
        };

        let mut rusage = libc::rusage {
            ru_utime: libc::timeval {
                tv_sec: 0,
                tv_usec: 0,
            },
            ru_stime: libc::timeval {
                tv_sec: 0,
                tv_usec: 0,
            },
            ru_maxrss: 0,
            ru_ixrss: 0,
            ru_idrss: 0,
            ru_isrss: 0,
            ru_minflt: 0,
            ru_majflt: 0,
            ru_nswap: 0,
            ru_inblock: 0,
            ru_oublock: 0,
            ru_msgsnd: 0,
            ru_msgrcv: 0,
            ru_nsignals: 0,
            ru_nvcsw: 0,
            ru_nivcsw: 0,
        };

        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) } == 0 {
            RU_UTIME.set(rusage.ru_utime.tv_sec as u64 * S + rusage.ru_utime.tv_usec as u64 * US);
            RU_STIME.set(rusage.ru_stime.tv_sec as u64 * S + rusage.ru_stime.tv_usec as u64 * US);
            RU_MAXRSS.set(rusage.ru_maxrss * KB as i64);
            RU_IXRSS.set(rusage.ru_ixrss * KB as i64);
            RU_IDRSS.set(rusage.ru_idrss * KB as i64);
            RU_ISRSS.set(rusage.ru_isrss * KB as i64);
            RU_MINFLT.set(rusage.ru_minflt as u64);
            RU_MAJFLT.set(rusage.ru_majflt as u64);
            RU_NSWAP.set(rusage.ru_nswap as u64);
            RU_INBLOCK.set(rusage.ru_inblock as u64);
            RU_OUBLOCK.set(rusage.ru_oublock as u64);
            RU_MSGSND.set(rusage.ru_msgsnd as u64);
            RU_MSGRCV.set(rusage.ru_msgrcv as u64);
            RU_NSIGNALS.set(rusage.ru_nsignals as u64);
            RU_NVCSW.set(rusage.ru_nvcsw as u64);
            RU_NIVCSW.set(rusage.ru_nivcsw as u64);
        }

        tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    }
}

async fn handle_admin_client(mut socket: tokio::net::TcpStream) {
    // initialize a buffer for incoming bytes from the client
    let mut buf = Buffer::new(INITIAL_BUFFER_SIZE);

    // initialize the request parser
    let parser = AdminProtocol::default();
    loop {
        if do_read(&mut socket, &mut buf).await.is_err() {
            break;
        }

        match parser.parse_request(buf.borrow()) {
            Ok(request) => {
                ADMIN_REQUEST_PARSE.increment();

                let consumed = request.consumed();
                let request = request.into_inner();

                match request {
                    AdminRequest::Stats => {
                        ADMIN_RESPONSE_COMPOSE.increment();

                        if stats_response(&mut socket).await.is_err() {
                            break;
                        }
                    }
                    _ => {
                        debug!("unsupported command: {:?}", request);
                    }
                }
                buf.advance(consumed);
            }
            Err(e) => match e.kind() {
                ErrorKind::WouldBlock => {}
                _ => {
                    // invalid request
                    let _ = socket.write_all(b"CLIENT_ERROR\r\n").await;
                    break;
                }
            },
        }
    }
}

async fn stats_response(socket: &mut tokio::net::TcpStream) -> Result<(), Error> {
    let message = protocol_admin::memcache_stats();
    socket.write_all(message.as_bytes()).await
}
