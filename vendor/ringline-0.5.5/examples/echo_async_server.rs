//! Async echo server using AsyncEventHandler.
//!
//! Usage:
//!   cargo run --example echo_async_server [BIND_ADDR]
//!   # default: 127.0.0.1:7878

use ringline::{AsyncEventHandler, ConfigBuilder, ConnCtx, ParseResult, RinglineBuilder};

struct AsyncEcho {
    worker_id: usize,
}

impl AsyncEventHandler for AsyncEcho {
    fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
        let worker_id = self.worker_id;
        async move {
            eprintln!("[worker {worker_id}] accepted connection {}", conn.index());
            loop {
                let consumed = conn
                    .with_data(|data| {
                        if let Err(e) = conn.send_nowait(data) {
                            eprintln!("[worker {worker_id}] send error: {e}");
                        }
                        ParseResult::Consumed(data.len())
                    })
                    .await;
                if consumed == 0 {
                    break;
                }
            }
            eprintln!("[worker {worker_id}] connection {} closed", conn.index());
        }
    }

    fn create_for_worker(worker_id: usize) -> Self {
        eprintln!("[worker {worker_id}] starting");
        AsyncEcho { worker_id }
    }
}

fn main() {
    let bind_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7878".to_string());

    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(128)
        .recv_buffer(128, 4096)
        .max_connections(1024)
        .build()
        .expect("valid config");

    eprintln!("starting async echo server on {bind_addr}");

    let (_shutdown, handles) = RinglineBuilder::new(config)
        .bind(bind_addr.parse().expect("invalid bind address"))
        .launch::<AsyncEcho>()
        .expect("failed to launch workers");

    for handle in handles {
        if let Err(e) = handle.join().expect("worker thread panicked") {
            eprintln!("worker exited with error: {e:?}");
        }
    }
}
