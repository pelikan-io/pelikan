//! Echo server using `ConnStream` with `futures-io` traits.
//!
//! This demonstrates the streaming API as an alternative to the callback-based
//! `with_data` / `with_bytes` API. The `AsyncBufRead` path is zero-copy on the
//! recv side, making it a good default for protocol implementations that prefer
//! trait-based I/O.
//!
//! Usage:
//!   cargo run --example echo_stream_server [BIND_ADDR]
//!   # default: 127.0.0.1:7878

use futures_util::io::AsyncWriteExt;
use ringline::{AsyncEventHandler, ConfigBuilder, ConnCtx, ConnStream, RinglineBuilder};

struct StreamEcho {
    worker_id: usize,
}

impl AsyncEventHandler for StreamEcho {
    fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
        let worker_id = self.worker_id;
        async move {
            eprintln!("[worker {worker_id}] accepted connection {}", conn.index());
            let mut stream = ConnStream::new(conn);

            // AsyncBufRead::fill_buf returns a zero-copy slice into the recv
            // accumulator. No memcpy on the read side.
            loop {
                let data = match futures_util::AsyncBufReadExt::fill_buf(&mut stream).await {
                    Ok([]) => break, // EOF
                    Ok(buf) => buf.to_vec(),
                    Err(e) => {
                        eprintln!("[worker {worker_id}] read error: {e}");
                        break;
                    }
                };
                let n = data.len();
                if let Err(e) = stream.write_all(&data).await {
                    eprintln!("[worker {worker_id}] write error: {e}");
                    break;
                }
                futures_util::AsyncBufReadExt::consume_unpin(&mut stream, n);
            }
            eprintln!("[worker {worker_id}] connection {} closed", conn.index());
        }
    }

    fn create_for_worker(worker_id: usize) -> Self {
        eprintln!("[worker {worker_id}] starting");
        StreamEcho { worker_id }
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

    eprintln!("starting stream echo server on {bind_addr}");

    let (_shutdown, handles) = RinglineBuilder::new(config)
        .bind(bind_addr.parse().expect("invalid bind address"))
        .launch::<StreamEcho>()
        .expect("failed to launch workers");

    for handle in handles {
        if let Err(e) = handle.join().expect("worker thread panicked") {
            eprintln!("worker exited with error: {e:?}");
        }
    }
}
