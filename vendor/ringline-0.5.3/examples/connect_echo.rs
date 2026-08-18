#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::pin::Pin;

use ringline::{AsyncEventHandler, ConfigBuilder, ConnCtx, ParseResult, RinglineBuilder, connect};

/// Demonstrates outbound `connect()`. On start, connects to a remote
/// echo server, sends "Hello from ringline!\n", prints the echoed response,
/// then shuts down.
struct ConnectHandler {
    worker_id: usize,
    target: std::net::SocketAddr,
}

impl AsyncEventHandler for ConnectHandler {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        // Not used in this example.
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let target = self.target;
        let worker_id = self.worker_id;
        Some(Box::pin(async move {
            eprintln!("[worker {worker_id}] connecting to {target}");
            let connect_future = match connect(target) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("[worker {worker_id}] connect failed: {e}");
                    ringline::request_shutdown().ok();
                    return;
                }
            };
            match connect_future.await {
                Ok(conn) => {
                    eprintln!("[worker {worker_id}] connected to {target}");
                    let msg: &[u8] = b"Hello from ringline!\n";
                    if let Err(e) = conn.send_nowait(msg) {
                        eprintln!("[worker {worker_id}] send error: {e}");
                        return;
                    }
                    let n = conn
                        .with_data(|data| {
                            let text = String::from_utf8_lossy(data);
                            eprintln!("[worker {worker_id}] received: {}", text.trim());
                            ParseResult::Consumed(data.len())
                        })
                        .await;
                    if n == 0 {
                        eprintln!("[worker {worker_id}] connection closed by peer");
                    }
                    ringline::request_shutdown().ok();
                }
                Err(e) => {
                    eprintln!("[worker {worker_id}] connect failed: {e}");
                    ringline::request_shutdown().ok();
                }
            }
        }))
    }

    fn create_for_worker(worker_id: usize) -> Self {
        let target: std::net::SocketAddr = std::env::var("TARGET")
            .unwrap_or_else(|_| "127.0.0.1:7878".to_string())
            .parse()
            .expect("invalid TARGET address");

        eprintln!("[worker {worker_id}] will connect to {target}");
        ConnectHandler { worker_id, target }
    }
}

fn main() {
    // This example needs a running echo server (e.g., the echo_async_server example).
    // Start it first:  cargo run --example echo_async_server
    // Then run:         cargo run --example connect_echo
    // Or specify:       TARGET=10.0.0.1:8080 cargo run --example connect_echo

    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .max_connections(64)
        .build()
        .expect("valid config");

    eprintln!("starting connect_echo example (client-only mode)");

    // Client-only mode: no bind address, no acceptor thread.
    let (_shutdown, handles) = RinglineBuilder::new(config)
        .launch::<ConnectHandler>()
        .expect("failed to launch workers");

    for handle in handles {
        if let Err(e) = handle.join().expect("worker thread panicked") {
            eprintln!("worker exited with error: {e}");
        }
    }
}
