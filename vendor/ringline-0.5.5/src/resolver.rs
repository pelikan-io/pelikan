//! Dedicated DNS resolver pool.
//!
//! Runs `getaddrinfo` on a small pool of background threads, keeping
//! blocking DNS resolution isolated from the io_uring event loop.
//!
//! Workers submit requests via [`resolve()`](crate::resolve) and receive
//! results through a per-worker crossbeam channel + wakeup.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::thread;

use crossbeam_channel::{Receiver, Sender};

/// A request from a worker to the resolver pool.
pub(crate) struct ResolveRequest {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) request_id: u64,
    /// Per-worker response channel.
    pub(crate) response_tx: Sender<ResolveResponse>,
    /// Wake handle — used to wake the worker after sending the response.
    pub(crate) wake_handle: crate::wakeup::WakeFd,
}

/// A response from the resolver pool to a worker.
pub(crate) struct ResolveResponse {
    pub(crate) request_id: u64,
    pub(crate) result: io::Result<SocketAddr>,
}

/// A pool of threads that perform blocking DNS resolution.
///
/// Created once in [`launch_inner`](crate::worker) and shared (via `Arc`)
/// across all workers. Shutdown is driven by dropping the request sender,
/// which causes all pool threads to exit.
pub(crate) struct ResolverPool {
    pub(crate) request_tx: Sender<ResolveRequest>,
    _threads: Vec<thread::JoinHandle<()>>,
}

impl ResolverPool {
    /// Create the channel pair and spawn resolver threads.
    pub(crate) fn start(num_threads: usize) -> Self {
        let (request_tx, request_rx) = crossbeam_channel::unbounded::<ResolveRequest>();
        let mut threads = Vec::with_capacity(num_threads);

        for i in 0..num_threads {
            let rx = request_rx.clone();
            let handle = thread::Builder::new()
                .name(format!("ringline-resolver-{i}"))
                .spawn(move || resolver_thread(rx))
                .expect("failed to spawn resolver thread");
            threads.push(handle);
        }

        ResolverPool {
            request_tx,
            _threads: threads,
        }
    }
}

/// Main loop for a resolver thread.
fn resolver_thread(rx: Receiver<ResolveRequest>) {
    while let Ok(req) = rx.recv() {
        let result = resolve_blocking(&req.host, req.port);
        let _ = req.response_tx.send(ResolveResponse {
            request_id: req.request_id,
            result,
        });
        // Wake the requesting worker so it drains the response channel.
        req.wake_handle.wake();
    }
    // Channel closed — pool is shutting down.
}

/// Perform blocking DNS resolution via `getaddrinfo`.
fn resolve_blocking(host: &str, port: u16) -> io::Result<SocketAddr> {
    (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no addresses found"))
}
