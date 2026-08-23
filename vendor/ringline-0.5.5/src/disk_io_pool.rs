//! Dedicated disk I/O thread pool for the mio backend.
//!
//! Offloads blocking filesystem syscalls (pread, pwrite, fsync, stat, rename,
//! unlink, mkdir) from the mio event loop to a pool of background threads.
//! Each thread executes one blocking call at a time and sends the result back
//! via a per-worker crossbeam channel + WakeHandle, matching the pattern used
//! by [`BlockingPool`](crate::blocking::BlockingPool).
//!
//! Unlike the blocking pool, these threads do NOT use `SCHED_IDLE` — disk I/O
//! is latency-sensitive and should not be deprioritized.

use std::thread;

use crossbeam_channel::{Receiver, Sender};

/// A request from a worker to the disk I/O pool.
pub(crate) struct DiskIoRequest {
    /// The blocking syscall to execute. Returns `(i32, Option<Metadata>)`.
    /// The i32 follows io_uring CQE convention: >= 0 for success/bytes,
    /// < 0 for -errno.
    pub(crate) work: Box<dyn FnOnce() -> DiskIoResult + Send>,
    /// Sequence number for correlating request → response.
    pub(crate) seq: u32,
    /// Per-worker response channel.
    pub(crate) response_tx: Sender<DiskIoResponse>,
    /// Wake handle — used to wake the worker after sending the response.
    pub(crate) wake_handle: crate::wakeup::WakeFd,
}

/// Result from a disk I/O worker thread.
pub(crate) struct DiskIoResult {
    /// io_uring-compatible result: >= 0 for success, < 0 for -errno.
    pub(crate) result: i32,
    /// Optional metadata (populated only for stat operations).
    pub(crate) metadata: Option<crate::fs::Metadata>,
}

/// A response from the disk I/O pool to a worker.
pub(crate) struct DiskIoResponse {
    /// Sequence number matching the original request.
    pub(crate) seq: u32,
    /// io_uring-compatible result.
    pub(crate) result: i32,
    /// Optional metadata (populated only for stat operations).
    pub(crate) metadata: Option<crate::fs::Metadata>,
}

/// A pool of threads that perform blocking disk I/O.
///
/// Created once during launch and shared (via `Arc`) across all workers.
/// Shutdown is driven by dropping the request sender, which causes all pool
/// threads to exit.
pub(crate) struct DiskIoPool {
    pub(crate) request_tx: Sender<DiskIoRequest>,
    _threads: Vec<thread::JoinHandle<()>>,
}

impl DiskIoPool {
    /// Create the channel pair and spawn disk I/O threads.
    pub(crate) fn start(num_threads: usize) -> Self {
        let (request_tx, request_rx) = crossbeam_channel::unbounded::<DiskIoRequest>();
        let mut threads = Vec::with_capacity(num_threads);

        for i in 0..num_threads {
            let rx = request_rx.clone();
            let handle = thread::Builder::new()
                .name(format!("ringline-disk-io-{i}"))
                .spawn(move || {
                    disk_io_thread(rx);
                })
                .expect("failed to spawn disk I/O thread");
            threads.push(handle);
        }

        DiskIoPool {
            request_tx,
            _threads: threads,
        }
    }
}

/// Main loop for a disk I/O thread.
fn disk_io_thread(rx: Receiver<DiskIoRequest>) {
    while let Ok(req) = rx.recv() {
        let result = (req.work)();
        let _ = req.response_tx.send(DiskIoResponse {
            seq: req.seq,
            result: result.result,
            metadata: result.metadata,
        });
        // Wake the requesting worker so it drains the response channel.
        req.wake_handle.wake();
    }
    // Channel closed — pool is shutting down.
}
