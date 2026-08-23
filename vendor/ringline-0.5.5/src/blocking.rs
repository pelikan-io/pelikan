//! User-facing blocking thread pool.
//!
//! Offloads blocking or CPU-bound work from io_uring worker threads.
//! Threads run at `SCHED_IDLE` priority so the kernel prefers async workers.

use std::any::Any;
use std::thread;

use crossbeam_channel::{Receiver, Sender};

/// A request from a worker to the blocking pool.
pub(crate) struct BlockingRequest {
    pub(crate) work: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    pub(crate) request_id: u64,
    /// Per-worker response channel.
    pub(crate) response_tx: Sender<BlockingResponse>,
    /// Wake handle — used to wake the worker after sending the response.
    pub(crate) wake_handle: crate::wakeup::WakeFd,
}

/// A response from the blocking pool to a worker.
pub(crate) struct BlockingResponse {
    pub(crate) request_id: u64,
    pub(crate) result: Box<dyn Any + Send>,
}

/// A pool of threads that perform blocking work.
///
/// Created once in [`launch_inner`](crate::worker) and shared (via `Arc`)
/// across all workers. Shutdown is driven by dropping the request sender,
/// which causes all pool threads to exit.
pub(crate) struct BlockingPool {
    pub(crate) request_tx: Sender<BlockingRequest>,
    _threads: Vec<thread::JoinHandle<()>>,
}

impl BlockingPool {
    /// Create the channel pair and spawn blocking threads.
    pub(crate) fn start(num_threads: usize) -> Self {
        let (request_tx, request_rx) = crossbeam_channel::unbounded::<BlockingRequest>();
        let mut threads = Vec::with_capacity(num_threads);

        for i in 0..num_threads {
            let rx = request_rx.clone();
            let handle = thread::Builder::new()
                .name(format!("ringline-blocking-{i}"))
                .spawn(move || {
                    // Set SCHED_IDLE priority — lowest possible (Linux only).
                    #[cfg(target_os = "linux")]
                    {
                        let param: libc::sched_param = unsafe { std::mem::zeroed() };
                        unsafe {
                            libc::sched_setscheduler(0, libc::SCHED_IDLE, &param);
                        }
                    }
                    blocking_thread(rx);
                })
                .expect("failed to spawn blocking thread");
            threads.push(handle);
        }

        BlockingPool {
            request_tx,
            _threads: threads,
        }
    }
}

/// Main loop for a blocking thread.
fn blocking_thread(rx: Receiver<BlockingRequest>) {
    while let Ok(req) = rx.recv() {
        let result = (req.work)();
        let _ = req.response_tx.send(BlockingResponse {
            request_id: req.request_id,
            result,
        });
        // Wake the requesting worker so it drains the response channel.
        req.wake_handle.wake();
    }
    // Channel closed — pool is shutting down.
}
