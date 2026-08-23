use std::io;

use thiserror::Error;

/// Errors returned by the ringline driver.
///
/// # Recovery Guidance
///
/// | Error | Cause | Recovery |
/// |-------|-------|----------|
/// | `Io` | System call failure | Check `io::ErrorKind`; transient network errors may be retryable |
/// | `RingSetup` | Unsupported kernel feature | Upgrade kernel or use mio backend (`--no-default-features`) |
/// | `BufferRegistration` | `mmap()` or io_uring registration failed | Check system memory limits (`ulimit -v`) |
/// | `ConnectionLimitReached` | All connection slots in use | Increase via `ConfigBuilder::max_connections(...)` or close idle connections |
/// | `InvalidConnection` | Stale token, connection closed | Re-establish connection; do not reuse the `ConnCtx` |
/// | `SendPoolExhausted` | All send buffer slots in use | Await pending sends to complete before sending more |
/// | `InvalidRegion` | Region ID not registered | Check `MemoryRegion` registration; ensure region outlives usage |
/// | `PointerOutOfRegion` | SendGuard pointer outside registered region | Verify pointer arithmetic; region boundaries are strict |
/// | `ResourceLimit` | `RLIMIT_NOFILE` too low | Increase with `ulimit -n` (recommended: 65536+) |
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// I/O error from a system call.
    ///
    /// Check the underlying [`io::ErrorKind`] for transient vs permanent failures.
    /// Network-related errors (e.g., `ConnectionReset`, `BrokenPipe`) typically
    /// indicate the peer closed the connection.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// io_uring ring setup failed.
    ///
    /// Common causes:
    /// - Kernel too old (< 5.8 for multishot recv, < 6.0 for provided buffers)
    /// - Missing capabilities (requires `CAP_SYS_NICE` for some operations)
    /// - Unsupported io_uring features on this kernel
    ///
    /// Use `--no-default-features` to build with the mio backend as a fallback.
    #[error("ring setup: {0}")]
    RingSetup(String),

    /// Buffer registration with io_uring failed.
    ///
    /// This typically indicates a system resource limit (memory, VMAs) or
    /// an invalid registration request. Check `ulimit -v` for virtual memory limits.
    #[error("buffer registration: {0}")]
    BufferRegistration(String),

    /// Connection limit reached.
    ///
    /// The worker has no free slots for new connections. Either:
    /// - Increase via `ConfigBuilder::max_connections(...)` (default: 16000)
    /// - Close idle connections to free slots
    /// - Add more worker threads to distribute load
    #[error("connection limit reached")]
    ConnectionLimitReached,

    /// Invalid or stale connection token.
    ///
    /// This occurs when:
    /// - The connection was closed and the slot was reused
    /// - The `ConnCtx` was used after the peer disconnected
    /// - A `ConnToken` was incorrectly cached and reused
    ///
    /// Do not retry with the same token; establish a new connection.
    #[error("invalid connection")]
    InvalidConnection,

    /// Send pool exhausted.
    ///
    /// All send buffer slots are in flight. This is a backpressure signal:
    /// - Await pending `send()` futures before sending more
    /// - Use `send_nowait()` for fire-and-forget with explicit error handling
    /// - Increase via `ConfigBuilder::send_pool(count, slot_size)` (default count: 1024)
    #[error("send pool exhausted")]
    SendPoolExhausted,

    /// Invalid memory region ID.
    ///
    /// The `RegionId` passed to `SendGuard` does not correspond to a
    /// registered `MemoryRegion`. Ensure:
    /// - The region was registered via [`ConfigBuilder::registered_regions`](crate::ConfigBuilder::registered_regions)
    ///   or [`ShutdownHandle::register_region`](crate::ShutdownHandle::register_region)
    /// - The region is still valid (not dropped)
    #[error("invalid memory region ID")]
    InvalidRegion,

    /// Pointer not within the registered memory region.
    ///
    /// `SendGuard` requires the pointer to be strictly within the bounds
    /// of the registered region. This check prevents:
    /// - Use-after-free (pointer to freed memory)
    /// - Buffer overflows (pointer past region end)
    ///
    /// Debug by printing the pointer and region bounds when registering.
    #[error("pointer not within registered region")]
    PointerOutOfRegion,

    /// System resource limit is too low.
    ///
    /// Ringline requires sufficient file descriptors for connections.
    /// The default `RLIMIT_NOFILE` (often 1024) is insufficient for
    /// high-concurrency workloads.
    ///
    /// Set before running: `ulimit -n 65536` or higher.
    #[error("{0}")]
    ResourceLimit(String),
}

/// Errors returned by UDP send operations.
///
/// UDP sends can fail due to resource exhaustion even though UDP is
/// connectionless. The ringline runtime maintains per-worker send pools
/// to bound memory usage.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UdpSendError {
    /// UDP send pool exhausted.
    ///
    /// No free send slot or copy-pool slot available. This is transient:
    /// await pending UDP receives/sends to complete, then retry.
    #[error("UDP send pool exhausted")]
    PoolExhausted,

    /// UDP submission queue full.
    ///
    /// The io_uring submission queue is full. This is rare and indicates
    /// the application is submitting faster than the kernel can process.
    /// Await pending operations before submitting more.
    #[error("UDP submission queue full")]
    SubmissionQueueFull,

    /// UDP I/O error.
    #[error("UDP I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Error returned by [`try_sleep`](crate::try_sleep) and
/// [`try_timeout`](crate::try_timeout) when the timer slot pool is full.
///
/// The timer pool is pre-allocated to avoid allocations during async
/// execution. When exhausted, use the infallible variants [`sleep()`]
/// and [`timeout()`] which will panic instead (preferred in most cases).
///
/// [`sleep()`]: crate::sleep
/// [`timeout()`]: crate::timeout
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("timer slot pool exhausted")]
pub struct TimerExhausted;
