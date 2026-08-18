use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
#[cfg(not(has_io_uring))]
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::acceptor::{AcceptorConfig, run_acceptor};
use crate::backend::AsyncEventLoop;
use crate::config::Config;
use crate::runtime::handler::AsyncEventHandler;

/// Result type for `launch` / `RinglineBuilder::launch` to avoid type-complexity warnings.
type LaunchResult = Result<
    (
        ShutdownHandle,
        Vec<thread::JoinHandle<Result<(), crate::error::Error>>>,
    ),
    crate::error::Error,
>;
type WorkerHandle = thread::JoinHandle<Result<(), crate::error::Error>>;

fn rollback_workers(
    shutdown_flag: &Arc<AtomicBool>,
    worker_wake_fds: &[crate::wakeup::WakeFd],
    handles: Vec<WorkerHandle>,
) -> Option<crate::error::Error> {
    shutdown_flag.store(true, Ordering::SeqCst);
    for wake in worker_wake_fds {
        wake.wake();
    }

    let mut first_error = None;
    for handle in handles {
        if let Ok(Err(error)) = handle.join()
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error
}

/// Carries the worker wake read descriptor into its worker thread.
///
/// Mio has a distinct pipe read end, which this type owns until the driver is
/// constructed. On io_uring, [`crate::wakeup::WakeHandle`] owns the shared
/// eventfd and this type only carries its descriptor number.
struct WorkerReadFd {
    #[cfg(has_io_uring)]
    fd: RawFd,
    #[cfg(not(has_io_uring))]
    fd: Option<OwnedFd>,
}

impl WorkerReadFd {
    fn new(fd: RawFd) -> Self {
        #[cfg(has_io_uring)]
        {
            Self { fd }
        }
        #[cfg(not(has_io_uring))]
        Self {
            // SAFETY: create_wake_fd returns a fresh pipe read descriptor and
            // transfers its ownership to this constructor on the mio backend.
            fd: Some(unsafe { OwnedFd::from_raw_fd(fd) }),
        }
    }

    fn as_raw_fd(&self) -> RawFd {
        #[cfg(has_io_uring)]
        {
            self.fd
        }
        #[cfg(not(has_io_uring))]
        {
            self.fd
                .as_ref()
                .expect("worker read fd must not be transferred twice")
                .as_raw_fd()
        }
    }

    fn transfer_to_driver(&mut self) {
        #[cfg(not(has_io_uring))]
        {
            let owned = self
                .fd
                .take()
                .expect("worker read fd must not be transferred twice");
            let _ = owned.into_raw_fd();
        }
    }
}

/// Handle returned by `launch()` to trigger graceful shutdown of all workers.
pub struct ShutdownHandle {
    shutdown_flag: Arc<AtomicBool>,
    worker_wake_handles: Vec<crate::wakeup::WakeHandle>,
    listen_fd: Option<RawFd>,
    listen_fd_closed: Option<Arc<AtomicBool>>,
    bound_addr: Option<SocketAddr>,
    /// Read on the io_uring backend by `register_region` /
    /// `unregister_region`; on the mio backend it sits unused but is kept
    /// so the field layout is identical across backends.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    region_registrar: Arc<crate::region_registry::RegionRegistrar>,
}

impl ShutdownHandle {
    /// The actual TCP address the listener bound to, if any. Returns `Some`
    /// for TCP `bind()` (port may have been zero-resolved) and `None` for
    /// client-only mode or Unix-socket binds.
    pub fn bound_addr(&self) -> Option<SocketAddr> {
        self.bound_addr
    }

    /// Number of worker threads launched.
    pub fn worker_count(&self) -> usize {
        self.worker_wake_handles.len()
    }

    /// Refcounted wake handle for the given worker, or `None` if `idx` is
    /// out of range.
    ///
    /// The returned handle can be cloned and moved to other threads, and
    /// stays valid past [`ShutdownHandle`] drop — the underlying fd is
    /// reference-counted and closes only when the last clone is dropped.
    /// After workers join, calling [`wake`](crate::WakeHandle::wake) is a
    /// no-op write into an fd nobody is reading.
    pub fn worker_wake_handle(&self, idx: usize) -> Option<crate::wakeup::WakeHandle> {
        self.worker_wake_handles.get(idx).cloned()
    }

    /// Register a memory region with every worker's io_uring fixed-buffer
    /// table. Blocks until every worker has acknowledged the kernel-side
    /// update.
    ///
    /// The returned [`RegionId`](crate::RegionId) is valid for use in
    /// `SendGuard` on any worker.
    ///
    /// # Errors
    ///
    /// - `io::ErrorKind::Other` "registered-region table is full" if the
    ///   table has no free slots (size set by
    ///   [`ConfigBuilder::max_registered_regions`](crate::ConfigBuilder::max_registered_regions)).
    /// - The first kernel error reported by any worker if the underlying
    ///   `register_buffers_update` call fails.
    /// - On the mio backend (no io_uring): always returns
    ///   `io::ErrorKind::Unsupported`.
    ///
    /// # Caller contract
    ///
    /// The memory must outlive the registration: do not unmap or free
    /// `region` until [`unregister_region`](Self::unregister_region) has
    /// returned, or until the runtime has fully shut down.
    pub fn register_region(
        &self,
        region: crate::buffer::fixed::MemoryRegion,
    ) -> io::Result<crate::buffer::fixed::RegionId> {
        #[cfg(has_io_uring)]
        {
            self.region_registrar.register(region)
        }
        #[cfg(not(has_io_uring))]
        {
            let _ = region;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "register_region requires the io_uring backend",
            ))
        }
    }

    /// Unregister a previously registered region. Blocks until every worker
    /// has acknowledged. The slot is returned to the free list on success.
    ///
    /// # Caller contract
    ///
    /// No SQE referencing the slot may be in flight when this is called.
    /// After it returns, the underlying memory may be safely unmapped.
    ///
    /// On the mio backend, always returns `io::ErrorKind::Unsupported`.
    pub fn unregister_region(&self, id: crate::buffer::fixed::RegionId) -> io::Result<()> {
        #[cfg(has_io_uring)]
        {
            self.region_registrar.unregister(id)
        }
        #[cfg(not(has_io_uring))]
        {
            let _ = id;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unregister_region requires the io_uring backend",
            ))
        }
    }

    /// Block the calling thread until `SIGINT` or `SIGTERM` is received,
    /// then trigger graceful shutdown.
    ///
    /// Equivalent to calling [`signal::wait()`](crate::signal::wait) followed
    /// by [`shutdown()`](Self::shutdown).
    ///
    /// Returns which signal was caught.
    pub fn wait_on_signal(&self) -> crate::signal::Signal {
        let sig = crate::signal::wait();
        self.shutdown();
        sig
    }

    /// Signal all workers to shut down gracefully.
    ///
    /// Workers will stop accepting new connections, close all active connections,
    /// drain remaining completions, and exit their event loops returning `Ok(())`.
    /// Also closes the listen fd to unblock the acceptor's `accept()`.
    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Release);
        if let (Some(fd), Some(closed)) = (self.listen_fd, &self.listen_fd_closed)
            && !closed.swap(true, Ordering::AcqRel)
        {
            unsafe {
                // shutdown(SHUT_RD) first: on Linux this wakes a thread
                // blocked in accept4 (with EINVAL) and releases the bound
                // port immediately. close(2) alone does neither — the
                // in-progress syscall holds a file reference, so the
                // acceptor stayed parked and the socket stayed listening
                // until one more peer connected (EADDRINUSE on prompt
                // relaunch). The close below then runs after the acceptor
                // can no longer loop into a reused fd number.
                libc::shutdown(fd, libc::SHUT_RD);
                libc::close(fd);
            }
        }
        // Wake all workers so they see the flag even if blocked on I/O.
        for wh in &self.worker_wake_handles {
            wh.wake();
        }
    }
}

// The wake-fd lifetime no longer needs an explicit `Drop`: each
// `WakeHandle` reference-counts the underlying fd via `Arc<WakeFdInner>`
// and closes it when the last clone is dropped. Users may keep clones
// from `worker_wake_handle()` past `ShutdownHandle` drop without
// leaking the fd — the runtime itself drops its clones when shutdown
// completes.
//
// However, dropping the handle without ever calling `shutdown()` used
// to leave workers running forever: the shutdown flag was never set,
// the listen fd was never closed, and no wake-up was delivered, so the
// RAII idiom `drop(shutdown); for h in handles { h.join() }` hung
// indefinitely (reproducer: `cargo bench -p ringline --bench buffer`,
// which iterates several sizes and depends on each previous server
// shutting down between iterations). We restore the RAII contract by
// having `Drop` call `shutdown()` — it's safe to call regardless of
// whether the caller has already invoked it.
impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        // `shutdown()` is idempotent:
        //   * `shutdown_flag.store(true)` is monotonic — a second store
        //     is a no-op.
        //   * The listen-fd close is gated by an `AtomicBool::swap`, so
        //     a double-close is impossible whether `Drop` runs before
        //     or after an explicit `shutdown()`.
        //   * `WakeHandle::wake` is documented as a no-op write into
        //     an fd nobody is reading once workers have joined; the
        //     write either delivers a real wake or returns harmlessly.
        // Calling it unconditionally here makes the RAII idiom work
        // while leaving the explicit `.shutdown()` path unchanged.
        self.shutdown();
    }
}

/// Internal enum for the bound listen address.
enum BindAddr {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

/// Resolve the actual bound address of a TCP listen fd via `getsockname(2)`.
/// Returns `None` if the fd is not an IPv4/IPv6 TCP socket or the syscall fails.
fn getsockname_v4_v6(fd: RawFd) -> Option<SocketAddr> {
    use std::mem::{MaybeUninit, size_of};

    let mut storage: MaybeUninit<libc::sockaddr_storage> = MaybeUninit::zeroed();
    let mut len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockname(
            fd,
            storage.as_mut_ptr() as *mut libc::sockaddr,
            &mut len as *mut _,
        )
    };
    if rc != 0 {
        return None;
    }
    let s = unsafe { storage.assume_init() };
    match s.ss_family as i32 {
        libc::AF_INET => {
            let addr_in: libc::sockaddr_in =
                unsafe { std::ptr::read(&s as *const _ as *const libc::sockaddr_in) };
            let ip = std::net::Ipv4Addr::from(u32::from_be(addr_in.sin_addr.s_addr));
            let port = u16::from_be(addr_in.sin_port);
            Some(SocketAddr::from((ip, port)))
        }
        libc::AF_INET6 => {
            let addr_in6: libc::sockaddr_in6 =
                unsafe { std::ptr::read(&s as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(addr_in6.sin6_addr.s6_addr);
            let port = u16::from_be(addr_in6.sin6_port);
            Some(SocketAddr::from((ip, port)))
        }
        _ => None,
    }
}

/// Builder for launching ringline workers with optional listener/acceptor.
///
/// Create a builder with [`RinglineBuilder::new(config)`](Self::new), optionally
/// call [`.bind(addr)`](Self::bind) to listen for inbound connections, then
/// call [`.launch::<Handler>()`](Self::launch) to start the worker threads.
///
/// If no bind address is set, ringline runs in client-only mode: no TCP
/// listener or acceptor thread is created, and workers can initiate outbound
/// connections via [`AsyncEventHandler::on_start`].
///
/// # Example: Echo Server
///
/// ```no_run
/// use ringline::{AsyncEventHandler, Config, ConnCtx, ParseResult, RinglineBuilder};
///
/// struct Echo;
///
/// impl AsyncEventHandler for Echo {
///     fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
///         async move {
///             loop {
///                 let n = conn.with_data(|data| {
///                     conn.send_nowait(data).ok();
///                     ParseResult::Consumed(data.len())
///                 }).await;
///                 if n == 0 { break; }
///             }
///         }
///     }
///     fn create_for_worker(_id: usize) -> Self { Echo }
/// }
///
/// fn main() -> Result<(), ringline::Error> {
///     let config = Config::default();
///     let (shutdown, handles) = RinglineBuilder::new(config)
///         .bind("0.0.0.0:7878".parse().unwrap())
///         .launch::<Echo>()?;
///
///     // Wait for shutdown signal
///     shutdown.wait_on_signal();
///
///     // Join all worker threads
///     for h in handles {
///         h.join().unwrap()?;
///     }
///     Ok(())
/// }
/// ```
///
/// # Example: Client-Only Mode
///
/// ```no_run
/// use ringline::{AsyncEventHandler, Config, RinglineBuilder};
/// use std::pin::Pin;
/// use std::future::Future;
///
/// struct ClientHandler;
///
/// impl AsyncEventHandler for ClientHandler {
///     fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
///         Some(Box::pin(async {
///             // Connect to Redis on startup
///             match ringline::connect("127.0.0.1:6379".parse().unwrap()) {
///                 Ok(future) => {
///                     if let Ok(conn) = future.await {
///                         println!("Connected to Redis");
///                     }
///                 }
///                 Err(_) => eprintln!("Failed to initiate connection"),
///             }
///         }))
///     }
///     fn on_accept(&self, _conn: ringline::ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
///         async {} // No inbound connections in client-only mode
///     }
///     fn create_for_worker(_id: usize) -> Self { ClientHandler }
/// }
///
/// fn main() -> Result<(), ringline::Error> {
///     let config = Config::default();
///     let (shutdown, handles) = RinglineBuilder::new(config)
///         // No .bind() call = client-only mode
///         .launch::<ClientHandler>()?;
///
///     shutdown.wait_on_signal();
///     for h in handles { h.join().unwrap()?; }
///     Ok(())
/// }
/// ```
///
/// # Example: Unix Domain Socket
///
/// ```no_run
/// use ringline::{AsyncEventHandler, Config, ConnCtx, ParseResult, RinglineBuilder};
///
/// struct Handler;
/// impl AsyncEventHandler for Handler {
///     fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
///         async move {
///             loop {
///                 let n = conn.with_data(|_data| ParseResult::Consumed(0)).await;
///                 if n == 0 { break; }
///             }
///         }
///     }
///     fn create_for_worker(_id: usize) -> Self { Handler }
/// }
///
/// fn main() -> Result<(), ringline::Error> {
///     let config = Config::default();
///     let (shutdown, handles) = RinglineBuilder::new(config)
///         .bind_unix("/tmp/app.sock")
///         .launch::<Handler>()?;
///
///     shutdown.wait_on_signal();
///     for h in handles { h.join().unwrap()?; }
///     Ok(())
/// }
/// ```
pub struct RinglineBuilder {
    config: Config,
    bind_addr: Option<BindAddr>,
}

impl RinglineBuilder {
    /// Create a new builder with the given config.
    pub fn new(config: Config) -> Self {
        RinglineBuilder {
            config,
            bind_addr: None,
        }
    }

    /// Set the bind address for the TCP listener. If not set, no listener
    /// or acceptor thread is created (client-only mode).
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(BindAddr::Tcp(addr));
        self
    }

    /// Set the bind path for a Unix domain socket listener. If not set, no
    /// listener or acceptor thread is created (client-only mode).
    ///
    /// Any existing socket file at the given path is unlinked before binding.
    pub fn bind_unix(mut self, path: impl AsRef<Path>) -> Self {
        self.bind_addr = Some(BindAddr::Unix(path.as_ref().to_path_buf()));
        self
    }

    /// Bind a UDP socket on each worker (with `SO_REUSEPORT`).
    ///
    /// Can be called multiple times to bind multiple UDP addresses.
    /// Each worker creates its own socket per address.
    pub fn bind_udp(mut self, addr: SocketAddr) -> Self {
        self.config.udp_bind.push(addr);
        self.config.udp_connect_peers.push(None);
        self
    }

    /// Bind a UDP socket on each worker (with `SO_REUSEPORT`) and immediately
    /// `connect(2)` it to `peer`. The kernel then filters incoming datagrams
    /// to `peer` and the runtime uses the lighter `RecvUdp`/`SendUdp`
    /// opcodes instead of `RecvMsgUdp`/`SendMsgUdp`. Saves ~4 microseconds
    /// per round trip on single-shot client workloads.
    pub fn bind_udp_connected(mut self, local: SocketAddr, peer: SocketAddr) -> Self {
        self.config.udp_bind.push(local);
        self.config.udp_connect_peers.push(Some(peer));
        self
    }

    /// Launch worker threads with the async `AsyncEventHandler`.
    ///
    /// Each accepted connection gets a long-lived async task. The executor
    /// polls futures on the same thread-per-core model. `launch()` waits for
    /// each worker to construct and prepare its backend before creating the
    /// listener. Errors from the subsequent event-loop run can still surface
    /// through the returned worker handles after the listener is live.
    pub fn launch<A: AsyncEventHandler>(self) -> LaunchResult {
        self.launch_inner(
            |worker_id,
             config,
             accept_rx,
             mut eventfd,
             shutdown_flag,
             resolve_rx,
             resolve_tx,
             resolver,
             spawn_rx,
             spawn_tx,
             spawner,
             blocking_rx,
             blocking_tx,
             blocking_pool,
             region_rx,
             startup_tx| {
                let handler = A::create_for_worker(worker_id);
                // The io_uring backend's `AsyncEventLoop::new` returns
                // `crate::error::Error`; the mio backend returns
                // `io::Error`. Normalise to `crate::error::Error` so
                // the rest of this closure is backend-agnostic.
                #[cfg(has_io_uring)]
                let new_result = AsyncEventLoop::new(
                    &config,
                    handler,
                    accept_rx,
                    eventfd.0.as_raw_fd(),
                    shutdown_flag,
                    resolve_rx,
                    resolve_tx,
                    resolver,
                    spawn_rx,
                    spawn_tx,
                    spawner,
                    blocking_rx,
                    blocking_tx,
                    blocking_pool,
                    region_rx,
                );
                #[cfg(not(has_io_uring))]
                let new_result = {
                    drop(region_rx);
                    AsyncEventLoop::new(
                        &config,
                        handler,
                        accept_rx,
                        eventfd.0.as_raw_fd(),
                        eventfd.1,
                        shutdown_flag,
                        resolve_rx,
                        resolve_tx,
                        resolver,
                        spawn_rx,
                        spawn_tx,
                        spawner,
                        blocking_rx,
                        blocking_tx,
                        blocking_pool,
                    )
                };
                #[cfg(has_io_uring)]
                let event_loop_result: Result<_, crate::error::Error> = new_result;
                #[cfg(not(has_io_uring))]
                let event_loop_result: Result<_, crate::error::Error> =
                    new_result.map_err(crate::error::Error::Io);

                let mut event_loop = match event_loop_result {
                    Ok(event_loop) => event_loop,
                    Err(e) => {
                        let _ = startup_tx.send(Err(()));
                        return Err(e);
                    }
                };

                // Keep `event_loop` in this final local binding from
                // preparation through `run()`: the io_uring eventfd-read SQE
                // points into its inline driver storage, so moving the value
                // after `prepare_run()` would invalidate that pointer.
                eventfd.0.transfer_to_driver();
                if let Err(e) = event_loop.prepare_run() {
                    let _ = startup_tx.send(Err(()));
                    return Err(e);
                }

                // Signal only after the backend preparation Ringline knows can
                // fail before `run()`. This lets `launch()` surface those
                // errors rather than burying them in an unjoined worker.
                let _ = startup_tx.send(Ok(()));
                drop(startup_tx);
                event_loop.run()?;
                Ok(())
            },
        )
    }

    /// Common infrastructure setup for launch.
    #[allow(clippy::needless_range_loop)]
    #[allow(clippy::type_complexity)]
    fn launch_inner<F>(self, worker_fn: F) -> LaunchResult
    where
        F: Fn(
                usize,
                Config,
                Option<crossbeam_channel::Receiver<(RawFd, SocketAddr)>>,
                (WorkerReadFd, crate::wakeup::WakeFd),
                Arc<AtomicBool>,
                Option<crossbeam_channel::Receiver<crate::resolver::ResolveResponse>>,
                Option<crossbeam_channel::Sender<crate::resolver::ResolveResponse>>,
                Option<Arc<crate::resolver::ResolverPool>>,
                Option<crossbeam_channel::Receiver<crate::spawner::SpawnResponse>>,
                Option<crossbeam_channel::Sender<crate::spawner::SpawnResponse>>,
                Option<Arc<crate::spawner::SpawnerPool>>,
                Option<crossbeam_channel::Receiver<crate::blocking::BlockingResponse>>,
                Option<crossbeam_channel::Sender<crate::blocking::BlockingResponse>>,
                Option<Arc<crate::blocking::BlockingPool>>,
                crate::region_registry::RegionControlRx,
                crossbeam_channel::Sender<Result<(), ()>>,
            ) -> Result<(), crate::error::Error>
            + Send
            + Clone
            + 'static,
    {
        // Re-validate: RinglineBuilder::bind_udp / bind_udp_connected mutate
        // udp_bind AFTER ConfigBuilder::build() ran validate(), and every
        // UDP check is gated on !udp_bind.is_empty() — so builder-bound UDP
        // sockets used to skip validation entirely (e.g. GRO with a
        // too-small buffer silently truncates datagrams).
        self.config.validate()?;

        let num_threads = if self.config.worker.threads == 0 {
            crate::topology::physical_core_count()
        } else {
            self.config.worker.threads
        };

        ensure_nofile_limit(self.config.max_connections, num_threads)?;

        crate::metrics::init_metadata();

        // Create per-worker channels and wake fds. `worker_wake_handles`
        // (Arc-based) is what `ShutdownHandle` keeps and what
        // `worker_wake_handle()` hands out to users; `worker_wake_fds`
        // (Copy) is what the acceptor and internal request structs use on
        // hot paths.
        let mut worker_txs = Vec::with_capacity(num_threads);
        let mut worker_rxs = Vec::with_capacity(num_threads);
        let mut worker_eventfds = Vec::with_capacity(num_threads);
        let mut worker_wake_fds = Vec::with_capacity(num_threads);
        let mut worker_wake_handles = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            // Bounded so a slow worker applies backpressure on the acceptor
            // rather than queuing fds indefinitely. On full, the acceptor
            // tries the next worker; if every worker is full, the incoming
            // fd is closed so the kernel can signal connection-refused to
            // the peer instead of letting the listen queue overflow.
            let (tx, rx) = crossbeam_channel::bounded::<(RawFd, SocketAddr)>(
                self.config.accept_queue_capacity,
            );
            let (read_fd, wake_handle) =
                crate::wakeup::create_wake_fd().map_err(crate::error::Error::Io)?;
            worker_txs.push(tx);
            worker_rxs.push(rx);
            worker_eventfds.push(WorkerReadFd::new(read_fd));
            worker_wake_fds.push(wake_handle.as_wake_fd());
            worker_wake_handles.push(wake_handle);
        }

        let shutdown_flag = Arc::new(AtomicBool::new(false));

        // Create resolver pool if configured.
        let (resolver_pool, resolve_rxs) = if self.config.resolver_threads > 0 {
            let pool = Arc::new(crate::resolver::ResolverPool::start(
                self.config.resolver_threads,
            ));
            let mut rxs = Vec::with_capacity(num_threads);
            for _ in 0..num_threads {
                let (tx, rx) = crossbeam_channel::unbounded::<crate::resolver::ResolveResponse>();
                rxs.push((tx, rx));
            }
            (Some(pool), Some(rxs))
        } else {
            (None, None)
        };

        // Create spawner pool if configured.
        let (spawner_pool, spawn_rxs) = if self.config.spawner_threads > 0 {
            let pool = Arc::new(crate::spawner::SpawnerPool::start(
                self.config.spawner_threads,
            ));
            let mut rxs = Vec::with_capacity(num_threads);
            for _ in 0..num_threads {
                let (tx, rx) = crossbeam_channel::unbounded::<crate::spawner::SpawnResponse>();
                rxs.push((tx, rx));
            }
            (Some(pool), Some(rxs))
        } else {
            (None, None)
        };

        // Region-registry control channels (one per worker). Always built;
        // only the io_uring backend drains them and the public API is gated
        // behind `cfg(has_io_uring)`.
        let (region_txs, region_rxs) = crate::region_registry::build_worker_channels(num_threads);
        let mut region_rxs_iter: Vec<_> = region_rxs.into_iter().map(Some).collect();

        // Create blocking pool if configured.
        let (blocking_pool, blocking_rxs) = if self.config.blocking_threads > 0 {
            let pool = Arc::new(crate::blocking::BlockingPool::start(
                self.config.blocking_threads,
            ));
            let mut rxs = Vec::with_capacity(num_threads);
            for _ in 0..num_threads {
                let (tx, rx) = crossbeam_channel::unbounded::<crate::blocking::BlockingResponse>();
                rxs.push((tx, rx));
            }
            (Some(pool), Some(rxs))
        } else {
            (None, None)
        };

        // Retain only bind intent and worker senders until every worker has
        // completed fallible setup. No socket is bound or listening yet.
        let pending_bind_addr = self.bind_addr;
        let has_acceptor = pending_bind_addr.is_some();
        let pending_worker_txs = if has_acceptor {
            Some(worker_txs)
        } else {
            drop(worker_txs);
            None
        };

        // Spawn worker threads. Each worker reports its setup outcome
        // (Ok / Err) over `startup_rx` so we can surface bind / config
        // errors to the caller of `launch()` instead of silently
        // swallowing them inside a thread that never gets joined.
        let mut handles: Vec<WorkerHandle> = Vec::with_capacity(num_threads);
        let (startup_tx, startup_rx) = crossbeam_channel::bounded::<Result<(), ()>>(num_threads);

        // SMT-aware pinning: when the requested worker range fits within
        // the machine's physical cores, treat `core_offset + worker_id`
        // as a physical-core index and pin to that core's first SMT
        // sibling. On machines that enumerate hyperthread siblings
        // adjacently (cpu0/cpu1 = one core), raw logical ids would stack
        // two workers on one core. Ranges that don't fit fall back to
        // raw logical ids so deliberate hyperthread layouts remain
        // expressible.
        let physical_cpus: Option<Vec<usize>> = if self.config.worker.pin_to_core {
            crate::topology::physical_core_first_cpus()
                .filter(|cpus| self.config.worker.core_offset + num_threads <= cpus.len())
        } else {
            None
        };

        for worker_id in 0..num_threads {
            let config = self.config.clone();
            let rx = worker_rxs.remove(0);
            // (read end for polling, write end for cross-thread wakes —
            // on the mio backend these are the two ends of a pipe; the
            // disk-I/O pool must write the WRITE end. It used to be handed
            // the read end, so every fs completion wake was an EBADF no-op
            // and completions were only noticed at the poll timeout.)
            let eventfd = (worker_eventfds.remove(0), worker_wake_fds[worker_id]);
            let worker_shutdown_flag = shutdown_flag.clone();
            let worker_fn = worker_fn.clone();
            let startup_tx = startup_tx.clone();

            let (worker_resolve_rx, worker_resolve_tx, worker_resolver) =
                if let Some(ref rxs) = resolve_rxs {
                    let (ref tx, ref rx) = rxs[worker_id];
                    (Some(rx.clone()), Some(tx.clone()), resolver_pool.clone())
                } else {
                    (None, None, None)
                };

            let (worker_spawn_rx, worker_spawn_tx, worker_spawner) =
                if let Some(ref rxs) = spawn_rxs {
                    let (ref tx, ref rx) = rxs[worker_id];
                    (Some(rx.clone()), Some(tx.clone()), spawner_pool.clone())
                } else {
                    (None, None, None)
                };

            let (worker_blocking_rx, worker_blocking_tx, worker_blocking_pool) =
                if let Some(ref rxs) = blocking_rxs {
                    let (ref tx, ref rx) = rxs[worker_id];
                    (Some(rx.clone()), Some(tx.clone()), blocking_pool.clone())
                } else {
                    (None, None, None)
                };

            let worker_region_rx = region_rxs_iter[worker_id]
                .take()
                .expect("region rx already consumed");

            let pin_cpu = {
                let raw = self.config.worker.core_offset + worker_id;
                physical_cpus.as_ref().map(|cpus| cpus[raw]).unwrap_or(raw)
            };

            let spawn_result = thread::Builder::new()
                .name(format!("ringline-worker-{worker_id}"))
                .spawn(move || {
                    if config.worker.pin_to_core {
                        let core = pin_cpu;
                        // Report the failure before bailing — otherwise
                        // the launching thread waits indefinitely for
                        // a startup signal that never arrives.
                        if let Err(e) = pin_to_core(core) {
                            // A bare EINVAL here is opaque — name the knob.
                            eprintln!(
                                "ringline: failed to pin worker {worker_id} to logical CPU {core} \
                                 (core_offset {} + worker id): {e} — check Config::core_offset \
                                 against the machine's CPU count",
                                config.worker.core_offset
                            );
                            let _ = startup_tx.send(Err(()));
                            return Err(e);
                        }
                    }

                    metriken::set_thread_shard(worker_id);

                    let accept_rx = if has_acceptor { Some(rx) } else { None };
                    worker_fn(
                        worker_id,
                        config,
                        accept_rx,
                        eventfd,
                        worker_shutdown_flag,
                        worker_resolve_rx,
                        worker_resolve_tx,
                        worker_resolver,
                        worker_spawn_rx,
                        worker_spawn_tx,
                        worker_spawner,
                        worker_blocking_rx,
                        worker_blocking_tx,
                        worker_blocking_pool,
                        worker_region_rx,
                        startup_tx,
                    )
                });

            let handle = match spawn_result {
                Ok(handle) => handle,
                Err(error) => {
                    rollback_workers(&shutdown_flag, &worker_wake_fds, handles);
                    return Err(crate::error::Error::Io(error));
                }
            };

            handles.push(handle);
        }

        // Drop our copy so `recv()` on the receiver side terminates if
        // every worker happens to die before sending.
        drop(startup_tx);

        // Collect setup outcomes. If any worker failed setup, signal
        // shutdown to the rest, join everyone, and surface the first
        // setup error back to the caller of `launch()`.
        let mut setup_failed = false;
        for _ in 0..num_threads {
            match startup_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(())) | Err(_) => {
                    setup_failed = true;
                    break;
                }
            }
        }

        if setup_failed {
            let first_err = rollback_workers(&shutdown_flag, &worker_wake_fds, handles);
            return Err(first_err.unwrap_or_else(|| {
                crate::error::Error::Io(io::Error::other("worker setup failed"))
            }));
        }

        // Commit the listener only after every worker has completed fallible
        // initialization. Before this point clients cannot connect or enter a
        // kernel listen backlog.
        let (listen_fd, listen_fd_closed, bound_addr) = if has_acceptor {
            let listener = match pending_bind_addr.expect("bind intent must exist") {
                BindAddr::Tcp(addr) => create_listener(addr, self.config.backlog)
                    .map(|fd| (fd, false, getsockname_v4_v6(fd))),
                BindAddr::Unix(ref path) => {
                    create_unix_listener(path, self.config.backlog).map(|fd| (fd, true, None))
                }
            };
            let (fd, is_unix, bound_addr) = match listener {
                Ok(listener) => listener,
                Err(error) => {
                    rollback_workers(&shutdown_flag, &worker_wake_fds, handles);
                    return Err(error);
                }
            };
            let closed = Arc::new(AtomicBool::new(false));
            let acceptor_config = AcceptorConfig {
                listen_fd: fd,
                worker_channels: pending_worker_txs.expect("worker senders must exist"),
                worker_wake_handles: worker_wake_fds.clone(),
                shutdown_flag: shutdown_flag.clone(),
                tcp_nodelay: if is_unix {
                    false
                } else {
                    self.config.tcp_nodelay
                },
                #[cfg(feature = "timestamps")]
                timestamps: self.config.timestamps,
                conn_chunk_size: self.config.conn_chunk_size,
            };
            let acceptor_closed = closed.clone();
            let spawn_result = thread::Builder::new()
                .name("ringline-acceptor".to_string())
                .spawn(move || {
                    run_acceptor(acceptor_config);
                    if !acceptor_closed.swap(true, Ordering::AcqRel) {
                        unsafe {
                            libc::close(fd);
                        }
                    }
                });

            if let Err(error) = spawn_result {
                if !closed.swap(true, Ordering::AcqRel) {
                    unsafe {
                        libc::close(fd);
                    }
                }
                rollback_workers(&shutdown_flag, &worker_wake_fds, handles);
                return Err(crate::error::Error::Io(error));
            }
            (Some(fd), Some(closed), bound_addr)
        } else {
            (None, None, None)
        };

        let region_registrar = Arc::new(crate::region_registry::RegionRegistrar::new(
            self.config.max_registered_regions,
            self.config.registered_regions.len() as u16,
            region_txs,
            worker_wake_handles.clone(),
        ));

        let shutdown_handle = ShutdownHandle {
            shutdown_flag,
            worker_wake_handles,
            listen_fd,
            listen_fd_closed,
            bound_addr,
            region_registrar,
        };

        Ok((shutdown_handle, handles))
    }
}

/// Ensure RLIMIT_NOFILE is high enough for the io_uring fixed file table.
///
/// Each worker calls `register_files_sparse(max_connections)`, and the kernel
/// checks `nr_args > rlimit(RLIMIT_NOFILE)` per call (not cumulative across
/// workers). Connections use the fixed file table — the original FD is closed
/// immediately after `register_files_update` — so they don't consume process
/// FD table entries. We only need headroom for ring fds, eventfds, the listen
/// socket, stdin/stdout/stderr, etc.
fn ensure_nofile_limit(
    max_connections: u32,
    num_workers: usize,
) -> Result<(), crate::error::Error> {
    let mut rlim: libc::rlimit = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if ret != 0 {
        return Err(crate::error::Error::Io(io::Error::last_os_error()));
    }

    // io_uring: register_files_sparse(max_connections) needs RLIMIT_NOFILE
    // >= max_connections (the kernel's check is per-ring; connections live
    // in fixed-file tables, not real fds). Add per-worker overhead (ring
    // fd, eventfd, transient socket fds) and global overhead (listen
    // socket, stdio, misc).
    //
    // mio: every connection holds a REAL fd and each worker has its own
    // max_connections-slot table, so the worst case scales with the worker
    // count — the io_uring formula under-provisioned and configs passed
    // the check only to fail with EMFILE under load.
    let per_worker_overhead: u64 = 8;
    let global_overhead: u64 = 64;
    #[cfg(has_io_uring)]
    let conn_fds = max_connections as u64;
    #[cfg(not(has_io_uring))]
    let conn_fds = max_connections as u64 * num_workers as u64;
    let required = conn_fds + per_worker_overhead * num_workers as u64 + global_overhead;

    let soft = rlim.rlim_cur;
    let hard = rlim.rlim_max;

    if soft >= required {
        return Ok(());
    }

    if hard >= required || hard == libc::RLIM_INFINITY {
        // Raise soft limit to required (or hard if hard is finite and smaller)
        let new_soft = if hard == libc::RLIM_INFINITY {
            required
        } else {
            std::cmp::min(required, hard)
        };
        rlim.rlim_cur = new_soft;
        let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) };
        if ret != 0 {
            return Err(crate::error::Error::Io(io::Error::last_os_error()));
        }
        Ok(())
    } else {
        Err(crate::error::Error::ResourceLimit(format!(
            "RLIMIT_NOFILE too low: need {} but hard limit is {} (soft: {}). \
             Raise it with: ulimit -n {}",
            required, hard, soft, required
        )))
    }
}

/// Pin the current thread to a specific CPU core.
#[cfg(target_os = "linux")]
fn pin_to_core(core: usize) -> Result<(), crate::error::Error> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        if ret != 0 {
            return Err(crate::error::Error::Io(io::Error::last_os_error()));
        }
    }
    Ok(())
}

/// Pin the current thread to a specific CPU core (no-op on non-Linux).
#[cfg(not(target_os = "linux"))]
fn pin_to_core(_core: usize) -> Result<(), crate::error::Error> {
    // Thread pinning is not supported on this platform.
    Ok(())
}

/// Create a TCP listener without SO_REUSEPORT (just SO_REUSEADDR).
fn create_listener(addr: SocketAddr, backlog: i32) -> Result<RawFd, crate::error::Error> {
    let domain = if addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };

    let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(crate::error::Error::Io(io::Error::last_os_error()));
    }

    // Set SO_REUSEADDR only (no SO_REUSEPORT).
    let optval: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }

    // Bind — use the driver's sockaddr helper.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let addr_len = crate::backend::socket_addr_to_sockaddr(addr, &mut storage);

    let ret = unsafe { libc::bind(fd, &storage as *const _ as *const libc::sockaddr, addr_len) };
    if ret < 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(crate::error::Error::Io(err));
    }

    let ret = unsafe { libc::listen(fd, backlog) };
    if ret < 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(crate::error::Error::Io(err));
    }

    Ok(fd)
}

/// Create a Unix domain socket listener at the given path.
///
/// Unlinks any existing socket file before binding.
fn create_unix_listener(path: &Path, backlog: i32) -> Result<RawFd, crate::error::Error> {
    // Remove existing socket file if present (ignore errors — path may not exist).
    let _ = std::fs::remove_file(path);

    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(crate::error::Error::Io(io::Error::last_os_error()));
    }

    // Bind using the driver's sockaddr helper.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let addr_len = crate::backend::unix_path_to_sockaddr(path, &mut storage);

    let ret = unsafe { libc::bind(fd, &storage as *const _ as *const libc::sockaddr, addr_len) };
    if ret < 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(crate::error::Error::Io(err));
    }

    let ret = unsafe { libc::listen(fd, backlog) };
    if ret < 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(crate::error::Error::Io(err));
    }

    Ok(fd)
}

#[cfg(test)]
mod startup_gate_tests {
    use super::*;
    use std::path::PathBuf;
    #[cfg(target_os = "linux")]
    use std::process::Command;
    use std::time::Duration;

    #[cfg(target_os = "linux")]
    const FD_LEAK_CHILD: &str =
        "worker::startup_gate_tests::worker_startup_failure_closes_all_runtime_fds_child";

    fn unix_socket_path() -> PathBuf {
        std::env::temp_dir().join(format!("ringline-startup-gate-{}.sock", std::process::id()))
    }

    fn one_worker_config() -> Config {
        crate::ConfigBuilder::new()
            .workers(1)
            .pin_to_core(false)
            .resolver_threads(0)
            .spawner_threads(0)
            .blocking_threads(0)
            .disk_io_threads(0)
            .build()
            .unwrap()
    }

    #[test]
    fn listener_is_not_created_before_worker_startup_succeeds() {
        let path = unix_socket_path();
        let _ = std::fs::remove_file(&path);
        let (ready_tx, ready_rx) = crossbeam_channel::bounded(1);
        let (inspect_tx, inspect_rx) = crossbeam_channel::bounded(1);
        let (observed_tx, observed_rx) = crossbeam_channel::bounded(1);
        let launch_path = path.clone();

        let launcher = thread::spawn(move || {
            RinglineBuilder::new(one_worker_config())
                .bind_unix(launch_path)
                .launch_inner(
                    move |_,
                          _,
                          accept_rx,
                          _eventfd,
                          _,
                          _,
                          _,
                          _,
                          _,
                          _,
                          _,
                          _,
                          _,
                          _,
                          _,
                          startup_tx| {
                        ready_tx.send(()).unwrap();
                        inspect_rx.recv().unwrap();
                        let accepted = accept_rx
                            .unwrap()
                            .recv_timeout(Duration::from_millis(250))
                            .ok();
                        observed_tx.send(accepted.is_some()).unwrap();
                        if let Some((fd, _)) = accepted {
                            unsafe { libc::close(fd) };
                        }
                        let _ = startup_tx.send(Err(()));
                        Err(crate::error::Error::Io(io::Error::other(
                            "injected worker startup failure",
                        )))
                    },
                )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let absent_before_worker_ready = !path.exists();
        inspect_tx.send(()).unwrap();

        assert!(!observed_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        assert!(launcher.join().unwrap().is_err());
        let absent_after_rollback = !path.exists();
        let _ = std::fs::remove_file(path);
        assert!(absent_before_worker_ready);
        assert!(absent_after_rollback);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_startup_failure_closes_all_runtime_fds() {
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", FD_LEAK_CHILD])
            .env("RINGLINE_FD_LEAK_CHILD", "1")
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "spawned by worker_startup_failure_closes_all_runtime_fds"]
    fn worker_startup_failure_closes_all_runtime_fds_child() {
        if std::env::var("RINGLINE_FD_LEAK_CHILD").as_deref() != Ok("1") {
            return;
        }

        fn fd_count() -> usize {
            std::fs::read_dir("/proc/self/fd").unwrap().count()
        }

        let before = fd_count();
        for _ in 0..4 {
            let result = RinglineBuilder::new(one_worker_config()).launch_inner(
                |_, _, _, _eventfd, _, _, _, _, _, _, _, _, _, _, _, startup_tx| {
                    let _ = startup_tx.send(Err(()));
                    Err(crate::error::Error::Io(io::Error::other(
                        "injected worker startup failure",
                    )))
                },
            );
            assert!(result.is_err());
        }
        assert_eq!(fd_count(), before);
    }
}
