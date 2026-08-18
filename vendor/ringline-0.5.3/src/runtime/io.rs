use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::Future;
use std::io;
#[cfg(has_io_uring)]
use std::marker::PhantomData;
use std::net::SocketAddr;
#[cfg(has_io_uring)]
use std::ops::Deref;
#[cfg(has_io_uring)]
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::backend::Driver;
#[cfg(has_io_uring)]
use crate::completion::{OpTag, UserData};
use crate::error::TimerExhausted;
use crate::handler::ConnToken;
#[cfg(has_io_uring)]
use crate::runtime::TimerSlotPool;
use crate::runtime::task::TaskId;
use crate::runtime::waker::STANDALONE_BIT;
use crate::runtime::{CURRENT_TASK_ID, Executor, IoResult};

/// Result of a parse closure passed to [`ConnCtx::with_data`] or [`ConnCtx::with_bytes`].
///
/// When the closure returns `NeedMore` or `Consumed(0)`, the future parks and
/// retries when more data arrives. `Consumed(0)` on a non-empty buffer is
/// treated identically to `NeedMore`. When the connection is closed (EOF),
/// the `with_data`/`with_bytes` future resolves with `0` regardless of the
/// parse result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseResult {
    /// The closure consumed `n` bytes from the buffer.
    ///
    /// `Consumed(0)` on non-empty data is treated as "need more data" — the
    /// future will park and retry when additional bytes arrive.
    Consumed(usize),
    /// The closure needs more data before it can make progress.
    NeedMore,
    /// The closure needs at least `additional` more bytes beyond what it
    /// was shown (e.g. a length-prefixed protocol that has parsed its
    /// header). Parks exactly like [`NeedMore`](Self::NeedMore), but the
    /// runtime reserves recv-accumulator capacity for the announced
    /// remainder up front — one right-sized allocation instead of the
    /// doubling-regrowth cascade (~2× the payload in extra memcpy for a
    /// multi-MB message arriving in chunks).
    NeedAtLeast(usize),
}

/// Driver + executor state pointer, set before polling each task.
///
/// Using `NonNull` instead of raw pointers provides better type safety:
/// 1. **Non-null guarantee**: `NonNull` is guaranteed non-null at runtime
///    (enforced in debug builds), catching bugs earlier
/// 2. **Zero overhead**: Same performance as raw pointers
/// 3. **Clear ownership**: The pointer is set before poll and cleared after,
///    in a single-threaded context (no concurrent access)
pub(crate) struct DriverState {
    pub(crate) driver: NonNull<Driver>,
    pub(crate) executor: NonNull<Executor>,
}

thread_local! {
    /// Thread-local storage for the current driver state.
    ///
    /// # Safety
    ///
    /// This is safe because:
    /// 1. Single-threaded: each worker thread has its own driver/executor.
    /// 2. Scoped: set before poll, cleared after poll. The pointer is only
    ///    dereferenced within a Future::poll call.
    /// 3. The pointed-to data lives on the worker thread's stack (in AsyncEventLoop::run).
    pub(crate) static CURRENT_DRIVER: Cell<Option<NonNull<DriverState>>> =
        const { Cell::new(None) };
}

/// Set the thread-local driver pointer before polling a task.
///
/// # Safety
///
/// The caller must ensure `state` points to valid `DriverState` on the
/// current thread's stack.
pub(crate) unsafe fn set_driver_state(state: &mut DriverState) {
    CURRENT_DRIVER.with(|c| c.set(Some(NonNull::from(state))));
}

/// Clear the thread-local driver pointer after polling a task.
pub(crate) fn clear_driver_state() {
    CURRENT_DRIVER.with(|c| c.set(None));
}

/// RAII guard returned by [`set_driver_state_guarded`]. Clears the
/// thread-local driver pointer on drop — including during panic unwind.
/// Without this, a panic between set and clear leaves `CURRENT_DRIVER`
/// pointing into a popped stack frame, and futures' `Drop` impls
/// (`SendFuture`, `SleepFuture`, ...) dereference it while the executor's
/// slabs unwind — a use-after-free.
pub(crate) struct DriverStateGuard(());

impl Drop for DriverStateGuard {
    fn drop(&mut self) {
        clear_driver_state();
    }
}

/// Set the thread-local driver pointer, returning a guard that clears it
/// when dropped (normally or during unwind).
///
/// # Safety
///
/// Same contract as [`set_driver_state`]: `state` must point to valid
/// `DriverState` on the current thread's stack for the guard's lifetime.
pub(crate) unsafe fn set_driver_state_guarded(state: &mut DriverState) -> DriverStateGuard {
    unsafe { set_driver_state(state) };
    DriverStateGuard(())
}

/// Access the thread-local driver state. Panics if called outside the executor.
///
/// # Safety
///
/// This function is safe to call because:
/// 1. The pointer is always set before polling and cleared after
/// 2. Single-threaded execution means no concurrent access
/// 3. NonNull provides a runtime non-null check in debug builds
pub(crate) fn with_state<R>(f: impl FnOnce(&mut Driver, &mut Executor) -> R) -> R {
    let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
    let mut non_null = opt_non_null.expect("called outside executor");

    let state = unsafe { non_null.as_mut() };
    let driver = unsafe { &mut *state.driver.as_mut() };
    let executor = unsafe { &mut *state.executor.as_mut() };
    f(driver, executor)
}

/// Access the thread-local driver state, returning `None` if called outside the executor.
pub(crate) fn try_with_state<R>(f: impl FnOnce(&mut Driver, &mut Executor) -> R) -> Option<R> {
    let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
    let mut non_null = opt_non_null?;

    let state = unsafe { non_null.as_mut() };
    let driver = unsafe { &mut *state.driver.as_mut() };
    let executor = unsafe { &mut *state.executor.as_mut() };
    Some(f(driver, executor))
}

/// Spawn a standalone async task on the current worker thread.
///
/// Unlike connection tasks (which are 1:1 with connections), standalone tasks
/// are not bound to any connection. They run on the same single-threaded
/// executor and can use [`sleep()`](crate::sleep) and [`timeout()`](crate::timeout),
/// but cannot perform connection I/O directly.
///
/// Returns `Err` if called outside the ringline async executor or if the
/// standalone task slab is full.
pub fn spawn(future: impl Future<Output = ()> + 'static) -> io::Result<TaskId> {
    try_with_state(
        |_driver, executor| match executor.standalone_slab.spawn(Box::pin(future)) {
            Some(idx) => {
                executor.ready_queue.push_back(idx | STANDALONE_BIT);
                Ok(TaskId(idx))
            }
            None => Err(io::Error::other("standalone task slab exhausted")),
        },
    )
    .unwrap_or_else(|| Err(io::Error::other("called outside executor")))
}

impl TaskId {
    /// Cancel a standalone task. Drops the future immediately, freeing
    /// the slab slot. No-op if the task already completed.
    ///
    /// Must be called from within the ringline executor (i.e., from a
    /// connection task or standalone task). Panics otherwise.
    ///
    /// Any pending timers owned by the dropped future are cancelled
    /// via their `Drop` impl. Stale entries in the ready queue are
    /// silently skipped when the executor encounters them.
    pub fn cancel(self) {
        with_state(|_driver, executor| {
            executor.standalone_slab.remove(self.0);
        });
    }
}

// ── JoinHandle ──────────────────────────────────────────────────────

/// Shared state between a spawned wrapper future and its [`JoinHandle`].
struct JoinState<T> {
    /// The task's return value, written by the wrapper when it completes.
    result: Option<T>,
    /// Raw task ID of the task awaiting this handle (includes `STANDALONE_BIT`
    /// for standalone tasks). Set by `JoinHandle::poll` when it returns `Pending`.
    waiter: Option<u32>,
    /// True if [`JoinHandle::abort`] was called.
    aborted: bool,
}

/// Handle to a spawned task's return value.
///
/// Obtained from [`spawn_with_handle()`]. Implements [`Future`] — awaiting it
/// yields the task's return value `T` once the task completes.
///
/// # Drop semantics
///
/// Dropping a `JoinHandle` without awaiting it **detaches** the task: the
/// task continues running but its result is discarded. This matches tokio's
/// semantics.
///
/// # Abort
///
/// [`abort()`](Self::abort) cancels the spawned task. A `JoinHandle` that has
/// been aborted will never resolve if polled.
pub struct JoinHandle<T> {
    state: Rc<RefCell<JoinState<T>>>,
    task_id: TaskId,
}

impl<T> JoinHandle<T> {
    /// Get the underlying [`TaskId`].
    pub fn id(&self) -> TaskId {
        self.task_id
    }

    /// Cancel the spawned task.
    ///
    /// The future is dropped immediately and its slab slot is freed.
    /// After this call, awaiting the handle will hang forever — use
    /// [`select()`](crate::select) with a flag if you need to detect
    /// cancellation.
    pub fn abort(&self) {
        self.state.borrow_mut().aborted = true;
        self.task_id.cancel();
    }
}

impl<T: 'static> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<T> {
        let mut s = self.state.borrow_mut();
        if s.aborted {
            return Poll::Pending;
        }
        if let Some(value) = s.result.take() {
            return Poll::Ready(value);
        }
        // Register this task as the waiter so the child can wake us.
        s.waiter = Some(CURRENT_TASK_ID.with(|c| c.get()));
        Poll::Pending
    }
}

/// Spawn a standalone async task and return a handle to await its result.
///
/// Like [`spawn()`], the future runs on the current worker's single-threaded
/// executor. The returned [`JoinHandle<T>`] implements [`Future<Output = T>`] —
/// awaiting it yields the task's return value once it completes.
///
/// # Detach semantics
///
/// Dropping the handle without awaiting it detaches the task: the task keeps
/// running but its result is silently discarded.
///
/// # Errors
///
/// Returns `Err` if called outside the ringline executor or if the standalone
/// task slab is exhausted.
///
/// # Panics in the spawned task
///
/// A panic in the spawned future unwinds the worker thread (same as [`spawn()`]).
pub fn spawn_with_handle<T: 'static>(
    future: impl Future<Output = T> + 'static,
) -> io::Result<JoinHandle<T>> {
    let state = Rc::new(RefCell::new(JoinState {
        result: None,
        waiter: None,
        aborted: false,
    }));
    let state_for_wrapper = Rc::clone(&state);

    let wrapper = async move {
        let value = future.await;
        let mut s = state_for_wrapper.borrow_mut();
        s.result = Some(value);
        let waiter = s.waiter.take();
        // Drop the borrow before calling with_state — defensive against
        // any re-entrant borrow in the wakeup path.
        drop(s);
        if let Some(waiter_id) = waiter {
            with_state(|_driver, executor| {
                executor.wake_task(waiter_id);
            });
        }
    };

    try_with_state(
        |_driver, executor| match executor.standalone_slab.spawn(Box::pin(wrapper)) {
            Some(idx) => {
                executor.ready_queue.push_back(idx | STANDALONE_BIT);
                Ok(JoinHandle {
                    state: Rc::clone(&state),
                    task_id: TaskId(idx),
                })
            }
            None => Err(io::Error::other("standalone task slab exhausted")),
        },
    )
    .unwrap_or_else(|| Err(io::Error::other("called outside executor")))
}

/// Offload a blocking closure to the dedicated blocking thread pool.
///
/// The closure runs on a low-priority background thread (`SCHED_IDLE`),
/// keeping the io_uring event loop unblocked. Returns a future that
/// resolves to the closure's return value.
///
/// # Errors
///
/// Returns `Err` if called outside the ringline executor or if the blocking
/// pool is not configured (`blocking_threads = 0`).
pub fn spawn_blocking<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> io::Result<BlockingJoinHandle<T>> {
    try_with_state(|driver, executor| {
        let pool = driver
            .blocking_pool
            .as_ref()
            .ok_or_else(|| io::Error::other("blocking pool not configured"))?;
        let blocking_tx = driver
            .blocking_tx
            .as_ref()
            .ok_or_else(|| io::Error::other("blocking pool not configured"))?;

        let request_id = executor.next_blocking_id;
        executor.next_blocking_id += 1;

        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor
            .pending_blocking
            .insert(request_id, (task_id, None));

        let work = Box::new(move || -> Box<dyn std::any::Any + Send> { Box::new(f()) });

        pool.request_tx
            .send(crate::blocking::BlockingRequest {
                work,
                request_id,
                response_tx: blocking_tx.clone(),
                wake_handle: driver.wake_handle,
            })
            .map_err(|_| io::Error::other("blocking pool shut down"))?;

        Ok(BlockingJoinHandle {
            request_id,
            _phantom: std::marker::PhantomData,
        })
    })
    .unwrap_or_else(|| Err(io::Error::other("called outside executor")))
}

/// Future returned by [`spawn_blocking()`]. Resolves to the closure's return value.
pub struct BlockingJoinHandle<T> {
    request_id: u64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: 'static> Future for BlockingJoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<T> {
        with_state(|_driver, executor| {
            if let Some((_, slot)) = executor.pending_blocking.get_mut(&self.request_id)
                && let Some(boxed) = slot.take()
            {
                executor.pending_blocking.remove(&self.request_id);
                let value = *boxed
                    .downcast::<T>()
                    .expect("type mismatch in BlockingJoinHandle");
                return Poll::Ready(value);
            }
            Poll::Pending
        })
    }
}

/// Initiate an outbound TCP connection from any async task (connection or standalone).
///
/// This is the free-function equivalent of [`ConnCtx::connect()`] — it can be
/// called from standalone tasks spawned via [`spawn()`] or from an
/// [`AsyncEventHandler::on_start()`](crate::AsyncEventHandler::on_start) future,
/// where no `ConnCtx` is available.
///
/// Returns a [`ConnectFuture`] that resolves with a [`ConnCtx`] for the new connection.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn connect(addr: SocketAddr) -> io::Result<ConnectFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let token = ctx
            .connect(addr)
            .map_err(io::Error::other::<crate::error::Error>)?;
        let calling_task = CURRENT_TASK_ID.with(|c| c.get());
        executor.owner_task[token.index as usize] = Some(calling_task);
        executor.connect_waiters[token.index as usize] = true;
        Ok(ConnectFuture {
            conn_index: token.index,
            generation: token.generation,
        })
    })
}

/// Initiate an outbound TCP connection with a timeout from any async task.
///
/// Free-function equivalent of [`ConnCtx::connect_with_timeout()`].
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn connect_with_timeout(addr: SocketAddr, timeout_ms: u64) -> io::Result<ConnectFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let token = ctx
            .connect_with_timeout(addr, timeout_ms)
            .map_err(io::Error::other::<crate::error::Error>)?;
        let calling_task = CURRENT_TASK_ID.with(|c| c.get());
        executor.owner_task[token.index as usize] = Some(calling_task);
        executor.connect_waiters[token.index as usize] = true;
        Ok(ConnectFuture {
            conn_index: token.index,
            generation: token.generation,
        })
    })
}

/// Initiate an outbound Unix domain socket connection from any async task.
///
/// Free-function equivalent of [`ConnCtx::connect_unix()`].
///
/// Returns a [`ConnectFuture`] that resolves with a [`ConnCtx`] for the new connection.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn connect_unix(path: impl AsRef<std::path::Path>) -> io::Result<ConnectFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let token = ctx
            .connect_unix(path.as_ref())
            .map_err(io::Error::other::<crate::error::Error>)?;
        let calling_task = CURRENT_TASK_ID.with(|c| c.get());
        executor.owner_task[token.index as usize] = Some(calling_task);
        executor.connect_waiters[token.index as usize] = true;
        Ok(ConnectFuture {
            conn_index: token.index,
            generation: token.generation,
        })
    })
}

/// Initiate an outbound TLS connection from any async task (connection or standalone).
///
/// This is the free-function equivalent of [`ConnCtx::connect_tls()`] — it can be
/// called from standalone tasks spawned via [`spawn()`] or from an
/// [`AsyncEventHandler::on_start()`](crate::AsyncEventHandler::on_start) future,
/// where no `ConnCtx` is available.
///
/// `server_name` is the SNI hostname for the TLS handshake.
///
/// Returns a [`ConnectFuture`] that resolves with a [`ConnCtx`] for the new connection
/// once both the TCP and TLS handshakes complete.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn connect_tls(addr: SocketAddr, server_name: &str) -> io::Result<ConnectFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let token = ctx
            .connect_tls(addr, server_name)
            .map_err(io::Error::other::<crate::error::Error>)?;
        let calling_task = CURRENT_TASK_ID.with(|c| c.get());
        executor.owner_task[token.index as usize] = Some(calling_task);
        executor.connect_waiters[token.index as usize] = true;
        Ok(ConnectFuture {
            conn_index: token.index,
            generation: token.generation,
        })
    })
}

/// Initiate an outbound TLS connection with a timeout from any async task.
///
/// Free-function equivalent of [`ConnCtx::connect_tls_with_timeout()`].
///
/// `server_name` is the SNI hostname for the TLS handshake.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn connect_tls_with_timeout(
    addr: SocketAddr,
    server_name: &str,
    timeout_ms: u64,
) -> io::Result<ConnectFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let token = ctx
            .connect_tls_with_timeout(addr, server_name, timeout_ms)
            .map_err(io::Error::other::<crate::error::Error>)?;
        let calling_task = CURRENT_TASK_ID.with(|c| c.get());
        executor.owner_task[token.index as usize] = Some(calling_task);
        executor.connect_waiters[token.index as usize] = true;
        Ok(ConnectFuture {
            conn_index: token.index,
            generation: token.generation,
        })
    })
}

// ── DNS Resolution ──────────────────────────────────────────────────

/// Resolve a hostname to a [`SocketAddr`] using the dedicated resolver pool.
///
/// Performs `getaddrinfo` on a background thread, keeping the io_uring event
/// loop unblocked. Returns the first resolved address.
///
/// # Errors
///
/// Returns `Err` if called outside the ringline executor, the resolver pool
/// is not configured (`resolver_threads = 0`), or `getaddrinfo` fails.
pub fn resolve(host: &str, port: u16) -> io::Result<ResolveFuture> {
    let host = host.to_string();
    try_with_state(|driver, executor| {
        let resolver = driver
            .resolver
            .as_ref()
            .ok_or_else(|| io::Error::other("resolver pool not configured"))?;
        let resolve_tx = driver
            .resolve_tx
            .as_ref()
            .ok_or_else(|| io::Error::other("resolver pool not configured"))?;

        let request_id = executor.next_resolve_id;
        executor.next_resolve_id += 1;

        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor
            .pending_resolves
            .insert(request_id, (task_id, None));

        resolver
            .request_tx
            .send(crate::resolver::ResolveRequest {
                host,
                port,
                request_id,
                response_tx: resolve_tx.clone(),
                wake_handle: driver.wake_handle,
            })
            .map_err(|_| io::Error::other("resolver pool shut down"))?;

        Ok(ResolveFuture { request_id })
    })
    .unwrap_or_else(|| Err(io::Error::other("called outside executor")))
}

/// Future returned by [`resolve()`]. Resolves to a [`SocketAddr`].
pub struct ResolveFuture {
    request_id: u64,
}

impl Future for ResolveFuture {
    type Output = io::Result<std::net::SocketAddr>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_state(|_driver, executor| {
            if let Some((_, slot)) = executor.pending_resolves.get_mut(&self.request_id)
                && let Some(result) = slot.take()
            {
                executor.pending_resolves.remove(&self.request_id);
                return Poll::Ready(result);
            }
            Poll::Pending
        })
    }
}

/// Request graceful shutdown of the worker event loop from any async task.
///
/// This is the free-function equivalent of [`ConnCtx::request_shutdown()`] —
/// it can be called from standalone tasks or [`AsyncEventHandler::on_start()`](crate::AsyncEventHandler::on_start)
/// futures where no `ConnCtx` is available.
///
/// Returns `Err` if called outside the ringline async executor.
pub fn request_shutdown() -> io::Result<()> {
    try_with_state(|driver, _| {
        let mut ctx = driver.make_ctx();
        ctx.request_shutdown();
    })
    .ok_or_else(|| io::Error::other("called outside executor"))
}

/// Async connection context providing send, recv, and connect operations.
///
/// Each accepted connection receives a `ConnCtx` in [`AsyncEventHandler::on_accept`](crate::AsyncEventHandler::on_accept).
/// It exposes an async API for reading data ([`with_data`](Self::with_data),
/// [`with_bytes`](Self::with_bytes)), sending data ([`send`](Self::send),
/// [`send_nowait`](Self::send_nowait)), and initiating outbound connections
/// ([`connect`](Self::connect)).
///
/// A `ConnCtx` is valid for the lifetime of the connection's async task.
/// When the connection is closed, the task is dropped along with the `ConnCtx`.
///
/// # Example: Echo Handler
///
/// ```no_run
/// use ringline::{AsyncEventHandler, ConnCtx, ParseResult};
///
/// struct Echo;
///
/// impl AsyncEventHandler for Echo {
///     fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
///         async move {
///             loop {
///                 let n = conn.with_data(|data| {
///                     // Echo back whatever we received
///                     conn.send_nowait(data).ok();
///                     ParseResult::Consumed(data.len())
///                 }).await;
///                 // n == 0 means connection closed (EOF)
///                 if n == 0 { break; }
///             }
///         }
///     }
///     fn create_for_worker(_id: usize) -> Self { Echo }
/// }
/// ```
///
/// # Example: Line-Based Protocol
///
/// ```no_run
/// use ringline::{AsyncEventHandler, ConnCtx, ParseResult};
///
/// struct LineEcho;
///
/// impl AsyncEventHandler for LineEcho {
///     fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
///         async move {
///             loop {
///                 let n = conn.with_data(|data| {
///                     // Find newline
///                     if let Some(pos) = data.iter().position(|&b| b == b'\n') {
///                         let line = &data[..=pos];
///                         conn.send_nowait(line).ok();
///                         ParseResult::Consumed(pos + 1)
///                     } else {
///                         ParseResult::NeedMore
///                     }
///                 }).await;
///                 if n == 0 { break; }
///             }
///         }
///     }
///     fn create_for_worker(_id: usize) -> Self { LineEcho }
/// }
/// ```
///
/// # Example: Zero-Copy with `with_bytes`
///
/// For protocols where you want to avoid copying parsed values, use
/// [`with_bytes`](Self::with_bytes) which provides `Bytes` handles:
///
/// ```no_run
/// use ringline::{AsyncEventHandler, ConnCtx, ParseResult};
/// use bytes::Bytes;
///
/// struct ZeroCopyHandler;
///
/// impl AsyncEventHandler for ZeroCopyHandler {
///     fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
///         async move {
///             loop {
///                 let n = conn.with_bytes(|bytes| {
///                     // Parse protocol, return Bytes::slice() for the value
///                     // The slice stays valid even after the accumulator advances
///                     if let Some((consumed, value)) = parse_message(&bytes) {
///                         process_value(value); // value: Bytes
///                         ParseResult::Consumed(consumed)
///                     } else {
///                         ParseResult::NeedMore
///                     }
///                 }).await;
///                 if n == 0 { break; }
///             }
///         }
///     }
///     fn create_for_worker(_id: usize) -> Self { ZeroCopyHandler }
/// }
///
/// fn parse_message(data: &[u8]) -> Option<(usize, Bytes)> {
///     // Parse length-prefixed message: 4-byte big-endian length + payload
///     if data.len() < 4 { return None; }
///     let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
///     if data.len() < 4 + len { return None; }
///     let value = Bytes::copy_from_slice(&data[4..4+len]);
///     Some((4 + len, value))
/// }
///
/// fn process_value(_value: Bytes) {
///     // Process the zero-copy value
/// }
/// ```
///
/// # Send Patterns
///
/// ```no_run
/// use ringline::{AsyncEventHandler, ConnCtx, ParseResult};
///
/// struct SendExample;
///
/// impl AsyncEventHandler for SendExample {
///     fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
///         async move {
///             // Fire-and-forget (returns Err if send pool exhausted)
///             conn.send_nowait(b"hello").ok();
///
///             // Await send completion
///             if let Ok(future) = conn.send(b"world") {
///                 future.await.ok();
///             }
///         }
///     }
///     fn create_for_worker(_id: usize) -> Self { SendExample }
/// }
/// ```
#[derive(Clone, Copy)]
pub struct ConnCtx {
    pub(crate) conn_index: u32,
    pub(crate) generation: u32,
}

impl ConnCtx {
    /// Create a new ConnCtx for the given connection.
    pub(crate) fn new(conn_index: u32, generation: u32) -> Self {
        ConnCtx {
            conn_index,
            generation,
        }
    }

    /// Construct a `ConnCtx` with arbitrary index/generation for use in
    /// downstream-crate unit tests that exercise buffering / encoding logic
    /// without a live io_uring driver.
    ///
    /// The returned handle does **not** refer to any real connection. Any I/O
    /// method (`with_data`, `send`, `flush`, …) will index into a driver table
    /// that has no slot for it and is therefore undefined to call. It is only
    /// safe to use in tests that touch in-memory client state (write buffers,
    /// pending queues, encoders) and never reach the wire.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn for_test(conn_index: u32, generation: u32) -> Self {
        ConnCtx::new(conn_index, generation)
    }

    /// Returns the connection slot index. Useful for indexing into per-connection arrays.
    pub fn index(&self) -> usize {
        self.conn_index as usize
    }

    /// Returns the `ConnToken` for this connection.
    pub fn token(&self) -> ConnToken {
        ConnToken::new(self.conn_index, self.generation)
    }

    // ── Recv ─────────────────────────────────────────────────────────

    /// Wait until recv data is available, then process it.
    ///
    /// The closure receives accumulated bytes and returns a [`ParseResult`]:
    /// - `ParseResult::Consumed(n)` — `n` bytes were consumed from the buffer.
    /// - `ParseResult::NeedMore` — the closure needs more data before making progress.
    ///
    /// Resolves immediately when data is already buffered (cache-hit hot path).
    ///
    /// If the closure returns `NeedMore` or `Consumed(0)` on non-empty data
    /// (incomplete parse), the future parks and retries when more data arrives.
    /// The closure must therefore be safe to call multiple times (`FnMut`).
    pub fn with_data<F: FnMut(&[u8]) -> ParseResult>(&self, f: F) -> WithDataFuture<F> {
        WithDataFuture {
            conn_index: self.conn_index,
            generation: self.generation,
            f: Some(f),
        }
    }

    /// Wait until recv data is available, then provide it as zero-copy `Bytes`.
    ///
    /// Like [`with_data()`](Self::with_data), but the closure receives a `Bytes`
    /// handle that can be sliced (O(1), refcounted) instead of copied. The
    /// closure returns a [`ParseResult`] indicating bytes consumed.
    ///
    /// This enables zero-copy RESP parsing: the parser can call `bytes.slice()`
    /// to extract sub-ranges without allocating.
    pub fn with_bytes<F: FnMut(Bytes) -> ParseResult>(&self, f: F) -> WithBytesFuture<F> {
        WithBytesFuture {
            conn_index: self.conn_index,
            generation: self.generation,
            f: Some(f),
        }
    }

    /// Borrow a segmented-recv reader (Mode B "Borrow", the sound lending-iterator
    /// face — see `docs/segmented-recv-design.md`).
    ///
    /// Opts this connection into *segmented* delivery: arriving provided buffers
    /// are held in-place (no copy into the accumulator, bid not replenished) and
    /// surfaced one at a time as zero-copy [`RecvSegment`]s via
    /// [`SegmentReader::next`]. A segment is `!Send`, `!Clone`, and borrows the
    /// reader exclusively (`&mut`), so it cannot escape the runtime or outlive the
    /// buffer's pin — the "holdable ⇒ copied" invariant is enforced structurally.
    ///
    /// Call this **before** the connection starts receiving: it flips the delivery
    /// domain but does not retroactively segment bytes already gathered into the
    /// accumulator under the default `with_data`/`with_bytes` path.
    ///
    /// io_uring only — segmented delivery is backed by the provided-buffer ring.
    #[cfg(has_io_uring)]
    pub fn segments(&self) -> SegmentReader<'_> {
        with_state(|driver, _executor| {
            driver.recv_domain[self.conn_index as usize] =
                crate::recv::domain::RecvDomain::Segmented;
        });
        SegmentReader {
            conn_index: self.conn_index,
            generation: self.generation,
            _borrow: PhantomData,
            _not_send: PhantomData,
        }
    }

    /// Await the next received segment as an owned, freely holdable [`Bytes`]
    /// (Mode C — "Own", copy-at-delivery; see `docs/segmented-recv-design.md`).
    ///
    /// Opts this connection into *segmented* delivery (like [`segments`](Self::segments)),
    /// then, for each arriving provided buffer, **copies** its bytes into an owned
    /// heap `Bytes` and replenishes the bid **immediately at delivery** — the copy
    /// *is* the release, so this path never pins the ring and cannot deplete it by
    /// holding (unlike a [`RecvSegment`], which pins until dropped). The returned
    /// `Bytes` is `Send`, `Clone`, and retainable indefinitely.
    ///
    /// Resolves to:
    /// - `Ok(Some(bytes))` — one received buffer, copied and owned.
    /// - `Ok(None)` — end of stream: the recv side is closed (peer FIN) with no
    ///   held buffers remaining, or the connection slot was reused (stale handle).
    ///
    /// Parks when no buffer is held and the connection is still open, resuming
    /// when the recv completion handler holds a buffer and calls `wake_recv` (the
    /// same waiter mechanism as [`segments`](Self::segments) / [`with_data`](Self::with_data)).
    ///
    /// io_uring only — segmented delivery is backed by the provided-buffer ring.
    #[cfg(has_io_uring)]
    pub fn recv_owned_segment(&self) -> RecvOwnedSegment {
        with_state(|driver, _executor| {
            driver.recv_domain[self.conn_index as usize] =
                crate::recv::domain::RecvDomain::Segmented;
        });
        RecvOwnedSegment {
            conn_index: self.conn_index,
            generation: self.generation,
        }
    }

    /// End segmented-recv delivery on this connection and restore the default
    /// [`with_data`](Self::with_data) / [`with_bytes`](Self::with_bytes) path.
    ///
    /// [`segments`](Self::segments) and [`recv_owned_segment`](Self::recv_owned_segment)
    /// leave the connection in the *segmented* domain: arriving buffers are held
    /// for the segment reader instead of being gathered into the accumulator. A
    /// consumer that has finished pulling its segments **must** call this before
    /// issuing an ordinary `with_data`/`with_bytes` read — otherwise that read
    /// would never observe further data (it keeps getting held as segments).
    ///
    /// Any buffers still held (bytes received *past* what the consumer pulled) are
    /// gathered, in arrival order, into the front of the accumulator so the next
    /// read sees them first.
    ///
    /// Returns an error (and closes the connection) if gathering the leftover
    /// segments overflows the recv accumulator bound (`recv_accumulator_max`).
    ///
    /// io_uring only — segmented delivery is backed by the provided-buffer ring.
    #[cfg(has_io_uring)]
    pub fn end_segments(&self) -> io::Result<()> {
        // `settle_forward_end` is the shared "drain held segments into the
        // accumulator, reset the delivery domain to default" routine (named for
        // its first caller, the Mode A `forward_to` completion). Reused here for
        // the streaming-value read side, which needs the identical settle.
        let ok = with_state(|driver, _executor| driver.settle_forward_end(self.conn_index));
        if ok {
            Ok(())
        } else {
            self.close();
            Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "recv accumulator overflow while ending segmented recv",
            ))
        }
    }

    /// Process the connection's buffered recv data as an **ordered chain of
    /// borrowed segments** via a callback (Mode B "Borrow", the B1 callback face
    /// — see `docs/segmented-recv-design.md`).
    ///
    /// Opts this connection into *segmented* delivery, then hands the callback a
    /// [`SegChain`] presenting, **in order**:
    /// 1. the current accumulator contents (if any) as the leading segment, then
    /// 2. each held provided buffer, in arrival order.
    ///
    /// The callback borrows those slices for the call only (they cannot escape,
    /// like [`with_data`](Self::with_data)) and returns a [`SegConsumed`] — the
    /// number of bytes it consumed **from the front** of that concatenation.
    ///
    /// After the callback returns, the runtime settles state so that all
    /// un-consumed bytes end up contiguously at the **front of the accumulator**,
    /// in order:
    /// - fully-consumed accumulator bytes are dropped from the front;
    /// - each held buffer wholly consumed by the callback is replenished to the
    ///   ring;
    /// - any remainder — a partially-consumed buffer, plus buffers the callback
    ///   did not reach (under-drain) — is **copied into the accumulator** and its
    ///   bids replenished.
    ///
    /// So the fully-draining consumer (h2/grpc: extend-each + consume-all) pays
    /// **zero** ringline copies, while an under-draining or whole-frame consumer
    /// degrades to the [`with_data`](Self::with_data) copy — never a deadlock or a
    /// bid leak. The next `with_segments` / `with_data` / `with_bytes` call sees
    /// the carried-over bytes first, in order.
    ///
    /// Resolves to `Ok(n)` = total bytes consumed this call. A callback that
    /// returns [`SegConsumed(0)`](SegConsumed) (it needs a bigger frame than is
    /// buffered) gathers everything to the accumulator and **parks** for more
    /// data — the `NeedMore` idiom, bounded by `recv_accumulator_max` (an
    /// over-run closes the connection). Resolves to `Ok(0)` at EOF (recv side
    /// closed with nothing to present) or for a stale handle (slot reused).
    ///
    /// This deliberately does **not** reuse [`ParseResult`]: its `NeedMore` grows
    /// the heap accumulator by ring depth, which is the wrong bound here.
    ///
    /// Call this **before** the connection starts receiving; it flips the delivery
    /// domain but does not retroactively segment bytes already gathered into the
    /// accumulator.
    ///
    /// io_uring only — segmented delivery is backed by the provided-buffer ring.
    #[cfg(has_io_uring)]
    pub fn with_segments<F>(&self, f: F) -> WithSegmentsFuture<F>
    where
        F: FnMut(&SegChain<'_>) -> SegConsumed,
    {
        with_state(|driver, _executor| {
            driver.recv_domain[self.conn_index as usize] =
                crate::recv::domain::RecvDomain::Segmented;
        });
        WithSegmentsFuture {
            conn_index: self.conn_index,
            generation: self.generation,
            f: Some(f),
        }
    }

    /// Forward the next `len` received bytes straight to `sink` (Mode A —
    /// "Forward", zero userspace copy; see `docs/segmented-recv-design.md`).
    ///
    /// Opts this connection into *segmented* delivery, then interleaves recv and
    /// write: arriving provided buffers are held driver-side and written directly
    /// to `sink` — a **socket** (zero-copy proxy, `send` with `MSG_WAITALL`) or a
    /// **buffered file** (`pwrite` at an advancing offset starting at 0). No
    /// intermediate copy into the accumulator or a user buffer. Each held buffer's
    /// bid is released back to the provided ring on **its own write completion**.
    ///
    /// Writes to `sink` are strictly serialized — one in flight at a time — so the
    /// byte stream cannot be reordered (io_uring does not order independent SQEs);
    /// a large `len` therefore becomes a sequence of serialized buffer-sized
    /// writes. Shared-ring safety under fan-in is provided by the low-water
    /// reserve (`recv_segment_reserve`): when the ring runs low, arriving buffers
    /// are force-copied and their bids returned immediately, so a slow sink cannot
    /// deplete the shared ring and starve other connections.
    ///
    /// Resolves to `Ok(bytes_forwarded)`. `bytes_forwarded == len` on success; a
    /// value `< len` means the peer closed (FIN) before `len` bytes arrived — the
    /// forward is truncated (any received-but-unwritten tail is dropped as the
    /// connection unwinds). After the forward the delivery domain is reset to the
    /// default, and any bytes received *past* `len` are preserved at the front of
    /// the accumulator for the next `with_data`/`with_bytes` read.
    ///
    /// # Sink ownership
    ///
    /// [`SinkFd`] borrows the target descriptor for the forward's lifetime (it is
    /// **not** a bare `RawFd` the caller can close mid-forward — that would be a
    /// use-after-close, since an in-flight write would land on a recycled fd). The
    /// returned future borrows the `SinkFd`, so the borrow checker keeps the fd
    /// open until the forward resolves.
    ///
    /// # NVMe / `O_DIRECT`
    ///
    /// Not supported for zero-copy: provided buffers are mid-ring, unaligned, and
    /// unregistered, so `O_DIRECT`'s alignment contract cannot be met without an
    /// intermediate aligned copy (which defeats the purpose). [`SinkFd::file`]
    /// **rejects** an `O_DIRECT` descriptor with [`io::ErrorKind::InvalidInput`].
    ///
    /// io_uring only — Mode A is backed by the provided-buffer ring.
    #[cfg(has_io_uring)]
    pub fn forward_to<'a>(&self, sink: &'a SinkFd<'a>, len: usize) -> ForwardToFuture<'a> {
        with_state(|driver, _executor| {
            driver.recv_domain[self.conn_index as usize] =
                crate::recv::domain::RecvDomain::Segmented;
            // Mark this connection as a forwarder so the recv handler applies the
            // `forward_hold_cap` throttle (bounded hold + TCP-window backpressure)
            // — distinguishing it from a pure Mode B segment reader that also uses
            // the `Segmented` domain but is drained by a `SegmentReader`.
            driver.forward_recv_active[self.conn_index as usize] = true;
        });
        ForwardToFuture {
            conn_index: self.conn_index,
            generation: self.generation,
            len: len as u64,
            forwarded: 0,
            sink_fd: sink.fd,
            is_file: sink.is_file,
            _sink: PhantomData,
        }
    }

    /// Install a recv sink so that CQE data is written directly to the
    /// target buffer instead of the per-connection accumulator.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `target` points to writable memory of at
    /// least `len` bytes, and that the memory remains valid until
    /// [`take_recv_sink()`](Self::take_recv_sink) is called. In practice this
    /// is guaranteed because ringline is single-threaded: the task sets the sink,
    /// yields, and the CQE handler (same thread) writes to it before the task
    /// resumes and clears the sink.
    pub unsafe fn set_recv_sink(&self, target: *mut u8, len: usize) {
        with_state(|_driver, executor| {
            executor.recv_sinks[self.conn_index as usize] = Some(crate::runtime::RecvSink {
                ptr: target,
                cap: len,
                pos: 0,
            });
        });
    }

    /// Remove the recv sink and return the number of bytes written to it.
    /// Returns 0 if no sink was active.
    pub fn take_recv_sink(&self) -> usize {
        with_state(
            |_driver, executor| match executor.recv_sinks[self.conn_index as usize].take() {
                Some(sink) => sink.pos,
                None => 0,
            },
        )
    }

    /// Returns a future that becomes ready when any recv data is available
    /// (in the accumulator, recv sink, or connection is closed).
    ///
    /// Use this with [`set_recv_sink()`](Self::set_recv_sink) to wait for
    /// direct-to-buffer writes without processing accumulator data.
    pub fn recv_ready(&self) -> RecvReadyFuture {
        RecvReadyFuture {
            conn_index: self.conn_index,
            generation: self.generation,
        }
    }

    /// Non-blocking accumulator access. Calls `f` with buffered data if any,
    /// returning `Some(result)`. Returns `None` if the accumulator is empty.
    pub fn try_with_data<F: FnOnce(&[u8]) -> ParseResult>(&self, f: F) -> Option<ParseResult> {
        with_state(|driver, _executor| {
            let data = driver.accumulators.data(self.conn_index);
            if data.is_empty() {
                return None;
            }
            let result = f(data);
            if let ParseResult::Consumed(consumed) = result {
                driver.accumulators.consume(self.conn_index, consumed);
            }
            Some(result)
        })
    }

    // ── Send (synchronous / fire-and-forget) ─────────────────────────

    /// Fire-and-forget send: copies data into the send pool and submits the SQE.
    /// One copy, no heap allocation, no future.
    ///
    /// This is the hot-path send for cache responses. The CQE is handled
    /// internally by the executor (resource cleanup + send queue advancement).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the send copy pool is exhausted or the submission queue is full.
    ///
    /// For backpressure-aware sending, use [`send()`](Self::send) instead.
    pub fn send_nowait(&self, data: &[u8]) -> io::Result<()> {
        with_state(|driver, _| {
            let mut ctx = driver.make_ctx();
            ctx.send(self.token(), data)
        })
    }

    /// Forward the current pending recv buffer as a zero-copy send.
    ///
    /// This is intended for use inside a `with_data` closure. If the connection
    /// has a pending recv buffer (from the zero-copy recv path), it is taken and
    /// used as the send source directly — no copy into the send pool. The recv
    /// buffer is replenished when the send completes.
    ///
    /// Falls back to `send_nowait(data)` if there is no pending recv buffer
    /// (e.g., data came from the accumulator or a TLS connection).
    ///
    /// Only works for plaintext connections; TLS connections always copy.
    /// On the mio backend, this always uses the copy path.
    pub fn forward_recv_buf(&self, data: &[u8]) -> io::Result<()> {
        with_state(|driver, _| {
            #[cfg_attr(not(has_io_uring), allow(unused_variables))]
            let conn_index = self.conn_index;

            #[cfg(has_io_uring)]
            {
                // Check for pending recv buffer.
                if let Some(pending) = driver.pending_recv_bufs[conn_index as usize].take() {
                    // Verify the data pointer matches the pending buffer (sanity check).
                    let pending_ptr = pending.ptr;
                    let data_ptr = data.as_ptr();
                    if data_ptr == pending_ptr && data.len() == pending.len as usize {
                        // Submit send SQE from the recv buffer. The bid is replenished
                        // on send completion via handle_send_recv_buf.
                        // Payload: bid only (low 16 bits). The remaining byte count is
                        // tracked in driver.send_recv_buf_remaining[conn_index] so that
                        // buffer sizes > u16::MAX (e.g. 65536 B) are supported without
                        // truncation in the CQE user_data payload.
                        let payload = pending.bid as u32;
                        // Store original and remaining lengths for partial-send tracking.
                        driver.send_recv_buf_original_lens[conn_index as usize] = pending.len;
                        driver.send_recv_buf_remaining[conn_index as usize] = pending.len;
                        let user_data = crate::completion::UserData::encode(
                            crate::completion::OpTag::SendRecvBuf,
                            conn_index,
                            payload,
                        );
                        let entry = io_uring::opcode::Send::new(
                            io_uring::types::Fixed(conn_index),
                            pending_ptr,
                            pending.len,
                        )
                        .flags(crate::completion::STREAM_SEND_FLAGS)
                        .build()
                        .user_data(user_data.raw());

                        let built = crate::handler::BuiltSend {
                            entry,
                            pool_slot: u16::MAX,
                            #[cfg(has_io_uring)]
                            slab_idx: u16::MAX,
                            total_len: pending.len,
                        };

                        let result = driver.submit_or_queue_send(conn_index, built);
                        if result.is_err() {
                            // Submit failed — replenish the recv buffer.
                            driver.pending_replenish.push(pending.bid);
                        }
                        return result;
                    }

                    // Pointer mismatch �� put it back and fall through to copy path.
                    driver.pending_recv_bufs[conn_index as usize] = Some(pending);
                }
            }

            // No pending recv buffer (or mio backend) — fall back to copy send.
            let mut ctx = driver.make_ctx();
            ctx.send(self.token(), data)
        })
    }

    /// Run a direct-echo event loop for this connection (io_uring only).
    ///
    /// Instead of the standard task-driven approach where each recv CQE wakes
    /// the owning task, which then calls `with_data` → `forward_recv_buf`, this
    /// mode sets a flag on the connection that tells `handle_recv_multi` to
    /// submit the echo SQE directly from the CQE handler — bypassing the
    /// `collect_wakeups` → `poll_ready_tasks` roundtrip entirely.
    ///
    /// The returned future parks until the connection is closed, then resolves.
    /// Any data buffered before the flag was set is drained on the first poll.
    ///
    /// On the mio backend this degrades gracefully to the normal `with_data` /
    /// `forward_recv_buf` loop.
    #[cfg(has_io_uring)]
    pub fn run_direct_echo(&self) -> DirectEchoFuture {
        DirectEchoFuture {
            conn_index: self.conn_index,
            generation: self.generation,
            armed: false,
        }
    }

    /// Enable the zero-copy recv-forward path for this connection.
    ///
    /// Once enabled, incoming provided recv buffers are *held in place* (not
    /// copied into the accumulator) and can be echoed back zero-copy via
    /// [`forward_held`](Self::forward_held), which gathers all held buffers into
    /// one scatter-gather `sendmsg`. Intended for byte-pipe workloads (echo,
    /// proxy) where the handler does not parse the stream. While enabled,
    /// [`with_data`](Self::with_data) / [`with_bytes`](Self::with_bytes) will not
    /// observe data (it never reaches the accumulator).
    ///
    /// Backpressure is automatic: held buffer ids are not returned to the
    /// provided-buffer ring until their forward completes, so a slow peer
    /// naturally throttles recv (`ENOBUFS`) rather than growing memory.
    #[cfg(has_io_uring)]
    pub fn enable_recv_forward(&self) {
        with_state(|driver, _| {
            driver.recv_forward[self.conn_index as usize] = true;
        });
    }

    /// Forward all currently-held recv buffers back to the peer in one zero-copy
    /// scatter-gather `sendmsg` (up to `MAX_IOVECS` buffers), returning a
    /// [`SendFuture`] that resolves with the bytes sent. Requires
    /// [`enable_recv_forward`](Self::enable_recv_forward).
    ///
    /// Gate calls on [`recv_ready`](Self::recv_ready), which becomes ready when
    /// the hold is non-empty (or the connection closed). When the hold is empty
    /// (e.g. the connection closed), the returned future resolves to `0`.
    ///
    /// One forward is in flight per connection at a time (await the returned
    /// future before calling again); buffers received during the send accumulate
    /// in the hold and are picked up by the next call.
    #[cfg(has_io_uring)]
    pub fn forward_held(&self) -> io::Result<SendFuture> {
        with_state(|driver, executor| {
            let conn_index = self.conn_index;
            if driver.connections.generation(conn_index) != self.generation {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "stale connection",
                ));
            }

            let n = driver.recv_hold[conn_index as usize]
                .len()
                .min(crate::buffer::send_slab::MAX_IOVECS);

            // Nothing held (connection closed or spurious wake) — resolve to 0.
            if n == 0 {
                executor.io_results[conn_index as usize] = Some(IoResult::Send(Ok(0)));
                executor.owner_task[conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
                executor.send_waiters[conn_index as usize] = true;
                return Ok(SendFuture {
                    conn_index,
                    generation: self.generation,
                });
            }

            // Gather iovecs over the held provided-buffer memory (PendingRecvBuf
            // is Copy, so each index read releases the recv_hold borrow).
            let mut iovecs = [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; crate::buffer::send_slab::MAX_IOVECS];
            let mut bids = [0u16; crate::buffer::send_slab::MAX_IOVECS];
            let mut total: u32 = 0;
            for i in 0..n {
                let p = driver.recv_hold[conn_index as usize][i];
                iovecs[i] = libc::iovec {
                    iov_base: p.ptr as *mut libc::c_void,
                    iov_len: p.len as usize,
                };
                bids[i] = p.bid;
                total += p.len;
            }

            let (slab_idx, msg_ptr) = driver
                .send_slab
                .allocate_recv_forward(conn_index, &iovecs[..n], &bids[..n], total)
                .ok_or_else(|| io::Error::other("send slab exhausted"))?;

            match driver
                .ring
                .submit_send_recv_bufs_coalesced(conn_index, msg_ptr, slab_idx)
            {
                Ok(()) => {
                    // Buffers are now owned by the slab entry (bids replenished on
                    // completion) — remove them from the hold.
                    for _ in 0..n {
                        driver.recv_hold[conn_index as usize].pop_front();
                    }
                    driver.send_queues[conn_index as usize].in_flight = true;
                    executor.owner_task[conn_index as usize] =
                        Some(CURRENT_TASK_ID.with(|c| c.get()));
                    executor.send_waiters[conn_index as usize] = true;
                    Ok(SendFuture {
                        conn_index,
                        generation: self.generation,
                    })
                }
                Err(e) => {
                    // Submission failed — release the slab entry; buffers stay in
                    // the hold (bids un-replenished, still valid) for a later retry.
                    driver.send_slab.release(slab_idx);
                    Err(e)
                }
            }
        })
    }

    /// Enable the recv-forward path for this connection.
    ///
    /// mio fallback: no-op. There is no provided-buffer ring to hold, so recv
    /// data flows through the accumulator as usual and
    /// [`forward_held`](Self::forward_held) drains + copy-sends it (no
    /// zero-copy). Keeps the API portable across backends.
    #[cfg(not(has_io_uring))]
    pub fn enable_recv_forward(&self) {}

    /// Forward currently-buffered recv data back to the peer.
    ///
    /// mio fallback: drains the accumulator and copy-sends it, returning a
    /// [`SendFuture`] that resolves with the bytes sent (`0` when empty). Gate
    /// calls on [`recv_ready`](Self::recv_ready) as on the io_uring backend.
    #[cfg(not(has_io_uring))]
    pub fn forward_held(&self) -> io::Result<SendFuture> {
        with_state(|driver, executor| {
            let conn_index = self.conn_index;
            if driver.connections.generation(conn_index) != self.generation {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "stale connection",
                ));
            }
            // Copy out the accumulated bytes (releases the accumulator borrow so
            // we can take a DriverCtx), then consume them once the send is queued.
            let data = driver.accumulators.data(conn_index).to_vec();
            if data.is_empty() {
                executor.io_results[conn_index as usize] = Some(IoResult::Send(Ok(0)));
                executor.owner_task[conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
                executor.send_waiters[conn_index as usize] = true;
                return Ok(SendFuture {
                    conn_index,
                    generation: self.generation,
                });
            }
            let mut ctx = driver.make_ctx();
            ctx.send(self.token(), &data)?;
            // Deliver the completion only when the bytes actually reach the
            // socket — completing at queue time reported success for data
            // that was never written and swallowed write errors.
            ctx.mark_last_send_awaited(conn_index);
            driver.accumulators.consume(conn_index, data.len());
            executor.owner_task[conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.send_waiters[conn_index as usize] = true;
            Ok(SendFuture {
                conn_index,
                generation: self.generation,
            })
        })
    }

    /// Begin building a scatter-gather send with mixed copy + zero-copy guard parts.
    ///
    /// This mirrors `DriverCtx::send_parts()` — use `.copy(data)` for copied parts
    /// and `.guard(guard)` for zero-copy parts backed by `SendGuard`. Call `.submit()`
    /// to submit the SQE. Fire-and-forget: no future returned.
    #[cfg(has_io_uring)]
    pub fn send_parts(&self) -> AsyncSendBuilder {
        AsyncSendBuilder {
            token: self.token(),
        }
    }

    // ── Send (awaitable) ─────────────────────────────────────────────

    /// Send data and await completion. Copies data into the send pool, submits
    /// the SQE eagerly, then returns a future that resolves with the total bytes
    /// sent (or error).
    ///
    /// Use this when you need backpressure or send completion notification.
    /// For fire-and-forget sending, use [`send_nowait()`](Self::send_nowait).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the send copy pool is exhausted or the submission queue is full.
    pub fn send(&self, data: &[u8]) -> io::Result<SendFuture> {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            ctx.send(self.token(), data)?;
            // On mio, the send is buffered — mark it awaitable so the event
            // loop delivers wake_send when the bytes actually reach the
            // socket (not at queue time, which reported success for data
            // that was never written and swallowed write errors).
            #[cfg(not(has_io_uring))]
            ctx.mark_last_send_awaited(self.conn_index);
            executor.owner_task[self.conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.send_waiters[self.conn_index as usize] = true;
            Ok(SendFuture {
                conn_index: self.conn_index,
                generation: self.generation,
            })
        })
    }

    // ── Connect ──────────────────────────────────────────────────────

    /// Initiate an outbound TCP connection and await the result.
    ///
    /// Returns a new `ConnCtx` for the peer connection on success.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<ConnectFuture> {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            let token = ctx
                .connect(addr)
                .map_err(io::Error::other::<crate::error::Error>)?;
            let calling_task = CURRENT_TASK_ID.with(|c| c.get());
            executor.owner_task[token.index as usize] = Some(calling_task);
            executor.connect_waiters[token.index as usize] = true;
            Ok(ConnectFuture {
                conn_index: token.index,
                generation: token.generation,
            })
        })
    }

    /// Initiate an outbound TCP connection with a timeout and await the result.
    pub fn connect_with_timeout(
        &self,
        addr: SocketAddr,
        timeout_ms: u64,
    ) -> io::Result<ConnectFuture> {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            let token = ctx
                .connect_with_timeout(addr, timeout_ms)
                .map_err(io::Error::other::<crate::error::Error>)?;
            let calling_task = CURRENT_TASK_ID.with(|c| c.get());
            executor.owner_task[token.index as usize] = Some(calling_task);
            executor.connect_waiters[token.index as usize] = true;
            Ok(ConnectFuture {
                conn_index: token.index,
                generation: token.generation,
            })
        })
    }

    /// Initiate an outbound Unix domain socket connection and await the result.
    ///
    /// Returns a new `ConnCtx` for the peer connection on success.
    pub fn connect_unix(&self, path: impl AsRef<std::path::Path>) -> io::Result<ConnectFuture> {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            let token = ctx
                .connect_unix(path.as_ref())
                .map_err(io::Error::other::<crate::error::Error>)?;
            let calling_task = CURRENT_TASK_ID.with(|c| c.get());
            executor.owner_task[token.index as usize] = Some(calling_task);
            executor.connect_waiters[token.index as usize] = true;
            Ok(ConnectFuture {
                conn_index: token.index,
                generation: token.generation,
            })
        })
    }

    // ── Send chain ────────────────────────────────────────────────────

    /// Build an IO_LINK chained send on this connection (fire-and-forget).
    ///
    /// The closure receives a [`SendChainBuilder`](crate::handler::SendChainBuilder) for
    /// constructing linked SQEs. Call `.copy()`, `.parts()...add()` to add SQEs,
    /// then `.finish()` to submit the chain.
    ///
    /// For backpressure-aware chained sending, use [`send_chain()`](Self::send_chain).
    #[cfg(has_io_uring)]
    pub fn send_chain_nowait<F, R>(&self, f: F) -> R
    where
        F: FnOnce(crate::handler::SendChainBuilder<'_, '_>) -> R,
    {
        with_state(|driver, _| {
            let mut ctx = driver.make_ctx();
            let token = ConnToken::new(self.conn_index, self.generation);
            let builder = ctx.send_chain(token);
            f(builder)
        })
    }

    /// Build an IO_LINK chained send and await completion.
    ///
    /// The closure receives a [`SendChainBuilder`](crate::handler::SendChainBuilder) for
    /// constructing linked SQEs. Call `.copy()`, `.parts()...add()` to build the
    /// chain, then `.finish()` to submit it. Returns a [`SendFuture`] that
    /// resolves with total bytes sent.
    ///
    /// For fire-and-forget chained sending, use [`send_chain_nowait()`](Self::send_chain_nowait).
    #[cfg(has_io_uring)]
    pub fn send_chain<F>(&self, f: F) -> io::Result<SendFuture>
    where
        F: FnOnce(crate::handler::SendChainBuilder<'_, '_>) -> io::Result<()>,
    {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            let token = ConnToken::new(self.conn_index, self.generation);
            let builder = ctx.send_chain(token);
            f(builder)?;
            executor.owner_task[self.conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.send_waiters[self.conn_index as usize] = true;
            Ok(SendFuture {
                conn_index: self.conn_index,
                generation: self.generation,
            })
        })
    }

    // ── Shutdown / cancel ─────────────────────────────────────────────

    /// Shutdown the write side of the connection (half-close).
    ///
    /// Sends a TCP FIN to the peer. The read side remains open.
    pub fn shutdown_write(&self) {
        with_state(|driver, _| {
            let mut ctx = driver.make_ctx();
            ctx.shutdown_write(self.token());
        })
    }

    /// Cancel pending I/O operations on this connection.
    pub fn cancel(&self) -> io::Result<()> {
        with_state(|driver, _| {
            let mut ctx = driver.make_ctx();
            ctx.cancel(self.token())
        })
    }

    /// Request graceful shutdown of the worker event loop.
    pub fn request_shutdown(&self) {
        with_state(|driver, _| {
            let mut ctx = driver.make_ctx();
            ctx.request_shutdown();
        })
    }

    // ── TLS ──────────────────────────────────────────────────────────

    /// Query TLS session info for this connection.
    pub fn tls_info(&self) -> Option<crate::tls::TlsInfo> {
        with_state(|driver, _| {
            let ctx = driver.make_ctx();
            ctx.tls_info(self.token())
        })
    }

    /// Whether this TLS connection's EOF was a truncation: the peer's TCP
    /// FIN arrived *without* a preceding close_notify alert. A truncated
    /// stream may be missing data the peer (or an active attacker injecting
    /// the FIN) cut off — length- or delimiter-framed protocols should treat
    /// it as an error rather than a clean end-of-stream.
    ///
    /// Check this when a recv (`with_data`/`with_bytes`) reports EOF. The
    /// flag is readable while the connection slot is still held (the wake
    /// that delivered the EOF and this check run before the slot is
    /// recycled); it returns `false` for plaintext connections, for clean
    /// TLS shutdowns, and once the slot has been reused.
    pub fn eof_truncated(&self) -> bool {
        with_state(|driver, _| {
            driver
                .connections
                .get(self.conn_index)
                .filter(|c| c.generation == self.generation)
                .map(|c| c.eof_truncated)
                .unwrap_or(false)
        })
    }

    /// Initiate an outbound TLS connection and await the result.
    pub fn connect_tls(&self, addr: SocketAddr, server_name: &str) -> io::Result<ConnectFuture> {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            let token = ctx
                .connect_tls(addr, server_name)
                .map_err(io::Error::other::<crate::error::Error>)?;
            let calling_task = CURRENT_TASK_ID.with(|c| c.get());
            executor.owner_task[token.index as usize] = Some(calling_task);
            executor.connect_waiters[token.index as usize] = true;
            Ok(ConnectFuture {
                conn_index: token.index,
                generation: token.generation,
            })
        })
    }

    /// Initiate an outbound TLS connection with a timeout and await the result.
    pub fn connect_tls_with_timeout(
        &self,
        addr: SocketAddr,
        server_name: &str,
        timeout_ms: u64,
    ) -> io::Result<ConnectFuture> {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            let token = ctx
                .connect_tls_with_timeout(addr, server_name, timeout_ms)
                .map_err(io::Error::other::<crate::error::Error>)?;
            let calling_task = CURRENT_TASK_ID.with(|c| c.get());
            executor.owner_task[token.index as usize] = Some(calling_task);
            executor.connect_waiters[token.index as usize] = true;
            Ok(ConnectFuture {
                conn_index: token.index,
                generation: token.generation,
            })
        })
    }

    // ── Timestamps ─────────────────────────────────────────────────

    /// Returns the most recent kernel RX software timestamp as nanoseconds
    /// since epoch (`CLOCK_REALTIME`), or 0 if no timestamp has been received.
    ///
    /// Updated each time a `RecvMsgMulti` completion delivers an
    /// `SCM_TIMESTAMPING` cmsg. Only available when the `timestamps` feature
    /// is enabled and `Config::timestamps(true)` is set.
    #[cfg(feature = "timestamps")]
    pub fn recv_timestamp(&self) -> u64 {
        with_state(|driver, _| {
            driver
                .connections
                .get(self.conn_index)
                .map(|cs| cs.recv_timestamp_ns)
                .unwrap_or(0)
        })
    }

    // ── Close / metadata ─────────────────────────────────────────────

    /// Close this connection.
    pub fn close(&self) {
        let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
        if opt_non_null.is_none() {
            return;
        }
        let mut non_null = opt_non_null.unwrap();
        let state = unsafe { non_null.as_mut() };
        let driver = unsafe { &mut *state.driver.as_mut() };
        driver.close_connection(self.conn_index);
    }

    /// Access peer address.
    pub fn peer_addr(&self) -> Option<crate::connection::PeerAddr> {
        with_state(|driver, _| {
            let conn = driver.connections.get(self.conn_index)?;
            if conn.generation != self.generation {
                return None;
            }
            conn.peer_addr.clone()
        })
    }

    /// Check if this connection is outbound (initiated via connect).
    pub fn is_outbound(&self) -> bool {
        with_state(|driver, _| {
            driver
                .connections
                .get(self.conn_index)
                .map(|cs| cs.generation == self.generation && cs.outbound)
                .unwrap_or(false)
        })
    }

    /// Whether this handle still refers to a live, usable connection.
    ///
    /// Returns `false` if the slot has been released and its generation bumped
    /// (a stale handle after slot reuse) **or** if the connection is closing
    /// (`close()` was called — `recv_mode` is `Closed` — even before the Close
    /// CQE has released the slot). Because a proactive `close()` flips the mode
    /// synchronously, this is a *deterministic* poison probe: a caller that owns
    /// a `ConnCtx` copy (e.g. a connection pool) can detect an
    /// undrained-`ValueStream` poison on the very next turn, without waiting for
    /// the Close CQE to bump the generation.
    ///
    /// A clean, fully-drained streaming read (which restores the default read
    /// path via `end_segments`) leaves the connection `Multi`/armed, so this
    /// returns `true` and the connection stays reusable.
    pub fn is_alive(&self) -> bool {
        with_state(|driver, _| {
            driver
                .connections
                .get(self.conn_index)
                .map(|cs| {
                    cs.generation == self.generation
                        && !matches!(cs.recv_mode, crate::connection::RecvMode::Closed)
                })
                .unwrap_or(false)
        })
    }
}

// ── AsyncSendBuilder ─────────────────────────────────────────────────

#[cfg(has_io_uring)]
/// Builder for scatter-gather sends in the async API.
///
/// Wraps `DriverCtx::send_parts()` — call `.copy()` and `.guard()` to add
/// parts, then `.submit()`. This is a synchronous builder; the send is
/// fire-and-forget (no future).
pub struct AsyncSendBuilder {
    token: ConnToken,
}

#[cfg(has_io_uring)]
impl AsyncSendBuilder {
    /// Build and submit the send by calling the provided closure with a
    /// `SendBuilder` from the `DriverCtx`.
    ///
    /// The closure receives the `SendBuilder` and should chain `.copy()` / `.guard()`
    /// calls then call `.submit()`.
    ///
    /// # Example
    /// ```no_run
    /// # fn example(conn: ringline::ConnCtx) -> std::io::Result<()> {
    /// conn.send_parts().build(|b| {
    ///     b.copy(b"header").submit()
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn build<F>(self, f: F) -> io::Result<()>
    where
        F: FnOnce(crate::handler::SendBuilder<'_, '_>) -> io::Result<()>,
    {
        with_state(|driver, _| {
            let mut ctx = driver.make_ctx();
            let builder = ctx.send_parts(self.token);
            f(builder)
        })
    }

    /// Submit a scatter-gather send from pre-classified `SendPart`s.
    ///
    /// This avoids the lifetime constraints of the closure-based [`build()`](Self::build),
    /// allowing callers to mix copy and guard parts in a single SQE from borrowed data.
    ///
    /// All parts go into a single SQE. If the batch exceeds the internal
    /// iovec or guard limits, the entire batch is rejected with `Err`
    /// (nothing is sent, guard parts are dropped) — the call never consumes
    /// a prefix. On success, returns the number of parts submitted (always
    /// `parts.len()`); callers wanting larger batches should split them
    /// (the protocol clients cap at 4 guards / 9 iovecs per flush).
    pub fn submit_batch(self, parts: Vec<crate::handler::SendPart<'_>>) -> io::Result<usize> {
        use crate::handler::SendPart;
        with_state(|driver, _| {
            let mut ctx = driver.make_ctx();
            let mut builder = ctx.send_parts(self.token);
            let mut consumed = 0usize;
            for part in parts {
                match part {
                    SendPart::Copy(data) => {
                        builder = builder.copy(data);
                    }
                    SendPart::Guard(guard) => {
                        builder = builder.guard(guard);
                    }
                }
                consumed += 1;
            }
            if consumed == 0 {
                return Ok(0);
            }
            builder.submit()?;
            Ok(consumed)
        })
    }
}

// ── mio AsyncSendBuilder (copy-only fallback) ───────────────────────

#[cfg(not(has_io_uring))]
/// Builder for scatter-gather sends in the async API (mio fallback).
///
/// On the mio backend, all parts are copied into a single buffer and
/// sent as one operation. Zero-copy guards are consumed by copying their
/// data.
pub struct AsyncSendBuilder {
    token: ConnToken,
}

#[cfg(not(has_io_uring))]
impl ConnCtx {
    /// Begin building a scatter-gather send.
    ///
    /// On the mio backend, this degrades to copy-only sends.
    pub fn send_parts(&self) -> AsyncSendBuilder {
        AsyncSendBuilder {
            token: self.token(),
        }
    }
}

#[cfg(not(has_io_uring))]
impl AsyncSendBuilder {
    /// Build and submit the send by concatenating all copy parts.
    pub fn build<F>(self, f: F) -> io::Result<()>
    where
        F: FnOnce(MioSendBuilder<'_>) -> io::Result<()>,
    {
        with_state(|driver, _| {
            let mut buf = Vec::new();
            let builder = MioSendBuilder { buf: &mut buf };
            f(builder)?;
            if !buf.is_empty() {
                let mut ctx = driver.make_ctx();
                ctx.send(self.token, &buf)?;
            }
            Ok(())
        })
    }

    /// Submit a scatter-gather send from pre-classified `SendPart`s.
    ///
    /// On the mio backend, guard data is copied (no kernel zero-copy).
    pub fn submit_batch(self, parts: Vec<crate::handler::SendPart<'_>>) -> io::Result<usize> {
        use crate::handler::SendPart;
        with_state(|driver, _| {
            let mut buf = Vec::new();
            let mut consumed = 0usize;
            for part in &parts {
                match part {
                    SendPart::Copy(data) => buf.extend_from_slice(data),
                    SendPart::Guard(guard) => {
                        let (ptr, len) = guard.as_ptr_len();
                        let data = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
                        buf.extend_from_slice(data);
                    }
                }
                consumed += 1;
            }
            if !buf.is_empty() {
                let mut ctx = driver.make_ctx();
                ctx.send(self.token, &buf)?;
            }
            Ok(consumed)
        })
    }
}

/// Mio send builder — accumulates parts into a single buffer.
#[cfg(not(has_io_uring))]
pub struct MioSendBuilder<'a> {
    buf: &'a mut Vec<u8>,
}

#[cfg(not(has_io_uring))]
impl<'a> MioSendBuilder<'a> {
    /// Add a copy part.
    pub fn copy(self, data: &[u8]) -> Self {
        self.buf.extend_from_slice(data);
        self
    }

    /// Add a guard part (copies the data on mio backend).
    pub fn guard(self, guard: crate::guard::GuardBox) -> Self {
        let (ptr, len) = guard.as_ptr_len();
        let data = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
        self.buf.extend_from_slice(data);
        self
    }

    /// Submit the accumulated send.
    pub fn submit(self) -> io::Result<()> {
        Ok(())
    }
}

// ── WithDataFuture ───────────────────────────────────────────────────

/// Future returned by [`ConnCtx::with_data`].
pub struct WithDataFuture<F> {
    conn_index: u32,
    /// Generation snapshot from `ConnCtx` at construction time. Compared on
    /// poll / drop so a stale future that survived a slot-reuse cycle
    /// doesn't read or wake the *new* connection's state.
    generation: u32,
    f: Option<F>,
}

impl<F: FnMut(&[u8]) -> ParseResult + Unpin> Future for WithDataFuture<F> {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        with_state(|driver, executor| {
            // Reject stale futures that survived a slot-reuse cycle. A
            // standalone task holding a `ConnCtx` whose underlying
            // connection has closed-then-been-reused would otherwise read
            // the new connection's accumulator. Return Poll::Ready(0)
            // (EOF-equivalent) so the caller's read loop terminates
            // gracefully.
            if driver.connections.generation(self.conn_index) != self.generation {
                self.f.take();
                return Poll::Ready(0);
            }
            // Zero-copy fast path: check pending recv buffer before accumulator.
            // Only available on io_uring where kernel-provided buffers are used.
            #[cfg(has_io_uring)]
            if driver.pending_recv_bufs[self.conn_index as usize].is_some() {
                // Non-merging emptiness check — `data()` would merge a held
                // frozen remainder just to answer a boolean.
                let acc_empty = driver.accumulators.is_empty(self.conn_index);
                if acc_empty {
                    // Borrow the kernel buffer in-place — no copy.
                    let pending = driver.pending_recv_bufs[self.conn_index as usize].unwrap();
                    let data =
                        unsafe { std::slice::from_raw_parts(pending.ptr, pending.len as usize) };
                    let f = self.f.as_mut().expect("WithDataFuture polled after Ready");
                    let result = f(data);
                    match result {
                        ParseResult::Consumed(consumed) if consumed > 0 => {
                            // The closure may have called forward_recv_buf(), which
                            // takes the pending slot. Only replenish if still present.
                            if let Some(pending) =
                                driver.pending_recv_bufs[self.conn_index as usize].take()
                            {
                                if consumed < pending.len as usize {
                                    // Partial consume: copy remainder to accumulator.
                                    let remainder = unsafe {
                                        std::slice::from_raw_parts(
                                            pending.ptr.add(consumed),
                                            pending.len as usize - consumed,
                                        )
                                    };
                                    driver.accumulators.append(self.conn_index, remainder);
                                }
                                driver.pending_replenish.push(pending.bid);
                            }
                            self.f.take();
                            return Poll::Ready(consumed);
                        }
                        _ => {
                            // NeedMore / Consumed(0): flush pending to accumulator
                            // and fall through to the existing accumulator path.
                            if let Some(pending) =
                                driver.pending_recv_bufs[self.conn_index as usize].take()
                            {
                                let pending_data = unsafe {
                                    std::slice::from_raw_parts(pending.ptr, pending.len as usize)
                                };
                                driver.accumulators.append(self.conn_index, pending_data);
                                driver.pending_replenish.push(pending.bid);
                            }
                            // Fall through to accumulator path below.
                        }
                    }
                } else {
                    // Accumulator has data AND there's a pending buffer.
                    // Flush pending to accumulator (prepend — it arrived first).
                    let pending = driver.pending_recv_bufs[self.conn_index as usize]
                        .take()
                        .unwrap();
                    let pending_data =
                        unsafe { std::slice::from_raw_parts(pending.ptr, pending.len as usize) };
                    driver.accumulators.prepend(self.conn_index, pending_data);
                    driver.pending_replenish.push(pending.bid);
                    // Fall through to accumulator path below.
                }
            }

            let data = driver.accumulators.data(self.conn_index);
            if data.is_empty() {
                // Check if the connection has been closed — return 0 (EOF)
                // so the caller can detect disconnection.
                let is_closed = driver
                    .connections
                    .get(self.conn_index)
                    .map(|c| matches!(c.recv_mode, crate::connection::RecvMode::Closed))
                    .unwrap_or(true); // connection already released
                if is_closed {
                    let f = self.f.as_mut().expect("WithDataFuture polled after Ready");
                    let result = f(&[]);
                    self.f.take();
                    return Poll::Ready(match result {
                        ParseResult::Consumed(n) => n,
                        ParseResult::NeedMore | ParseResult::NeedAtLeast(_) => 0,
                    });
                }

                // No data available — register as recv waiter and park.
                executor.owner_task[self.conn_index as usize] =
                    Some(CURRENT_TASK_ID.with(|c| c.get()));
                executor.recv_waiters[self.conn_index as usize] = true;
                return Poll::Pending;
            }

            // Data available — call closure immediately (zero-overhead hot path).
            let f = self.f.as_mut().expect("WithDataFuture polled after Ready");
            let result = f(data);
            match result {
                ParseResult::Consumed(consumed) if consumed > 0 => {
                    driver.accumulators.consume(self.conn_index, consumed);
                    self.f.take();
                    return Poll::Ready(consumed);
                }
                _ => {}
            }
            // A length-prefixed parser announced the remainder — reserve
            // once so the arriving chunks append without regrowth.
            if let ParseResult::NeedAtLeast(additional) = result {
                driver.accumulators.reserve(self.conn_index, additional);
            }

            // NeedMore or Consumed(0) on non-empty data: incomplete parse.
            // Check if the connection is closed (EOF with leftover partial data).
            let is_closed = driver
                .connections
                .get(self.conn_index)
                .map(|c| matches!(c.recv_mode, crate::connection::RecvMode::Closed))
                .unwrap_or(true);
            if is_closed {
                self.f.take();
                return Poll::Ready(0);
            }

            // Connection still open — wait for more data before retrying.
            executor.owner_task[self.conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.recv_waiters[self.conn_index as usize] = true;
            Poll::Pending
        })
    }
}

// ── WithBytesFuture ──────────────────────────────────────────────────

/// Future returned by [`ConnCtx::with_bytes`].
pub struct WithBytesFuture<F> {
    conn_index: u32,
    /// See `WithDataFuture` for the role of `generation`.
    generation: u32,
    f: Option<F>,
}

impl<F: FnMut(Bytes) -> ParseResult + Unpin> Future for WithBytesFuture<F> {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        with_state(|driver, executor| {
            // Single connection-table lookup: extract generation + closed
            // state together so the rest of poll needs no further accesses.
            // `get()` returns None when the slot is inactive (connection
            // already released), which we treat the same as a stale token.
            let (conn_generation, is_closed) = match driver.connections.get(self.conn_index) {
                None => {
                    // Slot is inactive — definitely stale.
                    self.f.take();
                    return Poll::Ready(0);
                }
                Some(conn) => (
                    conn.generation,
                    matches!(conn.recv_mode, crate::connection::RecvMode::Closed),
                ),
            };
            if conn_generation != self.generation {
                // Slot was recycled for a different connection.
                self.f.take();
                return Poll::Ready(0);
            }

            // Flush any pending zero-copy recv buffer to accumulator so
            // take_frozen() will include it (io_uring only).
            #[cfg(has_io_uring)]
            if let Some(pending) = driver.pending_recv_bufs[self.conn_index as usize].take() {
                let pending_data =
                    unsafe { std::slice::from_raw_parts(pending.ptr, pending.len as usize) };
                driver.accumulators.append(self.conn_index, pending_data);
                driver.pending_replenish.push(pending.bid);
            }

            let data = driver.accumulators.data(self.conn_index);
            if data.is_empty() {
                // No data yet — check closed state from the lookup above.
                if is_closed {
                    let f = self.f.as_mut().expect("WithBytesFuture polled after Ready");
                    let result = f(Bytes::new());
                    self.f.take();
                    return Poll::Ready(match result {
                        ParseResult::Consumed(n) => n,
                        ParseResult::NeedMore | ParseResult::NeedAtLeast(_) => 0,
                    });
                }

                executor.owner_task[self.conn_index as usize] =
                    Some(CURRENT_TASK_ID.with(|c| c.get()));
                executor.recv_waiters[self.conn_index as usize] = true;
                return Poll::Pending;
            }

            // Detach accumulator as frozen Bytes (O(1), tail capacity retained).
            let frozen = driver.accumulators.take_frozen(self.conn_index);
            let len = frozen.len();

            let f = self.f.as_mut().expect("WithBytesFuture polled after Ready");
            // clone is an O(1) Bytes refcount bump; the original `frozen` is retained for the prepend calls below.
            let result = f(frozen.clone());

            match result {
                ParseResult::Consumed(consumed) if consumed > 0 => {
                    // Put back the unconsumed remainder (if any) as a
                    // refcounted slice handoff.
                    if consumed < len {
                        driver
                            .accumulators
                            .put_back(self.conn_index, frozen.slice(consumed..));
                    }
                    self.f.take();
                    return Poll::Ready(consumed);
                }
                _ => {}
            }

            // NeedMore or Consumed(0) on non-empty data: incomplete parse.
            // Put everything back and use closed state from the lookup above.
            driver.accumulators.put_back(self.conn_index, frozen);
            // A length-prefixed parser announced the remainder — record the
            // reserve target so the unfreeze merge allocates once at full
            // size when the next chunks arrive.
            if let ParseResult::NeedAtLeast(additional) = result {
                driver.accumulators.reserve(self.conn_index, additional);
            }

            if is_closed {
                self.f.take();
                return Poll::Ready(0);
            }

            executor.owner_task[self.conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.recv_waiters[self.conn_index as usize] = true;
            Poll::Pending
        })
    }
}

// ── Segmented recv (Mode B — Borrow) ─────────────────────────────────

/// An async **lending iterator** over a connection's received provided buffers,
/// obtained from [`ConnCtx::segments`] (io_uring only).
///
/// Each [`next`](Self::next) yields one zero-copy [`RecvSegment`] borrowing the
/// reader exclusively (`&mut self`). Because the segment holds that `&mut`
/// borrow, calling `next` again while a segment is alive is a **compile error** —
/// so "drop the previous segment before pulling the next" and "the segment cannot
/// escape the runtime" are enforced by the borrow checker, not by convention.
/// This is why it cannot be a `futures::Stream`/`for` loop (inherent to lending
/// iterators). Typical use:
///
/// ```ignore
/// let mut r = conn.segments();
/// while let Some(seg) = r.next().await? {
///     process(&seg);        // seg: Deref<Target = [u8]>, held across the await below
///     downstream.send(&seg).await?;
/// }                          // loop ends at EOF (peer FIN, hold drained)
/// ```
///
/// The reader is itself `!Send`/`!Clone`/non-`Copy`: it is bound to the worker
/// thread's driver and to the connection it was created for.
#[cfg(has_io_uring)]
pub struct SegmentReader<'a> {
    conn_index: u32,
    /// Generation snapshot from the originating `ConnCtx`; a stale reader (slot
    /// reused) resolves `next` to `Ok(None)` (EOF) rather than reading the new
    /// occupant's buffers.
    generation: u32,
    /// Ties the reader to the `ConnCtx` borrow it came from.
    _borrow: PhantomData<&'a ()>,
    /// `!Send` + `!Sync`: a provided buffer is thread-affine and driver-released.
    _not_send: PhantomData<*const ()>,
}

#[cfg(has_io_uring)]
impl SegmentReader<'_> {
    /// Await the next received segment.
    ///
    /// Resolves to:
    /// - `Ok(Some(seg))` — a held provided buffer, handed out as a zero-copy
    ///   [`RecvSegment`] pinned until it is dropped.
    /// - `Ok(None)` — end of stream: the recv side is closed (peer FIN) and no
    ///   held buffers remain, or the connection slot was reused (stale reader).
    ///
    /// Parks when no buffer is held and the connection is still open, resuming
    /// when the recv completion handler holds a buffer and calls `wake_recv`
    /// (the same waiter mechanism as [`ConnCtx::with_data`]).
    // This is a *lending* iterator: `next` borrows `&mut self` and yields a
    // segment tied to that borrow, which `std::iter::Iterator` cannot express.
    // The name is deliberate (it reads as an iterator to callers); it genuinely
    // cannot implement the trait.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> SegmentNext<'_> {
        SegmentNext {
            conn_index: self.conn_index,
            generation: self.generation,
            _borrow: PhantomData,
        }
    }
}

#[cfg(has_io_uring)]
impl Drop for SegmentReader<'_> {
    fn drop(&mut self) {
        // Auto-settle on drop: if the reader is dropped while the connection is
        // still in the segmented domain (i.e. `end_segments()` was not called —
        // the common pattern drops the reader first, then calls `end_segments`),
        // drain any still-held segments into the accumulator and restore the
        // default `with_data`/`with_bytes` read path. Without this, held bytes
        // would be stranded and a subsequent ordinary read would hang (arriving
        // buffers keep being held as segments). A live `RecvSegment` borrows the
        // reader `&mut`, so by here the pin slot is empty (the segment dropped
        // first) — only the hold needs draining.
        //
        // `try_with_state`: a reader owned by a parked task can be dropped in the
        // *unguarded* `drain_completions` during teardown (`CURRENT_DRIVER` is
        // `None`), where there is nothing to settle — the connection is closing
        // and `handle_close` reclaims the held bids. On accumulator overflow while
        // settling, poison (close) the connection.
        try_with_state(|driver, _executor| {
            if driver.connections.generation(self.conn_index) != self.generation {
                return; // slot reused — nothing of ours to settle.
            }
            if driver.recv_domain[self.conn_index as usize]
                != crate::recv::domain::RecvDomain::Segmented
            {
                return; // already settled (end_segments() was called).
            }
            if !driver.settle_forward_end(self.conn_index) {
                driver.close_connection(self.conn_index);
            }
        });
    }
}

/// Future returned by [`SegmentReader::next`]. Borrows the reader (`&mut`) for
/// `'a`; the yielded [`RecvSegment`] shares that borrow, keeping the reader
/// exclusively locked while the segment is alive.
#[cfg(has_io_uring)]
pub struct SegmentNext<'a> {
    conn_index: u32,
    generation: u32,
    _borrow: PhantomData<&'a mut ()>,
}

#[cfg(has_io_uring)]
impl<'a> Future for SegmentNext<'a> {
    type Output = io::Result<Option<RecvSegment<'a>>>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_state(|driver, executor| {
            let conn = self.conn_index;
            let idx = conn as usize;

            // Stale reader (slot closed-then-reused for a different connection):
            // surface EOF so the caller's `while let Some` loop terminates rather
            // than reading the new occupant's held buffers.
            if driver.connections.generation(conn) != self.generation {
                return Poll::Ready(Ok(None));
            }

            // One live pinned segment per connection at a time. A *single* reader
            // is kept sound by `&mut self` (a live `RecvSegment` borrows the reader
            // exclusively, so `next` cannot be called again). But `segments()`
            // takes `&self`, so a second concurrent `SegmentReader` on the same
            // connection could reach here with the pin slot already occupied —
            // overwriting it would orphan the first segment's bid (a ring leak).
            // Refuse rather than leak: surface the misuse as an error instead of
            // handing out a second pinned segment.
            if driver.segment_pinned[idx].is_some() {
                return Poll::Ready(Err(io::Error::other(
                    "a segmented-recv segment is already checked out — only one \
                     SegmentReader/RecvSegment may be live per connection at a time",
                )));
            }

            // A held buffer is available: hand it to a `RecvSegment`.
            if let Some(held) = driver.segment_hold[idx].pop_front() {
                match held {
                    crate::backend::HeldRecvBuf::Pinned { bid, len } => {
                        // Zero-copy: check the bid out into the (guaranteed-empty)
                        // pin slot; the segment's Drop / into_owned releases it
                        // exactly once.
                        driver.segment_pinned[idx] =
                            Some(crate::backend::HeldRecvBuf::Pinned { bid, len });
                        return Poll::Ready(Ok(Some(RecvSegment {
                            conn_index: conn,
                            generation: self.generation,
                            backing: SegBacking::Pinned { bid, len },
                            _borrow: PhantomData,
                            _not_send: PhantomData,
                        })));
                    }
                    crate::backend::HeldRecvBuf::Owned(bytes) => {
                        // Force-copied at delivery: no pin slot involved; the
                        // owned segment's Drop is a no-op.
                        return Poll::Ready(Ok(Some(RecvSegment {
                            conn_index: conn,
                            generation: self.generation,
                            backing: SegBacking::Owned(bytes),
                            _borrow: PhantomData,
                            _not_send: PhantomData,
                        })));
                    }
                }
            }

            // Hold empty. If the recv side is closed (peer FIN), this is EOF.
            let is_closed = driver
                .connections
                .get(conn)
                .map(|c| matches!(c.recv_mode, crate::connection::RecvMode::Closed))
                .unwrap_or(true);
            if is_closed {
                return Poll::Ready(Ok(None));
            }

            // Open and nothing held yet — park as a recv waiter. `handle_recv_multi`
            // pushes into `segment_hold` and calls `wake_recv` on the next arrival.
            executor.owner_task[idx] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.recv_waiters[idx] = true;
            Poll::Pending
        })
    }
}

/// A single received provided buffer, borrowed zero-copy from the ring.
///
/// Yielded by [`SegmentReader::next`]. Derefs to the received bytes (`&[u8]`).
/// The backing buffer stays pinned in the provided-buffer ring for the segment's
/// whole lifetime — its bid is **not** replenished until the segment is dropped
/// (or the connection closes). By construction it is:
///
/// - **`!Send` / `!Sync`** (`PhantomData<*const ()>`): a provided buffer is
///   thread-affine and released only by its owning worker's driver.
/// - **`!Clone` / non-`Copy`**: exactly one release per bid; a clone would
///   double-replenish or leak.
/// - **borrow-scoped** (`&'a mut` the reader): it cannot escape the async scope,
///   so the "holdable ⇒ copied-at-delivery" invariant holds structurally. To
///   *keep* the bytes past the scope, [`into_owned()`](Self::into_owned) copies
///   (Mode C); there is no zero-copy retain.
///
/// # Drop / release soundness
///
/// A segment's `Drop` does **not** always run inside an executor poll: a parked
/// task holding a segment can be dropped by a close/EOF CQE handled in the
/// *unguarded* `drain_completions` (where `CURRENT_DRIVER` is `None`). So `Drop`
/// uses [`try_with_state`]: when the driver is reachable it replenishes the bid
/// and clears the driver's pin slot; when it is not, it no-ops and
/// `close_connection` reclaims the pinned bid instead. The driver pin slot
/// (`segment_pinned[conn]`) is the single-release discriminant — whoever `take()`s
/// it first replenishes, so the bid is returned exactly once across the
/// drop/close race and any slot-reuse.
#[cfg(has_io_uring)]
pub struct RecvSegment<'a> {
    conn_index: u32,
    generation: u32,
    /// Where this segment's bytes live: a pinned provided buffer (zero-copy,
    /// released via the driver pin slot on drop/`into_owned`) or an owned copy
    /// (force-copied at delivery under ring pressure; its bid was already
    /// replenished, so drop/`into_owned` touch no ring state).
    backing: SegBacking,
    _borrow: PhantomData<&'a mut ()>,
    _not_send: PhantomData<*const ()>,
}

/// Backing store for a [`RecvSegment`] — one of the two segmented-recv
/// deliveries (see `docs/segmented-recv-design.md`, "Backpressure and ring
/// safety").
#[cfg(has_io_uring)]
enum SegBacking {
    /// A provided buffer pinned in the ring. The bid is released exactly once via
    /// the driver pin slot (`segment_pinned[conn]`) on drop or `into_owned`.
    Pinned { bid: u16, len: u32 },
    /// An owned copy of the received bytes. The bid was replenished at delivery,
    /// so this segment pins nothing: drop is a no-op and `into_owned` just hands
    /// back the bytes.
    Owned(Bytes),
}

#[cfg(has_io_uring)]
impl RecvSegment<'_> {
    /// The number of received bytes in this segment.
    pub fn len(&self) -> usize {
        match &self.backing {
            SegBacking::Pinned { len, .. } => *len as usize,
            SegBacking::Owned(b) => b.len(),
        }
    }

    /// Whether this segment carries no bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Copy this segment's bytes into an owned heap [`Bytes`] and release the
    /// pinned provided buffer (Mode C — "Own"; see `docs/segmented-recv-design.md`).
    /// Consumes the segment.
    ///
    /// This is a **real copy**, not an alias: the returned `Bytes` owns freshly
    /// allocated heap memory with no release-on-drop tie to the ring, so it is
    /// freely `Send`, `Clone`, and holdable past the connection's lifetime —
    /// unlike the borrowed [`RecvSegment`], which cannot escape the runtime. This
    /// is the same single copy the accumulator path pays today. (Deliberately
    /// *not* `Bytes::from_owner` over the provided buffer — that would keep the
    /// bid pinned, which Mode C exists to avoid.)
    ///
    /// # Release-exactly-once
    ///
    /// The copy is taken first (INC ordering: copy-before-replenish, no await
    /// between), then this method `take()`s the driver pin slot
    /// (`segment_pinned[conn]`, the single-release discriminant — the **same**
    /// guard as [`RecvSegment::drop`]) and queues the bid for replenish. Because
    /// `self` is consumed by value, its `Drop` runs at the end of this method and
    /// finds the pin slot already `None` (or holding a different bid, after a
    /// slot reuse) → it no-ops. So the bid is replenished **exactly once**: here.
    // Consumes `self` by design: the conversion *is* the release (it `take()`s the
    // pin slot), so a `&self` signature would be unsound — the borrow would leave
    // the bid pinned and let `Drop` also try to release it. Named `into_owned`
    // (not `to_owned`) because it consumes: `into_*` is the by-value convention.
    pub fn into_owned(self) -> Bytes {
        match &self.backing {
            SegBacking::Owned(b) => {
                // Already owned (force-copied at delivery): no pin to release. The
                // clone is an O(1) refcount bump; `self`'s `Drop` no-ops for Owned.
                b.clone()
            }
            SegBacking::Pinned { bid, len } => {
                let (bid, len) = (*bid, *len);
                // Copy before replenish (INC template), no await between.
                let owned = with_state(|driver, _executor| {
                    let (ptr, _) = driver.provided_bufs.get_buffer(bid);
                    // SAFETY: the bid is pinned (not yet replenished) and `len`
                    // bytes were received into its backing buffer.
                    let slice = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
                    Bytes::copy_from_slice(slice)
                });
                // Release the pin — identical guard to `Drop`; `take()` is the
                // single release. Uses `try_with_state` for the same reason `Drop`
                // does (this normally runs in-poll, but staying symmetric keeps it
                // panic-free).
                try_with_state(|driver, _executor| {
                    if driver.connections.generation(self.conn_index) != self.generation {
                        return;
                    }
                    match driver.segment_pinned[self.conn_index as usize] {
                        Some(crate::backend::HeldRecvBuf::Pinned {
                            bid: pinned_bid, ..
                        }) if pinned_bid == bid => {
                            driver.segment_pinned[self.conn_index as usize] = None;
                            driver.pending_replenish.push(bid);
                        }
                        _ => {}
                    }
                });
                owned
            }
        }
        // `self` drops here: for Pinned the pin slot is now `None` (or holds a
        // different bid after a slot reuse) → `Drop` no-ops; for Owned, `Drop` is
        // inherently a no-op. Exactly one replenish either way.
    }
}

#[cfg(has_io_uring)]
impl Deref for RecvSegment<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self.backing {
            // Owned bytes (force-copied at delivery) — borrow them directly.
            SegBacking::Owned(b) => &b[..],
            SegBacking::Pinned { bid, len } => {
                // The buffer is pinned in the ring (bid not replenished) for this
                // segment's lifetime, so the pointer is valid and `len <= buf cap`.
                let ptr = with_state(|driver, _executor| driver.provided_bufs.get_buffer(*bid).0);
                // SAFETY: `ptr` points into the provided-buffer backing for `bid`,
                // which stays pinned until this segment drops; `len` bytes were
                // received into it. The returned slice borrows `self`, so it
                // cannot outlive the pin.
                unsafe { std::slice::from_raw_parts(ptr, *len as usize) }
            }
        }
    }
}

#[cfg(has_io_uring)]
impl Drop for RecvSegment<'_> {
    fn drop(&mut self) {
        // Owned segments (force-copied at delivery) pin nothing: the bid was
        // already replenished, so there is nothing to release — just drop the
        // owned `Bytes`. Only the pinned backing touches the ring.
        let bid = match &self.backing {
            SegBacking::Pinned { bid, .. } => *bid,
            SegBacking::Owned(_) => return,
        };
        // May run inside a poll (driver reachable) or in the unguarded
        // `drain_completions` while a parked task is torn down (driver == None).
        try_with_state(|driver, _executor| {
            // Slot recycled to a new connection: the bid was already replenished
            // by the old occupant's `close_connection`. Do nothing.
            if driver.connections.generation(self.conn_index) != self.generation {
                return;
            }
            // Reclaim only if the pin slot still holds *this* segment's buffer.
            // If `close_connection` already drained it (None), or it somehow holds
            // a different bid, do not replenish — that avoids the drop/close
            // double-replenish. `take()` makes this the single release.
            match driver.segment_pinned[self.conn_index as usize] {
                Some(crate::backend::HeldRecvBuf::Pinned {
                    bid: pinned_bid, ..
                }) if pinned_bid == bid => {
                    driver.segment_pinned[self.conn_index as usize] = None;
                    driver.pending_replenish.push(bid);
                }
                _ => {}
            }
        });
        // When `try_with_state` returned `None` (unguarded drop), the bid stays
        // pinned and `close_connection` replenishes it — still exactly once.
    }
}

// ── Segmented recv (Mode C — Own, copy-at-delivery) ──────────────────

/// Future returned by [`ConnCtx::recv_owned_segment`]. Copies each arriving
/// provided buffer into an owned [`Bytes`] and replenishes its bid at delivery,
/// so it never pins the ring (the ring-safe owned-delivery path).
#[cfg(has_io_uring)]
pub struct RecvOwnedSegment {
    conn_index: u32,
    generation: u32,
}

#[cfg(has_io_uring)]
impl Future for RecvOwnedSegment {
    type Output = io::Result<Option<Bytes>>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_state(|driver, executor| {
            let conn = self.conn_index;
            let idx = conn as usize;

            // Stale handle (slot closed-then-reused): surface EOF rather than
            // reading the new occupant's held buffers.
            if driver.connections.generation(conn) != self.generation {
                return Poll::Ready(Ok(None));
            }

            // A held buffer is available: COPY it into an owned `Bytes` and
            // replenish the bid immediately. The copy is the release — this path
            // never touches `segment_pinned`, so it can never deplete the ring by
            // holding. (INC ordering: copy-before-replenish, no await between.)
            if let Some(held) = driver.segment_hold[idx].pop_front() {
                match held {
                    crate::backend::HeldRecvBuf::Pinned { bid, len } => {
                        let (ptr, _) = driver.provided_bufs.get_buffer(bid);
                        // SAFETY: `bid` is held (not yet replenished), and `len`
                        // bytes were received into its backing buffer. The slice is
                        // consumed by the copy below before `driver` is mutated.
                        let slice = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
                        let owned = Bytes::copy_from_slice(slice);
                        driver.pending_replenish.push(bid);
                        return Poll::Ready(Ok(Some(owned)));
                    }
                    crate::backend::HeldRecvBuf::Owned(bytes) => {
                        // Already owned (force-copied at delivery): its bid was
                        // replenished at delivery, so just hand back the bytes.
                        return Poll::Ready(Ok(Some(bytes)));
                    }
                }
            }

            // Hold empty. If the recv side is closed (peer FIN), this is EOF.
            let is_closed = driver
                .connections
                .get(conn)
                .map(|c| matches!(c.recv_mode, crate::connection::RecvMode::Closed))
                .unwrap_or(true);
            if is_closed {
                return Poll::Ready(Ok(None));
            }

            // Open and nothing held yet — park as a recv waiter, resumed by
            // `handle_recv_multi`'s `wake_recv` on the next arrival.
            executor.owner_task[idx] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.recv_waiters[idx] = true;
            Poll::Pending
        })
    }
}

// ── Segmented recv (Mode A — Forward to an fd) ───────────────────────

/// A non-raw sink descriptor for [`ConnCtx::forward_to`] (Mode A — "Forward";
/// see `docs/segmented-recv-design.md`).
///
/// Wraps a [`BorrowedFd`] so the descriptor is guaranteed open for the forward's
/// lifetime — deliberately **not** a bare [`RawFd`] the caller could close
/// mid-forward, which would be a use-after-close (an in-flight write landing on a
/// recycled fd; generation guards ring *slots*, not caller fds). The
/// [`forward_to`](ConnCtx::forward_to) future borrows the `SinkFd`, so the borrow
/// checker keeps the fd alive until the forward resolves.
///
/// Two sink kinds are supported for **zero userspace copy**:
/// - [`socket`](Self::socket): a stream socket (zero-copy proxy).
/// - [`file`](Self::file): a **buffered** regular file (`pwrite` at offset).
///
/// `O_DIRECT` / NVMe sinks are **not** supported for zero-copy (provided buffers
/// are unaligned) and are rejected by [`file`](Self::file).
#[cfg(has_io_uring)]
#[derive(Debug)]
pub struct SinkFd<'a> {
    fd: RawFd,
    /// Seekable buffered file (`pwrite` at an advancing offset) vs. stream socket
    /// (`send` with `MSG_WAITALL`, no offset).
    is_file: bool,
    /// Ties this handle to the borrowed descriptor's lifetime.
    _borrow: PhantomData<BorrowedFd<'a>>,
}

#[cfg(has_io_uring)]
impl<'a> SinkFd<'a> {
    /// A **socket** sink (zero-copy proxy). Forwarded bytes are written with
    /// `send`(`MSG_WAITALL`), so the kernel retries short stream sends in place.
    pub fn socket(fd: BorrowedFd<'a>) -> Self {
        SinkFd {
            fd: fd.as_raw_fd(),
            is_file: false,
            _borrow: PhantomData,
        }
    }

    /// A **buffered file** sink. Forwarded bytes are written with `pwrite` at an
    /// offset that advances from 0 as bytes are forwarded; short writes resubmit
    /// the remainder at the advanced offset.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if the descriptor was opened with
    /// `O_DIRECT`: zero-copy forwarding requires buffered I/O because provided
    /// recv buffers are mid-ring, unaligned, and unregistered — `O_DIRECT`'s
    /// alignment contract cannot be met without an intermediate aligned copy,
    /// which defeats the purpose. Use a buffered file (or copy the bytes out via
    /// Mode C).
    pub fn file(fd: BorrowedFd<'a>) -> io::Result<Self> {
        let raw = fd.as_raw_fd();
        // Reject O_DIRECT: unaligned provided buffers cannot be its source.
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if flags & libc::O_DIRECT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "O_DIRECT sink is not supported for zero-copy forward (provided \
                 buffers are unaligned); use a buffered file",
            ));
        }
        Ok(SinkFd {
            fd: raw,
            is_file: true,
            _borrow: PhantomData,
        })
    }
}

/// Future returned by [`ConnCtx::forward_to`]. Drives the recv/write interleave
/// that forwards `len` received bytes to the sink, one serialized write at a
/// time. Borrows the [`SinkFd`] (`'a`) so the sink descriptor stays open for the
/// whole forward.
#[cfg(has_io_uring)]
pub struct ForwardToFuture<'a> {
    conn_index: u32,
    generation: u32,
    /// Total bytes to forward.
    len: u64,
    /// Bytes whose write has completed.
    forwarded: u64,
    /// Sink descriptor (borrowed via `SinkFd` for `'a`).
    sink_fd: RawFd,
    /// File sink (`pwrite` at `forwarded` as the offset) vs. socket sink.
    is_file: bool,
    _sink: PhantomData<&'a SinkFd<'a>>,
}

#[cfg(has_io_uring)]
impl Future for ForwardToFuture<'_> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        with_state(|driver, executor| {
            let conn = me.conn_index;
            let idx = conn as usize;
            // Single pass: consume any completed write, then either park (write in
            // flight / awaiting data), resolve (done / EOF / stale), or start the
            // next write. No loop — each poll makes exactly one transition.
            {
                // Stale handle (slot closed/reused): the driver already released
                // any held/in-flight backing on close. Resolve with what we sent.
                if driver.connections.generation(conn) != me.generation {
                    return Poll::Ready(Ok(me.forwarded as usize));
                }

                // Consume a completed write result (set by `handle_forward_write`).
                if let Some(res) = driver.forward_done[idx].take() {
                    match res {
                        Ok(n) => me.forwarded += n as u64,
                        Err(errno) => {
                            let _ = driver.settle_forward_end(conn);
                            return Poll::Ready(Err(io::Error::from_raw_os_error(errno)));
                        }
                    }
                }

                // A write is still in flight — one at a time. Park for its CQE.
                if driver.forward_write[idx].is_some() {
                    executor.owner_task[idx] = Some(CURRENT_TASK_ID.with(|c| c.get()));
                    executor.recv_waiters[idx] = true;
                    return Poll::Pending;
                }

                // Done: forwarded the requested length. Settle carried-over bytes
                // (received past `len`) into the accumulator and reset the domain.
                if me.forwarded >= me.len {
                    if !driver.settle_forward_end(conn) {
                        executor.wake_recv(conn);
                        driver.close_connection(conn);
                        return Poll::Ready(Err(io::Error::other(
                            "recv accumulator overflow settling forward tail",
                        )));
                    }
                    return Poll::Ready(Ok(me.forwarded as usize));
                }

                // Start the next write from a held buffer, if any.
                if let Some(held) = driver.segment_hold[idx].pop_front() {
                    let remaining = me.len - me.forwarded;
                    // Determine the prefix to forward and stash any overshoot
                    // suffix (bytes past `len`) at the front of the accumulator.
                    let started = match held {
                        crate::backend::HeldRecvBuf::Pinned { bid, len } => {
                            if (len as u64) <= remaining {
                                Some((crate::backend::HeldRecvBuf::Pinned { bid, len }, len))
                            } else {
                                let chunk = remaining as u32;
                                let (ptr, _) = driver.provided_bufs.get_buffer(bid);
                                // SAFETY: `bid` is pinned (unreplenished) and `len`
                                // bytes were received into it; the suffix slice
                                // `[chunk..len]` is valid and copied out now.
                                let suffix = unsafe {
                                    std::slice::from_raw_parts(
                                        ptr.add(chunk as usize),
                                        (len - chunk) as usize,
                                    )
                                };
                                if !driver.accumulators.append(conn, suffix) {
                                    driver.pending_replenish.push(bid);
                                    executor.wake_recv(conn);
                                    driver.close_connection(conn);
                                    return Poll::Ready(Err(io::Error::other(
                                        "recv accumulator overflow stashing forward tail",
                                    )));
                                }
                                Some((
                                    crate::backend::HeldRecvBuf::Pinned { bid, len: chunk },
                                    chunk,
                                ))
                            }
                        }
                        crate::backend::HeldRecvBuf::Owned(bytes) => {
                            if (bytes.len() as u64) <= remaining {
                                let total = bytes.len() as u32;
                                Some((crate::backend::HeldRecvBuf::Owned(bytes), total))
                            } else {
                                let chunk = remaining as usize;
                                let overflow = !driver.accumulators.append(conn, &bytes[chunk..]);
                                if overflow {
                                    executor.wake_recv(conn);
                                    driver.close_connection(conn);
                                    return Poll::Ready(Err(io::Error::other(
                                        "recv accumulator overflow stashing forward tail",
                                    )));
                                }
                                let prefix = bytes.slice(0..chunk);
                                Some((crate::backend::HeldRecvBuf::Owned(prefix), chunk as u32))
                            }
                        }
                    };
                    let (backing, total) = started.expect("started set on both arms");
                    let base_offset = if me.is_file { me.forwarded } else { 0 };
                    match driver.start_forward_write(
                        conn,
                        backing,
                        total,
                        base_offset,
                        me.sink_fd,
                        me.is_file,
                    ) {
                        Ok(()) => {
                            executor.owner_task[idx] = Some(CURRENT_TASK_ID.with(|c| c.get()));
                            executor.recv_waiters[idx] = true;
                            return Poll::Pending;
                        }
                        Err(e) => {
                            let _ = driver.settle_forward_end(conn);
                            return Poll::Ready(Err(e));
                        }
                    }
                }

                // Hold empty. If the recv side is closed (peer FIN), the forward is
                // truncated — resolve with what we managed to forward.
                let is_closed = driver
                    .connections
                    .get(conn)
                    .map(|c| matches!(c.recv_mode, crate::connection::RecvMode::Closed))
                    .unwrap_or(true);
                if is_closed {
                    let _ = driver.settle_forward_end(conn);
                    return Poll::Ready(Ok(me.forwarded as usize));
                }

                // Open and nothing held — park for the next arrival (`wake_recv`).
                executor.owner_task[idx] = Some(CURRENT_TASK_ID.with(|c| c.get()));
                executor.recv_waiters[idx] = true;
                Poll::Pending
            }
        })
    }
}

// ── Segmented recv (Mode B — Borrow, B1 callback face) ───────────────

/// Bytes a [`with_segments`](ConnCtx::with_segments) callback consumed from the
/// **front** of the presented [`SegChain`].
///
/// This is deliberately *not* [`ParseResult`]: `with_segments` re-presents
/// carried-over bytes bounded by ring depth, so the only signal it needs is
/// "how many front bytes did you take". `SegConsumed(0)` is the "need a bigger
/// frame" case (the runtime gathers the chain to the accumulator and parks).
#[cfg(has_io_uring)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegConsumed(pub usize);

/// An ordered, borrowed view of a connection's buffered recv data, handed to a
/// [`with_segments`](ConnCtx::with_segments) callback.
///
/// The chain presents, **in order**, the accumulator remainder (if any) as the
/// leading segment followed by each held provided buffer in arrival order — so a
/// consumer walking [`iter`](Self::iter) never sees a later byte before an
/// earlier one. The slices borrow driver-internal memory for the callback's
/// duration only; they cannot escape (lifetime `'a`, like `with_data`).
#[cfg(has_io_uring)]
pub struct SegChain<'a> {
    /// Ordered borrowed segments: accumulator-first, then held buffers.
    segs: &'a [&'a [u8]],
}

#[cfg(has_io_uring)]
impl<'a> SegChain<'a> {
    /// Iterate the segments in order (accumulator remainder first, then held
    /// provided buffers in arrival order).
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.segs.iter().copied()
    }

    /// Total number of bytes across all segments.
    pub fn total_len(&self) -> usize {
        self.segs.iter().map(|s| s.len()).sum()
    }
}

/// By-value record of a held segment, captured during gather so the settle step
/// can run after the borrowed [`SegChain`] slices are released. `Pinned` carries
/// the bid+len so the remainder can be re-read from the (still-pinned) provided
/// buffer and the bid replenished; `Owned` carries the bytes themselves (an O(1)
/// refcount clone of the held copy), which keep the remainder alive after the
/// hold is cleared and need no replenish.
#[cfg(has_io_uring)]
enum SegSettle {
    Pinned { bid: u16, len: usize },
    Owned(Bytes),
}

/// Future returned by [`ConnCtx::with_segments`]. Resolves to the total bytes
/// the callback consumed this call (`Ok(0)` at EOF / on a stale handle).
#[cfg(has_io_uring)]
pub struct WithSegmentsFuture<F> {
    conn_index: u32,
    /// See `WithDataFuture` for the role of `generation`.
    generation: u32,
    f: Option<F>,
}

#[cfg(has_io_uring)]
impl<F: FnMut(&SegChain<'_>) -> SegConsumed + Unpin> Future for WithSegmentsFuture<F> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_state(|driver, executor| {
            let conn = self.conn_index;
            let idx = conn as usize;

            // Stale future (slot closed-then-reused): surface EOF so the caller's
            // read loop terminates rather than reading the new occupant's data.
            if driver.connections.generation(conn) != self.generation {
                self.f.take();
                return Poll::Ready(Ok(0));
            }

            // Flush a leftover single-buffer zero-copy recv (from a pre-`segments`
            // `with_data` cycle on this slot) into the accumulator so it is
            // presented accumulator-first, in order, ahead of any held buffers.
            if let Some(pending) = driver.pending_recv_bufs[idx].take() {
                let pending_data =
                    unsafe { std::slice::from_raw_parts(pending.ptr, pending.len as usize) };
                // Intermediate buffer shuffle: on breach the authoritative cap
                // check below (during settle) closes the connection.
                let _ = driver.accumulators.append(conn, pending_data);
                driver.pending_replenish.push(pending.bid);
            }

            // Nothing to present yet?
            let acc_empty = driver.accumulators.is_empty(conn);
            let hold_empty = driver.segment_hold[idx].is_empty();
            if acc_empty && hold_empty {
                let is_closed = driver
                    .connections
                    .get(conn)
                    .map(|c| matches!(c.recv_mode, crate::connection::RecvMode::Closed))
                    .unwrap_or(true);
                if is_closed {
                    self.f.take();
                    return Poll::Ready(Ok(0));
                }
                // Open + nothing buffered → park; `handle_recv_multi` holds a
                // buffer and calls `wake_recv` on the next arrival.
                executor.owner_task[idx] = Some(CURRENT_TASK_ID.with(|c| c.get()));
                executor.recv_waiters[idx] = true;
                return Poll::Pending;
            }

            // Gather the ordered borrowed view: accumulator remainder first, then
            // each held provided buffer. `held_meta` records (bid, len) by value so
            // the settle below can run after the borrows are released.
            let hold_len = driver.segment_hold[idx].len();
            let mut slices: Vec<&[u8]> = Vec::with_capacity(1 + hold_len);
            let mut held_meta: Vec<SegSettle> = Vec::with_capacity(hold_len);
            let acc = driver.accumulators.data(conn);
            let acc_len = acc.len();
            if !acc.is_empty() {
                slices.push(acc);
            }
            for held in &driver.segment_hold[idx] {
                match held {
                    crate::backend::HeldRecvBuf::Pinned { bid, len } => {
                        // `get_buffer` returns a raw pointer (no borrow tie), so the
                        // held slice does not alias the `provided_bufs` field borrow.
                        let (ptr, _) = driver.provided_bufs.get_buffer(*bid);
                        let s = unsafe { std::slice::from_raw_parts(ptr, *len as usize) };
                        slices.push(s);
                        held_meta.push(SegSettle::Pinned {
                            bid: *bid,
                            len: *len as usize,
                        });
                    }
                    crate::backend::HeldRecvBuf::Owned(b) => {
                        // Borrow the owned bytes for the chain; keep an O(1)
                        // refcount clone in `held_meta` so the remainder survives
                        // the `segment_hold.clear()` below (which drops the
                        // original).
                        slices.push(&b[..]);
                        held_meta.push(SegSettle::Owned(b.clone()));
                    }
                }
            }

            // Present to the callback; clamp its answer to what we actually showed.
            let consumed = {
                let chain = SegChain { segs: &slices };
                let total = chain.total_len();
                let f = self
                    .f
                    .as_mut()
                    .expect("WithSegmentsFuture polled after Ready");
                let SegConsumed(c) = f(&chain);
                c.min(total)
            };
            // Release the accumulator/provided-buffer borrows before mutating.
            drop(slices);

            // ── Settle ──────────────────────────────────────────────────────
            // All held buffers are being resolved this call, so drain the hold up
            // front; each bid is either replenished (consumed) or gathered +
            // replenished (remainder). Net: hold empties, un-consumed bytes live
            // contiguously at the accumulator front, in order.
            driver.segment_hold[idx].clear();

            let mut rem_n = consumed;
            // (1) Consume from the accumulator front (O(1) advance). If only part
            // of the accumulator was consumed, `rem_n` is now 0 and every held
            // buffer is untouched → gathered behind the accumulator remainder.
            let acc_consumed = rem_n.min(acc_len);
            driver.accumulators.consume(conn, acc_consumed);
            rem_n -= acc_consumed;

            // (2) Walk the held buffers in order.
            let mut overflowed = false;
            for meta in held_meta {
                match meta {
                    SegSettle::Pinned { bid, len } => {
                        if rem_n >= len {
                            // Wholly consumed by the callback — just replenish.
                            rem_n -= len;
                            driver.pending_replenish.push(bid);
                        } else {
                            // Partially consumed (or untouched, when rem_n == 0):
                            // copy the unconsumed remainder [rem_n..len] into the
                            // accumulator, in order, then replenish. Push the bid
                            // even on breach so it is never leaked.
                            if !overflowed {
                                let (ptr, _) = driver.provided_bufs.get_buffer(bid);
                                let remainder = unsafe {
                                    std::slice::from_raw_parts(ptr.add(rem_n), len - rem_n)
                                };
                                if !driver.accumulators.append(conn, remainder) {
                                    overflowed = true;
                                }
                            }
                            rem_n = 0;
                            driver.pending_replenish.push(bid);
                        }
                    }
                    SegSettle::Owned(bytes) => {
                        let len = bytes.len();
                        if rem_n >= len {
                            // Wholly consumed; the bid was replenished at delivery,
                            // so there is nothing to return to the ring here.
                            rem_n -= len;
                        } else {
                            // Copy the unconsumed remainder into the accumulator,
                            // in order. No replenish (owned copy holds no bid).
                            if !overflowed && !driver.accumulators.append(conn, &bytes[rem_n..]) {
                                overflowed = true;
                            }
                            rem_n = 0;
                        }
                    }
                }
            }

            if overflowed {
                // The under-drain gather streamed past `recv_accumulator_max`
                // (a misbehaving whole-frame consumer). Close rather than OOM;
                // held bids were already queued for replenish above.
                self.f.take();
                driver.close_connection(conn);
                return Poll::Ready(Ok(0));
            }

            if consumed > 0 {
                // Progress made — return it. Any carried-over remainder now sits at
                // the accumulator front and is re-presented next call.
                self.f.take();
                return Poll::Ready(Ok(consumed));
            }

            // consumed == 0 (need a bigger frame): everything is gathered to the
            // accumulator. Park for more data (bounded by recv_accumulator_max),
            // or surface EOF if the recv side already closed. This mirrors
            // `with_data`'s NeedMore idiom — never a busy loop.
            let is_closed = driver
                .connections
                .get(conn)
                .map(|c| matches!(c.recv_mode, crate::connection::RecvMode::Closed))
                .unwrap_or(true);
            if is_closed {
                self.f.take();
                return Poll::Ready(Ok(0));
            }
            executor.owner_task[idx] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.recv_waiters[idx] = true;
            Poll::Pending
        })
    }
}

// ── RecvReadyFuture ──────────────────────────────────────────────────

/// Future returned by [`ConnCtx::recv_ready`]. Resolves when:
/// 1. The recv sink has received data (`pos > 0`), OR
/// 2. The accumulator has data, OR
/// 3. The connection is closed.
pub struct RecvReadyFuture {
    conn_index: u32,
    /// See `WithDataFuture` for the role of `generation`.
    generation: u32,
}

impl Future for RecvReadyFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        with_state(|driver, executor| {
            // Stale future from a reused slot — synthesize "ready" so the
            // caller's loop sees the closure and terminates.
            if driver.connections.generation(self.conn_index) != self.generation {
                return Poll::Ready(());
            }
            // Check recv sink.
            if let Some(sink) = &executor.recv_sinks[self.conn_index as usize]
                && sink.pos > 0
            {
                return Poll::Ready(());
            }

            // Check accumulator.
            if !driver.accumulators.is_empty(self.conn_index) {
                return Poll::Ready(());
            }

            // Check the zero-copy recv-forward hold.
            #[cfg(has_io_uring)]
            if !driver.recv_hold[self.conn_index as usize].is_empty() {
                return Poll::Ready(());
            }

            // Check if the connection is closed.
            let is_closed = driver
                .connections
                .get(self.conn_index)
                .map(|c| matches!(c.recv_mode, crate::connection::RecvMode::Closed))
                .unwrap_or(true);
            if is_closed {
                return Poll::Ready(());
            }

            // Not ready — register as recv waiter and park.
            executor.owner_task[self.conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.recv_waiters[self.conn_index as usize] = true;
            Poll::Pending
        })
    }
}

// ── SendFuture ───────────────────────────────────────────────────────

/// Future that awaits send completion. The SQE was already submitted eagerly
/// by [`ConnCtx::send`] — this future only waits for the CQE result.
/// No data stored in the future. No allocation.
pub struct SendFuture {
    conn_index: u32,
    /// See `WithDataFuture` for the role of `generation`.
    generation: u32,
}

impl Future for SendFuture {
    type Output = io::Result<u32>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u32>> {
        with_state(|driver, executor| {
            // Slot-reuse safety: don't observe another connection's send
            // completions. Treat as ConnectionAborted.
            if driver.connections.generation(self.conn_index) != self.generation {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "connection closed",
                )));
            }
            match executor.io_results[self.conn_index as usize].take() {
                Some(IoResult::Send(result)) => Poll::Ready(result),
                _ => {
                    // Not ready yet — re-register waiter.
                    executor.owner_task[self.conn_index as usize] =
                        Some(CURRENT_TASK_ID.with(|c| c.get()));
                    executor.send_waiters[self.conn_index as usize] = true;
                    Poll::Pending
                }
            }
        })
    }
}

impl Drop for SendFuture {
    fn drop(&mut self) {
        let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
        if opt_non_null.is_none() {
            return;
        }
        let mut non_null = opt_non_null.unwrap();
        let state = unsafe { non_null.as_mut() };
        // Verify the connection slot still belongs to us before touching
        // its waiter flag. After a close/reuse cycle the same `conn_index`
        // can be a *different* connection with its own send_waiter, which
        // this Drop must not clear.
        #[cfg(has_io_uring)]
        let driver = unsafe { &mut *state.driver.as_mut() };
        #[cfg(not(has_io_uring))]
        let driver = unsafe { &mut *state.driver.as_mut() };
        if driver.connections.generation(self.conn_index) != self.generation {
            return;
        }
        let executor = unsafe { &mut *state.executor.as_mut() };
        executor.send_waiters[self.conn_index as usize] = false;
    }
}

// ── DirectEchoFuture ─────────────────────────────────────────────────

/// Future that drives a connection in direct-echo mode (io_uring only).
///
/// On first poll it sets `ConnectionState::direct_echo = true` so that all
/// subsequent recv CQEs are echoed directly from `handle_recv_multi` without
/// waking this task — eliminating the `collect_wakeups` → `poll_ready_tasks`
/// roundtrip on the hot path. Any data buffered before the flag was set is
/// drained on the first poll. The future parks until the connection is closed.
///
/// Created by [`ConnCtx::run_direct_echo`].
#[cfg(has_io_uring)]
pub struct DirectEchoFuture {
    conn_index: u32,
    generation: u32,
    armed: bool,
}

#[cfg(has_io_uring)]
impl Future for DirectEchoFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        with_state(|driver, executor| {
            // Generation check: the slot was reused while we were parked.
            if driver.connections.generation(self.conn_index) != self.generation {
                return Poll::Ready(());
            }

            if !self.armed {
                // Arm direct-echo mode on the connection.
                if let Some(cs) = driver.connections.get_mut(self.conn_index) {
                    cs.direct_echo = true;
                }
                self.armed = true;

                // Drain any buffer that arrived before the flag was set.
                // After this, all new bufs are echoed directly by handle_recv_multi.
                if let Some(pending) = driver.pending_recv_bufs[self.conn_index as usize].take() {
                    assert!(
                        pending.len <= 0xFFFF,
                        "DirectEchoFuture: data length {} exceeds 16-bit payload capacity",
                        pending.len,
                    );
                    let payload = (pending.bid as u32) | (pending.len << 16);
                    let ud = UserData::encode(OpTag::SendRecvBuf, self.conn_index, payload);
                    let entry = io_uring::opcode::Send::new(
                        io_uring::types::Fixed(self.conn_index),
                        pending.ptr,
                        pending.len,
                    )
                    .flags(crate::completion::STREAM_SEND_FLAGS)
                    .build()
                    .user_data(ud.raw());
                    let built = crate::handler::BuiltSend {
                        entry,
                        pool_slot: u16::MAX,
                        slab_idx: u16::MAX,
                        total_len: pending.len,
                    };
                    driver.send_recv_buf_original_lens[self.conn_index as usize] = pending.len;
                    if driver.submit_or_queue_send(self.conn_index, built).is_err() {
                        driver.pending_replenish.push(pending.bid);
                    }
                }
            }

            // Check if the connection is already closed.
            let is_closed = driver
                .connections
                .get(self.conn_index)
                .map(|c| matches!(c.recv_mode, crate::connection::RecvMode::Closed))
                .unwrap_or(true);

            if is_closed {
                return Poll::Ready(());
            }

            // Park until close — handle_recv_multi calls wake_recv when
            // result == 0 (EOF) or on error, which will wake this future.
            executor.owner_task[self.conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.recv_waiters[self.conn_index as usize] = true;
            Poll::Pending
        })
    }
}

#[cfg(has_io_uring)]
impl Drop for DirectEchoFuture {
    fn drop(&mut self) {
        let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
        if opt_non_null.is_none() {
            return;
        }
        let mut non_null = opt_non_null.unwrap();
        let state = unsafe { non_null.as_mut() };
        let driver = unsafe { &mut *state.driver.as_mut() };
        // Verify the slot still belongs to this connection before clearing
        // its waiter — a close/reuse cycle gives the slot a new generation.
        if driver.connections.generation(self.conn_index) != self.generation {
            return;
        }
        let executor = unsafe { &mut *state.executor.as_mut() };
        executor.recv_waiters[self.conn_index as usize] = false;
    }
}

// ── ConnectFuture ────────────────────────────────────────────────────

/// Future that awaits an outbound TCP connection. The connect SQE was submitted
/// eagerly by [`ConnCtx::connect`] — this future waits for the CQE result.
pub struct ConnectFuture {
    conn_index: u32,
    generation: u32,
}

impl Future for ConnectFuture {
    type Output = io::Result<ConnCtx>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<ConnCtx>> {
        with_state(|driver, executor| {
            // Slot-reuse safety.
            if driver.connections.generation(self.conn_index) != self.generation {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "connection closed",
                )));
            }
            match executor.io_results[self.conn_index as usize].take() {
                Some(IoResult::Connect(result)) => match result {
                    Ok(()) => Poll::Ready(Ok(ConnCtx::new(self.conn_index, self.generation))),
                    Err(e) => Poll::Ready(Err(e)),
                },
                _ => {
                    // Not ready yet — re-register waiter.
                    executor.connect_waiters[self.conn_index as usize] = true;
                    Poll::Pending
                }
            }
        })
    }
}

impl Drop for ConnectFuture {
    fn drop(&mut self) {
        let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
        if opt_non_null.is_none() {
            return;
        }
        let mut non_null = opt_non_null.unwrap();
        let state = unsafe { non_null.as_mut() };
        let driver = unsafe { &mut *state.driver.as_mut() };
        // Don't clear another connection's waiter flag if the slot has
        // already been reused. (Quite rare for connect: drop usually
        // happens before any close/reuse cycle on the same slot, but
        // belt-and-suspenders.)
        if driver.connections.generation(self.conn_index) != self.generation {
            return;
        }
        let executor = unsafe { &mut *state.executor.as_mut() };
        executor.connect_waiters[self.conn_index as usize] = false;
    }
}

// ── Sleep ────────────────────────────────────────────────────────────

/// Create a future that completes after the given duration.
///
/// Uses an io_uring timeout SQE internally — no busy-waiting, no timer
/// thread. The timer fires on the same worker thread as the calling task.
///
/// # Panics
///
/// Panics if the timer slot pool is exhausted, or if called outside the
/// ringline async executor.
pub fn sleep(duration: Duration) -> SleepFuture {
    SleepFuture {
        duration,
        timer_slot: None,
        generation: 0,
        absolute: None,
    }
}

/// Future returned by [`sleep()`] or [`sleep_until()`]. Completes after
/// the configured duration or at the given deadline.
pub struct SleepFuture {
    duration: Duration,
    /// None until first poll, then Some(slot_index).
    timer_slot: Option<u32>,
    /// Generation when the slot was allocated.
    generation: u16,
    /// If Some, this is an absolute timer (sleep_until).
    absolute: Option<Deadline>,
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        with_state(|driver, executor| {
            let _ = driver; // used only on io_uring path
            if let Some(slot) = self.timer_slot {
                // Already submitted — check if fired.
                if executor.timer_pool.is_fired(slot) {
                    executor.timer_pool.release(slot);
                    self.timer_slot = None;
                    return Poll::Ready(());
                }
                return Poll::Pending;
            }

            // First poll — allocate slot, fill timespec, submit SQE.
            let waker_id = CURRENT_TASK_ID.with(|c| c.get());
            let (slot, generation) = match executor.timer_pool.allocate(waker_id) {
                Some(pair) => pair,
                None => {
                    // Documented contract: the infallible variants panic on
                    // pool exhaustion (try_sleep/try_timeout are the fallible
                    // API). Completing immediately instead was far worse than
                    // the panic: every timeout() fired a spurious
                    // zero-duration Elapsed that cancelled healthy I/O, and a
                    // sleep() loop became a busy-spin keeping the pool
                    // exhausted.
                    panic!(
                        "timer slot pool exhausted ({} slots) — raise Config::timer_slots or use try_sleep()/try_timeout()",
                        executor.timer_pool.capacity()
                    );
                }
            };

            #[cfg(has_io_uring)]
            {
                let payload = TimerSlotPool::encode_payload(slot, generation);
                let ud = UserData::encode(OpTag::Timer, 0, payload);

                let submit_result = if let Some(deadline) = self.absolute {
                    let ts_ptr =
                        executor
                            .timer_pool
                            .set_absolute(slot, deadline.secs, deadline.nsecs);
                    driver.ring.submit_timeout_abs(ts_ptr, ud)
                } else {
                    let ts_ptr = executor.timer_pool.set_relative(slot, self.duration);
                    driver.ring.submit_timeout(ts_ptr, ud)
                };

                if let Err(e) = submit_result {
                    executor.timer_pool.release(slot);
                    // SQ full at timer-arm time. Completing immediately would
                    // fire a spurious zero-duration timeout; panicking matches
                    // the documented infallible contract.
                    panic!(
                        "timer SQE submission failed: {e} — use try_sleep()/try_timeout() for fallible arming"
                    );
                }
            }

            #[cfg(not(has_io_uring))]
            {
                // Mio backend: store deadline; the event loop polls timer expiry.
                if let Some(deadline) = self.absolute {
                    executor
                        .timer_pool
                        .set_absolute(slot, deadline.secs, deadline.nsecs);
                } else {
                    executor.timer_pool.set_relative(slot, self.duration);
                }
            }

            self.timer_slot = Some(slot);
            self.generation = generation;
            Poll::Pending
        })
    }
}

impl Drop for SleepFuture {
    fn drop(&mut self) {
        if let Some(slot) = self.timer_slot {
            // Timer was submitted but not yet fired — try to cancel it.
            //
            // If `CURRENT_DRIVER` is unset, the future is being dropped
            // outside an active executor poll. This happens during normal
            // worker teardown (the slab is dropped after the event loop
            // exits, so `CURRENT_DRIVER` is no longer installed) and we
            // can't do anything useful here — the io_uring instance is
            // gone too. The timer slot is leaked from this worker's pool
            // but the pool itself is being dropped, so no real leak.
            let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
            if opt_non_null.is_none() {
                return;
            }
            let mut non_null = opt_non_null.unwrap();
            let state = unsafe { non_null.as_mut() };
            #[cfg(has_io_uring)]
            let driver = unsafe { &mut *state.driver.as_mut() };
            let executor = unsafe { &mut *state.executor.as_mut() };

            if !executor.timer_pool.is_fired(slot) {
                #[cfg(has_io_uring)]
                {
                    let payload = TimerSlotPool::encode_payload(slot, self.generation);
                    let target_ud = UserData::encode(OpTag::Timer, 0, payload);
                    // Best effort cancel; timer fires harmlessly if already expired.
                    let _ = driver.ring.submit_async_cancel(target_ud.raw(), 0);
                }
            }
            // Slot released regardless — stale timer CQE detected via generation.
            executor.timer_pool.release(slot);
        }
    }
}

/// Create a sleep future, returning an error if the timer pool is exhausted.
///
/// Unlike [`sleep()`] which panics on pool exhaustion, this returns
/// `Err(TimerExhausted)` so callers can handle capacity limits gracefully.
///
/// The timer slot is allocated eagerly (at call time, not on first poll).
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn try_sleep(duration: Duration) -> Result<SleepFuture, TimerExhausted> {
    with_state(|driver, executor| {
        let _ = driver; // used only on io_uring path
        let waker_id = CURRENT_TASK_ID.with(|c| c.get());
        let (slot, generation) = executor.timer_pool.allocate(waker_id).ok_or_else(|| {
            crate::metrics::POOL.increment(crate::metrics::pool::TIMER_EXHAUSTED);
            TimerExhausted
        })?;

        #[cfg(has_io_uring)]
        {
            let payload = TimerSlotPool::encode_payload(slot, generation);
            let ud = UserData::encode(OpTag::Timer, 0, payload);
            let ts_ptr = executor.timer_pool.set_relative(slot, duration);

            if let Err(_e) = driver.ring.submit_timeout(ts_ptr, ud) {
                executor.timer_pool.release(slot);
                // SQE submission failure — complete immediately (same as sleep()).
                return Ok(SleepFuture {
                    duration,
                    timer_slot: None,
                    generation: 0,
                    absolute: None,
                });
            }
        }

        #[cfg(not(has_io_uring))]
        {
            executor.timer_pool.set_relative(slot, duration);
        }

        Ok(SleepFuture {
            duration,
            timer_slot: Some(slot),
            generation,
            absolute: None,
        })
    })
}

// ── Deadline (absolute timer support) ─────────────────────────────────

/// A monotonic clock deadline for use with absolute timers.
///
/// Created via [`Deadline::after()`] or [`Deadline::now()`].
/// Uses `CLOCK_MONOTONIC` to match io_uring's default clock.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Deadline {
    pub(crate) secs: u64,
    pub(crate) nsecs: u32,
}

impl Deadline {
    /// Capture the current monotonic time.
    pub fn now() -> Self {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // Safety: clock_gettime with CLOCK_MONOTONIC is safe.
        unsafe {
            libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
        }
        Deadline {
            secs: ts.tv_sec as u64,
            nsecs: ts.tv_nsec as u32,
        }
    }

    /// Create a deadline `duration` from now.
    pub fn after(duration: Duration) -> Self {
        let now = Self::now();
        let mut secs = now.secs + duration.as_secs();
        let mut nsecs = now.nsecs + duration.subsec_nanos();
        if nsecs >= 1_000_000_000 {
            nsecs -= 1_000_000_000;
            secs += 1;
        }
        Deadline { secs, nsecs }
    }

    /// Duration remaining until this deadline (saturates at zero).
    pub fn remaining(&self) -> Duration {
        let now = Self::now();
        if now.secs > self.secs || (now.secs == self.secs && now.nsecs >= self.nsecs) {
            return Duration::ZERO;
        }
        let mut secs = self.secs - now.secs;
        let nsecs = if self.nsecs >= now.nsecs {
            self.nsecs - now.nsecs
        } else {
            secs -= 1;
            1_000_000_000 + self.nsecs - now.nsecs
        };
        Duration::new(secs, nsecs)
    }
}

/// Create a future that completes at the given absolute deadline.
///
/// Uses io_uring's `TIMEOUT_ABS` flag with `CLOCK_MONOTONIC` for
/// precise deadline-based timing without accumulated drift.
///
/// # Panics
///
/// Panics if the timer slot pool is exhausted, or if called outside the
/// ringline async executor.
pub fn sleep_until(deadline: Deadline) -> SleepFuture {
    SleepFuture {
        duration: Duration::ZERO, // unused for absolute timers
        timer_slot: None,
        generation: 0,
        absolute: Some(deadline),
    }
}

/// Create an absolute-deadline sleep, returning an error if the timer pool is exhausted.
///
/// Unlike [`sleep_until()`] which panics on pool exhaustion, this returns
/// `Err(TimerExhausted)` so callers can handle capacity limits gracefully.
///
/// The timer slot is allocated eagerly (at call time, not on first poll).
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn try_sleep_until(deadline: Deadline) -> Result<SleepFuture, TimerExhausted> {
    with_state(|driver, executor| {
        let _ = driver; // used only on io_uring path
        let waker_id = CURRENT_TASK_ID.with(|c| c.get());
        let (slot, generation) = executor.timer_pool.allocate(waker_id).ok_or_else(|| {
            crate::metrics::POOL.increment(crate::metrics::pool::TIMER_EXHAUSTED);
            TimerExhausted
        })?;

        #[cfg(has_io_uring)]
        {
            let payload = TimerSlotPool::encode_payload(slot, generation);
            let ud = UserData::encode(OpTag::Timer, 0, payload);
            let ts_ptr = executor
                .timer_pool
                .set_absolute(slot, deadline.secs, deadline.nsecs);

            if let Err(_e) = driver.ring.submit_timeout_abs(ts_ptr, ud) {
                executor.timer_pool.release(slot);
                return Ok(SleepFuture {
                    duration: Duration::ZERO,
                    timer_slot: None,
                    generation: 0,
                    absolute: Some(deadline),
                });
            }
        }

        #[cfg(not(has_io_uring))]
        {
            executor
                .timer_pool
                .set_absolute(slot, deadline.secs, deadline.nsecs);
        }

        Ok(SleepFuture {
            duration: Duration::ZERO,
            timer_slot: Some(slot),
            generation,
            absolute: Some(deadline),
        })
    })
}

// ── Timeout ──────────────────────────────────────────────────────────

/// Error returned when a [`timeout()`] deadline expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Wrap a future with a deadline. If the future does not complete within
/// `duration`, returns `Err(Elapsed)`.
///
/// # Example
///
/// ```no_run
/// # async fn example() {
/// use std::time::Duration;
/// match ringline::timeout(Duration::from_secs(1), async { 42 }).await {
///     Ok(value) => { /* completed in time */ }
///     Err(_elapsed) => { /* timed out */ }
/// }
/// # }
/// ```
pub fn timeout<F: Future>(duration: Duration, future: F) -> TimeoutFuture<F> {
    TimeoutFuture {
        future,
        sleep: sleep(duration),
    }
}

/// Wrap a future with a deadline, returning an error if the timer pool is full.
///
/// Unlike [`timeout()`] which panics on pool exhaustion, this returns
/// `Err(TimerExhausted)` so callers can handle capacity limits gracefully.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn try_timeout<F: Future>(
    duration: Duration,
    future: F,
) -> Result<TimeoutFuture<F>, TimerExhausted> {
    let sleep = try_sleep(duration)?;
    Ok(TimeoutFuture { future, sleep })
}

/// Wrap a future with an absolute deadline. If the future does not complete
/// before `deadline`, returns `Err(Elapsed)`.
///
/// Uses io_uring's `TIMEOUT_ABS` flag with `CLOCK_MONOTONIC`.
///
/// # Panics
///
/// Panics if the timer slot pool is exhausted.
pub fn timeout_at<F: Future>(deadline: Deadline, future: F) -> TimeoutFuture<F> {
    TimeoutFuture {
        future,
        sleep: sleep_until(deadline),
    }
}

/// Wrap a future with an absolute deadline, returning an error if the timer pool is full.
///
/// Unlike [`timeout_at()`] which panics on pool exhaustion, this returns
/// `Err(TimerExhausted)` so callers can handle capacity limits gracefully.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn try_timeout_at<F: Future>(
    deadline: Deadline,
    future: F,
) -> Result<TimeoutFuture<F>, TimerExhausted> {
    let sleep = try_sleep_until(deadline)?;
    Ok(TimeoutFuture { future, sleep })
}

pin_project_lite::pin_project! {
    /// Future returned by [`timeout()`] or [`timeout_at()`].
    pub struct TimeoutFuture<F> {
        #[pin]
        future: F,
        #[pin]
        sleep: SleepFuture,
    }
}

impl<F: Future> Future for TimeoutFuture<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // Poll the inner future first.
        if let Poll::Ready(output) = this.future.poll(cx) {
            return Poll::Ready(Ok(output));
        }

        // Poll the sleep timer.
        if let Poll::Ready(()) = this.sleep.poll(cx) {
            return Poll::Ready(Err(Elapsed));
        }

        Poll::Pending
    }
}

// ── Disk I/O async API ──────────────────────────────────────────────

/// Future that awaits a disk I/O completion (NVMe or Direct I/O).
///
/// The io_uring SQE was submitted before this future was created.
/// On completion, the CQE handler stores the result and wakes the task.
pub struct DiskIoFuture {
    pub(crate) seq: u32,
}

impl Future for DiskIoFuture {
    type Output = io::Result<i32>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<i32>> {
        with_state(|_driver, executor| {
            match executor.disk_io_results.remove(&self.seq) {
                Some(result) if result < 0 => {
                    Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
                }
                Some(result) => Poll::Ready(Ok(result)),
                None => {
                    // Re-register waiter (polled before CQE arrived or after spurious wake).
                    let task_id = CURRENT_TASK_ID.with(|c| c.get());
                    executor.disk_io_waiters.insert(self.seq, task_id);
                    Poll::Pending
                }
            }
        })
    }
}

impl Drop for DiskIoFuture {
    fn drop(&mut self) {
        let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
        if opt_non_null.is_none() {
            return;
        }
        let mut non_null = opt_non_null.unwrap();
        let state = unsafe { non_null.as_mut() };
        let executor = unsafe { &mut *state.executor.as_mut() };
        executor.disk_io_waiters.remove(&self.seq);
    }
}

/// Open a Direct I/O file from any async task.
///
/// Returns a [`DirectIoFile`](crate::direct_io::DirectIoFile) handle
/// for use with [`direct_io_read()`].
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn open_direct_io_file(path: &str) -> io::Result<crate::direct_io::DirectIoFile> {
    with_state(|driver, _| {
        let mut ctx = driver.make_ctx();
        ctx.open_direct_io_file(path)
    })
}

/// Open an NVMe device from any async task.
///
/// Returns an [`NvmeDevice`](crate::nvme::NvmeDevice) handle
/// for use with [`nvme_read()`], [`nvme_write()`], and [`nvme_flush()`].
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn open_nvme_device(path: &str, nsid: u32) -> io::Result<crate::nvme::NvmeDevice> {
    with_state(|driver, _| {
        let mut ctx = driver.make_ctx();
        ctx.open_nvme_device(path, nsid)
    })
}

/// Submit a Direct I/O read and return a future for the result.
///
/// Reads `len` bytes from `offset` into the buffer at `buf`.
/// The returned future completes when the io_uring CQE arrives.
///
/// # Safety
///
/// `buf` must point to aligned, writable memory of at least `len` bytes
/// that remains valid until the future completes.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub unsafe fn direct_io_read(
    file: crate::direct_io::DirectIoFile,
    offset: u64,
    buf: *mut u8,
    len: u32,
) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        // Safety: the outer `direct_io_read()` is already unsafe, and the
        // caller guarantees the buffer invariants.
        #[allow(unused_unsafe)]
        let seq = unsafe { ctx.direct_io_read(file, offset, buf, len)? };
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

/// Submit an NVMe read and return a future for the result.
///
/// Reads `num_blocks` logical blocks starting at `lba` into the buffer
/// at `buf_addr` with length `buf_len`. The returned future completes
/// when the io_uring CQE arrives.
///
/// # Safety
///
/// `buf_addr` must point to valid, aligned memory of at least `buf_len`
/// bytes that remains valid until the returned future completes.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub unsafe fn nvme_read(
    device: crate::nvme::NvmeDevice,
    lba: u64,
    num_blocks: u16,
    buf_addr: u64,
    buf_len: u32,
) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        // SAFETY: forwarded contract — the caller of the (unsafe) public
        // wrapper guarantees buf_addr/buf_len validity and lifetime.
        let seq = unsafe { ctx.nvme_read(device, lba, num_blocks, buf_addr, buf_len)? };
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

/// Submit a Direct I/O write and return a future for the result.
///
/// Writes `len` bytes from the buffer at `buf` to `offset` in the file.
/// The returned future completes when the io_uring CQE arrives.
///
/// # Safety
///
/// `buf` must point to aligned, readable memory of at least `len` bytes
/// that remains valid until the future completes.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub unsafe fn direct_io_write(
    file: crate::direct_io::DirectIoFile,
    offset: u64,
    buf: *const u8,
    len: u32,
) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        // Safety: the outer `direct_io_write()` is already unsafe, and the
        // caller guarantees the buffer invariants.
        #[allow(unused_unsafe)]
        let seq = unsafe { ctx.direct_io_write(file, offset, buf, len)? };
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

/// Submit an NVMe flush and return a future for the result.
///
/// Flushes volatile write caches on the device, ensuring all previously
/// written data is persisted to non-volatile storage. The returned future
/// completes when the io_uring CQE arrives.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn nvme_flush(device: crate::nvme::NvmeDevice) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let seq = ctx.nvme_flush(device)?;
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

/// Submit an NVMe write and return a future for the result.
///
/// Writes `num_blocks` logical blocks starting at `lba` from the buffer
/// at `buf_addr` with length `buf_len`. The returned future completes
/// when the io_uring CQE arrives.
///
/// # Safety
///
/// `buf_addr` must point to valid, aligned memory of at least `buf_len`
/// bytes that remains valid until the returned future completes.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub unsafe fn nvme_write(
    device: crate::nvme::NvmeDevice,
    lba: u64,
    num_blocks: u16,
    buf_addr: u64,
    buf_len: u32,
) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        // SAFETY: forwarded contract — see nvme_read above.
        let seq = unsafe { ctx.nvme_write(device, lba, num_blocks, buf_addr, buf_len)? };
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

// ── UDP async API ───────────────────────────────────────────────────

/// Async context for a UDP socket.
///
/// Passed to [`AsyncEventHandler::on_udp_bind()`](crate::AsyncEventHandler::on_udp_bind)
/// for each bound UDP socket. Provides async recv and fire-and-forget send.
#[derive(Clone, Copy)]
pub struct UdpCtx {
    pub(crate) udp_index: u32,
}

impl UdpCtx {
    /// Returns the UDP socket index within this worker.
    pub fn index(&self) -> usize {
        self.udp_index as usize
    }

    /// Receive a datagram, returning the payload and source address.
    ///
    /// Suspends until a datagram is available. Each call returns exactly one
    /// datagram. The payload is copied into a `Vec<u8>` (datagrams are
    /// typically small, so this is acceptable for the initial implementation).
    ///
    /// Only one task should await this per socket at a time; a second waiter
    /// overwrites the first, which is then leaked (it must still be driven
    /// from another wake source). If you need fan-out to multiple consumers,
    /// run a single recv loop and forward the payloads through your own
    /// channel.
    pub fn recv_from(&self) -> UdpRecvFuture {
        UdpRecvFuture {
            udp_index: self.udp_index,
        }
    }

    /// Receive the next datagram and invoke `f(payload, peer)` zero-copy.
    ///
    /// Unlike [`recv_from()`](Self::recv_from), this does not allocate a `Vec`
    /// for the payload. On the io_uring backend the callback runs over the
    /// kernel-provided buffer directly; on the mio backend it runs over the
    /// per-socket recv buffer. Either way, the buffer is released as soon as
    /// the callback returns, so any data the caller wants to keep must be
    /// copied out inside `f`.
    ///
    /// Same single-consumer semantics as [`recv_from()`](Self::recv_from).
    pub fn with_datagram<F, R>(&self, f: F) -> UdpWithDatagramFuture<F, R>
    where
        F: FnMut(&[u8], SocketAddr) -> R + Unpin,
    {
        UdpWithDatagramFuture {
            udp_index: self.udp_index,
            f: Some(f),
            _marker: std::marker::PhantomData,
        }
    }

    /// Timestamped counterpart of
    /// [`recv_batch`](Self::recv_batch) — each invocation of `f`
    /// receives a third argument: the [`Instant`] the driver first
    /// observed the datagram in user space (in the io_uring CQE
    /// handler on the io_uring backend, or in the `recv_from` poll
    /// on the mio backend). Use this to feed protocol drivers that
    /// measure RTT (like quinn-proto's `handle_datagram(now, ...)`)
    /// without including executor wake + task poll latency in the
    /// measurement — that latency would otherwise be charged to the
    /// network path and trigger spurious loss / congestion signals
    /// at high pps.
    ///
    /// All other semantics — single-consumer, zero-copy borrow,
    /// `max` trade-off — match [`recv_batch`](Self::recv_batch).
    pub fn recv_batch_timed<F>(&self, max: usize, f: F) -> UdpRecvBatchTimedFuture<F>
    where
        F: FnMut(&[u8], SocketAddr, Instant) + Unpin,
    {
        debug_assert!(max > 0, "recv_batch_timed max must be at least 1");
        UdpRecvBatchTimedFuture {
            udp_index: self.udp_index,
            max,
            f: Some(f),
        }
    }

    /// Drain up to `max` currently-queued datagrams in a single poll.
    ///
    /// Resolves once at least one datagram is available. On poll-Ready,
    /// the callback is invoked up to `max` times — once per queued
    /// datagram — before the future returns the count drained. This
    /// collapses what would otherwise be N task wake-cycles into one
    /// and is the high-throughput counterpart to
    /// [`with_datagram()`](Self::with_datagram).
    ///
    /// **Pick `max` carefully.** A larger value reduces executor
    /// overhead at high pps but delays the next trip through your
    /// recv→handle→send loop, which on protocol drivers like QUIC
    /// translates into delayed ACK / `MAX_STREAM_DATA` emission and
    /// can stall the peer's congestion window. As a rule of thumb,
    /// 4–16 is a reasonable starting point: enough to amortise the
    /// per-poll overhead at thousands of pps without buffering more
    /// than a few millisecond's worth of inbound traffic before the
    /// next send-side flush. `max = 0` is invalid and panics in debug
    /// builds.
    ///
    /// io_uring multishot recv keeps writing CQEs into our recv queue
    /// between event-loop iterations, so by the time a task is polled
    /// the queue may already hold several datagrams. Draining several
    /// at once avoids the per-packet executor wake/poll overhead that
    /// becomes the bottleneck at packet rates approaching the upper
    /// end of what a single core can process (5K+ pps).
    ///
    /// Same zero-copy semantics as [`with_datagram()`](Self::with_datagram):
    /// each invocation of `f` borrows directly from the kernel-provided
    /// buffer; the buffer is released back to the kernel after `f`
    /// returns and before the next datagram is dispatched.
    ///
    /// Same single-consumer semantics as [`recv_from()`](Self::recv_from).
    pub fn recv_batch<F>(&self, max: usize, f: F) -> UdpRecvBatchFuture<F>
    where
        F: FnMut(&[u8], SocketAddr) + Unpin,
    {
        debug_assert!(max > 0, "recv_batch max must be at least 1");
        UdpRecvBatchFuture {
            udp_index: self.udp_index,
            max,
            f: Some(f),
        }
    }

    /// Resolve when at least one UDP send slot is available on this socket.
    ///
    /// Use this to back off when [`UdpCtx::send_to`] returns
    /// [`crate::error::UdpSendError::PoolExhausted`], rather than busy-looping.
    /// On the io_uring backend the future suspends until a completion frees
    /// a slot. On the mio backend sends are synchronous and this future is
    /// always immediately ready — callers can still use it to stay
    /// backend-agnostic.
    ///
    /// Only one task should await this per socket at a time; a second waiter
    /// overwrites the first, which is then leaked (it must still be driven
    /// from another wake source).
    pub fn send_ready(&self) -> UdpSendReadyFuture {
        UdpSendReadyFuture {
            udp_index: self.udp_index,
            #[cfg(not(has_io_uring))]
            yielded: false,
        }
    }

    /// Send a datagram to the given peer (fire-and-forget, copying).
    ///
    /// Copies `data` into the send pool and submits a `sendmsg` SQE.
    /// Only one send can be in-flight per UDP socket at a time.
    #[cfg(has_io_uring)]
    pub fn send_to(&self, peer: SocketAddr, data: &[u8]) -> Result<(), crate::error::UdpSendError> {
        with_state(|driver, _executor| driver.udp_send_to(self.udp_index, peer, data, None))
    }

    /// Send a datagram to the given peer (mio backend — synchronous non-blocking send).
    ///
    /// `WouldBlock` is surfaced as [`crate::error::UdpSendError::PoolExhausted`] so callers
    /// have a single "try again later" branch that matches the io_uring backend.
    #[cfg(not(has_io_uring))]
    pub fn send_to(&self, peer: SocketAddr, data: &[u8]) -> Result<(), crate::error::UdpSendError> {
        with_state(|driver, _executor| {
            let idx = self.udp_index as usize;
            if idx >= driver.udp_sockets.len() {
                return Err(crate::error::UdpSendError::Io(io::Error::other(
                    "invalid UDP socket index",
                )));
            }
            match driver.udp_sockets[idx].send_to(data, peer) {
                Ok(_) => {
                    crate::metrics::UDP.increment(crate::metrics::udp::DATAGRAMS_SENT);
                    Ok(())
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    Err(crate::error::UdpSendError::PoolExhausted)
                }
                Err(e) => Err(crate::error::UdpSendError::Io(e)),
            }
        })
    }

    /// Send `data` segmented into back-to-back `segment_size`-byte UDP
    /// datagrams in a *single* `sendmsg` syscall, using Linux GSO
    /// (`UDP_SEGMENT`).
    ///
    /// `data.len()` must be a positive multiple of `segment_size`, except
    /// the final packet may be shorter — the kernel splits the buffer
    /// at every `segment_size` boundary and sends a smaller trailing
    /// packet for any remainder.
    ///
    /// On the io_uring backend this attaches the cmsg to the existing
    /// per-slot `msghdr` and submits one SQE — at high packet rates
    /// this is several times faster than calling `send_to` per
    /// datagram. On the mio backend (which doesn't have a built-in
    /// place to bolt on cmsgs), this falls back to calling the
    /// underlying `send_to` once per `segment_size` chunk; the
    /// observable behavior is identical to applications, just without
    /// the syscall savings.
    ///
    /// Errors:
    /// - `Io(InvalidInput)` if `data` is too large for the send pool
    ///   slot, or if `segment_size` is zero or larger than `data`.
    /// - `PoolExhausted` if no free slot is currently available — wait
    ///   on [`send_ready`](Self::send_ready) and retry.
    #[cfg(has_io_uring)]
    pub fn send_to_gso(
        &self,
        peer: SocketAddr,
        data: &[u8],
        segment_size: u16,
    ) -> Result<(), crate::error::UdpSendError> {
        with_state(|driver, _executor| {
            driver.udp_send_to(self.udp_index, peer, data, Some(segment_size))
        })
    }

    /// mio fallback for [`send_to_gso`](Self::send_to_gso): emits
    /// per-segment `send_to` calls. Same observable result, no syscall
    /// batching benefit.
    #[cfg(not(has_io_uring))]
    pub fn send_to_gso(
        &self,
        peer: SocketAddr,
        data: &[u8],
        segment_size: u16,
    ) -> Result<(), crate::error::UdpSendError> {
        if segment_size == 0 || (segment_size as usize) > data.len() {
            return Err(crate::error::UdpSendError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GSO segment_size invalid for data length",
            )));
        }
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + segment_size as usize).min(data.len());
            self.send_to(peer, &data[offset..end])?;
            offset = end;
        }
        Ok(())
    }
}

/// Future returned by [`UdpCtx::recv_from()`].
pub struct UdpRecvFuture {
    udp_index: u32,
}

impl Future for UdpRecvFuture {
    type Output = (Vec<u8>, SocketAddr);

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_state(|driver, executor| {
            let idx = self.udp_index as usize;
            if idx < executor.udp_recv_queues.len()
                && let Some(mut entry) = executor.udp_recv_queues[idx].pop_front()
            {
                // GRO-coalesced entry: hand out ONE datagram per call (the
                // docs promise "the next datagram"). Returning the whole
                // coalesced blob concatenated N datagrams into one.
                if entry.segment_size != 0 {
                    let (start, end) = entry.next_segment_range();
                    let peer = entry.peer;
                    let seg = entry.data()[start..end].to_vec();
                    entry.consumed = end as u32;
                    if entry.exhausted() {
                        let bid = entry.bid_to_release();
                        #[cfg(has_io_uring)]
                        if let Some(bid) = bid {
                            driver.udp_pending_replenish.push(bid);
                        }
                        #[cfg(not(has_io_uring))]
                        let _ = bid;
                    } else {
                        executor.udp_recv_queues[idx].push_front(entry);
                    }
                    let _ = &driver;
                    return Poll::Ready((seg, peer));
                }
                let bid = entry.bid_to_release();
                let owned = entry.into_owned();
                #[cfg(has_io_uring)]
                if let Some(bid) = bid {
                    driver.udp_pending_replenish.push(bid);
                }
                #[cfg(not(has_io_uring))]
                let _ = (bid, driver);
                return Poll::Ready(owned);
            }
            // Register as waiter so the CQE handler wakes us. There is
            // only one waiter slot per socket; if a different task
            // already registered, we'd silently overwrite and the prior
            // task would get stuck forever. That's documented as
            // "single-consumer" semantics on `recv_from`, but it's an
            // easy mistake — fail loud in debug builds.
            let task_id = CURRENT_TASK_ID.with(|c| c.get());
            if idx < executor.udp_recv_waiters.len() {
                debug_assert!(
                    executor.udp_recv_waiters[idx].is_none_or(|t| t == task_id),
                    "two distinct tasks awaiting recv_from on UdpCtx index {idx}; \
                     UdpCtx::recv_from supports a single consumer per socket"
                );
                executor.udp_recv_waiters[idx] = Some(task_id);
            }
            Poll::Pending
        })
    }
}

/// Future returned by [`UdpCtx::with_datagram()`].
///
/// Zero-copy alternative to [`UdpCtx::recv_from()`]: the callback is invoked
/// over a borrowed slice into the kernel-provided buffer (io_uring) or the
/// owned recv buffer (mio), without ever allocating a `Vec` for the payload.
/// The buffer is released back to the kernel once the callback returns.
pub struct UdpWithDatagramFuture<F, R>
where
    F: FnMut(&[u8], SocketAddr) -> R + Unpin,
{
    udp_index: u32,
    f: Option<F>,
    _marker: std::marker::PhantomData<fn() -> R>,
}

impl<F, R> Future for UdpWithDatagramFuture<F, R>
where
    F: FnMut(&[u8], SocketAddr) -> R + Unpin,
{
    type Output = R;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<R> {
        let this = self.get_mut();
        with_state(|driver, executor| {
            let idx = this.udp_index as usize;
            if idx < executor.udp_recv_queues.len()
                && let Some(mut entry) = executor.udp_recv_queues[idx].pop_front()
            {
                let f = this
                    .f
                    .as_mut()
                    .expect("UdpWithDatagramFuture polled after Ready");
                // One datagram per call: for a GRO-coalesced entry, run the
                // callback on the next segment and keep the remainder queued.
                let (start, end) = entry.next_segment_range();
                let r = f(&entry.data()[start..end], entry.peer);
                this.f.take();
                entry.consumed = end as u32;
                if entry.exhausted() {
                    let bid = entry.bid_to_release();
                    drop(entry);
                    #[cfg(has_io_uring)]
                    if let Some(bid) = bid {
                        driver.udp_pending_replenish.push(bid);
                    }
                    #[cfg(not(has_io_uring))]
                    let _ = (bid, driver);
                } else {
                    executor.udp_recv_queues[idx].push_front(entry);
                }
                return Poll::Ready(r);
            }
            let task_id = CURRENT_TASK_ID.with(|c| c.get());
            if idx < executor.udp_recv_waiters.len() {
                debug_assert!(
                    executor.udp_recv_waiters[idx].is_none_or(|t| t == task_id),
                    "two distinct tasks awaiting recv on UdpCtx index {idx}; \
                     UdpCtx::with_datagram supports a single consumer per socket"
                );
                executor.udp_recv_waiters[idx] = Some(task_id);
            }
            Poll::Pending
        })
    }
}

/// Future returned by [`UdpCtx::recv_batch()`].
///
/// Drains up to `max` queued datagrams on the first poll where at
/// least one is available. Returns the count of datagrams drained
/// (between 1 and `max`). See [`UdpCtx::recv_batch`] for the rationale
/// on choosing `max`.
pub struct UdpRecvBatchFuture<F>
where
    F: FnMut(&[u8], SocketAddr) + Unpin,
{
    udp_index: u32,
    max: usize,
    f: Option<F>,
}

impl<F> Future for UdpRecvBatchFuture<F>
where
    F: FnMut(&[u8], SocketAddr) + Unpin,
{
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        let this = self.get_mut();
        with_state(|driver, executor| {
            let idx = this.udp_index as usize;
            if idx >= executor.udp_recv_queues.len() || executor.udp_recv_queues[idx].is_empty() {
                let task_id = CURRENT_TASK_ID.with(|c| c.get());
                if idx < executor.udp_recv_waiters.len() {
                    debug_assert!(
                        executor.udp_recv_waiters[idx].is_none_or(|t| t == task_id),
                        "two distinct tasks awaiting recv on UdpCtx index {idx}; \
                         UdpCtx::recv_batch supports a single consumer per socket"
                    );
                    executor.udp_recv_waiters[idx] = Some(task_id);
                }
                return Poll::Pending;
            }

            let f = this
                .f
                .as_mut()
                .expect("UdpRecvBatchFuture polled after Ready");

            let mut drained: usize = 0;
            while drained < this.max
                && let Some(entry) = executor.udp_recv_queues[idx].pop_front()
            {
                let bid = entry.bid_to_release();
                let peer = entry.peer;
                // One callback per datagram: GRO-coalesced entries fan out
                // into per-segment slices here. `drained` counts entries
                // (kernel recv completions), so the bid lifetime stays simple
                // — released once after the whole entry is consumed.
                entry.for_each_segment(|seg| f(seg, peer));
                drop(entry);
                #[cfg(has_io_uring)]
                if let Some(bid) = bid {
                    driver.udp_pending_replenish.push(bid);
                }
                // On mio the bid is always `None` (no kernel buf ring) and
                // `driver` is borrowed but never read inside the loop. Keep
                // `let _ = bid;` here to silence the per-iteration unused
                // warning without moving `driver` (which the loop will
                // touch again on the next iteration).
                #[cfg(not(has_io_uring))]
                let _ = bid;
                drained += 1;
            }
            this.f.take();
            #[cfg(not(has_io_uring))]
            let _ = driver;
            Poll::Ready(drained)
        })
    }
}

/// Future returned by [`UdpCtx::recv_batch_timed()`].
///
/// Identical to [`UdpRecvBatchFuture`] except the callback receives
/// the driver-captured arrival timestamp as a third argument.
pub struct UdpRecvBatchTimedFuture<F>
where
    F: FnMut(&[u8], SocketAddr, Instant) + Unpin,
{
    udp_index: u32,
    max: usize,
    f: Option<F>,
}

impl<F> Future for UdpRecvBatchTimedFuture<F>
where
    F: FnMut(&[u8], SocketAddr, Instant) + Unpin,
{
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        let this = self.get_mut();
        with_state(|driver, executor| {
            let idx = this.udp_index as usize;
            if idx >= executor.udp_recv_queues.len() || executor.udp_recv_queues[idx].is_empty() {
                let task_id = CURRENT_TASK_ID.with(|c| c.get());
                if idx < executor.udp_recv_waiters.len() {
                    debug_assert!(
                        executor.udp_recv_waiters[idx].is_none_or(|t| t == task_id),
                        "two distinct tasks awaiting recv on UdpCtx index {idx}; \
                         UdpCtx::recv_batch_timed supports a single consumer per socket"
                    );
                    executor.udp_recv_waiters[idx] = Some(task_id);
                }
                return Poll::Pending;
            }

            let f = this
                .f
                .as_mut()
                .expect("UdpRecvBatchTimedFuture polled after Ready");

            let mut drained: usize = 0;
            while drained < this.max
                && let Some(entry) = executor.udp_recv_queues[idx].pop_front()
            {
                let bid = entry.bid_to_release();
                let recv_at = entry.recv_at;
                let peer = entry.peer;
                // One callback per datagram; GRO-coalesced entries fan out
                // into per-segment slices, all sharing this entry's arrival
                // timestamp. `drained` counts entries, not segments.
                entry.for_each_segment(|seg| f(seg, peer, recv_at));
                drop(entry);
                #[cfg(has_io_uring)]
                if let Some(bid) = bid {
                    driver.udp_pending_replenish.push(bid);
                }
                #[cfg(not(has_io_uring))]
                let _ = bid;
                drained += 1;
            }
            this.f.take();
            #[cfg(not(has_io_uring))]
            let _ = driver;
            Poll::Ready(drained)
        })
    }
}

/// Future returned by [`UdpCtx::send_ready()`].
pub struct UdpSendReadyFuture {
    /// Mio: whether this future has yielded once (cooperative backoff).
    #[cfg(not(has_io_uring))]
    yielded: bool,
    /// Never read on the mio backend — the future resolves immediately
    /// without looking at any per-socket state.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    udp_index: u32,
}

impl Future for UdpSendReadyFuture {
    type Output = ();

    #[cfg(has_io_uring)]
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        with_state(|driver, executor| {
            let idx = self.udp_index as usize;
            if idx < driver.udp_sockets.len() && !driver.udp_sockets[idx].send_freelist.is_empty() {
                return Poll::Ready(());
            }
            // Same single-consumer story as `recv_from` — if another
            // task is already awaiting `send_ready`, overwriting their
            // wakeup leaks them forever.
            let task_id = CURRENT_TASK_ID.with(|c| c.get());
            if idx < executor.udp_send_ready_waiters.len() {
                debug_assert!(
                    executor.udp_send_ready_waiters[idx].is_none_or(|t| t == task_id),
                    "two distinct tasks awaiting send_ready on UdpCtx index {idx}; \
                     UdpCtx::send_ready supports a single consumer per socket"
                );
                executor.udp_send_ready_waiters[idx] = Some(task_id);
            }
            Poll::Pending
        })
    }

    /// Mio backend: sends are synchronous `send_to` calls that never queue.
    /// Yield once before resolving: the documented retry pattern
    /// (`PoolExhausted`/`WouldBlock` → `send_ready().await` → retry) would
    /// otherwise be a hard busy-loop inside a single task poll,
    /// monopolizing the worker until the kernel drains the socket buffer.
    #[cfg(not(has_io_uring))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if !self.yielded {
            self.yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(())
    }
}
