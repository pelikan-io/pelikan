use std::io;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::time::Instant;

use io_uring::cqueue;

use crate::backend::Driver;
use crate::backend::sockaddr_to_socket_addr;
use crate::chain::ChainEvent;
use crate::completion::{OpTag, UserData};
use crate::connection::RecvMode;
use crate::metrics;
use crate::runtime::handler::AsyncEventHandler;
use crate::runtime::io::{ConnCtx, DriverState, UdpCtx, set_driver_state_guarded};
use crate::runtime::waker::{STANDALONE_BIT, conn_waker, standalone_waker};
use crate::runtime::{CURRENT_TASK_ID, Executor, TimerSlotPool};

/// Async event loop that reuses `Driver` infrastructure with an `Executor`
/// for polling connection futures instead of push-based callbacks.
pub(crate) struct AsyncEventLoop<A: AsyncEventHandler> {
    driver: Driver,
    handler: A,
    executor: Executor,
}

impl<A: AsyncEventHandler> AsyncEventLoop<A> {
    /// Create a new async event loop for a worker thread.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: &crate::config::Config,
        handler: A,
        accept_rx: Option<crossbeam_channel::Receiver<(std::os::fd::RawFd, std::net::SocketAddr)>>,
        eventfd: std::os::fd::RawFd,
        shutdown_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
        resolve_rx: Option<crossbeam_channel::Receiver<crate::resolver::ResolveResponse>>,
        resolve_tx: Option<crossbeam_channel::Sender<crate::resolver::ResolveResponse>>,
        resolver: Option<std::sync::Arc<crate::resolver::ResolverPool>>,
        spawn_rx: Option<crossbeam_channel::Receiver<crate::spawner::SpawnResponse>>,
        spawn_tx: Option<crossbeam_channel::Sender<crate::spawner::SpawnResponse>>,
        spawner: Option<std::sync::Arc<crate::spawner::SpawnerPool>>,
        blocking_rx: Option<crossbeam_channel::Receiver<crate::blocking::BlockingResponse>>,
        blocking_tx: Option<crossbeam_channel::Sender<crate::blocking::BlockingResponse>>,
        blocking_pool: Option<std::sync::Arc<crate::blocking::BlockingPool>>,
        region_rx: crate::region_registry::RegionControlRx,
    ) -> Result<Self, crate::error::Error> {
        let driver = Driver::new(
            config,
            accept_rx,
            eventfd,
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
        )?;
        // On io_uring the recv queue holds kernel ring buffer bids
        // (`PendingUdpBuf::Kernel`), so each queued datagram pins one
        // provided buffer until the task consumes it. Capacity beyond the
        // ring size is physically unusable: the ring starves (ENOBUFS)
        // before the queue ever fills, and the drop accounting never
        // fires. Clamp so overflow is an observable drop instead of a
        // silent stall. (The mio backend copies into owned buffers and
        // keeps the full configured depth.)
        let udp_queue_capacity = config
            .udp_recv_queue_capacity
            .min(config.udp_recv_buffer.ring_size as usize);
        let executor = Executor::new(
            config.max_connections,
            config.standalone_task_capacity,
            config.timer_slots,
            config.udp_bind.len() as u32,
            udp_queue_capacity,
        );
        Ok(AsyncEventLoop {
            driver,
            handler,
            executor,
        })
    }

    /// Complete fallible backend setup before the runtime is advertised as ready.
    pub(crate) fn prepare_run(&mut self) -> Result<(), crate::error::Error> {
        // Always arm eventfd read — needed for shutdown wakeup even in client-only mode.
        self.driver
            .ring
            .submit_eventfd_read(self.driver.eventfd, self.driver.eventfd_buf.as_mut_ptr())?;
        self.driver.eventfd_armed = true;

        // Kick the eventfd so the first submit_and_wait(1) returns immediately.
        let kick: u64 = 1;
        unsafe {
            libc::write(
                self.driver.eventfd,
                &kick as *const u64 as *const libc::c_void,
                8,
            );
        }

        Ok(())
    }

    /// Run the async event loop. Blocks the current thread.
    pub(crate) fn run(&mut self) -> Result<(), crate::error::Error> {
        // Spawn UDP handler tasks for each bound UDP socket.
        for udp_idx in 0..self.driver.udp_sockets.len() {
            let udp_ctx = UdpCtx {
                udp_index: udp_idx as u32,
            };
            if let Some(future) = self.handler.on_udp_bind(udp_ctx)
                && let Some(idx) = self.executor.standalone_slab.spawn(future)
            {
                self.executor.ready_queue.push_back(idx | STANDALONE_BIT);
            }
        }

        // Spawn on_start task (client-only entry point).
        if let Some(future) = self.handler.on_start()
            && let Some(idx) = self.executor.standalone_slab.spawn(future)
        {
            self.executor.ready_queue.push_back(idx | STANDALONE_BIT);
        }

        // Wall-clock stall instrumentation costs ~4 clock reads per iteration,
        // so it is opt-in: set RINGLINE_LOOP_DIAG=1 to record wait/work stall
        // buckets (reported in the `[ringline stall]` line at shutdown). The
        // cheap iteration-mix counters below are always on.
        let loop_diag = std::env::var_os("RINGLINE_LOOP_DIAG").is_some();

        // ── Diagnostic counters (printed to stderr at shutdown) ────────────────
        // These measure the event-loop iteration mix to help diagnose client
        // throughput deficits.  All counters are u64 locals — zero overhead
        // in release builds when the eprintln! at shutdown is compiled away.
        let mut diag_iters: u64 = 0;
        let mut diag_dead_iters: u64 = 0; // iters where no tasks were polled in first poll_ready_tasks
        let mut diag_cqes_1st: u64 = 0; // CQEs from first drain_completions (after submit_and_wait)
        let mut diag_cqes_2nd: u64 = 0; // CQEs from second drain_completions (after flush)
        let mut diag_tasks_1st: u64 = 0; // tasks polled in first poll_ready_tasks
        let mut diag_tasks_fp: u64 = 0; // tasks polled in fast-path poll_ready_tasks

        // ── Latency stall counters ─────────────────────────────────────────────
        // "wait"  = time blocked inside submit_and_wait (kernel side).
        // "work"  = rest of the iteration (drain, tasks, flush, on_tick).
        // Buckets count iterations where that phase exceeded the threshold.
        // max values record the single worst observation.
        let mut diag_wait_ge_1ms: u64 = 0;
        let mut diag_wait_ge_5ms: u64 = 0;
        let mut diag_wait_ge_10ms: u64 = 0;
        let mut diag_wait_ns_max: u64 = 0;
        let mut diag_work_ge_1ms: u64 = 0;
        let mut diag_work_ge_5ms: u64 = 0;
        let mut diag_work_ge_10ms: u64 = 0;
        let mut diag_work_ns_max: u64 = 0;

        loop {
            // Only read the clock when stall instrumentation is enabled.
            let iter_start = if loop_diag {
                Some(std::time::Instant::now())
            } else {
                None
            };

            // Retry eventfd re-arm if a previous attempt failed (SQ was full).
            if !self.driver.eventfd_armed && !self.driver.shutdown_flag.load(Ordering::Relaxed) {
                self.driver.eventfd_armed = self
                    .driver
                    .ring
                    .submit_eventfd_read(self.driver.eventfd, self.driver.eventfd_buf.as_mut_ptr())
                    .is_ok();
            }

            // Arm a tick timeout before blocking.
            // Only mark as armed if the SQE was actually submitted — if the SQ
            // is full the submission silently fails, and leaving armed=false
            // ensures we retry on the next iteration rather than calling
            // submit_and_wait without any timeout in the ring.
            if !self.driver.tick_timeout_armed
                && let Some(ref ts) = self.driver.tick_timeout_ts
            {
                let ud = UserData::encode(OpTag::TickTimeout, 0, 0);
                if self
                    .driver
                    .ring
                    .submit_tick_timeout(ts as *const _, ud.raw())
                    .is_ok()
                {
                    self.driver.tick_timeout_armed = true;
                }
            }

            // The blocking wait itself is always performed; only the timing
            // around it is gated on `loop_diag`.
            let wait_start = if loop_diag {
                Some(std::time::Instant::now())
            } else {
                None
            };
            // Don't block while tasks are already runnable (self-wakes
            // collected after the last poll pass, tasks woken from
            // on_tick): submit SQEs but return immediately so the ready
            // queue is polled now instead of after the next CQE or tick
            // timeout (indefinitely, with tick_timeout_us = 0).
            self.executor.collect_wakeups();
            // Commit buffer returns from the poll pass and revive
            // ENOBUFS-parked receivers before we block.
            self.flush_replenish_and_rearm();
            let min_complete = u32::from(self.executor.ready_queue.is_empty());
            self.driver.ring.submit_and_wait(min_complete)?;
            let mut wait_ns: u64 = 0;
            if let Some(start) = wait_start {
                wait_ns = start.elapsed().as_nanos() as u64;
                if wait_ns >= 1_000_000 {
                    diag_wait_ge_1ms += 1;
                }
                if wait_ns >= 5_000_000 {
                    diag_wait_ge_5ms += 1;
                }
                if wait_ns >= 10_000_000 {
                    diag_wait_ge_10ms += 1;
                }
                if wait_ns > diag_wait_ns_max {
                    diag_wait_ns_max = wait_ns;
                }
            }

            self.drain_completions();
            diag_cqes_1st += self.driver.cqe_batch.len() as u64;

            // Check for shutdown after processing completions.
            if self.driver.shutdown_local || self.driver.shutdown_flag.load(Ordering::Relaxed) {
                // Print per-worker diagnostics before exiting.
                let dead_pct = if diag_iters > 0 {
                    100.0 * diag_dead_iters as f64 / diag_iters as f64
                } else {
                    0.0
                };
                eprintln!(
                    "[ringline diag] iters={diag_iters} dead={diag_dead_iters} ({dead_pct:.1}%) \
                     cqes_1st_avg={:.2} cqes_2nd_avg={:.2} \
                     tasks_1st_avg={:.2} tasks_fp_avg={:.2} parks={} fallbacks={}",
                    diag_cqes_1st as f64 / diag_iters.max(1) as f64,
                    diag_cqes_2nd as f64 / diag_iters.max(1) as f64,
                    diag_tasks_1st as f64 / diag_iters.max(1) as f64,
                    diag_tasks_fp as f64 / diag_iters.max(1) as f64,
                    self.driver.recv_park_count,
                    self.driver.recv_fallback_count,
                );
                if loop_diag {
                    eprintln!(
                        "[ringline stall] \
                         wait_ge_1ms={diag_wait_ge_1ms} wait_ge_5ms={diag_wait_ge_5ms} \
                         wait_ge_10ms={diag_wait_ge_10ms} wait_max={:.1}ms | \
                         work_ge_1ms={diag_work_ge_1ms} work_ge_5ms={diag_work_ge_5ms} \
                         work_ge_10ms={diag_work_ge_10ms} work_max={:.1}ms",
                        diag_wait_ns_max as f64 / 1_000_000.0,
                        diag_work_ns_max as f64 / 1_000_000.0,
                    );
                }
                self.driver.run_shutdown();
                return Ok(());
            }

            // Recv buffer replenish for TCP now happens eagerly at the end of
            // `drain_completions` (same iteration the buffers were consumed).
            // UDP replenish stays here — it is conditional on the UDP buffer
            // ring being configured and is off the burst hot path.
            if !self.driver.udp_pending_replenish.is_empty()
                && let Some(ref mut udp_bufs) = self.driver.udp_provided_bufs
            {
                udp_bufs.replenish_batch(&self.driver.udp_pending_replenish);
                self.driver.udp_pending_replenish.clear();
            }

            // Drain pending region-registry updates dispatched from
            // `ShutdownHandle::register_region` / `unregister_region`. Each
            // message is applied to this worker's ring + registry and then
            // acknowledged so the registrar can return to its caller.
            self.drain_region_control();
            // Retry send/close submissions that failed on a previous tick
            // (SQ was full). The SQ has been flushed by submit_and_wait above.
            self.drain_zc_retries();
            self.drain_coalesced_retries();
            self.drain_recv_forward_retries();
            self.drain_copy_retries();
            self.drain_close_retries();
            self.drain_send_pollout_retries();
            self.driver.tick_count += 1;

            // Check close_notify deadlines — force-close connections where
            // close_notify was sent but the close CQE never arrived.
            self.check_close_notify_deadlines();

            // Drain waker-based ready queue (from wakers fired during poll).
            self.executor.collect_wakeups();

            // Poll all ready tasks.
            let tasks_before = self.executor.ready_queue.len();
            self.poll_ready_tasks();
            diag_tasks_1st += tasks_before as u64;
            if tasks_before == 0 {
                diag_dead_iters += 1;
            }

            // Flush any SQEs enqueued by poll_ready_tasks (e.g., client sends).
            // flush() does two things:
            //   1. submit() — delivers the SQEs to the kernel.
            //   2. enter(GETEVENTS, min=0) — triggers DEFER_TASKRUN task_work
            //      so deferred CQEs (send completions, recv arrivals) are
            //      posted to the CQ ring *before* flush() returns.
            // This means the drain_completions() below sees those CQEs
            // inline, eliminating the "dead" submit_and_wait(1) iteration
            // that would otherwise burn a full event-loop cycle just to
            // process send CQEs that wake no tasks (no send waiter for
            // send_nowait callers).
            let _ = self.driver.ring.flush();

            // Non-blocking drain: consume the CQEs (primarily send completions)
            // that flush()'s GETEVENTS step posted to the CQ ring.
            // For send_nowait the pool slot is freed here; wake_send is a
            // no-op (no waiter).  Draining inline collapses the two-iteration
            // pattern (dead send-CQE iter + live recv-CQE iter) down to one.
            self.drain_completions();
            diag_cqes_2nd += self.driver.cqe_batch.len() as u64;

            // If the drain woke any tasks (recv CQEs that arrived while we
            // were running tasks and were flushed by the GETEVENTS enter),
            // run them now so their sends land in the SQ before we block.
            if !self.executor.ready_queue.is_empty() {
                diag_tasks_fp += self.executor.ready_queue.len() as u64;
                self.poll_ready_tasks();
                let _ = self.driver.ring.flush();
            }

            // on_tick callback (synchronous). Set the executor's
            // driver_state thread-local so user code that calls
            // `ringline::spawn()` / wakers / `with_state` works from
            // inside the handler. Raw pointers dodge the borrow conflict
            // with `make_ctx()` (which would otherwise hold &mut self.driver).
            let handler = &mut self.handler;
            let driver_ptr = &mut self.driver as *mut Driver;
            let executor_ptr = &mut self.executor as *mut crate::runtime::Executor;
            let mut driver_state = DriverState {
                driver: unsafe { NonNull::new_unchecked(driver_ptr) },
                executor: unsafe { NonNull::new_unchecked(executor_ptr) },
            };
            let guard = unsafe { set_driver_state_guarded(&mut driver_state) };
            // Safety: driver_ptr is the only live reference to self.driver
            // until the guard is dropped below. catch_unwind keeps a panic
            // in user tick code from unwinding past the guard's scope with
            // futures observing a dangling CURRENT_DRIVER, and from killing
            // the worker.
            {
                let mut ctx = unsafe { (*driver_ptr).make_ctx() };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    handler.on_tick(&mut ctx);
                }));
                if result.is_err() {
                    eprintln!("ringline: handler on_tick panicked; continuing");
                }
            }
            drop(guard);

            // Record work-phase (everything except the blocking wait) duration.
            if let Some(start) = iter_start {
                // wait_ns was filled by the wait_start guard above — iter_start
                // and wait_start are Some iff loop_diag, so it is never stale
                // here. saturating_sub guards the (monotonic-clock-impossible)
                // elapsed < wait_ns edge so a skew can't produce a huge bucket.
                let work_ns = (start.elapsed().as_nanos() as u64).saturating_sub(wait_ns);
                if work_ns >= 1_000_000 {
                    diag_work_ge_1ms += 1;
                }
                if work_ns >= 5_000_000 {
                    diag_work_ge_5ms += 1;
                }
                if work_ns >= 10_000_000 {
                    diag_work_ge_10ms += 1;
                }
                if work_ns > diag_work_ns_max {
                    diag_work_ns_max = work_ns;
                }
            }

            diag_iters += 1;
        }
    }

    /// Apply any pending region register/unregister messages from the
    /// runtime registrar. Each message is acked individually; the registrar
    /// blocks on its caller until every worker reports.
    fn drain_region_control(&mut self) {
        use crate::region_registry::RegionControlMsg;
        // try_iter returns immediately when the channel is empty.
        loop {
            let msg = match self.driver.region_rx.try_recv() {
                Ok(m) => m,
                Err(_) => break,
            };
            match msg {
                RegionControlMsg::Register { slot, region, ack } => {
                    let result = self
                        .driver
                        .fixed_buffers
                        .set_slot(slot, &region)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))
                        .and_then(|()| {
                            let iov = libc::iovec {
                                iov_base: region.ptr() as *mut _,
                                iov_len: region.len(),
                            };
                            // Safety: caller of `register_region` guarantees the
                            // region outlives the registration.
                            unsafe { self.driver.ring.register_buffers_update_one(slot, iov) }
                        });
                    let _ = ack.send(result);
                }
                RegionControlMsg::Unregister { slot, ack } => {
                    let result = self
                        .driver
                        .fixed_buffers
                        .clear_slot(slot)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))
                        .and_then(|()| {
                            let iov = libc::iovec {
                                iov_base: std::ptr::null_mut(),
                                iov_len: 0,
                            };
                            // Safety: clearing a slot does not reference any user
                            // memory; iov is null/zero.
                            unsafe { self.driver.ring.register_buffers_update_one(slot, iov) }
                        });
                    let _ = ack.send(result);
                }
            }
        }
    }

    /// Poll all tasks in the ready queue (both connection and standalone tasks).
    fn poll_ready_tasks(&mut self) {
        // Form raw pointers once and access driver/executor exclusively through
        // them for the duration of this method. This avoids Stacked Borrows
        // violations: accessing self.driver or self.executor directly after
        // forming these pointers would invalidate them, but futures dereference
        // them via with_state() during poll.
        let driver = &mut self.driver as *mut Driver;
        let executor = &mut self.executor as *mut Executor;

        // Safety: NonNull::new_unchecked is safe because we have valid pointers
        // from &mut self.driver and &mut self.executor above.
        let mut driver_state = DriverState {
            driver: unsafe { NonNull::new_unchecked(driver) },
            executor: unsafe { NonNull::new_unchecked(executor) },
        };
        let driver_state_guard = unsafe { set_driver_state_guarded(&mut driver_state) };

        // Safety: we have exclusive access to driver/executor via self, and
        // only access them through these raw pointers until the guard drops.
        let driver = unsafe { &mut *driver };
        let executor = unsafe { &mut *executor };

        // Per-batch dedup: collapse duplicate ready-queue entries that were
        // present at the START of this poll_ready_tasks call. N CQEs for the
        // same connection landing in one drain_completions batch push N copies
        // of the same id; without dedup, the second through Nth entries each
        // build a waker, set CURRENT_TASK_ID, and call take_ready → None —
        // pure overhead.
        //
        // Safety argument for lost-wakeup freedom:
        //
        // Dedup applies ONLY to entries at indices < initial_len (captured
        // before the loop). Entries appended to ready_queue *during* this call
        // (via wake_task() called from within a polled future, which pushes
        // directly to executor.ready_queue) have i >= initial_len and bypass
        // the dedup check entirely — they are processed unconditionally.
        //
        // This boundary is essential: without it, a future that parks itself
        // and then is immediately re-woken by another task in the same batch
        // (e.g. A wakes B, B's continuation wakes A) would be suppressed by
        // the dedup bit set for A's first occurrence, causing a lost wakeup.
        // With the boundary, only the initial-batch duplicates (from the drain)
        // are collapsed; in-flight wakeups from the futures themselves are
        // always honored.
        //
        // STANDALONE_BIT separates the two dedup arrays so a standalone task
        // and a connection task with the same low-bit index are never confused.
        // Arrays are pre-allocated in Executor (zero per-call heap allocation).
        // Bits are reset by scanning only the initial-batch slice.

        let initial_len = executor.ready_queue.len();

        let mut i = 0;
        while i < executor.ready_queue.len() {
            let raw_id = executor.ready_queue[i];
            let in_initial_batch = i < initial_len;
            i += 1;

            if raw_id & STANDALONE_BIT != 0 {
                // Standalone task.
                let task_idx = (raw_id & !STANDALONE_BIT) as usize;
                if in_initial_batch && task_idx < executor.poll_dedup_standalone.len() {
                    if executor.poll_dedup_standalone[task_idx] {
                        // Duplicate in initial batch — skip.
                        continue;
                    }
                    executor.poll_dedup_standalone[task_idx] = true;
                }
                let task_idx = task_idx as u32;
                if let Some(mut fut) = executor.standalone_slab.take_ready(task_idx) {
                    let waker = standalone_waker(task_idx);
                    let mut cx = Context::from_waker(&waker);

                    CURRENT_TASK_ID.with(|c| c.set(raw_id));
                    executor.currently_polling = Some(raw_id);
                    executor.woken_while_polling = false;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        fut.as_mut().poll(&mut cx)
                    }));
                    executor.currently_polling = None;
                    match result {
                        Ok(std::task::Poll::Ready(())) => {
                            executor.standalone_slab.remove(task_idx);
                        }
                        Ok(std::task::Poll::Pending) => {
                            executor.standalone_slab.park(task_idx, fut);
                            // The task woke itself mid-poll (its slot read
                            // Empty, so the wake couldn't transition it) —
                            // re-queue now that it is parked.
                            if executor.woken_while_polling {
                                let _ = executor.wake_task(raw_id);
                            }
                        }
                        Err(_panic) => {
                            // Drop the future and free the slot. We swallow
                            // the panic to keep the worker alive — without
                            // this, a single buggy `on_udp_bind` /
                            // `on_start` future panics the whole worker
                            // thread and tears down every other connection
                            // on it.
                            drop(fut);
                            executor.standalone_slab.remove(task_idx);
                            eprintln!("ringline: standalone task panicked; dropped");
                        }
                    }
                }
            } else {
                // Connection task.
                let conn_index = raw_id as usize;
                if in_initial_batch && conn_index < executor.poll_dedup_conn.len() {
                    if executor.poll_dedup_conn[conn_index] {
                        // Duplicate in initial batch — skip.
                        continue;
                    }
                    executor.poll_dedup_conn[conn_index] = true;
                }
                let conn_index = conn_index as u32;
                if let Some(mut fut) = executor.task_slab.take_ready(conn_index) {
                    let waker = conn_waker(conn_index);
                    let mut cx = Context::from_waker(&waker);

                    CURRENT_TASK_ID.with(|c| c.set(conn_index));
                    executor.currently_polling = Some(conn_index);
                    executor.woken_while_polling = false;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        fut.as_mut().poll(&mut cx)
                    }));
                    executor.currently_polling = None;
                    match result {
                        Ok(std::task::Poll::Ready(())) => {
                            // Task completed — connection handler is done.
                            driver.close_connection(conn_index);
                            executor.remove_connection(conn_index);
                        }
                        Ok(std::task::Poll::Pending) => {
                            executor.task_slab.park(conn_index, fut);
                            // Self-wake during poll (slot read Empty) —
                            // re-queue now that the task is parked.
                            if executor.woken_while_polling {
                                let _ = executor.wake_task(conn_index);
                            }
                        }
                        Err(_panic) => {
                            // A panicking connection handler tears down the
                            // connection but must not take the worker
                            // thread with it.
                            drop(fut);
                            driver.close_connection(conn_index);
                            executor.remove_connection(conn_index);
                            eprintln!(
                                "ringline: connection task panicked; connection {conn_index} closed"
                            );
                        }
                    }
                }
            }
        }

        // Reset dedup bits for the initial-batch entries only (those are the
        // only ones whose bits could have been set). Zero extra allocation —
        // we index into the ready_queue we already hold.
        let reset_end = initial_len.min(executor.ready_queue.len());
        for idx in 0..reset_end {
            let raw_id = executor.ready_queue[idx];
            if raw_id & STANDALONE_BIT != 0 {
                let task_idx = (raw_id & !STANDALONE_BIT) as usize;
                if task_idx < executor.poll_dedup_standalone.len() {
                    executor.poll_dedup_standalone[task_idx] = false;
                }
            } else {
                let conn_index = raw_id as usize;
                if conn_index < executor.poll_dedup_conn.len() {
                    executor.poll_dedup_conn[conn_index] = false;
                }
            }
        }

        drop(driver_state_guard);

        // Clear processed entries.
        executor.ready_queue.clear();

        // Drain any wakeups that happened during polling.
        executor.collect_wakeups();
    }

    fn drain_completions(&mut self) {
        // One arrival timestamp per batch for queued UDP datagrams (a vDSO
        // clock call per packet showed up on the hot path).
        if !self.driver.udp_sockets.is_empty() {
            self.driver.udp_batch_recv_at = std::time::Instant::now();
        }
        self.driver.cqe_batch.clear();

        {
            let cq = self.driver.ring.ring.completion();
            for cqe in cq {
                self.driver
                    .cqe_batch
                    .push((cqe.user_data(), cqe.result(), cqe.flags()));
            }
        }

        if let Some(interval) = self.driver.flush_interval {
            let mut last_flush = Instant::now();
            for i in 0..self.driver.cqe_batch.len() {
                let (user_data_raw, result, flags) = self.driver.cqe_batch[i];
                self.dispatch_cqe(user_data_raw, result, flags);
                // Check the clock every 16 CQEs to amortise Instant::now() cost.
                if (i & 0xF) == 0xF {
                    let now = Instant::now();
                    if now.duration_since(last_flush) >= interval {
                        // Best effort latency optimization; SQEs submitted by next submit_and_wait.
                        let _ = self.driver.ring.flush();
                        last_flush = now;
                    }
                }
            }
        } else {
            for i in 0..self.driver.cqe_batch.len() {
                let (user_data_raw, result, flags) = self.driver.cqe_batch[i];
                self.dispatch_cqe(user_data_raw, result, flags);
            }
        }

        // Eagerly return consumed recv buffers to the kernel ring in the same
        // iteration they were consumed, keeping the ring fuller under burst.
        self.flush_replenish_and_rearm();
    }

    /// Commit pending provided-buffer returns to the kernel ring and re-arm
    /// any connections whose multishot recv was parked on ENOBUFS.
    ///
    /// Safe because every bid pushed to `pending_replenish` had its contents
    /// copied out (into the accumulator / recv sink / TLS state) before being
    /// pushed — dispatch has fully completed, so no handler still references
    /// these buffers. Zero-copy held buffers are tracked in
    /// `pending_recv_bufs` / `recv_hold` slots and are never in this queue.
    ///
    /// Also called from the run loop right before the blocking wait: bids
    /// released by tasks during the poll pass would otherwise sit uncommitted
    /// (and starved connections parked) until the next unrelated CQE.
    fn flush_replenish_and_rearm(&mut self) {
        // Starved connections holding a zero-copy single-buffer hold
        // (`pending_recv_bufs`) with data the parser hasn't consumed: flush
        // the hold into the accumulator so its bid can rejoin the ring.
        // This both frees a buffer (often enough to revive the multishot
        // below) and moves the partial message where the fallback path
        // expects it.
        if !self.driver.recv_starved.is_empty() {
            for i in 0..self.driver.recv_starved.len() {
                let conn_index = self.driver.recv_starved[i];
                if let Some(pending) = self.driver.pending_recv_bufs[conn_index as usize].take() {
                    let data =
                        unsafe { std::slice::from_raw_parts(pending.ptr, pending.len as usize) };
                    if !self.driver.accumulators.append(conn_index, data) {
                        self.executor.wake_recv(conn_index);
                        self.driver.close_connection(conn_index);
                    }
                    self.driver.pending_replenish.push(pending.bid);
                }
            }
        }

        // Commit returned bids up front so re-armed multishots find them.
        let replenished = if !self.driver.pending_replenish.is_empty() {
            self.driver
                .provided_bufs
                .replenish_batch(&self.driver.pending_replenish);
            self.driver.pending_replenish.clear();
            true
        } else {
            false
        };

        if self.driver.recv_starved.is_empty() {
            return;
        }

        // Arbitrate each parked connection:
        //
        // - Fallback in flight: stays parked untouched — a multishot armed
        //   alongside the outstanding one-shot could append out of order
        //   (io_uring does not order independent SQEs). The fallback's
        //   completion re-parks it and a later pass hands off.
        // - Partial message on the plaintext accumulator path: prefer a
        //   fallback recv EVEN IF buffers were replenished. Re-arming the
        //   multishot moves at most one ring's worth before parking again —
        //   with responses larger than the ring that park/re-arm churn is
        //   the pathology (per-pass throughput = ring capacity × pass
        //   rate), while a fallback moves one `fallback_chunk` (> ring
        //   capacity) per pass and never closes the TCP window. The
        //   multishot resumes once the message completes and the
        //   accumulator drains.
        // - Everything else: re-arm the multishot when buffers came back,
        //   otherwise keep waiting (nothing is half-delivered).
        let mut i = 0;
        while i < self.driver.recv_starved.len() {
            let conn_index = self.driver.recv_starved[i];
            if self.driver.recv_fallback_inflight[conn_index as usize] {
                i += 1;
                continue;
            }
            // A forwarding connection throttled by the Mode A hold cap owns its own
            // re-arm (`maybe_rearm_throttled_forward`, gated on the hold draining
            // below the cap). Leave it parked here so the two paths do not both
            // arm a multishot.
            if self.driver.forward_hold_throttled[conn_index as usize] {
                i += 1;
                continue;
            }
            let alive = self
                .driver
                .connections
                .get(conn_index)
                .is_some_and(|c| matches!(c.recv_mode, RecvMode::Multi));
            if !alive {
                self.driver.recv_starved.swap_remove(i);
                continue;
            }
            if self.fallback_eligible(conn_index)
                && self.driver.try_submit_fallback_recv(conn_index)
            {
                self.driver.recv_starved.swap_remove(i);
                continue;
            }
            if replenished {
                self.driver.recv_starved.swap_remove(i);
                if self.driver.ring.submit_multishot_recv(conn_index).is_err() {
                    metrics::RING.increment(metrics::ring::RECV_ARM_FAILURES);
                    self.executor.wake_recv(conn_index);
                    self.driver.close_connection(conn_index);
                } else if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.recv_multishot_armed = true;
                }
                continue;
            }
            i += 1;
        }
    }

    /// Whether a parked connection may take the fallback recv path:
    /// plaintext accumulator route only (no TLS, recv sink, zero-copy
    /// forward, or direct echo — those paths keep the park-until-replenish
    /// behavior) with a partial message already accumulated. The caller
    /// has already checked liveness and that no fallback is in flight.
    fn fallback_eligible(&mut self, conn_index: u32) -> bool {
        let ci = conn_index as usize;
        let is_tls = self
            .driver
            .tls_table
            .as_ref()
            .is_some_and(|t| t.has(conn_index));
        let is_direct_echo = self
            .driver
            .connections
            .get(conn_index)
            .is_some_and(|c| c.direct_echo);
        if is_tls
            || is_direct_echo
            || self.driver.recv_forward[ci]
            || self.executor.recv_sinks[ci].is_some()
        {
            return false;
        }
        // Only degrade when a message is half-delivered; an empty
        // accumulator means nothing is torn and the connection can simply
        // wait for replenish (holds were flushed above).
        !self.driver.accumulators.data(conn_index).is_empty()
    }

    /// Completion of a fallback one-shot recv (`OpTag::RecvFallback`).
    ///
    /// The payload carries the fallback pool slot; the recorded
    /// `(conn_index, generation)` owner is validated before any connection
    /// state is touched — slots recycle and stale CQEs are normal. The
    /// pool slot is released here and only here, so the kernel's write
    /// target stays valid for exactly the life of the operation.
    fn handle_recv_fallback(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let slot = ud.payload() as u16;

        let pool_ok = self
            .driver
            .fallback_recv_pool
            .as_ref()
            .is_some_and(|p| p.in_use(slot));
        if !pool_ok {
            return;
        }
        let (owner_conn, owner_gen) = self.driver.fallback_slot_owner[slot as usize];
        let stale = owner_conn != conn_index
            || self.driver.connections.generation(conn_index) != owner_gen
            || self.driver.connections.get(conn_index).is_none();
        if stale {
            // The connection this recv was submitted for is gone; the data
            // (if any) belongs to a closed socket. Release the slot only.
            self.driver
                .fallback_recv_pool
                .as_mut()
                .expect("checked in_use above")
                .release(slot);
            return;
        }

        self.driver.recv_fallback_inflight[conn_index as usize] = false;

        if result < 0 {
            self.driver
                .fallback_recv_pool
                .as_mut()
                .expect("checked in_use above")
                .release(slot);
            if -result == libc::ECANCELED {
                // Cancelled, connection still alive: re-park so a later
                // flush re-arms the multishot (or retries the fallback) —
                // otherwise no recv is armed and the connection hangs.
                if !self.driver.recv_starved.contains(&conn_index) {
                    self.driver.recv_starved.push(conn_index);
                }
                return;
            }
            self.executor.wake_recv(conn_index);
            self.driver.close_connection(conn_index);
            return;
        }

        if result == 0 {
            // TCP FIN mid-message (fallback only runs with a partial
            // message accumulated): same truncation semantics as the
            // multishot path for plaintext connections.
            self.driver
                .fallback_recv_pool
                .as_mut()
                .expect("checked in_use above")
                .release(slot);
            self.executor.wake_recv(conn_index);
            self.driver.close_connection(conn_index);
            return;
        }

        let bytes_received = result as u32;
        metrics::BYTES.add(metrics::bytes::RECEIVED, bytes_received as u64);
        metrics::BYTES.add(metrics::bytes::FALLBACK_RECEIVED, bytes_received as u64);

        let pool = self
            .driver
            .fallback_recv_pool
            .as_mut()
            .expect("checked in_use above");
        let (ptr, _) = pool.current_ptr_remaining(slot);
        let data = unsafe { std::slice::from_raw_parts(ptr, bytes_received as usize) };
        let appended = self.driver.accumulators.append(conn_index, data);
        self.driver
            .fallback_recv_pool
            .as_mut()
            .expect("checked in_use above")
            .release(slot);
        if !appended {
            // Streamed past recv_accumulator_max — close rather than OOM,
            // matching the multishot overflow path.
            self.executor.wake_recv(conn_index);
            self.driver.close_connection(conn_index);
            return;
        }

        self.executor.wake_recv(conn_index);
        // Re-park: the multishot is still dead. The next flush either
        // re-arms it (replenish arrived) or continues the fallback chain
        // (parse still incomplete, ring still dry). If the parse completed,
        // the connection waits parked until bids return — exactly the
        // pre-fallback steady state.
        if !self.driver.recv_starved.contains(&conn_index) {
            self.driver.recv_starved.push(conn_index);
        }
    }

    fn dispatch_cqe(&mut self, user_data_raw: u64, result: i32, flags: u32) {
        metrics::RING.increment(metrics::ring::CQE_PROCESSED);
        let ud = UserData(user_data_raw);
        let tag = match ud.tag() {
            Some(t) => t,
            None => {
                // Unknown OpTag — most likely a future enum reorder or a
                // corrupted CQE. In debug builds, panic so the bug surfaces;
                // in release, swallow but increment the metric so it's at
                // least observable.
                debug_assert!(
                    false,
                    "dispatch_cqe: unknown OpTag {:#x} in user_data {:#x}",
                    (user_data_raw >> 56) & 0xFF,
                    user_data_raw,
                );
                metrics::RING.increment(metrics::ring::CQE_UNKNOWN_TAG);
                return;
            }
        };

        match tag {
            OpTag::RecvMulti => self.handle_recv_multi(ud, result, flags),
            OpTag::RecvFallback => self.handle_recv_fallback(ud, result),
            OpTag::Send => self.handle_send(ud, result),
            OpTag::SendMsgZc => self.handle_send_msg_zc(ud, result, flags),
            OpTag::Close => self.handle_close(ud),
            OpTag::Shutdown => {}
            OpTag::EventFdRead => self.handle_eventfd_read(),
            OpTag::TlsSend => self.handle_tls_send(ud, result),
            OpTag::Connect => self.handle_connect(ud, result),
            OpTag::Timeout => self.handle_timeout(ud, result),
            OpTag::Cancel => {}
            OpTag::TickTimeout => {
                self.driver.tick_timeout_armed = false;
            }
            OpTag::Timer => self.handle_timer(ud, result),
            OpTag::RecvMsgUdp => self.handle_recv_msg_udp(ud, result, flags),
            OpTag::SendMsgUdp => self.handle_send_msg_udp(ud, result),
            OpTag::RecvUdp => self.handle_recv_udp(ud, result, flags),
            OpTag::SendUdp => self.handle_send_udp(ud, result),
            OpTag::NvmeCmd => self.handle_nvme_cmd(ud, result),
            OpTag::DirectIo => self.handle_direct_io(ud, result),
            OpTag::Fs => self.handle_fs(ud, result),
            OpTag::PidfdPoll => self.handle_pidfd_poll(ud, result),
            OpTag::SendRecvBuf => self.handle_send_recv_buf(ud, result),
            OpTag::SendPollOut => self.handle_send_pollout(ud, result),
            OpTag::SendMsgCoalesced => self.handle_send_msg_coalesced(ud, result),
            OpTag::SendMsgCoalescedPollOut => self.handle_send_msg_coalesced_pollout(ud, result),
            OpTag::SendRecvBufsCoalesced => self.handle_send_recv_bufs_coalesced(ud, result),
            OpTag::SendRecvBufsCoalescedPollOut => {
                self.handle_send_recv_bufs_coalesced_pollout(ud, result)
            }
            OpTag::ForwardWrite => self.handle_forward_write(ud, result),
            OpTag::ForwardWritePollOut => self.handle_forward_write_pollout(ud, result),
            #[cfg(feature = "timestamps")]
            OpTag::RecvMsgMultiTs => self.handle_recv_msg_multi_ts(ud, result, flags),
        }
    }

    fn handle_recv_multi(&mut self, ud: UserData, result: i32, flags: u32) {
        let conn_index = ud.conn_index();
        let has_more = cqueue::more(flags);

        // A completion without `IORING_CQE_F_MORE` means the kernel terminated
        // this multishot recv. Record that the recv is no longer armed so the
        // close path knows it need not cancel it (a re-arm below sets it back).
        if !has_more && let Some(cs) = self.driver.connections.get_mut(conn_index) {
            cs.recv_multishot_armed = false;
        }

        if self.driver.connections.get(conn_index).is_none() {
            // Connection already released — but if result > 0, the kernel
            // consumed a provided buffer that must be replenished.
            if result > 0
                && let Some(bid) = cqueue::buffer_select(flags)
            {
                self.driver.provided_bufs.on_handout();
                self.driver.pending_replenish.push(bid);
            }
            return;
        }

        if result <= 0 {
            if result == 0 {
                // TCP FIN. For a TLS connection this is only a clean EOF if
                // the peer's close_notify was processed first — otherwise
                // it's a truncation (possibly an attacker-injected FIN) and
                // recv futures must surface UnexpectedEof, not clean EOF.
                let close_notify_seen = self
                    .driver
                    .tls_table
                    .as_mut()
                    .and_then(|t| t.get_mut(conn_index))
                    .map(|tc| tc.peer_sent_close_notify);
                if close_notify_seen == Some(false)
                    && let Some(cs) = self.driver.connections.get_mut(conn_index)
                {
                    cs.eof_truncated = true;
                }
                // Wake recv waiter before closing so the owning task can
                // detect EOF (with_data will see RecvMode::Closed and return 0).
                self.executor.wake_recv(conn_index);
                self.driver.close_connection(conn_index);
                return;
            }
            let errno = -result;
            if errno == libc::ENOBUFS {
                metrics::POOL.increment(metrics::pool::BUFFER_RING_EMPTY);
                // Park until buffers return to the provided ring (see
                // flush_replenish_and_rearm). Re-arming immediately
                // completed with ENOBUFS again while data was pending and
                // the ring was empty — submit_and_wait(1) never blocked and
                // the worker spun at 100% CPU until a task freed a bid.
                if !has_more && !self.driver.recv_starved.contains(&conn_index) {
                    self.driver.recv_starved.push(conn_index);
                    self.driver.recv_park_count += 1;
                    metrics::POOL.increment(metrics::pool::RECV_PARKED);
                }
            } else if errno == libc::ECANCELED {
                // A cancel terminated the multishot. If this connection was
                // throttled by the Mode A hold cap, this is the ECANCELED for that
                // throttle-cancel — `recv_multishot_armed` was just cleared at the
                // top of the handler, so try to re-arm now if the hold has already
                // drained below the cap (otherwise a later write completion will).
                self.maybe_rearm_throttled_forward(conn_index);
                return;
            } else if !has_more {
                self.executor.wake_recv(conn_index);
                self.driver.close_connection(conn_index);
            }
            return;
        }

        let bid = match cqueue::buffer_select(flags) {
            Some(bid) => bid,
            None => {
                // No buffer selected despite result > 0 — should not happen.
                // Close the connection to prevent a silent hang (no recv armed).
                if !has_more {
                    self.executor.wake_recv(conn_index);
                    self.driver.close_connection(conn_index);
                }
                return;
            }
        };

        self.driver.provided_bufs.on_handout();
        let bytes_received = result as u32;
        metrics::BYTES.add(metrics::bytes::RECEIVED, bytes_received as u64);
        let (buf_ptr, _) = self.driver.provided_bufs.get_buffer(bid);
        let data = unsafe { std::slice::from_raw_parts(buf_ptr, bytes_received as usize) };

        // NOTE: bid is NOT unconditionally pushed to pending_replenish here.
        // The zero-copy recv path defers replenishment until the task consumes
        // the data. Each branch below is responsible for either pushing the bid
        // to pending_replenish or storing it in a pending_recv_bufs slot.

        // TLS path
        let is_tls_conn = self
            .driver
            .tls_table
            .as_ref()
            .is_some_and(|t| t.has(conn_index));

        if is_tls_conn {
            // The ciphertext bid is replenished immediately (TLS decrypts into
            // rustls's own buffer, so the provided buffer is free at feed time);
            // TLS segments never pin the ring.
            self.driver.pending_replenish.push(bid);
            {
                // Route decrypted plaintext by recv domain. In the segmented
                // domain, each drained plaintext chunk becomes an owned segment
                // pushed to this connection's hold (copy-per-chunk — rustls owns
                // the plaintext, so TLS recv can never be zero-copy; see
                // `docs/segmented-recv-design.md`, "## TLS"). Otherwise it lands
                // in the recv accumulator (the default with_data/with_bytes path).
                let is_segmented = self.driver.recv_domain[conn_index as usize]
                    == crate::recv::domain::RecvDomain::Segmented;
                let recv_accumulator_max = self.driver.recv_accumulator_max;
                let tls_table = self.driver.tls_table.as_mut().unwrap();
                let sink = if is_segmented {
                    // Bound total outstanding held plaintext exactly as the
                    // accumulator path bounds its buffer (recv_accumulator_max):
                    // an unbounded plaintext flood must still kill the connection.
                    // TLS holds are always `Owned`, but sum defensively.
                    let hold = &mut self.driver.segment_hold[conn_index as usize];
                    let outstanding: usize = hold
                        .iter()
                        .map(|h| match h {
                            crate::backend::HeldRecvBuf::Owned(b) => b.len(),
                            crate::backend::HeldRecvBuf::Pinned { len, .. } => *len as usize,
                        })
                        .sum();
                    crate::tls::PlaintextSink::Segments {
                        hold,
                        outstanding,
                        max: recv_accumulator_max,
                    }
                } else {
                    crate::tls::PlaintextSink::Accumulator(&mut self.driver.accumulators)
                };
                let result = crate::tls::feed_tls_recv(
                    tls_table,
                    sink,
                    &mut self.driver.send_copy_pool,
                    conn_index,
                    data,
                    &mut self.driver.tls_out_scratch,
                );

                // Route collected TLS output (handshake responses, alerts)
                // through the per-connection send queue — including on the
                // Error path, where an alert should still go out before the
                // close below.
                if !self.driver.tls_out_scratch.is_empty() {
                    let mut sends = std::mem::take(&mut self.driver.tls_out_scratch);
                    let _ = self.driver.queue_built_sends(conn_index, &mut sends);
                    self.driver.tls_out_scratch = sends;
                }

                match result {
                    crate::tls::TlsRecvResult::HandshakeJustCompleted => {
                        let is_outbound = self
                            .driver
                            .connections
                            .get(conn_index)
                            .map(|c| c.outbound)
                            .unwrap_or(false);

                        if is_outbound {
                            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                                cs.established = true;
                            }
                            // Wake connect waiter.
                            self.executor.wake_connect(conn_index, Ok(()));
                        } else {
                            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                                cs.established = true;
                            }
                            metrics::CONNECTIONS.increment(metrics::conn::ACCEPTED);
                            metrics::CONNECTIONS_ACTIVE.increment();
                            // Spawn async task for accepted connection.
                            self.spawn_accept_task(conn_index);
                        }

                        // Wake recv waiter if data accumulated during handshake.
                        self.executor.wake_recv(conn_index);
                    }
                    crate::tls::TlsRecvResult::Ok => {
                        self.executor.wake_recv(conn_index);
                    }
                    crate::tls::TlsRecvResult::Error(e) => {
                        // Wake connect waiter if handshake hasn't completed yet.
                        let established = self
                            .driver
                            .connections
                            .get(conn_index)
                            .map(|c| c.established)
                            .unwrap_or(false);
                        if !established {
                            let err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, e);
                            self.executor.wake_connect(conn_index, Err(err));
                        }
                        self.executor.wake_recv(conn_index);
                        self.driver.close_connection(conn_index);
                    }
                    crate::tls::TlsRecvResult::Closed => {
                        self.executor.wake_recv(conn_index);
                        self.driver.close_connection(conn_index);
                    }
                }
            }
        } else if self.driver.recv_domain[conn_index as usize]
            == crate::recv::domain::RecvDomain::Segmented
        {
            // Segmented delivery (Mode B/C). Consult the aggregate low-water
            // reserve on the shared per-worker recv ring (see
            // `docs/segmented-recv-design.md`, "Backpressure and ring safety").
            // `on_handout()` above already counted this bid, so `free()` reflects
            // this delivery.
            let reserve = self.driver.recv_segment_reserve;
            let free = self.driver.provided_bufs.free();
            match crate::recv::occupancy::delivery_decision(free, reserve) {
                crate::recv::occupancy::Delivery::ZeroCopyOk => {
                    // Above the reserve: hold the provided buffer in-place (bid
                    // NOT replenished, no accumulator copy) for a future segment
                    // reader. The buffer stays pinned until the reader or
                    // `close_connection` drains the hold. Backpressure is natural
                    // — unreplenished bids deplete the ring (ENOBUFS) until a
                    // reader releases them, exactly like the recv-forward hold.
                    self.driver.segment_hold[conn_index as usize].push_back(
                        crate::backend::HeldRecvBuf::Pinned {
                            bid,
                            len: bytes_received,
                        },
                    );
                }
                crate::recv::occupancy::Delivery::ForceCopy => {
                    // At/below the reserve: copy the bytes into an owned `Bytes`
                    // and replenish the bid IMMEDIATELY so the ring recovers and
                    // holders cannot deplete it. `on_handout()` counted the bid at
                    // buffer_select; this replenish balances it (net-zero pin), so
                    // the outstanding/free accounting stays consistent. INC
                    // ordering: copy before replenish, no await between.
                    let owned = bytes::Bytes::copy_from_slice(data);
                    self.driver.segment_hold[conn_index as usize]
                        .push_back(crate::backend::HeldRecvBuf::Owned(owned));
                    self.driver.pending_replenish.push(bid);
                }
            }
            // Mode A hold cap (see `docs/segmented-recv-design.md`, "Mode A"). A
            // `forward_to` connection whose held-buffer backlog reaches
            // `forward_hold_cap` (a slow/high-latency sink, or a very large
            // object) would otherwise pin much of the shared per-worker ring (and
            // grow heap when the reserve force-copies), starving other
            // connections. Throttle it: cancel its multishot recv so its TCP
            // receive window closes and the source stops sending. The recv is
            // re-armed once writes drain the hold below the cap
            // (`maybe_rearm_throttled_forward`). Applies only to forwarders
            // (`forward_recv_active`), not pure Mode B segment readers.
            let ci = conn_index as usize;
            if self.driver.forward_recv_active[ci]
                && !self.driver.forward_hold_throttled[ci]
                && self.driver.segment_hold[ci].len() >= self.driver.forward_hold_cap
            {
                self.driver.forward_hold_throttled[ci] = true;
                // Only cancel a still-armed multishot. If this CQE terminated the
                // multishot (`!has_more` cleared `recv_multishot_armed` at the top
                // of the handler), there is nothing to cancel — the re-arm gate
                // below (`!has_more`) already skips re-arming a throttled conn.
                let armed = self
                    .driver
                    .connections
                    .get(conn_index)
                    .is_some_and(|c| c.recv_multishot_armed);
                if armed {
                    // Cancel by the RecvMulti user_data (targets the request, not
                    // the fd — immune to reordering). `recv_multishot_armed` stays
                    // set until the ECANCELED CQE clears it (top of the handler),
                    // which gates re-arm so two multishots with the same user_data
                    // never overlap.
                    let recv_ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
                    let _ = self
                        .driver
                        .ring
                        .submit_async_cancel(recv_ud.raw(), conn_index);
                    metrics::POOL.increment(metrics::pool::FORWARD_THROTTLED);
                }
            }
            self.executor.wake_recv(conn_index);
        } else if self.driver.recv_forward[conn_index as usize] {
            // Zero-copy recv-forward path: hold the provided buffer in-place
            // (bid NOT replenished) for scatter-gather forwarding via
            // `forward_held`. No accumulator copy. Backpressure is natural —
            // unreplenished bids deplete the ring (ENOBUFS) until a forward
            // completes. The hold is drained on close (see close_connection).
            self.driver.recv_hold[conn_index as usize].push_back(crate::backend::PendingRecvBuf {
                bid,
                len: bytes_received,
                ptr: buf_ptr,
            });
            self.executor.wake_recv(conn_index);
        } else {
            // Direct echo fast path: submit the echo SQE directly from the CQE
            // handler, bypassing task wakeup entirely. This eliminates the
            // collect_wakeups → poll_ready_tasks roundtrip (~1 full event-loop
            // iteration of latency) on the hot single-connection echo path.
            let is_direct_echo = self
                .driver
                .connections
                .get(conn_index)
                .is_some_and(|c| c.direct_echo);

            if is_direct_echo {
                // Payload carries only the bid; remaining is in send_recv_buf_remaining.
                let payload = bid as u32;
                let ud = UserData::encode(OpTag::SendRecvBuf, conn_index, payload);
                let entry = io_uring::opcode::Send::new(
                    io_uring::types::Fixed(conn_index),
                    buf_ptr,
                    bytes_received,
                )
                .flags(crate::completion::STREAM_SEND_FLAGS)
                .build()
                .user_data(ud.raw());
                let built = crate::handler::BuiltSend {
                    entry,
                    pool_slot: u16::MAX,
                    slab_idx: u16::MAX,
                    total_len: bytes_received,
                };
                self.driver.send_recv_buf_original_lens[conn_index as usize] = bytes_received;
                self.driver.send_recv_buf_remaining[conn_index as usize] = bytes_received;
                if self.driver.submit_or_queue_send(conn_index, built).is_err() {
                    // SQ full — replenish and give up on this echo.
                    self.driver.pending_replenish.push(bid);
                }
                // Do NOT call wake_recv here. DirectEchoFuture only needs to
                // be woken on connection close (handled by the result <= 0 path
                // above), not on every incoming buffer.
            } else {
                // Plaintext path: route through recv sink if active, else zero-copy/accumulator.
                if let Some(sink) = &mut self.executor.recv_sinks[conn_index as usize] {
                    self.driver.pending_replenish.push(bid);
                    let remaining_cap = sink.cap - sink.pos;
                    let to_sink = data.len().min(remaining_cap);
                    if to_sink > 0 {
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                data.as_ptr(),
                                sink.ptr.add(sink.pos),
                                to_sink,
                            );
                        }
                        sink.pos += to_sink;
                    }
                    // Overflow (trailing CRLF, next commands) goes to accumulator.
                    if to_sink < data.len()
                        && !self
                            .driver
                            .accumulators
                            .append(conn_index, &data[to_sink..])
                    {
                        self.executor.wake_recv(conn_index);
                        self.driver.close_connection(conn_index);
                        return;
                    }
                } else {
                    // Zero-copy fast path: if no pending buffer AND accumulator is
                    // empty, hold the kernel buffer in-place instead of copying.
                    // NOTE: must be the non-merging `is_empty` — `data()` here
                    // would merge a held frozen remainder on every recv CQE,
                    // a full-remainder copy per chunk while a large response
                    // streams in (O(N·K)).
                    let acc_empty = self.driver.accumulators.is_empty(conn_index);
                    let slot = &mut self.driver.pending_recv_bufs[conn_index as usize];

                    if acc_empty && slot.is_none() {
                        *slot = Some(crate::backend::PendingRecvBuf {
                            bid,
                            len: bytes_received,
                            ptr: buf_ptr,
                        });
                    } else {
                        // Flush any existing pending buffer to accumulator first.
                        let mut accumulator_overflowed = false;
                        if let Some(pending) = slot.take() {
                            let pending_data = unsafe {
                                std::slice::from_raw_parts(pending.ptr, pending.len as usize)
                            };
                            if !self.driver.accumulators.append(conn_index, pending_data) {
                                accumulator_overflowed = true;
                            }
                            self.driver.pending_replenish.push(pending.bid);
                        }
                        if !accumulator_overflowed
                            && !self.driver.accumulators.append(conn_index, data)
                        {
                            accumulator_overflowed = true;
                        }
                        self.driver.pending_replenish.push(bid);
                        if accumulator_overflowed {
                            // Handler kept returning NeedMore while the peer
                            // streamed past `recv_accumulator_max`. Close the
                            // connection rather than OOM the worker.
                            self.executor.wake_recv(conn_index);
                            self.driver.close_connection(conn_index);
                            return;
                        }
                    }
                }
                self.executor.wake_recv(conn_index);
            }
        }

        if !has_more
            && !self.driver.forward_hold_throttled[conn_index as usize]
            && let Some(conn) = self.driver.connections.get(conn_index)
            && matches!(conn.recv_mode, RecvMode::Multi)
        {
            if self.driver.ring.submit_multishot_recv(conn_index).is_err() {
                metrics::RING.increment(metrics::ring::RECV_ARM_FAILURES);
                self.executor.wake_recv(conn_index);
                self.driver.close_connection(conn_index);
            } else if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                cs.recv_multishot_armed = true;
            }
        }
    }

    /// Handle a RecvMsgMulti CQE (multishot recvmsg with SO_TIMESTAMPING).
    ///
    /// The provided buffer contains an `io_uring_recvmsg_out` header followed by
    /// name (0 bytes for TCP), control data (cmsg with SCM_TIMESTAMPING), and
    /// the TCP payload.
    #[cfg(feature = "timestamps")]
    fn handle_recv_msg_multi_ts(&mut self, ud: UserData, result: i32, flags: u32) {
        let conn_index = ud.conn_index();
        let has_more = cqueue::more(flags);

        if self.driver.connections.get(conn_index).is_none() {
            if result > 0
                && let Some(bid) = cqueue::buffer_select(flags)
            {
                self.driver.provided_bufs.on_handout();
                self.driver.pending_replenish.push(bid);
            }
            return;
        }

        if result <= 0 {
            if result == 0 {
                self.executor.wake_recv(conn_index);
                self.driver.close_connection(conn_index);
                return;
            }
            let errno = -result;
            if errno == libc::ENOBUFS {
                metrics::POOL.increment(metrics::pool::BUFFER_RING_EMPTY);
                if !has_more {
                    let msghdr_ptr = &*self.driver.recvmsg_msghdr as *const libc::msghdr;
                    let _ = self
                        .driver
                        .ring
                        .submit_multishot_recvmsg(conn_index, msghdr_ptr);
                }
            } else if errno == libc::ECANCELED {
                return;
            } else if !has_more {
                self.executor.wake_recv(conn_index);
                self.driver.close_connection(conn_index);
            }
            return;
        }

        let bid = match cqueue::buffer_select(flags) {
            Some(bid) => bid,
            None => {
                if !has_more {
                    self.executor.wake_recv(conn_index);
                    self.driver.close_connection(conn_index);
                }
                return;
            }
        };

        self.driver.provided_bufs.on_handout();
        let buf_len = result as u32;
        let (buf_ptr, _) = self.driver.provided_bufs.get_buffer(bid);
        let buf = unsafe { std::slice::from_raw_parts(buf_ptr, buf_len as usize) };

        self.driver.pending_replenish.push(bid);

        // Parse the io_uring_recvmsg_out header to extract control data + payload.
        let msg_out = match io_uring::types::RecvMsgOut::parse(buf, &self.driver.recvmsg_msghdr) {
            Ok(out) => out,
            Err(()) => {
                // Parse failed — treat as regular data (shouldn't happen).
                return;
            }
        };

        let payload = msg_out.payload_data();
        if payload.is_empty() {
            // EOF via recvmsg.
            self.executor.wake_recv(conn_index);
            self.driver.close_connection(conn_index);
            return;
        }

        metrics::BYTES.add(metrics::bytes::RECEIVED, payload.len() as u64);

        // Extract SCM_TIMESTAMPING from control data.
        let control = msg_out.control_data();
        if let Some(ts_ns) = Self::parse_scm_timestamp(control) {
            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                cs.recv_timestamp_ns = ts_ns;
            }
        }

        // Route payload through accumulator (same as plaintext RecvMulti path).
        if let Some(sink) = &mut self.executor.recv_sinks[conn_index as usize] {
            let remaining_cap = sink.cap - sink.pos;
            let to_sink = payload.len().min(remaining_cap);
            if to_sink > 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        payload.as_ptr(),
                        sink.ptr.add(sink.pos),
                        to_sink,
                    );
                }
                sink.pos += to_sink;
            }
            if to_sink < payload.len()
                && !self
                    .driver
                    .accumulators
                    .append(conn_index, &payload[to_sink..])
            {
                self.executor.wake_recv(conn_index);
                self.driver.close_connection(conn_index);
                return;
            }
        } else if !self.driver.accumulators.append(conn_index, payload) {
            self.executor.wake_recv(conn_index);
            self.driver.close_connection(conn_index);
            return;
        }
        self.executor.wake_recv(conn_index);

        if !has_more
            && let Some(conn) = self.driver.connections.get(conn_index)
            && matches!(conn.recv_mode, RecvMode::MsgMulti)
        {
            let msghdr_ptr = &*self.driver.recvmsg_msghdr as *const libc::msghdr;
            let _ = self
                .driver
                .ring
                .submit_multishot_recvmsg(conn_index, msghdr_ptr);
        }
    }

    /// Parse SCM_TIMESTAMPING from cmsg control data.
    /// Returns the software RX timestamp as nanoseconds since epoch, or None.
    #[cfg(feature = "timestamps")]
    fn parse_scm_timestamp(control: &[u8]) -> Option<u64> {
        // cmsg layout: cmsghdr { cmsg_len (usize), cmsg_level (i32), cmsg_type (i32) }
        // followed by payload data, then padding to align next cmsghdr.
        let hdr_size = std::mem::size_of::<libc::cmsghdr>();
        let align = std::mem::align_of::<libc::cmsghdr>();
        let mut offset = 0usize;

        while offset + hdr_size <= control.len() {
            // Safety: read_unaligned handles the case where control is not
            // aligned to cmsghdr's alignment requirement.
            let hdr_ptr = control[offset..].as_ptr() as *const libc::cmsghdr;
            let hdr = unsafe { std::ptr::read_unaligned(hdr_ptr) };

            if hdr.cmsg_len < hdr_size {
                break;
            }

            let data_offset = offset + hdr_size;
            let data_len = hdr.cmsg_len - hdr_size;

            if hdr.cmsg_level == libc::SOL_SOCKET && hdr.cmsg_type == libc::SO_TIMESTAMPING {
                // Payload is 3 × struct timespec: [software, hw_transformed, hw_raw].
                // We want the software timestamp (index 0).
                let ts_size = std::mem::size_of::<libc::timespec>();
                if data_len >= ts_size && data_offset + ts_size <= control.len() {
                    let ts_ptr = control[data_offset..].as_ptr() as *const libc::timespec;
                    let ts = unsafe { std::ptr::read_unaligned(ts_ptr) };
                    if ts.tv_sec != 0 || ts.tv_nsec != 0 {
                        return Some(ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64);
                    }
                }
            }

            // Advance to next cmsg (aligned).
            let next = offset + ((hdr.cmsg_len + align - 1) & !(align - 1));
            if next <= offset {
                break;
            }
            offset = next;
        }

        None
    }

    fn handle_eventfd_read(&mut self) {
        // Drain accept channel (server mode only).
        {
            loop {
                let item = match self.driver.accept_rx {
                    Some(ref rx) => rx.try_recv().ok(),
                    None => None,
                };
                let Some((raw_fd, peer_addr)) = item else {
                    break;
                };

                let conn_index = match self.driver.connections.allocate() {
                    Some(idx) => idx,
                    None => {
                        unsafe {
                            libc::close(raw_fd);
                        }
                        continue;
                    }
                };

                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.peer_addr = Some(crate::connection::PeerAddr::Tcp(peer_addr));
                }

                if self
                    .driver
                    .ring
                    .register_files_update(conn_index, &[raw_fd])
                    .is_err()
                {
                    self.driver.connections.release(conn_index);
                    unsafe {
                        libc::close(raw_fd);
                    }
                    continue;
                }
                unsafe {
                    libc::close(raw_fd);
                }

                if let Some(pending) = self.driver.pending_recv_bufs[conn_index as usize].take() {
                    self.driver.pending_replenish.push(pending.bid);
                }
                self.driver.accumulators.reset(conn_index);
                self.driver.reset_segment_state(conn_index);
                self.arm_recv(conn_index);

                // TLS path: defer accept until handshake completes.
                if let Some(ref mut tls_table) = self.driver.tls_table
                    && tls_table.has_server_config()
                {
                    if tls_table.create(conn_index).is_err() {
                        self.driver.close_connection(conn_index);
                    }
                    continue;
                }

                // Plaintext path: mark established and spawn async task.
                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.established = true;
                }
                metrics::CONNECTIONS.increment(metrics::conn::ACCEPTED);
                metrics::CONNECTIONS_ACTIVE.increment();
                self.spawn_accept_task(conn_index);
            }
        }

        // Drain DNS resolve responses.
        if let Some(ref rx) = self.driver.resolve_rx {
            while let Ok(response) = rx.try_recv() {
                self.executor
                    .deliver_resolve(response.request_id, response.result);
            }
        }

        // Drain process spawn responses.
        if let Some(ref rx) = self.driver.spawn_rx {
            while let Ok(response) = rx.try_recv() {
                self.executor
                    .deliver_spawn(response.request_id, response.result);
            }
        }

        // Drain blocking responses.
        if let Some(ref rx) = self.driver.blocking_rx {
            while let Ok(response) = rx.try_recv() {
                self.executor
                    .deliver_blocking(response.request_id, response.result);
            }
        }

        // on_notify (synchronous). Set the executor's driver_state
        // thread-local so user code that calls `ringline::spawn()` /
        // wakers / `with_state` works from inside the handler. Raw
        // pointers dodge the borrow conflict with `make_ctx()`.
        {
            let handler = &mut self.handler;
            let driver_ptr = &mut self.driver as *mut Driver;
            let executor_ptr = &mut self.executor as *mut crate::runtime::Executor;
            let mut driver_state = DriverState {
                driver: unsafe { NonNull::new_unchecked(driver_ptr) },
                executor: unsafe { NonNull::new_unchecked(executor_ptr) },
            };
            let guard = unsafe { set_driver_state_guarded(&mut driver_state) };
            {
                let mut ctx = unsafe { (*driver_ptr).make_ctx() };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    handler.on_notify(&mut ctx);
                }));
                if result.is_err() {
                    eprintln!("ringline: handler on_notify panicked; continuing");
                }
            }
            drop(guard);
        }

        // Re-arm eventfd read. Track whether the re-arm succeeded so the
        // event loop can retry on the next tick if the SQ was full.
        if !self.driver.shutdown_flag.load(Ordering::Relaxed) {
            self.driver.eventfd_armed = self
                .driver
                .ring
                .submit_eventfd_read(self.driver.eventfd, self.driver.eventfd_buf.as_mut_ptr())
                .is_ok();
        }
    }

    fn handle_send(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let pool_slot = ud.payload() as u16;

        // Guard against stale CQE for an already-released pool slot
        // (e.g., Close CQE processed before this Send CQE in the same batch).
        if !self.driver.send_copy_pool.in_use(pool_slot) {
            return;
        }

        // Chain path.
        if self.driver.chain_table.is_active(conn_index) {
            self.driver.send_copy_pool.release(pool_slot);
            let event = self.driver.chain_table.on_operation_cqe(conn_index, result);
            if matches!(event, ChainEvent::Complete { .. }) {
                self.fire_chain_complete(conn_index);
            }
            return;
        }

        if result > 0 {
            if let Some((ptr, remaining)) = self
                .driver
                .send_copy_pool
                .try_advance(pool_slot, result as u32)
            {
                if self
                    .driver
                    .ring
                    .submit_send_copied(conn_index, ptr, remaining, pool_slot)
                    .is_err()
                {
                    // SQ full — queue for retry on next tick.
                    let generation = self.driver.connections.generation(conn_index);
                    self.driver.pending_copy_retries.push((
                        conn_index,
                        generation,
                        pool_slot,
                        0,
                        OpTag::Send,
                    ));
                }
                return;
            }
            let total = self.driver.send_copy_pool.original_len(pool_slot);
            // Read the end-of-send flag before releasing the slot.
            let end_of_send = self.driver.send_copy_pool.is_end_of_send(pool_slot);
            metrics::BYTES.add(metrics::bytes::SENT, total as u64);
            self.driver.send_copy_pool.release(pool_slot);

            // Accumulate this chunk's bytes against the logical send.
            self.driver.send_queues[conn_index as usize].acked_bytes += total;

            // Pop the next queued send (if any) into the kernel,
            // *then* check whether a deferred close should fire —
            // submit_next_queued may have just emptied the queue and
            // cleared `in_flight`, which is exactly when a
            // close_pending connection is ready to actually close.
            self.driver.submit_next_queued(conn_index);
            self.driver.note_send_finalized(conn_index);

            // Wake the send waiter once, when this logical send's final chunk
            // completes, reporting its whole byte count. Intermediate chunks of
            // a multi-slot send only accumulate; waking on one would report a
            // short count, and pipelined independent sends share this queue so
            // waking on queue-drain would wake the wrong future.
            if end_of_send {
                let acked =
                    std::mem::take(&mut self.driver.send_queues[conn_index as usize].acked_bytes);
                self.executor.wake_send(conn_index, Ok(acked));
            }
            return;
        }

        // `EAGAIN` / `EWOULDBLOCK` from the kernel means the socket
        // send buffer is full and we'd block. The right response is to
        // wait for `POLLOUT` and resubmit the same data — the pool
        // slot still holds the unsent bytes via
        // `current_ptr_remaining`. Don't release the slot, don't drain
        // the queue, don't wake the send waiter.
        let errno = -result;
        if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
            if self
                .driver
                .ring
                .submit_send_pollout(conn_index, pool_slot, false)
                .is_err()
            {
                // SQ full now — queue for retry on the next tick.
                let generation = self.driver.connections.generation(conn_index);
                self.driver
                    .pending_send_pollout_retries
                    .push((conn_index, generation, pool_slot, 0, false));
            }
            metrics::POOL.increment(metrics::pool::SEND_EAGAIN);
            return;
        }

        self.driver.send_copy_pool.release(pool_slot);
        self.driver.drain_conn_send_queue(conn_index);
        self.driver.note_send_finalized(conn_index);

        let io_result = if result == 0 {
            Ok(0u32)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        };
        self.executor.wake_send(conn_index, io_result);
    }

    /// Handle a `POLLOUT` CQE armed after a `Send` returned `-EAGAIN`.
    ///
    /// Resubmits the same send using `current_ptr_remaining(pool_slot)`,
    /// which still points at the unsent bytes inside the same pool slot
    /// we kept alive across the EAGAIN. If the resubmit also fails to
    /// land (SQ full), pushes onto `pending_copy_retries` so the next
    /// tick picks it up.
    fn handle_send_pollout(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        // Payload: pool_slot in the low 16 bits, is_tls flag in bit 16 —
        // the resubmit must keep a TLS chunk on the TlsSend completion path.
        let pool_slot = ud.payload() as u16;
        let is_tls = ud.payload() & (1 << 16) != 0;

        // Connection may have been closed while we were waiting; in
        // that case the pool slot was released and the queue drained
        // by `close_connection`.
        if !self.driver.send_copy_pool.in_use(pool_slot)
            || self.driver.connections.get(conn_index).is_none()
        {
            return;
        }

        // Poll itself failed (e.g. fd closed unexpectedly). Treat
        // the same as a generic send failure: drop everything for
        // this connection.
        if result < 0 {
            self.driver.send_copy_pool.release(pool_slot);
            self.driver.drain_conn_send_queue(conn_index);
            self.driver.note_send_finalized(conn_index);
            self.executor
                .wake_send(conn_index, Err(io::Error::from_raw_os_error(-result)));
            return;
        }

        let (ptr, remaining) = self.driver.send_copy_pool.current_ptr_remaining(pool_slot);
        let resubmit = if is_tls {
            self.driver
                .ring
                .submit_tls_send(conn_index, ptr, remaining, pool_slot)
        } else {
            self.driver
                .ring
                .submit_send_copied(conn_index, ptr, remaining, pool_slot)
        };
        if resubmit.is_err() {
            // SQ full — pick up on the next tick.
            let generation = self.driver.connections.generation(conn_index);
            let tag = if is_tls { OpTag::TlsSend } else { OpTag::Send };
            self.driver
                .pending_copy_retries
                .push((conn_index, generation, pool_slot, 0, tag));
        }
    }

    /// Release the backing pool slots of a coalesced send, then the slab entry.
    fn release_coalesced(&mut self, slab_idx: u16) {
        let mut slots = [u16::MAX; crate::buffer::send_slab::MAX_IOVECS];
        let mut n = 0;
        for &s in self.driver.send_slab.coalesced_pool_slots(slab_idx) {
            slots[n] = s;
            n += 1;
        }
        for &s in &slots[..n] {
            self.driver.send_copy_pool.release(s);
        }
        self.driver.send_slab.release(slab_idx);
    }

    /// Handle completion of a coalesced plaintext `sendmsg` (OpTag::SendMsgCoalesced).
    /// Mirrors `handle_send` but the backing is a slab entry holding several
    /// pool slots; partial sends advance the iovec array via `try_advance`.
    fn handle_send_msg_coalesced(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let slab_idx = ud.payload() as u16;

        // Guard against a stale CQE for an already-released slab entry.
        if !self.driver.send_slab.in_use(slab_idx) {
            return;
        }

        if result > 0 {
            // Partial send: advance the iovec array and resubmit the remainder.
            if let Some(msg_ptr) = self.driver.send_slab.try_advance(slab_idx, result as u32) {
                if self
                    .driver
                    .ring
                    .submit_send_msg_coalesced(conn_index, msg_ptr, slab_idx)
                    .is_err()
                {
                    let generation = self.driver.connections.generation(conn_index);
                    self.driver
                        .pending_coalesced_retries
                        .push((conn_index, generation, slab_idx, 0));
                }
                return;
            }
            // Fully sent.
            let total = self.driver.send_slab.total_len(slab_idx);
            // Read the end-of-send flag before releasing the slab entry.
            let end_of_send = self.driver.send_slab.is_end_of_send(slab_idx);
            metrics::BYTES.add(metrics::bytes::SENT, total as u64);
            self.release_coalesced(slab_idx);

            // Accumulate these chunks' bytes against the logical send, and wake
            // the waiter once, when the entry carrying the send's final chunk
            // completes, reporting the whole logical byte count. See
            // `ConnSendState::acked_bytes`.
            self.driver.send_queues[conn_index as usize].acked_bytes += total;
            self.driver.submit_next_queued(conn_index);
            self.driver.note_send_finalized(conn_index);
            if end_of_send {
                let acked =
                    std::mem::take(&mut self.driver.send_queues[conn_index as usize].acked_bytes);
                self.executor.wake_send(conn_index, Ok(acked));
            }
            return;
        }

        // EAGAIN/EWOULDBLOCK: socket buffer full — wait for POLLOUT, keep the
        // slab entry (and its data) alive, then resubmit the same sendmsg.
        let errno = -result;
        if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
            if self
                .driver
                .ring
                .submit_send_msg_coalesced_pollout(conn_index, slab_idx)
                .is_err()
            {
                let generation = self.driver.connections.generation(conn_index);
                self.driver
                    .pending_coalesced_retries
                    .push((conn_index, generation, slab_idx, 0));
            }
            metrics::POOL.increment(metrics::pool::SEND_EAGAIN);
            return;
        }

        // Real error — release everything and drain the connection's queue.
        self.release_coalesced(slab_idx);
        self.driver.drain_conn_send_queue(conn_index);
        self.driver.note_send_finalized(conn_index);
        let io_result = if result == 0 {
            Ok(0u32)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        };
        self.executor.wake_send(conn_index, io_result);
    }

    /// Handle a POLLOUT CQE armed after a coalesced send returned `-EAGAIN`.
    /// Resubmits the same sendmsg (data still intact in the slab entry).
    fn handle_send_msg_coalesced_pollout(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let slab_idx = ud.payload() as u16;

        if !self.driver.send_slab.in_use(slab_idx)
            || self.driver.connections.get(conn_index).is_none()
        {
            return;
        }

        if result < 0 {
            self.release_coalesced(slab_idx);
            self.driver.drain_conn_send_queue(conn_index);
            self.driver.note_send_finalized(conn_index);
            self.executor
                .wake_send(conn_index, Err(io::Error::from_raw_os_error(-result)));
            return;
        }

        let msg_ptr = self.driver.send_slab.msghdr_ptr(slab_idx);
        if self
            .driver
            .ring
            .submit_send_msg_coalesced(conn_index, msg_ptr, slab_idx)
            .is_err()
        {
            let generation = self.driver.connections.generation(conn_index);
            self.driver
                .pending_coalesced_retries
                .push((conn_index, generation, slab_idx, 0));
        }
    }

    /// Replenish the held provided-buffer bids backing a recv-forward entry and
    /// release the slab slot. The bids become available in the `ProvidedBufRing`
    /// again (resuming recv if it was ENOBUFS-stalled).
    fn release_recv_forward(&mut self, slab_idx: u16) {
        let mut bids = [u16::MAX; crate::buffer::send_slab::MAX_IOVECS];
        let mut n = 0;
        for &b in self.driver.send_slab.recv_forward_bids(slab_idx) {
            bids[n] = b;
            n += 1;
        }
        for &b in &bids[..n] {
            self.driver.pending_replenish.push(b);
        }
        self.driver.send_slab.release(slab_idx);
    }

    /// Handle completion of a zero-copy recv-forward `sendmsg`
    /// (OpTag::SendRecvBufsCoalesced). Mirrors `handle_send_msg_coalesced` but
    /// the backing is held provided buffers whose bids are replenished (not pool
    /// slots released) on completion. Partial sends advance the iovec array via
    /// `try_advance`; the bids stay held (memory valid) until full completion.
    fn handle_send_recv_bufs_coalesced(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let slab_idx = ud.payload() as u16;

        if !self.driver.send_slab.in_use(slab_idx) {
            return;
        }

        if result > 0 {
            // Partial send: advance the iovec array and resubmit the remainder.
            // Bids are NOT replenished yet — the provided-buffer memory must stay
            // valid for the resubmitted iovecs.
            if let Some(msg_ptr) = self.driver.send_slab.try_advance(slab_idx, result as u32) {
                if self
                    .driver
                    .ring
                    .submit_send_recv_bufs_coalesced(conn_index, msg_ptr, slab_idx)
                    .is_err()
                {
                    let generation = self.driver.connections.generation(conn_index);
                    self.driver
                        .pending_recv_forward_retries
                        .push((conn_index, generation, slab_idx, 0));
                }
                return;
            }
            // Fully sent.
            let total = self.driver.send_slab.total_len(slab_idx);
            metrics::BYTES.add(metrics::bytes::SENT, total as u64);
            self.release_recv_forward(slab_idx);
            self.driver.submit_next_queued(conn_index);
            self.driver.note_send_finalized(conn_index);
            self.executor.wake_send(conn_index, Ok(total));
            return;
        }

        // EAGAIN/EWOULDBLOCK: wait for POLLOUT, keep the slab entry (and the held
        // buffers) alive, then resubmit the same sendmsg.
        let errno = -result;
        if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
            if self
                .driver
                .ring
                .submit_send_recv_bufs_coalesced_pollout(conn_index, slab_idx)
                .is_err()
            {
                let generation = self.driver.connections.generation(conn_index);
                self.driver
                    .pending_recv_forward_retries
                    .push((conn_index, generation, slab_idx, 0));
            }
            metrics::POOL.increment(metrics::pool::SEND_EAGAIN);
            return;
        }

        // Real error — replenish bids, release, and unwind the connection.
        self.release_recv_forward(slab_idx);
        self.driver.submit_next_queued(conn_index);
        self.driver.note_send_finalized(conn_index);
        let io_result = if result == 0 {
            Ok(0u32)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        };
        self.executor.wake_send(conn_index, io_result);
    }

    /// Handle a POLLOUT CQE armed after a recv-forward send returned `-EAGAIN`.
    /// Resubmits the same sendmsg (held buffers still intact in the slab entry).
    fn handle_send_recv_bufs_coalesced_pollout(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let slab_idx = ud.payload() as u16;

        if !self.driver.send_slab.in_use(slab_idx)
            || self.driver.connections.get(conn_index).is_none()
        {
            return;
        }

        if result < 0 {
            self.release_recv_forward(slab_idx);
            self.driver.submit_next_queued(conn_index);
            self.driver.note_send_finalized(conn_index);
            self.executor
                .wake_send(conn_index, Err(io::Error::from_raw_os_error(-result)));
            return;
        }

        let msg_ptr = self.driver.send_slab.msghdr_ptr(slab_idx);
        if self
            .driver
            .ring
            .submit_send_recv_bufs_coalesced(conn_index, msg_ptr, slab_idx)
            .is_err()
        {
            let generation = self.driver.connections.generation(conn_index);
            self.driver
                .pending_recv_forward_retries
                .push((conn_index, generation, slab_idx, 0));
        }
    }

    /// Release the backing of the in-flight forward write, record an error for
    /// the `ForwardToFuture`, and wake it. A pinned bid returns to the ring.
    fn fail_forward_write(&mut self, conn_index: u32, errno: i32) {
        if let Some(crate::backend::HeldRecvBuf::Pinned { bid, .. }) = self.driver.forward_write
            [conn_index as usize]
            .take()
            .map(|s| s.backing)
        {
            self.driver.pending_replenish.push(bid);
        }
        // On a closing connection this is the cancelled-write's (ECANCELED) CQE:
        // the backing is now released, so continue the deferred close instead of
        // waking the doomed forward future.
        if self.driver.send_queues[conn_index as usize].close_pending {
            self.driver.try_finalize_close(conn_index);
            return;
        }
        self.driver.forward_done[conn_index as usize] = Some(Err(errno));
        self.executor.wake_recv(conn_index);
    }

    /// Resubmit the remaining bytes of the in-flight forward write (after a short
    /// write, or after a POLLOUT re-arm). The backing stays held; only the source
    /// pointer, length, and (for files) offset advance. On submit failure the
    /// forward is failed (releasing the backing).
    fn resubmit_forward_write(&mut self, conn_index: u32) {
        let (sink_fd, is_file, ptr, len, offset, generation) = {
            let Some(state) = self.driver.forward_write[conn_index as usize].as_ref() else {
                return;
            };
            let (ptr, len) = state.remainder(&self.driver.provided_bufs);
            (
                state.sink_fd,
                state.is_file,
                ptr,
                len,
                state.base_offset + state.written as u64,
                state.generation,
            )
        };
        let ud = UserData::encode(OpTag::ForwardWrite, conn_index, generation);
        let res = if is_file {
            unsafe {
                self.driver
                    .ring
                    .submit_forward_write_file(sink_fd, ptr, len, offset, ud)
            }
        } else {
            unsafe {
                self.driver
                    .ring
                    .submit_forward_write_socket(sink_fd, ptr, len, ud)
            }
        };
        if res.is_err() {
            // SQ full on a mid-forward resubmit: surface an error so the caller
            // recovers (a partial forward already reached the sink, so the stream
            // is desynced and the connection should be torn down).
            self.fail_forward_write(conn_index, libc::EAGAIN);
        }
    }

    /// Re-arm a forwarding connection's multishot recv after the Mode A hold cap
    /// throttled (cancelled) it, once the held-buffer backlog has drained below
    /// `forward_hold_cap`.
    ///
    /// Called from the write-completion handler (the hold drains as writes finish)
    /// and from the ECANCELED branch (the throttle-cancel's own completion). Both
    /// converge on the same guard, so there is no deadlock: after a throttle the
    /// hold is drained one buffer per serialized write completion, and each
    /// completion re-checks this gate; the ECANCELED path covers the case where
    /// the hold already drained before the cancel completed. The `!armed` guard
    /// waits for the old multishot to fully terminate (its ECANCELED clears
    /// `recv_multishot_armed` at the top of `handle_recv_multi`) so two multishots
    /// with the same `RecvMulti` user_data never overlap.
    fn maybe_rearm_throttled_forward(&mut self, conn_index: u32) {
        let ci = conn_index as usize;
        if !self.driver.forward_hold_throttled[ci] {
            return;
        }
        // Wait for the cancelled multishot to terminate before arming a fresh one.
        let armed = self
            .driver
            .connections
            .get(conn_index)
            .is_some_and(|c| c.recv_multishot_armed);
        if armed {
            return;
        }
        // Only re-arm once the hold has drained below the cap.
        if self.driver.segment_hold[ci].len() >= self.driver.forward_hold_cap {
            return;
        }
        // Connection must still be open in multishot recv mode.
        let open = self
            .driver
            .connections
            .get(conn_index)
            .is_some_and(|c| matches!(c.recv_mode, RecvMode::Multi));
        if !open {
            self.driver.forward_hold_throttled[ci] = false;
            return;
        }
        // If the cancelled multishot happened to ENOBUFS-terminate (rather than
        // ECANCELED) it may have parked in `recv_starved`; take it back so the
        // starved-rearm path and this throttle re-arm cannot both fire.
        if let Some(pos) = self
            .driver
            .recv_starved
            .iter()
            .position(|&c| c == conn_index)
        {
            self.driver.recv_starved.swap_remove(pos);
        }
        self.driver.forward_hold_throttled[ci] = false;
        if self.driver.ring.submit_multishot_recv(conn_index).is_err() {
            metrics::RING.increment(metrics::ring::RECV_ARM_FAILURES);
            self.executor.wake_recv(conn_index);
            self.driver.close_connection(conn_index);
        } else if let Some(cs) = self.driver.connections.get_mut(conn_index) {
            cs.recv_multishot_armed = true;
        }
    }

    /// Handle completion of a segmented-recv Mode A forward write
    /// (`OpTag::ForwardWrite`). The payload carries the connection generation at
    /// submit, so a stale completion (slot closed/reused — `close_connection`
    /// already released the backing) is ignored. On full completion the held bid
    /// is replenished exactly once and the `ForwardToFuture` is woken; a short
    /// write resubmits the remainder at the advanced offset; `-EAGAIN` (socket
    /// sink) arms POLLOUT.
    fn handle_forward_write(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let submit_gen = ud.payload();
        let live = self.driver.forward_write[conn_index as usize]
            .as_ref()
            .is_some_and(|s| s.generation == submit_gen);
        if !live {
            return;
        }

        // If the connection is closing, `close_connection` cancelled this write;
        // stop forwarding regardless of the result. Reclaim the backing (the CQE
        // means the kernel is done reading it) and drive the deferred close — do
        // not resubmit a short write, arm POLLOUT, or wake the doomed future.
        let closing = self.driver.send_queues[conn_index as usize].close_pending;

        if result > 0 {
            let n = result as u32;
            let reached_total = {
                let state = self.driver.forward_write[conn_index as usize]
                    .as_mut()
                    .expect("checked live above");
                state.written = state.written.saturating_add(n);
                state.written >= state.total
            };
            if !reached_total && !closing {
                // Short write — resubmit the remainder (files, or a socket send
                // the kernel did not fully retry).
                self.resubmit_forward_write(conn_index);
                return;
            }
            // Fully written (or closing — stop forwarding): release the backing
            // exactly once.
            let state = self.driver.forward_write[conn_index as usize]
                .take()
                .expect("checked live above");
            let total = state.total;
            if let crate::backend::HeldRecvBuf::Pinned { bid, .. } = state.backing {
                self.driver.pending_replenish.push(bid);
            }
            if closing {
                // The forward write was the last thing pinning this slot; its bid
                // is now released, so continue the deferred close.
                self.driver.try_finalize_close(conn_index);
                return;
            }
            metrics::BYTES.add(metrics::bytes::SENT, total as u64);
            self.driver.forward_done[conn_index as usize] = Some(Ok(total));
            self.executor.wake_recv(conn_index);
            // A write completed, so the forward future will pop the next held
            // buffer — draining the hold. If the recv was throttled by the hold
            // cap and the hold is now below it (and the throttle-cancel's ECANCELED
            // has been observed), re-arm the multishot so the source resumes.
            self.maybe_rearm_throttled_forward(conn_index);
            return;
        }

        let errno = -result;
        if !closing && (errno == libc::EAGAIN || errno == libc::EWOULDBLOCK) {
            // Socket sink buffer full: arm POLLOUT, then resubmit when writable.
            let sink_fd = self.driver.forward_write[conn_index as usize]
                .as_ref()
                .expect("checked live above")
                .sink_fd;
            let pud = UserData::encode(OpTag::ForwardWritePollOut, conn_index, submit_gen);
            if self
                .driver
                .ring
                .submit_forward_write_pollout(sink_fd, pud)
                .is_err()
            {
                self.fail_forward_write(conn_index, libc::EAGAIN);
            }
            metrics::POOL.increment(metrics::pool::SEND_EAGAIN);
            return;
        }

        // Real error (or a 0-byte write, which would otherwise loop forever).
        let e = if result == 0 { libc::EIO } else { errno };
        self.fail_forward_write(conn_index, e);
    }

    /// Handle a POLLOUT CQE armed after a forward write to a socket sink returned
    /// `-EAGAIN` (`OpTag::ForwardWritePollOut`). Resubmits the remaining bytes.
    fn handle_forward_write_pollout(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let submit_gen = ud.payload();
        let live = self.driver.forward_write[conn_index as usize]
            .as_ref()
            .is_some_and(|s| s.generation == submit_gen);
        if !live {
            return;
        }
        if result < 0 {
            self.fail_forward_write(conn_index, -result);
            return;
        }
        // A closing connection cancelled its forward write; even if this POLLOUT
        // raced in writable, stop forwarding — reclaim the backing and finalize
        // the close rather than resubmitting onto a doomed connection.
        if self.driver.send_queues[conn_index as usize].close_pending {
            self.fail_forward_write(conn_index, libc::ECANCELED);
            return;
        }
        self.resubmit_forward_write(conn_index);
    }

    /// Handle completion of a send from a recv buffer (zero-copy forward).
    ///
    /// Payload encoding: `bid` in low 16 bits, `remaining_len` in high 16 bits.
    /// On partial send, resubmits from offset. On completion, replenishes the bid.
    fn handle_send_recv_buf(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let payload = ud.payload();
        // Payload carries only the bid. The remaining byte count is in the driver
        // field (send_recv_buf_remaining) so that buffer sizes > u16::MAX work.
        let bid = payload as u16;
        let remaining_before = self.driver.send_recv_buf_remaining[conn_index as usize];

        if result > 0 {
            let bytes_sent = result as u32;

            if bytes_sent < remaining_before {
                // Partial send — resubmit the remainder.
                let new_remaining = remaining_before - bytes_sent;
                self.driver.send_recv_buf_remaining[conn_index as usize] = new_remaining;
                let (buf_ptr, _buf_size) = self.driver.provided_bufs.get_buffer(bid);
                let original_len = self.driver.send_recv_buf_original_lens[conn_index as usize];
                let offset = original_len - new_remaining;
                let new_ptr = unsafe { buf_ptr.add(offset as usize) };
                let new_payload = bid as u32;
                let new_ud = UserData::encode(
                    crate::completion::OpTag::SendRecvBuf,
                    conn_index,
                    new_payload,
                );
                let entry = io_uring::opcode::Send::new(
                    io_uring::types::Fixed(conn_index),
                    new_ptr,
                    new_remaining,
                )
                .flags(crate::completion::STREAM_SEND_FLAGS)
                .build()
                .user_data(new_ud.raw());

                if unsafe { self.driver.ring.push_sqe(entry) }.is_err() {
                    // SQ full — replenish and give up.
                    self.driver.pending_replenish.push(bid);
                    self.driver.submit_next_queued(conn_index);
                }
                return;
            }

            // Full send complete.
            metrics::BYTES.add(metrics::bytes::SENT, remaining_before as u64);
            self.driver.pending_replenish.push(bid);
            self.driver.submit_next_queued(conn_index);
            self.executor.wake_send(conn_index, Ok(remaining_before));
            return;
        }

        // Error or zero-length send.
        self.driver.pending_replenish.push(bid);
        self.driver.submit_next_queued(conn_index);

        let io_result = if result == 0 {
            Ok(0u32)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        };
        self.executor.wake_send(conn_index, io_result);
    }

    fn handle_send_msg_zc(&mut self, ud: UserData, result: i32, flags: u32) {
        let conn_index = ud.conn_index();
        let slab_idx = ud.payload() as u16;

        if !self.driver.send_slab.in_use(slab_idx) {
            return;
        }

        // Chain path.
        if self.driver.chain_table.is_active(conn_index) {
            if cqueue::notif(flags) {
                self.driver.send_slab.dec_pending_notifs(slab_idx);
                if self.driver.send_slab.should_release(slab_idx) {
                    let ps = self.driver.send_slab.release(slab_idx);
                    if ps != u16::MAX {
                        self.driver.send_copy_pool.release(ps);
                    }
                }
                let event = self.driver.chain_table.on_notif_cqe(conn_index);
                if matches!(event, ChainEvent::Complete { .. }) {
                    self.fire_chain_complete(conn_index);
                }
                return;
            }
            if result == -libc::ECANCELED {
                let ps = self.driver.send_slab.release(slab_idx);
                if ps != u16::MAX {
                    self.driver.send_copy_pool.release(ps);
                }
            } else if result > 0 {
                // Kernel sends a ZC notification only when result > 0.
                // result == 0 means no bytes sent — no notification will arrive.
                self.driver.send_slab.inc_pending_notifs(slab_idx);
                self.driver.send_slab.mark_awaiting_notifications(slab_idx);
                self.driver.chain_table.inc_zc_notif(conn_index);
            } else {
                // result == 0 or result < 0 (excluding ECANCELED above):
                // release immediately — no ZC notification coming.
                let ps = self.driver.send_slab.release(slab_idx);
                if ps != u16::MAX {
                    self.driver.send_copy_pool.release(ps);
                }
            }
            let event = self.driver.chain_table.on_operation_cqe(conn_index, result);
            if matches!(event, ChainEvent::Complete { .. }) {
                self.fire_chain_complete(conn_index);
            }
            return;
        }

        if cqueue::notif(flags) {
            self.driver.send_slab.dec_pending_notifs(slab_idx);
            if self.driver.send_slab.should_release(slab_idx) {
                let pool_slot = self.driver.send_slab.release(slab_idx);
                if pool_slot != u16::MAX {
                    self.driver.send_copy_pool.release(pool_slot);
                }
            }
            return;
        }

        // Only increment pending notifications for successful sends — the kernel
        // sends a ZC notification CQE only when result > 0. On error (result <= 0),
        // no notification arrives, so incrementing would permanently leak the slab slot.
        if result > 0 {
            self.driver.send_slab.inc_pending_notifs(slab_idx);
        }

        #[allow(clippy::collapsible_if)]
        if result > 0 {
            if let Some(msg_ptr) = self.driver.send_slab.try_advance(slab_idx, result as u32) {
                // Partial send — resubmit the remainder.
                if self
                    .driver
                    .ring
                    .submit_send_msg_zc(conn_index, msg_ptr, slab_idx)
                    .is_ok()
                {
                    return;
                }
                // Resubmission failed (SQ full) — queue for retry on the
                // next event loop tick. The slab entry retains all iovec
                // state from try_advance, so we can resubmit later.
                let generation = self.driver.connections.generation(conn_index);
                self.driver
                    .pending_zc_retries
                    .push((conn_index, generation, slab_idx, 0));
                return;
            }
        }

        // Send complete (all bytes sent) or error (result <= 0).
        self.driver.send_slab.mark_awaiting_notifications(slab_idx);

        let total_len = self.driver.send_slab.total_len(slab_idx);
        let should_release = self.driver.send_slab.should_release(slab_idx);

        if should_release {
            let pool_slot = self.driver.send_slab.release(slab_idx);
            if pool_slot != u16::MAX {
                self.driver.send_copy_pool.release(pool_slot);
            }
        }

        if result >= 0 {
            metrics::BYTES.add(metrics::bytes::SENT, total_len as u64);
            self.driver.submit_next_queued(conn_index);
        } else {
            self.driver.drain_conn_send_queue(conn_index);
        }

        let io_result = if result >= 0 {
            Ok(total_len)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        };
        self.executor.wake_send(conn_index, io_result);
    }

    fn handle_connect(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();

        if self.driver.connections.get(conn_index).is_none() {
            return;
        }

        if result < 0 {
            let errno = -result;

            if errno == libc::ECANCELED {
                let timeout_armed = self
                    .driver
                    .connections
                    .get(conn_index)
                    .map(|c| c.connect_timeout_armed)
                    .unwrap_or(false);
                if !timeout_armed {
                    let err = io::Error::from_raw_os_error(errno);
                    self.executor.wake_connect(conn_index, Err(err));
                    // Don't call remove_connection here — it would clear io_results
                    // before the owning task can read the error via ConnectFuture.
                    // handle_close (triggered by close_connection) will clean up.
                    self.driver.close_connection(conn_index);
                    return;
                }
                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.connect_timeout_armed = false;
                }
                return;
            }

            if self
                .driver
                .connections
                .get(conn_index)
                .map(|c| c.connect_timeout_armed)
                .unwrap_or(false)
            {
                let timeout_ud = UserData::encode(OpTag::Timeout, conn_index, 0);
                let _ = self
                    .driver
                    .ring
                    .submit_async_cancel(timeout_ud.raw(), conn_index);
                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.connect_timeout_armed = false;
                }
            }

            if let Some(ref mut tls_table) = self.driver.tls_table {
                tls_table.remove(conn_index);
            }

            let err = io::Error::from_raw_os_error(errno);
            self.executor.wake_connect(conn_index, Err(err));
            // Don't call remove_connection here — it would clear io_results
            // before the owning task can read the error via ConnectFuture.
            // handle_close (triggered by close_connection) will clean up.
            self.driver.close_connection(conn_index);
            return;
        }

        // Connect succeeded.
        let timeout_was_armed = self
            .driver
            .connections
            .get(conn_index)
            .map(|c| c.connect_timeout_armed)
            .unwrap_or(false);
        if timeout_was_armed {
            let still_connecting = self
                .driver
                .connections
                .get(conn_index)
                .map(|c| matches!(c.recv_mode, RecvMode::Connecting))
                .unwrap_or(false);
            if !still_connecting {
                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.connect_timeout_armed = false;
                }
                return;
            }
            let timeout_ud = UserData::encode(OpTag::Timeout, conn_index, 0);
            let _ = self
                .driver
                .ring
                .submit_async_cancel(timeout_ud.raw(), conn_index);
            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                cs.connect_timeout_armed = false;
            }
        }

        // Orphaned-future guard: if the ConnectFuture was dropped while the
        // SQE was in flight (e.g. `select!` with a timeout that won), no
        // task will pick up this connection — and we don't have an
        // `on_accept` path for outbound connects. Letting the slot go
        // "established" would leak it until the peer closes (which may be
        // never). Close it now instead.
        if !self.executor.connect_waiters[conn_index as usize] {
            if let Some(pending) = self.driver.pending_recv_bufs[conn_index as usize].take() {
                self.driver.pending_replenish.push(pending.bid);
            }
            self.driver.accumulators.reset(conn_index);
            if let Some(ref mut tls_table) = self.driver.tls_table {
                tls_table.remove(conn_index);
            }
            self.driver.close_connection(conn_index);
            return;
        }

        if let Some(pending) = self.driver.pending_recv_bufs[conn_index as usize].take() {
            self.driver.pending_replenish.push(pending.bid);
        }
        self.driver.accumulators.reset(conn_index);
        self.driver.reset_segment_state(conn_index);

        // TLS client path
        if let Some(ref mut tls_table) = self.driver.tls_table
            && tls_table.get_mut(conn_index).is_some()
        {
            let flushed = crate::tls::flush_tls_output(
                tls_table,
                &mut self.driver.send_copy_pool,
                conn_index,
                &mut self.driver.tls_out_scratch,
            );
            if !self.driver.tls_out_scratch.is_empty() {
                let mut sends = std::mem::take(&mut self.driver.tls_out_scratch);
                let _ = self.driver.queue_built_sends(conn_index, &mut sends);
                self.driver.tls_out_scratch = sends;
            }
            if !flushed {
                let err = std::io::Error::other("send pool exhausted during TLS flush");
                self.executor.wake_connect(conn_index, Err(err));
                self.driver.close_connection(conn_index);
                return;
            }
            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                cs.recv_mode = RecvMode::Multi;
            }
            self.arm_recv(conn_index);
            return;
        }

        // Plaintext path
        if let Some(cs) = self.driver.connections.get_mut(conn_index) {
            cs.established = true;
            cs.recv_mode = RecvMode::Multi;
        }
        self.arm_recv(conn_index);

        self.executor.wake_connect(conn_index, Ok(()));
    }

    fn handle_timeout(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();

        if result != -libc::ETIME {
            return;
        }

        let conn = match self.driver.connections.get(conn_index) {
            Some(c) => c,
            None => return,
        };

        // Generation check (payload carries it): a stale -ETIME arriving
        // after close + slot reuse must not kill the new occupant's connect.
        if conn.generation != ud.payload() || !conn.connect_timeout_armed {
            return;
        }

        if !matches!(conn.recv_mode, RecvMode::Connecting) {
            return;
        }

        let connect_ud = UserData::encode(OpTag::Connect, conn_index, 0);
        let _ = self
            .driver
            .ring
            .submit_async_cancel(connect_ud.raw(), conn_index);

        if let Some(ref mut tls_table) = self.driver.tls_table {
            tls_table.remove(conn_index);
        }

        let err = io::Error::new(io::ErrorKind::TimedOut, "connect timed out");
        self.executor.wake_connect(conn_index, Err(err));
        // Don't call remove_connection here — handle_close will clean up.
        self.driver.close_connection(conn_index);
    }

    fn handle_close(&mut self, ud: UserData) {
        let conn_index = ud.conn_index();

        // Replenish any held zero-copy recv buffer.
        if let Some(pending) = self.driver.pending_recv_bufs[conn_index as usize].take() {
            self.driver.pending_replenish.push(pending.bid);
        }

        // Reclaim a bid still pinned by a live `RecvSegment` (Mode B). The future
        // is dropped just below by `remove_connection`, which drops the segment —
        // but that `Drop` runs unguarded here (`CURRENT_DRIVER == None`) and
        // no-ops, so the release is done explicitly. `close_connection`
        // deliberately did NOT reclaim this bid (a parked task could still have
        // deref'd it before this point); by now the connection is fully closing
        // and no live segment can read the buffer, so it is safe to return. If an
        // in-poll `RecvSegment::drop`/`into_owned` already released it, the slot is
        // `None` and this is a no-op (single-release via the pin slot).
        if let Some(crate::backend::HeldRecvBuf::Pinned { bid, .. }) =
            self.driver.segment_pinned[conn_index as usize].take()
        {
            self.driver.pending_replenish.push(bid);
        }

        // Drain any segmented-recv buffers still held (a Mode B reader that never
        // finished consuming them, or a Mode A forward aborted by close).
        // `close_connection` deliberately left these so a post-FIN reader could
        // consume them; by teardown no consumer remains, so reclaim each Pinned
        // bid (Owned entries just drop). Symmetric to the `segment_pinned` reclaim
        // above and the `pending_recv_bufs` reclaim.
        for held in self.driver.segment_hold[conn_index as usize]
            .drain(..)
            .collect::<Vec<_>>()
        {
            if let crate::backend::HeldRecvBuf::Pinned { bid, .. } = held {
                self.driver.pending_replenish.push(bid);
            }
        }

        let was_established = self
            .driver
            .connections
            .get(conn_index)
            .map(|c| c.established)
            .unwrap_or(false);

        if let Some(ref mut tls_table) = self.driver.tls_table {
            tls_table.remove(conn_index);
        }

        if was_established {
            metrics::CONNECTIONS.increment(metrics::conn::CLOSED);
            metrics::CONNECTIONS_ACTIVE.decrement();
        }

        // Remove the async task (drops the future).
        self.executor.remove_connection(conn_index);
        self.driver.connections.release(conn_index);
    }

    fn handle_tls_send(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let pool_slot = ud.payload() as u16;

        // Guard against stale CQE for an already-released pool slot, matching
        // handle_send. Without this, try_advance on a released slot would
        // wrap in release mode and resubmit a wild length.
        if !self.driver.send_copy_pool.in_use(pool_slot) {
            return;
        }

        if result > 0
            && let Some((ptr, remaining)) = self
                .driver
                .send_copy_pool
                .try_advance(pool_slot, result as u32)
        {
            if self
                .driver
                .ring
                .submit_tls_send(conn_index, ptr, remaining, pool_slot)
                .is_err()
            {
                // SQ full — queue for retry on next tick. Use copy retry
                // since TLS sends use SendCopyPool slots; the stored OpTag
                // keeps the resubmission on the TlsSend completion path.
                let generation = self.driver.connections.generation(conn_index);
                self.driver.pending_copy_retries.push((
                    conn_index,
                    generation,
                    pool_slot,
                    0,
                    OpTag::TlsSend,
                ));
            }
            return;
        }
        // Socket send buffer full: wait for POLLOUT and resubmit the same
        // chunk. Tearing the connection down here (the old behavior) turned
        // ordinary backpressure during a large TLS write into a broken
        // connection. Keep the slot; is_tls=true keeps the resubmission on
        // the TlsSend path.
        if result < 0 {
            let errno = -result;
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                if self
                    .driver
                    .ring
                    .submit_send_pollout(conn_index, pool_slot, true)
                    .is_err()
                {
                    let generation = self.driver.connections.generation(conn_index);
                    self.driver
                        .pending_send_pollout_retries
                        .push((conn_index, generation, pool_slot, 0, true));
                }
                metrics::POOL.increment(metrics::pool::SEND_EAGAIN);
                return;
            }
        }

        self.driver.send_copy_pool.release(pool_slot);

        // Intermediate TLS chunks are serialized through the per-connection
        // send queue, so a completion must pop the next queued send and let
        // a deferred close finalize, exactly like handle_send.
        if result >= 0 {
            self.driver.submit_next_queued(conn_index);
            self.driver.note_send_finalized(conn_index);
            return;
        }

        // On error, fail the connection so the owning task unblocks: drain
        // the queued sibling chunks (their slots are released by the drain)
        // and close.
        if result < 0 {
            self.driver.drain_conn_send_queue(conn_index);
            self.driver.close_connection(conn_index);
        }
    }

    fn fire_chain_complete(&mut self, conn_index: u32) {
        let chain = match self.driver.chain_table.take(conn_index) {
            Some(c) => c,
            None => return,
        };

        let io_result = match chain.first_error {
            Some(errno) => Err(io::Error::from_raw_os_error(-errno)),
            None => Ok(chain.bytes_sent),
        };

        if chain.first_error.is_none() {
            self.driver.submit_next_queued(conn_index);
        } else {
            self.driver.drain_conn_send_queue(conn_index);
        }

        self.executor.wake_send(conn_index, io_result);
    }

    fn handle_timer(&mut self, ud: UserData, result: i32) {
        // Timer CQE: -ETIME means the timeout expired normally.
        // -ECANCELED means it was cancelled (e.g., SleepFuture dropped).
        if result != -libc::ETIME {
            // Cancelled or error — the SleepFuture::drop already released the slot.
            return;
        }

        let payload = ud.payload();
        let (slot, generation) = TimerSlotPool::decode_payload(payload);

        if let Some(waker_id) = self.executor.timer_pool.fire(slot, generation) {
            self.executor.wake_task(waker_id);
        }
    }

    fn handle_recv_msg_udp(&mut self, ud: UserData, result: i32, flags: u32) {
        let batch_recv_at = self.driver.udp_batch_recv_at;
        /// Parse the `name` region from a multishot `recvmsg` output into a
        /// `SocketAddr`. The region is a `sockaddr_in` or `sockaddr_in6`
        /// depending on `ss_family`; we copy it into an aligned
        /// `sockaddr_storage` before decoding.
        fn parse_recvmsg_name(name: &[u8]) -> Option<std::net::SocketAddr> {
            if name.len() < std::mem::size_of::<libc::sa_family_t>() {
                return None;
            }
            let max = std::mem::size_of::<libc::sockaddr_storage>();
            let copy_len = name.len().min(max);
            let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    name.as_ptr(),
                    &mut storage as *mut _ as *mut u8,
                    copy_len,
                );
            }
            sockaddr_to_socket_addr(&storage, copy_len as u32)
        }

        let udp_index = ud.conn_index();
        let idx = udp_index as usize;
        let has_more = cqueue::more(flags);

        if idx >= self.driver.udp_sockets.len() {
            // Stale socket. If a buffer was attached, hand it back so it
            // doesn't leak out of the ring.
            if result > 0
                && let Some(bid) = cqueue::buffer_select(flags)
            {
                // Count the handout so the replenish's occupancy decrement is
                // balanced. `replenish_batch` only decrements when the ring is
                // Some, so guard the increment the same way.
                if let Some(r) = self.driver.udp_provided_bufs.as_mut() {
                    r.on_handout();
                }
                self.driver.udp_pending_replenish.push(bid);
            }
            return;
        }

        // udp_sockets is non-empty → udp_provided_bufs is Some.
        let udp_bgid = match self.driver.udp_provided_bufs.as_ref() {
            Some(r) => r.bgid(),
            None => return,
        };

        if result <= 0 {
            let errno = -result;
            // If the multishot was torn down, rearm so the socket stays
            // live. ECANCELED is a quiet teardown during shutdown; skip.
            if !has_more && errno != libc::ECANCELED {
                if errno == libc::ENOBUFS {
                    metrics::POOL.increment(metrics::pool::BUFFER_RING_EMPTY);
                }
                self.driver.rearm_udp_recvmsg(udp_index, udp_bgid);
            }
            return;
        }

        let bid = match cqueue::buffer_select(flags) {
            Some(b) => b,
            None => {
                if !has_more {
                    self.driver.rearm_udp_recvmsg(udp_index, udp_bgid);
                }
                return;
            }
        };
        // The bid is checked out of the UDP ring; account it against occupancy so
        // `free()` and the double-replenish tripwire stay accurate. It is
        // replenished exactly once — immediately below on a parse/drop, or when
        // the consumer reads the queued datagram.
        self.driver.udp_provided_bufs.as_mut().unwrap().on_handout();

        // SAFETY: The buffer pointer belongs to the UDP provided buffer ring
        // and remains valid until we replenish the bid below.
        let buf_len = result as u32;
        let buf = {
            let udp_bufs = self.driver.udp_provided_bufs.as_ref().unwrap();
            let (buf_ptr, _) = udp_bufs.get_buffer(bid);
            unsafe { std::slice::from_raw_parts(buf_ptr, buf_len as usize) }
        };

        // Parse the recvmsg header out of the kernel buffer. If parsing fails
        // (or the datagram is truncated, or the queue is full) the bid is
        // returned to the ring immediately. Otherwise the bid travels with the
        // queue entry and is replenished when the consumer reads it — that's
        // what makes the recv path zero-copy.
        let parse_result =
            io_uring::types::RecvMsgOut::parse(buf, &self.driver.udp_sockets[idx].recv_msghdr);

        let mut handed_to_queue = false;
        if let Ok(msg_out) = parse_result
            && !msg_out.is_name_data_truncated()
            && !msg_out.is_payload_truncated()
            && let Some(peer) = parse_recvmsg_name(msg_out.name_data())
        {
            metrics::UDP.increment(metrics::udp::DATAGRAMS_RECEIVED);
            if idx < self.executor.udp_recv_queues.len() {
                if self.executor.udp_recv_queues[idx].len() >= self.executor.udp_recv_queue_capacity
                {
                    // The handler isn't draining fast enough (or has
                    // exited). Drop on the floor so we don't grow without
                    // bound. UDP is lossy by definition; the metric flags
                    // it for operators.
                    metrics::UDP.increment(metrics::udp::DATAGRAMS_DROPPED);
                } else {
                    let payload = msg_out.payload_data();
                    // When GRO is on, the kernel may have coalesced several
                    // datagrams into this payload; the UDP_GRO cmsg carries
                    // the per-segment size used to split them back apart at
                    // drain time. `is_control_data_truncated()` (MSG_CTRUNC)
                    // means we lost the cmsg but the payload is intact —
                    // treat it as a single datagram (segment_size 0) rather
                    // than dropping it.
                    let segment_size = if self.driver.udp_sockets[idx].gro
                        && !msg_out.is_control_data_truncated()
                    {
                        crate::backend::udp_gro::parse_segment_size(msg_out.control_data())
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    // SAFETY: `payload` is a slice borrowed from the kernel
                    // buffer at `(buf_ptr, buf_len)`. The buffer remains valid
                    // until `bid` is pushed to `udp_pending_replenish` (which
                    // happens when the consumer reads the queue entry).
                    let payload_ptr = payload.as_ptr();
                    let payload_len = payload.len() as u32;
                    self.executor.udp_recv_queues[idx].push_back(
                        crate::runtime::PendingUdpDatagram {
                            peer,
                            buf: crate::runtime::PendingUdpBuf::Kernel {
                                bid,
                                ptr: payload_ptr,
                                payload_len,
                            },
                            recv_at: batch_recv_at,
                            segment_size,
                            consumed: 0,
                        },
                    );
                    handed_to_queue = true;
                    self.executor.wake_udp_recv(udp_index);
                }
            }
        }

        if !handed_to_queue {
            self.driver.udp_pending_replenish.push(bid);
        }

        if !has_more {
            self.driver.rearm_udp_recvmsg(udp_index, udp_bgid);
        }
    }

    fn handle_send_msg_udp(&mut self, ud: UserData, result: i32) {
        let udp_index = ud.conn_index();
        let (slot_idx, pool_slot) =
            crate::backend::uring::driver::decode_udp_send_payload(ud.payload());
        let idx = udp_index as usize;

        self.driver.send_copy_pool.release(pool_slot);

        let mut slot_returned = false;
        if idx < self.driver.udp_sockets.len() {
            let sock = &mut self.driver.udp_sockets[idx];
            if (slot_idx as usize) < sock.send_slots.len() {
                sock.send_freelist.push(slot_idx);
                slot_returned = true;
            }
        }

        // A freed slot may unblock a task awaiting `UdpCtx::send_ready`.
        if slot_returned {
            self.executor.wake_udp_send_ready(udp_index);
        }

        if result < 0 {
            metrics::UDP.increment(metrics::udp::SEND_ERRORS);
        }
    }

    /// Handle a CQE for a `Send` issued on a connected UDP socket (the
    /// `RecvMsgUdp` fast-path counterpart). Same bookkeeping as
    /// `handle_send_msg_udp`: release the pool slot and per-socket send
    /// slot, wake any task awaiting `send_ready`.
    fn handle_send_udp(&mut self, ud: UserData, result: i32) {
        // Send completion bookkeeping is identical to the sendmsg path —
        // only the SQE opcode differed. Delegate.
        self.handle_send_msg_udp(ud, result);
    }

    /// Handle a multishot `Recv` CQE for a connected UDP socket. The buffer
    /// contains only the payload (no `io_uring_recvmsg_out` header / sockaddr,
    /// since we used `IORING_OP_RECV` rather than `RECVMSG`). The peer is
    /// known from `UdpSocketState::connected_peer`.
    fn handle_recv_udp(&mut self, ud: UserData, result: i32, flags: u32) {
        let batch_recv_at = self.driver.udp_batch_recv_at;
        let udp_index = ud.conn_index();
        let idx = udp_index as usize;
        let has_more = cqueue::more(flags);

        if idx >= self.driver.udp_sockets.len() {
            if result > 0
                && let Some(bid) = cqueue::buffer_select(flags)
            {
                // Count the handout so the replenish's occupancy decrement is
                // balanced. `replenish_batch` only decrements when the ring is
                // Some, so guard the increment the same way.
                if let Some(r) = self.driver.udp_provided_bufs.as_mut() {
                    r.on_handout();
                }
                self.driver.udp_pending_replenish.push(bid);
            }
            return;
        }

        let udp_bgid = match self.driver.udp_provided_bufs.as_ref() {
            Some(r) => r.bgid(),
            None => return,
        };

        if result <= 0 {
            // Defensive: a zero-length datagram (or an error CQE on some
            // kernels) can still carry a selected buffer — failing to
            // replenish its bid shrinks the UDP ring by one per occurrence
            // until ENOBUFS (a remote peer sending empty datagrams could
            // drain the ring entirely).
            if let Some(bid) = cqueue::buffer_select(flags) {
                // The ring is Some here (guarded by the `udp_bgid` match above).
                // Count the handout so the replenish's occupancy decrement is
                // balanced.
                self.driver.udp_provided_bufs.as_mut().unwrap().on_handout();
                self.driver.udp_pending_replenish.push(bid);
            }
            let errno = -result;
            if !has_more && errno != libc::ECANCELED {
                if errno == libc::ENOBUFS {
                    metrics::POOL.increment(metrics::pool::BUFFER_RING_EMPTY);
                }
                self.driver.rearm_udp_recvmsg(udp_index, udp_bgid);
            }
            return;
        }

        let bid = match cqueue::buffer_select(flags) {
            Some(b) => b,
            None => {
                if !has_more {
                    self.driver.rearm_udp_recvmsg(udp_index, udp_bgid);
                }
                return;
            }
        };
        // The bid is checked out of the UDP ring; account it against occupancy so
        // `free()` and the double-replenish tripwire stay accurate. It is
        // replenished exactly once — immediately below on a parse/drop, or when
        // the consumer reads the queued datagram.
        self.driver.udp_provided_bufs.as_mut().unwrap().on_handout();

        let payload_len = result as u32;
        let buf_ptr = {
            let udp_bufs = self.driver.udp_provided_bufs.as_ref().unwrap();
            let (p, _) = udp_bufs.get_buffer(bid);
            p
        };

        metrics::UDP.increment(metrics::udp::DATAGRAMS_RECEIVED);

        let mut handed_to_queue = false;
        if let Some(peer) = self.driver.udp_sockets[idx].connected_peer
            && idx < self.executor.udp_recv_queues.len()
        {
            if self.executor.udp_recv_queues[idx].len() >= self.executor.udp_recv_queue_capacity {
                metrics::UDP.increment(metrics::udp::DATAGRAMS_DROPPED);
            } else {
                self.executor.udp_recv_queues[idx].push_back(crate::runtime::PendingUdpDatagram {
                    peer,
                    buf: crate::runtime::PendingUdpBuf::Kernel {
                        bid,
                        ptr: buf_ptr,
                        payload_len,
                    },
                    recv_at: batch_recv_at,
                    // Connected sockets use the plain `recv` path with no
                    // msghdr/control region, so GRO never applies here.
                    segment_size: 0,
                    consumed: 0,
                });
                handed_to_queue = true;
                self.executor.wake_udp_recv(udp_index);
            }
        }

        if !handed_to_queue {
            self.driver.udp_pending_replenish.push(bid);
        }

        if !has_more {
            self.driver.rearm_udp_recvmsg(udp_index, udp_bgid);
        }
    }

    fn handle_nvme_cmd(&mut self, ud: UserData, result: i32) {
        let slab_idx = ud.payload() as u16;

        let nvme_cmd_slab = match self.driver.nvme_cmd_slab {
            Some(ref mut s) => s,
            None => return,
        };

        if !nvme_cmd_slab.in_use(slab_idx) {
            return;
        }

        let device_index = nvme_cmd_slab.release(slab_idx);

        // Decrement in-flight count.
        if let Some(ref mut devices) = self.driver.nvme_devices
            && let Some(dev) = devices.get_mut(device_index)
        {
            dev.in_flight = dev.in_flight.saturating_sub(1);
        }

        // NVMe passthrough puts the device-level status word (positive,
        // e.g. 0x281 media error) in cqe->res on command failure; 0 is
        // success and negative is a transport errno. Treating result >= 0
        // as success returned Ok(status) for failed reads/writes — silent
        // data corruption on device errors.
        let result = if result > 0 { -libc::EIO } else { result };

        // Wake the async task waiting for this NVMe completion.
        self.executor.wake_disk_io(ud.payload(), result);
    }

    fn handle_direct_io(&mut self, ud: UserData, result: i32) {
        let slab_idx = ud.payload() as u16;

        let cmd_slab = match self.driver.direct_io_cmd_slab {
            Some(ref mut s) => s,
            None => return,
        };

        if !cmd_slab.in_use(slab_idx) {
            return;
        }

        let (file_index, _op) = cmd_slab.release(slab_idx);

        // Decrement in-flight count.
        if let Some(ref mut files) = self.driver.direct_io_files
            && let Some(f) = files.get_mut(file_index)
        {
            f.in_flight = f.in_flight.saturating_sub(1);
        }

        // Wake the async task waiting for this Direct I/O completion.
        self.executor.wake_disk_io(ud.payload(), result);
    }

    fn handle_fs(&mut self, ud: UserData, result: i32) {
        let slab_idx = ud.payload() as u16;
        let file_index = ud.conn_index() as u16;

        let cmd_slab = match self.driver.fs_cmd_slab {
            Some(ref mut s) => s,
            None => return,
        };

        if !cmd_slab.in_use(slab_idx) {
            return;
        }

        let op = cmd_slab.get(slab_idx).map(|e| e.op);

        // For Statx ops, convert the statx buffer to Metadata before releasing the slab.
        if op == Some(crate::fs::FsOp::Statx)
            && result >= 0
            && let Some(entry) = cmd_slab.get(slab_idx)
            && let Some(ref statx_buf) = entry.statx_buf
        {
            let metadata = crate::fs::Metadata::from_statx(statx_buf);
            // Keyed by the full disk-I/O key (StatFuture holds the same),
            // not the raw slab index.
            self.executor.fs_stat_results.insert(ud.payload(), metadata);
        }

        // For Open ops, handle success/failure of the file slot.
        if op == Some(crate::fs::FsOp::Open) && result < 0 {
            // Open failed — release the pre-allocated file slot.
            if let Some(ref mut files) = self.driver.fs_files {
                files.release(file_index);
            }
        }

        let (released_file_index, released_op) = cmd_slab.release(slab_idx);

        // Decrement in-flight count for file-bound ops.
        match released_op {
            crate::fs::FsOp::Read | crate::fs::FsOp::Write | crate::fs::FsOp::Fsync => {
                if let Some(ref mut files) = self.driver.fs_files
                    && let Some(f) = files.get_mut(released_file_index)
                {
                    f.in_flight = f.in_flight.saturating_sub(1);
                }
            }
            _ => {}
        }

        // Wake the async task waiting for this completion.
        self.executor.wake_disk_io(ud.payload(), result);
    }

    fn handle_pidfd_poll(&mut self, ud: UserData, result: i32) {
        let seq = ud.payload();
        self.executor.wake_pidfd(seq, result);
    }

    /// Arm the appropriate multishot recv for a connection.
    ///
    /// When the `timestamps` feature is enabled and configured, uses
    /// `RecvMsgMulti` (multishot recvmsg) to receive cmsg ancillary data
    /// containing kernel timestamps. Otherwise, uses `RecvMulti` (plain
    /// multishot recv).
    fn arm_recv(&mut self, conn_index: u32) {
        #[cfg(feature = "timestamps")]
        if self.driver.timestamps {
            let msghdr_ptr = &*self.driver.recvmsg_msghdr as *const libc::msghdr;
            if self
                .driver
                .ring
                .submit_multishot_recvmsg(conn_index, msghdr_ptr)
                .is_err()
            {
                metrics::RING.increment(metrics::ring::RECV_ARM_FAILURES);
                self.executor.wake_recv(conn_index);
                self.driver.close_connection(conn_index);
                return;
            }
            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                cs.recv_mode = RecvMode::MsgMulti;
            }
            return;
        }
        if self.driver.ring.submit_multishot_recv(conn_index).is_err() {
            metrics::RING.increment(metrics::ring::RECV_ARM_FAILURES);
            self.executor.wake_recv(conn_index);
            self.driver.close_connection(conn_index);
        } else if let Some(cs) = self.driver.connections.get_mut(conn_index) {
            cs.recv_multishot_armed = true;
        }
    }

    /// Spawn an async task for a newly accepted connection.
    ///
    /// The handler's `on_accept` future *constructor* (everything before
    /// the first `.await`, including any async-block initializer code) runs
    /// synchronously inside this method. `poll_ready_tasks` catches panics
    /// from the future's `poll`, but a panic during construction would
    /// otherwise tear down the worker thread along with every other
    /// connection on it. We wrap construction in `catch_unwind` and close
    /// the connection on panic instead.
    fn spawn_accept_task(&mut self, conn_index: u32) {
        let generation = self.driver.connections.generation(conn_index);
        let conn_ctx = ConnCtx::new(conn_index, generation);
        // SAFETY: `AssertUnwindSafe` is required because `self.handler` is
        // not `UnwindSafe`. A panic here is treated like a fatal handler
        // error — the connection is closed; the worker continues serving
        // others.
        let future_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Box::pin(self.handler.on_accept(conn_ctx))
        }));
        let future = match future_result {
            Ok(f) => f,
            Err(_) => {
                // Handler panicked during async-block construction.
                // Close the connection and skip task setup. Don't propagate
                // — the worker keeps serving its other connections.
                self.driver.close_connection(conn_index);
                return;
            }
        };
        self.executor.owner_task[conn_index as usize] = Some(conn_index);
        self.executor.task_slab.spawn(conn_index, future);
        self.executor.ready_queue.push_back(conn_index);
    }

    /// Test-only: expose dispatch_cqe for synthetic CQE testing.
    #[cfg(test)]
    pub(crate) fn test_dispatch_cqe(&mut self, user_data_raw: u64, result: i32, flags: u32) {
        self.dispatch_cqe(user_data_raw, result, flags);
    }

    /// Test-only: submit a NOP with injected result through the real io_uring
    /// pipeline, then drain and dispatch all completions.
    ///
    /// This exercises the full submit_and_wait → drain_completions → dispatch_cqe
    /// path, unlike test_dispatch_cqe which bypasses SQE submission.
    ///
    /// Requires kernel 6.6+ for IORING_NOP_INJECT_RESULT.
    #[cfg(test)]
    pub(crate) fn inject_and_dispatch(&mut self, user_data_raw: u64, result: i32) {
        self.driver
            .ring
            .submit_nop_inject(user_data_raw, result)
            .expect("submit_nop_inject failed — kernel 6.6+ required");
        self.driver
            .ring
            .submit_and_wait(1)
            .expect("submit_and_wait failed");
        self.drain_completions();
    }

    /// Retry ZC send resubmissions that failed on a previous tick (SQ full).
    fn drain_zc_retries(&mut self) {
        if self.driver.pending_zc_retries.is_empty() {
            return;
        }
        std::mem::swap(
            &mut self.driver.pending_zc_retries,
            &mut self.driver.zc_retry_scratch,
        );
        for idx in 0..self.driver.zc_retry_scratch.len() {
            let (conn_index, generation, slab_idx, retries) = self.driver.zc_retry_scratch[idx];
            if !self.driver.send_slab.in_use(slab_idx) {
                continue; // slab was released in the meantime
            }
            // Connection closed or reused — release the slab and nothing
            // else: the slot may already belong to a new connection.
            if self.driver.connections.get(conn_index).is_none()
                || self.driver.connections.generation(conn_index) != generation
            {
                self.release_zc_slab(slab_idx);
                continue;
            }
            if retries >= 2 {
                // Give up: bytes of this send are already missing from the
                // stream, so fail the waiter and close rather than leaving
                // in_flight stuck and the connection wedged open.
                self.release_zc_slab(slab_idx);
                self.driver.drain_conn_send_queue(conn_index);
                let err = io::Error::other("max retries during zc send resubmit");
                self.executor.wake_send(conn_index, Err(err));
                self.driver.close_connection(conn_index);
                continue;
            }
            let msg_ptr = self.driver.send_slab.msghdr_ptr(slab_idx);
            if self
                .driver
                .ring
                .submit_send_msg_zc(conn_index, msg_ptr, slab_idx)
                .is_err()
            {
                self.driver.pending_zc_retries.push((
                    conn_index,
                    generation,
                    slab_idx,
                    retries + 1,
                ));
            }
        }
        self.driver.zc_retry_scratch.clear();
    }

    /// Release a ZC slab entry (and its paired pool slot) that will never
    /// get a completion CQE because its resubmission was abandoned.
    fn release_zc_slab(&mut self, slab_idx: u16) {
        self.driver.send_slab.mark_awaiting_notifications(slab_idx);
        if self.driver.send_slab.should_release(slab_idx) {
            let pool_slot = self.driver.send_slab.release(slab_idx);
            if pool_slot != u16::MAX {
                self.driver.send_copy_pool.release(pool_slot);
            }
        }
    }

    /// Retry coalesced send resubmissions that failed (SQ was full).
    fn drain_coalesced_retries(&mut self) {
        if self.driver.pending_coalesced_retries.is_empty() {
            return;
        }
        std::mem::swap(
            &mut self.driver.pending_coalesced_retries,
            &mut self.driver.coalesced_retry_scratch,
        );
        for idx in 0..self.driver.coalesced_retry_scratch.len() {
            let (conn_index, generation, slab_idx, retries) =
                self.driver.coalesced_retry_scratch[idx];
            if !self.driver.send_slab.in_use(slab_idx) {
                continue; // slab released meanwhile
            }
            // Connection closed or reused — release the slab only. Touching
            // the send queue here would drain a *new* connection's sends.
            if self.driver.connections.get(conn_index).is_none()
                || self.driver.connections.generation(conn_index) != generation
            {
                self.release_coalesced(slab_idx);
                continue;
            }
            if retries >= 2 {
                // Give up: fail the waiter and close so the connection isn't
                // left open with a hole in its byte stream.
                self.release_coalesced(slab_idx);
                self.driver.drain_conn_send_queue(conn_index);
                let err = io::Error::other("max retries during coalesced send resubmit");
                self.executor.wake_send(conn_index, Err(err));
                self.driver.close_connection(conn_index);
                continue;
            }
            let msg_ptr = self.driver.send_slab.msghdr_ptr(slab_idx);
            if self
                .driver
                .ring
                .submit_send_msg_coalesced(conn_index, msg_ptr, slab_idx)
                .is_err()
            {
                self.driver.pending_coalesced_retries.push((
                    conn_index,
                    generation,
                    slab_idx,
                    retries + 1,
                ));
            }
        }
        self.driver.coalesced_retry_scratch.clear();
    }

    /// Retry recv-forward send resubmissions that failed (SQ was full).
    fn drain_recv_forward_retries(&mut self) {
        if self.driver.pending_recv_forward_retries.is_empty() {
            return;
        }
        std::mem::swap(
            &mut self.driver.pending_recv_forward_retries,
            &mut self.driver.recv_forward_retry_scratch,
        );
        for idx in 0..self.driver.recv_forward_retry_scratch.len() {
            let (conn_index, generation, slab_idx, retries) =
                self.driver.recv_forward_retry_scratch[idx];
            if !self.driver.send_slab.in_use(slab_idx) {
                continue; // slab released meanwhile
            }
            // Connection closed or reused — replenish bids and release only.
            if self.driver.connections.get(conn_index).is_none()
                || self.driver.connections.generation(conn_index) != generation
            {
                self.release_recv_forward(slab_idx);
                continue;
            }
            if retries >= 2 {
                // Give up: forwarded bytes were dropped mid-stream, so close
                // instead of forwarding the rest of the queue after the gap.
                self.release_recv_forward(slab_idx);
                self.driver.drain_conn_send_queue(conn_index);
                let err = io::Error::other("max retries during recv-forward resubmit");
                self.executor.wake_send(conn_index, Err(err));
                self.driver.close_connection(conn_index);
                continue;
            }
            let msg_ptr = self.driver.send_slab.msghdr_ptr(slab_idx);
            if self
                .driver
                .ring
                .submit_send_recv_bufs_coalesced(conn_index, msg_ptr, slab_idx)
                .is_err()
            {
                self.driver.pending_recv_forward_retries.push((
                    conn_index,
                    generation,
                    slab_idx,
                    retries + 1,
                ));
            }
        }
        self.driver.recv_forward_retry_scratch.clear();
    }

    /// Retry copy send resubmissions that failed (SQ was full).
    fn drain_copy_retries(&mut self) {
        if self.driver.pending_copy_retries.is_empty() {
            return;
        }
        std::mem::swap(
            &mut self.driver.pending_copy_retries,
            &mut self.driver.copy_retry_scratch,
        );
        for idx in 0..self.driver.copy_retry_scratch.len() {
            let (conn_index, generation, pool_slot, retries, op) =
                self.driver.copy_retry_scratch[idx];
            if !self.driver.send_copy_pool.in_use(pool_slot) {
                continue;
            }
            // Connection closed or reused — release the slot only.
            if self.driver.connections.get(conn_index).is_none()
                || self.driver.connections.generation(conn_index) != generation
            {
                self.driver.send_copy_pool.release(pool_slot);
                continue;
            }
            if retries >= 2 {
                // Give up: fail the waiter and close so the connection isn't
                // left open with a hole in its byte stream.
                self.driver.send_copy_pool.release(pool_slot);
                self.driver.drain_conn_send_queue(conn_index);
                let err = io::Error::other("max retries during send resubmit");
                self.executor.wake_send(conn_index, Err(err));
                self.driver.close_connection(conn_index);
                continue;
            }
            let (ptr, remaining) = self.driver.send_copy_pool.current_ptr_remaining(pool_slot);
            // Resubmit with the entry's original OpTag. Choosing by
            // tls_table membership here re-tagged the *final* chunk of a TLS
            // send (deliberately OpTag::Send so its CQE wakes the waiter) as
            // TlsSend, whose handler never wakes — a permanent send() hang.
            let result = if matches!(op, OpTag::TlsSend) {
                self.driver
                    .ring
                    .submit_tls_send(conn_index, ptr, remaining, pool_slot)
            } else {
                self.driver
                    .ring
                    .submit_send_copied(conn_index, ptr, remaining, pool_slot)
            };
            if result.is_err() {
                self.driver.pending_copy_retries.push((
                    conn_index,
                    generation,
                    pool_slot,
                    retries + 1,
                    op,
                ));
            }
        }
        self.driver.copy_retry_scratch.clear();
    }

    /// Retry Close submissions that failed (SQ was full). Entries are never
    /// dropped: the connection slot cannot be reused until the Close CQE
    /// runs handle_close, so giving up would leak the fd and the slot
    /// permanently. Backoff: attempt only every 4th tick.
    fn drain_close_retries(&mut self) {
        if self.driver.pending_close_retries.is_empty() {
            return;
        }
        let retries: Vec<_> = self.driver.pending_close_retries.drain(..).collect();
        let tick_mod = self.driver.tick_count % 4;
        for (conn_index, retry) in retries {
            if tick_mod != 0 {
                // Not this tick — keep the entry queued.
                self.driver.pending_close_retries.push((conn_index, retry));
                continue;
            }
            if self.driver.ring.submit_close(conn_index).is_err() {
                self.driver
                    .pending_close_retries
                    .push((conn_index, retry.saturating_add(1)));
            }
        }
    }

    /// Retry POLLOUT arming that failed at EAGAIN time (SQ was full).
    /// Max 3 attempts with backoff: attempt only every 2nd tick, keeping
    /// entries queued in between.
    fn drain_send_pollout_retries(&mut self) {
        if self.driver.pending_send_pollout_retries.is_empty() {
            return;
        }
        std::mem::swap(
            &mut self.driver.pending_send_pollout_retries,
            &mut self.driver.send_pollout_retry_scratch,
        );
        let tick_mod = self.driver.tick_count % 2;
        for idx in 0..self.driver.send_pollout_retry_scratch.len() {
            let (conn_index, generation, pool_slot, retry, is_tls) =
                self.driver.send_pollout_retry_scratch[idx];
            if retry >= 3 {
                // Max retries exceeded — release pool + drain queue + close.
                if self.driver.send_copy_pool.in_use(pool_slot) {
                    self.driver.send_copy_pool.release(pool_slot);
                }
                self.driver.drain_conn_send_queue(conn_index);
                let err = io::Error::other("max retries during send pollout retry");
                self.executor.wake_send(conn_index, Err(err));
                self.driver.close_connection(conn_index);
                continue;
            }
            if tick_mod != 0 {
                // Not this tick — keep the entry queued.
                self.driver
                    .pending_send_pollout_retries
                    .push((conn_index, generation, pool_slot, retry, is_tls));
                continue;
            }
            if !self.driver.send_copy_pool.in_use(pool_slot) {
                continue;
            }
            if self.driver.connections.get(conn_index).is_none()
                || self.driver.connections.generation(conn_index) != generation
            {
                if self.driver.send_copy_pool.in_use(pool_slot) {
                    self.driver.send_copy_pool.release(pool_slot);
                }
                continue;
            }
            if self
                .driver
                .ring
                .submit_send_pollout(conn_index, pool_slot, is_tls)
                .is_err()
            {
                self.driver.pending_send_pollout_retries.push((
                    conn_index,
                    generation,
                    pool_slot,
                    retry + 1,
                    is_tls,
                ));
            }
        }
        self.driver.send_pollout_retry_scratch.clear();
    }

    /// Test-only: inject multiple NOPs and dispatch them all in one batch.
    /// This tests batch CQE processing where one handler's side effects
    /// affect subsequent handlers in the same drain_completions() call.
    #[cfg(test)]
    /// Test-only: submit a linked chain of NOP injects and dispatch.
    /// The first N-1 SQEs have IO_LINK set; the last does not.
    /// This tests IOSQE_IO_LINK error propagation through the kernel.
    #[cfg(test)]
    pub(crate) fn inject_linked_chain_and_dispatch(&mut self, cqes: &[(u64, i32)]) {
        let last = cqes.len() - 1;
        for (i, &(user_data_raw, result)) in cqes.iter().enumerate() {
            if i < last {
                self.driver
                    .ring
                    .submit_nop_inject_linked(user_data_raw, result)
                    .expect("submit_nop_inject_linked failed");
            } else {
                self.driver
                    .ring
                    .submit_nop_inject(user_data_raw, result)
                    .expect("submit_nop_inject failed");
            }
        }
        self.driver
            .ring
            .submit_and_wait(cqes.len() as u32)
            .expect("submit_and_wait failed");
        self.drain_completions();
    }

    /// Test-only: inject multiple NOPs and dispatch them all in one batch.
    #[cfg(test)]
    pub(crate) fn inject_batch_and_dispatch(&mut self, cqes: &[(u64, i32)]) {
        for &(user_data_raw, result) in cqes {
            self.driver
                .ring
                .submit_nop_inject(user_data_raw, result)
                .expect("submit_nop_inject failed");
        }
        self.driver
            .ring
            .submit_and_wait(cqes.len() as u32)
            .expect("submit_and_wait failed");
        self.drain_completions();
    }

    /// Check close_notify deadlines on the armed set. If a connection
    /// has `close_pending` and the `close_notify_deadline` has elapsed,
    /// force-close it by calling `try_finalize_close`.
    ///
    /// Iterates only the indices in `driver.close_notify_armed`, not
    /// every entry in `driver.send_queues`. For non-TLS workloads the
    /// armed set is permanently empty and this method is a single
    /// `Vec::is_empty()` check; for TLS workloads the set is bounded
    /// by the number of concurrent in-flight TLS graceful shutdowns
    /// (typically 0 or single digits).
    ///
    /// Profiling the redis bench at 1 client × 64 B (i.e. plain TCP)
    /// showed the previous O(N over all slots) walk at ~25 % of
    /// worker CPU because it ran on every event-loop iteration; the
    /// armed-set version drops that to noise.
    fn check_close_notify_deadlines(&mut self) {
        if self.driver.close_notify_armed.is_empty() {
            return;
        }
        let now = std::time::Instant::now();
        // Collect timed-out indices first to avoid borrow conflict
        // with `try_finalize_close`, which mutates `close_notify_armed`.
        let mut timed_out: Vec<u32> = Vec::new();
        for &idx in self.driver.close_notify_armed.iter() {
            let state = &self.driver.send_queues[idx as usize];
            if state.close_pending
                && let Some(deadline) = state.close_notify_deadline
                && now >= deadline
            {
                timed_out.push(idx);
            }
        }
        for idx in timed_out {
            self.driver.try_finalize_close(idx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConfigBuilder;
    use crate::completion::{OpTag, UserData};
    use crate::config::Config;
    use crate::runtime::io::ConnCtx;
    use crate::runtime::io::SegConsumed;
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    /// Minimal handler for testing — does nothing.
    struct NoopHandler;

    impl AsyncEventHandler for NoopHandler {
        #[allow(clippy::manual_async_fn)]
        fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
            async {}
        }
        fn create_for_worker(_id: usize) -> Self {
            NoopHandler
        }
    }

    fn test_config_builder() -> ConfigBuilder {
        ConfigBuilder::new()
            .workers(1)
            .pin_to_core(false)
            .sq_entries(32)
            .recv_buffer(16, 4096)
            // Reserve 0: the shared 16-buffer ring never hits the low-water mark
            // in these unit tests, so segmented deliveries stay zero-copy
            // (Pinned) as the existing assertions expect. Force-copy behavior is
            // covered by dedicated tests that raise the reserve explicitly.
            .recv_segment_reserve(0)
            .max_connections(16)
            .send_pool(16, 16384)
            .send_slab_slots(8)
            .fs(crate::fs::FsConfig {
                max_files: 2,
                max_commands_in_flight: 4,
            })
    }

    fn test_config() -> Config {
        test_config_builder().build().expect("valid config")
    }

    /// A test config with an explicit segmented-recv low-water reserve. With the
    /// 16-buffer test ring, `reserve == 16` forces every segmented delivery to
    /// Mode C (Owned copy), while a small reserve keeps early deliveries Pinned.
    fn config_with_reserve(reserve: u32) -> Config {
        test_config_builder()
            .recv_segment_reserve(reserve)
            .build()
            .expect("valid config")
    }

    /// A test config with an explicit Mode A `forward_to` held-buffer cap (and
    /// reserve 0 so held buffers stay Pinned in the 16-buffer test ring).
    fn config_with_forward_cap(cap: usize) -> Config {
        test_config_builder()
            .recv_segment_reserve(0)
            .forward_hold_cap(cap)
            .build()
            .expect("valid config")
    }

    /// Create a test event loop. Requires Linux with io_uring support.
    fn make_test_loop() -> AsyncEventLoop<NoopHandler> {
        make_test_loop_with_config(test_config())
    }

    /// Create a test event loop from an explicit config (e.g. to exercise the
    /// segmented-recv low-water reserve, which `test_config` pins to 0).
    fn make_test_loop_with_config(config: Config) -> AsyncEventLoop<NoopHandler> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        assert!(eventfd >= 0, "eventfd creation failed");
        let (_region_tx, region_rx) = crossbeam_channel::unbounded();
        AsyncEventLoop::new(
            &config,
            NoopHandler,
            None,
            eventfd,
            shutdown,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            region_rx,
        )
        .expect("failed to create test event loop")
    }

    /// Simulate an accepted plaintext connection at the given index.
    /// Returns the conn_index that was allocated.
    fn accept_connection(el: &mut AsyncEventLoop<NoopHandler>) -> u32 {
        let conn_index = el.driver.connections.allocate().expect("no free slots");
        el.driver.accumulators.reset(conn_index);
        // arm_recv needs to submit an SQE — skip in test since we inject CQEs directly.
        // Just set recv_mode = Multi so the handlers work correctly.
        if let Some(cs) = el.driver.connections.get_mut(conn_index) {
            cs.recv_mode = RecvMode::Multi;
            cs.established = true;
        }
        conn_index
    }

    // ── Send path tests ────────────────────────────────────────────

    #[test]
    fn handle_send_complete_releases_pool_slot() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Allocate a pool slot (simulating send_nowait).
        let data = b"hello";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        // Simulate send CQE: all bytes sent.
        let ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), data.len() as i32, 0);

        // Pool slot should be released.
        assert_eq!(
            el.driver.send_copy_pool.free_count(),
            free_before + 1,
            "pool slot not released after send complete"
        );
    }

    #[test]
    fn handle_send_error_releases_pool_slot() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let data = b"hello";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        // Simulate send error (ECONNRESET = -104).
        let ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), -104, 0);

        assert_eq!(
            el.driver.send_copy_pool.free_count(),
            free_before + 1,
            "pool slot not released after send error"
        );
    }

    #[test]
    fn handle_send_wakes_send_waiter() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Set up a send waiter.
        el.executor.send_waiters[conn_index as usize] = true;

        let data = b"hello";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();

        let ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), data.len() as i32, 0);

        // Waiter should be cleared and result stored.
        assert!(
            !el.executor.send_waiters[conn_index as usize],
            "send waiter not cleared"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "send result not stored"
        );
    }

    #[test]
    fn handle_send_multichunk_wakes_once_with_total() {
        // A logical send larger than one pool slot is split into several
        // chunks that complete as separate CQEs, but the connection has a
        // single send waiter. The waiter must be woken exactly once — when
        // the whole logical send has drained — reporting the full byte
        // count, not the first chunk's short count.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.executor.send_waiters[conn_index as usize] = true;

        // Chunk 0 is in flight; chunk 1 is queued behind it. Distinct sizes
        // so the summed total can't be mistaken for either chunk alone.
        let chunk0 = vec![b'a'; 16384];
        let chunk1 = vec![b'b'; 4096];
        let total = (chunk0.len() + chunk1.len()) as u32;
        let (slot0, _p0, _l0) = el.driver.send_copy_pool.copy_in(&chunk0).unwrap();
        let (slot1, ptr1, len1) = el.driver.send_copy_pool.copy_in(&chunk1).unwrap();
        // One logical send split across two slots: only the last is end-of-send.
        el.driver.send_copy_pool.set_end_of_send(slot0, false);
        el.driver.send_copy_pool.set_end_of_send(slot1, true);

        // Queue chunk 1 as a real BuiltSend behind the in-flight chunk 0.
        let ud1 = UserData::encode(OpTag::Send, conn_index, slot1 as u32);
        let entry1 = io_uring::opcode::Send::new(io_uring::types::Fixed(conn_index), ptr1, len1)
            .flags(crate::completion::STREAM_SEND_FLAGS)
            .build()
            .user_data(ud1.raw());
        el.driver.send_queues[conn_index as usize]
            .queue
            .push_back(crate::handler::BuiltSend {
                entry: entry1,
                pool_slot: slot1,
                slab_idx: u16::MAX,
                total_len: chunk1.len() as u32,
            });
        el.driver.send_queues[conn_index as usize].in_flight = true;

        // Chunk 0 completes. The waiter must NOT be woken yet.
        let ud0 = UserData::encode(OpTag::Send, conn_index, slot0 as u32);
        el.test_dispatch_cqe(ud0.raw(), chunk0.len() as i32, 0);
        assert!(
            el.executor.send_waiters[conn_index as usize],
            "send waiter woken on the first chunk of a multi-chunk send"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_none(),
            "send result stored before the logical send drained"
        );
        assert_eq!(
            el.driver.send_queues[conn_index as usize].acked_bytes,
            chunk0.len() as u32,
            "first chunk's bytes not accumulated"
        );

        // Chunk 1 completes and drains the queue. The waiter wakes once,
        // reporting the whole logical send, and the accumulator resets.
        el.test_dispatch_cqe(ud1.raw(), chunk1.len() as i32, 0);
        assert!(
            !el.executor.send_waiters[conn_index as usize],
            "send waiter not woken after the queue drained"
        );
        match &el.executor.io_results[conn_index as usize] {
            Some(crate::runtime::IoResult::Send(Ok(n))) => assert_eq!(
                *n, total,
                "waiter woken with a short count instead of the full logical send"
            ),
            _ => panic!("expected Send(Ok(_)) result after the logical send drained"),
        }
        assert_eq!(
            el.driver.send_queues[conn_index as usize].acked_bytes, 0,
            "accumulator not reset after the logical send completed"
        );
    }

    #[test]
    fn handle_send_pipelined_independent_sends_wake_separately() {
        // Two independent conn.send() calls pipelined on one connection share
        // the per-connection send queue but each has its own waiter/result.
        // Unlike chunks of one logical send, each must wake with its own byte
        // count, not a running total. (Regression: joining two sends hung when
        // the fix woke once on queue-drain and conflated the two.)
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.executor.send_waiters[conn_index as usize] = true;

        // Each is its own logical send, so both slots are end-of-send (the
        // copy_in default).
        let a = b"HELLO";
        let b = b"WORLD";
        let (slot_a, _pa, _la) = el.driver.send_copy_pool.copy_in(a).unwrap();
        let (slot_b, ptr_b, len_b) = el.driver.send_copy_pool.copy_in(b).unwrap();

        // Send A is in flight; send B is queued behind it.
        let ud_b = UserData::encode(OpTag::Send, conn_index, slot_b as u32);
        let entry_b = io_uring::opcode::Send::new(io_uring::types::Fixed(conn_index), ptr_b, len_b)
            .flags(crate::completion::STREAM_SEND_FLAGS)
            .build()
            .user_data(ud_b.raw());
        el.driver.send_queues[conn_index as usize]
            .queue
            .push_back(crate::handler::BuiltSend {
                entry: entry_b,
                pool_slot: slot_b,
                slab_idx: u16::MAX,
                total_len: b.len() as u32,
            });
        el.driver.send_queues[conn_index as usize].in_flight = true;

        // A completes: its waiter wakes with A's own byte count, not accumulated.
        let ud_a = UserData::encode(OpTag::Send, conn_index, slot_a as u32);
        el.test_dispatch_cqe(ud_a.raw(), a.len() as i32, 0);
        match &el.executor.io_results[conn_index as usize] {
            Some(crate::runtime::IoResult::Send(Ok(n))) => {
                assert_eq!(*n, a.len() as u32, "send A woke with the wrong count")
            }
            _ => panic!("send A's waiter was not woken"),
        }

        // The future consumes A's result; the next send re-arms the waiter.
        el.executor.io_results[conn_index as usize] = None;
        el.executor.send_waiters[conn_index as usize] = true;

        // B completes: its waiter wakes with B's own count, not A + B.
        el.test_dispatch_cqe(ud_b.raw(), b.len() as i32, 0);
        match &el.executor.io_results[conn_index as usize] {
            Some(crate::runtime::IoResult::Send(Ok(n))) => {
                assert_eq!(*n, b.len() as u32, "send B woke with an accumulated count")
            }
            _ => panic!("send B's waiter was not woken"),
        }
    }

    // ── ZC send path tests ─────────────────────────────────────────

    #[test]
    fn handle_send_msg_zc_notif_releases_slab() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Allocate a slab entry.
        let iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 100,
        }];
        let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
        let (slab_idx, _ptr) = el
            .driver
            .send_slab
            .allocate(conn_index, &iovecs, u16::MAX, guards, 0, 100)
            .unwrap();
        let free_before = el.driver.send_slab.free_count();

        // Simulate successful operation CQE (result > 0, not partial).
        el.driver.send_slab.inc_pending_notifs(slab_idx);
        el.driver.send_slab.mark_awaiting_notifications(slab_idx);

        // Simulate notification CQE.
        let ud = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        let notif_flags = 8u32; // IORING_CQE_F_NOTIF
        el.test_dispatch_cqe(ud.raw(), 0, notif_flags);

        assert_eq!(
            el.driver.send_slab.free_count(),
            free_before + 1,
            "slab entry not released after notification"
        );
    }

    #[test]
    fn handle_send_msg_zc_error_does_not_increment_notifs() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 100,
        }];
        let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
        let (slab_idx, _ptr) = el
            .driver
            .send_slab
            .allocate(conn_index, &iovecs, u16::MAX, guards, 0, 100)
            .unwrap();

        // Simulate error CQE (result < 0).
        let ud = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        el.test_dispatch_cqe(ud.raw(), -104, 0);

        // Slab should be released (not leaked waiting for notification).
        assert!(
            el.driver.send_slab.should_release(slab_idx) || !el.driver.send_slab.in_use(slab_idx),
            "slab entry leaked after ZC send error"
        );
    }

    #[test]
    fn handle_send_msg_zc_result_zero_does_not_leak_slab() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 100,
        }];
        let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
        let (slab_idx, _ptr) = el
            .driver
            .send_slab
            .allocate(conn_index, &iovecs, u16::MAX, guards, 0, 100)
            .unwrap();

        // Simulate result == 0 CQE (no bytes sent, no notification expected).
        let ud = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        el.test_dispatch_cqe(ud.raw(), 0, 0);

        // Slab should be releasable (pending_notifs == 0).
        assert!(
            !el.driver.send_slab.in_use(slab_idx) || el.driver.send_slab.should_release(slab_idx),
            "slab entry leaked on result == 0"
        );
    }

    // ── Recv path tests ────────────────────────────────────────────

    #[test]
    fn handle_recv_multi_eof_closes_connection() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Simulate EOF CQE (result == 0).
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), 0, 0);

        // Connection should be marked as closing.
        let conn = el.driver.connections.get(conn_index);
        assert!(
            conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed),
            "connection not closed after recv EOF"
        );
    }

    #[test]
    fn handle_recv_multi_stale_connection_replenishes_buffer() {
        let mut el = make_test_loop();

        // Don't allocate a connection — simulate stale CQE for conn_index 0.
        let replenish_before = el.driver.pending_replenish.len();

        // Simulate recv CQE with result > 0 and a buffer ID in flags.
        // IORING_CQE_F_BUFFER = 1 << 0, buffer ID in upper 16 bits of flags.
        let bid: u16 = 5;
        let flags = (1u32) | ((bid as u32) << 16); // CQE_F_BUFFER | bid
        let ud = UserData::encode(OpTag::RecvMulti, 0, 0);
        el.test_dispatch_cqe(ud.raw(), 100, flags);

        // Buffer should be replenished despite stale connection.
        assert_eq!(
            el.driver.pending_replenish.len(),
            replenish_before + 1,
            "buffer not replenished on stale connection CQE"
        );
        assert_eq!(el.driver.pending_replenish[0], bid);
    }

    // ── Close path tests ───────────────────────────────────────────

    #[test]
    fn handle_close_releases_connection_slot() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        assert!(el.driver.connections.get(conn_index).is_some());

        // Close the connection (sets recv_mode = Closed, submits Close SQE).
        el.driver.close_connection(conn_index);

        // Simulate Close CQE.
        let ud = UserData::encode(OpTag::Close, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), 0, 0);

        // Connection slot should be released.
        assert!(
            el.driver.connections.get(conn_index).is_none(),
            "connection slot not released after Close CQE"
        );
    }

    // ── Recv data delivery tests ───────────────────────────────────

    #[test]
    fn handle_recv_multi_data_appends_to_accumulator() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // The provided buffer ring has real buffers. Get a valid buffer ID.
        // We'll simulate a recv CQE that references buffer 0.
        let bid: u16 = 0;
        // IORING_CQE_F_BUFFER = 1, IORING_CQE_F_MORE = 2. bid in upper 16 bits.
        let flags = 1u32 | 2u32 | ((bid as u32) << 16);
        let bytes_received = 5i32;

        // Write test data into the buffer ring's backing memory so the
        // handler reads it into the accumulator.
        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(bid);
        unsafe {
            std::ptr::copy_nonoverlapping(b"hello".as_ptr(), buf_ptr as *mut u8, 5);
        }

        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), bytes_received, flags);

        // With zero-copy recv, first completion should be held in pending
        // buffer slot (not copied to accumulator). Accumulator should be empty.
        let data = el.driver.accumulators.data(conn_index);
        assert!(
            data.is_empty(),
            "data should NOT be in accumulator (zero-copy)"
        );

        let pending = el.driver.pending_recv_bufs[conn_index as usize];
        assert!(pending.is_some(), "pending recv buf should be set");
        let pending = pending.unwrap();
        assert_eq!(pending.bid, bid);
        assert_eq!(pending.len, bytes_received as u32);

        // Buffer should NOT be queued for replenish yet (deferred).
        assert!(
            !el.driver.pending_replenish.contains(&bid),
            "buffer should NOT be replenished yet (zero-copy deferred)"
        );
    }

    #[test]
    fn handle_recv_multi_handout_decrements_free_then_replenish_restores() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let entries = el.driver.provided_bufs.ring_entries();
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "all buffers free before any recv"
        );

        let bid: u16 = 0;
        let flags = 1u32 | 2u32 | ((bid as u32) << 16); // F_BUFFER | F_MORE, bid=0
        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(bid);
        unsafe {
            std::ptr::copy_nonoverlapping(b"hi".as_ptr(), buf_ptr as *mut u8, 2);
        }
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), 2, flags);

        // Handout accounted: exactly one fewer free.
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries - 1,
            "recv handout must decrement free by one"
        );

        // The consume path returns the bid via replenish_batch; free is restored.
        el.driver.provided_bufs.replenish_batch(&[bid]);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "replenish must restore free (balanced accounting)"
        );
    }

    #[test]
    fn handle_recv_multi_segmented_holds_buffer_and_teardown_drains() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Opt this connection into segmented delivery.
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        let entries = el.driver.provided_bufs.ring_entries();
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "all buffers free before any recv"
        );

        let bid: u16 = 0;
        let flags = 1u32 | 2u32 | ((bid as u32) << 16); // F_BUFFER | F_MORE, bid=0
        let bytes_received = 5i32;
        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(bid);
        unsafe {
            std::ptr::copy_nonoverlapping(b"hello".as_ptr(), buf_ptr as *mut u8, 5);
        }
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), bytes_received, flags);

        // (a) Buffer went to the segment hold, NOT the accumulator or the
        // single-buffer zero-copy slot, and is NOT queued for replenish.
        assert!(
            el.driver.accumulators.data(conn_index).is_empty(),
            "segmented recv must not append to the accumulator"
        );
        assert!(
            el.driver.pending_recv_bufs[conn_index as usize].is_none(),
            "segmented recv must not use the single-buffer zero-copy slot"
        );
        assert!(
            !el.driver.pending_replenish.contains(&bid),
            "held segment bid must NOT be replenished while held"
        );
        let hold = &el.driver.segment_hold[conn_index as usize];
        assert_eq!(hold.len(), 1, "buffer should be held in segment_hold");
        match &hold[0] {
            crate::backend::HeldRecvBuf::Pinned { bid: hbid, len } => {
                assert_eq!(*hbid, bid);
                assert_eq!(*len, bytes_received as u32);
            }
            crate::backend::HeldRecvBuf::Owned(_) => {
                panic!("above the reserve, delivery must be Pinned (zero-copy)")
            }
        }

        // (b) The held buffer is accounted against the ring's free count.
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries - 1,
            "held segment must decrement free by one"
        );

        // (c) Close does NOT drain the hold — a post-FIN reader must still be able
        // to consume the already-received bytes (see the data+FIN-loss regression
        // test). The held bid stays out of the ring.
        let generation = el.driver.connections.generation(conn_index);
        el.driver.close_connection(conn_index);
        assert_eq!(
            el.driver.segment_hold[conn_index as usize].len(),
            1,
            "close must NOT drain the segment hold (a reader may still consume it)"
        );
        assert!(
            !el.driver.pending_replenish.contains(&bid),
            "the held bid must not return to the ring at close time"
        );

        // (d) Teardown (the Close CQE → handle_close) reclaims any unconsumed held
        // buffers; committing the replenish restores the ring.
        let close_ud = UserData::encode(OpTag::Close, conn_index, generation);
        el.test_dispatch_cqe(close_ud.raw(), 0, 0);
        assert!(
            el.driver.segment_hold[conn_index as usize].is_empty(),
            "handle_close drains the unconsumed hold"
        );
        assert!(
            el.driver.pending_replenish.contains(&bid),
            "handle_close queues the held bid for replenish"
        );
        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "free must return to ring_entries after the held bid is replenished"
        );
    }

    // ── Segmented recv reader (SegmentReader / RecvSegment, Mode B2) ────

    /// A minimal no-op waker for driving `SegmentReader::next` in tests. The
    /// futures park via `recv_waiters` and are re-polled manually, so the waker
    /// is never actually invoked.
    fn noop_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        unsafe fn no_op(_: *const ()) {}
        unsafe fn clone_fn(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, no_op, no_op, no_op);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    /// Run `f` with the worker's `CURRENT_DRIVER` thread-local pointing at
    /// `el`'s driver/executor — the same mechanism the real event loop installs
    /// around task polls — so futures/`Drop` impls that call `with_state` /
    /// `try_with_state` observe a live driver. `f` must not touch `el` directly
    /// (it aliases the raw pointers installed here); use `with_state` inside.
    fn with_driver_state<R>(el: &mut AsyncEventLoop<NoopHandler>, f: impl FnOnce() -> R) -> R {
        let driver_ptr = &mut el.driver as *mut Driver;
        let executor_ptr = &mut el.executor as *mut crate::runtime::Executor;
        let mut ds = DriverState {
            driver: unsafe { NonNull::new_unchecked(driver_ptr) },
            executor: unsafe { NonNull::new_unchecked(executor_ptr) },
        };
        let _guard = unsafe { set_driver_state_guarded(&mut ds) };
        f()
    }

    /// Deliver one segmented recv buffer to `conn_index` via a synthetic
    /// multishot-recv CQE (`bid`, `data`). The connection must already be in the
    /// `Segmented` domain. Mirrors the real `handle_recv_multi` hold path.
    fn deliver_segment(
        el: &mut AsyncEventLoop<NoopHandler>,
        conn_index: u32,
        bid: u16,
        data: &[u8],
    ) {
        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(bid);
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr as *mut u8, data.len());
        }
        let flags = 1u32 | 2u32 | ((bid as u32) << 16); // F_BUFFER | F_MORE
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), data.len() as i32, flags);
    }

    #[test]
    fn segment_reader_hands_out_segment_bytes_and_drop_replenishes() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();

        // Opt in and deliver one buffer; it lands in the hold, not the accumulator.
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;
        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");
        assert_eq!(el.driver.provided_bufs.free(), entries - 1);

        // Drive the reader: next() moves the held buffer into the pin slot and
        // hands out a segment.
        let conn = ConnCtx::new(conn_index, generation);
        let mut reader = with_driver_state(&mut el, || conn.segments());
        let waker = noop_waker();
        let mut fut = std::pin::pin!(reader.next());
        let seg = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(seg))) => seg,
            _ => panic!("expected a segment from a non-empty hold"),
        };

        // (a) The segment derefs to the received bytes.
        with_driver_state(&mut el, || {
            assert_eq!(&seg[..], b"hello", "segment bytes must match received data");
            assert_eq!(seg.len(), 5);
            assert!(!seg.is_empty());
        });
        // Checked out: hold emptied into the pin slot, still one buffer outstanding.
        assert!(el.driver.segment_hold[conn_index as usize].is_empty());
        assert!(el.driver.segment_pinned[conn_index as usize].is_some());
        assert_eq!(el.driver.provided_bufs.free(), entries - 1);

        // (b) Dropping the segment (in-poll / guarded) replenishes its bid and
        // clears the pin slot; committing the replenish restores the ring.
        with_driver_state(&mut el, || drop(seg));
        assert!(
            el.driver.segment_pinned[conn_index as usize].is_none(),
            "drop clears pin slot"
        );
        assert!(
            el.driver.pending_replenish.contains(&bid),
            "drop queues the bid for replenish"
        );
        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "free restored after drop-replenish"
        );
    }

    #[test]
    fn segment_reader_next_parks_then_resumes_on_recv() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        let conn = ConnCtx::new(conn_index, generation);
        let mut reader = with_driver_state(&mut el, || conn.segments());
        let waker = noop_waker();
        let mut fut = std::pin::pin!(reader.next());

        // First poll: hold empty, connection open → parks as a recv waiter.
        let p1 = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(
            matches!(p1, std::task::Poll::Pending),
            "next parks on an empty hold"
        );
        assert!(
            el.executor.recv_waiters[conn_index as usize],
            "parked next registers a recv waiter"
        );

        // Simulate a recv arrival: the hold gets a buffer and wake_recv fires.
        deliver_segment(&mut el, conn_index, 0, b"world");
        assert!(
            !el.executor.recv_waiters[conn_index as usize],
            "wake_recv cleared the recv waiter on delivery"
        );

        // Second poll: the buffer is now held → resumes with a segment.
        let seg = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(seg))) => seg,
            _ => panic!("expected a segment after the simulated recv"),
        };
        with_driver_state(&mut el, || {
            assert_eq!(
                &seg[..],
                b"world",
                "resumed segment carries the delivered bytes"
            );
            drop(seg);
        });
    }

    /// Regression for the Mode B RecvSegment UAF: closing a connection while a
    /// `RecvSegment` is still checked out must NOT return its pinned bid to the
    /// ring. The segment's `deref` still reads that provided buffer, and a parked
    /// task can resume and read it before the `Close` CQE — recycling the bid at
    /// close time would let another connection's recv overwrite live data. The
    /// release is deferred: an in-poll drop (here) releases exactly once via the
    /// pin slot; teardown via `handle_close` is covered by the next test.
    #[test]
    fn close_while_segment_pinned_defers_bid_release() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        // Deliver and check out a segment so its bid sits in the pin slot.
        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");
        let conn = ConnCtx::new(conn_index, generation);
        let mut reader = with_driver_state(&mut el, || conn.segments());
        let waker = noop_waker();
        let mut fut = std::pin::pin!(reader.next());
        let seg = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(seg))) => seg,
            _ => panic!("expected a pinned segment"),
        };
        assert!(
            el.driver.segment_pinned[conn_index as usize].is_some(),
            "segment is pinned"
        );

        // Close while the segment is still checked out: the bid must stay pinned.
        el.driver.close_connection(conn_index);
        assert!(
            el.driver.segment_pinned[conn_index as usize].is_some(),
            "close must not drain the pin slot while a live segment can read it"
        );
        assert!(
            !el.driver.pending_replenish.contains(&bid),
            "the pinned bid must not return to the ring at close time"
        );

        // The segment is still valid after close (reads its own buffer, not
        // recycled memory); dropping it in-poll then releases the bid exactly once.
        with_driver_state(&mut el, || {
            assert_eq!(
                &seg[..],
                b"hello",
                "the live segment still reads its own buffer after close"
            );
            drop(seg);
        });
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "in-poll drop after close replenishes the bid exactly once"
        );
        assert!(
            el.driver.segment_pinned[conn_index as usize].is_none(),
            "the pin slot is cleared by the drop"
        );

        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "free returns to ring_entries — no leak, no double-replenish"
        );
    }

    /// The other Mode B UAF-fix half: when a connection is torn down (the `Close`
    /// CQE → `handle_close` → the future, and with it the segment, is dropped) the
    /// pinned bid must be reclaimed exactly once — the unguarded `RecvSegment::drop`
    /// during teardown no-ops (`CURRENT_DRIVER == None`), so `handle_close` does the
    /// release. Without it the bid would leak.
    #[test]
    fn handle_close_reclaims_pinned_segment_bid_on_teardown() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");
        let conn = ConnCtx::new(conn_index, generation);
        let mut reader = with_driver_state(&mut el, || conn.segments());
        let waker = noop_waker();
        let mut fut = std::pin::pin!(reader.next());
        let seg = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(seg))) => seg,
            _ => panic!("expected a pinned segment"),
        };

        // Close, then let the Close CQE tear the connection down. The bid is still
        // pinned at close time; `handle_close` reclaims it.
        el.driver.close_connection(conn_index);
        assert!(
            !el.driver.pending_replenish.contains(&bid),
            "bid not returned at close time"
        );
        let close_ud = UserData::encode(OpTag::Close, conn_index, generation);
        el.test_dispatch_cqe(close_ud.raw(), 0, 0);
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "handle_close reclaims the pinned segment bid exactly once"
        );

        // The slot is released; the now-stale segment's unguarded drop no-ops (no
        // double-replenish).
        drop(seg);
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "stale segment drop after teardown does not double-replenish"
        );
        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "no leak, no double"
        );
    }

    /// Regression for the Mode B data+FIN-loss bug: a data CQE and the peer FIN can
    /// arrive in the same batch, so `close_connection` runs before the woken reader
    /// is polled. The already-received bytes must NOT be discarded — the reader must
    /// still deliver them, then report EOF.
    #[test]
    fn segment_reader_delivers_held_data_after_close_then_eof() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        // A response buffer is received (held), then the peer FIN closes the conn
        // before the reader runs.
        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"response");
        el.driver.close_connection(conn_index);

        let conn = ConnCtx::new(conn_index, generation);
        let mut reader = with_driver_state(&mut el, || conn.segments());
        let waker = noop_waker();

        // The reader still sees the held response — the data was not lost at close.
        {
            let mut fut = std::pin::pin!(reader.next());
            let seg = match with_driver_state(&mut el, || {
                let mut cx = std::task::Context::from_waker(&waker);
                fut.as_mut().poll(&mut cx)
            }) {
                std::task::Poll::Ready(Ok(Some(seg))) => seg,
                _ => panic!("expected the held response segment after close"),
            };
            with_driver_state(&mut el, || {
                assert_eq!(
                    &seg[..],
                    b"response",
                    "held response delivered after close, not lost"
                );
                drop(seg);
            });
        }

        // Next poll: hold drained + connection `Closed` → clean EOF.
        let mut fut = std::pin::pin!(reader.next());
        let eof = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            matches!(fut.as_mut().poll(&mut cx), std::task::Poll::Ready(Ok(None)))
        });
        assert!(eof, "reader reports EOF after draining the held data");
    }

    /// Medium: a second concurrent `SegmentReader` on the same connection must not
    /// overwrite the pin slot (which would orphan the first segment's bid — a ring
    /// leak). Its `next()` errors instead, consuming nothing.
    #[test]
    fn second_concurrent_segment_reader_errors_rather_than_leaking() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        deliver_segment(&mut el, conn_index, 0, b"hello");
        deliver_segment(&mut el, conn_index, 1, b"world");

        let conn = ConnCtx::new(conn_index, generation);
        let waker = noop_waker();

        // Reader A checks out the first segment → pins bid 0.
        let mut reader_a = with_driver_state(&mut el, || conn.segments());
        let mut fut_a = std::pin::pin!(reader_a.next());
        let seg_a = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut_a.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(s))) => s,
            _ => panic!("reader A: expected a pinned segment"),
        };
        assert!(matches!(
            el.driver.segment_pinned[conn_index as usize],
            Some(crate::backend::HeldRecvBuf::Pinned { bid: 0, .. })
        ));

        // Reader B tries to check out a second segment while A's is still live: the
        // pin slot is occupied → error, and it must NOT pop the next held buffer.
        let mut reader_b = with_driver_state(&mut el, || conn.segments());
        let mut fut_b = std::pin::pin!(reader_b.next());
        let res_b = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut_b.as_mut().poll(&mut cx)
        });
        assert!(
            matches!(res_b, std::task::Poll::Ready(Err(_))),
            "a second concurrent reader must error, not overwrite the pin slot"
        );
        assert!(
            matches!(
                el.driver.segment_pinned[conn_index as usize],
                Some(crate::backend::HeldRecvBuf::Pinned { bid: 0, .. })
            ),
            "reader A's bid stays pinned (not overwritten)"
        );
        assert_eq!(
            el.driver.segment_hold[conn_index as usize].len(),
            1,
            "reader B must not consume the held buffer on the error path"
        );

        // Cleanup: dropping A's segment releases bid 0 exactly once — no leak.
        with_driver_state(&mut el, || drop(seg_a));
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == 0)
                .count(),
            1,
        );
    }

    /// Medium: dropping a `SegmentReader` while the connection is still in the
    /// segmented domain (no explicit `end_segments`) auto-settles — held bytes are
    /// gathered into the accumulator and the default read path is restored — so a
    /// later ordinary read neither hangs nor loses data.
    #[test]
    fn segment_reader_drop_auto_settles_held_data() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        deliver_segment(&mut el, conn_index, 0, b"hello");
        deliver_segment(&mut el, conn_index, 1, b"world");

        let conn = ConnCtx::new(conn_index, generation);
        // Create a reader, consume nothing, drop it — guarded (CURRENT_DRIVER set),
        // as it would be inside a real task poll.
        with_driver_state(&mut el, || {
            let reader = conn.segments();
            drop(reader);
        });

        assert_eq!(
            el.driver.recv_domain[conn_index as usize],
            crate::recv::domain::RecvDomain::default(),
            "reader drop restores the default read path"
        );
        assert_eq!(
            el.driver.accumulators.data(conn_index),
            b"helloworld",
            "held segments are gathered into the accumulator, in order, on drop"
        );
    }

    #[test]
    fn segment_reader_next_returns_none_at_eof() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        // Close the connection (recv side) with nothing held.
        el.driver.close_connection(conn_index);

        let conn = ConnCtx::new(conn_index, generation);
        let mut reader = with_driver_state(&mut el, || conn.segments());
        let waker = noop_waker();
        let mut fut = std::pin::pin!(reader.next());
        let done = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            matches!(fut.as_mut().poll(&mut cx), std::task::Poll::Ready(Ok(None)))
        });
        assert!(
            done,
            "closed connection with an empty hold yields EOF (Ok(None))"
        );
    }

    // ── Segmented recv Mode C (into_owned / recv_owned_segment) ──────────

    #[test]
    fn recv_segment_into_owned_copies_and_replenishes_exactly_once() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        // Deliver and check out a segment so its bid sits in the pin slot.
        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");
        let conn = ConnCtx::new(conn_index, generation);
        let mut reader = with_driver_state(&mut el, || conn.segments());
        let waker = noop_waker();
        let mut fut = std::pin::pin!(reader.next());
        let seg = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(seg))) => seg,
            _ => panic!("expected a pinned segment"),
        };
        assert!(el.driver.segment_pinned[conn_index as usize].is_some());

        // into_owned: copies the bytes and releases the pin (take() → replenish).
        let owned = with_driver_state(&mut el, || seg.into_owned());
        assert_eq!(&owned[..], b"hello", "into_owned returns the correct bytes");

        // (a) The pin slot was taken and the bid queued for replenish — exactly once
        // (into_owned's take(), then `self` drop no-ops on the now-empty slot).
        assert!(
            el.driver.segment_pinned[conn_index as usize].is_none(),
            "into_owned clears the pin slot"
        );
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "into_owned replenishes the bid exactly once (drop-after does not double)"
        );

        // (b) Commit the replenish, then overwrite the underlying buffer: the owned
        // Bytes must be unaffected — proving it is a real copy, not an alias.
        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "free returns to ring_entries after into_owned's single replenish"
        );
        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(bid);
        unsafe {
            std::ptr::copy_nonoverlapping(b"XXXXX".as_ptr(), buf_ptr as *mut u8, 5);
        }
        assert_eq!(
            &owned[..],
            b"hello",
            "owned Bytes is a copy — still valid after the bid is replenished and reused"
        );
    }

    #[test]
    fn recv_owned_segment_copies_and_replenishes_at_delivery() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();

        // recv_owned_segment opts into segmented delivery itself.
        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.recv_owned_segment()));
        // First poll opts in + parks (nothing delivered yet).
        let waker = noop_waker();
        let p0 = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(matches!(p0, std::task::Poll::Pending));
        assert_eq!(
            el.driver.recv_domain[conn_index as usize],
            crate::recv::domain::RecvDomain::Segmented,
            "recv_owned_segment sets the Segmented domain"
        );

        // Deliver a buffer; it lands in the hold and clears the waiter.
        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"world");
        assert_eq!(el.driver.provided_bufs.free(), entries - 1);

        // Second poll: COPY at delivery, bid replenished IMMEDIATELY (never pinned).
        let owned = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(b))) => b,
            other => panic!("expected owned bytes, got {other:?}"),
        };
        assert_eq!(&owned[..], b"world", "recv_owned_segment returns the bytes");

        // Never pinned — the copy IS the release.
        assert!(
            el.driver.segment_pinned[conn_index as usize].is_none(),
            "recv_owned_segment must never pin a buffer"
        );
        assert!(
            el.driver.segment_hold[conn_index as usize].is_empty(),
            "the held buffer was consumed"
        );
        // Bid is queued for replenish while the caller STILL holds the owned Bytes.
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "bid replenished at delivery, before the owned Bytes is dropped"
        );
        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "free already restored while the caller still holds the Bytes"
        );
        // And it is a real copy: reuse the buffer, owned Bytes is unaffected.
        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(bid);
        unsafe {
            std::ptr::copy_nonoverlapping(b"ZZZZZ".as_ptr(), buf_ptr as *mut u8, 5);
        }
        assert_eq!(&owned[..], b"world", "owned Bytes is a copy, not an alias");
    }

    #[test]
    fn recv_owned_segment_parks_then_resumes_on_recv() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);

        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.recv_owned_segment()));
        let waker = noop_waker();

        // First poll: hold empty, connection open → parks as a recv waiter.
        let p1 = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(
            matches!(p1, std::task::Poll::Pending),
            "parks on empty hold"
        );
        assert!(
            el.executor.recv_waiters[conn_index as usize],
            "parked recv_owned_segment registers a recv waiter"
        );

        // Simulate a recv arrival: the hold gets a buffer and wake_recv fires.
        deliver_segment(&mut el, conn_index, 0, b"again");
        assert!(
            !el.executor.recv_waiters[conn_index as usize],
            "wake_recv cleared the recv waiter on delivery"
        );

        // Second poll resumes with owned bytes.
        let owned = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(b))) => b,
            _ => panic!("expected owned bytes after the simulated recv"),
        };
        assert_eq!(&owned[..], b"again");
    }

    #[test]
    fn recv_owned_segment_returns_none_at_eof() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        // Close the connection (recv side) with nothing held.
        el.driver.close_connection(conn_index);

        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.recv_owned_segment()));
        let waker = noop_waker();
        let done = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            matches!(fut.as_mut().poll(&mut cx), std::task::Poll::Ready(Ok(None)))
        });
        assert!(
            done,
            "closed connection with an empty hold yields EOF (Ok(None))"
        );
    }

    /// Regression: a parked `recv_owned_segment` reader must resolve to
    /// `Ok(None)` when a peer-FIN (`result == 0`) multishot completion arrives
    /// while it is parked — matching `with_data`/`with_bytes` EOF behavior — and
    /// the provided-buffer accounting must stay balanced (no bid leak). Guards
    /// against a segmented reader hanging forever on a mid-stream peer close.
    #[test]
    fn parked_recv_owned_segment_resolves_none_on_fin_completion() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;
        // Model an armed multishot recv (accept_connection injects CQEs directly
        // and does not arm one).
        if let Some(cs) = el.driver.connections.get_mut(conn_index) {
            cs.recv_multishot_armed = true;
        }

        // Park the reader: empty hold, connection open → Pending + recv waiter.
        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.recv_owned_segment()));
        let waker = noop_waker();
        let p1 = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(
            matches!(p1, std::task::Poll::Pending),
            "parks on empty hold"
        );
        assert!(
            el.executor.recv_waiters[conn_index as usize],
            "parked reader registers a recv waiter"
        );

        // Deliver a peer FIN: multishot recv completion with result == 0 and no
        // F_MORE. This must wake the parked reader and close the recv side.
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), 0, 0);
        assert!(
            !el.executor.recv_waiters[conn_index as usize],
            "FIN wakes the parked segmented reader"
        );

        // Second poll now observes the closed recv side and resolves to EOF.
        let done = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            matches!(fut.as_mut().poll(&mut cx), std::task::Poll::Ready(Ok(None)))
        });
        assert!(done, "FIN while parked yields Ok(None), not a hang");

        // No held buffers were leaked: the ring's free count is fully restored.
        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "provided-buffer accounting balanced after FIN + close"
        );

        // The self-terminated multishot must be marked disarmed (a FIN
        // completion carries no F_MORE), so the close path issues no needless
        // recv-cancel.
        let armed = el
            .driver
            .connections
            .get(conn_index)
            .map(|c| c.recv_multishot_armed);
        assert!(
            armed != Some(true),
            "a FIN completion clears the armed flag"
        );
    }

    /// A *proactive* close (peer has NOT sent a FIN, so the multishot recv is
    /// still armed) must clear the armed flag as part of finalizing the close —
    /// the code path that cancels the still-armed recv so the kernel drops its
    /// socket reference and actually FINs the peer.
    #[test]
    fn proactive_close_clears_armed_recv_flag() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        if let Some(cs) = el.driver.connections.get_mut(conn_index) {
            cs.recv_multishot_armed = true;
        }

        // No queued sends (accept_connection leaves the send queue empty), so
        // close_connection finalizes immediately (submits the recv-cancel +
        // Close SQEs).
        el.driver.close_connection(conn_index);

        let armed = el
            .driver
            .connections
            .get(conn_index)
            .map(|c| c.recv_multishot_armed);
        assert!(
            armed != Some(true),
            "finalizing a proactive close disarms (cancels) the still-armed recv"
        );
    }

    // ── Segmented recv B1 callback (with_segments / SegChain) ────────────

    /// (a) Two held buffers are presented to the callback IN ORDER; a callback
    /// that consumes everything replenishes both bids, leaves the accumulator
    /// empty, and restores the ring's free count.
    #[test]
    fn with_segments_presents_two_held_buffers_in_order_full_drain() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        // Two arrivals held in order.
        deliver_segment(&mut el, conn_index, 0, b"AAA");
        deliver_segment(&mut el, conn_index, 1, b"BBB");
        assert_eq!(el.driver.provided_bufs.free(), entries - 2);

        let seen: Rc<RefCell<Vec<Vec<u8>>>> = Rc::new(RefCell::new(Vec::new()));
        let seen_cb = Rc::clone(&seen);
        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.with_segments(
            move |chain| {
                for s in chain.iter() {
                    seen_cb.borrow_mut().push(s.to_vec());
                }
                SegConsumed(chain.total_len()) // full drain
            }
        )));
        let waker = noop_waker();
        let n = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(n)) => n,
            other => panic!("expected Ready(Ok(n)), got {other:?}"),
        };

        assert_eq!(n, 6, "full drain consumes all presented bytes");
        assert_eq!(
            *seen.borrow(),
            vec![b"AAA".to_vec(), b"BBB".to_vec()],
            "segments presented in arrival order"
        );
        assert!(
            el.driver.accumulators.data(conn_index).is_empty(),
            "full drain leaves the accumulator empty"
        );
        assert!(el.driver.segment_hold[conn_index as usize].is_empty());
        assert!(el.driver.pending_replenish.contains(&0));
        assert!(el.driver.pending_replenish.contains(&1));
        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "both held bids replenished — free restored"
        );
    }

    /// (b) Under-drain: the callback consumes only part of the first buffer; the
    /// remainder of that buffer plus the un-reached buffer gather to the FRONT of
    /// the accumulator, in order, and both bids are replenished.
    #[test]
    fn with_segments_under_drain_gathers_remainder_to_accumulator_front() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        deliver_segment(&mut el, conn_index, 0, b"hello");
        deliver_segment(&mut el, conn_index, 1, b"world");
        assert_eq!(el.driver.provided_bufs.free(), entries - 2);

        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(
            with_driver_state(&mut el, || conn.with_segments(|_chain| SegConsumed(3)))
        ); // consume "hel" only
        let waker = noop_waker();
        let n = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(n)) => n,
            other => panic!("expected Ready(Ok(3)), got {other:?}"),
        };
        assert_eq!(n, 3);

        // Remainder "lo" + un-reached "world" now live contiguously at the front
        // of the accumulator, in order.
        assert_eq!(
            el.driver.accumulators.data(conn_index),
            b"loworld",
            "under-drain remainder gathers to the accumulator front, in order"
        );
        assert!(
            el.driver.segment_hold[conn_index as usize].is_empty(),
            "hold is drained after settle"
        );
        assert!(el.driver.pending_replenish.contains(&0));
        assert!(el.driver.pending_replenish.contains(&1));
        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "both bids replenished despite the gather — free restored"
        );
    }

    /// (c) Accumulator-first ordering: pre-seed the accumulator, then a held
    /// buffer arrives; the SegChain presents the accumulator bytes BEFORE the
    /// held buffer. A `SegConsumed(0)` (need-more) gathers everything and parks.
    #[test]
    fn with_segments_presents_accumulator_before_held_and_parks_on_need_more() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        // Pre-seed the accumulator, then hold a later-arriving buffer.
        assert!(el.driver.accumulators.append(conn_index, b"ACC"));
        deliver_segment(&mut el, conn_index, 0, b"HELD");

        let seen: Rc<RefCell<Vec<Vec<u8>>>> = Rc::new(RefCell::new(Vec::new()));
        let seen_cb = Rc::clone(&seen);
        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.with_segments(
            move |chain| {
                for s in chain.iter() {
                    seen_cb.borrow_mut().push(s.to_vec());
                }
                SegConsumed(0) // need a bigger frame
            }
        )));
        let waker = noop_waker();
        let p = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });

        assert!(
            matches!(p, std::task::Poll::Pending),
            "SegConsumed(0) parks for more data"
        );
        assert_eq!(
            *seen.borrow(),
            vec![b"ACC".to_vec(), b"HELD".to_vec()],
            "accumulator remainder presented BEFORE the held buffer"
        );
        assert!(
            el.executor.recv_waiters[conn_index as usize],
            "need-more registers a recv waiter"
        );
        // Everything gathered to the accumulator front, in order; hold empty.
        assert_eq!(el.driver.accumulators.data(conn_index), b"ACCHELD");
        assert!(el.driver.segment_hold[conn_index as usize].is_empty());
        assert!(el.driver.pending_replenish.contains(&0));
    }

    /// (d) Park on an empty hold, then resume when a buffer arrives; and EOF on a
    /// closed connection with nothing to present.
    #[test]
    fn with_segments_parks_then_resumes_and_reports_eof() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);

        // with_segments opts into Segmented itself.
        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn
            .with_segments(|chain| SegConsumed(chain.total_len()))));
        let waker = noop_waker();

        // First poll: nothing to present, connection open → parks.
        let p1 = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(matches!(p1, std::task::Poll::Pending), "parks on empty");
        assert_eq!(
            el.driver.recv_domain[conn_index as usize],
            crate::recv::domain::RecvDomain::Segmented,
            "with_segments sets the Segmented domain"
        );
        assert!(el.executor.recv_waiters[conn_index as usize]);

        // A buffer arrives; wake fires.
        deliver_segment(&mut el, conn_index, 0, b"data");
        assert!(!el.executor.recv_waiters[conn_index as usize]);

        // Second poll: resumes, consumes everything.
        let n = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(n)) => n,
            other => panic!("expected Ready(Ok(4)), got {other:?}"),
        };
        assert_eq!(n, 4);
        assert!(el.driver.accumulators.data(conn_index).is_empty());

        // EOF: a fresh closed connection with nothing held yields Ok(0).
        let c2 = accept_connection(&mut el);
        let g2 = el.driver.connections.generation(c2);
        el.driver.recv_domain[c2 as usize] = crate::recv::domain::RecvDomain::Segmented;
        el.driver.close_connection(c2);
        let conn2 = ConnCtx::new(c2, g2);
        let mut eof = std::pin::pin!(with_driver_state(&mut el, || conn2
            .with_segments(|chain| SegConsumed(chain.total_len()))));
        let done = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            matches!(eof.as_mut().poll(&mut cx), std::task::Poll::Ready(Ok(0)))
        });
        assert!(
            done,
            "closed connection with an empty hold yields Ok(0) EOF"
        );
    }

    // ── Segmented recv low-water reserve (force-copy under ring pressure) ──

    /// (a) At/below the reserve, a segmented delivery is force-copied into an
    /// OWNED segment (correct bytes) and its bid is replenished IMMEDIATELY —
    /// holding an owned segment does not pin the ring, so `free()` recovers at
    /// delivery.
    #[test]
    fn segmented_force_copy_at_reserve_delivers_owned_and_replenishes_immediately() {
        // reserve == ring size (16): free is always <= reserve → every delivery
        // force-copies (Mode C).
        let mut el = make_test_loop_with_config(config_with_reserve(16));
        let conn_index = accept_connection(&mut el);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;
        let entries = el.driver.provided_bufs.ring_entries();
        assert_eq!(el.driver.provided_bufs.free(), entries);

        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");

        // Held as an OWNED copy (not pinned), with the correct bytes.
        let hold = &el.driver.segment_hold[conn_index as usize];
        assert_eq!(hold.len(), 1, "buffer held in segment_hold");
        match &hold[0] {
            crate::backend::HeldRecvBuf::Owned(b) => {
                assert_eq!(&b[..], b"hello", "owned segment carries the received bytes")
            }
            crate::backend::HeldRecvBuf::Pinned { .. } => {
                panic!("at/below the reserve, delivery must be Owned (force-copy)")
            }
        }
        // The bid was replenished IMMEDIATELY (queued at delivery), before any
        // consumer runs — so the pinned-buffer count never grows under pressure.
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "force-copy replenishes the bid at delivery"
        );
        // Committing the queued replenish restores the full ring while the owned
        // segment is STILL held — proving the hold pins nothing.
        let to_replenish: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&to_replenish);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "holding an owned segment does not pin the ring — free recovered at delivery"
        );
        assert_eq!(
            el.driver.segment_hold[conn_index as usize].len(),
            1,
            "the owned segment is still held after its bid returned to the ring"
        );
    }

    /// (b) Above the reserve, delivery is still Pinned (zero-copy) as before — the
    /// bid is NOT replenished until consumed.
    #[test]
    fn segmented_zero_copy_when_ring_above_reserve_stays_pinned() {
        // reserve 4, ring 16: the first delivery leaves free = 15 > 4 → Pinned.
        let mut el = make_test_loop_with_config(config_with_reserve(4));
        let conn_index = accept_connection(&mut el);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;
        let entries = el.driver.provided_bufs.ring_entries();

        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");

        let hold = &el.driver.segment_hold[conn_index as usize];
        assert_eq!(hold.len(), 1);
        match &hold[0] {
            crate::backend::HeldRecvBuf::Pinned { bid: hbid, len } => {
                assert_eq!(*hbid, bid);
                assert_eq!(*len, 5);
            }
            crate::backend::HeldRecvBuf::Owned(_) => {
                panic!("above the reserve, delivery must stay Pinned (zero-copy)")
            }
        }
        assert!(
            !el.driver.pending_replenish.contains(&bid),
            "a pinned delivery does not replenish while held"
        );
        // Pinned: the buffer is still outstanding.
        assert_eq!(el.driver.provided_bufs.free(), entries - 1);
    }

    /// (c1) An owned held segment consumed via the reader returns the correct
    /// bytes, never enters the pin slot, and its drop replenishes nothing (no
    /// bid) — the ring stays balanced with no double-replenish.
    #[test]
    fn owned_segment_via_reader_returns_bytes_and_drop_does_not_replenish() {
        let mut el = make_test_loop_with_config(config_with_reserve(16));
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");
        // Commit the force-copy's delivery-time replenish so the ring is full.
        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(el.driver.provided_bufs.free(), entries);

        // Read the owned segment out via the lending-iterator reader.
        let conn = ConnCtx::new(conn_index, generation);
        let mut reader = with_driver_state(&mut el, || conn.segments());
        let waker = noop_waker();
        let mut fut = std::pin::pin!(reader.next());
        let seg = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(seg))) => seg,
            _ => panic!("expected an owned segment from the hold"),
        };
        with_driver_state(&mut el, || {
            assert_eq!(&seg[..], b"hello", "owned segment derefs to the bytes");
            assert_eq!(seg.len(), 5);
        });
        // Owned segments never use the pin slot.
        assert!(
            el.driver.segment_pinned[conn_index as usize].is_none(),
            "owned segment must not occupy the pin slot"
        );
        // Drop the owned segment: no bid, so nothing is replenished.
        with_driver_state(&mut el, || drop(seg));
        assert!(
            el.driver.pending_replenish.is_empty(),
            "dropping an owned segment replenishes nothing"
        );
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "ring stays balanced (no double-replenish)"
        );
    }

    /// (c2) An owned held segment consumed via `recv_owned_segment` returns the
    /// correct bytes and does not replenish a second time (the bid was already
    /// returned at delivery).
    #[test]
    fn recv_owned_segment_over_owned_hold_returns_bytes_no_double_replenish() {
        let mut el = make_test_loop_with_config(config_with_reserve(16));
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"world");
        // Force-copy queued the bid exactly once at delivery; do NOT commit yet so
        // we can prove the consume path does not queue it again.
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1
        );

        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.recv_owned_segment()));
        let waker = noop_waker();
        let owned = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(Some(b))) => b,
            other => panic!("expected owned bytes, got {other:?}"),
        };
        assert_eq!(&owned[..], b"world");
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "consuming an owned hold entry must not replenish its bid again"
        );
        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(el.driver.provided_bufs.free(), entries, "balanced");
    }

    /// (c3) `with_segments` over two OWNED held buffers presents them in order,
    /// full-drains, and replenishes nothing at settle (owned entries hold no bid)
    /// — the ring stays balanced.
    #[test]
    fn with_segments_owned_entries_full_drain_replenishes_nothing() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let mut el = make_test_loop_with_config(config_with_reserve(16));
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        deliver_segment(&mut el, conn_index, 0, b"AAA");
        deliver_segment(&mut el, conn_index, 1, b"BBB");
        // Both force-copied: each queued its bid at delivery. Commit them so the
        // ring is full before consuming.
        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        assert_eq!(r.len(), 2, "both force-copied bids queued at delivery");
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(el.driver.provided_bufs.free(), entries);

        let seen: Rc<RefCell<Vec<Vec<u8>>>> = Rc::new(RefCell::new(Vec::new()));
        let seen_cb = Rc::clone(&seen);
        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.with_segments(
            move |chain| {
                for s in chain.iter() {
                    seen_cb.borrow_mut().push(s.to_vec());
                }
                SegConsumed(chain.total_len())
            }
        )));
        let waker = noop_waker();
        let n = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(n)) => n,
            other => panic!("expected Ready(Ok(6)), got {other:?}"),
        };
        assert_eq!(n, 6);
        assert_eq!(
            *seen.borrow(),
            vec![b"AAA".to_vec(), b"BBB".to_vec()],
            "owned segments presented in arrival order"
        );
        assert!(el.driver.accumulators.data(conn_index).is_empty());
        assert!(el.driver.segment_hold[conn_index as usize].is_empty());
        assert!(
            el.driver.pending_replenish.is_empty(),
            "owned settle replenishes nothing (bids already returned at delivery)"
        );
        assert_eq!(el.driver.provided_bufs.free(), entries, "ring balanced");
    }

    /// (c4) `with_segments` under-drain over an OWNED held buffer copies the
    /// remainder from the owned bytes into the accumulator front, in order, and
    /// replenishes nothing.
    #[test]
    fn with_segments_owned_under_drain_gathers_remainder_from_owned_bytes() {
        let mut el = make_test_loop_with_config(config_with_reserve(16));
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        deliver_segment(&mut el, conn_index, 0, b"hello");
        deliver_segment(&mut el, conn_index, 1, b"world");
        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(el.driver.provided_bufs.free(), entries);

        let conn = ConnCtx::new(conn_index, generation);
        let mut fut = std::pin::pin!(
            with_driver_state(&mut el, || conn.with_segments(|_chain| SegConsumed(3)))
        ); // consume "hel" only
        let waker = noop_waker();
        let n = match with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        }) {
            std::task::Poll::Ready(Ok(n)) => n,
            other => panic!("expected Ready(Ok(3)), got {other:?}"),
        };
        assert_eq!(n, 3);
        assert_eq!(
            el.driver.accumulators.data(conn_index),
            b"loworld",
            "under-drain remainder gathered from the owned bytes, in order"
        );
        assert!(el.driver.segment_hold[conn_index as usize].is_empty());
        assert!(
            el.driver.pending_replenish.is_empty(),
            "owned under-drain settle replenishes nothing"
        );
        assert_eq!(el.driver.provided_bufs.free(), entries, "balanced");
    }

    /// (d) Tearing down a connection with an OWNED segment still held must not
    /// double-replenish: the owned entry carries no bid (already returned at
    /// delivery), so draining it at `handle_close` queues no replenish.
    #[test]
    fn close_with_owned_segment_held_does_not_double_replenish() {
        let mut el = make_test_loop_with_config(config_with_reserve(16));
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");
        // The bid was queued exactly once at delivery (force-copy).
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1
        );
        assert!(matches!(
            el.driver.segment_hold[conn_index as usize][0],
            crate::backend::HeldRecvBuf::Owned(_)
        ));

        // Close leaves the hold for a possible reader; teardown drains it. The
        // owned entry carries no bid, so neither step re-queues a replenish.
        el.driver.close_connection(conn_index);
        assert_eq!(
            el.driver.segment_hold[conn_index as usize].len(),
            1,
            "close must not drain the hold"
        );
        let close_ud = UserData::encode(OpTag::Close, conn_index, generation);
        el.test_dispatch_cqe(close_ud.raw(), 0, 0);
        assert!(
            el.driver.segment_hold[conn_index as usize].is_empty(),
            "handle_close drains the owned hold"
        );
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "teardown must NOT re-queue an owned entry's bid (no bid to return)"
        );
        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "exactly one replenish — no leak, no double"
        );
    }

    // ── Segmented recv Mode A (forward_to / ForwardWrite) ────────────────

    fn make_socketpair() -> (std::os::fd::OwnedFd, std::os::fd::OwnedFd) {
        use std::os::fd::FromRawFd;
        let mut fds = [0 as libc::c_int; 2];
        let r = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(r, 0, "socketpair failed");
        unsafe {
            (
                std::os::fd::OwnedFd::from_raw_fd(fds[0]),
                std::os::fd::OwnedFd::from_raw_fd(fds[1]),
            )
        }
    }

    fn temp_file() -> (std::fs::File, std::path::PathBuf) {
        let mut path = std::env::temp_dir();
        let uniq = format!(
            "ringline-forward-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        path.push(uniq);
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("temp file");
        (f, path)
    }

    /// (a) Forward a held buffer to a socket sink: the first poll pops the
    /// buffer, submits a write, and parks with the bid held; the write CQE
    /// releases the bid exactly once and the future resolves with the bytes
    /// forwarded, resetting the delivery domain and restoring `free()`.
    #[test]
    fn forward_to_socket_releases_bid_on_write_cqe_and_resolves() {
        use std::os::fd::AsFd;
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");
        assert_eq!(el.driver.provided_bufs.free(), entries - 1);

        let (sink, _peer) = make_socketpair();
        let sinkfd = crate::runtime::io::SinkFd::socket(sink.as_fd());
        let conn = ConnCtx::new(conn_index, generation);
        let waker = noop_waker();

        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.forward_to(&sinkfd, 5)));

        // First poll: pops the held buffer, submits a write, parks.
        let p1 = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(
            matches!(p1, std::task::Poll::Pending),
            "parks on the write CQE"
        );
        assert!(
            el.driver.forward_write[conn_index as usize].is_some(),
            "one write recorded in flight"
        );
        assert!(
            !el.driver.pending_replenish.contains(&bid),
            "bid stays held while the write is in flight"
        );
        assert_eq!(el.driver.provided_bufs.free(), entries - 1);

        // Simulate the write CQE (all 5 bytes).
        let ud = UserData::encode(OpTag::ForwardWrite, conn_index, generation);
        el.test_dispatch_cqe(ud.raw(), 5, 0);
        assert!(
            el.driver.forward_write[conn_index as usize].is_none(),
            "in-flight state cleared on completion"
        );
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "the write CQE replenishes the held bid exactly once"
        );
        assert_eq!(el.driver.forward_done[conn_index as usize], Some(Ok(5)));

        // Re-poll: forwarded == len → resolves, domain reset.
        let p2 = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(
            matches!(p2, std::task::Poll::Ready(Ok(5))),
            "resolves with bytes forwarded"
        );
        assert_eq!(
            el.driver.recv_domain[conn_index as usize],
            crate::recv::domain::RecvDomain::default(),
            "delivery domain reset after the forward"
        );
        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(el.driver.provided_bufs.free(), entries, "free restored");
    }

    /// (b) File sink: a short write resubmits the remainder at the advanced
    /// offset (`written` advances, the bid stays held), and the running file
    /// offset advances across buffers (`base_offset == bytes forwarded so far`).
    #[test]
    fn forward_to_file_short_write_resubmits_and_offset_advances() {
        use std::os::fd::AsFd;
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        // Two 5-byte buffers; forward all 10 bytes.
        deliver_segment(&mut el, conn_index, 0, b"hello");
        deliver_segment(&mut el, conn_index, 1, b"world");
        assert_eq!(el.driver.provided_bufs.free(), entries - 2);

        let (file, path) = temp_file();
        let sinkfd = crate::runtime::io::SinkFd::file(file.as_fd()).expect("buffered file ok");
        let conn = ConnCtx::new(conn_index, generation);
        let waker = noop_waker();
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.forward_to(&sinkfd, 10)));

        // Poll: submit write of buffer 0 at offset 0.
        let _ = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        {
            let st = el.driver.forward_write[conn_index as usize]
                .as_ref()
                .unwrap();
            assert!(st.is_file);
            assert_eq!(st.base_offset, 0, "first buffer writes at offset 0");
            assert_eq!(st.total, 5);
        }

        // Short write: only 3 of 5 bytes → resubmit remainder, bid still held.
        let ud = UserData::encode(OpTag::ForwardWrite, conn_index, generation);
        el.test_dispatch_cqe(ud.raw(), 3, 0);
        {
            let st = el.driver.forward_write[conn_index as usize]
                .as_ref()
                .expect("still in flight after a short write");
            assert_eq!(st.written, 3, "short write advanced `written`");
            assert_eq!(st.total, 5);
        }
        assert!(
            !el.driver.pending_replenish.contains(&0),
            "bid 0 stays held across the short-write resubmit"
        );

        // Remainder completes (2 bytes) → buffer 0 done, bid 0 replenished once.
        el.test_dispatch_cqe(ud.raw(), 2, 0);
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == 0)
                .count(),
            1,
            "buffer 0 bid replenished exactly once on full completion"
        );

        // Re-poll: pops buffer 1, submits at the advanced file offset 5.
        let _ = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        {
            let st = el.driver.forward_write[conn_index as usize]
                .as_ref()
                .unwrap();
            assert_eq!(
                st.base_offset, 5,
                "second buffer writes at the advanced offset"
            );
            assert_eq!(st.total, 5);
        }
        // Complete buffer 1.
        el.test_dispatch_cqe(ud.raw(), 5, 0);
        let p = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(
            matches!(p, std::task::Poll::Ready(Ok(10))),
            "forwarded all 10 bytes"
        );
        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "both bids restored"
        );
        let _ = std::fs::remove_file(path);
    }

    /// (c) Under ring pressure the forward path force-copies at delivery (Mode C
    /// backing): the bid returns to the ring immediately, so a slow sink cannot
    /// deplete the shared ring, and the owned backing's write completion does not
    /// double-replenish.
    #[test]
    fn forward_to_owned_backing_does_not_double_replenish() {
        use std::os::fd::AsFd;
        let mut el = make_test_loop_with_config(config_with_reserve(16));
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        // Below the reserve → force-copy: the bid is replenished at delivery.
        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");
        assert!(matches!(
            el.driver.segment_hold[conn_index as usize][0],
            crate::backend::HeldRecvBuf::Owned(_)
        ));
        // Commit the delivery-time replenish so `free()` reflects the ring is not
        // depleted by the (owned) held buffer.
        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "force-copy returned the bid — the forwarding conn did not deplete the ring"
        );

        let (sink, _peer) = make_socketpair();
        let sinkfd = crate::runtime::io::SinkFd::socket(sink.as_fd());
        let conn = ConnCtx::new(conn_index, generation);
        let waker = noop_waker();
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.forward_to(&sinkfd, 5)));

        let _ = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        // Complete the write of the owned backing.
        let ud = UserData::encode(OpTag::ForwardWrite, conn_index, generation);
        el.test_dispatch_cqe(ud.raw(), 5, 0);
        assert!(
            el.driver.pending_replenish.is_empty(),
            "owned backing carries no bid — the write CQE replenishes nothing"
        );
        let p = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(matches!(p, std::task::Poll::Ready(Ok(5))));
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "no double replenish"
        );
    }

    /// (d) Close mid-forward must NOT return the in-flight write's source bid to
    /// the ring while the kernel is still reading it (invariant #1). Regression
    /// for the CRITICAL Mode A UAF: `close_connection` cancels the write and holds
    /// the backing until the (ECANCELED) CQE lands, which releases the bid exactly
    /// once and drives the deferred close. Returning the bid at close time would
    /// let another connection's recv overwrite a buffer the kernel is still
    /// DMA-reading.
    #[test]
    fn close_mid_forward_defers_bid_release_until_write_cqe() {
        use std::os::fd::AsFd;
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;

        let bid: u16 = 0;
        deliver_segment(&mut el, conn_index, bid, b"hello");
        let (sink, _peer) = make_socketpair();
        let sinkfd = crate::runtime::io::SinkFd::socket(sink.as_fd());
        let conn = ConnCtx::new(conn_index, generation);
        let waker = noop_waker();
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.forward_to(&sinkfd, 5)));
        let _ = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert!(el.driver.forward_write[conn_index as usize].is_some());
        assert!(!el.driver.pending_replenish.contains(&bid));

        // Close while the write is in flight: the backing is STILL held (the
        // kernel is reading it) and the bid is NOT yet back in the ring.
        el.driver.close_connection(conn_index);
        assert!(
            el.driver.forward_write[conn_index as usize].is_some(),
            "close must not drain the in-flight forward write early — the kernel \
             still owns its source buffer"
        );
        assert!(
            !el.driver.pending_replenish.contains(&bid),
            "the in-flight source bid must not return to the ring before the write CQE"
        );

        // The cancelled write's CQE (ECANCELED) lands: now the kernel is done, so
        // the bid is replenished exactly once and the deferred close proceeds.
        let ud = UserData::encode(OpTag::ForwardWrite, conn_index, generation);
        el.test_dispatch_cqe(ud.raw(), -libc::ECANCELED, 0);
        assert!(
            el.driver.forward_write[conn_index as usize].is_none(),
            "the write CQE releases the backing"
        );
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "the write CQE replenishes the in-flight bid exactly once"
        );

        // A further stale write CQE for the (now released) occupant must no-op.
        el.test_dispatch_cqe(ud.raw(), 5, 0);
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == bid)
                .count(),
            1,
            "a stale forward-write CQE does not double-replenish"
        );
        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "no leak, no double"
        );
    }

    /// Mode A hold cap: mark a connection as a forwarder, arm its recv, and
    /// deliver segments. The throttle engages *exactly* at the cap (not before):
    /// below the cap the recv stays un-throttled, and reaching the cap sets the
    /// throttle flag (cancelling the multishot — `recv_multishot_armed` stays set
    /// until the ECANCELED CQE clears it, gating re-arm).
    #[test]
    fn forward_hold_cap_throttles_recv_at_cap() {
        let cap = 4;
        let mut el = make_test_loop_with_config(config_with_forward_cap(cap));
        let conn_index = accept_connection(&mut el);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;
        el.driver.forward_recv_active[conn_index as usize] = true;
        el.driver
            .connections
            .get_mut(conn_index)
            .unwrap()
            .recv_multishot_armed = true;

        // Below the cap: no throttle, still armed.
        for bid in 0..(cap - 1) as u16 {
            deliver_segment(&mut el, conn_index, bid, b"x");
        }
        assert!(
            !el.driver.forward_hold_throttled[conn_index as usize],
            "not throttled below the cap"
        );
        assert!(
            el.driver
                .connections
                .get(conn_index)
                .unwrap()
                .recv_multishot_armed,
            "recv stays armed below the cap"
        );

        // Reaching the cap engages the throttle (cancel submitted).
        deliver_segment(&mut el, conn_index, (cap - 1) as u16, b"x");
        assert!(
            el.driver.forward_hold_throttled[conn_index as usize],
            "throttled at the cap"
        );
        assert_eq!(
            el.driver.segment_hold[conn_index as usize].len(),
            cap,
            "held exactly cap buffers"
        );
        assert!(
            el.driver
                .connections
                .get(conn_index)
                .unwrap()
                .recv_multishot_armed,
            "armed flag stays set until the cancel's ECANCELED clears it (gates re-arm)"
        );
    }

    /// Mode A hold cap: after the throttle, once the ECANCELED lands and writes
    /// drain the hold below the cap, the write-completion handler re-arms the
    /// recv — no permanent throttle / deadlock.
    #[test]
    fn forward_hold_cap_rearms_after_hold_drains_on_write() {
        use std::os::fd::AsFd;
        let cap = 2;
        let mut el = make_test_loop_with_config(config_with_forward_cap(cap));
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;
        el.driver.forward_recv_active[conn_index as usize] = true;
        el.driver
            .connections
            .get_mut(conn_index)
            .unwrap()
            .recv_multishot_armed = true;

        // Fill to the cap → throttled (cancel submitted).
        deliver_segment(&mut el, conn_index, 0, b"hello");
        deliver_segment(&mut el, conn_index, 1, b"world");
        assert!(el.driver.forward_hold_throttled[conn_index as usize]);

        // ECANCELED lands: clears `recv_multishot_armed`; hold still full → stays
        // throttled (re-arm waits for the hold to drain).
        let recv_ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(recv_ud.raw(), -libc::ECANCELED, 0);
        assert!(
            !el.driver
                .connections
                .get(conn_index)
                .unwrap()
                .recv_multishot_armed,
            "ECANCELED clears the armed flag"
        );
        assert!(
            el.driver.forward_hold_throttled[conn_index as usize],
            "still throttled while the hold is at the cap"
        );

        // Drive the forward: poll pops buffer 0 (hold drops to 1 < cap) and
        // submits a write.
        let (sink, _peer) = make_socketpair();
        let sinkfd = crate::runtime::io::SinkFd::socket(sink.as_fd());
        let conn = ConnCtx::new(conn_index, generation);
        let waker = noop_waker();
        // Forward a large len so the future keeps going.
        let mut fut = std::pin::pin!(with_driver_state(&mut el, || conn.forward_to(&sinkfd, 100)));
        let _ = with_driver_state(&mut el, || {
            let mut cx = std::task::Context::from_waker(&waker);
            fut.as_mut().poll(&mut cx)
        });
        assert_eq!(
            el.driver.segment_hold[conn_index as usize].len(),
            1,
            "one buffer popped into the in-flight write"
        );
        assert!(
            el.driver.forward_hold_throttled[conn_index as usize],
            "not yet re-armed — waiting for the write to complete"
        );

        // Write completes → handle_forward_write drains + re-arms (hold 1 < cap 2).
        let fw_ud = UserData::encode(OpTag::ForwardWrite, conn_index, generation);
        el.test_dispatch_cqe(fw_ud.raw(), 5, 0);
        assert!(
            !el.driver.forward_hold_throttled[conn_index as usize],
            "re-armed after the hold drained below the cap"
        );
        assert!(
            el.driver
                .connections
                .get(conn_index)
                .unwrap()
                .recv_multishot_armed,
            "multishot re-armed"
        );
    }

    /// Mode A hold cap: if the hold drains below the cap *before* the throttle's
    /// ECANCELED completes (writes outran the cancel), the ECANCELED handler
    /// itself re-arms — the deadlock-avoidance path (no write completion is left
    /// to trigger it).
    #[test]
    fn forward_throttle_rearms_from_ecanceled_when_hold_already_drained() {
        let cap = 3;
        let mut el = make_test_loop_with_config(config_with_forward_cap(cap));
        let conn_index = accept_connection(&mut el);
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;
        el.driver.forward_recv_active[conn_index as usize] = true;
        el.driver
            .connections
            .get_mut(conn_index)
            .unwrap()
            .recv_multishot_armed = true;

        for bid in 0..cap as u16 {
            deliver_segment(&mut el, conn_index, bid, b"x");
        }
        assert!(el.driver.forward_hold_throttled[conn_index as usize]);

        // Simulate the forward future draining the hold below the cap while the
        // cancel is still in flight (pop two of three held buffers).
        el.driver.segment_hold[conn_index as usize].pop_front();
        el.driver.segment_hold[conn_index as usize].pop_front();
        assert!(el.driver.segment_hold[conn_index as usize].len() < cap);

        // ECANCELED now lands with the hold already below the cap → re-arm here.
        let recv_ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(recv_ud.raw(), -libc::ECANCELED, 0);
        assert!(
            !el.driver.forward_hold_throttled[conn_index as usize],
            "ECANCELED branch re-armed since the hold had drained"
        );
        assert!(
            el.driver
                .connections
                .get(conn_index)
                .unwrap()
                .recv_multishot_armed,
            "multishot re-armed from the ECANCELED path"
        );
    }

    /// Mode A hold cap: closing a connection while it is throttled drains its held
    /// bids exactly once (no leak, no double-replenish), and a later stale
    /// ECANCELED for the throttle-cancel is a no-op.
    #[test]
    fn close_while_throttled_releases_held_bids_once() {
        let cap = 2;
        let mut el = make_test_loop_with_config(config_with_forward_cap(cap));
        let conn_index = accept_connection(&mut el);
        let entries = el.driver.provided_bufs.ring_entries();
        el.driver.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::Segmented;
        el.driver.forward_recv_active[conn_index as usize] = true;
        el.driver
            .connections
            .get_mut(conn_index)
            .unwrap()
            .recv_multishot_armed = true;

        // Fill to the cap (reserve 0 → Pinned) → throttled, two bids held.
        deliver_segment(&mut el, conn_index, 0, b"hello");
        deliver_segment(&mut el, conn_index, 1, b"world");
        assert!(el.driver.forward_hold_throttled[conn_index as usize]);
        assert_eq!(el.driver.provided_bufs.free(), entries - 2);

        // Close while throttled clears the forwarder flags but does NOT drain the
        // held bids (a reader could still consume them post-FIN); teardown reclaims
        // any it never reaches.
        let generation = el.driver.connections.generation(conn_index);
        el.driver.close_connection(conn_index);
        assert!(
            !el.driver.forward_hold_throttled[conn_index as usize],
            "throttle flag cleared on close"
        );
        assert!(
            !el.driver.forward_recv_active[conn_index as usize],
            "forwarder flag cleared on close"
        );
        assert_eq!(
            el.driver.segment_hold[conn_index as usize].len(),
            2,
            "close does not drain the held bids"
        );

        // Teardown (the Close CQE → handle_close) reclaims the held bids.
        let close_ud = UserData::encode(OpTag::Close, conn_index, generation);
        el.test_dispatch_cqe(close_ud.raw(), 0, 0);
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == 0)
                .count(),
            1,
            "bid 0 replenished exactly once"
        );
        assert_eq!(
            el.driver
                .pending_replenish
                .iter()
                .filter(|&&b| b == 1)
                .count(),
            1,
            "bid 1 replenished exactly once"
        );

        // A stale ECANCELED for the throttle-cancel after close is a no-op.
        let recv_ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(recv_ud.raw(), -libc::ECANCELED, 0);

        let r: Vec<u16> = el.driver.pending_replenish.drain(..).collect();
        el.driver.provided_bufs.replenish_batch(&r);
        assert_eq!(
            el.driver.provided_bufs.free(),
            entries,
            "no leak, no double replenish"
        );
    }

    /// `SinkFd::file` rejects an `O_DIRECT` descriptor (unaligned provided
    /// buffers cannot be a zero-copy source).
    #[test]
    fn sink_fd_file_rejects_o_direct() {
        use std::os::fd::{AsFd, FromRawFd, OwnedFd};
        let (_f, path) = temp_file();
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        let raw = unsafe { libc::open(cpath.as_ptr(), libc::O_RDWR | libc::O_DIRECT) };
        if raw < 0 {
            // Some filesystems (e.g. tmpfs) reject O_DIRECT open — skip.
            let _ = std::fs::remove_file(&path);
            return;
        }
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        let err = crate::runtime::io::SinkFd::file(owned.as_fd()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn handle_recv_multi_second_completion_flushes_to_accumulator() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let flags = 1u32 | 2u32; // IORING_CQE_F_BUFFER | IORING_CQE_F_MORE, bid=0

        // First recv: bid=0, "hello"
        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(0);
        unsafe {
            std::ptr::copy_nonoverlapping(b"hello".as_ptr(), buf_ptr as *mut u8, 5);
        }
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), 5, flags);

        // Second recv: bid=1, " world"
        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(1);
        unsafe {
            std::ptr::copy_nonoverlapping(b" world".as_ptr(), buf_ptr as *mut u8, 6);
        }
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), 6, flags | (1u32 << 16));

        // Both buffers should be replenished (first flushed, second appended directly).
        assert!(
            el.driver.pending_replenish.contains(&0),
            "first buffer should be replenished"
        );
        assert!(
            el.driver.pending_replenish.contains(&1),
            "second buffer should be replenished"
        );

        // No pending buffer (second completion went through accumulator path).
        assert!(el.driver.pending_recv_bufs[conn_index as usize].is_none());

        // Accumulator should contain both buffers' data concatenated.
        let data = el.driver.accumulators.data(conn_index);
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn handle_recv_multi_enobufs_does_not_close() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // ENOBUFS = -105. has_more = false (bit 1 not set).
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), -105, 0);

        // Connection should still be alive (ENOBUFS is recoverable).
        assert!(
            el.driver.connections.get(conn_index).is_some(),
            "connection closed on ENOBUFS"
        );
    }

    #[test]
    fn handle_recv_multi_unknown_error_closes_when_no_more() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Unknown error, !has_more — should close.
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), -99, 0); // -99 = unknown errno

        let conn = el.driver.connections.get(conn_index);
        assert!(
            conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed),
            "connection not closed on unknown recv error"
        );
    }

    #[test]
    fn handle_recv_multi_ecanceled_does_nothing() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // ECANCELED = -125.
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), -125, 0);

        // Connection should still be alive.
        assert!(
            el.driver.connections.get(conn_index).is_some(),
            "connection closed on ECANCELED"
        );
    }

    // ── Fallback recv tests ────────────────────────────────────────

    /// Park a connection via an ENOBUFS multishot CQE and return its index.
    fn park_connection(el: &mut AsyncEventLoop<NoopHandler>) -> u32 {
        let conn_index = accept_connection(el);
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), -libc::ENOBUFS, 0);
        assert!(el.driver.recv_starved.contains(&conn_index));
        conn_index
    }

    #[test]
    fn dry_flush_submits_fallback_for_partial_message() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);
        assert!(el.driver.accumulators.append(conn_index, b"$16\r\npart"));

        // No pending replenish: the ring is dry.
        assert!(el.driver.pending_replenish.is_empty());
        el.flush_replenish_and_rearm();

        assert!(
            el.driver.recv_fallback_inflight[conn_index as usize],
            "fallback not submitted"
        );
        assert!(
            !el.driver.recv_starved.contains(&conn_index),
            "connection still parked after fallback submit"
        );
        assert_eq!(el.driver.recv_fallback_count, 1);
        let pool = el.driver.fallback_recv_pool.as_ref().expect("pool built");
        assert!(pool.in_use(0), "first fallback should occupy slot 0");
    }

    #[test]
    fn dry_flush_keeps_waiting_with_empty_accumulator() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);

        el.flush_replenish_and_rearm();

        assert!(
            !el.driver.recv_fallback_inflight[conn_index as usize],
            "fallback submitted with nothing half-delivered"
        );
        assert!(
            el.driver.recv_starved.contains(&conn_index),
            "connection should stay parked"
        );
    }

    #[test]
    fn partial_message_prefers_fallback_over_rearm() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);
        assert!(el.driver.accumulators.append(conn_index, b"part"));

        // Buffers came back — but re-arming a connection with a partial
        // message would only move one ring's worth before parking again
        // (the churn cycle). The fallback must win the arbitration.
        el.driver.provided_bufs.on_handout(); // a replenished bid was handed out first
        el.driver.pending_replenish.push(0);
        el.flush_replenish_and_rearm();

        assert!(
            el.driver.recv_fallback_inflight[conn_index as usize],
            "partial-message connection must take the fallback path"
        );
        assert!(el.driver.recv_starved.is_empty());
    }

    #[test]
    fn empty_accumulator_rearms_multishot_on_replenish() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);

        el.driver.provided_bufs.on_handout(); // a replenished bid was handed out first
        el.driver.pending_replenish.push(0);
        el.flush_replenish_and_rearm();

        assert!(
            !el.driver.recv_fallback_inflight[conn_index as usize],
            "nothing half-delivered — multishot re-arm expected"
        );
        assert!(el.driver.recv_starved.is_empty());
    }

    #[test]
    fn replenish_does_not_rearm_while_fallback_inflight() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);
        assert!(el.driver.accumulators.append(conn_index, b"part"));
        el.flush_replenish_and_rearm();
        assert!(el.driver.recv_fallback_inflight[conn_index as usize]);
        // Fallback submission takes the connection out of the park queue;
        // its completion re-parks it.
        assert!(!el.driver.recv_starved.contains(&conn_index));

        // A stale multishot ENOBUFS CQE re-parks the connection while the
        // fallback is still in flight. When buffers come back, the
        // replenish pass must NOT arm a multishot alongside the
        // outstanding one-shot — the connection stays parked until the
        // fallback CQE hands off.
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), -libc::ENOBUFS, 0);
        assert!(el.driver.recv_starved.contains(&conn_index));

        el.driver.provided_bufs.on_handout(); // a replenished bid was handed out first
        el.driver.pending_replenish.push(0);
        el.flush_replenish_and_rearm();

        assert!(
            el.driver.recv_starved.contains(&conn_index),
            "fallback-inflight connection must stay parked until its CQE"
        );
        assert!(el.driver.recv_fallback_inflight[conn_index as usize]);
    }

    #[test]
    fn dry_flush_moves_held_buffer_to_accumulator_and_revives() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);

        // Simulate a zero-copy held buffer with unconsumed partial data.
        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(3);
        unsafe { std::ptr::copy_nonoverlapping(b"held".as_ptr(), buf_ptr as *mut u8, 4) };
        el.driver.provided_bufs.on_handout(); // bid 3 was handed out before being held
        el.driver.pending_recv_bufs[conn_index as usize] = Some(crate::backend::PendingRecvBuf {
            bid: 3,
            len: 4,
            ptr: buf_ptr,
        });

        el.flush_replenish_and_rearm();

        assert_eq!(el.driver.accumulators.data(conn_index), b"held");
        assert!(
            el.driver.pending_recv_bufs[conn_index as usize].is_none(),
            "hold not flushed"
        );
        // The flushed hold is a partial message — the fallback continues
        // draining it (its bid went back to the ring for other conns).
        assert!(el.driver.recv_fallback_inflight[conn_index as usize]);
        assert!(el.driver.recv_starved.is_empty());
    }

    #[test]
    fn fallback_completion_appends_wakes_and_reparks() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);
        assert!(el.driver.accumulators.append(conn_index, b"part-"));
        el.flush_replenish_and_rearm();
        assert!(el.driver.recv_fallback_inflight[conn_index as usize]);

        // Write payload into the pool slot the way the kernel would.
        // (`alloc_raw` leaves `remaining` at 0 — only the base pointer is
        // meaningful here, matching what the recv SQE was built from.)
        let pool = el.driver.fallback_recv_pool.as_mut().expect("pool built");
        let slot: u16 = 0;
        assert!(pool.in_use(slot));
        let (ptr, _) = pool.current_ptr_remaining(slot);
        unsafe { std::ptr::copy_nonoverlapping(b"chunk".as_ptr(), ptr as *mut u8, 5) };

        let ud = UserData::encode(OpTag::RecvFallback, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), 5, 0);

        assert_eq!(el.driver.accumulators.data(conn_index), b"part-chunk");
        assert!(
            !el.driver.recv_fallback_inflight[conn_index as usize],
            "inflight flag not cleared"
        );
        assert!(
            !el.driver.fallback_recv_pool.as_ref().unwrap().in_use(slot),
            "pool slot not released"
        );
        assert!(
            el.driver.recv_starved.contains(&conn_index),
            "connection not re-parked after fallback completion"
        );
        assert!(el.driver.connections.get(conn_index).is_some());
    }

    #[test]
    fn fallback_completion_eof_closes_connection() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);
        assert!(el.driver.accumulators.append(conn_index, b"part"));
        el.flush_replenish_and_rearm();

        let ud = UserData::encode(OpTag::RecvFallback, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), 0, 0);

        assert!(
            !el.driver.fallback_recv_pool.as_ref().unwrap().in_use(0),
            "pool slot not released on EOF"
        );
        let closed = el
            .driver
            .connections
            .get(conn_index)
            .is_none_or(|c| matches!(c.recv_mode, RecvMode::Closed));
        assert!(closed, "FIN mid-message must close the connection");
    }

    #[test]
    fn fallback_completion_error_closes_connection() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);
        assert!(el.driver.accumulators.append(conn_index, b"part"));
        el.flush_replenish_and_rearm();

        let ud = UserData::encode(OpTag::RecvFallback, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), -libc::ECONNRESET, 0);

        assert!(!el.driver.fallback_recv_pool.as_ref().unwrap().in_use(0));
        let closed = el
            .driver
            .connections
            .get(conn_index)
            .is_none_or(|c| matches!(c.recv_mode, RecvMode::Closed));
        assert!(closed);
    }

    #[test]
    fn fallback_completion_ecanceled_reparks_alive_connection() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);
        assert!(el.driver.accumulators.append(conn_index, b"part"));
        el.flush_replenish_and_rearm();

        let ud = UserData::encode(OpTag::RecvFallback, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), -libc::ECANCELED, 0);

        assert!(!el.driver.fallback_recv_pool.as_ref().unwrap().in_use(0));
        assert!(!el.driver.recv_fallback_inflight[conn_index as usize]);
        assert!(
            el.driver.recv_starved.contains(&conn_index),
            "cancelled fallback must re-park, not strand the connection"
        );
    }

    #[test]
    fn stale_fallback_completion_releases_slot_only() {
        let mut el = make_test_loop();
        let conn_index = park_connection(&mut el);
        assert!(el.driver.accumulators.append(conn_index, b"part"));
        el.flush_replenish_and_rearm();
        assert!(el.driver.recv_fallback_inflight[conn_index as usize]);

        // Close and release the slot; the generation bumps on release.
        el.driver.close_connection(conn_index);
        let close_ud = UserData::encode(OpTag::Close, conn_index, 0);
        el.test_dispatch_cqe(close_ud.raw(), 0, 0);
        assert!(el.driver.connections.get(conn_index).is_none());

        // Reuse the slot for a new occupant.
        let new_index = accept_connection(&mut el);
        assert_eq!(new_index, conn_index, "test expects slot reuse");
        assert!(
            !el.driver.recv_fallback_inflight[new_index as usize],
            "close must clear the inflight flag for the next occupant"
        );

        // The stale fallback CQE for the old occupant arrives now.
        let ud = UserData::encode(OpTag::RecvFallback, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), 5, 0);

        assert!(
            !el.driver.fallback_recv_pool.as_ref().unwrap().in_use(0),
            "stale CQE must release the pool slot"
        );
        assert!(
            el.driver.accumulators.data(new_index).is_empty(),
            "stale CQE must not append into the new occupant's accumulator"
        );
        assert!(el.driver.connections.get(new_index).is_some());
    }

    // ── Connect tests ──────────────────────────────────────────────

    #[test]
    fn handle_connect_success_wakes_waiter() {
        let mut el = make_test_loop();

        // Allocate an outbound connection slot.
        let conn_index = el
            .driver
            .connections
            .allocate_outbound()
            .expect("no free slots");
        el.executor.connect_waiters[conn_index as usize] = true;

        // Simulate successful connect CQE (result == 0).
        let ud = UserData::encode(OpTag::Connect, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), 0, 0);

        // Connect waiter should be cleared and result stored.
        assert!(
            !el.executor.connect_waiters[conn_index as usize],
            "connect waiter not cleared"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "connect result not stored"
        );
        // Connection should be established with recv_mode = Multi.
        let conn = el.driver.connections.get(conn_index).unwrap();
        assert!(conn.established, "connection not marked established");
        assert!(
            matches!(conn.recv_mode, RecvMode::Multi),
            "recv_mode not set to Multi after connect"
        );
    }

    #[test]
    fn handle_connect_error_wakes_waiter_and_closes() {
        let mut el = make_test_loop();

        let conn_index = el
            .driver
            .connections
            .allocate_outbound()
            .expect("no free slots");
        el.executor.connect_waiters[conn_index as usize] = true;

        // Simulate ECONNREFUSED (errno 111).
        let ud = UserData::encode(OpTag::Connect, conn_index, 0);
        el.test_dispatch_cqe(ud.raw(), -111, 0);

        // Connect waiter should be cleared with error result.
        assert!(
            !el.executor.connect_waiters[conn_index as usize],
            "connect waiter not cleared on error"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "connect error result not stored"
        );
        // Connection should be closing.
        let conn = el.driver.connections.get(conn_index);
        assert!(
            conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed),
            "connection not closed after connect error"
        );
    }

    // ── Timer tests ────────────────────────────────────────────────

    #[test]
    fn handle_timer_fires_and_wakes_task() {
        let mut el = make_test_loop();

        // Allocate a timer slot.
        let waker_id = 0u32; // conn_index 0 as waker
        let (slot, generation) = el.executor.timer_pool.allocate(waker_id).unwrap();

        let payload = TimerSlotPool::encode_payload(slot, generation);
        let ud = UserData::encode(OpTag::Timer, 0, payload);

        // Simulate timer CQE (result == -ETIME = -62).
        el.test_dispatch_cqe(ud.raw(), -62, 0);

        // Timer should be marked as fired.
        assert!(
            el.executor.timer_pool.is_fired(slot),
            "timer not marked as fired"
        );
    }

    #[test]
    fn handle_timer_stale_generation_ignored() {
        let mut el = make_test_loop();

        let (slot, generation) = el.executor.timer_pool.allocate(0).unwrap();
        // Release and reallocate to bump generation.
        el.executor.timer_pool.release(slot);
        let (_slot2, gen2) = el.executor.timer_pool.allocate(0).unwrap();
        assert_ne!(generation, gen2, "generation should have changed");

        // Dispatch with OLD generation — should be ignored.
        let payload = TimerSlotPool::encode_payload(slot, generation);
        let ud = UserData::encode(OpTag::Timer, 0, payload);
        el.test_dispatch_cqe(ud.raw(), -62, 0);

        // Timer should NOT be fired (stale generation).
        assert!(
            !el.executor.timer_pool.is_fired(slot),
            "stale timer should not be fired"
        );
    }

    // ── TLS send tests ─────────────────────────────────────────────

    #[test]
    fn handle_tls_send_complete_releases_pool_slot() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let data = b"ciphertext";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        // Simulate full TLS send completion (all bytes sent, try_advance returns None).
        let ud = UserData::encode(OpTag::TlsSend, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), data.len() as i32, 0);

        assert_eq!(
            el.driver.send_copy_pool.free_count(),
            free_before + 1,
            "pool slot not released after TLS send complete"
        );
    }

    #[test]
    fn handle_tls_send_error_closes_connection() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let data = b"ciphertext";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();

        // Simulate TLS send error (result < 0).
        let ud = UserData::encode(OpTag::TlsSend, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), -104, 0);

        // Pool slot should be released.
        assert!(
            !el.driver.send_copy_pool.in_use(slot),
            "pool slot not released after TLS send error"
        );
        // Connection should be closing.
        let conn = el.driver.connections.get(conn_index);
        assert!(
            conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed),
            "connection not closed after TLS send error"
        );
    }

    #[test]
    fn handle_tls_send_eagain_arms_pollout() {
        // EAGAIN is ordinary socket backpressure: keep the slot (the unsent
        // ciphertext) and arm POLLOUT — do NOT tear the connection down.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let data = b"ciphertext";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();

        let ud = UserData::encode(OpTag::TlsSend, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), -(libc::EAGAIN), 0);

        assert!(
            el.driver.send_copy_pool.in_use(slot),
            "slot must stay alive across EAGAIN (unsent bytes)"
        );
        let conn = el.driver.connections.get(conn_index);
        assert!(
            conn.is_some() && !matches!(conn.unwrap().recv_mode, RecvMode::Closed),
            "EAGAIN must not close the connection"
        );
    }

    #[test]
    fn handle_tls_send_stale_slot_ignored() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Allocate and immediately release a slot, then deliver a stale
        // TlsSend CQE for it (Close CQE processed earlier in the same batch).
        let data = b"ciphertext";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();
        el.driver.send_copy_pool.release(slot);
        let free_before = el.driver.send_copy_pool.free_count();

        let ud = UserData::encode(OpTag::TlsSend, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), -104, 0);

        assert_eq!(
            el.driver.send_copy_pool.free_count(),
            free_before,
            "stale CQE must not double-release the slot"
        );
        let conn = el.driver.connections.get(conn_index);
        assert!(
            conn.is_some() && !matches!(conn.unwrap().recv_mode, RecvMode::Closed),
            "stale TlsSend CQE must not close the connection"
        );
    }

    // ── Tick timeout test ──────────────────────────────────────────

    #[test]
    fn handle_tick_timeout_clears_armed_flag() {
        let mut el = make_test_loop();
        el.driver.tick_timeout_armed = true;

        let ud = UserData::encode(OpTag::TickTimeout, 0, 0);
        el.test_dispatch_cqe(ud.raw(), -62, 0);

        assert!(
            !el.driver.tick_timeout_armed,
            "tick_timeout_armed not cleared"
        );
    }

    // ── UDP send error metric test ─────────────────────────────────

    #[test]
    fn handle_send_msg_udp_error_releases_pool_slot() {
        let mut el = make_test_loop();

        // Set up a UDP socket state (need at least one for the handler).
        if el.driver.udp_sockets.is_empty() {
            return; // Skip if no UDP sockets configured.
        }

        let data = b"datagram";
        let (pool_slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        // Pop the send slot we'll "simulate" — so the CQE pushes it back cleanly
        // (mirrors the real submit path).
        let slot_idx = el.driver.udp_sockets[0].send_freelist.pop().unwrap();

        // Simulate UDP send CQE (success).
        let udp_index = 0u32;
        let payload = crate::backend::uring::driver::encode_udp_send_payload(slot_idx, pool_slot);
        let ud = UserData::encode(OpTag::SendMsgUdp, udp_index, payload);
        el.test_dispatch_cqe(ud.raw(), data.len() as i32, 0);

        assert_eq!(
            el.driver.send_copy_pool.free_count(),
            free_before + 1,
            "pool slot not released after UDP send"
        );
        assert!(
            el.driver.udp_sockets[0].send_freelist.contains(&slot_idx),
            "send slot not returned to freelist after CQE"
        );
    }

    #[test]
    fn handle_send_msg_udp_wakes_send_ready_waiter() {
        let mut el = make_test_loop();

        if el.driver.udp_sockets.is_empty() {
            return;
        }

        // Register a waiter as though a task had polled UdpCtx::send_ready.
        el.executor.udp_send_ready_waiters[0] = Some(42);

        let data = b"wake";
        let (pool_slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();
        let slot_idx = el.driver.udp_sockets[0].send_freelist.pop().unwrap();

        let payload = crate::backend::uring::driver::encode_udp_send_payload(slot_idx, pool_slot);
        let ud = UserData::encode(OpTag::SendMsgUdp, 0u32, payload);
        el.test_dispatch_cqe(ud.raw(), data.len() as i32, 0);

        assert!(
            el.executor.udp_send_ready_waiters[0].is_none(),
            "send_ready waiter not cleared after CQE"
        );
    }

    #[test]
    fn udp_send_payload_roundtrip() {
        use crate::backend::uring::driver::{decode_udp_send_payload, encode_udp_send_payload};
        for slot_idx in [0u16, 1, 63, 255, u16::MAX] {
            for pool_slot in [0u16, 1, 511, 1023, u16::MAX] {
                let p = encode_udp_send_payload(slot_idx, pool_slot);
                assert_eq!(decode_udp_send_payload(p), (slot_idx, pool_slot));
            }
        }
    }

    // ── Partial send retry queue tests ─────────────────────────────

    #[test]
    fn handle_send_partial_queues_or_resubmits() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Allocate a pool slot with 10 bytes.
        let data = b"0123456789";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();

        // Mark send as in-flight.
        el.driver.send_queues[conn_index as usize].in_flight = true;

        // Simulate partial send: only 5 of 10 bytes sent.
        let ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), 5, 0);

        // The pool slot should still be in use (resubmitted or queued for retry).
        assert!(
            el.driver.send_copy_pool.in_use(slot),
            "pool slot released prematurely on partial send"
        );
    }

    #[test]
    fn handle_send_error_wakes_send_waiter_with_error() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Set up send waiter.
        el.executor.send_waiters[conn_index as usize] = true;
        el.driver.send_queues[conn_index as usize].in_flight = true;

        let data = b"hello";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();

        // Simulate send error (ECONNRESET).
        let ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
        el.test_dispatch_cqe(ud.raw(), -104, 0);

        // Send waiter should be cleared.
        assert!(
            !el.executor.send_waiters[conn_index as usize],
            "send waiter not cleared on error"
        );
        // Result should be stored (so SendFuture can retrieve it).
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "send error result not stored"
        );
    }

    #[test]
    fn handle_send_msg_zc_error_wakes_send_waiter() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Set up send waiter.
        el.executor.send_waiters[conn_index as usize] = true;

        let iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 100,
        }];
        let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
        let (slab_idx, _ptr) = el
            .driver
            .send_slab
            .allocate(conn_index, &iovecs, u16::MAX, guards, 0, 100)
            .unwrap();

        // Simulate ZC send error (ECONNRESET).
        let ud = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        el.test_dispatch_cqe(ud.raw(), -104, 0);

        // Send waiter should be cleared.
        assert!(
            !el.executor.send_waiters[conn_index as usize],
            "send waiter not cleared on ZC error"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "ZC send error result not stored"
        );
    }

    // ── DiskIoFuture Drop test ─────────────────────────────────────

    #[test]
    fn disk_io_future_drop_clears_waiter() {
        let mut el = make_test_loop();

        // Insert a disk_io_waiter entry manually.
        let seq = 42u32;
        let task_id = 0u32;
        el.executor.disk_io_waiters.insert(seq, task_id);
        assert!(el.executor.disk_io_waiters.contains_key(&seq));

        // Set up thread-local so DiskIoFuture::drop can access executor.
        let driver_ptr = &mut el.driver as *mut Driver;
        let executor_ptr = &mut el.executor as *mut Executor;

        // Safety: NonNull::new_unchecked is safe because we have valid pointers
        // from &mut el.driver and &mut el.executor above.
        let mut driver_state = DriverState {
            driver: unsafe { NonNull::new_unchecked(driver_ptr) },
            executor: unsafe { NonNull::new_unchecked(executor_ptr) },
        };
        let guard = unsafe { set_driver_state_guarded(&mut driver_state) };

        // Create and immediately drop a DiskIoFuture.
        {
            let _fut = crate::runtime::io::DiskIoFuture { seq };
        }

        drop(guard);

        // Waiter should be cleaned up.
        assert!(
            !el.executor.disk_io_waiters.contains_key(&seq),
            "disk_io_waiter not cleared on DiskIoFuture drop"
        );
    }

    // ── NOP error injection tests (real io_uring pipeline) ─────────
    //
    // These tests use IORING_NOP_INJECT_RESULT to send CQEs through
    // the full submit_and_wait → drain_completions → dispatch_cqe path.
    // Requires kernel 6.6+.

    #[test]
    fn nop_inject_send_complete() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let data = b"hello";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        let ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
        el.inject_and_dispatch(ud.raw(), data.len() as i32);

        assert_eq!(
            el.driver.send_copy_pool.free_count(),
            free_before + 1,
            "pool slot not released via NOP inject path"
        );
    }

    #[test]
    fn nop_inject_send_error_releases_and_wakes() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.executor.send_waiters[conn_index as usize] = true;
        el.driver.send_queues[conn_index as usize].in_flight = true;

        let data = b"hello";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();

        // Inject ECONNRESET through real io_uring.
        let ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
        el.inject_and_dispatch(ud.raw(), -104);

        // Pool slot released, waiter woken with error.
        assert!(
            !el.driver.send_copy_pool.in_use(slot),
            "pool slot not released on injected send error"
        );
        assert!(
            !el.executor.send_waiters[conn_index as usize],
            "send waiter not cleared on injected error"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "error result not stored"
        );
    }

    #[test]
    fn nop_inject_recv_eof_closes_connection() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.inject_and_dispatch(ud.raw(), 0);

        let conn = el.driver.connections.get(conn_index);
        assert!(
            conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed),
            "connection not closed on injected recv EOF"
        );
    }

    #[test]
    fn nop_inject_zc_send_error_no_slab_leak() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 100,
        }];
        let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
        let (slab_idx, _ptr) = el
            .driver
            .send_slab
            .allocate(conn_index, &iovecs, u16::MAX, guards, 0, 100)
            .unwrap();

        // Inject ZC send error through real pipeline.
        let ud = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        el.inject_and_dispatch(ud.raw(), -104);

        // Slab should be releasable (no notification expected on error).
        assert!(
            !el.driver.send_slab.in_use(slab_idx) || el.driver.send_slab.should_release(slab_idx),
            "slab entry leaked on injected ZC error"
        );
    }

    #[test]
    fn nop_inject_timer_fires() {
        let mut el = make_test_loop();

        let waker_id = 0u32;
        let (slot, generation) = el.executor.timer_pool.allocate(waker_id).unwrap();

        let payload = TimerSlotPool::encode_payload(slot, generation);
        let ud = UserData::encode(OpTag::Timer, 0, payload);

        // Inject timer expiry through real pipeline.
        el.inject_and_dispatch(ud.raw(), -62); // -ETIME

        assert!(
            el.executor.timer_pool.is_fired(slot),
            "timer not fired via NOP inject"
        );
    }

    #[test]
    fn nop_inject_send_wakes_waiter() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.executor.send_waiters[conn_index as usize] = true;
        el.driver.send_queues[conn_index as usize].in_flight = true;

        let data = b"hello";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();

        let ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
        el.inject_and_dispatch(ud.raw(), data.len() as i32);

        assert!(!el.executor.send_waiters[conn_index as usize]);
        assert!(el.executor.io_results[conn_index as usize].is_some());
    }

    #[test]
    fn nop_inject_zc_notif_releases_slab() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 100,
        }];
        let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
        let (slab_idx, _ptr) = el
            .driver
            .send_slab
            .allocate(conn_index, &iovecs, u16::MAX, guards, 0, 100)
            .unwrap();
        let free_before = el.driver.send_slab.free_count();

        // Set up as if operation CQE already processed.
        el.driver.send_slab.inc_pending_notifs(slab_idx);
        el.driver.send_slab.mark_awaiting_notifications(slab_idx);

        // Inject notification CQE (IORING_CQE_F_NOTIF = 8).
        let ud = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        // NOP inject only sets result, not flags. The notif flag is in CQE flags.
        // We can't inject CQE flags via NOP — use synthetic for this.
        // Fall back to test_dispatch_cqe for the notif path.
        el.test_dispatch_cqe(ud.raw(), 0, 8); // IORING_CQE_F_NOTIF

        assert_eq!(el.driver.send_slab.free_count(), free_before + 1);
    }

    #[test]
    fn nop_inject_zc_result_zero() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 100,
        }];
        let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
        let (slab_idx, _ptr) = el
            .driver
            .send_slab
            .allocate(conn_index, &iovecs, u16::MAX, guards, 0, 100)
            .unwrap();

        let ud = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        el.inject_and_dispatch(ud.raw(), 0);

        assert!(
            !el.driver.send_slab.in_use(slab_idx) || el.driver.send_slab.should_release(slab_idx),
        );
    }

    #[test]
    fn nop_inject_recv_enobufs() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // ENOBUFS = -105.
        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.inject_and_dispatch(ud.raw(), -105);

        assert!(
            el.driver.connections.get(conn_index).is_some(),
            "connection closed on ENOBUFS"
        );
    }

    #[test]
    fn nop_inject_recv_unknown_error_closes() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.inject_and_dispatch(ud.raw(), -99);

        let conn = el.driver.connections.get(conn_index);
        assert!(conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed),);
    }

    #[test]
    fn nop_inject_recv_ecanceled() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        el.inject_and_dispatch(ud.raw(), -125); // ECANCELED

        assert!(el.driver.connections.get(conn_index).is_some());
    }

    #[test]
    fn nop_inject_connect_success() {
        let mut el = make_test_loop();
        let conn_index = el
            .driver
            .connections
            .allocate_outbound()
            .expect("no free slots");
        el.executor.connect_waiters[conn_index as usize] = true;

        let ud = UserData::encode(OpTag::Connect, conn_index, 0);
        el.inject_and_dispatch(ud.raw(), 0);

        assert!(!el.executor.connect_waiters[conn_index as usize]);
        assert!(el.executor.io_results[conn_index as usize].is_some());
        let conn = el.driver.connections.get(conn_index).unwrap();
        assert!(conn.established);
        assert!(matches!(conn.recv_mode, RecvMode::Multi));
    }

    #[test]
    fn nop_inject_connect_error() {
        let mut el = make_test_loop();
        let conn_index = el
            .driver
            .connections
            .allocate_outbound()
            .expect("no free slots");
        el.executor.connect_waiters[conn_index as usize] = true;

        let ud = UserData::encode(OpTag::Connect, conn_index, 0);
        el.inject_and_dispatch(ud.raw(), -111); // ECONNREFUSED

        assert!(!el.executor.connect_waiters[conn_index as usize]);
        assert!(el.executor.io_results[conn_index as usize].is_some());
    }

    #[test]
    fn nop_inject_timer_stale_generation() {
        let mut el = make_test_loop();
        let (slot, generation) = el.executor.timer_pool.allocate(0).unwrap();
        el.executor.timer_pool.release(slot);
        let (_slot2, _gen2) = el.executor.timer_pool.allocate(0).unwrap();

        let payload = TimerSlotPool::encode_payload(slot, generation);
        let ud = UserData::encode(OpTag::Timer, 0, payload);
        el.inject_and_dispatch(ud.raw(), -62);

        assert!(!el.executor.timer_pool.is_fired(slot));
    }

    #[test]
    fn nop_inject_tls_send_complete() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let data = b"ciphertext";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        let ud = UserData::encode(OpTag::TlsSend, conn_index, slot as u32);
        el.inject_and_dispatch(ud.raw(), data.len() as i32);

        assert_eq!(el.driver.send_copy_pool.free_count(), free_before + 1);
    }

    #[test]
    fn nop_inject_tls_send_error_closes() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let data = b"ciphertext";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();

        let ud = UserData::encode(OpTag::TlsSend, conn_index, slot as u32);
        el.inject_and_dispatch(ud.raw(), -104);

        assert!(!el.driver.send_copy_pool.in_use(slot));
        let conn = el.driver.connections.get(conn_index);
        assert!(conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed));
    }

    #[test]
    fn nop_inject_tick_timeout() {
        let mut el = make_test_loop();
        el.driver.tick_timeout_armed = true;

        let ud = UserData::encode(OpTag::TickTimeout, 0, 0);
        el.inject_and_dispatch(ud.raw(), -62);

        assert!(!el.driver.tick_timeout_armed);
    }

    #[test]
    fn nop_inject_close_releases_slot() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        el.driver.close_connection(conn_index);

        let ud = UserData::encode(OpTag::Close, conn_index, 0);
        el.inject_and_dispatch(ud.raw(), 0);

        assert!(el.driver.connections.get(conn_index).is_none());
    }

    // ── Batch interaction tests (multi-CQE in one drain) ───────────
    //
    // These test cross-CQE interactions where one handler's side effects
    // affect subsequent handlers in the same drain_completions() call.

    #[test]
    fn batch_send_error_then_recv_on_same_conn() {
        // A send error and recv EOF arrive in the same batch for the
        // same connection. Both handlers should process without panic.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.executor.send_waiters[conn_index as usize] = true;
        el.driver.send_queues[conn_index as usize].in_flight = true;

        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(b"data").unwrap();

        let send_ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
        let recv_ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);

        el.inject_batch_and_dispatch(&[
            (send_ud.raw(), -104), // send error
            (recv_ud.raw(), 0),    // recv EOF
        ]);

        // Both should have processed. Pool slot released, connection closing.
        assert!(!el.driver.send_copy_pool.in_use(slot));
        let conn = el.driver.connections.get(conn_index);
        assert!(conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed));
    }

    #[test]
    fn batch_recv_eof_then_stale_send_cqe() {
        // Recv EOF closes the connection (sets recv_mode=Closed, submits
        // Close SQE), then a stale send CQE arrives for the same
        // conn_index in the same batch. The send handler should not
        // panic on the closing connection.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.driver.send_queues[conn_index as usize].in_flight = true;

        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(b"data").unwrap();

        // Recv EOF + stale send in the same batch.
        // The EOF handler calls close_connection internally.
        let recv_ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        let send_ud = UserData::encode(OpTag::Send, conn_index, slot as u32);

        el.inject_batch_and_dispatch(&[
            (recv_ud.raw(), 0), // EOF → close_connection
            (send_ud.raw(), 4), // stale send "completes" (4 bytes = b"data")
        ]);

        // Connection should be closing. Pool slot should be released
        // cleanly (no panic).
        let conn = el.driver.connections.get(conn_index);
        assert!(conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed));
        assert!(!el.driver.send_copy_pool.in_use(slot));
    }

    #[test]
    fn batch_two_sends_on_same_conn() {
        // Two send completions arrive in the same batch. The first should
        // release its pool slot and advance the queue. The second should
        // also release cleanly.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.driver.send_queues[conn_index as usize].in_flight = true;

        let (slot1, _p, _l) = el.driver.send_copy_pool.copy_in(b"aaa").unwrap();
        let (slot2, _p, _l) = el.driver.send_copy_pool.copy_in(b"bbb").unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        let ud1 = UserData::encode(OpTag::Send, conn_index, slot1 as u32);
        let ud2 = UserData::encode(OpTag::Send, conn_index, slot2 as u32);

        el.inject_batch_and_dispatch(&[(ud1.raw(), 3), (ud2.raw(), 3)]);

        // Both pool slots should be released.
        assert_eq!(
            el.driver.send_copy_pool.free_count(),
            free_before + 2,
            "both pool slots should be released"
        );
    }

    #[test]
    fn batch_multiple_connections_interleaved() {
        // CQEs for different connections arrive interleaved in one batch.
        let mut el = make_test_loop();
        let c1 = accept_connection(&mut el);
        let c2 = accept_connection(&mut el);
        el.executor.send_waiters[c1 as usize] = true;
        el.executor.send_waiters[c2 as usize] = true;
        el.driver.send_queues[c1 as usize].in_flight = true;
        el.driver.send_queues[c2 as usize].in_flight = true;

        let (s1, _p, _l) = el.driver.send_copy_pool.copy_in(b"hello").unwrap();
        let (s2, _p, _l) = el.driver.send_copy_pool.copy_in(b"world").unwrap();

        let ud1 = UserData::encode(OpTag::Send, c1, s1 as u32);
        let ud2 = UserData::encode(OpTag::Send, c2, s2 as u32);

        el.inject_batch_and_dispatch(&[
            (ud1.raw(), 5),    // c1 send complete
            (ud2.raw(), -104), // c2 send error
        ]);

        // c1: success result stored.
        assert!(el.executor.io_results[c1 as usize].is_some());
        // c2: error result stored.
        assert!(el.executor.io_results[c2 as usize].is_some());
        // Both pool slots released.
        assert!(!el.driver.send_copy_pool.in_use(s1));
        assert!(!el.driver.send_copy_pool.in_use(s2));
    }

    // ── Retry drain tests ──────────────────────────────────────────
    //
    // Test the pending retry mechanism by manually populating the
    // retry queues and draining them.

    #[test]
    fn retry_drain_copy_send_releases_on_closed_connection() {
        // Queue a copy retry for a connection that has since been closed.
        // The retry drain should release the pool slot and skip.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let data = b"retry-data";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();
        let generation = el.driver.connections.generation(conn_index);

        // Queue the retry.
        el.driver
            .pending_copy_retries
            .push((conn_index, generation, slot, 0, OpTag::Send));

        // Close the connection before the retry fires.
        el.driver.close_connection(conn_index);
        // Simulate the Close CQE to fully release the slot.
        let close_ud = UserData::encode(OpTag::Close, conn_index, 0);
        el.inject_and_dispatch(close_ud.raw(), 0);

        // Now drain retries — the connection is gone.
        el.drain_copy_retries();

        // Retry queue should be empty.
        assert!(el.driver.pending_copy_retries.is_empty());
        // Pool slot should be released (not leaked).
        assert!(
            !el.driver.send_copy_pool.in_use(slot),
            "pool slot leaked on retry with closed connection"
        );
    }

    #[test]
    fn retry_drain_copy_send_with_reused_connection() {
        // Queue a copy retry, then close and reuse the connection slot.
        // The generation check should prevent resubmission to the new connection.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let old_generation = el.driver.connections.generation(conn_index);

        let data = b"retry-data";
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(data).unwrap();

        // Queue the retry with the old generation.
        el.driver
            .pending_copy_retries
            .push((conn_index, old_generation, slot, 0, OpTag::Send));

        // Close the connection.
        el.driver.close_connection(conn_index);
        let close_ud = UserData::encode(OpTag::Close, conn_index, 0);
        el.inject_and_dispatch(close_ud.raw(), 0);

        // Reuse the slot with a new connection.
        let new_conn_index = accept_connection(&mut el);
        assert_eq!(
            new_conn_index, conn_index,
            "expected slot reuse for generation test"
        );
        let new_generation = el.driver.connections.generation(conn_index);
        assert_ne!(old_generation, new_generation);

        // Drain retries — should detect generation mismatch.
        el.drain_copy_retries();

        // Pool slot should be released (not resubmitted to new connection).
        assert!(
            !el.driver.send_copy_pool.in_use(slot),
            "pool slot should be released on generation mismatch"
        );
        // New connection should be unaffected.
        assert!(el.driver.connections.get(new_conn_index).is_some());
    }

    #[test]
    fn retry_drain_zc_send_releases_on_closed_connection() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);

        let iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 100,
        }];
        let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
        let (slab_idx, _ptr) = el
            .driver
            .send_slab
            .allocate(conn_index, &iovecs, u16::MAX, guards, 0, 100)
            .unwrap();

        // Simulate: operation CQE already incremented pending_notifs.
        el.driver.send_slab.inc_pending_notifs(slab_idx);

        // Queue the ZC retry.
        el.driver
            .pending_zc_retries
            .push((conn_index, generation, slab_idx, 0));

        // Close the connection.
        el.driver.close_connection(conn_index);
        let close_ud = UserData::encode(OpTag::Close, conn_index, 0);
        el.inject_and_dispatch(close_ud.raw(), 0);

        // Drain retries — connection is gone.
        el.drain_zc_retries();

        assert!(el.driver.pending_zc_retries.is_empty());
        // Slab should be marked for release (awaiting_notifications set,
        // and pending_notifs is still 1 — will be released when the
        // notification CQE arrives).
        // The key assertion: no panic, no hang, retry was handled.
    }

    #[test]
    fn disk_io_keys_are_unique_per_op() {
        // fs/NVMe/direct-io share the executor's completion maps; the key
        // must differ across ops even for the same slab index (three
        // independent slabs all start their free lists at 0).
        let mut el = make_test_loop();
        let mut ctx = el.driver.make_ctx();
        let k1 = ctx.disk_io_key(3);
        let k2 = ctx.disk_io_key(3);
        assert_ne!(k1, k2, "same slab index must map to distinct keys");
        assert_eq!(k1 & 0xFFFF, 3, "low 16 bits must carry the slab index");
        assert_eq!(k2 & 0xFFFF, 3);
    }

    #[test]
    fn stale_connect_timeout_ignored_on_reused_slot() {
        // A -ETIME deferred through CQ overflow can arrive after the slot
        // was closed and reused for a NEW outbound connect. The generation
        // in the payload must prevent it from killing the new connect.
        let mut el = make_test_loop();
        let conn_index = el.driver.connections.allocate_outbound().unwrap();
        let old_generation = el.driver.connections.generation(conn_index);

        // Close + reuse: new outbound connect in the same slot.
        el.driver.close_connection(conn_index);
        let close_ud = UserData::encode(OpTag::Close, conn_index, 0);
        el.inject_and_dispatch(close_ud.raw(), 0);
        let reused = el.driver.connections.allocate_outbound().unwrap();
        assert_eq!(reused, conn_index);
        if let Some(cs) = el.driver.connections.get_mut(conn_index) {
            cs.connect_timeout_armed = true;
        }

        // Stale -ETIME with the OLD generation: must be ignored.
        let ud = UserData::encode(OpTag::Timeout, conn_index, old_generation);
        el.inject_and_dispatch(ud.raw(), -62);

        let conn = el.driver.connections.get(conn_index);
        assert!(
            conn.is_some() && matches!(conn.unwrap().recv_mode, RecvMode::Connecting),
            "stale connect-timeout CQE must not kill the reused slot's connect"
        );
    }

    #[test]
    fn close_retry_backoff_preserves_entry() {
        // On a backoff tick (tick_count % 4 != 0) the entry must stay
        // queued — it used to be silently dropped, leaking the fd + slot.
        let mut el = make_test_loop();
        el.driver.tick_count = 1;
        el.driver.pending_close_retries.push((7, 2));

        el.drain_close_retries();

        assert_eq!(
            el.driver.pending_close_retries,
            vec![(7, 2)],
            "backoff tick must preserve the close-retry entry unchanged"
        );
    }

    #[test]
    fn pollout_retry_backoff_preserves_entry() {
        // Same as above for the POLLOUT retry queue (every 2nd tick).
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(b"data").unwrap();

        el.driver.tick_count = 1;
        el.driver
            .pending_send_pollout_retries
            .push((conn_index, generation, slot, 1, false));

        el.drain_send_pollout_retries();

        assert_eq!(
            el.driver.pending_send_pollout_retries,
            vec![(conn_index, generation, slot, 1, false)],
            "backoff tick must preserve the pollout-retry entry unchanged"
        );
    }

    #[test]
    fn copy_retry_giveup_fails_send_and_closes() {
        // Exhausted retries must not leave the connection wedged open with
        // in_flight stuck: release the slot, wake the waiter, close.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        let generation = el.driver.connections.generation(conn_index);
        let (slot, _ptr, _len) = el.driver.send_copy_pool.copy_in(b"data").unwrap();
        el.driver.send_queues[conn_index as usize].in_flight = true;
        el.executor.send_waiters[conn_index as usize] = true;

        el.driver
            .pending_copy_retries
            .push((conn_index, generation, slot, 2, OpTag::Send));
        el.drain_copy_retries();

        assert!(
            !el.driver.send_copy_pool.in_use(slot),
            "give-up must release the pool slot"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "give-up must wake the send waiter with a result"
        );
        let conn = el.driver.connections.get(conn_index);
        assert!(
            conn.is_none() || matches!(conn.unwrap().recv_mode, RecvMode::Closed),
            "give-up must close the connection"
        );
    }

    #[test]
    fn submit_next_queued_empty_finalizes_deferred_close() {
        // A close deferred behind an in-flight ZC/recv-forward send must
        // finalize when the queue empties, even though those completion
        // paths never call note_send_finalized.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Simulate an in-flight send, then close: the Close SQE is deferred.
        el.driver.send_queues[conn_index as usize].in_flight = true;
        el.driver.close_connection(conn_index);
        assert!(
            el.driver.send_queues[conn_index as usize].close_pending,
            "close must defer while a send is in flight"
        );

        // The send's CQE path ends in submit_next_queued with an empty
        // queue — this must fire the deferred close.
        el.driver.submit_next_queued(conn_index);
        assert!(
            !el.driver.send_queues[conn_index as usize].close_pending,
            "deferred close must finalize once the queue drains"
        );
    }

    #[test]
    fn drain_conn_send_queue_finalizes_deferred_close() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        el.driver.send_queues[conn_index as usize].in_flight = true;
        el.driver.close_connection(conn_index);
        assert!(el.driver.send_queues[conn_index as usize].close_pending);

        el.driver.drain_conn_send_queue(conn_index);
        assert!(
            !el.driver.send_queues[conn_index as usize].close_pending,
            "deferred close must finalize when the queue is force-drained"
        );
    }

    // ── Executor wake-path regression tests ────────────────────────

    #[test]
    fn self_wake_via_channel_completes() {
        // join(rx.recv(), try_send) inside ONE task: recv registers its
        // waiter, try_send wakes it while the task is mid-poll (slot
        // Empty). The wake used to be dropped -> permanent deadlock.
        use crate::runtime::{channel::mpsc, join::join};
        let mut el = make_test_loop();
        let (tx, rx) = mpsc::channel::<u32>(4);
        let done = std::rc::Rc::new(std::cell::Cell::new(false));
        let done2 = done.clone();

        let idx = el
            .executor
            .standalone_slab
            .spawn(Box::pin(async move {
                let (v, _) = join(rx.recv(), async move {
                    tx.try_send(7).unwrap();
                })
                .await;
                assert_eq!(v, Some(7));
                done2.set(true);
            }))
            .unwrap();
        el.executor.ready_queue.push_back(idx | STANDALONE_BIT);

        for _ in 0..4 {
            el.poll_ready_tasks();
        }
        assert!(
            done.get(),
            "self-wake during poll was lost — task deadlocked"
        );
    }

    #[test]
    fn stored_std_waker_wakes_parked_task() {
        // A future that stashes cx.waker() and is woken later from outside
        // its own poll. The drain path used to skip the Parked->Ready
        // transition, so the wake was lost forever.
        use std::cell::RefCell;
        use std::rc::Rc;
        use std::task::{Poll, Waker};

        struct StashWaker {
            slot: Rc<RefCell<Option<Waker>>>,
            polls: u32,
        }
        impl std::future::Future for StashWaker {
            type Output = ();
            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> Poll<()> {
                self.polls += 1;
                if self.polls == 1 {
                    *self.slot.borrow_mut() = Some(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
        }

        let mut el = make_test_loop();
        let slot = Rc::new(RefCell::new(None));
        let done = std::rc::Rc::new(std::cell::Cell::new(false));
        let (slot2, done2) = (slot.clone(), done.clone());

        let idx = el
            .executor
            .standalone_slab
            .spawn(Box::pin(async move {
                StashWaker {
                    slot: slot2,
                    polls: 0,
                }
                .await;
                done2.set(true);
            }))
            .unwrap();
        el.executor.ready_queue.push_back(idx | STANDALONE_BIT);
        el.poll_ready_tasks();
        assert!(!done.get(), "future must park on first poll");

        // Wake from "outside" (e.g. another task) via the stored waker.
        slot.borrow().as_ref().unwrap().wake_by_ref();
        el.executor.collect_wakeups();
        el.poll_ready_tasks();

        assert!(done.get(), "stored-waker wake of a parked task was lost");
    }

    #[test]
    fn mpsc_two_blocked_senders_both_complete() {
        // Two producers blocked on a capacity-1 channel: the single-slot
        // send_waiter used to let the second registration overwrite the
        // first, hanging it forever.
        use crate::runtime::channel::mpsc;
        let mut el = make_test_loop();
        let (tx, rx) = mpsc::channel::<u32>(1);
        tx.try_send(0).unwrap(); // fill the queue
        let completed = std::rc::Rc::new(std::cell::Cell::new(0u32));

        for _ in 0..2 {
            let tx = tx.clone();
            let completed = completed.clone();
            let idx = el
                .executor
                .standalone_slab
                .spawn(Box::pin(async move {
                    tx.send(1).await.unwrap();
                    completed.set(completed.get() + 1);
                }))
                .unwrap();
            el.executor.ready_queue.push_back(idx | STANDALONE_BIT);
        }
        // Park both senders on the full queue.
        el.poll_ready_tasks();
        assert_eq!(completed.get(), 0);

        // Drain three values (the prefill + both sends), interleaving polls
        // so each freed slot wakes exactly one blocked sender.
        let drainer_done = std::rc::Rc::new(std::cell::Cell::new(false));
        let dd = drainer_done.clone();
        let idx = el
            .executor
            .standalone_slab
            .spawn(Box::pin(async move {
                for _ in 0..3 {
                    assert!(rx.recv().await.is_some());
                }
                dd.set(true);
            }))
            .unwrap();
        el.executor.ready_queue.push_back(idx | STANDALONE_BIT);

        for _ in 0..8 {
            el.poll_ready_tasks();
        }
        assert!(drainer_done.get(), "drainer did not finish");
        assert_eq!(
            completed.get(),
            2,
            "a blocked sender's wake was lost (single-slot send_waiter)"
        );
    }

    // ── Linked SQE chain error propagation tests ───────────────────
    //
    // Submit linked NOP chains where the first SQE fails. The kernel
    // cancels subsequent linked SQEs with ECANCELED. This tests the
    // chain error handling path end-to-end through the real kernel.

    #[test]
    fn linked_chain_copy_send_first_fails_releases_all() {
        // 3-SQE chain: first Send fails, second and third get ECANCELED.
        // All pool slots should be released, chain should complete.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.executor.send_waiters[conn_index as usize] = true;

        // Allocate 3 pool slots for the chain.
        let (s1, _, _) = el.driver.send_copy_pool.copy_in(b"aaa").unwrap();
        let (s2, _, _) = el.driver.send_copy_pool.copy_in(b"bbb").unwrap();
        let (s3, _, _) = el.driver.send_copy_pool.copy_in(b"ccc").unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        // Register chain state: 3 SQEs, 9 total bytes.
        el.driver.chain_table.start(conn_index, 3, 9);

        // Submit linked NOPs: first with injected error, rest linked.
        // The kernel will deliver: error CQE, ECANCELED CQE, ECANCELED CQE.
        let ud1 = UserData::encode(OpTag::Send, conn_index, s1 as u32);
        let ud2 = UserData::encode(OpTag::Send, conn_index, s2 as u32);
        let ud3 = UserData::encode(OpTag::Send, conn_index, s3 as u32);

        el.inject_linked_chain_and_dispatch(&[
            (ud1.raw(), -104), // ECONNRESET — first SQE fails
            (ud2.raw(), 0),    // kernel sets result for linked NOPs
            (ud3.raw(), 0),    // kernel sets result for linked NOPs
        ]);

        // All 3 pool slots should be released.
        assert_eq!(
            el.driver.send_copy_pool.free_count(),
            free_before + 3,
            "not all pool slots released after chain error"
        );

        // Chain should be complete (no longer active).
        assert!(
            !el.driver.chain_table.is_active(conn_index),
            "chain still active after all CQEs processed"
        );

        // Send waiter should have been woken with an error.
        assert!(
            !el.executor.send_waiters[conn_index as usize],
            "send waiter not cleared"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "chain result not stored"
        );
    }

    #[test]
    fn linked_chain_middle_fails_rest_canceled() {
        // 3-SQE chain: first succeeds, second fails, third ECANCELED.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.executor.send_waiters[conn_index as usize] = true;

        let (s1, _, _) = el.driver.send_copy_pool.copy_in(b"aaa").unwrap();
        let (s2, _, _) = el.driver.send_copy_pool.copy_in(b"bbb").unwrap();
        let (s3, _, _) = el.driver.send_copy_pool.copy_in(b"ccc").unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        el.driver.chain_table.start(conn_index, 3, 9);

        let ud1 = UserData::encode(OpTag::Send, conn_index, s1 as u32);
        let ud2 = UserData::encode(OpTag::Send, conn_index, s2 as u32);
        let ud3 = UserData::encode(OpTag::Send, conn_index, s3 as u32);

        el.inject_linked_chain_and_dispatch(&[
            (ud1.raw(), 3),    // first succeeds (3 bytes)
            (ud2.raw(), -104), // second fails
            (ud3.raw(), 0),    // third ECANCELED
        ]);

        assert_eq!(
            el.driver.send_copy_pool.free_count(),
            free_before + 3,
            "not all pool slots released"
        );
        assert!(!el.driver.chain_table.is_active(conn_index));
        assert!(!el.executor.send_waiters[conn_index as usize]);
    }

    #[test]
    fn linked_chain_all_succeed() {
        // 3-SQE chain: all succeed. No errors.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);
        el.executor.send_waiters[conn_index as usize] = true;

        let (s1, _, _) = el.driver.send_copy_pool.copy_in(b"aaa").unwrap();
        let (s2, _, _) = el.driver.send_copy_pool.copy_in(b"bbb").unwrap();
        let (s3, _, _) = el.driver.send_copy_pool.copy_in(b"ccc").unwrap();
        let free_before = el.driver.send_copy_pool.free_count();

        el.driver.chain_table.start(conn_index, 3, 9);

        let ud1 = UserData::encode(OpTag::Send, conn_index, s1 as u32);
        let ud2 = UserData::encode(OpTag::Send, conn_index, s2 as u32);
        let ud3 = UserData::encode(OpTag::Send, conn_index, s3 as u32);

        el.inject_linked_chain_and_dispatch(&[(ud1.raw(), 3), (ud2.raw(), 3), (ud3.raw(), 3)]);

        assert_eq!(el.driver.send_copy_pool.free_count(), free_before + 3);
        assert!(!el.driver.chain_table.is_active(conn_index));
        assert!(!el.executor.send_waiters[conn_index as usize]);
        assert!(el.executor.io_results[conn_index as usize].is_some());
    }

    // ── Cancel injection tests ─────────────────────────────────────
    //
    // Submit a real timeout SQE, then cancel it with ASYNC_CANCEL.
    // This exercises the timer ECANCELED path through the real kernel,
    // simulating what happens when SleepFuture is dropped via select!.

    #[test]
    fn cancel_injection_timer_ecanceled() {
        let mut el = make_test_loop();

        // Allocate a timer slot and submit a real timeout (10 seconds — won't fire).
        let waker_id = 0u32;
        let (slot, generation) = el.executor.timer_pool.allocate(waker_id).unwrap();

        // Set up the timespec in the pool.
        el.executor.timer_pool.timespecs[slot as usize] =
            io_uring::types::Timespec::new().sec(10).nsec(0);

        let payload = TimerSlotPool::encode_payload(slot, generation);
        let timer_ud = UserData::encode(OpTag::Timer, 0, payload);
        let ts_ptr =
            &el.executor.timer_pool.timespecs[slot as usize] as *const io_uring::types::Timespec;

        // Submit the real timeout SQE.
        el.driver
            .ring
            .submit_timeout(ts_ptr, timer_ud)
            .expect("submit_timeout failed");

        // Now cancel it — simulating SleepFuture::drop.
        el.driver
            .ring
            .submit_async_cancel(timer_ud.raw(), 0)
            .expect("submit_async_cancel failed");

        // Process CQEs: should get Timer CQE with -ECANCELED,
        // and Cancel CQE (which is a no-op in dispatch).
        el.driver
            .ring
            .submit_and_wait(2)
            .expect("submit_and_wait failed");
        el.drain_completions();

        // Timer should NOT be fired (it was cancelled, not expired).
        assert!(
            !el.executor.timer_pool.is_fired(slot),
            "cancelled timer should not be fired"
        );

        // Simulate SleepFuture::drop releasing the slot.
        el.executor.timer_pool.release(slot);

        // Verify the slot can be reallocated (proves it was returned).
        let (slot2, _gen2) = el.executor.timer_pool.allocate(0).unwrap();
        assert_eq!(slot2, slot, "released slot should be reusable");
        el.executor.timer_pool.release(slot2);
    }

    #[test]
    fn cancel_injection_timer_fires_before_cancel() {
        // Submit a very short timeout (1ns), then cancel. The timeout
        // might fire before the cancel takes effect. Both outcomes
        // should be handled without panic or leak.
        let mut el = make_test_loop();

        let waker_id = 0u32;
        let (slot, generation) = el.executor.timer_pool.allocate(waker_id).unwrap();

        // 1 nanosecond timeout — will fire almost immediately.
        el.executor.timer_pool.timespecs[slot as usize] =
            io_uring::types::Timespec::new().sec(0).nsec(1);

        let payload = TimerSlotPool::encode_payload(slot, generation);
        let timer_ud = UserData::encode(OpTag::Timer, 0, payload);
        let ts_ptr =
            &el.executor.timer_pool.timespecs[slot as usize] as *const io_uring::types::Timespec;

        el.driver
            .ring
            .submit_timeout(ts_ptr, timer_ud)
            .expect("submit_timeout failed");
        el.driver
            .ring
            .submit_async_cancel(timer_ud.raw(), 0)
            .expect("submit_async_cancel failed");

        // Process all CQEs.
        el.driver
            .ring
            .submit_and_wait(1)
            .expect("submit_and_wait failed");
        // Small sleep to let both CQEs arrive.
        std::thread::sleep(std::time::Duration::from_millis(10));
        el.drain_completions();

        // Either the timer fired (-ETIME) or was cancelled (-ECANCELED).
        // In both cases: no panic, no leak.
        // If fired, the slot is marked as fired.
        // If cancelled, the slot is not fired.
        // Release the slot (simulating SleepFuture::drop).
        el.executor.timer_pool.release(slot);

        // No assertions on fired state — both outcomes are valid.
        // The key assertion: no panic during processing, and the slot
        // is cleanly released.
    }

    // ── SendRecvBuf (zero-copy forward) tests ──────────────────────

    #[test]
    fn handle_send_recv_buf_full_send_replenishes_and_wakes() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Set up a send waiter.
        el.executor.send_waiters[conn_index as usize] = true;

        // Simulate a forward_recv_buf send: bid=3, data_len=100.
        let bid: u16 = 3;
        let data_len: u32 = 100;
        el.driver.send_recv_buf_original_lens[conn_index as usize] = data_len;
        el.driver.send_recv_buf_remaining[conn_index as usize] = data_len;
        let payload = bid as u32;
        let ud = UserData::encode(OpTag::SendRecvBuf, conn_index, payload);

        // Full send: all 100 bytes sent.
        el.test_dispatch_cqe(ud.raw(), 100, 0);

        // Buffer should be replenished.
        assert!(
            el.driver.pending_replenish.contains(&bid),
            "buffer not replenished after full send"
        );
        // Send waiter should be woken with Ok(100).
        assert!(
            !el.executor.send_waiters[conn_index as usize],
            "send waiter not cleared"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "send result not stored"
        );
    }

    #[test]
    fn handle_send_recv_buf_error_replenishes_buffer() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        el.executor.send_waiters[conn_index as usize] = true;

        let bid: u16 = 5;
        let data_len: u32 = 200;
        el.driver.send_recv_buf_original_lens[conn_index as usize] = data_len;
        el.driver.send_recv_buf_remaining[conn_index as usize] = data_len;
        let payload = bid as u32;
        let ud = UserData::encode(OpTag::SendRecvBuf, conn_index, payload);

        // Simulate ECONNRESET.
        el.test_dispatch_cqe(ud.raw(), -104, 0);

        // Buffer should be replenished even on error.
        assert!(
            el.driver.pending_replenish.contains(&bid),
            "buffer not replenished after send error"
        );
        assert!(
            el.executor.io_results[conn_index as usize].is_some(),
            "error result not stored"
        );
    }

    #[test]
    fn handle_send_recv_buf_partial_send_computes_correct_offset() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Configure: buffer_size = 4096, but data is only 100 bytes.
        // This is the common case — TCP segments are smaller than buffer capacity.
        let bid: u16 = 0;
        let data_len: u32 = 100;

        // Write recognizable data into the provided buffer.
        let (buf_ptr, buf_size) = el.driver.provided_bufs.get_buffer(bid);
        assert!(
            buf_size > data_len,
            "test requires buffer_size > data_len to exercise the bug"
        );
        let test_data: Vec<u8> = (0..data_len as u8).collect();
        unsafe {
            std::ptr::copy_nonoverlapping(
                test_data.as_ptr(),
                buf_ptr as *mut u8,
                data_len as usize,
            );
        }

        // Set up original length tracking (mirrors forward_recv_buf).
        el.driver.send_recv_buf_original_lens[conn_index as usize] = data_len;
        el.driver.send_recv_buf_remaining[conn_index as usize] = data_len;
        let payload = bid as u32;
        let ud = UserData::encode(OpTag::SendRecvBuf, conn_index, payload);

        // Partial send: only 60 of 100 bytes sent.
        el.test_dispatch_cqe(ud.raw(), 60, 0);

        // Buffer should NOT be replenished yet (still in-flight).
        assert!(
            !el.driver.pending_replenish.contains(&bid),
            "buffer replenished prematurely on partial send"
        );

        // The retry SQE should have been pushed to the ring. The handler updated
        // send_recv_buf_remaining to 40; the retry CQE just needs bid in the payload.
        assert_eq!(
            el.driver.send_recv_buf_remaining[conn_index as usize], 40,
            "remaining not updated after partial send"
        );
        let new_ud = UserData::encode(OpTag::SendRecvBuf, conn_index, bid as u32);

        // Complete the retry — all 40 remaining bytes sent.
        el.test_dispatch_cqe(new_ud.raw(), 40, 0);

        // Now the buffer should be replenished.
        assert!(
            el.driver.pending_replenish.contains(&bid),
            "buffer not replenished after retry completed"
        );
    }

    #[test]
    fn handle_send_recv_buf_double_partial_send_offset() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        // Data is 100 bytes in a 4096-byte buffer.
        let bid: u16 = 2;
        let data_len: u32 = 100;

        let (buf_ptr, _) = el.driver.provided_bufs.get_buffer(bid);
        // Fill with pattern so we can verify offset correctness.
        let test_data: Vec<u8> = (0u8..100).collect();
        unsafe {
            std::ptr::copy_nonoverlapping(test_data.as_ptr(), buf_ptr as *mut u8, 100);
        }

        el.driver.send_recv_buf_original_lens[conn_index as usize] = data_len;
        el.driver.send_recv_buf_remaining[conn_index as usize] = data_len;
        let payload = bid as u32;
        let ud = UserData::encode(OpTag::SendRecvBuf, conn_index, payload);

        // First partial: 30 of 100 bytes sent. Remaining = 70. Offset should be 30.
        el.test_dispatch_cqe(ud.raw(), 30, 0);
        assert!(!el.driver.pending_replenish.contains(&bid));
        assert_eq!(el.driver.send_recv_buf_remaining[conn_index as usize], 70);

        // Second partial: 20 of 70 bytes sent. Remaining = 50. Offset should be 50.
        let ud2 = UserData::encode(OpTag::SendRecvBuf, conn_index, bid as u32);
        el.test_dispatch_cqe(ud2.raw(), 20, 0);
        assert!(!el.driver.pending_replenish.contains(&bid));
        assert_eq!(el.driver.send_recv_buf_remaining[conn_index as usize], 50);

        // Final: 50 of 50 bytes sent. Should complete.
        let ud3 = UserData::encode(OpTag::SendRecvBuf, conn_index, bid as u32);
        el.test_dispatch_cqe(ud3.raw(), 50, 0);
        assert!(
            el.driver.pending_replenish.contains(&bid),
            "buffer not replenished after final partial send"
        );
    }

    #[test]
    fn handle_send_recv_buf_partial_send_pointer_correctness() {
        // Verify the fix: on partial send, the resubmitted SQE pointer must be
        // buf_ptr + (original_len - new_remaining), NOT buf_ptr + (buf_size - new_remaining).
        //
        // We can't inspect the SQE directly, but we can verify the logic by checking
        // that handle_send_recv_buf computes the offset from original_len rather than buf_size.
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let bid: u16 = 1;
        let data_len: u32 = 50;
        let (_, _buf_size) = el.driver.provided_bufs.get_buffer(bid);

        // The bug: offset = buf_size - new_remaining = 4096 - 25 = 4071 (WRONG)
        // The fix: offset = original_len - new_remaining = 50 - 25 = 25 (CORRECT)
        // With buf_size=4096 and data_len=50, the wrong offset points way past the data.

        el.driver.send_recv_buf_original_lens[conn_index as usize] = data_len;
        el.driver.send_recv_buf_remaining[conn_index as usize] = data_len;
        let payload = bid as u32;
        let ud = UserData::encode(OpTag::SendRecvBuf, conn_index, payload);

        // Partial send: 25 of 50 bytes.
        el.test_dispatch_cqe(ud.raw(), 25, 0);

        // If the offset was computed correctly (25, not 4071), the resubmitted SQE
        // will have a valid pointer within the data. With the bug, the pointer
        // would be past the buffer entirely (buf_ptr + 4071 vs buf_ptr + 25).
        // Since we successfully pushed the SQE without crashing or panicking,
        // and buf_size (4096) > buggy offset (4071), the SQE was "valid" but pointed
        // to garbage. The fix ensures correct data is referenced.
        //
        // The real verification is that the resubmitted send completes successfully.
        // Simulate that by completing the retry.
        assert_eq!(el.driver.send_recv_buf_remaining[conn_index as usize], 25);
        let retry_ud = UserData::encode(OpTag::SendRecvBuf, conn_index, bid as u32);
        el.test_dispatch_cqe(retry_ud.raw(), 25, 0);

        assert!(
            el.driver.pending_replenish.contains(&bid),
            "buffer not replenished after partial send retry"
        );
        // Verify the original_len was preserved correctly across retries.
        assert_eq!(
            el.driver.send_recv_buf_original_lens[conn_index as usize], data_len,
            "original_len should be preserved across partial send retries"
        );
    }

    #[test]
    fn handle_send_recv_buf_zero_result_replenishes() {
        let mut el = make_test_loop();
        let conn_index = accept_connection(&mut el);

        let bid: u16 = 7;
        let data_len: u32 = 50;
        el.driver.send_recv_buf_original_lens[conn_index as usize] = data_len;
        el.driver.send_recv_buf_remaining[conn_index as usize] = data_len;
        let payload = bid as u32;
        let ud = UserData::encode(OpTag::SendRecvBuf, conn_index, payload);

        // Result == 0 (zero-length send).
        el.test_dispatch_cqe(ud.raw(), 0, 0);

        assert!(
            el.driver.pending_replenish.contains(&bid),
            "buffer not replenished on zero-length send"
        );
    }

    // ── Property-based tests (proptest) ────────────────────────────
    //
    // Generate random sequences of CQE events and verify resource
    // invariants hold: no pool leaks, no slab leaks, no panics.

    mod proptest_cqe {
        use super::*;
        use proptest::prelude::*;

        /// Random CQE action on a connection with an allocated pool slot.
        #[derive(Debug, Clone)]
        enum SendAction {
            /// Send completes successfully (all bytes).
            Ok,
            /// Send fails with an error.
            Error,
            /// Send completes with 0 bytes.
            Zero,
        }

        /// Random CQE action for ZC sends.
        #[derive(Debug, Clone)]
        enum ZcAction {
            /// ZC send succeeds, notification follows.
            OkThenNotif,
            /// ZC send fails with error.
            Error,
            /// ZC send result == 0.
            Zero,
        }

        /// Random recv CQE result.
        #[derive(Debug, Clone)]
        enum RecvAction {
            /// EOF (result == 0).
            Eof,
            /// Error (unknown errno).
            Error,
            /// ENOBUFS — buffer ring exhausted.
            Enobufs,
            /// ECANCELED.
            Ecanceled,
        }

        fn send_action_strategy() -> impl Strategy<Value = SendAction> {
            prop_oneof![
                Just(SendAction::Ok),
                Just(SendAction::Error),
                Just(SendAction::Zero),
            ]
        }

        fn zc_action_strategy() -> impl Strategy<Value = ZcAction> {
            prop_oneof![
                Just(ZcAction::OkThenNotif),
                Just(ZcAction::Error),
                Just(ZcAction::Zero),
            ]
        }

        fn recv_action_strategy() -> impl Strategy<Value = RecvAction> {
            prop_oneof![
                Just(RecvAction::Eof),
                Just(RecvAction::Error),
                Just(RecvAction::Enobufs),
                Just(RecvAction::Ecanceled),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(200))]

            #[test]
            fn send_sequence_no_pool_leak(actions in proptest::collection::vec(send_action_strategy(), 1..8)) {
                let mut el = make_test_loop();
                let conn_index = accept_connection(&mut el);
                el.driver.send_queues[conn_index as usize].in_flight = true;

                let initial_free = el.driver.send_copy_pool.free_count();

                for action in &actions {
                    let data = b"test";
                    let (slot, _, _) = match el.driver.send_copy_pool.copy_in(data) {
                        Some(s) => s,
                        None => break, // pool exhausted — stop sequence
                    };

                    let ud = UserData::encode(OpTag::Send, conn_index, slot as u32);
                    let result = match action {
                        SendAction::Ok => data.len() as i32,
                        SendAction::Error => -104, // ECONNRESET
                        SendAction::Zero => 0,
                    };
                    el.test_dispatch_cqe(ud.raw(), result, 0);
                }

                // All pool slots should be released (no leaks).
                prop_assert_eq!(
                    el.driver.send_copy_pool.free_count(),
                    initial_free,
                    "pool slot leak detected"
                );
            }

            #[test]
            fn zc_sequence_no_slab_leak(actions in proptest::collection::vec(zc_action_strategy(), 1..6)) {
                let mut el = make_test_loop();
                let conn_index = accept_connection(&mut el);

                let initial_slab_free = el.driver.send_slab.free_count();
                let _initial_pool_free = el.driver.send_copy_pool.free_count();

                for action in &actions {
                    let iovecs = [libc::iovec { iov_base: std::ptr::null_mut(), iov_len: 100 }];
                    let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
                    let (slab_idx, _) = match el.driver.send_slab.allocate(conn_index, &iovecs, u16::MAX, guards, 0, 100) {
                        Some(s) => s,
                        None => break,
                    };

                    let ud = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);

                    match action {
                        ZcAction::OkThenNotif => {
                            // Operation CQE with success.
                            el.test_dispatch_cqe(ud.raw(), 100, 0);
                            // Notification CQE.
                            el.test_dispatch_cqe(ud.raw(), 0, 8); // IORING_CQE_F_NOTIF
                        }
                        ZcAction::Error => {
                            el.test_dispatch_cqe(ud.raw(), -104, 0);
                            // Error path: mark_awaiting + should_release.
                            // May need explicit release if should_release is true.
                            if el.driver.send_slab.in_use(slab_idx) && el.driver.send_slab.should_release(slab_idx) {
                                el.driver.send_slab.release(slab_idx);
                            }
                        }
                        ZcAction::Zero => {
                            el.test_dispatch_cqe(ud.raw(), 0, 0);
                            if el.driver.send_slab.in_use(slab_idx) && el.driver.send_slab.should_release(slab_idx) {
                                el.driver.send_slab.release(slab_idx);
                            }
                        }
                    }
                }

                // All slab entries should be released.
                prop_assert_eq!(
                    el.driver.send_slab.free_count(),
                    initial_slab_free,
                    "slab entry leak detected"
                );
            }

            #[test]
            fn recv_sequence_no_panic(actions in proptest::collection::vec(recv_action_strategy(), 1..10)) {
                let mut el = make_test_loop();
                let conn_index = accept_connection(&mut el);

                for action in &actions {
                    // Skip if connection already closed.
                    if el.driver.connections.get(conn_index).is_none()
                        || matches!(
                            el.driver.connections.get(conn_index).unwrap().recv_mode,
                            RecvMode::Closed
                        )
                    {
                        break;
                    }

                    let ud = UserData::encode(OpTag::RecvMulti, conn_index, 0);
                    let result = match action {
                        RecvAction::Eof => 0,
                        RecvAction::Error => -99,
                        RecvAction::Enobufs => -105,
                        RecvAction::Ecanceled => -125,
                    };
                    el.test_dispatch_cqe(ud.raw(), result, 0);
                }

                // No assertion needed — the property is "no panic".
                // If we get here, the sequence was handled cleanly.
            }

            /// Mixed operation sequence across multiple connections.
            /// This is the most aggressive test — it interleaves different
            /// operation types on different connections, including connection
            /// lifecycle (accept, use, close, slot reuse).
            #[test]
            fn mixed_operations_no_leak_no_panic(
                actions in proptest::collection::vec(0..10u8, 5..30)
            ) {
                let mut el = make_test_loop();
                let initial_pool_free = el.driver.send_copy_pool.free_count();
                let initial_slab_free = el.driver.send_slab.free_count();

                // Track live connections and their allocated resources.
                let mut live_conns: Vec<u32> = Vec::new();
                let mut pool_slots_in_flight: Vec<u16> = Vec::new();

                for action in actions {
                    match action {
                        // Accept a new connection (if capacity available).
                        0 if live_conns.len() < 8 => {
                            let ci = accept_connection(&mut el);
                            el.driver.send_queues[ci as usize].in_flight = false;
                            live_conns.push(ci);
                        }

                        // Send success on a random live connection.
                        1 if !live_conns.is_empty() => {
                            let ci = live_conns[0];
                            if let Some((slot, _, _)) = el.driver.send_copy_pool.copy_in(b"data") {
                                let ud = UserData::encode(OpTag::Send, ci, slot as u32);
                                el.test_dispatch_cqe(ud.raw(), 4, 0);
                            }
                        }

                        // Send error on a random live connection.
                        2 if !live_conns.is_empty() => {
                            let ci = live_conns[0];
                            if let Some((slot, _, _)) = el.driver.send_copy_pool.copy_in(b"data") {
                                let ud = UserData::encode(OpTag::Send, ci, slot as u32);
                                el.test_dispatch_cqe(ud.raw(), -104, 0);
                            }
                        }

                        // ZC send success + notification on a live connection.
                        3 if !live_conns.is_empty() => {
                            let ci = live_conns[0];
                            let iovecs = [libc::iovec {
                                iov_base: std::ptr::null_mut(),
                                iov_len: 50,
                            }];
                            let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
                            if let Some((slab_idx, _)) = el.driver.send_slab.allocate(
                                ci, &iovecs, u16::MAX, guards, 0, 50,
                            ) {
                                let ud = UserData::encode(
                                    OpTag::SendMsgZc, ci, slab_idx as u32,
                                );
                                // Operation CQE (success).
                                el.test_dispatch_cqe(ud.raw(), 50, 0);
                                // Notification CQE.
                                el.test_dispatch_cqe(ud.raw(), 0, 8);
                            }
                        }

                        // ZC send error on a live connection.
                        4 if !live_conns.is_empty() => {
                            let ci = live_conns[0];
                            let iovecs = [libc::iovec {
                                iov_base: std::ptr::null_mut(),
                                iov_len: 50,
                            }];
                            let guards = [const { None }; crate::buffer::send_slab::MAX_GUARDS];
                            if let Some((slab_idx, _)) = el.driver.send_slab.allocate(
                                ci, &iovecs, u16::MAX, guards, 0, 50,
                            ) {
                                let ud = UserData::encode(
                                    OpTag::SendMsgZc, ci, slab_idx as u32,
                                );
                                el.test_dispatch_cqe(ud.raw(), -104, 0);
                                // Release if should_release.
                                if el.driver.send_slab.in_use(slab_idx)
                                    && el.driver.send_slab.should_release(slab_idx)
                                {
                                    el.driver.send_slab.release(slab_idx);
                                }
                            }
                        }

                        // Recv EOF — closes the connection.
                        5 if !live_conns.is_empty() => {
                            let ci = live_conns.remove(0);
                            let ud = UserData::encode(OpTag::RecvMulti, ci, 0);
                            el.test_dispatch_cqe(ud.raw(), 0, 0);
                            // Simulate Close CQE.
                            let close_ud = UserData::encode(OpTag::Close, ci, 0);
                            el.test_dispatch_cqe(close_ud.raw(), 0, 0);
                        }

                        // Recv error.
                        6 if !live_conns.is_empty() => {
                            let ci = live_conns[0];
                            let ud = UserData::encode(OpTag::RecvMulti, ci, 0);
                            el.test_dispatch_cqe(ud.raw(), -105, 0); // ENOBUFS
                        }

                        // Send + Recv EOF in same batch (the cross-CQE bug pattern).
                        7 if !live_conns.is_empty() => {
                            let ci = live_conns.remove(0);
                            if let Some((slot, _, _)) = el.driver.send_copy_pool.copy_in(b"data") {
                                pool_slots_in_flight.push(slot);
                                let send_ud = UserData::encode(OpTag::Send, ci, slot as u32);
                                let recv_ud = UserData::encode(OpTag::RecvMulti, ci, 0);
                                // EOF first, then stale send — the bug pattern.
                                el.test_dispatch_cqe(recv_ud.raw(), 0, 0);
                                el.test_dispatch_cqe(send_ud.raw(), 4, 0);
                                // Close CQE.
                                let close_ud = UserData::encode(OpTag::Close, ci, 0);
                                el.test_dispatch_cqe(close_ud.raw(), 0, 0);
                            } else {
                                live_conns.insert(0, ci); // put it back
                            }
                        }

                        // Close a live connection directly.
                        8 if !live_conns.is_empty() => {
                            let ci = live_conns.remove(0);
                            el.driver.close_connection(ci);
                            let close_ud = UserData::encode(OpTag::Close, ci, 0);
                            el.test_dispatch_cqe(close_ud.raw(), 0, 0);
                        }

                        // No-op (or action on empty conn list).
                        _ => {}
                    }
                }

                // Clean up remaining live connections.
                for ci in &live_conns {
                    el.driver.close_connection(*ci);
                    let close_ud = UserData::encode(OpTag::Close, *ci, 0);
                    el.test_dispatch_cqe(close_ud.raw(), 0, 0);
                }

                // Invariants: no resource leaks.
                prop_assert_eq!(
                    el.driver.send_copy_pool.free_count(),
                    initial_pool_free,
                    "pool slot leak after mixed operations"
                );
                prop_assert_eq!(
                    el.driver.send_slab.free_count(),
                    initial_slab_free,
                    "slab entry leak after mixed operations"
                );
            }
        }
    }
}
