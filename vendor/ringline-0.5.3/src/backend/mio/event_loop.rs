//! Mio backend event loop — readiness-based I/O dispatch.

use std::io;
use std::io::Read;
use std::net::SocketAddr;
use std::os::fd::{FromRawFd, RawFd};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Context;
use std::time::{Duration, Instant};

use crate::backend::Driver;
use crate::config::Config;
use crate::connection::RecvMode;
use crate::metrics;
use crate::runtime::handler::AsyncEventHandler;
use crate::runtime::io::{ConnCtx, DriverState, UdpCtx, set_driver_state_guarded};
use crate::runtime::waker::{STANDALONE_BIT, conn_waker, standalone_waker};
use crate::runtime::{CURRENT_TASK_ID, Executor};

use super::driver::WAKE_TOKEN;

/// Mio-based event loop (one per worker thread).
pub(crate) struct AsyncEventLoop<A: AsyncEventHandler> {
    driver: Driver,
    handler: A,
    executor: Executor,
}

impl<A: AsyncEventHandler> AsyncEventLoop<A> {
    /// Create a new mio event loop.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: &Config,
        handler: A,
        accept_rx: Option<crossbeam_channel::Receiver<(RawFd, SocketAddr)>>,
        eventfd: RawFd,
        wake_fd: crate::wakeup::WakeFd,
        shutdown_flag: Arc<AtomicBool>,
        resolve_rx: Option<crossbeam_channel::Receiver<crate::resolver::ResolveResponse>>,
        resolve_tx: Option<crossbeam_channel::Sender<crate::resolver::ResolveResponse>>,
        resolver: Option<Arc<crate::resolver::ResolverPool>>,
        spawn_rx: Option<crossbeam_channel::Receiver<crate::spawner::SpawnResponse>>,
        spawn_tx: Option<crossbeam_channel::Sender<crate::spawner::SpawnResponse>>,
        spawner: Option<Arc<crate::spawner::SpawnerPool>>,
        blocking_rx: Option<crossbeam_channel::Receiver<crate::blocking::BlockingResponse>>,
        blocking_tx: Option<crossbeam_channel::Sender<crate::blocking::BlockingResponse>>,
        blocking_pool: Option<Arc<crate::blocking::BlockingPool>>,
    ) -> io::Result<Self> {
        // Create per-worker disk I/O pool and channels if configured.
        // Each worker gets its own pool instance (lightweight — just thread
        // handles) and its own channel pair. This avoids changing the
        // launch_inner / worker_fn signature.
        let (disk_io_rx, disk_io_tx, disk_io_pool) = if config.disk_io_threads > 0
            && (config.direct_io.is_some() || config.fs.is_some())
        {
            let pool = Arc::new(crate::disk_io_pool::DiskIoPool::start(
                config.disk_io_threads,
            ));
            let (tx, rx) = crossbeam_channel::unbounded::<crate::disk_io_pool::DiskIoResponse>();
            (Some(rx), Some(tx), Some(pool))
        } else {
            (None, None, None)
        };

        let driver = Driver::new(
            config,
            accept_rx,
            eventfd,
            wake_fd,
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
            disk_io_rx,
            disk_io_tx,
            disk_io_pool,
        )?;

        let executor = Executor::new(
            config.max_connections,
            config.standalone_task_capacity,
            config.timer_slots,
            config.udp_bind.len() as u32,
            config.udp_recv_queue_capacity,
        );

        Ok(AsyncEventLoop {
            driver,
            handler,
            executor,
        })
    }

    /// Complete the fallible backend setup known before the runtime is ready.
    ///
    /// `run()` can still return an error after the listener becomes live.
    pub(crate) fn prepare_run(&mut self) -> Result<(), crate::error::Error> {
        // Register the wake pipe read-end with mio Poll.
        self.driver.poll.registry().register(
            &mut mio::unix::SourceFd(&self.driver.wake_pipe_fd),
            WAKE_TOKEN,
            mio::Interest::READABLE,
        )?;

        Ok(())
    }

    /// Run the mio event loop until shutdown.
    pub(crate) fn run(&mut self) -> Result<(), crate::error::Error> {
        // Spawn UDP handler tasks for each bound UDP socket.
        for udp_index in 0..self.driver.udp_sockets.len() {
            let udp_ctx = UdpCtx {
                udp_index: udp_index as u32,
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

        // Recv buffer for reading from sockets.
        let mut recv_buf = vec![0u8; 8192];

        loop {
            // 1. Fire expired timers.
            self.fire_expired_timers();

            // 2. Compute poll timeout from nearest timer deadline. Don't
            // block at all while tasks are already runnable (self-wakes
            // collected after the last poll pass, tasks woken from on_tick).
            self.executor.collect_wakeups();
            let timeout = if self.executor.ready_queue.is_empty() {
                self.compute_poll_timeout()
            } else {
                Duration::ZERO
            };

            // 3. Poll for I/O events.
            match self
                .driver
                .poll
                .poll(&mut self.driver.events, Some(timeout))
            {
                Ok(()) => {}
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(crate::error::Error::Io(e)),
            }

            // 4. Handle events.
            // Collect events into a temporary vec to avoid borrow conflict
            // (self.driver.events borrows driver, but handlers need &mut driver).
            let mut event_list: Vec<(mio::Token, bool, bool, bool)> =
                Vec::with_capacity(self.driver.events.iter().count());
            for event in self.driver.events.iter() {
                let is_err = event.is_error() || event.is_read_closed() || event.is_write_closed();
                event_list.push((
                    event.token(),
                    event.is_readable(),
                    event.is_writable(),
                    is_err,
                ));
            }

            for (token, readable, writable, is_err) in event_list {
                match token {
                    WAKE_TOKEN => {
                        if readable {
                            self.drain_wake_pipe();
                        }
                    }
                    tok if tok.0 >= self.driver.udp_token_base
                        && tok.0 < self.driver.udp_token_base + self.driver.udp_sockets.len() =>
                    {
                        if readable {
                            let udp_index = (tok.0 - self.driver.udp_token_base) as u32;
                            self.handle_udp_readable(udp_index);
                        }
                    }
                    tok => {
                        let conn_index = (tok.0 - 1) as u32;
                        let connecting = self
                            .driver
                            .connections
                            .get(conn_index)
                            .is_some_and(|cs| matches!(cs.recv_mode, RecvMode::Connecting));
                        if connecting {
                            // Writable (or error — handle_writable reads
                            // SO_ERROR) resolves the connect FIRST. Handling
                            // readable first let a server-speaks-first
                            // greeting land in the accumulator only to race
                            // connect bookkeeping, and a FIN in the same
                            // batch marked the conn Closed so the Connecting
                            // check failed and wake_connect never fired —
                            // connect().await hung forever (edge-triggered
                            // mio never re-delivers).
                            if writable || is_err {
                                self.handle_writable(conn_index);
                            }
                            if readable {
                                self.handle_readable(conn_index, &mut recv_buf);
                            }
                            continue;
                        }
                        if readable {
                            self.handle_readable(conn_index, &mut recv_buf);
                        }
                        if writable {
                            self.handle_writable(conn_index);
                        }
                    }
                }
            }

            // 5. Drain cross-thread channels unconditionally (not just on wake
            //    events). On macOS/kqueue, SourceFd edge-triggered semantics can
            //    miss pipe writes that arrive between reregister and poll. The
            //    try_recv calls are cheap — O(1) when empty.
            self.drain_channels();

            // 6. Collect wakeups and poll ready tasks.
            self.executor.collect_wakeups();
            self.poll_ready_tasks();

            // 6a. Flush pending sends queued during task polling, then
            // deliver the completions that flushing produced (completions
            // are recorded when bytes reach the socket, so flush must run
            // first or every awaited send waits an extra iteration).
            self.flush_all_pending_sends();
            self.drain_send_completions();

            // 6b. Finish teardown of connections closed during this
            // iteration: executor cleanup (parked futures, waiter flags,
            // recv sinks) before the slot is released for reuse.
            self.drain_pending_closes();

            // 7. on_tick callback (synchronous). Set the executor's
            // driver_state thread-local so user code that calls
            // `ringline::spawn()` / wakers / `with_state` works from
            // inside the handler. Raw pointers dodge the borrow conflict
            // with `make_ctx()`.
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
                        handler.on_tick(&mut ctx);
                    }));
                    if result.is_err() {
                        eprintln!("ringline: handler on_tick panicked; continuing");
                    }
                }
                drop(guard);
            }

            // 8. Flush any sends queued by on_tick, deliver their
            // completions, and finish any closes it triggered.
            self.flush_all_pending_sends();
            self.drain_send_completions();
            self.drain_pending_closes();

            // 9. Check shutdown.
            if self.driver.shutdown_local || self.driver.shutdown_flag.load(Ordering::Relaxed) {
                return Ok(());
            }
        }
    }

    /// Drain the wake pipe and re-register for the next event.
    fn drain_wake_pipe(&mut self) {
        let mut drain_buf = [0u8; 256];
        loop {
            let result = unsafe {
                libc::read(
                    self.driver.wake_pipe_fd,
                    drain_buf.as_mut_ptr() as *mut libc::c_void,
                    drain_buf.len(),
                )
            };
            if result <= 0 {
                break;
            }
        }
        // Re-register so we get notified again (kqueue consumes the registration).
        let _ = self.driver.poll.registry().reregister(
            &mut mio::unix::SourceFd(&self.driver.wake_pipe_fd),
            WAKE_TOKEN,
            mio::Interest::READABLE,
        );
    }

    /// Drain all cross-thread channels: accept, resolve, spawn, blocking.
    ///
    /// Called unconditionally on every event loop iteration (not just on wake
    /// pipe events) to avoid missed wakeups on macOS/kqueue.
    fn drain_channels(&mut self) {
        // Drain accept channel (server mode).
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

            // Set peer address.
            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                cs.peer_addr = Some(crate::connection::PeerAddr::Tcp(peer_addr));
            }

            // Convert raw fd to mio TcpStream.
            let std_stream = unsafe { std::net::TcpStream::from_raw_fd(raw_fd) };
            std_stream.set_nonblocking(true).ok();
            if self.driver.tcp_nodelay {
                std_stream.set_nodelay(true).ok();
            }
            let mut mio_stream = mio::net::TcpStream::from_std(std_stream);

            // Register with poll for READABLE interest.
            let mio_token = mio::Token(conn_index as usize + 1);
            if self
                .driver
                .poll
                .registry()
                .register(&mut mio_stream, mio_token, mio::Interest::READABLE)
                .is_err()
            {
                self.driver.connections.release(conn_index);
                continue;
            }

            let idx = conn_index as usize;
            self.driver.tcp_streams[idx] = Some(mio_stream);
            self.driver.accumulators.reset(conn_index);
            self.driver.pending_sends[idx].clear();
            self.driver.writable[idx] = false;

            // TLS path: defer accept until handshake completes in handle_readable.
            if let Some(ref mut tls_table) = self.driver.tls_table
                && tls_table.has_server_config()
            {
                if tls_table.create(conn_index).is_err() {
                    self.driver.close_connection(conn_index);
                }
                continue;
            }

            // Plaintext path: mark connection as established and spawn accept task.
            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                cs.established = true;
            }

            metrics::CONNECTIONS.increment(metrics::conn::ACCEPTED);
            metrics::CONNECTIONS_ACTIVE.increment();

            // Spawn async accept task.
            self.spawn_accept_task(conn_index);
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

        // Drain disk I/O responses.
        if let Some(ref rx) = self.driver.disk_io_rx {
            while let Ok(response) = rx.try_recv() {
                // Handle fs_open completions: install fd or release slot.
                if let Some(file_index) = self.driver.pending_fs_opens.remove(&response.seq) {
                    if response.result >= 0 {
                        // Success — result is the fd.
                        let fd = response.result;
                        self.driver.fs_fds[file_index as usize] = Some(fd as std::os::fd::RawFd);
                        if let Some(ref mut files) = self.driver.fs_files
                            && let Some(f) = files.get_mut(file_index)
                        {
                            f.fd_index = fd as u32;
                        }
                        // Convert to success (0) for the OpenFuture.
                        self.executor.wake_disk_io(response.seq, 0);
                    } else {
                        // Failure — release the pre-allocated file slot.
                        if let Some(ref mut files) = self.driver.fs_files {
                            files.release(file_index);
                        }
                        self.executor.wake_disk_io(response.seq, response.result);
                    }
                    continue;
                }

                // If the response carries metadata (stat), store it.
                if let Some(metadata) = response.metadata {
                    self.executor.fs_stat_results.insert(response.seq, metadata);
                }
                self.executor.wake_disk_io(response.seq, response.result);
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
    }

    /// Handle a connection becoming readable: read data into accumulator.
    fn handle_readable(&mut self, conn_index: u32, recv_buf: &mut [u8]) {
        let idx = conn_index as usize;

        // Check the connection is still active.
        if self.driver.tcp_streams[idx].is_none() {
            return;
        }

        // Check if this is a TLS connection.
        let is_tls = self
            .driver
            .tls_table
            .as_ref()
            .is_some_and(|t| t.has(conn_index));

        if is_tls {
            // TLS path: read ciphertext, decrypt, put plaintext in accumulator.
            loop {
                // Take the stream out temporarily to avoid borrow conflicts
                // (feed_tls_recv_mio needs &mut tls_table, &mut accumulators,
                // &mut stream — all fields of driver).
                let mut stream = match self.driver.tcp_streams[idx].take() {
                    Some(s) => s,
                    None => return,
                };

                let n = match stream.read(recv_buf) {
                    Ok(0) => {
                        self.driver.tcp_streams[idx] = Some(stream);
                        // EOF. A FIN without the peer's close_notify is a
                        // truncation, not a clean TLS shutdown.
                        let close_notify_seen = self
                            .driver
                            .tls_table
                            .as_mut()
                            .and_then(|t| t.get_mut(conn_index))
                            .map(|tc| tc.peer_sent_close_notify)
                            .unwrap_or(true);
                        if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                            cs.recv_mode = RecvMode::Closed;
                            if !close_notify_seen {
                                cs.eof_truncated = true;
                            }
                        }
                        self.executor.wake_recv(conn_index);
                        break;
                    }
                    Ok(n) => n,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        self.driver.tcp_streams[idx] = Some(stream);
                        break;
                    }
                    Err(_) => {
                        self.driver.tcp_streams[idx] = Some(stream);
                        if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                            cs.recv_mode = RecvMode::Closed;
                        }
                        self.executor.wake_recv(conn_index);
                        break;
                    }
                };

                let tls_table = self.driver.tls_table.as_mut().unwrap();
                let result = crate::tls::feed_tls_recv_mio(
                    tls_table,
                    &mut self.driver.accumulators,
                    &mut self.driver.pending_sends[idx],
                    conn_index,
                    &recv_buf[..n],
                );

                // Put the stream back.
                self.driver.tcp_streams[idx] = Some(stream);

                // feed_tls_recv_mio pushes handshake/ciphertext output into
                // pending_sends directly (not via DriverCtx), so uphold the
                // dirty invariant here or the flush pass never visits it.
                if !self.driver.pending_sends[idx].is_empty() {
                    self.driver.mark_send_dirty(idx);
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
                        break;
                    }
                    crate::tls::TlsRecvResult::Closed => {
                        self.executor.wake_recv(conn_index);
                        self.driver.close_connection(conn_index);
                        break;
                    }
                }
            }
            return;
        }

        // Plaintext path.
        loop {
            let stream = match self.driver.tcp_streams[idx].as_mut() {
                Some(s) => s,
                None => return,
            };

            match stream.read(recv_buf) {
                Ok(0) => {
                    // EOF — mark connection as recv-closed.
                    if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                        cs.recv_mode = RecvMode::Closed;
                    }
                    // Wake any task waiting for recv so it sees EOF.
                    self.executor.wake_recv(conn_index);
                    break;
                }
                Ok(n) => {
                    // Check if the connection has a recv sink (direct-to-buffer).
                    let sink = &mut self.executor.recv_sinks[idx];
                    if let Some(recv_sink) = sink {
                        let remaining = recv_sink.cap - recv_sink.pos;
                        let to_copy = n.min(remaining);
                        if to_copy > 0 {
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    recv_buf.as_ptr(),
                                    recv_sink.ptr.add(recv_sink.pos),
                                    to_copy,
                                );
                            }
                            recv_sink.pos += to_copy;
                        }
                        // If there's overflow beyond the sink, put it in accumulator.
                        if n > to_copy {
                            self.driver
                                .accumulators
                                .append(conn_index, &recv_buf[to_copy..n]);
                        }
                    } else {
                        self.driver.accumulators.append(conn_index, &recv_buf[..n]);
                    }
                    self.executor.wake_recv(conn_index);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(_) => {
                    // Read error — mark as closed.
                    if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                        cs.recv_mode = RecvMode::Closed;
                    }
                    self.executor.wake_recv(conn_index);
                    break;
                }
            }
        }
    }

    /// Handle a connection becoming writable: detect connect completion or flush pending sends.
    fn handle_writable(&mut self, conn_index: u32) {
        let idx = conn_index as usize;

        // Check if this is a connecting socket completing its connect.
        if let Some(cs) = self.driver.connections.get_mut(conn_index)
            && matches!(cs.recv_mode, RecvMode::Connecting)
        {
            // Connect completed — check SO_ERROR for connect failure.
            // Clear any connect timeout.
            if self.driver.connect_deadlines[idx].take().is_some() {
                self.driver.connect_pending -= 1;
            }

            let result = if let Some(ref stream) = self.driver.tcp_streams[idx] {
                match stream.take_error() {
                    Ok(Some(e)) => Err(e), // connect failed (ECONNREFUSED, etc.)
                    Ok(None) => Ok(()),    // connect succeeded
                    Err(e) => Err(e),      // getsockopt itself failed
                }
            } else {
                Err(io::Error::other("stream missing"))
            };

            if result.is_ok() {
                cs.recv_mode = RecvMode::Multi;

                // Set TCP_NODELAY if configured.
                if self.driver.tcp_nodelay
                    && let Some(ref stream) = self.driver.tcp_streams[idx]
                {
                    let _ = stream.set_nodelay(true);
                }

                // TLS client path: flush ClientHello, don't wake connect waiter
                // yet — wait for the TLS handshake to complete in handle_readable.
                if let Some(ref mut tls_table) = self.driver.tls_table
                    && tls_table.has(conn_index)
                {
                    crate::tls::flush_tls_output_mio_queued(
                        tls_table,
                        &mut self.driver.pending_sends[idx],
                        conn_index,
                    );
                    if !self.driver.pending_sends[idx].is_empty() {
                        self.driver.mark_send_dirty(idx);
                    }
                    self.driver.register_writable(conn_index);
                    let _ = self.driver.flush_sends(conn_index);
                    return;
                }

                cs.established = true;
                metrics::CONNECTIONS_ACTIVE.increment();
            }

            match result {
                Err(e) => {
                    // Connect failed — clean up the connection.
                    self.executor.wake_connect(conn_index, Err(e));
                    self.driver.close_connection(conn_index);
                }
                Ok(()) => {
                    self.executor.wake_connect(conn_index, Ok(()));
                }
            }
            return;
        }

        // Normal writable — mark writable and flush sends.
        self.driver.writable[idx] = true;
        if let Err(e) = self.driver.flush_sends(conn_index) {
            self.fail_connection_on_send_error(conn_index, e);
        }
    }

    /// A hard write error (EPIPE/ECONNRESET/peer-closed) during a flush:
    /// fail the awaiting sender, wake the recv side so the owning task
    /// observes the close, and tear the connection down. Previously the
    /// error was swallowed — the queue was retried every loop iteration
    /// forever while send().await had already reported success.
    fn fail_connection_on_send_error(&mut self, conn_index: u32, e: io::Error) {
        self.driver.pending_sends[conn_index as usize].clear();
        self.executor.wake_send(conn_index, Err(e));
        self.executor.wake_recv(conn_index);
        self.driver.close_connection(conn_index);
    }

    /// Handle a UDP socket becoming readable: drain datagrams into the
    /// executor's recv queue and wake the waiting task.
    fn handle_udp_readable(&mut self, udp_index: u32) {
        // GRO is Linux-only; elsewhere `udp_gro` is inert and we always take
        // the plain `recv_from` path below.
        #[cfg(target_os = "linux")]
        if self.driver.udp_gro {
            self.handle_udp_readable_gro(udp_index);
            return;
        }
        let idx = udp_index as usize;
        let socket = &self.driver.udp_sockets[idx];
        let mut buf = [0u8; 65536];

        loop {
            match socket.recv_from(&mut buf) {
                Ok((n, peer)) => {
                    metrics::UDP.increment(metrics::udp::DATAGRAMS_RECEIVED);
                    if self.executor.udp_recv_queues[idx].len()
                        >= self.executor.udp_recv_queue_capacity
                    {
                        // Drop on the floor — see io_uring backend for
                        // rationale.
                        metrics::UDP.increment(metrics::udp::DATAGRAMS_DROPPED);
                    } else {
                        let data = buf[..n].to_vec();
                        self.executor.udp_recv_queues[idx].push_back(
                            crate::runtime::PendingUdpDatagram {
                                peer,
                                buf: crate::runtime::PendingUdpBuf::Owned(data),
                                recv_at: std::time::Instant::now(),
                                segment_size: 0,
                                consumed: 0,
                            },
                        );
                        self.executor.wake_udp_recv(udp_index);
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    /// GRO variant of [`handle_udp_readable`]: `recvmsg` with a control
    /// buffer so the kernel can report the `UDP_GRO` segment size. The
    /// coalesced payload is stored whole; the shared drain path splits it.
    #[cfg(target_os = "linux")]
    fn handle_udp_readable_gro(&mut self, udp_index: u32) {
        use std::os::fd::AsRawFd;
        let idx = udp_index as usize;
        let fd = self.driver.udp_sockets[idx].as_raw_fd();
        // Hold a full coalesced datagram (~64 KiB) plus headroom.
        let mut buf = [0u8; 1 << 16];
        let mut control = [0u8; crate::backend::udp_gro::UDP_GRO_CMSG_LEN];

        loop {
            let mut name: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            let mut iov = libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            };
            let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
            msg.msg_name = &mut name as *mut _ as *mut libc::c_void;
            msg.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            msg.msg_iov = &mut iov;
            msg.msg_iovlen = 1;
            msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
            msg.msg_controllen = control.len() as _;

            let n = unsafe { libc::recvmsg(fd, &mut msg, 0) };
            if n < 0 {
                // EWOULDBLOCK / EAGAIN ends the drain; other errors too.
                break;
            }
            let n = n as usize;
            let peer = match crate::backend::sockaddr_to_socket_addr(&name, msg.msg_namelen) {
                Some(p) => p,
                None => continue,
            };
            metrics::UDP.increment(metrics::udp::DATAGRAMS_RECEIVED);
            if self.executor.udp_recv_queues[idx].len() >= self.executor.udp_recv_queue_capacity {
                metrics::UDP.increment(metrics::udp::DATAGRAMS_DROPPED);
                continue;
            }
            // MSG_CTRUNC means we lost the cmsg but the payload is intact —
            // treat it as a single datagram rather than dropping it.
            let segment_size = if msg.msg_flags & libc::MSG_CTRUNC == 0 && msg.msg_controllen > 0 {
                // `msg_controllen` is `size_t` on Linux (cast redundant) but
                // `socklen_t` (u32) on macOS (cast required) — allow the
                // platform-conditional cast so both clippy jobs pass.
                #[allow(clippy::unnecessary_cast)]
                let clen = msg.msg_controllen as usize;
                crate::backend::udp_gro::parse_segment_size(&control[..clen]).unwrap_or(0)
            } else {
                0
            };
            let data = buf[..n].to_vec();
            self.executor.udp_recv_queues[idx].push_back(crate::runtime::PendingUdpDatagram {
                peer,
                buf: crate::runtime::PendingUdpBuf::Owned(data),
                recv_at: std::time::Instant::now(),
                segment_size,
                consumed: 0,
            });
            self.executor.wake_udp_recv(udp_index);
        }
    }

    /// Flush pending sends for connections with buffered data. Visits only
    /// the dirty list (marked on queue) instead of scanning every slot;
    /// connections whose queue survives the flush attempt are re-marked.
    fn flush_all_pending_sends(&mut self) {
        let dirty = std::mem::take(&mut self.driver.sends_dirty);
        for conn_index in dirty {
            let idx = conn_index as usize;
            self.driver.sends_dirty_flag[idx] = false;
            if self.driver.pending_sends[idx].is_empty() {
                continue;
            }
            // Register writable interest so mio tells us when we can write.
            self.driver.register_writable(conn_index);
            // If we already know the socket is writable, try flushing now.
            if self.driver.writable[idx]
                && let Err(e) = self.driver.flush_sends(conn_index)
            {
                self.fail_connection_on_send_error(conn_index, e);
                continue;
            }
            if !self.driver.pending_sends[idx].is_empty() && !self.driver.sends_dirty_flag[idx] {
                self.driver.sends_dirty_flag[idx] = true;
                self.driver.sends_dirty.push(conn_index);
            }
        }
    }

    /// Finish teardown for connections closed since the last drain.
    /// Executor cleanup runs first — the slot must not be released (and
    /// reusable) while a stale parked future, waiter flags, or a recv-sink
    /// raw pointer still reference it.
    fn drain_pending_closes(&mut self) {
        while let Some(conn_index) = self.driver.pending_closes.pop() {
            self.executor.remove_connection(conn_index);
            self.driver.finish_close(conn_index);
        }
    }

    /// Drain per-connection send completion queues, calling wake_send for
    /// each and re-polling tasks so that each SendFuture resolves. Visits
    /// only connections marked dirty at completion-push time; a connection
    /// with results left over (single waiter slot, or no waiter yet) is
    /// re-marked for the next pass.
    fn drain_send_completions(&mut self) {
        loop {
            let mut delivered = false;
            let dirty = std::mem::take(&mut self.driver.completions_dirty);
            for conn_index in dirty {
                let idx = conn_index as usize;
                self.driver.completions_dirty_flag[idx] = false;
                if let Some(bytes) = self.driver.send_completions[idx].pop_front()
                    && self.executor.send_waiters[idx]
                {
                    self.executor.wake_send(conn_index, Ok(bytes));
                    delivered = true;
                }
                if !self.driver.send_completions[idx].is_empty()
                    && !self.driver.completions_dirty_flag[idx]
                {
                    self.driver.completions_dirty_flag[idx] = true;
                    self.driver.completions_dirty.push(conn_index);
                }
            }
            if !delivered {
                break;
            }
            // Re-poll tasks woken by the completions so they can consume
            // the results and potentially re-register waiters.
            self.executor.collect_wakeups();
            self.poll_ready_tasks();
        }
    }

    /// Fire all expired timers and push the associated tasks to the ready queue.
    fn fire_expired_timers(&mut self) {
        let now = Instant::now();
        // Heap-driven expiry: O(log n) per fired timer instead of a full
        // pool scan per loop iteration.
        while let Some((slot, generation)) = self.executor.timer_pool.pop_expired(now) {
            if let Some(waker_id) = self.executor.timer_pool.fire(slot, generation) {
                self.executor.wake_task(waker_id);
            }
        }

        // Check for timed-out connect operations. `connect_pending` counts
        // armed deadlines so the common no-outbound-connects case skips the
        // scan entirely.
        if self.driver.connect_pending > 0 {
            let mut timed_out: Vec<u32> = Vec::new();
            for (idx, deadline) in self.driver.connect_deadlines.iter().enumerate() {
                if let Some(dl) = deadline
                    && now >= *dl
                {
                    timed_out.push(idx as u32);
                }
            }
            for conn_index in timed_out {
                self.driver.connect_deadlines[conn_index as usize] = None;
                self.driver.connect_pending -= 1;
                let err = io::Error::new(io::ErrorKind::TimedOut, "connect timed out");
                self.executor.wake_connect(conn_index, Err(err));
                self.driver.close_connection(conn_index);
            }
        }
    }

    /// Compute the poll timeout from the nearest timer deadline (heap
    /// peek — no pool scan).
    fn compute_poll_timeout(&mut self) -> Duration {
        let default = Duration::from_millis(10); // default tick interval
        match self.executor.timer_pool.next_deadline() {
            Some(deadline) => deadline
                .saturating_duration_since(Instant::now())
                .min(default),
            None => default,
        }
    }

    /// Spawn an async task for a newly accepted connection.
    fn spawn_accept_task(&mut self, conn_index: u32) {
        let generation = self.driver.connections.generation(conn_index);
        let conn_ctx = ConnCtx::new(conn_index, generation);
        let future = Box::pin(self.handler.on_accept(conn_ctx));
        self.executor.owner_task[conn_index as usize] = Some(conn_index);
        self.executor.task_slab.spawn(conn_index, future);
        self.executor.ready_queue.push_back(conn_index);
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

        // Safety: We have valid mutable references to self.driver and self.executor
        // that outlive this method. Creating NonNull from &mut is safe.
        let mut driver_state = DriverState {
            driver: unsafe { NonNull::new_unchecked(driver) },
            executor: unsafe { NonNull::new_unchecked(executor) },
        };
        let driver_state_guard = unsafe { set_driver_state_guarded(&mut driver_state) };

        // Safety: we have exclusive access to driver/executor via self, and
        // only access them through these raw pointers until the guard drops.
        let driver = unsafe { &mut *driver };
        let executor = unsafe { &mut *executor };

        // Per-batch dedup: same strategy as the io_uring backend.
        // See that backend's poll_ready_tasks for the full safety argument,
        // including the initial_len boundary that prevents lost wakeups when
        // a future wakes itself during the current poll pass.

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
                            // Self-wake during poll (slot read Empty) —
                            // re-queue now that the task is parked.
                            if executor.woken_while_polling {
                                let _ = executor.wake_task(raw_id);
                            }
                        }
                        Err(_panic) => {
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

        // Reset dedup bits for the initial-batch entries only.
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
}
