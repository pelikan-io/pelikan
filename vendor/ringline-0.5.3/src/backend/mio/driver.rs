//! Mio backend driver — owns per-worker I/O state.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::accumulator::AccumulatorTable;
use crate::buffer::send_copy::SendCopyPool;
use crate::config::Config;
use crate::connection::{ConnectionTable, RecvMode};
use crate::disk_io_pool::DiskIoPool;
use crate::handler::{ConnSendState, DriverCtx};

use mio::Interest;

/// mio token 0 is reserved for the wake pipe.
pub(crate) const WAKE_TOKEN: mio::Token = mio::Token(0);

/// Per-connection pending send: `(data, offset, notify_len)` for partial
/// writes. `notify_len` is `Some(len)` for awaitable sends: the completion
/// (wake_send) is delivered only when the entry has fully reached the
/// socket — completing at queue time reported success for bytes that were
/// never written and swallowed write errors entirely.
pub(crate) type PendingSend = (Vec<u8>, usize, Option<u32>);

/// Per-worker mio driver state.
pub(crate) struct Driver {
    pub(crate) connections: ConnectionTable,
    pub(crate) accumulators: AccumulatorTable,
    pub(crate) send_copy_pool: SendCopyPool,
    pub(crate) send_queues: Vec<ConnSendState>,
    pub(crate) accept_rx: Option<crossbeam_channel::Receiver<(RawFd, SocketAddr)>>,
    pub(crate) wake_handle: crate::wakeup::WakeFd,
    pub(crate) shutdown_flag: Arc<AtomicBool>,
    pub(crate) shutdown_local: bool,
    pub(crate) tls_table: Option<crate::tls::TlsTable>,
    pub(crate) connect_addrs: Vec<libc::sockaddr_storage>,
    /// Per-connection mio tokens -> connection index mapping.
    pub(crate) poll: mio::Poll,
    pub(crate) events: mio::Events,
    /// Resolver response channels.
    pub(crate) resolve_rx: Option<crossbeam_channel::Receiver<crate::resolver::ResolveResponse>>,
    pub(crate) resolve_tx: Option<crossbeam_channel::Sender<crate::resolver::ResolveResponse>>,
    pub(crate) resolver: Option<Arc<crate::resolver::ResolverPool>>,
    /// Spawner response channels.
    pub(crate) spawn_rx: Option<crossbeam_channel::Receiver<crate::spawner::SpawnResponse>>,
    pub(crate) spawn_tx: Option<crossbeam_channel::Sender<crate::spawner::SpawnResponse>>,
    pub(crate) spawner: Option<Arc<crate::spawner::SpawnerPool>>,
    /// Blocking pool channels.
    pub(crate) blocking_rx: Option<crossbeam_channel::Receiver<crate::blocking::BlockingResponse>>,
    pub(crate) blocking_tx: Option<crossbeam_channel::Sender<crate::blocking::BlockingResponse>>,
    pub(crate) blocking_pool: Option<Arc<crate::blocking::BlockingPool>>,

    // ── mio-specific state ───────────────────────────────────────────
    /// Per-connection mio TcpStream storage.
    pub(crate) tcp_streams: Vec<Option<mio::net::TcpStream>>,
    /// Per-connection pending send buffers: `VecDeque<(data, offset)>`.
    /// Populated by DriverCtx::send(), drained by the event loop on writable.
    pub(crate) pending_sends: Vec<VecDeque<PendingSend>>,
    /// Connection indices with non-empty `pending_sends`, so the per-loop
    /// flush pass touches only dirty connections instead of scanning all
    /// slots. Invariant: `pending_sends[i]` non-empty ⇒ `sends_dirty_flag[i]`
    /// set (and `i` present in `sends_dirty`).
    pub(crate) sends_dirty: Vec<u32>,
    pub(crate) sends_dirty_flag: Vec<bool>,
    /// Same shape for `send_completions`.
    pub(crate) completions_dirty: Vec<u32>,
    pub(crate) completions_dirty_flag: Vec<bool>,
    /// Number of connections with an armed connect deadline — lets the
    /// per-loop timeout sweep skip the scan entirely in the common case.
    pub(crate) connect_pending: u32,
    /// Per-connection writable flag (most recent readiness from mio).
    pub(crate) writable: Vec<bool>,
    /// Per-connection connect timeout deadline (None if no timeout or not connecting).
    pub(crate) connect_deadlines: Vec<Option<std::time::Instant>>,
    /// Raw fd of the wake pipe read end — registered with mio as WAKE_TOKEN.
    pub(crate) wake_pipe_fd: RawFd,
    /// Whether to set TCP_NODELAY on accepted connections.
    pub(crate) tcp_nodelay: bool,
    /// Connections closed this iteration, awaiting executor cleanup and
    /// slot release by the event loop's `drain_pending_closes`. Deferring
    /// the release (a) lets `Executor::remove_connection` run (stale parked
    /// futures, waiter flags, and recv sinks used to survive into the
    /// slot's next occupant — a use-after-free via the recv-sink raw
    /// pointer), and (b) closes the reuse window between a task closing a
    /// connection and its own post-poll cleanup.
    pub(crate) pending_closes: Vec<u32>,
    /// Per-connection queue of awaitable-send byte counts.
    /// `DriverCtx::send_await()` pushes len here; the event loop drains
    /// these and calls `Executor::wake_send()` for each.
    pub(crate) send_completions: Vec<VecDeque<u32>>,
    /// Bound UDP sockets (one per `config.udp_bind` address).
    pub(crate) udp_sockets: Vec<mio::net::UdpSocket>,
    /// Whether UDP GRO was requested; when set, the readable handler uses
    /// `recvmsg` with a control buffer to read the `UDP_GRO` segment size.
    /// Only consulted on Linux (GRO is a Linux feature).
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) udp_gro: bool,
    /// First mio token used for UDP sockets. UDP socket `i` has token
    /// `udp_token_base + i`. Tokens below this are WAKE_TOKEN (0) and
    /// TCP connections (1..=max_connections).
    pub(crate) udp_token_base: usize,

    // ── Disk I/O pool state ─────────���───────────────────────────────
    /// Disk I/O response channel (worker-local receive end).
    pub(crate) disk_io_rx: Option<crossbeam_channel::Receiver<crate::disk_io_pool::DiskIoResponse>>,
    /// Disk I/O response channel (worker-local send end, passed into requests).
    pub(crate) disk_io_tx: Option<crossbeam_channel::Sender<crate::disk_io_pool::DiskIoResponse>>,
    /// Shared disk I/O pool.
    pub(crate) disk_io_pool: Option<Arc<DiskIoPool>>,
    /// Monotonic sequence counter for disk I/O requests.
    pub(crate) next_disk_io_seq: u32,

    // ── Direct I/O file management ──────────��───────────────────────
    /// Direct I/O file table (allocates file slots, tracks raw fds).
    pub(crate) direct_io_files: Option<crate::direct_io::DirectIoFileTable>,
    /// Raw fds for direct I/O files, indexed by file slot.
    pub(crate) direct_io_fds: Vec<Option<RawFd>>,

    // ── Filesystem file management ──────────────────────────────────
    /// Filesystem file table (allocates file slots, tracks raw fds).
    pub(crate) fs_files: Option<crate::fs::FsFileTable>,
    /// Raw fds for filesystem files, indexed by file slot.
    pub(crate) fs_fds: Vec<Option<RawFd>>,
    /// Pending fs_open requests: maps seq → file_index. On completion, the
    /// result (fd) is stored in `fs_fds[file_index]`. On failure, the file
    /// slot is released.
    pub(crate) pending_fs_opens: std::collections::HashMap<u32, u16>,
}

impl Driver {
    /// Create a new mio-backed driver.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: &Config,
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
        disk_io_rx: Option<crossbeam_channel::Receiver<crate::disk_io_pool::DiskIoResponse>>,
        disk_io_tx: Option<crossbeam_channel::Sender<crate::disk_io_pool::DiskIoResponse>>,
        disk_io_pool: Option<Arc<DiskIoPool>>,
    ) -> io::Result<Self> {
        let max_conn = config.max_connections as usize;
        let poll = mio::Poll::new()?;
        let events = mio::Events::with_capacity(1024);

        let tls_table = {
            let server_config = config.tls.as_ref().map(|t| t.server_config.clone());
            let client_config = config.tls_client.as_ref().map(|t| t.client_config.clone());
            if server_config.is_some() || client_config.is_some() {
                Some(crate::tls::TlsTable::new(
                    config.max_connections,
                    server_config,
                    client_config,
                ))
            } else {
                None
            }
        };

        // UDP token range starts after WAKE_TOKEN (0) and TCP connections
        // (1..=max_connections).
        let udp_token_base = max_conn + 2;

        // Bind UDP sockets and register with mio poll.
        //
        // We can't use `std::net::UdpSocket::bind` here because it binds
        // before we get a chance to set `SO_REUSEPORT`, which the io_uring
        // backend already enables and which is required for multi-worker
        // setups (each worker creates its own socket bound to the same
        // address).
        let mut udp_sockets = Vec::with_capacity(config.udp_bind.len());
        for (i, addr) in config.udp_bind.iter().enumerate() {
            let std_socket = bind_udp_with_reuseport(*addr, config.udp_gro)
                .map_err(|e| io::Error::new(e.kind(), format!("UDP bind {addr}: {e}")))?;
            std_socket.set_nonblocking(true)?;
            let mut mio_socket = mio::net::UdpSocket::from_std(std_socket);
            poll.registry().register(
                &mut mio_socket,
                mio::Token(udp_token_base + i),
                Interest::READABLE,
            )?;
            udp_sockets.push(mio_socket);
        }

        Ok(Driver {
            connections: ConnectionTable::new(config.max_connections),
            accumulators: AccumulatorTable::new_with_max(
                config.max_connections,
                config.recv_buffer.buffer_size as usize,
                config.recv_accumulator_max,
            ),
            send_copy_pool: SendCopyPool::new(config.send_copy_count, config.send_copy_slot_size),
            send_queues: (0..max_conn).map(|_| ConnSendState::new()).collect(),
            accept_rx,
            // The pipe's WRITE end: handed to worker-pool threads (disk I/O,
            // resolver, spawner) so their completion wakes actually reach
            // this worker. `eventfd` is the READ end (polled by the loop) —
            // it used to be wrapped here, making every pool wake an EBADF
            // no-op observed only at the poll timeout (~10 ms).
            wake_handle: wake_fd,
            shutdown_flag,
            shutdown_local: false,
            tls_table,
            connect_addrs: vec![unsafe { std::mem::zeroed() }; max_conn],
            poll,
            events,
            resolve_rx,
            resolve_tx,
            resolver,
            spawn_rx,
            spawn_tx,
            spawner,
            blocking_rx,
            blocking_tx,
            blocking_pool,
            tcp_streams: (0..max_conn).map(|_| None).collect(),
            pending_closes: Vec::new(),
            pending_sends: (0..max_conn).map(|_| VecDeque::new()).collect(),
            sends_dirty: Vec::new(),
            sends_dirty_flag: vec![false; max_conn],
            completions_dirty: Vec::new(),
            completions_dirty_flag: vec![false; max_conn],
            connect_pending: 0,
            writable: vec![false; max_conn],
            connect_deadlines: vec![None; max_conn],
            wake_pipe_fd: eventfd,
            tcp_nodelay: config.tcp_nodelay,
            send_completions: (0..max_conn).map(|_| VecDeque::new()).collect(),
            udp_sockets,
            udp_gro: config.udp_gro,
            udp_token_base,
            disk_io_rx,
            disk_io_tx,
            disk_io_pool,
            next_disk_io_seq: 0,
            direct_io_files: config
                .direct_io
                .as_ref()
                .map(|dio| crate::direct_io::DirectIoFileTable::new(dio.max_files)),
            direct_io_fds: config
                .direct_io
                .as_ref()
                .map(|dio| vec![None; dio.max_files as usize])
                .unwrap_or_default(),
            fs_files: config
                .fs
                .as_ref()
                .map(|fs| crate::fs::FsFileTable::new(fs.max_files)),
            fs_fds: config
                .fs
                .as_ref()
                .map(|fs| vec![None; fs.max_files as usize])
                .unwrap_or_default(),
            pending_fs_opens: std::collections::HashMap::new(),
        })
    }

    /// Create a `DriverCtx` borrow for issuing operations.
    pub(crate) fn make_ctx(&mut self) -> DriverCtx<'_> {
        let tls_ptr = self
            .tls_table
            .as_mut()
            .map(|t| t as *mut _)
            .unwrap_or(std::ptr::null_mut());

        DriverCtx {
            connections: &mut self.connections,
            send_copy_pool: &mut self.send_copy_pool,
            tls_table: tls_ptr,
            shutdown_requested: &mut self.shutdown_local,
            connect_addrs: &mut self.connect_addrs,
            tcp_nodelay: self.tcp_nodelay,
            #[cfg(feature = "timestamps")]
            timestamps: false,
            #[cfg(feature = "timestamps")]
            recvmsg_msghdr: std::ptr::null(),
            send_queues: &mut self.send_queues,
            pending_sends: &mut self.pending_sends,
            sends_dirty: &mut self.sends_dirty,
            sends_dirty_flag: &mut self.sends_dirty_flag,
            completions_dirty: &mut self.completions_dirty,
            completions_dirty_flag: &mut self.completions_dirty_flag,
            connect_pending: &mut self.connect_pending,
            pending_closes: &mut self.pending_closes,
            tcp_streams: &mut self.tcp_streams,
            poll: &mut self.poll,
            writable: &mut self.writable,
            send_completions: &mut self.send_completions,
            connect_deadlines: &mut self.connect_deadlines,
            disk_io_pool: &self.disk_io_pool,
            disk_io_tx: &self.disk_io_tx,
            wake_handle: self.wake_handle,
            next_disk_io_seq: &mut self.next_disk_io_seq,
            direct_io_files: &mut self.direct_io_files,
            direct_io_fds: &mut self.direct_io_fds,
            fs_files: &mut self.fs_files,
            fs_fds: &mut self.fs_fds,
            pending_fs_opens: &mut self.pending_fs_opens,
        }
    }

    /// Close and clean up a connection.
    pub(crate) fn close_connection(&mut self, conn_index: u32) {
        let idx = conn_index as usize;

        // Check that the connection is active and not already closing.
        if let Some(conn) = self.connections.get_mut(conn_index) {
            if matches!(conn.recv_mode, RecvMode::Closed) {
                return; // already closing
            }
            conn.recv_mode = RecvMode::Closed;
        } else {
            return;
        }
        let _ = idx;
        // Teardown (socket, buffers, executor state, slot release) happens
        // in the event loop's drain_pending_closes, which has Executor
        // access. Marking Closed above makes this idempotent.
        self.pending_closes.push(conn_index);
    }

    /// Tear down a closed connection's driver-side state: best-effort
    /// nonblocking flush of pending sends, TLS close_notify, socket
    /// deregistration, buffer cleanup, and slot release. Called by the
    /// event loop after `Executor::remove_connection`.
    ///
    /// The flush is a single nonblocking attempt — the previous behavior
    /// flipped the fd to blocking and `write_all`'d, which let one
    /// zero-window peer stall the entire worker indefinitely.
    pub(crate) fn finish_close(&mut self, conn_index: u32) {
        let idx = conn_index as usize;

        let _ = self.flush_sends(conn_index);

        if let Some(ref mut stream) = self.tcp_streams[idx] {
            use std::io::Write;
            // Send TLS close_notify if this is a TLS connection
            // (best-effort, nonblocking).
            if let Some(ref mut tls_table) = self.tls_table
                && tls_table.has(conn_index)
            {
                if let Some(tls_conn) = tls_table.get_mut(conn_index) {
                    tls_conn.conn.send_close_notify();
                }
                crate::tls::flush_tls_output_mio_direct(tls_table, stream, conn_index);
                tls_table.remove(conn_index);
            }
            let _ = stream.flush();
        }

        // Deregister from poll and drop the TcpStream.
        if let Some(mut stream) = self.tcp_streams[idx].take() {
            let _ = self.poll.registry().deregister(&mut stream);
            // stream is dropped here, closing the fd
        }

        let was_established = self
            .connections
            .get(conn_index)
            .map(|c| c.established)
            .unwrap_or(false);

        self.pending_sends[idx].clear();
        self.writable[idx] = false;
        if self.connect_deadlines[idx].take().is_some() {
            self.connect_pending -= 1;
        }
        self.send_completions[idx].clear();
        self.accumulators.reset(conn_index);

        self.send_queues[idx].queue.clear();
        self.send_queues[idx].in_flight = false;

        if self.connections.get(conn_index).is_some() {
            self.connections.release(conn_index);
        }

        crate::metrics::CONNECTIONS.increment(crate::metrics::conn::CLOSED);
        // Only decrement the active gauge for connections that were counted
        // into it — failed connects and TLS-handshake failures never
        // incremented, so unconditional decrement underflowed the gauge.
        if was_established {
            crate::metrics::CONNECTIONS_ACTIVE.decrement();
        }
    }

    /// Record `idx` in the dirty-sends list so the event loop's flush pass
    /// visits it. Invariant: non-empty `pending_sends[idx]` ⇒ flag set.
    /// Every push into `pending_sends` — including the TLS paths that push
    /// from the event loop — and every partial flush must uphold this, or
    /// the queue stalls until an unrelated writable event arrives.
    pub(crate) fn mark_send_dirty(&mut self, idx: usize) {
        if !self.sends_dirty_flag[idx] {
            self.sends_dirty_flag[idx] = true;
            self.sends_dirty.push(idx as u32);
        }
    }

    /// Flush pending sends for a connection. Called by the event loop when
    /// the connection becomes writable.
    ///
    /// Returns `Ok((all_flushed, bytes_written))`: `all_flushed` is true if
    /// all pending data was flushed (or there was nothing to flush), false
    /// if we got WouldBlock mid-flush. A hard write error is returned as
    /// `Err` — the caller must fail the connection (the previous code
    /// swallowed it, kept the queue, and retried the failing writev every
    /// loop iteration forever while awaited sends reported success).
    ///
    /// Awaitable entries (`notify_len` set) push their completion when the
    /// entry's last byte reaches the socket.
    pub(crate) fn flush_sends(&mut self, conn_index: u32) -> io::Result<(bool, u32)> {
        use std::os::fd::AsRawFd;

        let idx = conn_index as usize;
        let stream = match self.tcp_streams[idx].as_mut() {
            Some(s) => s,
            None => return Ok((true, 0)),
        };

        let mut total_written: u32 = 0;

        // Use writev() to coalesce multiple pending sends into a single
        // syscall, reducing TCP segment count under pipelining.
        while !self.pending_sends[idx].is_empty() {
            let mut iovecs: Vec<libc::iovec> =
                Vec::with_capacity(self.pending_sends[idx].len().min(1024));
            for (data, offset, _notify) in self.pending_sends[idx].iter() {
                if iovecs.len() >= 1024 {
                    break;
                }
                let remaining = &data[*offset..];
                if !remaining.is_empty() {
                    iovecs.push(libc::iovec {
                        iov_base: remaining.as_ptr() as *mut libc::c_void,
                        iov_len: remaining.len(),
                    });
                }
            }

            if iovecs.is_empty() {
                self.pending_sends[idx].clear();
                break;
            }

            let fd = stream.as_raw_fd();
            let result = unsafe { libc::writev(fd, iovecs.as_ptr(), iovecs.len() as i32) };

            if result < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    self.writable[idx] = false;
                    // Queue survives this flush — keep the dirty invariant.
                    self.mark_send_dirty(idx);
                    return Ok((false, total_written));
                }
                return Err(err);
            }
            if result == 0 {
                // Connection closed by peer.
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "peer closed during send flush",
                ));
            }

            // Advance through the pending sends by the number of bytes written.
            let mut remaining = result as usize;
            total_written += result as u32;
            while remaining > 0 {
                if let Some((data, offset, notify)) = self.pending_sends[idx].front_mut() {
                    let avail = data.len() - *offset;
                    if remaining >= avail {
                        remaining -= avail;
                        if let Some(len) = notify.take() {
                            self.send_completions[idx].push_back(len);
                            if !self.completions_dirty_flag[idx] {
                                self.completions_dirty_flag[idx] = true;
                                self.completions_dirty.push(idx as u32);
                            }
                        }
                        self.pending_sends[idx].pop_front();
                    } else {
                        *offset += remaining;
                        remaining = 0;
                    }
                } else {
                    break;
                }
            }
        }

        // All sends flushed. Switch back to read-only interest.
        if let Some(stream) = self.tcp_streams[idx].as_mut() {
            let _ = self.poll.registry().reregister(
                stream,
                mio::Token(idx + 1),
                mio::Interest::READABLE,
            );
        }
        Ok((true, total_written))
    }

    /// Register writable interest for a connection (because we have
    /// pending send data).
    pub(crate) fn register_writable(&mut self, conn_index: u32) {
        let idx = conn_index as usize;
        if let Some(stream) = self.tcp_streams[idx].as_mut() {
            let _ = self.poll.registry().reregister(
                stream,
                mio::Token(idx + 1),
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            );
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        // Close the wake pipe's read end. The write end is held by
        // `WakeHandle` clones that may live longer than the worker;
        // those are closed by `ShutdownHandle::Drop`.
        unsafe {
            libc::close(self.wake_pipe_fd);
        }
    }
}

/// Create and bind a UDP socket with `SO_REUSEPORT` enabled, returning a
/// `std::net::UdpSocket`.
///
/// `std::net::UdpSocket::bind` binds before any setsockopt can run, so it
/// can't be used here — multi-worker setups bind every worker to the same
/// port and need `SO_REUSEPORT` set before bind.
fn bind_udp_with_reuseport(addr: SocketAddr, udp_gro: bool) -> io::Result<std::net::UdpSocket> {
    use std::os::fd::FromRawFd;

    let domain = if addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    // `SOCK_CLOEXEC` is Linux-only; on macOS/BSD we set FD_CLOEXEC via fcntl
    // after the socket is created, mirroring the pattern in `acceptor.rs`.
    #[cfg(target_os = "linux")]
    let sock_type = libc::SOCK_DGRAM | libc::SOCK_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let sock_type = libc::SOCK_DGRAM;
    let fd = unsafe { libc::socket(domain, sock_type, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    #[cfg(not(target_os = "linux"))]
    unsafe {
        let fd_flags = libc::fcntl(fd, libc::F_GETFD);
        if fd_flags < 0 || libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) < 0 {
            let err = io::Error::last_os_error();
            libc::close(fd);
            return Err(err);
        }
    }

    let optval: libc::c_int = 1;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        let err = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(err);
    }

    // Enable UDP GRO (opt-in → hard-fail, mirroring the io_uring backend).
    // GRO is Linux-only; on other platforms `udp_gro` is a no-op.
    #[cfg(target_os = "linux")]
    if udp_gro {
        let on: libc::c_int = 1;
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_UDP,
                crate::backend::udp_gro::UDP_GRO,
                &on as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            let err = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(err);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = udp_gro;

    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let addr_len = crate::backend::socket_addr_to_sockaddr(addr, &mut storage);
    let rc = unsafe { libc::bind(fd, &storage as *const _ as *const libc::sockaddr, addr_len) };
    if rc < 0 {
        let err = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(err);
    }

    Ok(unsafe { std::net::UdpSocket::from_raw_fd(fd) })
}
