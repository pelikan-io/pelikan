use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;

use crate::buffer::send_copy::SendCopyPool;
#[cfg(has_io_uring)]
use crate::buffer::send_slab::{InFlightSendSlab, MAX_GUARDS, MAX_IOVECS};
use crate::guard::GuardBox;

/// Per-connection send queue state.
///
/// Ensures at most one send SQE is in-flight per connection at a time.
/// When a send is already in-flight, subsequent sends are queued and
/// submitted immediately inside the CQE completion handler — before
/// `on_send_complete`, before returning to the event loop.
pub(crate) struct ConnSendState {
    pub in_flight: bool,
    pub queue: VecDeque<BuiltSend>,
    /// Deferred shutdown_write — submitted after the send queue drains.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub shutdown_pending: bool,
    /// Set when the application's connection task has returned while
    /// there were still queued / in-flight sends. The runtime defers
    /// the actual `Close` SQE until the serialized
    /// `submit_next_queued` cycle drains both the queue and the
    /// in-flight slot, then fires the close from
    /// `try_finalize_close`. Without the deferral, queued bytes were
    /// silently truncated when the fd closed.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub close_pending: bool,
    /// Count of queued sends pushed during close. Each CQE decrements
    /// this; when it reaches zero, `try_finalize_close` fires.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    #[allow(dead_code)]
    pub close_send_count: u32,
    /// Deadline for close_notify completion. Set when close_notify is
    /// sent via `flush_close_notify_linked`. If elapsed while
    /// `close_pending` is true, the runtime force-closes the connection.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub close_notify_deadline: Option<std::time::Instant>,
    /// Bytes acknowledged so far for the in-progress logical send.
    ///
    /// A logical send larger than one send-pool slot is split into
    /// several `BuiltSend` chunks that complete as separate CQEs, but the
    /// connection has exactly one send waiter. Waking that waiter on the
    /// first chunk's completion reports a short byte count while the
    /// remaining chunks are still queued or in flight, and consumes the
    /// waiter so their completions are dropped. Instead, each chunk's
    /// completion accumulates here; the waiter is woken exactly once, when
    /// the send queue fully drains, reporting the whole logical byte count.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub acked_bytes: u32,
}

impl ConnSendState {
    pub fn new() -> Self {
        ConnSendState {
            in_flight: false,
            queue: VecDeque::new(),
            shutdown_pending: false,
            close_pending: false,
            close_send_count: 0,
            close_notify_deadline: None,
            acked_bytes: 0,
        }
    }
}

/// Opaque connection token handed to the handler.
/// Encodes the connection index and generation for stale detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnToken {
    pub(crate) index: u32,
    pub(crate) generation: u32,
}

impl ConnToken {
    pub(crate) fn new(index: u32, generation: u32) -> Self {
        ConnToken { index, generation }
    }

    /// Returns the connection slot index. Useful for indexing into per-connection arrays.
    pub fn index(&self) -> usize {
        self.index as usize
    }
}

/// Opaque handle for a UDP socket.
///
/// Each worker that binds a UDP address gets its own socket (via `SO_REUSEPORT`).
/// The token identifies a specific UDP socket within a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpToken(pub(crate) u32);

impl UdpToken {
    /// Returns the UDP socket index within this worker.
    pub fn index(&self) -> usize {
        self.0 as usize
    }
}

// ── io_uring DriverCtx + send builders ──────────────��───────────────────
//
// The entire DriverCtx implementation, SendBuilder, SendChainBuilder, and
// ChainPartsBuilder are io_uring-specific. On the mio backend, a minimal
// DriverCtx is provided below.

#[cfg(has_io_uring)]
/// The context provided to handler callbacks for issuing operations.
///
/// This is a short-lived borrow into the driver's internal state.
pub struct DriverCtx<'a> {
    pub(crate) ring: &'a mut crate::backend::Ring,
    pub(crate) connections: &'a mut crate::connection::ConnectionTable,
    pub(crate) fixed_buffers: &'a mut crate::buffer::fixed::FixedBufferRegistry,
    pub(crate) send_copy_pool: &'a mut SendCopyPool,
    #[cfg(has_io_uring)]
    pub(crate) send_slab: &'a mut InFlightSendSlab,
    // SAFETY: Raw pointer for borrow splitting with the connection table.
    // Sound because: (1) single-threaded — DriverCtx is only created and used
    // on the worker thread that owns the Driver; (2) the pointer is derived
    // from `&mut Driver` which is live for the entire duration of any DriverCtx
    // borrow; (3) no mutable alias exists while DriverCtx holds this pointer
    // since DriverCtx borrows the other Driver fields mutably via split borrows.
    // Null when plaintext (TLS feature disabled or no TLS config).
    pub(crate) tls_table: *mut crate::tls::TlsTable,
    pub(crate) shutdown_requested: &'a mut bool,
    /// Pre-allocated sockaddr storage for outbound connect SQEs.
    pub(crate) connect_addrs: &'a mut Vec<libc::sockaddr_storage>,
    /// Whether to set TCP_NODELAY on outbound connections.
    pub(crate) tcp_nodelay: bool,
    /// Guard sends below this total length fall back to copy (0 = always ZC).
    pub(crate) send_zc_threshold: u32,
    /// Whether SO_TIMESTAMPING is enabled.
    #[cfg(feature = "timestamps")]
    pub(crate) timestamps: bool,
    /// Pointer to the per-worker RecvMsgMulti msghdr template.
    #[cfg(feature = "timestamps")]
    pub(crate) recvmsg_msghdr: *const libc::msghdr,
    /// Pre-allocated timespec storage for connect timeouts (io_uring only).
    #[cfg(has_io_uring)]
    pub(crate) connect_timespecs: &'a mut Vec<io_uring::types::Timespec>,
    /// Per-connection send chain tracking.
    pub(crate) chain_table: &'a mut crate::chain::SendChainTable,
    /// Maximum SQEs per chain (0 = disabled).
    pub(crate) max_chain_length: u16,
    /// Per-connection send queues for serializing sends.
    pub(crate) send_queues: &'a mut Vec<ConnSendState>,
    /// Connection indices with armed `close_notify_deadline`s. See the
    /// matching field on `backend::uring::driver::Driver` — the event
    /// loop's deadline check iterates this instead of all send queues
    /// so non-TLS workloads pay zero per-iteration cost.
    pub(crate) close_notify_armed: &'a mut Vec<u32>,
    /// Per-worker UDP socket state.
    pub(crate) udp_sockets: &'a mut Vec<crate::backend::UdpSocketState>,
    /// NVMe device table. `None` when NVMe is not configured.
    pub(crate) nvme_devices: &'a mut Option<crate::nvme::NvmeDeviceTable>,
    /// NVMe command slab. `None` when NVMe is not configured.
    pub(crate) nvme_cmd_slab: &'a mut Option<crate::nvme::NvmeCmdSlab>,
    /// Base offset in the fixed file table for NVMe device fds.
    pub(crate) nvme_fd_base: u32,
    /// Direct I/O file table. `None` when direct I/O is not configured.
    pub(crate) direct_io_files: &'a mut Option<crate::direct_io::DirectIoFileTable>,
    /// Direct I/O command slab. `None` when direct I/O is not configured.
    pub(crate) direct_io_cmd_slab: &'a mut Option<crate::direct_io::DirectIoCmdSlab>,
    /// Base offset in the fixed file table for direct I/O file fds.
    pub(crate) direct_io_fd_base: u32,
    /// Filesystem file table. `None` when fs is not configured.
    pub(crate) fs_files: &'a mut Option<crate::fs::FsFileTable>,
    /// Filesystem command slab. `None` when fs is not configured.
    pub(crate) fs_cmd_slab: &'a mut Option<crate::fs::FsCmdSlab>,
    /// Base offset in the fixed file table for filesystem file fds.
    pub(crate) fs_fd_base: u32,
    /// Pending close retries from failed submit_close calls.
    pub(crate) pending_close_retries: &'a mut Vec<(u32, u8)>,
    pub(crate) close_notify_timeout: std::time::Duration,
    pub(crate) next_disk_io_seq: &'a mut u16,
}

#[cfg(has_io_uring)]
impl<'a> DriverCtx<'a> {
    /// Request shutdown of this worker's event loop.
    /// The worker will stop after the current iteration completes.
    pub fn request_shutdown(&mut self) {
        *self.shutdown_requested = true;
    }

    /// Get the peer address for a connection.
    pub fn peer_addr(&self, conn: ConnToken) -> Option<crate::connection::PeerAddr> {
        let cs = self.connections.get(conn.index)?;
        if cs.generation != conn.generation {
            return None;
        }
        cs.peer_addr.clone()
    }

    /// Check if a connection is outbound (initiated via connect/connect_tls).
    pub fn is_outbound(&self, conn: ConnToken) -> bool {
        self.connections
            .get(conn.index)
            .map(|cs| cs.generation == conn.generation && cs.outbound)
            .unwrap_or(false)
    }

    /// Get TLS session information for a connection.
    pub fn tls_info(&self, conn: ConnToken) -> Option<crate::tls::TlsInfo> {
        let cs = self.connections.get(conn.index)?;
        if cs.generation != conn.generation {
            return None;
        }
        if self.tls_table.is_null() {
            return None;
        }
        let tls_table = unsafe { &*self.tls_table };
        tls_table.get_info(conn.index)
    }

    /// Regular (copying) send — copies data into library-owned pool before SQE submission.
    ///
    /// Data larger than one send-pool slot is queued as multiple chunks. If
    /// a chunk fails mid-loop (pool exhausted), the chunks queued before it
    /// are already committed to the wire and `Err` is returned — retrying
    /// the whole buffer would duplicate that prefix. Treat a mid-buffer
    /// error as fatal for the connection (close it) rather than retrying.
    pub fn send(&mut self, conn: ConnToken, data: &[u8]) -> io::Result<()> {
        let conn_state = self
            .connections
            .get(conn.index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "invalid connection"))?;
        if conn_state.generation != conn.generation {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "stale connection",
            ));
        }

        if !self.tls_table.is_null() {
            let tls_table = unsafe { &mut *self.tls_table };
            if tls_table.get_mut(conn.index).is_some() {
                let sends =
                    crate::tls::encrypt_to_sends(tls_table, self.send_copy_pool, conn.index, data)?;
                // Route every ciphertext chunk through the per-connection
                // send queue: io_uring doesn't order independent SQEs, and
                // a partial-send resubmit would interleave chunks on the
                // wire (bad_record_mac at the peer).
                return self.queue_built_sends(conn.index, sends);
            }
        }

        let slot_size = self.send_copy_pool.slot_size() as usize;

        // Chunk data that exceeds the send copy slot size. Each chunk gets its
        // own pool slot and SQE; the per-connection send queue ensures they are
        // transmitted in order. Only the final chunk is marked end-of-send, so
        // the waiter is woken once for the whole logical send rather than once
        // per chunk (which would report a short count and, for pipelined sends,
        // wake the wrong future).
        let mut chunks = data.chunks(slot_size).peekable();
        while let Some(chunk) = chunks.next() {
            let (slot, ptr, len) = self
                .send_copy_pool
                .copy_in(chunk)
                .ok_or_else(|| io::Error::other("send copy pool exhausted"))?;
            self.send_copy_pool
                .set_end_of_send(slot, chunks.peek().is_none());

            let user_data = crate::completion::UserData::encode(
                crate::completion::OpTag::Send,
                conn.index,
                slot as u32,
            );
            let entry = io_uring::opcode::Send::new(io_uring::types::Fixed(conn.index), ptr, len)
                .flags(crate::completion::STREAM_SEND_FLAGS)
                .build()
                .user_data(user_data.raw());

            let built = BuiltSend {
                entry,
                pool_slot: slot,
                slab_idx: u16::MAX,
                total_len: chunk.len() as u32,
            };

            self.submit_or_queue(conn.index, built)?;
        }

        Ok(())
    }

    /// Allocate a unique 32-bit disk-I/O completion key: monotonic sequence
    /// in the high 16 bits, slab index in the low 16. fs, NVMe, and
    /// direct-io share the executor's completion/graveyard maps; a raw slab
    /// index collided across their three independent slabs (results swapped
    /// between subsystems, a dropped fs future's graveyard buffer freed by
    /// an unrelated NVMe completion while the kernel was still writing to
    /// it) and across LIFO reuse of one slab slot within a drain batch.
    /// CQE handlers extract the slab index from the low 16 bits and wake
    /// with the full key.
    pub(crate) fn disk_io_key(&mut self, slab_idx: u16) -> u32 {
        let seq = *self.next_disk_io_seq;
        *self.next_disk_io_seq = seq.wrapping_add(1);
        ((seq as u32) << 16) | slab_idx as u32
    }

    /// Queue a batch of built sends in order; on a submit failure the
    /// remaining entries' resources are released so nothing leaks.
    pub(crate) fn queue_built_sends(
        &mut self,
        conn_index: u32,
        sends: Vec<BuiltSend>,
    ) -> io::Result<()> {
        let mut it = sends.into_iter();
        while let Some(built) = it.next() {
            if let Err(e) = self.submit_or_queue(conn_index, built) {
                for rest in it {
                    if rest.slab_idx != u16::MAX {
                        let pool_slot = self.send_slab.release(rest.slab_idx);
                        if pool_slot != u16::MAX {
                            self.send_copy_pool.release(pool_slot);
                        }
                    } else if rest.pool_slot != u16::MAX {
                        self.send_copy_pool.release(rest.pool_slot);
                    }
                }
                return Err(e);
            }
        }
        Ok(())
    }

    /// Submit a built send SQE or queue it if a send is already in-flight.
    pub(crate) fn submit_or_queue(&mut self, conn_index: u32, built: BuiltSend) -> io::Result<()> {
        let state = &mut self.send_queues[conn_index as usize];
        if state.in_flight {
            state.queue.push_back(built);
            Ok(())
        } else {
            // Destructure instead of cloning the 64-byte SQE: the fields are
            // only needed on the error branch.
            let BuiltSend {
                entry,
                pool_slot,
                slab_idx,
                total_len: _,
            } = built;
            match unsafe { self.ring.push_sqe(entry) } {
                Ok(()) => {
                    state.in_flight = true;
                    Ok(())
                }
                Err(e) => {
                    // Release resources that would otherwise leak.
                    if slab_idx != u16::MAX {
                        let pool_slot = self.send_slab.release(slab_idx);
                        if pool_slot != u16::MAX {
                            self.send_copy_pool.release(pool_slot);
                        }
                    } else if pool_slot != u16::MAX {
                        self.send_copy_pool.release(pool_slot);
                    }
                    Err(e)
                }
            }
        }
    }

    /// Returns the maximum number of SQEs per IO_LINK chain.
    /// 0 means chaining is disabled.
    pub fn max_chain_length(&self) -> u16 {
        self.max_chain_length
    }

    /// Begin building an IO_LINK send chain for a connection.
    ///
    /// Multiple sends (copy-only or scatter-gather) are collected and submitted
    /// as a linked SQE chain. The kernel executes them sequentially, and a single
    /// `on_send_complete` fires when the entire chain is done.
    ///
    /// Returns a [`SendChainBuilder`]. Call `.copy()`, `.parts()...add()` to
    /// add SQEs, then `.finish()` to submit.
    pub fn send_chain(&mut self, conn: ConnToken) -> SendChainBuilder<'_, 'a> {
        SendChainBuilder {
            ctx: self,
            conn,
            built: Vec::new(),
            total_bytes: 0,
            error: None,
            finished: false,
        }
    }

    /// Begin building a scatter-gather send with mixed copy + zero-copy guard parts.
    pub fn send_parts(&mut self, conn: ConnToken) -> SendBuilder<'_, 'a> {
        SendBuilder {
            ctx: self,
            conn,
            parts: [PartSlot::Empty; MAX_IOVECS],
            part_count: 0,
            copy_slices: [(std::ptr::null(), 0); MAX_IOVECS],
            copy_count: 0,
            total_copy_len: 0,
            guards: [const { None }; crate::buffer::send_slab::MAX_GUARDS],
            guard_count: 0,
            total_len: 0,
            error: None,
        }
    }

    /// Close a connection.
    pub fn close(&mut self, conn: ConnToken) {
        if let Some(conn_state) = self.connections.get_mut(conn.index) {
            if conn_state.generation != conn.generation {
                return;
            }
            conn_state.recv_mode = crate::connection::RecvMode::Closed;

            // Drain the send queue and release all queued resources.
            let state = &mut self.send_queues[conn.index as usize];
            for built in state.queue.drain(..) {
                if built.slab_idx != u16::MAX {
                    let pool_slot = self.send_slab.release(built.slab_idx);
                    if pool_slot != u16::MAX {
                        self.send_copy_pool.release(pool_slot);
                    }
                } else if built.pool_slot != u16::MAX {
                    self.send_copy_pool.release(built.pool_slot);
                }
            }
            state.in_flight = false;

            // Graceful TLS shutdown: send close_notify before closing.
            if !self.tls_table.is_null() {
                let tls_table = unsafe { &mut *self.tls_table };
                if tls_table.has(conn.index) {
                    tls_table.send_close_notify(conn.index, self.ring, self.send_copy_pool);
                    // Arm the close_notify deadline for timeout detection
                    // and register this index in the armed set so the
                    // event loop's `check_close_notify_deadlines` will
                    // examine it. Avoids the full O(N) send_queues walk
                    // for non-TLS workloads, which previously dominated
                    // worker CPU at high request rates.
                    let state = &mut self.send_queues[conn.index as usize];
                    state.close_notify_deadline =
                        Some(std::time::Instant::now() + self.close_notify_timeout);
                    if !self.close_notify_armed.contains(&conn.index) {
                        self.close_notify_armed.push(conn.index);
                    }
                }
            }

            if self.ring.submit_close(conn.index).is_err() {
                crate::metrics::RING.increment(crate::metrics::ring::CLOSE_SUBMIT_FAILURES);
                self.pending_close_retries.push((conn.index, 0));
            }
        }
    }

    /// Shutdown the write side of a connection.
    ///
    /// If sends are in-flight or queued, the shutdown is deferred until the
    /// send queue drains to avoid racing with pending Send SQEs.
    pub fn shutdown_write(&mut self, conn: ConnToken) {
        if let Some(conn_state) = self.connections.get(conn.index) {
            if conn_state.generation != conn.generation {
                return;
            }
            let idx = conn.index as usize;
            if self.send_queues[idx].in_flight || !self.send_queues[idx].queue.is_empty() {
                // Defer until send queue drains.
                self.send_queues[idx].shutdown_pending = true;
            } else {
                let _ = self.ring.submit_shutdown(conn.index);
            }
        }
    }

    /// Send a UDP datagram to the given peer address.
    ///
    /// Copies `data` into the send pool and submits a `sendmsg` SQE. Up to
    /// `Config::udp_send_slots` sends can be in flight concurrently per
    /// socket; exhaustion returns [`crate::error::UdpSendError::PoolExhausted`].
    pub fn send_to(
        &mut self,
        socket: UdpToken,
        peer: SocketAddr,
        data: &[u8],
    ) -> Result<(), crate::error::UdpSendError> {
        let idx = socket.0 as usize;
        if idx >= self.udp_sockets.len() {
            return Err(crate::error::UdpSendError::Io(io::Error::other(
                "invalid UDP socket index",
            )));
        }

        let slot_idx = self.udp_sockets[idx]
            .send_freelist
            .pop()
            .ok_or(crate::error::UdpSendError::PoolExhausted)?;

        let (pool_slot, ptr, len) = match self.send_copy_pool.copy_in(data) {
            Some(v) => v,
            None => {
                self.udp_sockets[idx].send_freelist.push(slot_idx);
                return Err(crate::error::UdpSendError::PoolExhausted);
            }
        };

        let fd_index = self.udp_sockets[idx].fd_index;
        let slot = &mut self.udp_sockets[idx].send_slots[slot_idx as usize];
        let addr_len = crate::backend::socket_addr_to_sockaddr(peer, &mut slot.send_addr);
        slot.send_iov.iov_base = ptr as *mut libc::c_void;
        slot.send_iov.iov_len = len as usize;
        slot.send_msghdr.msg_namelen = addr_len;

        let msghdr_ptr = &*slot.send_msghdr as *const libc::msghdr;
        let payload = crate::backend::uring::driver::encode_udp_send_payload(slot_idx, pool_slot);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::SendMsgUdp,
            socket.0,
            payload,
        );

        match self.ring.submit_sendmsg(fd_index, msghdr_ptr, ud) {
            Ok(()) => {
                crate::metrics::UDP.increment(crate::metrics::udp::DATAGRAMS_SENT);
                Ok(())
            }
            Err(_e) => {
                self.send_copy_pool.release(pool_slot);
                self.udp_sockets[idx].send_freelist.push(slot_idx);
                Err(crate::error::UdpSendError::SubmissionQueueFull)
            }
        }
    }

    /// Initiate an outbound TCP connection. Returns immediately with a `ConnToken`.
    /// The `on_connect` callback fires when the TCP handshake completes (or fails).
    pub fn connect(&mut self, addr: SocketAddr) -> Result<ConnToken, crate::error::Error> {
        let conn_index = self
            .connections
            .allocate_outbound()
            .ok_or(crate::error::Error::ConnectionLimitReached)?;
        let generation = self.connections.generation(conn_index);

        // Store peer address.
        if let Some(cs) = self.connections.get_mut(conn_index) {
            cs.peer_addr = Some(crate::connection::PeerAddr::Tcp(addr));
        }

        // Create socket.
        let domain = if addr.is_ipv4() {
            libc::AF_INET
        } else {
            libc::AF_INET6
        };
        let raw_fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if raw_fd < 0 {
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(io::Error::last_os_error()));
        }

        // Set TCP_NODELAY if configured.
        if self.tcp_nodelay {
            let optval: libc::c_int = 1;
            unsafe {
                libc::setsockopt(
                    raw_fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Set SO_TIMESTAMPING for kernel-level RX timestamps.
        #[cfg(feature = "timestamps")]
        if self.timestamps {
            let flags: libc::c_int = (libc::SOF_TIMESTAMPING_SOFTWARE
                | libc::SOF_TIMESTAMPING_RX_SOFTWARE)
                as libc::c_int;
            unsafe {
                libc::setsockopt(
                    raw_fd,
                    libc::SOL_SOCKET,
                    libc::SO_TIMESTAMPING,
                    &flags as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Register in the direct file table, then close the original fd.
        if let Err(e) = self.ring.register_files_update(conn_index, &[raw_fd]) {
            unsafe {
                libc::close(raw_fd);
            }
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }
        unsafe {
            libc::close(raw_fd);
        }

        // Fill sockaddr_storage for the connect SQE.
        let addrlen = crate::backend::socket_addr_to_sockaddr(
            addr,
            &mut self.connect_addrs[conn_index as usize],
        );

        // Submit the async connect.
        if let Err(e) = self.ring.submit_connect(
            conn_index,
            &self.connect_addrs[conn_index as usize] as *const _ as *const libc::sockaddr,
            addrlen,
        ) {
            // Stale fixed file entry is overwritten when the slot is reused.
            let _ = self.ring.register_files_update(conn_index, &[-1]);
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }

        Ok(ConnToken::new(conn_index, generation))
    }

    /// Initiate an outbound Unix domain socket connection. Returns immediately
    /// with a `ConnToken`. The `on_connect` callback fires when the connection
    /// completes (or fails).
    pub fn connect_unix(
        &mut self,
        path: &std::path::Path,
    ) -> Result<ConnToken, crate::error::Error> {
        let conn_index = self
            .connections
            .allocate_outbound()
            .ok_or(crate::error::Error::ConnectionLimitReached)?;
        let generation = self.connections.generation(conn_index);

        // Store peer address.
        if let Some(cs) = self.connections.get_mut(conn_index) {
            cs.peer_addr = Some(crate::connection::PeerAddr::Unix(path.to_path_buf()));
        }

        // Create AF_UNIX socket.
        let raw_fd = unsafe {
            libc::socket(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if raw_fd < 0 {
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(io::Error::last_os_error()));
        }

        // Register in the direct file table, then close the original fd.
        if let Err(e) = self.ring.register_files_update(conn_index, &[raw_fd]) {
            unsafe {
                libc::close(raw_fd);
            }
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }
        unsafe {
            libc::close(raw_fd);
        }

        // Fill sockaddr_storage for the connect SQE.
        let addrlen = crate::backend::unix_path_to_sockaddr(
            path,
            &mut self.connect_addrs[conn_index as usize],
        );

        // Submit the async connect.
        if let Err(e) = self.ring.submit_connect(
            conn_index,
            &self.connect_addrs[conn_index as usize] as *const _ as *const libc::sockaddr,
            addrlen,
        ) {
            // Stale fixed file entry is overwritten when the slot is reused.
            let _ = self.ring.register_files_update(conn_index, &[-1]);
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }

        Ok(ConnToken::new(conn_index, generation))
    }

    /// Initiate an outbound TCP connection with a timeout.
    /// If the connection is not established within `timeout_ms`, `on_connect` fires
    /// with `Err(TimedOut)`.
    pub fn connect_with_timeout(
        &mut self,
        addr: SocketAddr,
        timeout_ms: u64,
    ) -> Result<ConnToken, crate::error::Error> {
        let token = self.connect(addr)?;
        self.arm_connect_timeout(token.index, timeout_ms);
        Ok(token)
    }

    /// Initiate an outbound TLS connection. Returns immediately with a `ConnToken`.
    /// The `on_connect` callback fires when both TCP + TLS handshakes complete (or fail).
    pub fn connect_tls(
        &mut self,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<ConnToken, crate::error::Error> {
        if self.tls_table.is_null() {
            return Err(crate::error::Error::RingSetup(
                "TLS not configured".to_string(),
            ));
        }
        let tls_table = unsafe { &mut *self.tls_table };
        if !tls_table.has_client_config() {
            return Err(crate::error::Error::RingSetup(
                "TLS client config not set".to_string(),
            ));
        }

        let conn_index = self
            .connections
            .allocate_outbound()
            .ok_or(crate::error::Error::ConnectionLimitReached)?;
        let generation = self.connections.generation(conn_index);

        // Store peer address.
        if let Some(cs) = self.connections.get_mut(conn_index) {
            cs.peer_addr = Some(crate::connection::PeerAddr::Tcp(addr));
        }

        // Create socket.
        let domain = if addr.is_ipv4() {
            libc::AF_INET
        } else {
            libc::AF_INET6
        };
        let raw_fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if raw_fd < 0 {
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(io::Error::last_os_error()));
        }

        // Set TCP_NODELAY if configured.
        if self.tcp_nodelay {
            let optval: libc::c_int = 1;
            unsafe {
                libc::setsockopt(
                    raw_fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Set SO_TIMESTAMPING for kernel-level RX timestamps.
        #[cfg(feature = "timestamps")]
        if self.timestamps {
            let flags: libc::c_int = (libc::SOF_TIMESTAMPING_SOFTWARE
                | libc::SOF_TIMESTAMPING_RX_SOFTWARE)
                as libc::c_int;
            unsafe {
                libc::setsockopt(
                    raw_fd,
                    libc::SOL_SOCKET,
                    libc::SO_TIMESTAMPING,
                    &flags as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Register in the direct file table, then close the original fd.
        if let Err(e) = self.ring.register_files_update(conn_index, &[raw_fd]) {
            unsafe {
                libc::close(raw_fd);
            }
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }
        unsafe {
            libc::close(raw_fd);
        }

        // Create TLS client state (buffers ClientHello internally).
        let sni = rustls::pki_types::ServerName::try_from(server_name.to_owned()).map_err(|e| {
            // Stale fixed file entry is overwritten when the slot is reused.
            let _ = self.ring.register_files_update(conn_index, &[-1]);
            self.connections.release(conn_index);
            crate::error::Error::RingSetup(format!("invalid server name: {e}"))
        })?;
        if let Err(e) = tls_table.create_client(conn_index, sni) {
            // Stale fixed file entry is overwritten when the slot is reused.
            let _ = self.ring.register_files_update(conn_index, &[-1]);
            self.connections.release(conn_index);
            return Err(crate::error::Error::RingSetup(format!(
                "TLS client setup failed: {e}"
            )));
        }

        // Fill sockaddr_storage for the connect SQE.
        let addrlen = crate::backend::socket_addr_to_sockaddr(
            addr,
            &mut self.connect_addrs[conn_index as usize],
        );

        // Submit the async connect.
        if let Err(e) = self.ring.submit_connect(
            conn_index,
            &self.connect_addrs[conn_index as usize] as *const _ as *const libc::sockaddr,
            addrlen,
        ) {
            tls_table.remove(conn_index);
            // Stale fixed file entry is overwritten when the slot is reused.
            let _ = self.ring.register_files_update(conn_index, &[-1]);
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }

        Ok(ConnToken::new(conn_index, generation))
    }

    /// Initiate an outbound TLS connection with a timeout.
    pub fn connect_tls_with_timeout(
        &mut self,
        addr: SocketAddr,
        server_name: &str,
        timeout_ms: u64,
    ) -> Result<ConnToken, crate::error::Error> {
        let token = self.connect_tls(addr, server_name)?;
        self.arm_connect_timeout(token.index, timeout_ms);
        Ok(token)
    }

    /// Cancel pending operations on a connection.
    pub fn cancel(&mut self, conn: ConnToken) -> io::Result<()> {
        let cs = self
            .connections
            .get_mut(conn.index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "invalid connection"))?;
        if cs.generation != conn.generation {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "stale connection",
            ));
        }

        // Determine target op to cancel.
        let target_tag = match cs.recv_mode {
            crate::connection::RecvMode::Connecting => crate::completion::OpTag::Connect,
            crate::connection::RecvMode::Multi => crate::completion::OpTag::RecvMulti,
            #[cfg(feature = "timestamps")]
            crate::connection::RecvMode::MsgMulti => crate::completion::OpTag::RecvMsgMultiTs,
            crate::connection::RecvMode::Closed => {
                return Ok(()); // nothing to cancel
            }
        };

        // If cancelling a connect with an armed timeout, also cancel the timeout
        // so the Connect ECANCELED CQE is handled as user-initiated (not timeout-initiated).
        if matches!(target_tag, crate::completion::OpTag::Connect) && cs.connect_timeout_armed {
            cs.connect_timeout_armed = false;
            let timeout_ud = crate::completion::UserData::encode(
                crate::completion::OpTag::Timeout,
                conn.index,
                0,
            );
            // Best effort cancel; timeout fires harmlessly if already established.
            let _ = self.ring.submit_async_cancel(timeout_ud.raw(), conn.index);
        }

        cs.recv_mode = crate::connection::RecvMode::Closed;

        let target_ud = crate::completion::UserData::encode(target_tag, conn.index, 0);
        self.ring.submit_async_cancel(target_ud.raw(), conn.index)?;
        Ok(())
    }

    // ── NVMe passthrough methods ──────────────────────────────────────────

    /// Open an NVMe device for passthrough I/O.
    ///
    /// `path` must be an NVMe-generic character device (e.g., `/dev/ng0n1`).
    /// `nsid` is the NVMe namespace ID (usually 1).
    ///
    /// The device fd is registered in the io_uring fixed file table. Returns
    /// an [`NvmeDevice`](crate::nvme::NvmeDevice) handle for subsequent operations.
    pub fn open_nvme_device(
        &mut self,
        path: &str,
        nsid: u32,
    ) -> io::Result<crate::nvme::NvmeDevice> {
        let devices = self
            .nvme_devices
            .as_mut()
            .ok_or_else(|| io::Error::other("NVMe not configured"))?;

        let index = devices
            .allocate()
            .ok_or_else(|| io::Error::other("NVMe device table full"))?;

        // Open the NVMe-generic character device.
        let c_path =
            std::ffi::CString::new(path).map_err(|_| io::Error::other("invalid device path"))?;
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDWR) };
        if fd < 0 {
            devices.release(index);
            return Err(io::Error::last_os_error());
        }

        // Register in the fixed file table.
        let fd_index = self.nvme_fd_base + index as u32;
        if self.ring.register_files_update(fd_index, &[fd]).is_err() {
            devices.release(index);
            unsafe {
                libc::close(fd);
            }
            return Err(io::Error::other("failed to register NVMe fd"));
        }
        unsafe {
            libc::close(fd);
        }

        // Store device state.
        if let Some(dev) = devices.get_mut(index) {
            dev.fd_index = fd_index;
            dev.nsid = nsid;
        }

        let generation = devices.get(index).map(|d| d.generation).unwrap_or(0);
        Ok(crate::nvme::NvmeDevice { index, generation })
    }

    /// Submit an NVMe read command.
    ///
    /// Reads `num_blocks` logical blocks starting at `lba` into the buffer
    /// at `buf_addr` with length `buf_len`.
    ///
    /// Returns the command slab index (sequence number) for correlation.
    ///
    /// # Safety
    ///
    /// `buf_addr` must point to a valid, aligned buffer of at least `buf_len`
    /// bytes that remains valid and exclusively accessible until the
    /// corresponding CQE completes.
    pub unsafe fn nvme_read(
        &mut self,
        device: crate::nvme::NvmeDevice,
        lba: u64,
        num_blocks: u16,
        buf_addr: u64,
        buf_len: u32,
    ) -> io::Result<u32> {
        if num_blocks == 0 {
            return Err(io::Error::other("num_blocks must be >= 1"));
        }
        let (fd_index, nsid) = self.validate_nvme_device(device)?;

        let slab = self
            .nvme_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("NVMe not configured"))?;
        let slab_idx = slab
            .allocate(device.index)
            .ok_or_else(|| io::Error::other("NVMe command slab exhausted"))?;

        let cmd = crate::nvme::NvmeUringCmd::read(nsid, lba, num_blocks, buf_addr, buf_len);
        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::NvmeCmd,
            device.index as u32,
            key,
        );

        match unsafe { self.ring.submit_nvme_cmd(fd_index, &cmd, ud) } {
            Ok(()) => {
                if let Some(devices) = self.nvme_devices.as_mut()
                    && let Some(dev) = devices.get_mut(device.index)
                {
                    dev.in_flight += 1;
                }
                Ok(key)
            }
            Err(e) => {
                if let Some(slab) = self.nvme_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Submit an NVMe write command.
    ///
    /// Writes `num_blocks` logical blocks starting at `lba` from the buffer
    /// at `buf_addr` with length `buf_len`.
    ///
    /// Returns the command slab index (sequence number) for correlation.
    ///
    /// # Safety
    ///
    /// `buf_addr` must point to a valid, aligned buffer of at least `buf_len`
    /// bytes that remains valid and exclusively accessible until the
    /// corresponding CQE completes.
    pub unsafe fn nvme_write(
        &mut self,
        device: crate::nvme::NvmeDevice,
        lba: u64,
        num_blocks: u16,
        buf_addr: u64,
        buf_len: u32,
    ) -> io::Result<u32> {
        if num_blocks == 0 {
            return Err(io::Error::other("num_blocks must be >= 1"));
        }
        let (fd_index, nsid) = self.validate_nvme_device(device)?;

        let slab = self
            .nvme_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("NVMe not configured"))?;
        let slab_idx = slab
            .allocate(device.index)
            .ok_or_else(|| io::Error::other("NVMe command slab exhausted"))?;

        let cmd = crate::nvme::NvmeUringCmd::write(nsid, lba, num_blocks, buf_addr, buf_len);
        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::NvmeCmd,
            device.index as u32,
            key,
        );

        match unsafe { self.ring.submit_nvme_cmd(fd_index, &cmd, ud) } {
            Ok(()) => {
                if let Some(devices) = self.nvme_devices.as_mut()
                    && let Some(dev) = devices.get_mut(device.index)
                {
                    dev.in_flight += 1;
                }
                Ok(key)
            }
            Err(e) => {
                if let Some(slab) = self.nvme_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Submit an NVMe flush command.
    ///
    /// Returns the command slab index (sequence number) for correlation.
    pub fn nvme_flush(&mut self, device: crate::nvme::NvmeDevice) -> io::Result<u32> {
        let (fd_index, nsid) = self.validate_nvme_device(device)?;

        let slab = self
            .nvme_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("NVMe not configured"))?;
        let slab_idx = slab
            .allocate(device.index)
            .ok_or_else(|| io::Error::other("NVMe command slab exhausted"))?;

        let cmd = crate::nvme::NvmeUringCmd::flush(nsid);
        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::NvmeCmd,
            device.index as u32,
            key,
        );

        match unsafe { self.ring.submit_nvme_cmd(fd_index, &cmd, ud) } {
            Ok(()) => {
                if let Some(devices) = self.nvme_devices.as_mut()
                    && let Some(dev) = devices.get_mut(device.index)
                {
                    dev.in_flight += 1;
                }
                Ok(key)
            }
            Err(e) => {
                if let Some(slab) = self.nvme_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Close an NVMe device.
    pub fn close_nvme_device(&mut self, device: crate::nvme::NvmeDevice) -> io::Result<()> {
        let (fd_index, _nsid) = self.validate_nvme_device(device)?;

        // Unregister from the fixed file table.
        self.ring.register_files_update(fd_index, &[-1i32])?;

        if let Some(devices) = self.nvme_devices.as_mut() {
            devices.release(device.index);
        }

        Ok(())
    }

    /// Validate an NVMe device handle and return (fd_index, nsid).
    fn validate_nvme_device(&self, device: crate::nvme::NvmeDevice) -> io::Result<(u32, u32)> {
        let devices = self
            .nvme_devices
            .as_ref()
            .ok_or_else(|| io::Error::other("NVMe not configured"))?;
        let dev = devices
            .get(device.index)
            .ok_or_else(|| io::Error::other("invalid NVMe device handle"))?;
        if dev.generation != device.generation {
            return Err(io::Error::other("stale NVMe device handle"));
        }
        Ok((dev.fd_index, dev.nsid))
    }

    // ── Direct I/O methods ────────────────────────────────────────────────

    /// Open a file for direct I/O (O_DIRECT).
    ///
    /// `path` can be any file or block device path. The file is opened with
    /// `O_RDWR | O_DIRECT`. The fd is registered in the io_uring fixed file table.
    ///
    /// Returns a [`DirectIoFile`](crate::direct_io::DirectIoFile) handle for
    /// subsequent operations.
    pub fn open_direct_io_file(
        &mut self,
        path: &str,
    ) -> io::Result<crate::direct_io::DirectIoFile> {
        let files = self
            .direct_io_files
            .as_mut()
            .ok_or_else(|| io::Error::other("direct I/O not configured"))?;

        let index = files
            .allocate()
            .ok_or_else(|| io::Error::other("direct I/O file table full"))?;

        // Open with O_DIRECT | O_RDWR.
        let c_path =
            std::ffi::CString::new(path).map_err(|_| io::Error::other("invalid file path"))?;
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDWR | libc::O_DIRECT) };
        if fd < 0 {
            files.release(index);
            return Err(io::Error::last_os_error());
        }

        // Register in the fixed file table.
        let fd_index = self.direct_io_fd_base + index as u32;
        if self.ring.register_files_update(fd_index, &[fd]).is_err() {
            files.release(index);
            unsafe {
                libc::close(fd);
            }
            return Err(io::Error::other("failed to register direct I/O fd"));
        }
        unsafe {
            libc::close(fd);
        }

        // Store file state.
        if let Some(f) = files.get_mut(index) {
            f.fd_index = fd_index;
        }

        let generation = files.get(index).map(|f| f.generation).unwrap_or(0);
        Ok(crate::direct_io::DirectIoFile { index, generation })
    }

    /// Submit a direct I/O read.
    ///
    /// Reads `len` bytes from `offset` into the buffer at `buf`.
    /// The buffer must be aligned to the logical block size and remain valid
    /// until the direct I/O completion fires.
    ///
    /// Returns the command slab index (sequence number) for correlation.
    ///
    /// # Safety
    /// `buf` must point to valid, aligned memory of at least `len` bytes
    /// that remains valid until the completion callback fires.
    pub unsafe fn direct_io_read(
        &mut self,
        file: crate::direct_io::DirectIoFile,
        offset: u64,
        buf: *mut u8,
        len: u32,
    ) -> io::Result<u32> {
        let fd_index = self.validate_direct_io_file(file)?;

        let slab = self
            .direct_io_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("direct I/O not configured"))?;
        let slab_idx = slab
            .allocate(file.index, crate::direct_io::DirectIoOp::Read)
            .ok_or_else(|| io::Error::other("direct I/O command slab exhausted"))?;

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::DirectIo,
            file.index as u32,
            key,
        );

        match unsafe { self.ring.submit_direct_read(fd_index, buf, len, offset, ud) } {
            Ok(()) => {
                if let Some(files) = self.direct_io_files.as_mut()
                    && let Some(f) = files.get_mut(file.index)
                {
                    f.in_flight += 1;
                }
                Ok(key)
            }
            Err(e) => {
                if let Some(slab) = self.direct_io_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Submit a direct I/O write.
    ///
    /// Writes `len` bytes from the buffer at `buf` to `offset`.
    /// The buffer must be aligned to the logical block size and remain valid
    /// until the direct I/O completion fires.
    ///
    /// Returns the command slab index (sequence number) for correlation.
    ///
    /// # Safety
    /// `buf` must point to valid, aligned memory of at least `len` bytes
    /// that remains valid until the completion callback fires.
    pub unsafe fn direct_io_write(
        &mut self,
        file: crate::direct_io::DirectIoFile,
        offset: u64,
        buf: *const u8,
        len: u32,
    ) -> io::Result<u32> {
        let fd_index = self.validate_direct_io_file(file)?;

        let slab = self
            .direct_io_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("direct I/O not configured"))?;
        let slab_idx = slab
            .allocate(file.index, crate::direct_io::DirectIoOp::Write)
            .ok_or_else(|| io::Error::other("direct I/O command slab exhausted"))?;

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::DirectIo,
            file.index as u32,
            key,
        );

        match unsafe {
            self.ring
                .submit_direct_write(fd_index, buf, len, offset, ud)
        } {
            Ok(()) => {
                if let Some(files) = self.direct_io_files.as_mut()
                    && let Some(f) = files.get_mut(file.index)
                {
                    f.in_flight += 1;
                }
                Ok(key)
            }
            Err(e) => {
                if let Some(slab) = self.direct_io_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Submit an fsync for a direct I/O file.
    ///
    /// Returns the command slab index (sequence number) for correlation.
    pub fn direct_io_fsync(&mut self, file: crate::direct_io::DirectIoFile) -> io::Result<u32> {
        let fd_index = self.validate_direct_io_file(file)?;

        let slab = self
            .direct_io_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("direct I/O not configured"))?;
        let slab_idx = slab
            .allocate(file.index, crate::direct_io::DirectIoOp::Fsync)
            .ok_or_else(|| io::Error::other("direct I/O command slab exhausted"))?;

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::DirectIo,
            file.index as u32,
            key,
        );

        match self.ring.submit_direct_fsync(fd_index, ud) {
            Ok(()) => {
                if let Some(files) = self.direct_io_files.as_mut()
                    && let Some(f) = files.get_mut(file.index)
                {
                    f.in_flight += 1;
                }
                Ok(key)
            }
            Err(e) => {
                if let Some(slab) = self.direct_io_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Close a direct I/O file.
    pub fn close_direct_io_file(&mut self, file: crate::direct_io::DirectIoFile) -> io::Result<()> {
        let fd_index = self.validate_direct_io_file(file)?;

        // Unregister from the fixed file table.
        self.ring.register_files_update(fd_index, &[-1i32])?;

        if let Some(files) = self.direct_io_files.as_mut() {
            files.release(file.index);
        }

        Ok(())
    }

    /// Validate a direct I/O file handle and return the fd_index.
    fn validate_direct_io_file(&self, file: crate::direct_io::DirectIoFile) -> io::Result<u32> {
        let files = self
            .direct_io_files
            .as_ref()
            .ok_or_else(|| io::Error::other("direct I/O not configured"))?;
        let f = files
            .get(file.index)
            .ok_or_else(|| io::Error::other("invalid direct I/O file handle"))?;
        if f.generation != file.generation {
            return Err(io::Error::other("stale direct I/O file handle"));
        }
        Ok(f.fd_index)
    }

    // ── Filesystem I/O methods ─────────────────────────────────────────────

    /// Open a file asynchronously via io_uring.
    ///
    /// Allocates a file table slot and command slab entry, submits an openat
    /// SQE that installs the fd directly into the fixed file table.
    ///
    /// Returns `(file_index, generation, slab_idx)`.
    pub(crate) fn fs_open(
        &mut self,
        path: &std::path::Path,
        flags: crate::fs::OpenFlags,
        mode: u32,
    ) -> io::Result<(u16, u16, u32)> {
        let files = self
            .fs_files
            .as_mut()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;

        let file_index = files
            .allocate()
            .ok_or_else(|| io::Error::other("filesystem file table full"))?;

        let generation = files.get(file_index).map(|f| f.generation).unwrap_or(0);
        let fd_index = self.fs_fd_base + file_index as u32;

        // Store fd_index in file state.
        if let Some(f) = files.get_mut(file_index) {
            f.fd_index = fd_index;
        }

        let c_path = crate::fs::path_to_cstring(path).inspect_err(|_| {
            self.fs_files.as_mut().unwrap().release(file_index);
        })?;

        let slab = self.fs_cmd_slab.as_mut().ok_or_else(|| {
            self.fs_files.as_mut().unwrap().release(file_index);
            io::Error::other("filesystem I/O not configured")
        })?;

        let slab_idx = slab
            .allocate(file_index, crate::fs::FsOp::Open)
            .ok_or_else(|| {
                self.fs_files.as_mut().unwrap().release(file_index);
                io::Error::other("filesystem command slab exhausted")
            })?;

        // Store the CString in the slab entry so it lives until CQE.
        if let Some(entry) = slab.get_mut(slab_idx) {
            entry.path = Some(c_path);
        }

        let path_ptr = self
            .fs_cmd_slab
            .as_ref()
            .unwrap()
            .get(slab_idx)
            .unwrap()
            .path
            .as_ref()
            .unwrap()
            .as_ptr();

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::Fs,
            file_index as u32,
            key,
        );

        match unsafe {
            self.ring
                .submit_openat(fd_index, path_ptr, flags.0, mode, ud.raw())
        } {
            Ok(()) => Ok((file_index, generation, key)),
            Err(e) => {
                if let Some(slab) = self.fs_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                if let Some(files) = self.fs_files.as_mut() {
                    files.release(file_index);
                }
                Err(e)
            }
        }
    }

    /// Submit a filesystem read.
    ///
    /// # Safety
    /// `buf` must point to valid, writable memory of at least `len` bytes
    /// that remains valid until the completion fires.
    pub(crate) unsafe fn fs_read(
        &mut self,
        file: crate::fs::File,
        offset: u64,
        buf: *mut u8,
        len: u32,
    ) -> io::Result<u32> {
        let fd_index = self.validate_fs_file(file)?;

        let slab = self
            .fs_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;
        let slab_idx = slab
            .allocate(file.index, crate::fs::FsOp::Read)
            .ok_or_else(|| io::Error::other("filesystem command slab exhausted"))?;

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::Fs,
            file.index as u32,
            key,
        );

        match unsafe { self.ring.submit_direct_read(fd_index, buf, len, offset, ud) } {
            Ok(()) => {
                if let Some(files) = self.fs_files.as_mut()
                    && let Some(f) = files.get_mut(file.index)
                {
                    f.in_flight += 1;
                }
                Ok(key)
            }
            Err(e) => {
                if let Some(slab) = self.fs_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Submit a filesystem write.
    ///
    /// # Safety
    /// `buf` must point to valid, readable memory of at least `len` bytes
    /// that remains valid until the completion fires.
    pub(crate) unsafe fn fs_write(
        &mut self,
        file: crate::fs::File,
        offset: u64,
        buf: *const u8,
        len: u32,
    ) -> io::Result<u32> {
        let fd_index = self.validate_fs_file(file)?;

        let slab = self
            .fs_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;
        let slab_idx = slab
            .allocate(file.index, crate::fs::FsOp::Write)
            .ok_or_else(|| io::Error::other("filesystem command slab exhausted"))?;

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::Fs,
            file.index as u32,
            key,
        );

        match unsafe {
            self.ring
                .submit_direct_write(fd_index, buf, len, offset, ud)
        } {
            Ok(()) => {
                if let Some(files) = self.fs_files.as_mut()
                    && let Some(f) = files.get_mut(file.index)
                {
                    f.in_flight += 1;
                }
                Ok(key)
            }
            Err(e) => {
                if let Some(slab) = self.fs_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Submit an fsync for a filesystem file.
    pub(crate) fn fs_fsync(&mut self, file: crate::fs::File) -> io::Result<u32> {
        let fd_index = self.validate_fs_file(file)?;

        let slab = self
            .fs_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;
        let slab_idx = slab
            .allocate(file.index, crate::fs::FsOp::Fsync)
            .ok_or_else(|| io::Error::other("filesystem command slab exhausted"))?;

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::Fs,
            file.index as u32,
            key,
        );

        match self.ring.submit_direct_fsync(fd_index, ud) {
            Ok(()) => {
                if let Some(files) = self.fs_files.as_mut()
                    && let Some(f) = files.get_mut(file.index)
                {
                    f.in_flight += 1;
                }
                Ok(key)
            }
            Err(e) => {
                if let Some(slab) = self.fs_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Close a filesystem file.
    ///
    /// Deregisters the fd from the fixed file table and releases the file slot.
    pub(crate) fn fs_close(&mut self, file: crate::fs::File) -> io::Result<()> {
        let fd_index = self.validate_fs_file(file)?;

        // Unregister from the fixed file table.
        self.ring.register_files_update(fd_index, &[-1i32])?;

        if let Some(files) = self.fs_files.as_mut() {
            files.release(file.index);
        }

        Ok(())
    }

    /// Submit a statx via io_uring.
    ///
    /// Returns the slab_idx (seq number for DiskIoFuture).
    pub(crate) fn fs_stat(&mut self, path: &std::path::Path) -> io::Result<u32> {
        let c_path = crate::fs::path_to_cstring(path)?;

        let slab = self
            .fs_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;
        let slab_idx = slab
            .allocate(0, crate::fs::FsOp::Statx)
            .ok_or_else(|| io::Error::other("filesystem command slab exhausted"))?;

        // Allocate the statx buffer and store it in the slab entry.
        let statx_buf: Box<libc::statx> = Box::new(unsafe { std::mem::zeroed() });
        let statx_ptr = &*statx_buf as *const libc::statx as *mut libc::statx;

        if let Some(entry) = slab.get_mut(slab_idx) {
            entry.path = Some(c_path);
            entry.statx_buf = Some(statx_buf);
        }

        let path_ptr = self
            .fs_cmd_slab
            .as_ref()
            .unwrap()
            .get(slab_idx)
            .unwrap()
            .path
            .as_ref()
            .unwrap()
            .as_ptr();

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(crate::completion::OpTag::Fs, 0, key);

        match unsafe { self.ring.submit_statx(path_ptr, statx_ptr, ud.raw()) } {
            Ok(()) => Ok(key),
            Err(e) => {
                if let Some(slab) = self.fs_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Submit a renameat via io_uring.
    pub(crate) fn fs_rename(
        &mut self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> io::Result<u32> {
        let c_from = crate::fs::path_to_cstring(from)?;
        let c_to = crate::fs::path_to_cstring(to)?;

        let slab = self
            .fs_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;
        let slab_idx = slab
            .allocate(0, crate::fs::FsOp::Rename)
            .ok_or_else(|| io::Error::other("filesystem command slab exhausted"))?;

        if let Some(entry) = slab.get_mut(slab_idx) {
            entry.path = Some(c_from);
            entry.path2 = Some(c_to);
        }

        let (old_ptr, new_ptr) = {
            let entry = self.fs_cmd_slab.as_ref().unwrap().get(slab_idx).unwrap();
            (
                entry.path.as_ref().unwrap().as_ptr(),
                entry.path2.as_ref().unwrap().as_ptr(),
            )
        };

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(crate::completion::OpTag::Fs, 0, key);

        match unsafe { self.ring.submit_renameat(old_ptr, new_ptr, ud.raw()) } {
            Ok(()) => Ok(key),
            Err(e) => {
                if let Some(slab) = self.fs_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Submit an unlinkat via io_uring.
    pub(crate) fn fs_unlink(&mut self, path: &std::path::Path) -> io::Result<u32> {
        let c_path = crate::fs::path_to_cstring(path)?;

        let slab = self
            .fs_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;
        let slab_idx = slab
            .allocate(0, crate::fs::FsOp::Unlink)
            .ok_or_else(|| io::Error::other("filesystem command slab exhausted"))?;

        if let Some(entry) = slab.get_mut(slab_idx) {
            entry.path = Some(c_path);
        }

        let path_ptr = self
            .fs_cmd_slab
            .as_ref()
            .unwrap()
            .get(slab_idx)
            .unwrap()
            .path
            .as_ref()
            .unwrap()
            .as_ptr();

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(crate::completion::OpTag::Fs, 0, key);

        match unsafe { self.ring.submit_unlinkat(path_ptr, 0, ud.raw()) } {
            Ok(()) => Ok(key),
            Err(e) => {
                if let Some(slab) = self.fs_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Submit a mkdirat via io_uring.
    pub(crate) fn fs_mkdir(&mut self, path: &std::path::Path, mode: u32) -> io::Result<u32> {
        let c_path = crate::fs::path_to_cstring(path)?;

        let slab = self
            .fs_cmd_slab
            .as_mut()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;
        let slab_idx = slab
            .allocate(0, crate::fs::FsOp::Mkdir)
            .ok_or_else(|| io::Error::other("filesystem command slab exhausted"))?;

        if let Some(entry) = slab.get_mut(slab_idx) {
            entry.path = Some(c_path);
        }

        let path_ptr = self
            .fs_cmd_slab
            .as_ref()
            .unwrap()
            .get(slab_idx)
            .unwrap()
            .path
            .as_ref()
            .unwrap()
            .as_ptr();

        let key = self.disk_io_key(slab_idx);
        let ud = crate::completion::UserData::encode(crate::completion::OpTag::Fs, 0, key);

        match unsafe { self.ring.submit_mkdirat(path_ptr, mode, ud.raw()) } {
            Ok(()) => Ok(key),
            Err(e) => {
                if let Some(slab) = self.fs_cmd_slab.as_mut() {
                    slab.release(slab_idx);
                }
                Err(e)
            }
        }
    }

    /// Validate a filesystem file handle and return the fd_index.
    fn validate_fs_file(&self, file: crate::fs::File) -> io::Result<u32> {
        let files = self
            .fs_files
            .as_ref()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;
        let f = files
            .get(file.index)
            .ok_or_else(|| io::Error::other("invalid filesystem file handle"))?;
        if f.generation != file.generation {
            return Err(io::Error::other("stale filesystem file handle"));
        }
        Ok(f.fd_index)
    }

    /// Arm a connect timeout for the given connection index.
    #[cfg(has_io_uring)]
    fn arm_connect_timeout(&mut self, conn_index: u32, timeout_ms: u64) {
        let ts = &mut self.connect_timespecs[conn_index as usize];
        *ts = io_uring::types::Timespec::new()
            .sec(timeout_ms / 1000)
            .nsec((timeout_ms % 1000) as u32 * 1_000_000);

        // Carry the connection generation in the payload: a stale -ETIME
        // deferred through CQ overflow could otherwise land after this slot
        // was closed and reused for a NEW outbound connect (again in
        // Connecting state) and kill it with a spurious TimedOut.
        let generation = self
            .connections
            .get(conn_index)
            .map(|c| c.generation)
            .unwrap_or(0);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::Timeout,
            conn_index,
            generation,
        );
        if self.ring.submit_timeout(ts as *const _, ud).is_ok()
            && let Some(cs) = self.connections.get_mut(conn_index)
        {
            cs.connect_timeout_armed = true;
        }
    }
}

// ── mio DriverCtx (minimal stub) ───────────────────────────────────────

#[cfg(not(has_io_uring))]
/// The context provided to handler callbacks for issuing operations.
///
/// This is a short-lived borrow into the driver's internal state.
#[cfg_attr(not(has_io_uring), allow(dead_code))]
pub struct DriverCtx<'a> {
    pub(crate) connections: &'a mut crate::connection::ConnectionTable,
    pub(crate) send_copy_pool: &'a mut SendCopyPool,
    pub(crate) tls_table: *mut crate::tls::TlsTable,
    pub(crate) shutdown_requested: &'a mut bool,
    pub(crate) connect_addrs: &'a mut Vec<libc::sockaddr_storage>,
    pub(crate) tcp_nodelay: bool,
    #[cfg(feature = "timestamps")]
    pub(crate) timestamps: bool,
    #[cfg(feature = "timestamps")]
    pub(crate) recvmsg_msghdr: *const libc::msghdr,
    pub(crate) send_queues: &'a mut Vec<ConnSendState>,
    /// Per-connection pending send buffers (mio backend).
    /// DriverCtx::send() pushes data here; the event loop flushes on writable.
    pub(crate) pending_sends:
        &'a mut Vec<std::collections::VecDeque<crate::backend::mio::driver::PendingSend>>,
    /// Per-connection mio TcpStream storage (for connect / shutdown_write).
    pub(crate) tcp_streams: &'a mut Vec<Option<mio::net::TcpStream>>,
    /// Mio poll instance (for registering new connections).
    pub(crate) poll: &'a mut mio::Poll,
    pub(crate) pending_closes: &'a mut Vec<u32>,
    /// Per-connection writable flag.
    pub(crate) writable: &'a mut Vec<bool>,
    /// Per-connection send completion queue (byte counts for awaitable sends).
    pub(crate) send_completions: &'a mut Vec<std::collections::VecDeque<u32>>,
    /// Per-connection connect timeout deadlines.
    pub(crate) connect_deadlines: &'a mut Vec<Option<std::time::Instant>>,
    pub(crate) sends_dirty: &'a mut Vec<u32>,
    pub(crate) sends_dirty_flag: &'a mut Vec<bool>,
    pub(crate) completions_dirty: &'a mut Vec<u32>,
    pub(crate) completions_dirty_flag: &'a mut Vec<bool>,
    pub(crate) connect_pending: &'a mut u32,
    /// Shared disk I/O pool for filesystem operations.
    pub(crate) disk_io_pool: &'a Option<std::sync::Arc<crate::disk_io_pool::DiskIoPool>>,
    /// Per-worker disk I/O response send channel (included in each request).
    pub(crate) disk_io_tx:
        &'a Option<crossbeam_channel::Sender<crate::disk_io_pool::DiskIoResponse>>,
    /// Wake handle for this worker (used to wake after disk I/O completion).
    pub(crate) wake_handle: crate::wakeup::WakeFd,
    /// Monotonic sequence counter for disk I/O requests.
    pub(crate) next_disk_io_seq: &'a mut u32,
    /// Direct I/O file table.
    pub(crate) direct_io_files: &'a mut Option<crate::direct_io::DirectIoFileTable>,
    /// Raw fds for direct I/O files, indexed by file slot.
    pub(crate) direct_io_fds: &'a mut Vec<Option<std::os::fd::RawFd>>,
    /// Filesystem file table.
    pub(crate) fs_files: &'a mut Option<crate::fs::FsFileTable>,
    /// Raw fds for filesystem files, indexed by file slot.
    pub(crate) fs_fds: &'a mut Vec<Option<std::os::fd::RawFd>>,
    /// Pending fs_open requests: maps seq → file_index.
    pub(crate) pending_fs_opens: &'a mut std::collections::HashMap<u32, u16>,
}

#[cfg(not(has_io_uring))]
impl<'a> DriverCtx<'a> {
    /// Request shutdown of this worker's event loop.
    pub fn request_shutdown(&mut self) {
        *self.shutdown_requested = true;
    }

    /// Get the peer address for a connection.
    pub fn peer_addr(&self, conn: ConnToken) -> Option<crate::connection::PeerAddr> {
        self.connections.get(conn.index)?.peer_addr.clone()
    }

    /// Whether the connection is outbound (initiated by this worker).
    pub fn is_outbound(&self, conn: ConnToken) -> bool {
        self.connections
            .get(conn.index)
            .is_some_and(|cs| cs.outbound)
    }

    /// Send data on a connection (copy into pending send buffer).
    ///
    /// The data is buffered in the per-connection send queue. The event loop
    /// flushes it when the socket becomes writable.
    ///
    /// For TLS connections, data is encrypted and written directly to the
    /// TcpStream (bypassing the pending send queue).
    pub fn send(&mut self, conn: ConnToken, data: &[u8]) -> io::Result<()> {
        let conn_state = self
            .connections
            .get(conn.index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "invalid connection"))?;
        if conn_state.generation != conn.generation {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "stale connection",
            ));
        }

        // TLS path: encrypt and push ciphertext into the pending send queue.
        if !self.tls_table.is_null() {
            let tls_table = unsafe { &mut *self.tls_table };
            if tls_table.has(conn.index) {
                let ciphertext = crate::tls::encrypt_for_send_mio(tls_table, conn.index, data)?;
                if !ciphertext.is_empty() {
                    let idx = conn.index as usize;
                    self.pending_sends[idx].push_back((ciphertext, 0, None));
                    self.mark_send_dirty(idx);
                }
                return Ok(());
            }
        }

        let idx = conn.index as usize;
        self.pending_sends[idx].push_back((data.to_vec(), 0, None));
        self.mark_send_dirty(idx);
        Ok(())
    }

    /// Record `idx` in the dirty-sends list so the event loop's flush pass
    /// visits it. Invariant: non-empty `pending_sends[idx]` ⇒ flag set.
    fn mark_send_dirty(&mut self, idx: usize) {
        if !self.sends_dirty_flag[idx] {
            self.sends_dirty_flag[idx] = true;
            self.sends_dirty.push(idx as u32);
        }
    }

    /// Mark the most recently queued pending send as awaitable: its
    /// completion (`wake_send(Ok(len))`) is delivered when the entry has
    /// fully reached the socket, not at queue time.
    pub(crate) fn mark_last_send_awaited(&mut self, conn_index: u32) {
        let idx = conn_index as usize;
        if let Some((data, offset, notify)) = self.pending_sends[idx].back_mut() {
            *notify = Some((data.len() - *offset) as u32);
        } else {
            // The send was flushed... it can't have been (mio sends are
            // queued, never written inline) — but if the queue is somehow
            // empty, deliver a zero-byte completion so the future resolves.
            self.send_completions[idx].push_back(0);
            if !self.completions_dirty_flag[idx] {
                self.completions_dirty_flag[idx] = true;
                self.completions_dirty.push(idx as u32);
            }
        }
    }

    /// Close a connection. Marks it Closed and defers teardown (socket,
    /// buffers, executor cleanup, slot release) to the event loop's
    /// drain_pending_closes — releasing the slot here left the executor's
    /// parked future, waiter flags, and recv sink alive into the slot's
    /// next occupant.
    pub fn close(&mut self, conn: ConnToken) {
        if let Some(cs) = self.connections.get_mut(conn.index) {
            if cs.generation != conn.generation {
                return;
            }
            if matches!(cs.recv_mode, crate::connection::RecvMode::Closed) {
                return;
            }
            cs.recv_mode = crate::connection::RecvMode::Closed;
        } else {
            return;
        }
        self.pending_closes.push(conn.index);
    }

    /// Get TLS session info for a connection.
    pub fn tls_info(&self, conn: ConnToken) -> Option<crate::tls::TlsInfo> {
        let cs = self.connections.get(conn.index)?;
        if cs.generation != conn.generation {
            return None;
        }
        if self.tls_table.is_null() {
            return None;
        }
        let tls_table = unsafe { &*self.tls_table };
        tls_table.get_info(conn.index)
    }

    /// Shut down the write half of a connection.
    ///
    /// Flushes any buffered pending sends before issuing the TCP half-close.
    pub fn shutdown_write(&mut self, conn: ConnToken) {
        let idx = conn.index as usize;
        if self.connections.get(conn.index).is_none()
            || self.connections.get(conn.index).unwrap().generation != conn.generation
        {
            return;
        }
        // Flush any pending send data before shutting down.
        if let Some(ref mut stream) = self.tcp_streams[idx] {
            use std::io::Write;
            for (data, offset, _notify) in self.pending_sends[idx].drain(..) {
                let _ = stream.write_all(&data[offset..]);
            }
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
        }
    }

    /// Cancel an in-flight operation.
    pub fn cancel(&mut self, _conn: ConnToken) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cancel not supported on mio backend",
        ))
    }

    /// Connect to a remote address.
    pub fn connect(&mut self, addr: SocketAddr) -> Result<ConnToken, crate::error::Error> {
        let conn_index = self
            .connections
            .allocate_outbound()
            .ok_or_else(|| crate::error::Error::Io(io::Error::other("connection table full")))?;

        let mut mio_stream = match mio::net::TcpStream::connect(addr) {
            Ok(s) => s,
            Err(e) => {
                self.connections.release(conn_index);
                return Err(crate::error::Error::Io(e));
            }
        };

        let token = mio::Token(conn_index as usize + 1);
        if let Err(e) = self.poll.registry().register(
            &mut mio_stream,
            token,
            mio::Interest::READABLE | mio::Interest::WRITABLE,
        ) {
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }

        let idx = conn_index as usize;
        self.tcp_streams[idx] = Some(mio_stream);
        self.writable[idx] = false;
        self.pending_sends[idx].clear();
        if let Some(cs) = self.connections.get_mut(conn_index) {
            cs.peer_addr = Some(crate::connection::PeerAddr::Tcp(addr));
        }

        let generation = self.connections.generation(conn_index);
        Ok(ConnToken::new(conn_index, generation))
    }

    /// Connect to a Unix socket.
    pub fn connect_unix(
        &mut self,
        _path: &std::path::Path,
    ) -> Result<ConnToken, crate::error::Error> {
        Err(crate::error::Error::Io(io::Error::other(
            "mio connect_unix not yet implemented",
        )))
    }

    /// Connect with a timeout.
    pub fn connect_with_timeout(
        &mut self,
        addr: SocketAddr,
        timeout_ms: u64,
    ) -> Result<ConnToken, crate::error::Error> {
        let token = self.connect(addr)?;
        if self.connect_deadlines[token.index as usize]
            .replace(std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms))
            .is_none()
        {
            *self.connect_pending += 1;
        }
        Ok(token)
    }

    /// Connect with TLS.
    pub fn connect_tls(
        &mut self,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<ConnToken, crate::error::Error> {
        if self.tls_table.is_null() {
            return Err(crate::error::Error::RingSetup(
                "TLS not configured".to_string(),
            ));
        }
        let tls_table = unsafe { &mut *self.tls_table };
        if !tls_table.has_client_config() {
            return Err(crate::error::Error::RingSetup(
                "TLS client config not set".to_string(),
            ));
        }

        // Perform the TCP connect first.
        let token = self.connect(addr)?;

        // Create TLS client state (buffers ClientHello internally). On
        // failure the already-established TCP connection must be torn down —
        // returning early here used to leak the slot and the registered
        // stream on every failed TLS connect attempt.
        let sni = match rustls::pki_types::ServerName::try_from(server_name.to_owned()) {
            Ok(sni) => sni,
            Err(e) => {
                self.close(token);
                return Err(crate::error::Error::RingSetup(format!(
                    "invalid server name: {e}"
                )));
            }
        };
        if let Err(e) = tls_table.create_client(token.index, sni) {
            self.close(token);
            return Err(crate::error::Error::RingSetup(format!(
                "TLS client setup failed: {e}"
            )));
        }

        Ok(token)
    }

    /// Connect with TLS and a timeout.
    pub fn connect_tls_with_timeout(
        &mut self,
        addr: SocketAddr,
        server_name: &str,
        timeout_ms: u64,
    ) -> Result<ConnToken, crate::error::Error> {
        let token = self.connect_tls(addr, server_name)?;
        if self.connect_deadlines[token.index as usize]
            .replace(std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms))
            .is_none()
        {
            *self.connect_pending += 1;
        }
        Ok(token)
    }

    /// Open an NVMe device (not supported on mio backend).
    pub fn open_nvme_device(
        &mut self,
        _path: &str,
        _nsid: u32,
    ) -> io::Result<crate::nvme::NvmeDevice> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "NVMe passthrough requires the io_uring backend",
        ))
    }

    /// NVMe read (not supported on mio backend).
    ///
    /// # Safety
    ///
    /// Mirrors the io_uring backend's contract (caller-supplied DMA address
    /// must be valid and outlive the operation); this stub always errors.
    pub unsafe fn nvme_read(
        &mut self,
        _device: crate::nvme::NvmeDevice,
        _lba: u64,
        _num_blocks: u16,
        _buf_addr: u64,
        _buf_len: u32,
    ) -> io::Result<u32> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "NVMe passthrough requires the io_uring backend",
        ))
    }

    /// NVMe write (not supported on mio backend).
    ///
    /// # Safety
    ///
    /// Mirrors the io_uring backend's contract; this stub always errors.
    pub unsafe fn nvme_write(
        &mut self,
        _device: crate::nvme::NvmeDevice,
        _lba: u64,
        _num_blocks: u16,
        _buf_addr: u64,
        _buf_len: u32,
    ) -> io::Result<u32> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "NVMe passthrough requires the io_uring backend",
        ))
    }

    /// NVMe flush (not supported on mio backend).
    pub fn nvme_flush(&mut self, _device: crate::nvme::NvmeDevice) -> io::Result<u32> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "NVMe passthrough requires the io_uring backend",
        ))
    }

    // ── Direct I/O methods ────────────────────────────────────────────

    /// Allocate a sequence number and submit work to the disk I/O pool.
    fn submit_disk_io(
        &mut self,
        work: Box<dyn FnOnce() -> crate::disk_io_pool::DiskIoResult + Send>,
    ) -> io::Result<u32> {
        let pool = self
            .disk_io_pool
            .as_ref()
            .ok_or_else(|| io::Error::other("disk I/O pool not configured"))?;
        let tx = self
            .disk_io_tx
            .as_ref()
            .ok_or_else(|| io::Error::other("disk I/O pool not configured"))?;

        let seq = *self.next_disk_io_seq;
        *self.next_disk_io_seq = seq.wrapping_add(1);

        pool.request_tx
            .send(crate::disk_io_pool::DiskIoRequest {
                work,
                seq,
                response_tx: tx.clone(),
                wake_handle: self.wake_handle,
            })
            .map_err(|_| io::Error::other("disk I/O pool shut down"))?;

        Ok(seq)
    }

    /// Open a file for direct I/O.
    ///
    /// On Linux, the file is opened with `O_DIRECT`. On macOS, `fcntl(F_NOCACHE)`
    /// is used as an approximation. This is synchronous (matching io_uring behavior
    /// where the fd is needed immediately).
    pub fn open_direct_io_file(
        &mut self,
        path: &str,
    ) -> io::Result<crate::direct_io::DirectIoFile> {
        let files = self
            .direct_io_files
            .as_mut()
            .ok_or_else(|| io::Error::other("direct I/O not configured"))?;

        let index = files
            .allocate()
            .ok_or_else(|| io::Error::other("direct I/O file table full"))?;

        let c_path =
            std::ffi::CString::new(path).map_err(|_| io::Error::other("invalid file path"))?;

        #[cfg(target_os = "linux")]
        let flags = libc::O_RDWR | libc::O_DIRECT;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::O_RDWR;

        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd < 0 {
            files.release(index);
            return Err(io::Error::last_os_error());
        }

        // On macOS, use F_NOCACHE to bypass the page cache.
        #[cfg(target_os = "macos")]
        {
            unsafe {
                libc::fcntl(fd, libc::F_NOCACHE, 1);
            }
        }

        // Store the fd.
        if let Some(f) = files.get_mut(index) {
            f.fd_index = fd as u32;
        }
        self.direct_io_fds[index as usize] = Some(fd);

        let generation = files.get(index).map(|f| f.generation).unwrap_or(0);
        Ok(crate::direct_io::DirectIoFile { index, generation })
    }

    /// Submit a direct I/O read via the disk I/O pool.
    ///
    /// Returns the sequence number for correlation with `DiskIoFuture`.
    pub fn direct_io_read(
        &mut self,
        file: crate::direct_io::DirectIoFile,
        offset: u64,
        buf: *mut u8,
        len: u32,
    ) -> io::Result<u32> {
        let fd = self.validate_direct_io_file(file)?;
        let buf_addr = buf as usize;
        let work = Box::new(move || {
            let result = unsafe {
                libc::pread(
                    fd,
                    buf_addr as *mut libc::c_void,
                    len as usize,
                    offset as libc::off_t,
                )
            };
            let r = if result < 0 {
                -(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            } else {
                result as i32
            };
            crate::disk_io_pool::DiskIoResult {
                result: r,
                metadata: None,
            }
        });
        self.submit_disk_io(work)
    }

    /// Submit a direct I/O write via the disk I/O pool.
    ///
    /// Returns the sequence number for correlation with `DiskIoFuture`.
    pub fn direct_io_write(
        &mut self,
        file: crate::direct_io::DirectIoFile,
        offset: u64,
        buf: *const u8,
        len: u32,
    ) -> io::Result<u32> {
        let fd = self.validate_direct_io_file(file)?;
        let buf_addr = buf as usize;
        let work = Box::new(move || {
            let result = unsafe {
                libc::pwrite(
                    fd,
                    buf_addr as *const libc::c_void,
                    len as usize,
                    offset as libc::off_t,
                )
            };
            let r = if result < 0 {
                -(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            } else {
                result as i32
            };
            crate::disk_io_pool::DiskIoResult {
                result: r,
                metadata: None,
            }
        });
        self.submit_disk_io(work)
    }

    /// Submit an fsync for a direct I/O file via the disk I/O pool.
    pub fn direct_io_fsync(&mut self, file: crate::direct_io::DirectIoFile) -> io::Result<u32> {
        let fd = self.validate_direct_io_file(file)?;
        let work = Box::new(move || {
            let result = unsafe { libc::fsync(fd) };
            let r = if result < 0 {
                -(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            } else {
                0
            };
            crate::disk_io_pool::DiskIoResult {
                result: r,
                metadata: None,
            }
        });
        self.submit_disk_io(work)
    }

    /// Close a direct I/O file. Synchronous — closes the fd and releases the slot.
    pub fn close_direct_io_file(&mut self, file: crate::direct_io::DirectIoFile) -> io::Result<()> {
        let fd = self.validate_direct_io_file(file)?;
        unsafe {
            libc::close(fd);
        }
        self.direct_io_fds[file.index as usize] = None;
        if let Some(files) = self.direct_io_files.as_mut() {
            files.release(file.index);
        }
        Ok(())
    }

    /// Validate a direct I/O file handle and return the raw fd.
    fn validate_direct_io_file(
        &self,
        file: crate::direct_io::DirectIoFile,
    ) -> io::Result<std::os::fd::RawFd> {
        let files = self
            .direct_io_files
            .as_ref()
            .ok_or_else(|| io::Error::other("direct I/O not configured"))?;
        let f = files
            .get(file.index)
            .ok_or_else(|| io::Error::other("invalid direct I/O file handle"))?;
        if f.generation != file.generation {
            return Err(io::Error::other("stale direct I/O file handle"));
        }
        self.direct_io_fds[file.index as usize]
            .ok_or_else(|| io::Error::other("direct I/O file fd not found"))
    }

    // ── Filesystem I/O methods ────────────────────────────────────────

    /// Open a file via the disk I/O pool.
    ///
    /// The open is dispatched to the pool. On completion, the pool sends back
    /// the fd (as the i32 result). The event loop stores the fd in `fs_fds`
    /// when it drains the response.
    ///
    /// Returns `(file_index, generation, seq)`.
    pub(crate) fn fs_open(
        &mut self,
        path: &std::path::Path,
        flags: crate::fs::OpenFlags,
        mode: u32,
    ) -> io::Result<(u16, u16, u32)> {
        let files = self
            .fs_files
            .as_mut()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;

        let file_index = files
            .allocate()
            .ok_or_else(|| io::Error::other("filesystem file table full"))?;

        let generation = files.get(file_index).map(|f| f.generation).unwrap_or(0);

        let c_path = crate::fs::path_to_cstring(path).inspect_err(|_| {
            self.fs_files.as_mut().unwrap().release(file_index);
        })?;

        let open_flags = flags.0;
        let work = Box::new(move || {
            let fd = unsafe { libc::open(c_path.as_ptr(), open_flags, mode as libc::c_int) };
            if fd < 0 {
                let errno = io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO);
                crate::disk_io_pool::DiskIoResult {
                    result: -errno,
                    metadata: None,
                }
            } else {
                // Return the fd as the result (positive value).
                crate::disk_io_pool::DiskIoResult {
                    result: fd,
                    metadata: None,
                }
            }
        });

        match self.submit_disk_io(work) {
            Ok(seq) => {
                self.pending_fs_opens.insert(seq, file_index);
                Ok((file_index, generation, seq))
            }
            Err(e) => {
                self.fs_files.as_mut().unwrap().release(file_index);
                Err(e)
            }
        }
    }

    /// Submit a filesystem read via the disk I/O pool.
    pub(crate) unsafe fn fs_read(
        &mut self,
        file: crate::fs::File,
        offset: u64,
        buf: *mut u8,
        len: u32,
    ) -> io::Result<u32> {
        let fd = self.validate_fs_file(file)?;
        let buf_addr = buf as usize;
        let work = Box::new(move || {
            let result = unsafe {
                libc::pread(
                    fd,
                    buf_addr as *mut libc::c_void,
                    len as usize,
                    offset as libc::off_t,
                )
            };
            let r = if result < 0 {
                -(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            } else {
                result as i32
            };
            crate::disk_io_pool::DiskIoResult {
                result: r,
                metadata: None,
            }
        });
        self.submit_disk_io(work)
    }

    /// Submit a filesystem write via the disk I/O pool.
    pub(crate) unsafe fn fs_write(
        &mut self,
        file: crate::fs::File,
        offset: u64,
        buf: *const u8,
        len: u32,
    ) -> io::Result<u32> {
        let fd = self.validate_fs_file(file)?;
        let buf_addr = buf as usize;
        let work = Box::new(move || {
            let result = unsafe {
                libc::pwrite(
                    fd,
                    buf_addr as *const libc::c_void,
                    len as usize,
                    offset as libc::off_t,
                )
            };
            let r = if result < 0 {
                -(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            } else {
                result as i32
            };
            crate::disk_io_pool::DiskIoResult {
                result: r,
                metadata: None,
            }
        });
        self.submit_disk_io(work)
    }

    /// Submit an fsync for a filesystem file via the disk I/O pool.
    pub(crate) fn fs_fsync(&mut self, file: crate::fs::File) -> io::Result<u32> {
        let fd = self.validate_fs_file(file)?;
        let work = Box::new(move || {
            let result = unsafe { libc::fsync(fd) };
            let r = if result < 0 {
                -(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            } else {
                0
            };
            crate::disk_io_pool::DiskIoResult {
                result: r,
                metadata: None,
            }
        });
        self.submit_disk_io(work)
    }

    /// Close a filesystem file. Synchronous — closes the fd and releases the slot.
    pub(crate) fn fs_close(&mut self, file: crate::fs::File) -> io::Result<()> {
        let fd = self.validate_fs_file(file)?;
        unsafe {
            libc::close(fd);
        }
        self.fs_fds[file.index as usize] = None;
        if let Some(files) = self.fs_files.as_mut() {
            files.release(file.index);
        }
        Ok(())
    }

    /// Submit a stat via the disk I/O pool.
    ///
    /// Uses `libc::stat` (portable) instead of `statx` (Linux-only). The
    /// result is converted to `crate::fs::Metadata` inside the pool closure
    /// and delivered via `DiskIoResponse::metadata`.
    pub(crate) fn fs_stat(&mut self, path: &std::path::Path) -> io::Result<u32> {
        let c_path = crate::fs::path_to_cstring(path)?;
        let work = Box::new(move || {
            let mut stat_buf: libc::stat = unsafe { std::mem::zeroed() };
            let result = unsafe { libc::stat(c_path.as_ptr(), &mut stat_buf) };
            if result < 0 {
                let errno = io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO);
                crate::disk_io_pool::DiskIoResult {
                    result: -errno,
                    metadata: None,
                }
            } else {
                let metadata = crate::fs::Metadata::from_stat(&stat_buf);
                crate::disk_io_pool::DiskIoResult {
                    result: 0,
                    metadata: Some(metadata),
                }
            }
        });
        self.submit_disk_io(work)
    }

    /// Submit a rename via the disk I/O pool.
    pub(crate) fn fs_rename(
        &mut self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> io::Result<u32> {
        let c_from = crate::fs::path_to_cstring(from)?;
        let c_to = crate::fs::path_to_cstring(to)?;
        let work = Box::new(move || {
            let result = unsafe { libc::rename(c_from.as_ptr(), c_to.as_ptr()) };
            let r = if result < 0 {
                -(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            } else {
                0
            };
            crate::disk_io_pool::DiskIoResult {
                result: r,
                metadata: None,
            }
        });
        self.submit_disk_io(work)
    }

    /// Submit an unlink via the disk I/O pool.
    pub(crate) fn fs_unlink(&mut self, path: &std::path::Path) -> io::Result<u32> {
        let c_path = crate::fs::path_to_cstring(path)?;
        let work = Box::new(move || {
            let result = unsafe { libc::unlink(c_path.as_ptr()) };
            let r = if result < 0 {
                -(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            } else {
                0
            };
            crate::disk_io_pool::DiskIoResult {
                result: r,
                metadata: None,
            }
        });
        self.submit_disk_io(work)
    }

    /// Submit a mkdir via the disk I/O pool.
    pub(crate) fn fs_mkdir(&mut self, path: &std::path::Path, mode: u32) -> io::Result<u32> {
        let c_path = crate::fs::path_to_cstring(path)?;
        let work = Box::new(move || {
            let result = unsafe { libc::mkdir(c_path.as_ptr(), mode as libc::mode_t) };
            let r = if result < 0 {
                -(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            } else {
                0
            };
            crate::disk_io_pool::DiskIoResult {
                result: r,
                metadata: None,
            }
        });
        self.submit_disk_io(work)
    }

    /// Validate a filesystem file handle and return the raw fd.
    fn validate_fs_file(&self, file: crate::fs::File) -> io::Result<std::os::fd::RawFd> {
        let files = self
            .fs_files
            .as_ref()
            .ok_or_else(|| io::Error::other("filesystem I/O not configured"))?;
        let f = files
            .get(file.index)
            .ok_or_else(|| io::Error::other("invalid filesystem file handle"))?;
        if f.generation != file.generation {
            return Err(io::Error::other("stale filesystem file handle"));
        }
        self.fs_fds[file.index as usize]
            .ok_or_else(|| io::Error::other("filesystem file fd not found"))
    }
}

/// A prepared send operation with its associated resources, ready for submission.
#[cfg_attr(not(has_io_uring), allow(dead_code))]
pub(crate) struct BuiltSend {
    /// The io_uring SQE to submit.
    #[cfg(has_io_uring)]
    pub entry: io_uring::squeue::Entry,
    /// SendCopyPool slot index. u16::MAX if none.
    pub pool_slot: u16,
    /// InFlightSendSlab index. u16::MAX if none (only for SendMsgZc).
    #[cfg(has_io_uring)]
    pub slab_idx: u16,
    /// Total bytes this send will transmit.
    pub total_len: u32,
}

/// A pre-classified part for scatter-gather sends via `submit_batch`.
///
/// Used to build mixed copy + zero-copy guard sends without the lifetime
/// constraints of the closure-based builder API.
pub enum SendPart<'a> {
    /// Data to be copied into the send pool on submit.
    Copy(&'a [u8]),
    /// Zero-copy guard — ownership is transferred to the kernel on submit.
    Guard(GuardBox),
}

#[cfg(has_io_uring)]
/// Part type in a scatter-gather send.
#[derive(Clone, Copy)]
enum PartSlot {
    Empty,
    Copy { slice_idx: u8 },
    Guard { guard_idx: u8 },
}

#[cfg(has_io_uring)]
/// Builder for scatter-gather sends with mixed copy + zero-copy guard parts.
pub struct SendBuilder<'b, 'a> {
    ctx: &'b mut DriverCtx<'a>,
    conn: ConnToken,
    parts: [PartSlot; MAX_IOVECS],
    part_count: u8,
    copy_slices: [(*const u8, usize); MAX_IOVECS],
    copy_count: u8,
    total_copy_len: usize,
    guards: [Option<GuardBox>; MAX_GUARDS],
    guard_count: u8,
    total_len: u32,
    error: Option<io::Error>,
}

#[cfg(has_io_uring)]
impl<'b, 'a> SendBuilder<'b, 'a> {
    /// Add a copy part. The data will be copied into the send pool on `submit()`.
    /// The data reference must outlive the builder (guaranteed by the `'b` lifetime).
    pub fn copy(mut self, data: &'b [u8]) -> Self {
        if self.error.is_some() {
            return self;
        }
        if self.part_count as usize >= MAX_IOVECS {
            self.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many send parts (max 32)",
            ));
            return self;
        }
        let idx = self.copy_count;
        self.copy_slices[idx as usize] = (data.as_ptr(), data.len());
        self.copy_count += 1;
        self.parts[self.part_count as usize] = PartSlot::Copy { slice_idx: idx };
        self.part_count += 1;
        self.total_len += data.len() as u32;
        self.total_copy_len += data.len();
        self
    }

    /// Add a zero-copy guard part. The guard keeps the memory alive until the kernel
    /// releases it via the ZC notification.
    pub fn guard(mut self, guard: GuardBox) -> Self {
        if self.error.is_some() {
            return self;
        }
        if self.part_count as usize >= MAX_IOVECS {
            self.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many send parts (max 32)",
            ));
            return self;
        }
        if self.guard_count as usize >= MAX_GUARDS {
            self.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many guards (max 8)",
            ));
            return self;
        }
        let (_, len) = guard.as_ptr_len();
        let gidx = self.guard_count;
        self.guards[gidx as usize] = Some(guard);
        self.guard_count += 1;
        self.parts[self.part_count as usize] = PartSlot::Guard { guard_idx: gidx };
        self.part_count += 1;
        self.total_len += len;
        self
    }

    /// Submit the scatter-gather send.
    pub fn submit(mut self) -> io::Result<()> {
        if let Some(e) = self.error.take() {
            return Err(e);
        }

        if self.part_count == 0 {
            return Ok(());
        }

        // Validate connection + generation.
        let conn_state = self
            .ctx
            .connections
            .get(self.conn.index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "invalid connection"))?;
        if conn_state.generation != self.conn.generation {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "stale connection",
            ));
        }

        // TLS path: gather all data, encrypt, copy-send. Drop guards immediately.
        if !self.ctx.tls_table.is_null() {
            let tls_table = unsafe { &mut *self.ctx.tls_table };
            if tls_table.get_mut(self.conn.index).is_some() {
                return self.submit_tls(tls_table);
            }
        }

        // No guards: gather all copy parts into one pool slot, submit as regular Send.
        if self.guard_count == 0 {
            return self.submit_copy_only();
        }

        // Small guard sends: ZC bookkeeping (slab entry + notification CQE) costs
        // more than a memcpy below the threshold. Gather everything — guard memory
        // included — into one pool slot and submit a plain Send. Guards drop
        // immediately (data is copied out before return). Falls through to the ZC
        // path when the pool is exhausted or the gather doesn't fit a slot.
        let threshold = self.ctx.send_zc_threshold;
        if threshold > 0 && self.total_len < threshold && self.submit_small_gather()? {
            return Ok(());
        }

        // With guards: build iovecs mixing copy pool subranges and guard pointers.
        self.submit_with_guards()
    }

    /// TLS fallback: gather all data into a contiguous buffer, encrypt, copy-send.
    fn submit_tls(mut self, tls_table: &mut crate::tls::TlsTable) -> io::Result<()> {
        let mut plaintext = Vec::with_capacity(self.total_len as usize);
        for i in 0..self.part_count as usize {
            match self.parts[i] {
                PartSlot::Copy { slice_idx } => {
                    let (ptr, len) = self.copy_slices[slice_idx as usize];
                    let data = unsafe { std::slice::from_raw_parts(ptr, len) };
                    plaintext.extend_from_slice(data);
                }
                PartSlot::Guard { guard_idx } => {
                    if let Some(ref g) = self.guards[guard_idx as usize] {
                        let (ptr, len) = g.as_ptr_len();
                        let data = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
                        plaintext.extend_from_slice(data);
                    }
                }
                PartSlot::Empty => {}
            }
        }
        // Drop guards — TLS encrypted copy-send doesn't need ZC
        for g in self.guards.iter_mut() {
            *g = None;
        }
        let sends = crate::tls::encrypt_to_sends(
            tls_table,
            self.ctx.send_copy_pool,
            self.conn.index,
            &plaintext,
        )?;
        self.ctx.queue_built_sends(self.conn.index, sends)
    }

    /// Copy-only path: gather all copy parts into one pool slot, return built SQE.
    fn build_copy_only(&mut self) -> io::Result<BuiltSend> {
        let (slot, ptr, len) = unsafe {
            self.ctx.send_copy_pool.copy_in_gather(
                &self.copy_slices[..self.copy_count as usize],
                self.total_copy_len,
            )
        }
        .ok_or_else(|| io::Error::other("send copy pool exhausted"))?;

        let user_data = crate::completion::UserData::encode(
            crate::completion::OpTag::Send,
            self.conn.index,
            slot as u32,
        );
        let entry = io_uring::opcode::Send::new(io_uring::types::Fixed(self.conn.index), ptr, len)
            .flags(crate::completion::STREAM_SEND_FLAGS)
            .build()
            .user_data(user_data.raw());

        Ok(BuiltSend {
            entry,
            pool_slot: slot,
            slab_idx: u16::MAX,
            total_len: self.total_len,
        })
    }

    /// Mixed copy+guard path: allocate pool slot + slab entry, return built SQE.
    #[allow(clippy::needless_range_loop)]
    fn build_with_guards(&mut self) -> io::Result<BuiltSend> {
        let slot_size = self.ctx.send_copy_pool.slot_size() as usize;
        if self.total_copy_len > slot_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "total copy data exceeds send pool slot size",
            ));
        }

        if self.total_copy_len > 0 {
            let (slot, pool_ptr, _pool_len) = unsafe {
                self.ctx.send_copy_pool.copy_in_gather(
                    &self.copy_slices[..self.copy_count as usize],
                    self.total_copy_len,
                )
            }
            .ok_or_else(|| io::Error::other("send copy pool exhausted"))?;

            let mut iovecs = [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS];
            let mut copy_offset: usize = 0;
            for i in 0..self.part_count as usize {
                match self.parts[i] {
                    PartSlot::Copy { slice_idx } => {
                        let (_src_ptr, src_len) = self.copy_slices[slice_idx as usize];
                        iovecs[i] = libc::iovec {
                            iov_base: pool_ptr.wrapping_add(copy_offset) as *mut _,
                            iov_len: src_len,
                        };
                        copy_offset += src_len;
                    }
                    PartSlot::Guard { guard_idx } => {
                        let g = self.guards[guard_idx as usize].as_ref().unwrap();
                        let (gptr, glen) = g.as_ptr_len();
                        let region = g.region();
                        if region != crate::buffer::fixed::RegionId::UNREGISTERED {
                            self.ctx
                                .fixed_buffers
                                .validate_region_ptr(region, gptr, glen)
                                .map_err(|e| {
                                    self.ctx.send_copy_pool.release(slot);
                                    io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
                                })?;
                        }
                        iovecs[i] = libc::iovec {
                            iov_base: gptr as *mut _,
                            iov_len: glen as usize,
                        };
                    }
                    PartSlot::Empty => {}
                }
            }

            // Take guards out of self (moved into slab).
            let guards = std::mem::take(&mut self.guards);
            let iov_slice = &iovecs[..self.part_count as usize];
            let total_len = self.total_len;
            let (slab_idx, msg_ptr) = self
                .ctx
                .send_slab
                .allocate(
                    self.conn.index,
                    iov_slice,
                    slot,
                    guards,
                    self.guard_count,
                    total_len,
                )
                .ok_or_else(|| {
                    self.ctx.send_copy_pool.release(slot);
                    io::Error::other("send slab exhausted")
                })?;

            let user_data = crate::completion::UserData::encode(
                crate::completion::OpTag::SendMsgZc,
                self.conn.index,
                slab_idx as u32,
            );
            let entry =
                io_uring::opcode::SendMsgZc::new(io_uring::types::Fixed(self.conn.index), msg_ptr)
                    .build()
                    .user_data(user_data.raw());

            Ok(BuiltSend {
                entry,
                pool_slot: slot,
                slab_idx,
                total_len,
            })
        } else {
            // No copy data, only guards.
            let mut iovecs = [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS];
            for i in 0..self.part_count as usize {
                if let PartSlot::Guard { guard_idx } = self.parts[i] {
                    let g = self.guards[guard_idx as usize].as_ref().unwrap();
                    let (gptr, glen) = g.as_ptr_len();
                    let region = g.region();
                    if region != crate::buffer::fixed::RegionId::UNREGISTERED {
                        self.ctx
                            .fixed_buffers
                            .validate_region_ptr(region, gptr, glen)
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
                            })?;
                    }
                    iovecs[i] = libc::iovec {
                        iov_base: gptr as *mut _,
                        iov_len: glen as usize,
                    };
                }
            }

            let guards = std::mem::take(&mut self.guards);
            let iov_slice = &iovecs[..self.part_count as usize];
            let total_len = self.total_len;
            let (slab_idx, msg_ptr) = self
                .ctx
                .send_slab
                .allocate(
                    self.conn.index,
                    iov_slice,
                    u16::MAX,
                    guards,
                    self.guard_count,
                    total_len,
                )
                .ok_or_else(|| io::Error::other("send slab exhausted"))?;

            let user_data = crate::completion::UserData::encode(
                crate::completion::OpTag::SendMsgZc,
                self.conn.index,
                slab_idx as u32,
            );
            let entry =
                io_uring::opcode::SendMsgZc::new(io_uring::types::Fixed(self.conn.index), msg_ptr)
                    .build()
                    .user_data(user_data.raw());

            Ok(BuiltSend {
                entry,
                pool_slot: u16::MAX,
                slab_idx,
                total_len,
            })
        }
    }

    /// Copy-only path: gather all copy parts, submit or queue.
    fn submit_copy_only(mut self) -> io::Result<()> {
        let built = self.build_copy_only()?;
        self.ctx.submit_or_queue(self.conn.index, built)
    }

    /// Small-send fallback: gather all parts (copy + guard memory, in part order)
    /// into one pool slot and submit as a plain `Send`. Guard memory is copied,
    /// so the guards are dropped on return instead of being held in the slab.
    ///
    /// Returns `Ok(false)` without consuming anything when the pool has no free
    /// slot or the gather exceeds the slot size — the caller falls through to
    /// the zero-copy path.
    #[allow(clippy::needless_range_loop)]
    fn submit_small_gather(&mut self) -> io::Result<bool> {
        // Build the gather list in part order, mixing copy slices and guard memory.
        let mut slices: [(*const u8, usize); MAX_IOVECS] = [(std::ptr::null(), 0); MAX_IOVECS];
        let mut n = 0usize;
        for i in 0..self.part_count as usize {
            match self.parts[i] {
                PartSlot::Copy { slice_idx } => {
                    slices[n] = self.copy_slices[slice_idx as usize];
                    n += 1;
                }
                PartSlot::Guard { guard_idx } => {
                    let g = self.guards[guard_idx as usize]
                        .as_ref()
                        .expect("guard slot must be Some in submit_small_gather");
                    let (ptr, len) = g.as_ptr_len();
                    slices[n] = (ptr, len as usize);
                    n += 1;
                }
                PartSlot::Empty => {}
            }
        }

        // SAFETY: every (ptr, len) in `slices[..n]` is live — copy slices borrow
        // caller data that outlives the builder, guard memory is owned by
        // `self.guards` until this method returns.
        // `self.total_len` equals the sum of all gathered part lengths, so the
        // slot's `remaining` accounting matches the bytes actually copied in.
        let Some((slot, ptr, len)) = (unsafe {
            self.ctx
                .send_copy_pool
                .copy_in_gather(&slices[..n], self.total_len as usize)
        }) else {
            // Pool exhausted or gather exceeds slot size: keep guards, use ZC path.
            return Ok(false);
        };

        let user_data = crate::completion::UserData::encode(
            crate::completion::OpTag::Send,
            self.conn.index,
            slot as u32,
        );
        let entry = io_uring::opcode::Send::new(io_uring::types::Fixed(self.conn.index), ptr, len)
            .flags(crate::completion::STREAM_SEND_FLAGS)
            .build()
            .user_data(user_data.raw());

        let built = BuiltSend {
            entry,
            pool_slot: slot,
            slab_idx: u16::MAX,
            total_len: self.total_len,
        };
        // Data is in the pool slot now — guards can die with `self` after return.
        self.ctx.submit_or_queue(self.conn.index, built)?;
        Ok(true)
    }

    /// Mixed copy+guard path: submit or queue.
    fn submit_with_guards(mut self) -> io::Result<()> {
        let built = self.build_with_guards()?;
        self.ctx.submit_or_queue(self.conn.index, built)
    }
}

#[cfg(has_io_uring)]
/// Builder for submitting multiple SQEs as a linked IO_LINK chain.
///
/// Collects send operations (copy-only or scatter-gather) and submits them
/// as an atomic chain. The kernel executes linked SQEs sequentially. If any
/// SQE fails, subsequent linked SQEs are cancelled with -ECANCELED.
///
/// Created via [`DriverCtx::send_chain`].
pub struct SendChainBuilder<'b, 'a> {
    ctx: &'b mut DriverCtx<'a>,
    conn: ConnToken,
    built: Vec<BuiltSend>,
    total_bytes: u32,
    error: Option<io::Error>,
    finished: bool,
}

#[cfg(has_io_uring)]
impl<'b, 'a> SendChainBuilder<'b, 'a> {
    /// Add a copy-only send to the chain.
    pub fn copy(mut self, data: &[u8]) -> Self {
        if self.error.is_some() {
            return self;
        }
        if self.built.len() >= self.ctx.max_chain_length as usize {
            self.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "chain exceeds max_chain_length",
            ));
            return self;
        }

        let (slot, ptr, len) = match self.ctx.send_copy_pool.copy_in(data) {
            Some(v) => v,
            None => {
                self.error = Some(io::Error::other("send copy pool exhausted"));
                return self;
            }
        };

        let user_data = crate::completion::UserData::encode(
            crate::completion::OpTag::Send,
            self.conn.index,
            slot as u32,
        );
        let entry = io_uring::opcode::Send::new(io_uring::types::Fixed(self.conn.index), ptr, len)
            .flags(crate::completion::STREAM_SEND_FLAGS)
            .build()
            .user_data(user_data.raw());

        self.total_bytes += data.len() as u32;
        self.built.push(BuiltSend {
            entry,
            pool_slot: slot,
            slab_idx: u16::MAX,
            total_len: data.len() as u32,
        });
        self
    }

    /// Begin a scatter-gather send within the chain.
    /// Returns a [`ChainPartsBuilder`] that collects copy + guard parts
    /// for a single SendMsgZc SQE.
    pub fn parts(self) -> ChainPartsBuilder<'b, 'a> {
        ChainPartsBuilder {
            chain: self,
            parts: [PartSlot::Empty; MAX_IOVECS],
            part_count: 0,
            copy_slices: [(std::ptr::null(), 0); MAX_IOVECS],
            copy_count: 0,
            total_copy_len: 0,
            guards: [const { None }; crate::buffer::send_slab::MAX_GUARDS],
            guard_count: 0,
            total_len: 0,
        }
    }

    /// Finalize and submit the chain.
    ///
    /// All SQEs are linked with IO_LINK except the last. Registers chain
    /// state in the SendChainTable for CQE tracking.
    pub fn finish(mut self) -> io::Result<()> {
        if let Some(e) = self.error.take() {
            // finished stays false — Drop will call release_all().
            return Err(e);
        }

        let count = self.built.len();
        if count == 0 {
            self.finished = true;
            return Ok(());
        }

        let total_bytes = self.total_bytes;
        let conn_index = self.conn.index;

        // Clone the SQE entries for submission, keeping self.built intact.
        // On failure, Drop will call release_all() on the original entries.
        if count == 1 {
            let entry = self.built[0].entry.clone();
            unsafe {
                self.ctx.ring.push_sqe(entry)?;
            }
            self.ctx.chain_table.start(conn_index, 1, total_bytes);
        } else {
            let mut entries: Vec<io_uring::squeue::Entry> =
                self.built.iter().map(|b| b.entry.clone()).collect();
            unsafe {
                self.ctx.ring.push_sqe_chain(&mut entries)?;
            }
            self.ctx
                .chain_table
                .start(conn_index, count as u16, total_bytes);
        }

        // Submission succeeded — resources now owned by kernel/CQE handlers.
        self.built.clear();

        self.finished = true;
        Ok(())
    }

    /// Release all allocated resources (pool slots and slab entries).
    fn release_all(&mut self) {
        for built in self.built.drain(..) {
            if built.slab_idx != u16::MAX {
                let pool_slot = self.ctx.send_slab.release(built.slab_idx);
                if pool_slot != u16::MAX {
                    self.ctx.send_copy_pool.release(pool_slot);
                }
            } else if built.pool_slot != u16::MAX {
                self.ctx.send_copy_pool.release(built.pool_slot);
            }
        }
    }
}

#[cfg(has_io_uring)]
impl Drop for SendChainBuilder<'_, '_> {
    fn drop(&mut self) {
        if !self.finished {
            self.release_all();
        }
    }
}

#[cfg(has_io_uring)]
/// Sub-builder for a scatter-gather SQE within a [`SendChainBuilder`] chain.
///
/// Created via [`SendChainBuilder::parts`]. Call `.copy()` and `.guard()`
/// to add parts, then `.add()` to finalize and return to the chain builder.
pub struct ChainPartsBuilder<'b, 'a> {
    chain: SendChainBuilder<'b, 'a>,
    parts: [PartSlot; MAX_IOVECS],
    part_count: u8,
    copy_slices: [(*const u8, usize); MAX_IOVECS],
    copy_count: u8,
    total_copy_len: usize,
    guards: [Option<GuardBox>; MAX_GUARDS],
    guard_count: u8,
    total_len: u32,
}

#[cfg(has_io_uring)]
impl<'b, 'a> ChainPartsBuilder<'b, 'a> {
    /// Add a copy part to this scatter-gather SQE. The data reference must
    /// outlive the builder (guaranteed by the `'b` lifetime) — the bytes are
    /// only read when `.add()` gathers them into the send pool.
    pub fn copy(mut self, data: &'b [u8]) -> Self {
        if self.chain.error.is_some() {
            return self;
        }
        if self.part_count as usize >= MAX_IOVECS {
            self.chain.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many send parts (max 32)",
            ));
            return self;
        }
        let idx = self.copy_count;
        self.copy_slices[idx as usize] = (data.as_ptr(), data.len());
        self.copy_count += 1;
        self.parts[self.part_count as usize] = PartSlot::Copy { slice_idx: idx };
        self.part_count += 1;
        self.total_len += data.len() as u32;
        self.total_copy_len += data.len();
        self
    }

    /// Add a zero-copy guard part to this scatter-gather SQE.
    pub fn guard(mut self, guard: GuardBox) -> Self {
        if self.chain.error.is_some() {
            return self;
        }
        if self.part_count as usize >= MAX_IOVECS {
            self.chain.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many send parts (max 32)",
            ));
            return self;
        }
        if self.guard_count as usize >= MAX_GUARDS {
            self.chain.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many guards (max 8)",
            ));
            return self;
        }
        let (_, len) = guard.as_ptr_len();
        let gidx = self.guard_count;
        self.guards[gidx as usize] = Some(guard);
        self.guard_count += 1;
        self.parts[self.part_count as usize] = PartSlot::Guard { guard_idx: gidx };
        self.part_count += 1;
        self.total_len += len;
        self
    }

    /// Finalize this scatter-gather SQE and add it to the chain.
    /// Returns the chain builder for further chaining.
    #[allow(clippy::needless_range_loop)]
    pub fn add(mut self) -> SendChainBuilder<'b, 'a> {
        if self.chain.error.is_some() || self.part_count == 0 {
            return self.chain;
        }

        if self.chain.built.len() >= self.chain.ctx.max_chain_length as usize {
            self.chain.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "chain exceeds max_chain_length",
            ));
            return self.chain;
        }

        // Build the SQE using a temporary SendBuilder on the chain's context.
        let conn_index = self.chain.conn.index;

        // Small guard sends: ZC bookkeeping (slab entry + notification CQE)
        // costs more than a memcpy below the threshold. Same fold that
        // `SendBuilder::submit` performs via `submit_small_gather`; without
        // it, chained sub-threshold guard parts paid the ZC path
        // unconditionally. Gather all parts — guard bytes included — into
        // one pool slot and emit a plain Send; guards drop immediately
        // (data is copied out before return). Falls through to the ZC path
        // when the pool is exhausted or the gather doesn't fit a slot.
        let threshold = self.chain.ctx.send_zc_threshold;
        let folded: Option<BuiltSend> =
            if self.guard_count > 0 && threshold > 0 && self.total_len < threshold {
                let mut all_slices = [(std::ptr::null::<u8>(), 0usize); MAX_IOVECS];
                let mut n = 0usize;
                for i in 0..self.part_count as usize {
                    match self.parts[i] {
                        PartSlot::Copy { slice_idx } => {
                            all_slices[n] = self.copy_slices[slice_idx as usize];
                            n += 1;
                        }
                        PartSlot::Guard { guard_idx } => {
                            let g = self.guards[guard_idx as usize].as_ref().unwrap();
                            let (gptr, glen) = g.as_ptr_len();
                            all_slices[n] = (gptr, glen as usize);
                            n += 1;
                        }
                        PartSlot::Empty => {}
                    }
                }
                let gathered = unsafe {
                    self.chain
                        .ctx
                        .send_copy_pool
                        .copy_in_gather(&all_slices[..n], self.total_len as usize)
                };
                gathered.map(|(slot, ptr, len)| {
                    // Data is in the pool now — release the guards.
                    for g in self.guards.iter_mut() {
                        *g = None;
                    }
                    let user_data = crate::completion::UserData::encode(
                        crate::completion::OpTag::Send,
                        conn_index,
                        slot as u32,
                    );
                    let entry =
                        io_uring::opcode::Send::new(io_uring::types::Fixed(conn_index), ptr, len)
                            .flags(crate::completion::STREAM_SEND_FLAGS)
                            .build()
                            .user_data(user_data.raw());
                    BuiltSend {
                        entry,
                        pool_slot: slot,
                        slab_idx: u16::MAX,
                        total_len: self.total_len,
                    }
                })
            } else {
                None
            };

        let built = if let Some(built) = folded {
            built
        } else if self.guard_count == 0 {
            // Copy-only: gather into pool slot.
            let result = unsafe {
                self.chain.ctx.send_copy_pool.copy_in_gather(
                    &self.copy_slices[..self.copy_count as usize],
                    self.total_copy_len,
                )
            };
            match result {
                Some((slot, ptr, len)) => {
                    let user_data = crate::completion::UserData::encode(
                        crate::completion::OpTag::Send,
                        conn_index,
                        slot as u32,
                    );
                    let entry =
                        io_uring::opcode::Send::new(io_uring::types::Fixed(conn_index), ptr, len)
                            .flags(crate::completion::STREAM_SEND_FLAGS)
                            .build()
                            .user_data(user_data.raw());

                    BuiltSend {
                        entry,
                        pool_slot: slot,
                        slab_idx: u16::MAX,
                        total_len: self.total_len,
                    }
                }
                None => {
                    self.chain.error = Some(io::Error::other("send copy pool exhausted"));
                    return self.chain;
                }
            }
        } else {
            // With guards: allocate pool slot (if copy data) + slab entry.
            let slot_size = self.chain.ctx.send_copy_pool.slot_size() as usize;
            if self.total_copy_len > slot_size {
                self.chain.error = Some(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "total copy data exceeds send pool slot size",
                ));
                return self.chain;
            }

            if self.total_copy_len > 0 {
                let result = unsafe {
                    self.chain.ctx.send_copy_pool.copy_in_gather(
                        &self.copy_slices[..self.copy_count as usize],
                        self.total_copy_len,
                    )
                };
                match result {
                    Some((slot, pool_ptr, _)) => {
                        // Build iovecs with copy parts pointing into pool slot.
                        let mut iovecs = [libc::iovec {
                            iov_base: std::ptr::null_mut(),
                            iov_len: 0,
                        }; MAX_IOVECS];
                        let mut copy_offset: usize = 0;
                        for i in 0..self.part_count as usize {
                            match self.parts[i] {
                                PartSlot::Copy { slice_idx } => {
                                    let (_, src_len) = self.copy_slices[slice_idx as usize];
                                    iovecs[i] = libc::iovec {
                                        iov_base: pool_ptr.wrapping_add(copy_offset) as *mut _,
                                        iov_len: src_len,
                                    };
                                    copy_offset += src_len;
                                }
                                PartSlot::Guard { guard_idx } => {
                                    let g = self.guards[guard_idx as usize].as_ref().unwrap();
                                    let (gptr, glen) = g.as_ptr_len();
                                    let region = g.region();
                                    if region != crate::buffer::fixed::RegionId::UNREGISTERED
                                        && let Err(e) = self
                                            .chain
                                            .ctx
                                            .fixed_buffers
                                            .validate_region_ptr(region, gptr, glen)
                                    {
                                        // Same check SendBuilder performs — an
                                        // out-of-region pointer must not reach a
                                        // kernel iovec.
                                        self.chain.ctx.send_copy_pool.release(slot);
                                        self.chain.error = Some(io::Error::new(
                                            io::ErrorKind::InvalidInput,
                                            e.to_string(),
                                        ));
                                        return self.chain;
                                    }
                                    iovecs[i] = libc::iovec {
                                        iov_base: gptr as *mut _,
                                        iov_len: glen as usize,
                                    };
                                }
                                PartSlot::Empty => {}
                            }
                        }

                        let iov_slice = &iovecs[..self.part_count as usize];
                        let total_len = self.total_len;
                        let guards = std::mem::take(&mut self.guards);
                        match self.chain.ctx.send_slab.allocate(
                            conn_index,
                            iov_slice,
                            slot,
                            guards,
                            self.guard_count,
                            total_len,
                        ) {
                            Some((slab_idx, msg_ptr)) => {
                                let user_data = crate::completion::UserData::encode(
                                    crate::completion::OpTag::SendMsgZc,
                                    conn_index,
                                    slab_idx as u32,
                                );
                                let entry = io_uring::opcode::SendMsgZc::new(
                                    io_uring::types::Fixed(conn_index),
                                    msg_ptr,
                                )
                                .build()
                                .user_data(user_data.raw());

                                BuiltSend {
                                    entry,
                                    pool_slot: slot,
                                    slab_idx,
                                    total_len,
                                }
                            }
                            None => {
                                self.chain.ctx.send_copy_pool.release(slot);
                                self.chain.error = Some(io::Error::other("send slab exhausted"));
                                return self.chain;
                            }
                        }
                    }
                    None => {
                        self.chain.error = Some(io::Error::other("send copy pool exhausted"));
                        return self.chain;
                    }
                }
            } else {
                // Guards only, no copy data.
                let mut iovecs = [libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                }; MAX_IOVECS];
                for i in 0..self.part_count as usize {
                    if let PartSlot::Guard { guard_idx } = self.parts[i] {
                        let g = self.guards[guard_idx as usize].as_ref().unwrap();
                        let (gptr, glen) = g.as_ptr_len();
                        let region = g.region();
                        if region != crate::buffer::fixed::RegionId::UNREGISTERED
                            && let Err(e) = self
                                .chain
                                .ctx
                                .fixed_buffers
                                .validate_region_ptr(region, gptr, glen)
                        {
                            self.chain.error =
                                Some(io::Error::new(io::ErrorKind::InvalidInput, e.to_string()));
                            return self.chain;
                        }
                        iovecs[i] = libc::iovec {
                            iov_base: gptr as *mut _,
                            iov_len: glen as usize,
                        };
                    }
                }

                let iov_slice = &iovecs[..self.part_count as usize];
                let total_len = self.total_len;
                let guards = std::mem::take(&mut self.guards);
                match self.chain.ctx.send_slab.allocate(
                    conn_index,
                    iov_slice,
                    u16::MAX,
                    guards,
                    self.guard_count,
                    total_len,
                ) {
                    Some((slab_idx, msg_ptr)) => {
                        let user_data = crate::completion::UserData::encode(
                            crate::completion::OpTag::SendMsgZc,
                            conn_index,
                            slab_idx as u32,
                        );
                        let entry = io_uring::opcode::SendMsgZc::new(
                            io_uring::types::Fixed(conn_index),
                            msg_ptr,
                        )
                        .build()
                        .user_data(user_data.raw());

                        BuiltSend {
                            entry,
                            pool_slot: u16::MAX,
                            slab_idx,
                            total_len,
                        }
                    }
                    None => {
                        self.chain.error = Some(io::Error::other("send slab exhausted"));
                        return self.chain;
                    }
                }
            }
        };

        self.chain.total_bytes += built.total_len;
        self.chain.built.push(built);
        self.chain
    }
}
