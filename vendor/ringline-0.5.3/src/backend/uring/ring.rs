use std::io;
use std::os::fd::RawFd;

use io_uring::cqueue;
use io_uring::squeue;
use io_uring::types::{DestinationSlot, Fd, Fixed};
use io_uring::{IoUring, opcode};

use crate::backend::ProvidedBufRing;
use crate::buffer::fixed::FixedBufferRegistry;
use crate::completion::{OpTag, UserData};
use crate::config::Config;
use crate::nvme::{NVME_URING_CMD_IO, NvmeUringCmd};

/// Wrapper around IoUring providing high-level SQE submission helpers.
///
/// The ring uses 128-byte SQEs and 32-byte CQEs (`IoUring<Entry128, Entry32>`)
/// to support NVMe passthrough via `IORING_OP_URING_CMD` / `UringCmd80`.
/// Standard network opcodes produce 64-byte `Entry` values which are
/// automatically converted to `Entry128` (zero-padded) via `Into`.
///
/// Memory overhead of Big SQE/CQE: +32 KB per worker with default config
/// (256 SQ × 64B extra + 1024 CQ × 16B extra), negligible relative to the
/// ~20 MB of buffer pools allocated per worker.
pub struct Ring {
    pub(crate) ring: IoUring<squeue::Entry128, cqueue::Entry32>,
    /// Recv buffer group ID for multishot recv.
    bgid: u16,
    /// Reusable Entry128 conversion scratch for chain pushes — avoids a
    /// heap allocation per chained send.
    chain_scratch: Vec<squeue::Entry128>,
}

impl Ring {
    /// Create and configure the io_uring instance.
    pub fn setup(config: &Config) -> io::Result<Self> {
        let cq_entries = config
            .sq_entries
            .checked_mul(4)
            .unwrap_or(config.sq_entries);

        let mut builder = IoUring::<squeue::Entry128, cqueue::Entry32>::builder();
        builder.setup_cqsize(cq_entries);
        builder.setup_coop_taskrun();
        builder.setup_single_issuer();

        if config.sqpoll {
            builder.setup_sqpoll(config.sqpoll_idle_ms);
            if let Some(cpu) = config.sqpoll_cpu {
                builder.setup_sqpoll_cpu(cpu);
            }
            // DEFER_TASKRUN is incompatible with SQPOLL (kernel returns EINVAL).
        } else {
            builder.setup_defer_taskrun();
        }

        let ring = builder.build(config.sq_entries)?;

        Ok(Ring {
            ring,
            bgid: config.recv_buffer.bgid,
            chain_scratch: Vec::new(),
        })
    }

    /// Register a sparse fixed-buffer table sized to the registry, then
    /// fill in any occupied slots via `register_buffers_update`.
    ///
    /// The sparse path lets us add and remove regions dynamically after
    /// launch without re-registering the entire table.
    pub fn register_buffers(&self, registry: &FixedBufferRegistry) -> io::Result<()> {
        let iovecs = registry.iovecs();
        if iovecs.is_empty() {
            return Ok(());
        }
        let submitter = self.ring.submitter();
        submitter.register_buffers_sparse(iovecs.len() as u32)?;

        // Apply each occupied slot. Empty slots stay zeroed in the kernel.
        for (slot, iov) in iovecs.iter().enumerate() {
            if iov.iov_base.is_null() {
                continue;
            }
            // Safety: the iovec points at user memory documented to outlive
            // the runtime; tags are unused.
            unsafe {
                submitter.register_buffers_update(slot as u32, std::slice::from_ref(iov), None)?;
            }
        }
        Ok(())
    }

    /// Update a single fixed-buffer slot with a new iovec.
    ///
    /// `iov.iov_base.is_null()` clears the slot.
    ///
    /// # Safety
    ///
    /// The memory described by `iov` must remain valid until either the slot
    /// is cleared or the runtime shuts down. No SQE referencing the slot may
    /// be in flight when this is called.
    pub unsafe fn register_buffers_update_one(
        &self,
        slot: u16,
        iov: libc::iovec,
    ) -> io::Result<()> {
        unsafe {
            self.ring.submitter().register_buffers_update(
                slot as u32,
                std::slice::from_ref(&iov),
                None,
            )?;
        }
        Ok(())
    }

    /// Register a sparse file table for direct descriptors.
    pub fn register_files_sparse(&self, count: u32) -> io::Result<()> {
        self.ring.submitter().register_files_sparse(count)?;
        Ok(())
    }

    /// Update registered file table at given offset.
    pub fn register_files_update(&self, offset: u32, fds: &[RawFd]) -> io::Result<()> {
        self.ring.submitter().register_files_update(offset, fds)?;
        Ok(())
    }

    /// Register the provided buffer ring with the kernel.
    pub fn register_buf_ring(&self, provided: &ProvidedBufRing) -> io::Result<()> {
        // Safety: ring_addr points to valid mmap'd memory that outlives the registration.
        unsafe {
            self.ring.submitter().register_buf_ring_with_flags(
                provided.ring_addr(),
                provided.ring_entries() as u16,
                provided.bgid(),
                0,
            )?;
        }
        Ok(())
    }

    /// Unregister the provided buffer ring from the kernel.
    /// Must be called before the ring memory is munmap'd.
    pub fn unregister_buf_ring(&self, bgid: u16) -> io::Result<()> {
        self.ring.submitter().unregister_buf_ring(bgid)?;
        Ok(())
    }

    /// Submit a multishot recvmsg with provided buffer ring for a connection.
    /// Used when SO_TIMESTAMPING is enabled to receive cmsg ancillary data
    /// (kernel timestamps) alongside TCP payload.
    #[cfg(feature = "timestamps")]
    pub fn submit_multishot_recvmsg(
        &mut self,
        conn_index: u32,
        msghdr: *const libc::msghdr,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::RecvMsgMultiTs, conn_index, 0);
        let entry = opcode::RecvMsgMulti::new(Fixed(conn_index), msghdr, self.bgid)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a one-shot fallback recv into fallback-pool memory for a
    /// connection whose multishot recv is parked on ENOBUFS. The pool slot
    /// is carried in the payload and released by `handle_recv_fallback`;
    /// the pool owns `ptr` until that CQE arrives (SQE memory outlives the
    /// operation even across close/slot-reuse).
    pub fn submit_recv_fallback(
        &mut self,
        conn_index: u32,
        ptr: *mut u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::RecvFallback, conn_index, pool_slot as u32);
        let entry = opcode::Recv::new(Fixed(conn_index), ptr, len)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a multishot recv with provided buffer ring for a connection.
    pub fn submit_multishot_recv(&mut self, conn_index: u32) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        let entry = opcode::RecvMulti::new(Fixed(conn_index), self.bgid)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a copied send. The data must be in a SendCopyPool slot.
    /// The pool slot index is stored in the payload for release on CQE.
    pub fn submit_send_copied(
        &mut self,
        conn_index: u32,
        ptr: *const u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Send, conn_index, pool_slot as u32);
        let entry = opcode::Send::new(Fixed(conn_index), ptr, len)
            .flags(crate::completion::STREAM_SEND_FLAGS)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a SendMsgZc operation.
    /// The slab index is stored in the payload for lookup on CQE.
    pub fn submit_send_msg_zc(
        &mut self,
        conn_index: u32,
        msg: *const libc::msghdr,
        slab_idx: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        let entry = opcode::SendMsgZc::new(Fixed(conn_index), msg)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a coalesced plaintext send: one plain (non-ZC) `sendmsg` whose
    /// iovecs gather several queued sends. The slab index is in the payload.
    pub fn submit_send_msg_coalesced(
        &mut self,
        conn_index: u32,
        msg: *const libc::msghdr,
        slab_idx: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::SendMsgCoalesced, conn_index, slab_idx as u32);
        let entry = opcode::SendMsg::new(Fixed(conn_index), msg)
            .flags(crate::completion::STREAM_SEND_FLAGS as u32)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Arm a POLLOUT poll after a coalesced send returned `-EAGAIN`; the slab
    /// index is carried in the payload so the handler can resubmit the sendmsg.
    pub fn submit_send_msg_coalesced_pollout(
        &mut self,
        conn_index: u32,
        slab_idx: u16,
    ) -> io::Result<()> {
        let user_data =
            UserData::encode(OpTag::SendMsgCoalescedPollOut, conn_index, slab_idx as u32);
        let entry = opcode::PollAdd::new(Fixed(conn_index), libc::POLLOUT as u32)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a zero-copy recv-forward send: one plain (non-ZC) `sendmsg` whose
    /// iovecs point directly into held provided recv buffers. The slab index is
    /// in the payload; the slab entry holds the bids to replenish on completion.
    pub fn submit_send_recv_bufs_coalesced(
        &mut self,
        conn_index: u32,
        msg: *const libc::msghdr,
        slab_idx: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::SendRecvBufsCoalesced, conn_index, slab_idx as u32);
        let entry = opcode::SendMsg::new(Fixed(conn_index), msg)
            .flags(crate::completion::STREAM_SEND_FLAGS as u32)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Arm a POLLOUT poll after a recv-forward send returned `-EAGAIN`; the slab
    /// index is carried in the payload so the handler can resubmit the sendmsg.
    pub fn submit_send_recv_bufs_coalesced_pollout(
        &mut self,
        conn_index: u32,
        slab_idx: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(
            OpTag::SendRecvBufsCoalescedPollOut,
            conn_index,
            slab_idx as u32,
        );
        let entry = opcode::PollAdd::new(Fixed(conn_index), libc::POLLOUT as u32)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a segmented-recv Mode A forward write to a **socket** sink.
    ///
    /// The source `buf`/`len` points into a held provided recv buffer (or an
    /// owned copy) whose lifetime is tracked in `Driver::forward_write` until
    /// this CQE arrives. `fd` is the sink socket (a non-fixed raw fd borrowed
    /// for the forward's duration via `SinkFd`). `MSG_WAITALL` makes the kernel
    /// retry short stream sends in place (5.19+).
    ///
    /// # Safety
    /// `buf`/`len` must stay valid until the CQE arrives (the driver holds the
    /// backing) and `fd` must stay open for the operation's lifetime.
    pub unsafe fn submit_forward_write_socket(
        &mut self,
        fd: RawFd,
        buf: *const u8,
        len: u32,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::Send::new(Fd(fd), buf, len)
            .flags(crate::completion::STREAM_SEND_FLAGS)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a segmented-recv Mode A forward write to a **buffered file** sink
    /// at an explicit offset (`pwrite` semantics). A short write is resubmitted
    /// at the advanced offset by the completion handler.
    ///
    /// # Safety
    /// `buf`/`len` must stay valid until the CQE arrives (the driver holds the
    /// backing) and `fd` must stay open for the operation's lifetime.
    pub unsafe fn submit_forward_write_file(
        &mut self,
        fd: RawFd,
        buf: *const u8,
        len: u32,
        offset: u64,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::Write::new(Fd(fd), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Arm a POLLOUT poll on a forward-write **socket** sink after a send
    /// returned `-EAGAIN`; the handler resubmits the remaining bytes when the
    /// sink becomes writable.
    pub fn submit_forward_write_pollout(
        &mut self,
        fd: RawFd,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::PollAdd::new(Fd(fd), libc::POLLOUT as u32)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a TLS-internal send (handshake, alert). Uses OpTag::TlsSend
    /// so the CQE handler releases the pool slot without calling on_send_complete.
    pub fn submit_tls_send(
        &mut self,
        conn_index: u32,
        ptr: *const u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::TlsSend, conn_index, pool_slot as u32);
        let entry = opcode::Send::new(Fixed(conn_index), ptr, len)
            .flags(crate::completion::STREAM_SEND_FLAGS)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a TLS-internal send with IOSQE_IO_LINK. Used for close_notify
    /// so the subsequent Close SQE is chained and only runs after the send completes.
    pub fn submit_tls_send_linked(
        &mut self,
        conn_index: u32,
        ptr: *const u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::TlsSend, conn_index, pool_slot as u32);
        let entry = opcode::Send::new(Fixed(conn_index), ptr, len)
            .flags(crate::completion::STREAM_SEND_FLAGS)
            .build()
            .user_data(user_data.raw())
            .flags(io_uring::squeue::Flags::IO_LINK);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit an eventfd read (8 bytes).
    pub fn submit_eventfd_read(&mut self, eventfd: RawFd, buf: *mut u8) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::EventFdRead, 0, 0);
        let entry = opcode::Read::new(Fd(eventfd), buf, 8)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a close for a direct file descriptor.
    pub fn submit_close(&mut self, conn_index: u32) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Close, conn_index, 0);
        let entry = opcode::Close::new(Fixed(conn_index))
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit an async connect for a direct file descriptor.
    pub fn submit_connect(
        &mut self,
        conn_index: u32,
        addr: *const libc::sockaddr,
        addrlen: libc::socklen_t,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Connect, conn_index, 0);
        let entry = opcode::Connect::new(Fixed(conn_index), addr, addrlen)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a timeout SQE. The timespec must remain valid until the CQE arrives.
    pub fn submit_timeout(
        &mut self,
        timespec: *const io_uring::types::Timespec,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::Timeout::new(timespec)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit an absolute timeout SQE. The timespec contains absolute
    /// `CLOCK_MONOTONIC` seconds/nanoseconds. The timespec must remain valid
    /// until the CQE arrives.
    pub fn submit_timeout_abs(
        &mut self,
        timespec: *const io_uring::types::Timespec,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::Timeout::new(timespec)
            .flags(io_uring::types::TimeoutFlags::ABS)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit an async cancel targeting a specific user_data value.
    pub fn submit_async_cancel(
        &mut self,
        target_user_data: u64,
        conn_index: u32,
    ) -> io::Result<()> {
        let ud = UserData::encode(OpTag::Cancel, conn_index, 0);
        let entry = opcode::AsyncCancel::new(target_user_data)
            .build()
            .user_data(ud.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a shutdown(SHUT_WR) for a connection.
    pub fn submit_shutdown(&mut self, conn_index: u32) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Shutdown, conn_index, 0);
        let entry = opcode::Shutdown::new(Fixed(conn_index), libc::SHUT_WR)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a multishot recvmsg for a UDP socket backed by a provided buffer ring.
    ///
    /// `msghdr` is used as a *template* by the kernel to decide how to lay out
    /// each datagram inside the ring buffer it picks (name / control / payload
    /// regions). It must remain valid for as long as the multishot is armed.
    /// Use [`io_uring::types::RecvMsgOut::parse`] on the returned buffer to
    /// extract the datagram.
    pub fn submit_recvmsg_multishot(
        &mut self,
        fd_index: u32,
        msghdr: *const libc::msghdr,
        bgid: u16,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::RecvMsgMulti::new(Fixed(fd_index), msghdr, bgid)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a sendmsg (copying) for a UDP socket with destination address.
    pub fn submit_sendmsg(
        &mut self,
        fd_index: u32,
        msghdr: *const libc::msghdr,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::SendMsg::new(Fixed(fd_index), msghdr)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a multishot Recv (no peer info, kernel uses the socket's
    /// connected peer) for a UDP socket. Lighter than `RecvMsgMulti` —
    /// the CQE buffer contains only the payload, no `io_uring_recvmsg_out`
    /// header or sockaddr. The socket must already be `connect(2)`ed.
    pub fn submit_multishot_recv_udp(
        &mut self,
        fd_index: u32,
        bgid: u16,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::RecvMulti::new(Fixed(fd_index), bgid)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a single-shot Send for a connected UDP socket. The data lives
    /// in a `send_copy_pool` slot; the slot index is in the payload of
    /// `user_data` so the CQE handler can release it.
    pub fn submit_send_udp(
        &mut self,
        fd_index: u32,
        ptr: *const u8,
        len: u32,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::Send::new(Fixed(fd_index), ptr, len)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a PollAdd for a raw file descriptor (e.g., pidfd for process exit).
    pub fn submit_poll_add(&mut self, fd: RawFd, mask: u32, ud: u64) -> io::Result<()> {
        let entry = opcode::PollAdd::new(Fd(fd), mask).build().user_data(ud);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a one-shot PollAdd on `POLLOUT` for a fixed-table TCP fd
    /// after a Send returned `-EAGAIN`. The CQE encodes `pool_slot` so
    /// the handler can resubmit the original send from where it
    /// stopped. (`current_ptr_remaining(pool_slot)` gives the right
    /// `(ptr, len)` to retry with.)
    pub fn submit_send_pollout(
        &mut self,
        conn_index: u32,
        pool_slot: u16,
        is_tls: bool,
    ) -> io::Result<()> {
        // Payload: pool_slot in the low 16 bits, is_tls flag in bit 16 so
        // the POLLOUT handler resubmits on the right completion path.
        let payload = pool_slot as u32 | (u32::from(is_tls) << 16);
        let user_data = UserData::encode(OpTag::SendPollOut, conn_index, payload);
        let entry = opcode::PollAdd::new(Fixed(conn_index), libc::POLLOUT as u32)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit all pending SQEs and wait for at least `min_complete` CQEs.
    ///
    /// A bare `?` here would kill the worker thread (and every connection on
    /// it) on the first transient `io_uring_enter` failure:
    /// - `EINTR`: any signal delivered to the worker interrupts the wait
    ///   regardless of `SA_RESTART` — restart it.
    /// - `EBUSY`: the CQ is backed up (overflow list non-empty); return `Ok`
    ///   so the caller drains completions, which frees CQ space.
    pub fn submit_and_wait(&self, min_complete: u32) -> io::Result<()> {
        loop {
            match self.ring.submitter().submit_and_wait(min_complete as usize) {
                Ok(_) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    /// Submit a timeout SQE that fires after the given duration.
    /// Produces a CQE with the given user_data when it fires (-ETIME)
    /// or is cancelled (-ECANCELED).
    pub fn submit_tick_timeout(
        &mut self,
        ts: *const io_uring::types::Timespec,
        user_data: u64,
    ) -> io::Result<()> {
        let entry = opcode::Timeout::new(ts).build().user_data(user_data);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit pending SQEs without waiting. Used for mid-iteration flush.
    ///
    /// After submitting the SQEs this method issues a second `io_uring_enter`
    /// with `IORING_ENTER_GETEVENTS` and `min_complete=0`.  With
    /// `IORING_SETUP_DEFER_TASKRUN` the kernel only runs task_work (and posts
    /// deferred CQEs to the completion ring) when `IORING_ENTER_GETEVENTS` is
    /// set.  A plain `submit()` call does NOT set that flag, so send-completion
    /// CQEs for the SQEs we just submitted sit in kernel-internal task_work
    /// until the next `submit_and_wait(1)`, causing a "dead" event-loop
    /// iteration that wakes up only to process those CQEs.
    ///
    /// By issuing a non-blocking `enter(GETEVENTS, min=0)` right after submit
    /// we flush task_work inline — the send CQEs land in the CQ ring before
    /// `flush()` returns, so the `drain_completions()` call that follows in
    /// the event loop can consume them immediately.
    pub fn flush(&self) -> io::Result<()> {
        // Combine submit + DEFER_TASKRUN flush into a single kernel entry.
        //
        // The old two-call path was:
        //   submit()                           → enter(sq_len, 0, 0=no-GETEVENTS, None)
        //   enter::<()>(0, 0, GETEVENTS, None) → enter(0,      0, GETEVENTS,       None)
        //
        // Merged into one:
        //   enter(sq_len, 0, GETEVENTS, None)
        //
        // This submits any pending SQEs AND triggers DEFER_TASKRUN task_work
        // delivery in a single syscall, saving one round-trip to the kernel
        // per flush() invocation (≈ once or twice per event-loop iteration).
        //
        // Safety: `submission_shared()` gives a shared view of the SQ head/tail
        // atomics.  We only read `.len()` (sq_tail − sq_head) and never push
        // new entries here, so there is no aliasing or mutation hazard.
        let n = unsafe { self.ring.submission_shared().len() } as u32;
        if n == 0 {
            // Nothing to submit. Pending DEFER_TASKRUN task_work and CQEs are
            // reaped by the next submit_and_wait(1) (always a GETEVENTS enter),
            // so skipping the syscall here only defers completion reaping by at
            // most one loop iteration — no correctness impact.
            return Ok(());
        }
        unsafe {
            self.ring
                .submitter()
                .enter::<()>(n, 0, 1 /* IORING_ENTER_GETEVENTS */, None)?;
        }
        Ok(())
    }

    /// Push a standard SQE to the submission queue.
    ///
    /// The 64-byte `Entry` is automatically converted to `Entry128` (zero-padded)
    /// for the Big SQE ring.
    ///
    /// Submit a NOP with injected result for error injection testing.
    ///
    /// The kernel will post a CQE with the given `user_data` and `result`,
    /// allowing tests to simulate any CQE (send error, recv EOF, etc.)
    /// through the real submit_and_wait → dispatch_cqe pipeline.
    ///
    /// Requires kernel 6.6+ (IORING_NOP_INJECT_RESULT support).
    #[cfg(test)]
    pub(crate) fn submit_nop_inject(&mut self, user_data: u64, result: i32) -> io::Result<()> {
        let mut entry = opcode::Nop::new().build().user_data(user_data);
        // The high-level Entry doesn't expose nop_flags or len fields.
        // Use raw pointer arithmetic to patch the SQE in-place.
        // SQE layout (64 bytes): opcode(1) flags(1) ioprio(2) fd(4) off(8) addr(8)
        //                         len(4@24) rw_flags/nop_flags(4@28) user_data(8) ...
        let ptr = &mut entry as *mut squeue::Entry as *mut u8;
        unsafe {
            // len is at byte offset 24 in the SQE
            std::ptr::write_unaligned(ptr.add(24) as *mut u32, result as u32);
            // nop_flags (union with rw_flags) is at byte offset 28
            std::ptr::write_unaligned(ptr.add(28) as *mut u32, 1); // IORING_NOP_INJECT_RESULT
        }
        unsafe { self.push_sqe(entry) }
    }

    /// Like `submit_nop_inject` but with IOSQE_IO_LINK set, so the
    /// next SQE in the submission queue is linked to this one.
    #[cfg(test)]
    pub(crate) fn submit_nop_inject_linked(
        &mut self,
        user_data: u64,
        result: i32,
    ) -> io::Result<()> {
        let mut entry = opcode::Nop::new()
            .build()
            .user_data(user_data)
            .flags(squeue::Flags::IO_LINK);
        let ptr = &mut entry as *mut squeue::Entry as *mut u8;
        unsafe {
            std::ptr::write_unaligned(ptr.add(24) as *mut u32, result as u32);
            std::ptr::write_unaligned(ptr.add(28) as *mut u32, 1); // IORING_NOP_INJECT_RESULT
        }
        unsafe { self.push_sqe(entry) }
    }

    /// # Safety
    /// The SQE must reference valid memory for the lifetime of the operation.
    pub(crate) unsafe fn push_sqe(&mut self, entry: squeue::Entry) -> io::Result<()> {
        let entry128: squeue::Entry128 = entry.into();
        unsafe {
            self.push_sqe128(entry128)?;
        }
        Ok(())
    }

    /// Push a 128-byte SQE to the submission queue.
    ///
    /// Used directly for NVMe passthrough (`UringCmd80`) which produces
    /// `Entry128` natively.
    ///
    /// # Safety
    /// The SQE must reference valid memory for the lifetime of the operation.
    pub(crate) unsafe fn push_sqe128(&mut self, entry: squeue::Entry128) -> io::Result<()> {
        // Try to push; if SQ is full, submit first to make room.
        unsafe {
            if self.ring.submission().push(&entry).is_err() {
                self.ring.submit()?;
                if self.ring.submission().push(&entry).is_err() {
                    crate::metrics::RING.increment(crate::metrics::ring::SQE_SUBMIT_FAILURES);
                    return Err(io::Error::other("SQ still full after submit"));
                }
            }
        }
        Ok(())
    }

    /// Push a chain of linked SQEs atomically.
    ///
    /// Sets `IOSQE_IO_LINK` on all entries except the last, so the kernel
    /// executes them sequentially. All entries are pushed via `push_multiple`
    /// to guarantee contiguous placement in the SQ.
    ///
    /// # Safety
    /// All SQEs must reference valid memory for the lifetime of their operations.
    pub(crate) unsafe fn push_sqe_chain(
        &mut self,
        entries: &mut [squeue::Entry],
    ) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        if entries.len() == 1 {
            return unsafe { self.push_sqe(entries[0].clone()) };
        }

        // Set IO_LINK on all entries except the last.
        let last = entries.len() - 1;
        for entry in entries[..last].iter_mut() {
            *entry = entry.clone().flags(io_uring::squeue::Flags::IO_LINK);
        }

        // Convert to Entry128 for the Big SQ ring, reusing the scratch to
        // avoid a per-chain heap allocation.
        let mut entries128 = std::mem::take(&mut self.chain_scratch);
        entries128.clear();
        entries128.extend(entries.iter().map(|e| e.clone().into()));

        // Ensure enough room in the SQ for the entire chain.
        {
            let sq = self.ring.submission();
            if sq.capacity() - sq.len() < entries128.len() {
                drop(sq);
                self.ring.submit()?;
                let sq = self.ring.submission();
                if sq.capacity() - sq.len() < entries128.len() {
                    entries128.clear();
                    self.chain_scratch = entries128;
                    return Err(io::Error::other("SQ too small for chain"));
                }
            }
        }

        // Atomic push of the entire chain.
        let pushed = unsafe {
            self.ring
                .submission()
                .push_multiple(&entries128)
                .map_err(|_| io::Error::other("SQ full after flush for chain"))
        };
        // Return the scratch for reuse regardless of outcome.
        entries128.clear();
        self.chain_scratch = entries128;
        pushed?;
        Ok(())
    }

    /// Submit an NVMe passthrough command via `IORING_OP_URING_CMD`.
    ///
    /// The `fd_index` must be a fixed file table index pointing to an opened
    /// NVMe-generic character device (`/dev/ng<X>n<Y>`).
    ///
    /// # Safety
    /// The buffer referenced by `cmd.addr` / `cmd.data_len` must remain valid
    /// until the CQE arrives.
    pub unsafe fn submit_nvme_cmd(
        &mut self,
        fd_index: u32,
        cmd: &NvmeUringCmd,
        user_data: UserData,
    ) -> io::Result<()> {
        let cmd_bytes = cmd.to_bytes();
        let entry = opcode::UringCmd80::new(Fixed(fd_index), NVME_URING_CMD_IO)
            .cmd(cmd_bytes)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe128(entry)?;
        }
        Ok(())
    }

    /// Submit a direct I/O read via `IORING_OP_READ`.
    ///
    /// The `fd_index` must be a fixed file table index pointing to a file
    /// opened with `O_DIRECT`.
    ///
    /// # Safety
    /// The buffer at `buf` with length `len` must remain valid and properly
    /// aligned until the CQE arrives. For `O_DIRECT`, the buffer address,
    /// length, and file offset must all be aligned to the logical block size.
    pub unsafe fn submit_direct_read(
        &mut self,
        fd_index: u32,
        buf: *mut u8,
        len: u32,
        offset: u64,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::Read::new(Fixed(fd_index), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a direct I/O write via `IORING_OP_WRITE`.
    ///
    /// The `fd_index` must be a fixed file table index pointing to a file
    /// opened with `O_DIRECT`.
    ///
    /// # Safety
    /// The buffer at `buf` with length `len` must remain valid and properly
    /// aligned until the CQE arrives. For `O_DIRECT`, the buffer address,
    /// length, and file offset must all be aligned to the logical block size.
    pub unsafe fn submit_direct_write(
        &mut self,
        fd_index: u32,
        buf: *const u8,
        len: u32,
        offset: u64,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::Write::new(Fixed(fd_index), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit an fsync via `IORING_OP_FSYNC`.
    ///
    /// The `fd_index` must be a fixed file table index pointing to an opened file.
    pub fn submit_direct_fsync(&mut self, fd_index: u32, user_data: UserData) -> io::Result<()> {
        let entry = opcode::Fsync::new(Fixed(fd_index))
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    // ── Filesystem I/O submission methods ──────────────────────────────

    /// Submit an openat via io_uring. The fd is installed directly into the
    /// fixed file table at `fd_index`.
    ///
    /// # Safety
    /// `pathname` must point to a valid null-terminated C string that remains
    /// valid until the CQE arrives.
    pub unsafe fn submit_openat(
        &mut self,
        fd_index: u32,
        pathname: *const libc::c_char,
        flags: i32,
        mode: u32,
        ud: u64,
    ) -> io::Result<()> {
        let dest = DestinationSlot::try_from_slot_target(fd_index)
            .map_err(|_| io::Error::other("invalid fd_index for openat"))?;
        let entry = opcode::OpenAt::new(Fd(libc::AT_FDCWD), pathname)
            .flags(flags)
            .mode(mode)
            .file_index(Some(dest))
            .build()
            .user_data(ud);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a statx via io_uring.
    ///
    /// # Safety
    /// `pathname` must point to a valid null-terminated C string and `statxbuf`
    /// must point to valid memory, both remaining valid until the CQE arrives.
    pub unsafe fn submit_statx(
        &mut self,
        pathname: *const libc::c_char,
        statxbuf: *mut libc::statx,
        ud: u64,
    ) -> io::Result<()> {
        // STATX_BASIC_STATS = 0x7ff
        let entry = opcode::Statx::new(
            Fd(libc::AT_FDCWD),
            pathname,
            statxbuf as *mut io_uring::types::statx,
        )
        .flags(libc::AT_STATX_SYNC_AS_STAT)
        .mask(0x7ff)
        .build()
        .user_data(ud);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a renameat via io_uring.
    ///
    /// # Safety
    /// `oldpath` and `newpath` must point to valid null-terminated C strings
    /// that remain valid until the CQE arrives.
    pub unsafe fn submit_renameat(
        &mut self,
        oldpath: *const libc::c_char,
        newpath: *const libc::c_char,
        ud: u64,
    ) -> io::Result<()> {
        let entry = opcode::RenameAt::new(Fd(libc::AT_FDCWD), oldpath, Fd(libc::AT_FDCWD), newpath)
            .build()
            .user_data(ud);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit an unlinkat via io_uring.
    ///
    /// # Safety
    /// `pathname` must point to a valid null-terminated C string that remains
    /// valid until the CQE arrives.
    pub unsafe fn submit_unlinkat(
        &mut self,
        pathname: *const libc::c_char,
        flags: i32,
        ud: u64,
    ) -> io::Result<()> {
        let entry = opcode::UnlinkAt::new(Fd(libc::AT_FDCWD), pathname)
            .flags(flags)
            .build()
            .user_data(ud);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a mkdirat via io_uring.
    ///
    /// # Safety
    /// `pathname` must point to a valid null-terminated C string that remains
    /// valid until the CQE arrives.
    pub unsafe fn submit_mkdirat(
        &mut self,
        pathname: *const libc::c_char,
        mode: u32,
        ud: u64,
    ) -> io::Result<()> {
        let entry = opcode::MkDirAt::new(Fd(libc::AT_FDCWD), pathname)
            .mode(mode)
            .build()
            .user_data(ud);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }
}
