use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use io_uring::cqueue;

use crate::accumulator::AccumulatorTable;
use crate::backend::ProvidedBufRing;
use crate::backend::Ring;
use crate::buffer::fixed::FixedBufferRegistry;
use crate::buffer::send_copy::SendCopyPool;
use crate::buffer::send_slab::InFlightSendSlab;
use crate::chain::SendChainTable;
use crate::completion::{OpTag, UserData};
use crate::config::Config;
use crate::connection::{ConnectionTable, RecvMode};
use crate::handler::{BuiltSend, ConnSendState, DriverCtx};
use crate::metrics;

/// Slots in the lazily-constructed fallback recv pool. At most one
/// fallback is in flight per connection, so this bounds how many
/// connections can be in fallback simultaneously; when exhausted the
/// remaining starved connections stay parked (the pre-fallback status
/// quo) until slots free up.
const FALLBACK_RECV_SLOTS: u16 = 32;

/// One in-flight UDP send slot. Owns the `sockaddr` + `iovec` + `msghdr`
/// triple referenced by a single `sendmsg` SQE; returned to the freelist
/// once the CQE arrives.
pub(crate) struct UdpSendSlot {
    pub send_addr: Box<libc::sockaddr_storage>,
    #[allow(dead_code)] // referenced via raw pointer in msghdr
    pub send_iov: Box<libc::iovec>,
    pub send_msghdr: Box<libc::msghdr>,
    /// Pinned per-slot scratch for `UDP_SEGMENT` cmsg. Sized to hold
    /// one `cmsghdr` + a `u16` segment size; only consulted when
    /// `send_to_gso` is used. Heap-stable so the kernel's view (via
    /// `msg_control`) stays valid until the CQE arrives.
    pub send_cmsg_buf: Box<[u8; UDP_GSO_CMSG_LEN]>,
}

/// Size of the per-slot control-message buffer used for
/// `UDP_SEGMENT` cmsgs. CMSG_SPACE(sizeof(u16)) on x86-64 is 24
/// (cmsghdr is 16 bytes, payload aligned to 8). 32 is comfortable
/// over-allocation that survives any reasonable cmsg layout change.
pub(crate) const UDP_GSO_CMSG_LEN: usize = 32;

/// Pack a UDP send slot index and copy-pool slot into a 32-bit CQE payload.
/// High 16 bits = send-slot index; low 16 bits = copy-pool slot.
#[inline]
pub(crate) fn encode_udp_send_payload(slot_idx: u16, pool_slot: u16) -> u32 {
    ((slot_idx as u32) << 16) | (pool_slot as u32)
}

/// Inverse of [`encode_udp_send_payload`]: returns `(slot_idx, pool_slot)`.
#[inline]
pub(crate) fn decode_udp_send_payload(payload: u32) -> (u16, u16) {
    ((payload >> 16) as u16, (payload & 0xFFFF) as u16)
}

/// Per-worker UDP socket state.
pub(crate) struct UdpSocketState {
    /// Fixed file table index for this socket.
    pub fd_index: u32,
    /// Bound address.
    #[allow(dead_code)] // stored for diagnostics
    pub local_addr: SocketAddr,
    /// If `Some`, the socket has been `connect(2)`ed to this peer at setup
    /// time and the runtime uses the lighter `RecvUdp`/`SendUdp` opcodes
    /// instead of `RecvMsgUdp`/`SendMsgUdp`.
    pub connected_peer: Option<SocketAddr>,
    /// `msghdr` template used by `IORING_OP_RECVMSG_MULTISHOT`. The kernel
    /// reads its `msg_namelen`/`msg_controllen`/etc. to decide how to carve
    /// up each provided buffer into `[io_uring_recvmsg_out][name][control]
    /// [payload]`. `RecvMsgOut::parse` reads the same template on the CQE
    /// side. Must stay heap-stable for the lifetime of the multishot.
    pub recv_msghdr: Box<libc::msghdr>,
    /// Whether `UDP_GRO` was enabled on this socket. When true, each recvmsg
    /// delivery may carry a `UDP_GRO` control message whose value is the
    /// segment size used to split the coalesced payload back into datagrams.
    pub gro: bool,
    // ── Send state: fixed-size ring of per-SQE slots + a stack-freelist ──
    pub send_slots: Box<[UdpSendSlot]>,
    pub send_freelist: Vec<u16>,
}

/// A kernel recv buffer held in-place for zero-copy access.
///
/// The pointer is into `ProvidedBufRing::buf_backing`, which is allocated once
/// and never resized, so it remains valid until the bid is replenished.
#[derive(Clone, Copy)]
pub(crate) struct PendingRecvBuf {
    pub(crate) bid: u16,
    pub(crate) len: u32,
    pub(crate) ptr: *const u8,
}

/// A held received buffer for segmented delivery (Mode B/C), in one of two
/// backings depending on ring pressure at delivery time.
///
/// - [`Pinned`](Self::Pinned): a provided-buffer bid pinned in the ring — the
///   bid is NOT replenished, so the buffer stays in the provided ring until a
///   segment reader consumes it or the connection closes (`close_connection`
///   drains the hold). The backing pointer is derivable via
///   `provided_bufs.get_buffer(bid)`, so only the id and length are stored. This
///   is the zero-copy delivery, used while the ring is above the low-water
///   reserve.
/// - [`Owned`](Self::Owned): an owned copy of the received bytes. When the ring
///   is at/below `recv_segment_reserve` the bytes are copied at delivery and the
///   bid is returned to the ring immediately (Mode C), so this entry pins
///   nothing and needs no replenish when consumed or on close.
#[derive(Clone)]
pub(crate) enum HeldRecvBuf {
    /// A provided-buffer bid pinned in the ring (zero-copy hold).
    Pinned {
        bid: u16,
        /// Bytes received into this buffer. The segment reader slices the buffer
        /// to this length; close-drain only needs the bid.
        len: u32,
    },
    /// An owned copy of the received bytes; its bid was already replenished at
    /// delivery, so consuming or dropping this entry replenishes nothing.
    Owned(bytes::Bytes),
}

/// In-flight segmented-recv Mode A forward write (see
/// `docs/segmented-recv-design.md`, "Mode A — Forward to an fd"). One per
/// connection at a time — writes to a sink are serialized (io_uring does not
/// order independent SQEs, so pipelining would reorder the byte stream). The
/// backing (a pinned provided-buffer bid or an owned copy) is kept alive here
/// until the write CQE arrives (SQE memory must outlive the op), then released
/// exactly once by `handle_forward_write`.
pub(crate) struct ForwardWriteState {
    /// Where the bytes being written live. `Pinned` releases its bid on
    /// completion; `Owned` just drops its heap bytes.
    pub(crate) backing: HeldRecvBuf,
    /// Total bytes to write from this backing — the prefix length, which may be
    /// shorter than the backing buffer when `len` ends mid-buffer (the suffix
    /// was already stashed into the accumulator by the forward future).
    pub(crate) total: u32,
    /// Bytes already written from this backing; advanced on short writes so the
    /// remainder resubmits at the correct source offset (and file offset).
    pub(crate) written: u32,
    /// Absolute file offset for byte 0 of this backing (0 for socket sinks).
    pub(crate) base_offset: u64,
    /// Sink fd (a non-fixed raw fd borrowed for the forward's lifetime via the
    /// non-raw `SinkFd` handle the caller passed to `forward_to`).
    pub(crate) sink_fd: RawFd,
    /// Whether the sink is a seekable buffered file (uses `pwrite` at offset)
    /// rather than a socket (`send` with `MSG_WAITALL`).
    pub(crate) is_file: bool,
    /// Connection generation captured at submit; the write CQE carries it in its
    /// payload so a stale completion (slot closed/reused) is ignored.
    pub(crate) generation: u32,
}

impl ForwardWriteState {
    /// Source pointer + remaining length for the next (re)submission, honoring
    /// bytes already written on a short write.
    pub(crate) fn remainder(&self, provided_bufs: &ProvidedBufRing) -> (*const u8, u32) {
        let base = match &self.backing {
            HeldRecvBuf::Pinned { bid, .. } => provided_bufs.get_buffer(*bid).0,
            HeldRecvBuf::Owned(bytes) => bytes.as_ptr(),
        };
        let ptr = unsafe { base.add(self.written as usize) };
        (ptr, self.total - self.written)
    }
}

/// I/O driver encapsulating all infrastructure state (ring, buffers, connections).
///
/// `AsyncEventLoop` is composed of a `Driver` + handler + executor.
///
/// # Drop order (load-bearing)
///
/// `ring` MUST be declared first so it drops *last*. `ProvidedBufRing` and
/// `InFlightSendSlab` reference kernel-pinned memory whose lifetime is tied
/// to the `io_uring` instance: the kernel only releases its DMA references
/// when `io_uring_release` runs. Dropping the buffer pools before the ring
/// would let `munmap` race against in-flight ZC notifications — a UAF.
///
/// The `driver_field_order` test below validates this ordering; do not
/// reorder these fields without updating both the assertion and the
/// shutdown drain in `run_shutdown`.
pub(crate) struct Driver {
    pub(crate) ring: Ring,
    pub(crate) connections: ConnectionTable,
    pub(crate) fixed_buffers: FixedBufferRegistry,
    pub(crate) provided_bufs: ProvidedBufRing,
    /// Provided buffer ring backing UDP multishot recvmsg. `None` when no UDP
    /// sockets are configured.
    pub(crate) udp_provided_bufs: Option<ProvidedBufRing>,
    /// Buffer IDs from `udp_provided_bufs` that have been consumed and need
    /// to be handed back to the kernel. Drained each tick.
    pub(crate) udp_pending_replenish: Vec<u16>,
    pub(crate) send_copy_pool: SendCopyPool,
    pub(crate) send_slab: InFlightSendSlab,
    pub(crate) accumulators: AccumulatorTable,
    pub(crate) pending_replenish: Vec<u16>,
    /// Per-connection pending recv buffer for zero-copy recv. When `Some`, the
    /// buffer ID has NOT been pushed to `pending_replenish` and must be
    /// replenished when the slot is cleared.
    pub(crate) pending_recv_bufs: Vec<Option<PendingRecvBuf>>,
    /// Per-connection original data length for in-flight SendRecvBuf operations.
    /// Set when `forward_recv_buf` initiates a send; used by `handle_send_recv_buf`
    /// to compute the correct offset on partial sends (since buf_size != data_len).
    pub(crate) send_recv_buf_original_lens: Vec<u32>,
    /// Per-connection remaining bytes for in-flight SendRecvBuf operations.
    /// Tracks how many bytes still need to be sent (decremented on each partial send).
    /// Stored here rather than in the CQE payload so that buffer sizes > u16::MAX are
    /// supported (the old encoding packed remaining into the high 16 bits of the payload).
    pub(crate) send_recv_buf_remaining: Vec<u32>,
    /// Per-connection multi-buffer zero-copy recv hold. When `recv_forward` is
    /// set for a connection, incoming provided buffers are pushed here (bids NOT
    /// replenished) instead of copied into the accumulator, then forwarded back
    /// in one coalesced `sendmsg` via `forward_held`. Backpressure is natural:
    /// unreplenished bids deplete the provided-buffer ring (ENOBUFS) until a
    /// forward completes and replenishes them.
    pub(crate) recv_hold: Vec<std::collections::VecDeque<PendingRecvBuf>>,
    /// Per-connection opt-in flag for the zero-copy recv-forward path.
    pub(crate) recv_forward: Vec<bool>,
    /// Per-connection recv delivery domain (segmented-recv). `CopyOrConsume`
    /// (default) uses the accumulator / single-buffer zero-copy path;
    /// `Segmented` holds arriving provided buffers in `segment_hold` instead.
    /// Reset at slot (re)activation and on close.
    pub(crate) recv_domain: Vec<crate::recv::domain::RecvDomain>,
    /// Per-connection held provided buffers for `Segmented` connections. Bids
    /// pushed here are pinned in the ring (NOT replenished, no accumulator copy)
    /// until a reader consumes them (later increment) or the connection closes.
    /// A reader consumes them (`SegmentReader::next`) or the connection closes.
    /// `close_connection` drains any still-held bids to `pending_replenish`.
    pub(crate) segment_hold: Vec<std::collections::VecDeque<HeldRecvBuf>>,
    /// Per-connection pin slot: the single provided buffer currently checked out
    /// to a live `RecvSegment` (moved here out of `segment_hold` by
    /// `SegmentReader::next`). The B2 lending-iterator contract is one live
    /// segment at a time, so a single `Option` suffices. This is the
    /// single-release discriminant: whoever `take()`s it (the segment's `Drop`
    /// under `try_with_state`, or `close_connection` under `&mut Driver`)
    /// replenishes the bid exactly once; the other sees `None` and does nothing.
    pub(crate) segment_pinned: Vec<Option<HeldRecvBuf>>,
    /// Per-connection in-flight segmented-recv Mode A forward write (see
    /// [`ForwardWriteState`]). `Some` while a write to the sink is outstanding;
    /// enforces the one-write-in-flight invariant and keeps the write's backing
    /// alive until its CQE. `close_connection` drains it (releasing a pinned
    /// bid); the write CQE clears it on completion.
    pub(crate) forward_write: Vec<Option<ForwardWriteState>>,
    /// Per-connection completed-forward-write result, produced by
    /// `handle_forward_write` and consumed by the `ForwardToFuture`: `Ok(n)` =
    /// bytes of the just-completed backing, `Err(errno)` = write failure.
    pub(crate) forward_done: Vec<Option<Result<u32, i32>>>,
    /// Per-connection flag: `true` while a Mode A `forward_to` is driving this
    /// connection (set by `ConnCtx::forward_to`, cleared by `settle_forward_end`
    /// / `reset_segment_state` / `close_connection`). Gates the `forward_hold_cap`
    /// throttle so it applies only to forwarding connections, not to pure Mode B
    /// segment readers that share the `Segmented` domain and `segment_hold`.
    pub(crate) forward_recv_active: Vec<bool>,
    /// Per-connection flag: `true` while a forwarding connection's multishot recv
    /// has been throttled (cancelled) because its `segment_hold` reached
    /// `forward_hold_cap`. Set at the throttle point in the recv handler; cleared
    /// when the recv is re-armed after the hold drains below the cap
    /// (`maybe_rearm_throttled_forward`) or on `settle_forward_end` / close.
    /// Gates re-arm so the starved-connection path does not fight the throttle.
    pub(crate) forward_hold_throttled: Vec<bool>,
    /// Per-connection held-buffer cap for Mode A `forward_to`
    /// (`Config::forward_hold_cap`). When a forwarding connection's `segment_hold`
    /// length reaches this, its multishot recv is cancelled (TCP window closes)
    /// and re-armed once the hold drains below the cap.
    pub(crate) forward_hold_cap: usize,
    pub(crate) accept_rx: Option<crossbeam_channel::Receiver<(RawFd, SocketAddr)>>,
    pub(crate) eventfd: RawFd,
    pub(crate) eventfd_buf: [u8; 8],
    /// Wake handle for cross-thread wakeup (wraps the eventfd).
    pub(crate) wake_handle: crate::wakeup::WakeFd,
    /// Deadline-based flush interval. None = disabled (SQPOLL or explicit 0).
    pub(crate) flush_interval: Option<Duration>,
    pub(crate) shutdown_flag: Arc<AtomicBool>,
    pub(crate) shutdown_local: bool,
    pub(crate) tls_table: Option<crate::tls::TlsTable>,
    /// Pre-allocated sockaddr storage for outbound connect SQEs.
    pub(crate) connect_addrs: Vec<libc::sockaddr_storage>,
    /// Pre-allocated timespec storage for connect timeouts.
    pub(crate) connect_timespecs: Vec<io_uring::types::Timespec>,
    /// Pre-allocated batch buffer for draining CQEs.
    /// Tuple: (user_data, result, flags).
    pub(crate) cqe_batch: Vec<(u64, i32, u32)>,
    /// Per-worker channel for DNS resolve responses from the resolver pool.
    pub(crate) resolve_rx: Option<crossbeam_channel::Receiver<crate::resolver::ResolveResponse>>,
    /// Per-worker sender for resolve responses (cloned into each request).
    pub(crate) resolve_tx: Option<crossbeam_channel::Sender<crate::resolver::ResolveResponse>>,
    /// Shared resolver pool (for submitting requests).
    pub(crate) resolver: Option<std::sync::Arc<crate::resolver::ResolverPool>>,
    /// Per-worker channel for spawn responses from the spawner pool.
    pub(crate) spawn_rx: Option<crossbeam_channel::Receiver<crate::spawner::SpawnResponse>>,
    /// Per-worker sender for spawn responses (cloned into each request).
    pub(crate) spawn_tx: Option<crossbeam_channel::Sender<crate::spawner::SpawnResponse>>,
    /// Shared spawner pool (for submitting requests).
    pub(crate) spawner: Option<std::sync::Arc<crate::spawner::SpawnerPool>>,
    /// Per-worker channel for blocking responses from the blocking pool.
    pub(crate) blocking_rx: Option<crossbeam_channel::Receiver<crate::blocking::BlockingResponse>>,
    /// Per-worker sender for blocking responses (cloned into each request).
    pub(crate) blocking_tx: Option<crossbeam_channel::Sender<crate::blocking::BlockingResponse>>,
    /// Shared blocking pool (for submitting requests).
    pub(crate) blocking_pool: Option<std::sync::Arc<crate::blocking::BlockingPool>>,
    /// Region-registry control channel — drained each tick to apply
    /// dynamic fixed-buffer registrations from
    /// [`ShutdownHandle::register_region`](crate::ShutdownHandle::register_region).
    pub(crate) region_rx: crate::region_registry::RegionControlRx,
    /// Whether to set TCP_NODELAY on connections.
    pub(crate) tcp_nodelay: bool,
    /// Guard sends below this total length fall back to copy (0 = always ZC).
    pub(crate) send_zc_threshold: u32,
    /// Aggregate low-water reserve for segmented recv (Config::recv_segment_reserve).
    /// When `provided_bufs.free() <= recv_segment_reserve`, an arriving segmented
    /// buffer is force-copied (Mode C) and its bid replenished immediately instead
    /// of pinned, so held segments cannot deplete the shared ring under fan-in.
    pub(crate) recv_segment_reserve: u32,
    /// Upper bound on outstanding recv bytes per connection (mirrors
    /// `AccumulatorTable`'s per-accumulator `max_size`). Used to bound held TLS
    /// plaintext delivered as owned segments in the segmented recv domain, where
    /// the bytes never touch the accumulator but the same flood-kill contract
    /// (`Config::recv_accumulator_max`) must hold.
    pub(crate) recv_accumulator_max: usize,
    /// Whether SO_TIMESTAMPING is enabled for connections.
    #[cfg(feature = "timestamps")]
    pub(crate) timestamps: bool,
    /// Pinned msghdr template for RecvMsgMulti with SO_TIMESTAMPING.
    /// Used as the SQE template and for parsing CQE buffers via RecvMsgOut.
    #[cfg(feature = "timestamps")]
    pub(crate) recvmsg_msghdr: Box<libc::msghdr>,
    /// Per-connection send chain tracking for IOSQE_IO_LINK chains.
    pub(crate) chain_table: SendChainTable,
    /// Maximum SQEs per chain (0 = disabled).
    pub(crate) max_chain_length: u16,
    /// Per-connection send queues for serializing sends (one in-flight at a time).
    pub(crate) send_queues: Vec<ConnSendState>,
    /// Connection indices that currently have a `close_notify_deadline`
    /// armed (TLS graceful-shutdown timeout). The event loop's
    /// `check_close_notify_deadlines` iterates this set instead of
    /// walking every entry in `send_queues`, which is critical for
    /// non-TLS workloads — without this, the per-iteration deadline
    /// scan dominates worker CPU at high request rates.
    pub(crate) close_notify_armed: Vec<u32>,
    /// Configured close_notify drain deadline (Config::close_notify_timeout_ms).
    pub(crate) close_notify_timeout: std::time::Duration,
    /// Scratch for TLS output sends collected during CQE handling.
    pub(crate) tls_out_scratch: Vec<crate::handler::BuiltSend>,
    /// Monotonic disk-I/O sequence (see DriverCtx::disk_io_key).
    pub(crate) next_disk_io_seq: u16,
    /// Connections whose multishot recv hit ENOBUFS and is parked until
    /// provided-ring buffers return. Re-arming immediately (the old
    /// behavior) completed instantly with ENOBUFS again while data was
    /// pending and the ring was empty — a 100% CPU spin until some task
    /// released a bid.
    pub(crate) recv_starved: Vec<u32>,
    /// Lifetime count of ENOBUFS parks on this worker (pushes onto
    /// `recv_starved`). Reported in the shutdown diag line; sustained
    /// growth means responses exceed the provided ring and receive
    /// throughput is gated on buffer recycling.
    pub(crate) recv_park_count: u64,
    /// Per-connection flag: a fallback one-shot recv is in flight. While
    /// set, the connection must be neither re-armed for multishot recv nor
    /// given a second fallback — io_uring does not order independent SQEs,
    /// so two in-flight recvs on one stream could append out of order. The
    /// flag is cleared only in `handle_recv_fallback` (the fallback's own
    /// completion) and in `close_connection` (any still-in-flight CQE is
    /// then generation-checked and releases its pool slot without touching
    /// connection state).
    pub(crate) recv_fallback_inflight: Vec<bool>,
    /// Slot pool backing fallback one-shot recvs. Kernel writes land in
    /// pool-owned memory whose slot is released only by the fallback CQE —
    /// stale completions after close/slot-reuse are memory-safe by the same
    /// lifecycle argument as `send_copy_pool` (SQE memory outlives the op).
    /// Lazily constructed on first fallback so workloads that never starve
    /// the provided ring pay nothing.
    pub(crate) fallback_recv_pool: Option<SendCopyPool>,
    /// Per-pool-slot owner: `(conn_index, generation)` recorded at submit,
    /// validated in `handle_recv_fallback` before any connection state is
    /// touched (slots recycle; stale CQEs are normal).
    pub(crate) fallback_slot_owner: Vec<(u32, u32)>,
    /// Size of each fallback recv chunk (bytes). A few multiples of the
    /// provided-ring buffer size: one event-loop pass moves one chunk per
    /// starved connection, so this bounds per-pass fallback throughput.
    pub(crate) fallback_chunk: u32,
    /// Lifetime count of fallback recv submissions on this worker
    /// (reported in the shutdown diag line).
    pub(crate) recv_fallback_count: u64,
    /// Arrival timestamp shared by all UDP datagrams queued in one CQE
    /// drain batch. One clock read per batch instead of one per datagram;
    /// precision loss is bounded by the drain duration (microseconds).
    pub(crate) udp_batch_recv_at: std::time::Instant,
    /// Tick timeout duration. When set, a timeout SQE ensures the event loop
    /// wakes periodically even when no I/O completions are pending.
    pub(crate) tick_timeout_ts: Option<io_uring::types::Timespec>,
    /// Whether a tick timeout SQE is currently in-flight.
    pub(crate) tick_timeout_armed: bool,
    /// Monotonic tick counter for backoff-based retry scheduling.
    pub(crate) tick_count: u64,
    /// Whether the eventfd read SQE is currently armed.
    pub(crate) eventfd_armed: bool,
    /// Pending ZC send retries: (conn_index, generation, slab_idx, retries). Drained each tick.
    pub(crate) pending_zc_retries: Vec<(u32, u32, u16, u8)>,
    /// Pending copy send retries: (conn_index, generation, pool_slot, retries). Drained each tick.
    pub(crate) pending_copy_retries: Vec<(u32, u32, u16, u8, OpTag)>,
    /// Pending PollAdd-on-POLLOUT retries from the EAGAIN backpressure
    /// path: (conn_index, generation, pool_slot, retries). Drained each tick.
    /// Only used when `submit_send_pollout` itself failed (SQ full at
    /// the time the EAGAIN CQE arrived).
    pub(crate) pending_send_pollout_retries: Vec<(u32, u32, u16, u8, bool)>,
    /// Pending coalesced-send retries: (conn_index, generation, slab_idx, retries).
    /// Drained each tick — used when resubmitting a coalesced `sendmsg` (partial
    /// remainder or POLLOUT rearm) found the SQ full.
    pub(crate) pending_coalesced_retries: Vec<(u32, u32, u16, u8)>,
    /// Pending recv-forward send resubmissions that failed (SQ full):
    /// (conn_index, generation, slab_idx, retries). Drained each tick.
    pub(crate) pending_recv_forward_retries: Vec<(u32, u32, u16, u8)>,
    /// Pending close retries: (conn_index, retries). Drained each tick.
    pub(crate) pending_close_retries: Vec<(u32, u8)>,
    /// Scratch buffers swapped with the corresponding `pending_*_retries`
    /// queue at drain time so the per-tick drain reuses a single allocation
    /// across ticks (the primary queue is left empty for in-iteration
    /// re-enqueues; `drain(..)` on the scratch keeps its capacity).
    pub(crate) zc_retry_scratch: Vec<(u32, u32, u16, u8)>,
    pub(crate) copy_retry_scratch: Vec<(u32, u32, u16, u8, OpTag)>,
    pub(crate) send_pollout_retry_scratch: Vec<(u32, u32, u16, u8, bool)>,
    pub(crate) coalesced_retry_scratch: Vec<(u32, u32, u16, u8)>,
    pub(crate) recv_forward_retry_scratch: Vec<(u32, u32, u16, u8)>,
    /// Per-worker UDP socket state.
    pub(crate) udp_sockets: Vec<UdpSocketState>,
    /// NVMe device tracking table. `None` when NVMe is not configured.
    pub(crate) nvme_devices: Option<crate::nvme::NvmeDeviceTable>,
    /// NVMe command slab for tracking in-flight commands. `None` when NVMe is not configured.
    pub(crate) nvme_cmd_slab: Option<crate::nvme::NvmeCmdSlab>,
    /// Base offset in the fixed file table for NVMe device fds.
    /// NVMe devices are registered at `nvme_fd_base + device_index`.
    pub(crate) nvme_fd_base: u32,
    /// Direct I/O file tracking table. `None` when direct I/O is not configured.
    pub(crate) direct_io_files: Option<crate::direct_io::DirectIoFileTable>,
    /// Direct I/O command slab for tracking in-flight commands. `None` when not configured.
    pub(crate) direct_io_cmd_slab: Option<crate::direct_io::DirectIoCmdSlab>,
    /// Base offset in the fixed file table for direct I/O file fds.
    pub(crate) direct_io_fd_base: u32,
    /// Filesystem file tracking table. `None` when fs is not configured.
    pub(crate) fs_files: Option<crate::fs::FsFileTable>,
    /// Filesystem command slab for tracking in-flight commands. `None` when not configured.
    pub(crate) fs_cmd_slab: Option<crate::fs::FsCmdSlab>,
    /// Base offset in the fixed file table for filesystem file fds.
    pub(crate) fs_fd_base: u32,
}

impl Driver {
    /// Create a new driver for a worker thread.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: &Config,
        accept_rx: Option<crossbeam_channel::Receiver<(RawFd, SocketAddr)>>,
        eventfd: RawFd,
        shutdown_flag: Arc<AtomicBool>,
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
        config.validate()?;
        let ring = Ring::setup(config)?;

        let fixed_buffers =
            FixedBufferRegistry::new(&config.registered_regions, config.max_registered_regions);

        let provided_bufs = ProvidedBufRing::new(
            config.recv_buffer.bgid,
            config.recv_buffer.ring_size,
            config.recv_buffer.buffer_size,
        )?;

        let udp_count = config.udp_bind.len() as u32;
        let udp_provided_bufs = if udp_count > 0 {
            Some(ProvidedBufRing::new(
                config.udp_recv_buffer.bgid,
                config.udp_recv_buffer.ring_size,
                config.udp_recv_buffer.buffer_size,
            )?)
        } else {
            None
        };
        let nvme_max = config
            .nvme
            .as_ref()
            .map(|n| n.max_devices as u32)
            .unwrap_or(0);
        let direct_io_max = config
            .direct_io
            .as_ref()
            .map(|d| d.max_files as u32)
            .unwrap_or(0);
        let fs_max = config.fs.as_ref().map(|f| f.max_files as u32).unwrap_or(0);

        // Register resources with the kernel
        ring.register_buffers(&fixed_buffers)?;
        ring.register_files_sparse(
            config.max_connections + udp_count + nvme_max + direct_io_max + fs_max,
        )?;
        ring.register_buf_ring(&provided_bufs)?;
        if let Some(ref udp_bufs) = udp_provided_bufs {
            ring.register_buf_ring(udp_bufs)?;
        }

        let connections = ConnectionTable::new(config.max_connections);
        let send_copy_pool = SendCopyPool::new(config.send_copy_count, config.send_copy_slot_size);
        let send_slab = InFlightSendSlab::new(config.send_slab_slots);
        let accumulators = AccumulatorTable::new_with_max(
            config.max_connections,
            config.recv_accumulator_capacity,
            config.recv_accumulator_max,
        );

        // Deadline flush: disabled when SQPOLL (kernel polls SQ) or interval is 0.
        let flush_interval = if config.sqpoll || config.flush_interval_us == 0 {
            None
        } else {
            Some(Duration::from_micros(config.flush_interval_us))
        };

        let tls_table = {
            let has_server = config.tls.is_some();
            let has_client = config.tls_client.is_some();
            if has_server || has_client {
                Some(crate::tls::TlsTable::new(
                    config.max_connections,
                    config.tls.as_ref().map(|tc| tc.server_config.clone()),
                    config
                        .tls_client
                        .as_ref()
                        .map(|tc| tc.client_config.clone()),
                ))
            } else {
                None
            }
        };

        let mut connect_addrs = Vec::with_capacity(config.max_connections as usize);
        connect_addrs.resize(config.max_connections as usize, unsafe {
            std::mem::zeroed()
        });

        let mut connect_timespecs = Vec::with_capacity(config.max_connections as usize);
        connect_timespecs.resize(
            config.max_connections as usize,
            io_uring::types::Timespec::new(),
        );

        let mut send_queues = Vec::with_capacity(config.max_connections as usize);
        for _ in 0..config.max_connections {
            send_queues.push(ConnSendState::new());
        }

        // Set up UDP sockets.
        let mut udp_sockets = Vec::with_capacity(config.udp_bind.len());
        for (udp_idx, bind_addr) in config.udp_bind.iter().enumerate() {
            let fd_index = config.max_connections + udp_idx as u32;
            let connect_peer = config.udp_connect_peers.get(udp_idx).copied().flatten();
            let state = Self::setup_udp_socket(
                &ring,
                *bind_addr,
                connect_peer,
                fd_index,
                config.udp_send_slots,
                config.udp_gro,
            )?;
            udp_sockets.push(state);
        }

        let mut driver = Driver {
            ring,
            connections,
            fixed_buffers,
            provided_bufs,
            udp_provided_bufs,
            udp_pending_replenish: Vec::new(),
            send_copy_pool,
            send_slab,
            accumulators,
            pending_replenish: Vec::with_capacity(config.recv_buffer.ring_size as usize),
            pending_recv_bufs: vec![None; config.max_connections as usize],
            send_recv_buf_original_lens: vec![0; config.max_connections as usize],
            send_recv_buf_remaining: vec![0; config.max_connections as usize],
            recv_hold: (0..config.max_connections)
                .map(|_| std::collections::VecDeque::new())
                .collect(),
            recv_forward: vec![false; config.max_connections as usize],
            recv_domain: vec![
                crate::recv::domain::RecvDomain::default();
                config.max_connections as usize
            ],
            segment_hold: (0..config.max_connections)
                .map(|_| std::collections::VecDeque::new())
                .collect(),
            segment_pinned: vec![None; config.max_connections as usize],
            forward_write: (0..config.max_connections).map(|_| None).collect(),
            forward_done: (0..config.max_connections).map(|_| None).collect(),
            forward_recv_active: vec![false; config.max_connections as usize],
            forward_hold_throttled: vec![false; config.max_connections as usize],
            forward_hold_cap: config.forward_hold_cap,
            accept_rx,
            eventfd,
            eventfd_buf: [0u8; 8],
            wake_handle: crate::wakeup::WakeFd::from_raw_fd(eventfd),
            flush_interval,
            shutdown_flag,
            shutdown_local: false,
            tls_table,
            connect_addrs,
            connect_timespecs,
            cqe_batch: Vec::with_capacity(config.sq_entries as usize * 4),
            tcp_nodelay: config.tcp_nodelay,
            send_zc_threshold: config.send_zc_threshold,
            recv_segment_reserve: config.recv_segment_reserve,
            recv_accumulator_max: config.recv_accumulator_max,
            #[cfg(feature = "timestamps")]
            timestamps: config.timestamps,
            #[cfg(feature = "timestamps")]
            recvmsg_msghdr: {
                let mut hdr: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
                // TCP: no source address needed.
                hdr.msg_namelen = 0;
                // Room for SCM_TIMESTAMPING cmsg: cmsghdr(16) + 3×timespec(48) = 64 bytes.
                hdr.msg_controllen = 64;
                hdr
            },
            chain_table: SendChainTable::new(config.max_connections),
            max_chain_length: config.max_chain_length,
            send_queues,
            close_notify_armed: Vec::new(),
            close_notify_timeout: std::time::Duration::from_millis(config.close_notify_timeout_ms),
            tls_out_scratch: Vec::new(),
            next_disk_io_seq: 0,
            recv_starved: Vec::new(),
            recv_park_count: 0,
            recv_fallback_inflight: vec![false; config.max_connections as usize],
            fallback_recv_pool: None,
            fallback_slot_owner: Vec::new(),
            // One event-loop pass moves at most one chunk per starved
            // connection, so the chunk — not the provided ring — is the
            // per-pass byte ceiling while degraded. It must be LARGER than
            // the ring's capacity to beat the park/re-arm churn cycle it
            // replaces (which moves one ring's worth per pass); a small
            // chunk would be slower than the pathology. Floor of 1 MiB,
            // scaled up for jumbo provided buffers.
            fallback_chunk: config
                .recv_buffer
                .buffer_size
                .saturating_mul(4)
                .max(1 << 20),
            recv_fallback_count: 0,
            udp_batch_recv_at: std::time::Instant::now(),
            tick_timeout_ts: if config.tick_timeout_us > 0 {
                Some(
                    io_uring::types::Timespec::new()
                        .sec(config.tick_timeout_us / 1_000_000)
                        .nsec((config.tick_timeout_us % 1_000_000) as u32 * 1000),
                )
            } else {
                None
            },
            tick_timeout_armed: false,
            tick_count: 0,
            eventfd_armed: false,
            pending_zc_retries: Vec::new(),
            pending_copy_retries: Vec::new(),
            pending_send_pollout_retries: Vec::new(),
            pending_coalesced_retries: Vec::new(),
            pending_recv_forward_retries: Vec::new(),
            pending_close_retries: Vec::new(),
            zc_retry_scratch: Vec::new(),
            copy_retry_scratch: Vec::new(),
            send_pollout_retry_scratch: Vec::new(),
            coalesced_retry_scratch: Vec::new(),
            recv_forward_retry_scratch: Vec::new(),
            udp_sockets,
            nvme_devices: config
                .nvme
                .as_ref()
                .map(|n| crate::nvme::NvmeDeviceTable::new(n.max_devices)),
            nvme_cmd_slab: config
                .nvme
                .as_ref()
                .map(|n| crate::nvme::NvmeCmdSlab::new(n.max_commands_in_flight)),
            nvme_fd_base: config.max_connections + udp_count,
            direct_io_files: config
                .direct_io
                .as_ref()
                .map(|d| crate::direct_io::DirectIoFileTable::new(d.max_files)),
            direct_io_cmd_slab: config
                .direct_io
                .as_ref()
                .map(|d| crate::direct_io::DirectIoCmdSlab::new(d.max_commands_in_flight)),
            direct_io_fd_base: config.max_connections + udp_count + nvme_max,
            fs_files: config
                .fs
                .as_ref()
                .map(|f| crate::fs::FsFileTable::new(f.max_files)),
            fs_cmd_slab: config
                .fs
                .as_ref()
                .map(|f| crate::fs::FsCmdSlab::new(f.max_commands_in_flight)),
            fs_fd_base: config.max_connections + udp_count + nvme_max + direct_io_max,
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
        };

        // Arm multishot recv for each UDP socket against the UDP buffer
        // group. One SQE per socket stays live in the kernel and fans out a
        // CQE per datagram until the buffer ring is exhausted. Connected
        // sockets use the lighter `RecvUdp` opcode (no recvmsg metadata);
        // unconnected sockets use `RecvMsgUdp` so the kernel returns the
        // peer address in each CQE buffer.
        let udp_bgid = config.udp_recv_buffer.bgid;
        for udp_idx in 0..driver.udp_sockets.len() {
            let fd_index = driver.udp_sockets[udp_idx].fd_index;
            let result = if driver.udp_sockets[udp_idx].connected_peer.is_some() {
                let ud = UserData::encode(OpTag::RecvUdp, udp_idx as u32, 0);
                driver
                    .ring
                    .submit_multishot_recv_udp(fd_index, udp_bgid, ud)
            } else {
                let ud = UserData::encode(OpTag::RecvMsgUdp, udp_idx as u32, 0);
                let msghdr_ptr = &*driver.udp_sockets[udp_idx].recv_msghdr as *const libc::msghdr;
                driver
                    .ring
                    .submit_recvmsg_multishot(fd_index, msghdr_ptr, udp_bgid, ud)
            };
            result.map_err(|e| {
                crate::error::Error::RingSetup(format!(
                    "failed to submit initial UDP multishot recv: {e}"
                ))
            })?;
        }

        Ok(driver)
    }

    /// Construct a [`DriverCtx`] by borrowing driver fields.
    ///
    /// Borrows `self` mutably, so callers cannot access individual driver
    /// fields while the returned `DriverCtx` is live. For cases requiring
    /// simultaneous access to specific fields (e.g., accumulators + ctx),
    /// construct `DriverCtx` inline with explicit field borrows.
    pub(crate) fn make_ctx(&mut self) -> DriverCtx<'_> {
        DriverCtx {
            ring: &mut self.ring,
            connections: &mut self.connections,
            fixed_buffers: &mut self.fixed_buffers,
            send_copy_pool: &mut self.send_copy_pool,
            send_slab: &mut self.send_slab,
            tls_table: match self.tls_table {
                Some(ref mut t) => t as *mut crate::tls::TlsTable,
                None => std::ptr::null_mut(),
            },
            shutdown_requested: &mut self.shutdown_local,
            connect_addrs: &mut self.connect_addrs,
            tcp_nodelay: self.tcp_nodelay,
            send_zc_threshold: self.send_zc_threshold,
            #[cfg(feature = "timestamps")]
            timestamps: self.timestamps,
            #[cfg(feature = "timestamps")]
            recvmsg_msghdr: &*self.recvmsg_msghdr as *const libc::msghdr,
            connect_timespecs: &mut self.connect_timespecs,
            chain_table: &mut self.chain_table,
            max_chain_length: self.max_chain_length,
            send_queues: &mut self.send_queues,
            close_notify_armed: &mut self.close_notify_armed,
            udp_sockets: &mut self.udp_sockets,
            nvme_devices: &mut self.nvme_devices,
            nvme_cmd_slab: &mut self.nvme_cmd_slab,
            nvme_fd_base: self.nvme_fd_base,
            direct_io_files: &mut self.direct_io_files,
            direct_io_cmd_slab: &mut self.direct_io_cmd_slab,
            direct_io_fd_base: self.direct_io_fd_base,
            fs_files: &mut self.fs_files,
            fs_cmd_slab: &mut self.fs_cmd_slab,
            fs_fd_base: self.fs_fd_base,
            pending_close_retries: &mut self.pending_close_retries,
            close_notify_timeout: self.close_notify_timeout,
            next_disk_io_seq: &mut self.next_disk_io_seq,
        }
    }

    /// Reset segmented-recv delivery state for a (re)activated connection slot.
    /// The hold is already drained by `close_connection`, so this is defensive;
    /// it also restores the domain to the default in case a slot is reused
    /// without an intervening close.
    pub(crate) fn reset_segment_state(&mut self, conn_index: u32) {
        self.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::default();
        self.segment_hold[conn_index as usize].clear();
        // The pin slot should already be None (close drains it), but clear it
        // defensively for a slot reused without an intervening close-drain.
        self.segment_pinned[conn_index as usize] = None;
        // Mode A forward state — cleared defensively (close drains any pinned
        // bid). A leftover `forward_done` result must not bleed into a reused
        // slot's forward.
        self.forward_write[conn_index as usize] = None;
        self.forward_done[conn_index as usize] = None;
        self.forward_recv_active[conn_index as usize] = false;
        self.forward_hold_throttled[conn_index as usize] = false;
    }

    /// Settle a connection when a Mode A `forward_to` finishes: any provided
    /// buffers still held in `segment_hold` carry stream bytes *past* the
    /// forwarded region, so their bytes are preserved by copying them into the
    /// accumulator (in arrival order) before the bid is replenished — exactly
    /// like the `with_segments` under-drain path. The delivery domain is then
    /// reset so subsequent `with_data`/`with_bytes` reads see those bytes first.
    ///
    /// Returns `false` if the accumulator overflowed (`recv_accumulator_max`),
    /// in which case the caller must close the connection.
    #[must_use]
    pub(crate) fn settle_forward_end(&mut self, conn_index: u32) -> bool {
        let mut ok = true;
        while let Some(held) = self.segment_hold[conn_index as usize].pop_front() {
            match held {
                HeldRecvBuf::Pinned { bid, len } => {
                    let (ptr, _) = self.provided_bufs.get_buffer(bid);
                    let data = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
                    if ok && !self.accumulators.append(conn_index, data) {
                        ok = false;
                    }
                    // Replenish the bid regardless (pushing outside the overflow
                    // guard avoids a leak on breach).
                    self.pending_replenish.push(bid);
                }
                HeldRecvBuf::Owned(bytes) => {
                    if ok && !self.accumulators.append(conn_index, &bytes[..]) {
                        ok = false;
                    }
                }
            }
        }
        self.recv_domain[conn_index as usize] = crate::recv::domain::RecvDomain::default();
        // If the forward throttled its recv (hold reached `forward_hold_cap`), its
        // multishot was cancelled. Re-arm it so the connection's subsequent
        // `with_data`/`with_bytes` reads resume — the domain is now the default,
        // so newly received bytes land in the accumulator. If the recv is still
        // armed (its ECANCELED not yet observed, or it was never throttled),
        // nothing to do. On a re-arm failure the connection is left unarmed; the
        // caller's next read errors and closes it (standard recovery).
        self.forward_recv_active[conn_index as usize] = false;
        if self.forward_hold_throttled[conn_index as usize] {
            self.forward_hold_throttled[conn_index as usize] = false;
            let armed = self
                .connections
                .get(conn_index)
                .is_some_and(|c| c.recv_multishot_armed);
            let open = self
                .connections
                .get(conn_index)
                .is_some_and(|c| matches!(c.recv_mode, RecvMode::Multi));
            if !armed
                && open
                && self.ring.submit_multishot_recv(conn_index).is_ok()
                && let Some(cs) = self.connections.get_mut(conn_index)
            {
                cs.recv_multishot_armed = true;
            }
        }
        ok
    }

    /// Submit the first write of a segmented-recv Mode A forward `backing`
    /// (`total` bytes) to `sink_fd` and record the in-flight state. One write is
    /// in flight per connection; the completion handler releases the backing.
    ///
    /// On submission failure the backing is released here (a pinned bid returns
    /// to the ring) and the error is propagated — the forward future surfaces it
    /// to the caller.
    pub(crate) fn start_forward_write(
        &mut self,
        conn_index: u32,
        backing: HeldRecvBuf,
        total: u32,
        base_offset: u64,
        sink_fd: RawFd,
        is_file: bool,
    ) -> io::Result<()> {
        debug_assert!(
            self.forward_write[conn_index as usize].is_none(),
            "start_forward_write while a forward write is already in flight"
        );
        let generation = self.connections.generation(conn_index);
        let ud = crate::completion::UserData::encode(
            crate::completion::OpTag::ForwardWrite,
            conn_index,
            generation,
        );
        let ptr = match &backing {
            HeldRecvBuf::Pinned { bid, .. } => self.provided_bufs.get_buffer(*bid).0,
            HeldRecvBuf::Owned(bytes) => bytes.as_ptr(),
        };
        let res = if is_file {
            unsafe {
                self.ring
                    .submit_forward_write_file(sink_fd, ptr, total, base_offset, ud)
            }
        } else {
            unsafe {
                self.ring
                    .submit_forward_write_socket(sink_fd, ptr, total, ud)
            }
        };
        match res {
            Ok(()) => {
                self.forward_write[conn_index as usize] = Some(ForwardWriteState {
                    backing,
                    total,
                    written: 0,
                    base_offset,
                    sink_fd,
                    is_file,
                    generation,
                });
                Ok(())
            }
            Err(e) => {
                if let HeldRecvBuf::Pinned { bid, .. } = backing {
                    self.pending_replenish.push(bid);
                }
                Err(e)
            }
        }
    }

    pub(crate) fn close_connection(&mut self, conn_index: u32) {
        if let Some(conn) = self.connections.get_mut(conn_index) {
            if matches!(conn.recv_mode, RecvMode::Closed) {
                return; // already closing — avoid double Close SQE
            }
            conn.recv_mode = RecvMode::Closed;
        } else {
            return;
        }
        // Replenish any held (not-yet-forwarded) zero-copy recv buffers so their
        // bids aren't leaked, and clear the opt-in flag for slot reuse. An
        // in-flight forward's bids live in its slab entry (already drained from
        // recv_hold) and are replenished by its own completion handler.
        if self.recv_forward[conn_index as usize] {
            for pending in self.recv_hold[conn_index as usize].drain(..) {
                self.pending_replenish.push(pending.bid);
            }
            self.recv_forward[conn_index as usize] = false;
        }
        // Do NOT drain held segmented-recv buffers here. When a peer FIN drives
        // this close, a parked Mode B reader must still consume the bytes already
        // held — draining them now (before the woken reader is polled) would
        // discard a fully-received response and surface an empty EOF (the
        // data+FIN-loss bug): a data CQE and the FIN can land in the same batch,
        // and `close_connection` runs before `poll_ready_tasks`. The reader drains
        // the hold (replenishing each bid as it consumes, seeing EOF once the hold
        // is empty and the connection is `Closed`); any buffers it never reaches
        // are reclaimed at `handle_close` (teardown), symmetric to `segment_pinned`.
        // The delivery domain is reset at slot reuse (`reset_segment_state`).
        // A bid checked out to a live `RecvSegment` (a parked task holding a
        // segment across an await) must NOT be reclaimed here. `RecvSegment::deref`
        // reads that bid's provided buffer directly, and a parked task can still
        // resume and deref it between now and the `Close` CQE — returning the bid
        // to the ring now would let another connection's recv overwrite a buffer a
        // live segment is still reading (a safe-API data leak). The bid stays
        // pinned in `segment_pinned[conn]` until `handle_close`, which reclaims it
        // after `remove_connection` has dropped the future (and with it the
        // segment). `RecvSegment::drop` running in-poll before then still releases
        // it (the pin slot is the single-release discriminant); if the drop runs
        // unguarded during teardown it no-ops and `handle_close` does the release.
        // An in-flight Mode A forward write's backing is still owned by the kernel
        // (it is the write's *source* memory) until the write CQE arrives —
        // reclaiming it now would either return a provided bid to the ring while
        // the kernel is still DMA-reading it (another connection's recv could then
        // overwrite it: cross-connection corruption / info leak) or free an Owned
        // copy the kernel is still reading (use-after-free). Both violate
        // invariant #1 (SQE memory must outlive the op). So do NOT reclaim it here.
        // Instead cancel the in-flight write so a stuck sink cannot pin the bid and
        // slot forever, and leave `forward_write[conn]` in place: the (possibly
        // ECANCELED) CQE is handled by `handle_forward_write` / `fail_forward_write`,
        // which replenish the bid exactly once and drive this deferred close
        // forward. `try_finalize_close` will not submit the `Close` SQE — and so the
        // slot is not reused and the captured generation stays valid — while
        // `forward_write[conn]` is still `Some`.
        if let Some(state) = self.forward_write[conn_index as usize].as_ref() {
            let write_gen = state.generation;
            // The in-flight op is either the write itself or its POLLOUT re-arm;
            // cancel both user_data variants (the non-matching one is a harmless
            // ENOENT whose `Cancel` CQE is ignored).
            let write_ud = crate::completion::UserData::encode(
                crate::completion::OpTag::ForwardWrite,
                conn_index,
                write_gen,
            );
            let pollout_ud = crate::completion::UserData::encode(
                crate::completion::OpTag::ForwardWritePollOut,
                conn_index,
                write_gen,
            );
            let _ = self.ring.submit_async_cancel(write_ud.raw(), conn_index);
            let _ = self.ring.submit_async_cancel(pollout_ud.raw(), conn_index);
        } else {
            // No forward write in flight — safe to clear a stale completed result.
            self.forward_done[conn_index as usize] = None;
        }
        // Clear the Mode A forward flags for slot reuse. Any held bids were
        // already drained above (segment_hold / segment_pinned / forward_write);
        // the throttle flag just tracks recv-arm state and needs no bid release.
        // A throttle-cancel's ECANCELED (if still in flight) is a no-op in
        // `handle_recv_multi` (result < 0 / generation checks).
        self.forward_recv_active[conn_index as usize] = false;
        self.forward_hold_throttled[conn_index as usize] = false;
        // Clear the fallback-recv flag so the slot's next occupant starts
        // clean. A still-in-flight fallback CQE for this occupant is
        // generation-checked in handle_recv_fallback and only releases its
        // pool slot — it can no longer touch connection state.
        self.recv_fallback_inflight[conn_index as usize] = false;
        // Cancel any active chain — per-SQE resources released as CQEs arrive.
        self.chain_table.cancel(conn_index);
        // Defer the actual `Close` SQE until every queued send has
        // drained through the regular `submit_next_queued` cycle and
        // its CQE has been handled. Two earlier mistakes are worth
        // calling out:
        //
        //   (a) The original code called `drain_conn_send_queue`,
        //       which freed slab/pool resources for queued sends
        //       *without ever submitting them* — silently dropping
        //       every byte queued behind the in-flight send.
        //
        //   (b) An interim attempt pushed all queued SQEs in parallel
        //       at close time, but io_uring doesn't strictly order
        //       independent SQEs, so the kernel could process them
        //       out of order and scramble the byte stream. (Visible
        //       in release mode where the SQEs land close together.)
        //
        // The current strategy preserves the queue's serialized
        // drain: leave in_flight + queue alone, mark `close_pending`,
        // and let `note_send_finalized` (called from `handle_send` /
        // `handle_send_pollout` after each completion) submit the
        // deferred `Close` once both `in_flight` is false and the
        // queue is empty.
        let state = &mut self.send_queues[conn_index as usize];
        state.close_pending = true;
        if state.in_flight {
            // Something is in-flight; wait for its CQE to drain the queue.
            return;
        }
        // Nothing in-flight but queue has items — push them into the SQ
        // so their CQEs can fire and continue the serialized drain cycle.
        if !state.queue.is_empty() {
            let pushed = self.flush_queued_sends_or_release(conn_index);
            if pushed == 0 {
                // SQ was full and we gave up — the queue was drained
                // by flush_queued_sends_or_release, so finalize immediately.
                self.try_finalize_close(conn_index);
            }
        } else {
            self.try_finalize_close(conn_index);
        }
    }

    /// Submit the deferred `Close` SQE once every pending send for
    /// this connection has either completed or been finally given up
    /// on. Called from `close_connection` (immediate-fast-path) and
    /// from `note_send_finalized` after each per-send CQE.
    pub(crate) fn try_finalize_close(&mut self, conn_index: u32) {
        let state = &self.send_queues[conn_index as usize];
        let sends_drained = !state.in_flight && state.queue.is_empty();
        // An in-flight Mode A forward write still owns a provided buffer (or Owned
        // copy) the kernel is reading as the write source; do not submit `Close`
        // (which recycles the slot) until its CQE has landed and released the
        // backing. `handle_forward_write` / `fail_forward_write` re-drive this
        // finalize once `forward_write[conn]` is cleared.
        let forward_drained = self.forward_write[conn_index as usize].is_none();
        if !(state.close_pending && sends_drained && forward_drained) {
            return;
        }
        self.send_queues[conn_index as usize].close_pending = false;
        // Disarm from the close_notify deadline set. swap_remove is
        // O(n) but the set is bounded by concurrent TLS shutdowns
        // (typically 0 or single digits) — well below the cost we just
        // saved by not walking every connection slot in the deadline
        // check.
        if let Some(pos) = self
            .close_notify_armed
            .iter()
            .position(|&i| i == conn_index)
        {
            self.close_notify_armed.swap_remove(pos);
        }
        // If a multishot recv is still armed on this connection, cancel it
        // before closing. Closing the fixed descriptor alone removes its
        // fixed-file table slot but does NOT drop the socket's last reference
        // while an in-flight recv SQE still pins it — so the kernel never sends
        // the peer a FIN and a parked reader on the other end hangs forever.
        // Cancelling the recv (by its `RecvMulti` user_data, which is immune to
        // Close reordering since it targets the request, not the fd) releases
        // that reference so the subsequent Close actually FINs. The recv's
        // ECANCELED completion is a no-op (generation/slot checks in
        // `handle_recv_multi`). Skipped when the recv already self-terminated
        // (e.g. a peer FIN drove this close) — nothing to cancel.
        let recv_armed = self
            .connections
            .get(conn_index)
            .is_some_and(|c| c.recv_multishot_armed);
        if recv_armed {
            let recv_ud = crate::completion::UserData::encode(
                crate::completion::OpTag::RecvMulti,
                conn_index,
                0,
            );
            let _ = self.ring.submit_async_cancel(recv_ud.raw(), conn_index);
            if let Some(cs) = self.connections.get_mut(conn_index) {
                cs.recv_multishot_armed = false;
            }
        }
        if self.ring.submit_close(conn_index).is_err() {
            crate::metrics::RING.increment(crate::metrics::ring::CLOSE_SUBMIT_FAILURES);
            // Queue this connection for retry on a later tick. (An earlier
            // version rebuilt the whole retry vec here — aging every other
            // entry toward discard and never enqueueing the connection whose
            // close just failed, leaking its fd and slot permanently.)
            self.pending_close_retries.push((conn_index, 0));
        }
    }

    /// Hook called from the send-path CQE handlers after the queue
    /// state has been updated — submits a deferred `Close` if the
    /// connection was waiting and the queue is now drained.
    pub(crate) fn note_send_finalized(&mut self, conn_index: u32) {
        self.try_finalize_close(conn_index);
    }

    /// Push all queued sends for a connection into the SQ. Returns the
    /// number of sends successfully submitted. On SQ full, drains the
    /// remaining queue (releasing slab/pool resources) and sets
    /// `in_flight = false`.
    pub(crate) fn flush_queued_sends_or_release(&mut self, conn_index: u32) -> usize {
        let state = &mut self.send_queues[conn_index as usize];
        let mut pushed = 0usize;
        while let Some(built) = state.queue.pop_front() {
            match unsafe { self.ring.push_sqe(built.entry) } {
                Ok(()) => {
                    pushed += 1;
                }
                Err(_) => {
                    // SQ full — release this entry and drain remaining queue.
                    Self::release_built_resources(
                        &mut self.send_slab,
                        &mut self.send_copy_pool,
                        built.pool_slot,
                        built.slab_idx,
                    );
                    Self::release_queued_sends(
                        &mut state.queue,
                        &mut self.send_slab,
                        &mut self.send_copy_pool,
                    );
                    state.in_flight = false;
                    break;
                }
            }
        }
        pushed
    }

    /// Submit a one-shot fallback recv into a pool slot for a connection
    /// parked on ENOBUFS with a partial message in its accumulator.
    ///
    /// Returns `true` if the SQE was submitted. The caller must have
    /// already established eligibility (plaintext accumulator path, no
    /// fallback in flight). On pool exhaustion or a full SQ the connection
    /// simply stays parked — the pre-fallback status quo — and is retried
    /// on a later flush.
    pub(crate) fn try_submit_fallback_recv(&mut self, conn_index: u32) -> bool {
        let chunk = self.fallback_chunk;
        let pool = self
            .fallback_recv_pool
            .get_or_insert_with(|| SendCopyPool::new(FALLBACK_RECV_SLOTS, chunk));
        if self.fallback_slot_owner.is_empty() {
            self.fallback_slot_owner = vec![(u32::MAX, u32::MAX); FALLBACK_RECV_SLOTS as usize];
        }
        let Some((slot, ptr, cap)) = pool.alloc_raw() else {
            return false;
        };
        if self
            .ring
            .submit_recv_fallback(conn_index, ptr, cap, slot)
            .is_err()
        {
            metrics::RING.increment(metrics::ring::SQE_SUBMIT_FAILURES);
            self.fallback_recv_pool
                .as_mut()
                .expect("pool constructed above")
                .release(slot);
            return false;
        }
        self.fallback_slot_owner[slot as usize] =
            (conn_index, self.connections.generation(conn_index));
        self.recv_fallback_inflight[conn_index as usize] = true;
        self.recv_fallback_count += 1;
        metrics::POOL.increment(metrics::pool::RECV_FALLBACK);
        true
    }

    /// Pop the next queued send for a connection and submit it to the ring.
    /// Returns true if a send was submitted, false if the queue was empty
    /// (in which case in_flight is set to false).
    pub(crate) fn submit_next_queued(&mut self, conn_index: u32) -> bool {
        use crate::buffer::send_slab::MAX_IOVECS;

        let ci = conn_index as usize;

        // Coalesce a run of consecutive plaintext copy sends (pool_slot set, no
        // ZC slab) at the front of the queue into a single `sendmsg`, so more
        // than one queued message is pipelined per CQE round-trip. Order is
        // preserved (one SQE; iovec order = FIFO queue order). ZC-guard sends
        // and recv-buffer forwards are not coalescable and fall through to the
        // single-submit path below.
        let coalescable =
            |b: &crate::handler::BuiltSend| b.pool_slot != u16::MAX && b.slab_idx == u16::MAX;
        // Coalesce at most one logical send's tail per op: stop the run after
        // the first chunk marked end-of-send. Otherwise a single coalesced
        // completion could span two independent pipelined sends, and only one
        // of their two waiters would ever be woken.
        let n = {
            let mut n = 0;
            while n < MAX_IOVECS {
                match self.send_queues[ci].queue.get(n) {
                    Some(b) if coalescable(b) => {
                        let pool_slot = b.pool_slot;
                        n += 1;
                        if self.send_copy_pool.is_end_of_send(pool_slot) {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            n
        };
        if n >= 2 {
            let mut pool_slots = [u16::MAX; MAX_IOVECS];
            {
                let q = &self.send_queues[ci].queue;
                for (i, slot) in pool_slots.iter_mut().enumerate().take(n) {
                    *slot = q[i].pool_slot;
                }
            }
            let mut iovecs = [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS];
            let mut total: u32 = 0;
            for i in 0..n {
                let (ptr, len) = self.send_copy_pool.current_ptr_remaining(pool_slots[i]);
                iovecs[i] = libc::iovec {
                    iov_base: ptr as *mut libc::c_void,
                    iov_len: len as usize,
                };
                total += len;
            }
            // The coalesced op carries the final chunk of a logical send only
            // if its last gathered chunk does; the completion handler wakes the
            // waiter on that.
            let end_of_send = self.send_copy_pool.is_end_of_send(pool_slots[n - 1]);
            // Only commit to coalescing if the slab has room; otherwise fall
            // through to single-submit (nothing popped yet).
            if let Some((slab_idx, msg_ptr)) = self.send_slab.allocate_coalesced(
                conn_index,
                &iovecs[..n],
                &pool_slots[..n],
                total,
                end_of_send,
            ) {
                for _ in 0..n {
                    self.send_queues[ci].queue.pop_front();
                }
                match self
                    .ring
                    .submit_send_msg_coalesced(conn_index, msg_ptr, slab_idx)
                {
                    Ok(()) => return true,
                    Err(_) => {
                        // SQ full — release the coalesced slab entry + its pool
                        // slots, drain the rest of the queue, clear in_flight.
                        for &s in &pool_slots[..n] {
                            self.send_copy_pool.release(s);
                        }
                        self.send_slab.release(slab_idx);
                        let state = &mut self.send_queues[ci];
                        Self::release_queued_sends(
                            &mut state.queue,
                            &mut self.send_slab,
                            &mut self.send_copy_pool,
                        );
                        state.in_flight = false;
                        state.shutdown_pending = false;
                        self.try_finalize_close(conn_index);
                        return false;
                    }
                }
            }
            // slab full → fall through to single-submit
        }

        let state = &mut self.send_queues[ci];
        match state.queue.pop_front() {
            Some(built) => {
                let pool_slot = built.pool_slot;
                let slab_idx = built.slab_idx;
                match unsafe { self.ring.push_sqe(built.entry) } {
                    Ok(()) => true,
                    Err(_) => {
                        // SQ full — release this entry and drain remaining queue.
                        Self::release_built_resources(
                            &mut self.send_slab,
                            &mut self.send_copy_pool,
                            pool_slot,
                            slab_idx,
                        );
                        Self::release_queued_sends(
                            &mut state.queue,
                            &mut self.send_slab,
                            &mut self.send_copy_pool,
                        );
                        state.in_flight = false;
                        state.shutdown_pending = false;
                        // If a deferred close was pending, fire it now that
                        // the queue is drained.
                        self.try_finalize_close(conn_index);
                        false
                    }
                }
            }
            None => {
                state.in_flight = false;
                // Submit deferred shutdown_write now that the send queue is drained.
                if state.shutdown_pending {
                    state.shutdown_pending = false;
                    let _ = self.ring.submit_shutdown(conn_index);
                }
                // Fire a deferred close now that nothing is in flight and the
                // queue is empty. The ZC and recv-forward completion paths
                // reach here without a note_send_finalized call, so without
                // this a close_pending connection would leak its fd and slot.
                self.try_finalize_close(conn_index);
                false
            }
        }
    }

    /// Submit a built send SQE or queue it if a send is already in-flight.
    ///
    /// This is the Driver-level equivalent of `DriverCtx::submit_or_queue`,
    /// used by zero-copy forward paths that bypass DriverCtx.
    pub(crate) fn submit_or_queue_send(
        &mut self,
        conn_index: u32,
        built: crate::handler::BuiltSend,
    ) -> io::Result<()> {
        let state = &mut self.send_queues[conn_index as usize];
        if state.in_flight {
            state.queue.push_back(built);
            Ok(())
        } else {
            // Destructure instead of cloning the 64-byte SQE: the fields are
            // only needed on the error branch.
            let crate::handler::BuiltSend {
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
                    // For SendRecvBuf, pool_slot and slab_idx are u16::MAX (no resources).
                    // The caller is responsible for replenishing the recv buffer bid on error.
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

    /// Queue a batch of built sends in order through the per-connection
    /// send queue. If one fails to submit (SQ full on the first while
    /// nothing is in flight — submit_or_queue_send releases that entry's
    /// own resources), the remaining entries' pool slots are released here
    /// so nothing leaks, and the error is returned.
    pub(crate) fn queue_built_sends(
        &mut self,
        conn_index: u32,
        sends: &mut Vec<crate::handler::BuiltSend>,
    ) -> io::Result<()> {
        let mut it = sends.drain(..);
        while let Some(built) = it.next() {
            if let Err(e) = self.submit_or_queue_send(conn_index, built) {
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

    /// Drain and release all queued sends for a connection.
    pub(crate) fn drain_conn_send_queue(&mut self, conn_index: u32) {
        let state = &mut self.send_queues[conn_index as usize];
        Self::release_queued_sends(
            &mut state.queue,
            &mut self.send_slab,
            &mut self.send_copy_pool,
        );
        state.in_flight = false;
        // Abandon any partially-accumulated logical send so the next one
        // starts from zero.
        state.acked_bytes = 0;
        // The queue is now empty and nothing is in flight — fire a deferred
        // close if one was pending so the connection can't leak.
        self.try_finalize_close(conn_index);
    }

    /// Release all entries from a send queue.
    pub(crate) fn release_queued_sends(
        queue: &mut VecDeque<BuiltSend>,
        send_slab: &mut InFlightSendSlab,
        send_copy_pool: &mut SendCopyPool,
    ) {
        for built in queue.drain(..) {
            Self::release_built_resources(
                send_slab,
                send_copy_pool,
                built.pool_slot,
                built.slab_idx,
            );
        }
    }

    /// Release pool slot and/or slab entry for a single BuiltSend.
    pub(crate) fn release_built_resources(
        send_slab: &mut InFlightSendSlab,
        send_copy_pool: &mut SendCopyPool,
        pool_slot: u16,
        slab_idx: u16,
    ) {
        if slab_idx != u16::MAX {
            let ps = send_slab.release(slab_idx);
            if ps != u16::MAX {
                send_copy_pool.release(ps);
            }
        } else if pool_slot != u16::MAX {
            send_copy_pool.release(pool_slot);
        }
    }

    /// Create a UDP socket, bind with SO_REUSEPORT, register in fixed file table.
    fn setup_udp_socket(
        ring: &Ring,
        bind_addr: SocketAddr,
        connect_peer: Option<SocketAddr>,
        fd_index: u32,
        send_slots: u16,
        udp_gro: bool,
    ) -> Result<UdpSocketState, crate::error::Error> {
        let domain = if bind_addr.is_ipv4() {
            libc::AF_INET
        } else {
            libc::AF_INET6
        };

        let fd = unsafe { libc::socket(domain, libc::SOCK_DGRAM | libc::SOCK_NONBLOCK, 0) };
        if fd < 0 {
            return Err(crate::error::Error::Io(std::io::Error::last_os_error()));
        }

        // Set SO_REUSEPORT for multi-worker binding.
        let optval: libc::c_int = 1;
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        // Enable UDP GRO. Opt-in, so a failure is hard rather than silent —
        // the caller has deliberately enlarged recv buffers expecting
        // coalescing, and quietly running without it would mislead.
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
                let err = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(crate::error::Error::Io(err));
            }
        }

        // Bind.
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let addr_len = crate::backend::socket_addr_to_sockaddr(bind_addr, &mut storage);
        let ret =
            unsafe { libc::bind(fd, &storage as *const _ as *const libc::sockaddr, addr_len) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            unsafe {
                libc::close(fd);
            }
            return Err(crate::error::Error::Io(err));
        }

        // If a peer was supplied, connect(2) the socket before registering
        // the fd. The kernel will filter incoming datagrams to this peer and
        // we can use the lighter Recv/Send opcodes for this socket.
        if let Some(peer) = connect_peer {
            let mut peer_storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            let peer_len = crate::backend::socket_addr_to_sockaddr(peer, &mut peer_storage);
            let ret = unsafe {
                libc::connect(
                    fd,
                    &peer_storage as *const _ as *const libc::sockaddr,
                    peer_len,
                )
            };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                unsafe {
                    libc::close(fd);
                }
                return Err(crate::error::Error::Io(err));
            }
        }

        // Register in the fixed file table, then close the original fd.
        ring.register_files_update(fd_index, &[fd])?;
        unsafe {
            libc::close(fd);
        }

        // Build the msghdr *template* for multishot recvmsg. The kernel only
        // reads `msg_namelen` / `msg_controllen` / `msg_iovlen` from this;
        // the actual name/control/payload land inside a buffer from the
        // provided buffer ring (laid out as `io_uring_recvmsg_out` + name +
        // control + payload). No iov is needed for multishot.
        let mut recv_msghdr: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
        recv_msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as u32;
        // When GRO is on, reserve control space so the kernel can attach the
        // UDP_GRO cmsg (segment size) inside each provided buffer. The kernel
        // reads this length off the template; `rearm_udp_recvmsg` only resets
        // `msg_namelen`, so this reservation survives re-arming. 0 keeps the
        // control region inert for non-GRO sockets.
        recv_msghdr.msg_controllen = if udp_gro {
            crate::backend::udp_gro::UDP_GRO_CMSG_LEN
        } else {
            0
        };
        recv_msghdr.msg_iov = std::ptr::null_mut();
        recv_msghdr.msg_iovlen = 0;

        // Allocate send slot ring. Each slot owns its own (addr, iov, msghdr)
        // so multiple sendmsg SQEs can be in-flight concurrently.
        let mut slots: Vec<UdpSendSlot> = Vec::with_capacity(send_slots as usize);
        for _ in 0..send_slots {
            let mut send_addr: Box<libc::sockaddr_storage> =
                Box::new(unsafe { std::mem::zeroed() });
            let mut send_iov = Box::new(libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            });
            let mut send_msghdr: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
            let mut send_cmsg_buf: Box<[u8; UDP_GSO_CMSG_LEN]> = Box::new([0u8; UDP_GSO_CMSG_LEN]);

            send_msghdr.msg_name = &mut *send_addr as *mut _ as *mut libc::c_void;
            send_msghdr.msg_iov = &mut *send_iov as *mut libc::iovec;
            send_msghdr.msg_iovlen = 1;
            // `msg_control` always points at our pinned cmsg buffer;
            // `msg_controllen = 0` for non-GSO sends keeps it inert.
            send_msghdr.msg_control = send_cmsg_buf.as_mut_ptr() as *mut libc::c_void;
            send_msghdr.msg_controllen = 0;

            slots.push(UdpSendSlot {
                send_addr,
                send_iov,
                send_msghdr,
                send_cmsg_buf,
            });
        }
        let send_slots_box = slots.into_boxed_slice();
        // Freelist is a LIFO stack of free slot indices (reverse order so slot 0 is popped first).
        let send_freelist: Vec<u16> = (0..send_slots).rev().collect();

        Ok(UdpSocketState {
            fd_index,
            local_addr: bind_addr,
            connected_peer: connect_peer,
            recv_msghdr,
            gro: udp_gro,
            send_slots: send_slots_box,
            send_freelist,
        })
    }

    /// Send a UDP datagram via the copy pool.
    ///
    /// Allocates one of the socket's pre-allocated send slots (bounded by
    /// `Config::udp_send_slots`) and submits a `sendmsg` SQE. Multiple sends
    /// can be in flight concurrently; returns `PoolExhausted` when no free
    /// send slot or copy-pool slot is available.
    ///
    /// When `gso_segment_size` is `Some(s)`, attaches a `UDP_SEGMENT`
    /// control message: the kernel splits `data` into back-to-back
    /// `s`-byte datagrams and emits each as a separate UDP packet from
    /// the *same* `sendmsg` call. This is Linux's GSO offload — one
    /// syscall, N datagrams. The pool slot still holds `data` end-to-
    /// end, so the per-datagram cost is just an iovec entry plus
    /// kernel-side segmentation work.
    pub(crate) fn udp_send_to(
        &mut self,
        udp_index: u32,
        peer: SocketAddr,
        data: &[u8],
        gso_segment_size: Option<u16>,
    ) -> Result<(), crate::error::UdpSendError> {
        let idx = udp_index as usize;
        if idx >= self.udp_sockets.len() {
            return Err(crate::error::UdpSendError::Io(std::io::Error::other(
                "invalid UDP socket index",
            )));
        }

        // Reject oversize datagrams up front with a non-retryable error so
        // callers don't burn cycles awaiting `send_ready` on data that will
        // never fit. `send_copy_pool::copy_in` would also fail here, but it
        // collapses size and exhaustion into a single `None` — losing the
        // distinction the API needs.
        let slot_size = self.send_copy_pool.slot_size() as usize;
        if data.len() > slot_size {
            return Err(crate::error::UdpSendError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "datagram size {} exceeds send_copy_slot_size {}",
                    data.len(),
                    slot_size
                ),
            )));
        }
        if let Some(seg) = gso_segment_size
            && (seg == 0 || (seg as usize) > data.len())
        {
            return Err(crate::error::UdpSendError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "GSO segment size {} invalid for {}-byte buffer",
                    seg,
                    data.len()
                ),
            )));
        }

        let slot_idx = self.udp_sockets[idx]
            .send_freelist
            .pop()
            .ok_or(crate::error::UdpSendError::PoolExhausted)?;

        let (pool_slot, ptr, len) = match self.send_copy_pool.copy_in(data) {
            Some(v) => v,
            None => {
                // Return the send slot to the freelist before reporting exhaustion.
                self.udp_sockets[idx].send_freelist.push(slot_idx);
                return Err(crate::error::UdpSendError::PoolExhausted);
            }
        };

        let fd_index = self.udp_sockets[idx].fd_index;

        // Fast path: if the socket is connect(2)ed to this peer and there is
        // no GSO segmenting, skip msghdr setup and submit the lighter `Send`
        // opcode. The kernel uses the socket's connected peer and the SQE
        // carries no sockaddr / iovec / cmsg. Saves a kernel msghdr
        // copy_from_user per send.
        let use_send_fast_path = gso_segment_size.is_none()
            && self.udp_sockets[idx]
                .connected_peer
                .is_some_and(|p| p == peer);

        if use_send_fast_path {
            let payload = encode_udp_send_payload(slot_idx, pool_slot);
            let ud = UserData::encode(OpTag::SendUdp, udp_index, payload);
            return match self.ring.submit_send_udp(fd_index, ptr, len, ud) {
                Ok(()) => {
                    crate::metrics::UDP.increment(crate::metrics::udp::DATAGRAMS_SENT);
                    Ok(())
                }
                Err(_) => {
                    self.send_copy_pool.release(pool_slot);
                    self.udp_sockets[idx].send_freelist.push(slot_idx);
                    Err(crate::error::UdpSendError::SubmissionQueueFull)
                }
            };
        }

        let slot = &mut self.udp_sockets[idx].send_slots[slot_idx as usize];
        let addr_len = crate::backend::socket_addr_to_sockaddr(peer, &mut slot.send_addr);
        slot.send_iov.iov_base = ptr as *mut libc::c_void;
        slot.send_iov.iov_len = len as usize;
        slot.send_msghdr.msg_namelen = addr_len;

        // Wire up (or tear down) the `UDP_SEGMENT` control message in
        // the pinned per-slot cmsg buffer. We do not touch
        // `slot.send_cmsg_buf` for non-GSO sends — `msg_controllen = 0`
        // tells the kernel to ignore it.
        if let Some(seg_size) = gso_segment_size {
            let cmsg_total = unsafe { libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) };
            debug_assert!(
                (cmsg_total as usize) <= UDP_GSO_CMSG_LEN,
                "UDP_GSO_CMSG_LEN too small for cmsg_space"
            );
            let buf = slot.send_cmsg_buf.as_mut_ptr();
            // SAFETY: `slot.send_cmsg_buf` is pinned in the slot
            // (Box-allocated, lives until the slot is dropped). We
            // populate exactly the bytes the kernel will read.
            unsafe {
                std::ptr::write_bytes(buf, 0, UDP_GSO_CMSG_LEN);
                slot.send_msghdr.msg_control = buf as *mut libc::c_void;
                slot.send_msghdr.msg_controllen = cmsg_total as _;
                let cmsg = libc::CMSG_FIRSTHDR(&*slot.send_msghdr);
                (*cmsg).cmsg_level = libc::IPPROTO_UDP;
                (*cmsg).cmsg_type = libc::UDP_SEGMENT;
                (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<u16>() as u32) as _;
                let data_ptr = libc::CMSG_DATA(cmsg) as *mut u16;
                std::ptr::write_unaligned(data_ptr, seg_size);
            }
        } else {
            slot.send_msghdr.msg_controllen = 0;
        }

        let msghdr_ptr = &*slot.send_msghdr as *const libc::msghdr;
        let payload = encode_udp_send_payload(slot_idx, pool_slot);
        let ud = UserData::encode(OpTag::SendMsgUdp, udp_index, payload);

        match self.ring.submit_sendmsg(fd_index, msghdr_ptr, ud) {
            Ok(()) => {
                crate::metrics::UDP.increment(crate::metrics::udp::DATAGRAMS_SENT);
                Ok(())
            }
            Err(_) => {
                self.send_copy_pool.release(pool_slot);
                self.udp_sockets[idx].send_freelist.push(slot_idx);
                Err(crate::error::UdpSendError::SubmissionQueueFull)
            }
        }
    }

    /// Re-arm the UDP multishot recvmsg for a socket. Called when the kernel
    /// tears the multishot down (e.g. on `ENOBUFS` after the buffer ring
    /// emptied) — the CQE handler pushes the hint here once replenishment
    /// has refilled the ring.
    pub(crate) fn rearm_udp_recvmsg(&mut self, udp_index: u32, bgid: u16) {
        let idx = udp_index as usize;
        if idx >= self.udp_sockets.len() {
            return;
        }
        // Reset msg_namelen in case the kernel cleared it during teardown.
        self.udp_sockets[idx].recv_msghdr.msg_namelen =
            std::mem::size_of::<libc::sockaddr_storage>() as u32;
        let fd_index = self.udp_sockets[idx].fd_index;
        let result = if self.udp_sockets[idx].connected_peer.is_some() {
            let ud = UserData::encode(OpTag::RecvUdp, udp_index, 0);
            self.ring.submit_multishot_recv_udp(fd_index, bgid, ud)
        } else {
            let ud = UserData::encode(OpTag::RecvMsgUdp, udp_index, 0);
            let msghdr_ptr = &*self.udp_sockets[idx].recv_msghdr as *const libc::msghdr;
            self.ring
                .submit_recvmsg_multishot(fd_index, msghdr_ptr, bgid, ud)
        };
        if result.is_err() {
            crate::metrics::RING.increment(crate::metrics::ring::SQE_SUBMIT_FAILURES);
        }
    }

    /// Shutdown: close all connections, drain remaining CQEs, close eventfd.
    pub(crate) fn run_shutdown(&mut self) {
        // 1. Close all active connections and drain their send queues.
        let max = self.connections.max_slots();
        for i in 0..max {
            if self.connections.get(i).is_some() {
                self.drain_conn_send_queue(i);
                // Best effort: kernel cleans up fds on thread/process exit.
                let _ = self.ring.submit_close(i);
            }
        }

        // 2. Submit + drain loop until all connections are closed.
        //    Arm a timeout SQE each iteration so submit_and_wait(1) never blocks
        //    indefinitely (the tick timeout from the main loop is not armed here).
        let shutdown_ts = io_uring::types::Timespec::new().nsec(100_000_000); // 100ms
        for _ in 0..100 {
            if self.connections.active_count() == 0 && !self.send_slab.has_in_flight() {
                break;
            }
            let ud = UserData::encode(OpTag::TickTimeout, 0, 0);
            // Best effort: the 100-iteration bound prevents infinite block.
            let _ = self.ring.submit_tick_timeout(&shutdown_ts, ud.raw());
            if self.ring.submit_and_wait(1).is_err() {
                break;
            }

            self.cqe_batch.clear();
            {
                let cq = self.ring.ring.completion();
                for cqe in cq {
                    self.cqe_batch
                        .push((cqe.user_data(), cqe.result(), cqe.flags()));
                }
            }

            for i in 0..self.cqe_batch.len() {
                let (user_data_raw, result, flags) = self.cqe_batch[i];
                let ud = UserData(user_data_raw);
                let tag = match ud.tag() {
                    Some(t) => t,
                    None => continue,
                };

                match tag {
                    OpTag::Send => {
                        let pool_slot = ud.payload() as u16;
                        self.send_copy_pool.release(pool_slot);
                    }
                    OpTag::SendMsgZc => {
                        let slab_idx = ud.payload() as u16;
                        if !self.send_slab.in_use(slab_idx) {
                            continue;
                        }
                        if cqueue::notif(flags) {
                            self.send_slab.dec_pending_notifs(slab_idx);
                            if self.send_slab.should_release(slab_idx) {
                                let pool_slot = self.send_slab.release(slab_idx);
                                if pool_slot != u16::MAX {
                                    self.send_copy_pool.release(pool_slot);
                                }
                            }
                        } else {
                            // Only expect a ZC notification when result > 0.
                            // On error/zero, no notification arrives — incrementing
                            // would permanently leak the slab entry.
                            if result > 0 {
                                self.send_slab.inc_pending_notifs(slab_idx);
                            }
                            self.send_slab.mark_awaiting_notifications(slab_idx);
                            if self.send_slab.should_release(slab_idx) {
                                let pool_slot = self.send_slab.release(slab_idx);
                                if pool_slot != u16::MAX {
                                    self.send_copy_pool.release(pool_slot);
                                }
                            }
                        }
                    }
                    OpTag::Close => {
                        let conn_index = ud.conn_index();
                        if let Some(ref mut tls_table) = self.tls_table {
                            tls_table.remove(conn_index);
                        }
                        self.connections.release(conn_index);
                    }
                    OpTag::TlsSend => {
                        let pool_slot = ud.payload() as u16;
                        self.send_copy_pool.release(pool_slot);
                    }
                    _ => {}
                }
            }
        }

        // 3. Unregister the provided buffer rings before Driver is dropped
        // (which munmaps the ring memory). Without this, the kernel holds a
        // dangling pointer to the freed mmap region.
        if self
            .ring
            .unregister_buf_ring(self.provided_bufs.bgid())
            .is_err()
        {
            crate::metrics::RING.increment(crate::metrics::ring::SQE_SUBMIT_FAILURES);
        }
        if let Some(ref udp_bufs) = self.udp_provided_bufs
            && self.ring.unregister_buf_ring(udp_bufs.bgid()).is_err()
        {
            crate::metrics::RING.increment(crate::metrics::ring::SQE_SUBMIT_FAILURES);
        }

        // The eventfd is owned by `ShutdownHandle::Drop` (it holds the
        // matching `WakeHandle`), so don't close it here — the handle's
        // wake clones live longer than the worker thread, and a
        // double-close would race against fd-number reuse.
    }
}

#[cfg(test)]
mod field_order_tests {
    //! Drop-order proof for the kernel/DMA fields of `Driver`.
    //!
    //! Rust drops struct fields in *declaration* order, so `ring` (declared
    //! first) drops last. We can't directly observe `Driver` dropping (it
    //! owns a real `io_uring`), but we can verify the property in a mirror
    //! struct with the same field ordering and `Drop`-instrumented stand-ins.
    use std::cell::RefCell;
    use std::sync::Mutex;

    static DROP_ORDER: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

    struct DropMarker(&'static str);
    impl Drop for DropMarker {
        fn drop(&mut self) {
            DROP_ORDER.lock().unwrap().push(self.0);
        }
    }

    /// Mirrors the head of `Driver`'s field declaration order. If a future
    /// edit moves `ring` out of first position, the order recorded here
    /// will diverge from `["send_slab", "provided_bufs", "ring"]` and the
    /// test will fail loudly.
    #[allow(dead_code)]
    struct DriverFieldOrderMirror {
        ring: DropMarker,
        // `connections`, `fixed_buffers` etc. interleave here in the real
        // struct; only the kernel-DMA-sensitive ones matter for this test.
        provided_bufs: DropMarker,
        send_slab: DropMarker,
    }

    #[test]
    fn ring_drops_after_buffer_pools() {
        // Reset.
        DROP_ORDER.lock().unwrap().clear();

        // Sanity: if someone re-orders the mirror to put ring last, this
        // test will catch it.
        let _ = RefCell::new(()); // silence stray-lifetime lints
        {
            let _m = DriverFieldOrderMirror {
                ring: DropMarker("ring"),
                provided_bufs: DropMarker("provided_bufs"),
                send_slab: DropMarker("send_slab"),
            };
        } // drop runs here

        let order = DROP_ORDER.lock().unwrap().clone();
        assert_eq!(
            order,
            vec!["ring", "provided_bufs", "send_slab"],
            "DriverFieldOrderMirror dropped in the wrong order — \
             reorder the fields to match `Driver`'s declaration so \
             `ring` is declared *first* and therefore drops *last*",
        );

        // The real assertion: in the *real* Driver, `ring` must be
        // declared before the DMA-sensitive fields.
        let src = include_str!("driver.rs");
        let ring_pos = src
            .find("pub(crate) ring: Ring,")
            .expect("Driver::ring field signature not found — was it renamed?");
        let provided_pos = src
            .find("pub(crate) provided_bufs: ProvidedBufRing,")
            .expect("Driver::provided_bufs field signature not found");
        let send_slab_pos = src
            .find("pub(crate) send_slab: InFlightSendSlab,")
            .expect("Driver::send_slab field signature not found");
        assert!(
            ring_pos < provided_pos,
            "Driver::ring must be declared before Driver::provided_bufs \
             so it drops *after* it (kernel DMA reference safety)",
        );
        assert!(
            ring_pos < send_slab_pos,
            "Driver::ring must be declared before Driver::send_slab \
             so it drops *after* it (ZC user-memory guards safety)",
        );
    }
}
