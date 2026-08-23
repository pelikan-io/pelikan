use std::net::SocketAddr;

use crate::buffer::fixed::MemoryRegion;

/// TLS configuration. Pass a pre-built rustls ServerConfig.
#[derive(Clone)]
pub struct TlsConfig {
    /// Pre-built rustls ServerConfig. User loads certs/keys and configures ALPN etc.
    pub(crate) server_config: std::sync::Arc<rustls::ServerConfig>,
}

impl TlsConfig {
    /// Wrap a pre-built rustls `ServerConfig` (configure certs/ALPN/etc. on the rustls side).
    pub fn new(server_config: std::sync::Arc<rustls::ServerConfig>) -> Self {
        Self { server_config }
    }
}

/// TLS client configuration for outbound connections.
#[derive(Clone)]
pub struct TlsClientConfig {
    /// Pre-built rustls ClientConfig. User configures root certs, ALPN, etc.
    pub(crate) client_config: std::sync::Arc<rustls::ClientConfig>,
}

impl TlsClientConfig {
    /// Wrap a pre-built rustls `ClientConfig`.
    pub fn new(client_config: std::sync::Arc<rustls::ClientConfig>) -> Self {
        Self { client_config }
    }
}

/// Configuration for the io_uring driver.
#[derive(Clone)]
pub struct Config {
    /// Number of SQ entries. CQ will be 4x this.
    pub(crate) sq_entries: u32,
    /// Enable SQPOLL mode (kernel-side submission polling).
    pub(crate) sqpoll: bool,
    /// SQPOLL idle timeout in milliseconds.
    pub(crate) sqpoll_idle_ms: u32,
    /// Pin SQPOLL kernel thread to this CPU core. Only meaningful when sqpoll=true.
    pub(crate) sqpoll_cpu: Option<u32>,
    /// Recv buffer configuration (provided buffer ring) for TCP multishot recv.
    pub(crate) recv_buffer: RecvBufferConfig,
    /// Recv buffer configuration for UDP multishot recvmsg.
    ///
    /// UDP uses a separate provided buffer ring from TCP so the two can be
    /// sized independently. Each buffer must fit an `io_uring_recvmsg_out`
    /// header (16 bytes) + `sockaddr_storage` (128 bytes) + the datagram
    /// payload. Default: 128 buffers × 2048 bytes (room for a standard-MTU
    /// datagram plus the multishot metadata); bump `buffer_size` if you
    /// expect jumbo datagrams.
    ///
    /// `bgid` must differ from `recv_buffer.bgid` when UDP is in use.
    pub(crate) udp_recv_buffer: RecvBufferConfig,
    /// User-registered memory regions (e.g., mmap'd storage arenas).
    ///
    /// Regions listed here occupy slots `0..registered_regions.len()` at
    /// startup. The remaining slots up to [`ConfigBuilder::max_registered_regions`] are
    /// available for dynamic registration via
    /// [`ShutdownHandle::register_region`](crate::ShutdownHandle::register_region).
    pub(crate) registered_regions: Vec<MemoryRegion>,
    /// Maximum number of fixed-buffer slots to reserve in the io_uring
    /// registered-buffer table. Must be `>= registered_regions.len()`.
    ///
    /// Slots beyond the initial regions are empty until filled by
    /// [`ShutdownHandle::register_region`](crate::ShutdownHandle::register_region).
    /// Cannot be grown after launch — io_uring's registered-buffer table is
    /// fixed-size; expand by re-launching with a larger value.
    ///
    /// Default: 64.
    pub(crate) max_registered_regions: u16,
    /// Worker/thread configuration.
    pub(crate) worker: WorkerConfig,
    /// TCP listen backlog.
    pub(crate) backlog: i32,
    /// Maximum number of direct file descriptors (connections).
    pub(crate) max_connections: u32,
    /// Initial capacity for per-connection recv accumulators.
    pub(crate) recv_accumulator_capacity: usize,
    /// Upper bound on a single per-connection recv accumulator. If the
    /// application's parser keeps returning `NeedMore` while the peer
    /// streams data, the accumulator grows indefinitely; setting this
    /// closes the connection once the cap is exceeded, protecting the
    /// worker from OOM.
    ///
    /// **Default: `usize::MAX` (disabled).** A bounded value should be set
    /// for any server that accepts data from untrusted peers. Sensible
    /// values are 4–16× the typical request size; setting it too low
    /// will close legitimate slow-consumer workloads (where kernel recv
    /// CQEs batch faster than the handler runs).
    pub(crate) recv_accumulator_max: usize,
    /// Aggregate low-water reserve for segmented recv (see
    /// `docs/segmented-recv-design.md`, "Backpressure and ring safety").
    ///
    /// When a `Segmented` connection receives a provided buffer, the runtime
    /// consults the shared per-worker recv ring's live free-buffer count. If
    /// `free() <= recv_segment_reserve`, the buffer is **force-copied** into an
    /// owned `Bytes` and its bid returned to the ring immediately (Mode C at
    /// delivery) instead of being pinned as a zero-copy held segment. This
    /// guarantees a well-behaved connection is never `ENOBUFS`-starved by other
    /// connections holding segments under fan-in, at the cost of a copy while the
    /// ring is under pressure. Above the reserve, delivery stays zero-copy
    /// (pinned).
    ///
    /// Larger values reserve more headroom (copy sooner, safer under fan-in);
    /// `0` force-copies only when the ring is fully drained. Tune relative to
    /// `recv_buffer` ring_size; a value `>=` ring_size makes segmented delivery
    /// always copy. Must be `<= 65535` (the maximum provided-ring size).
    ///
    /// **Default: 64** (a quarter of the default 256-buffer recv ring).
    pub(crate) recv_segment_reserve: u32,
    /// Per-connection held-buffer cap for a Mode A `forward_to` connection (see
    /// `docs/segmented-recv-design.md`, "Mode A — Forward to an fd").
    ///
    /// A `forward_to` connection holds arriving provided buffers in-place until
    /// each is written to the sink. A slow or high-latency sink (or a very large
    /// object) would otherwise let one forwarding connection accumulate an
    /// unbounded backlog of held buffers, pinning much of the shared per-worker
    /// recv ring (and growing heap when the low-water reserve force-copies) and
    /// starving every other connection on the worker. When a forwarding
    /// connection's held-buffer count reaches this cap, the runtime cancels its
    /// multishot recv so its TCP receive window closes and the peer stops sending
    /// (natural backpressure to the source); the recv is re-armed once the hold
    /// drains below the cap as writes complete. This bounds one slow forward to at
    /// most `forward_hold_cap` held buffers.
    ///
    /// Larger values allow more recv in flight (higher single-forward throughput)
    /// at the cost of more pinned ring buffers / held heap under a slow sink;
    /// smaller values apply backpressure sooner. Must be `>= 1`.
    ///
    /// **Default: 64** (2× `MAX_IOVECS`, the per-`sendmsg` iovec bound).
    pub(crate) forward_hold_cap: usize,
    /// Bound on the per-worker accept channel. If a worker can't drain its
    /// queue fast enough, the acceptor will skip past it (and possibly
    /// close the incoming fd if every worker is full) rather than
    /// accumulating fds without backpressure. Default: 1024.
    pub(crate) accept_queue_capacity: usize,
    /// Number of connections assigned to each worker before moving to the
    /// next one. `1` (the default) gives classic round-robin. Higher values
    /// pack connections onto fewer workers at low connection counts, keeping
    /// each active worker's CQE density high enough for io_uring batching to
    /// pay off. Has no effect once total connections exceed
    /// `conn_chunk_size * num_workers` — at that point every worker is
    /// active and each gets the same number of connections as round-robin.
    ///
    /// Rule of thumb: set to the minimum connections-per-worker at which
    /// your workload sees good batching (typically 16–64). Default: 1.
    pub(crate) conn_chunk_size: usize,
    /// Number of copy-send pool slots. Each in-flight `send()` or copy part of a
    /// `send_parts()` call holds one slot until the kernel completes the send.
    /// Size this to cover your peak in-flight send count — exhaustion returns an
    /// error to the handler. Memory cost: `send_copy_count * send_copy_slot_size`.
    pub(crate) send_copy_count: u16,
    /// Size of each copy-send pool slot in bytes. A single `send()` or the
    /// combined copy parts of one `send_parts()` call must fit in one slot.
    pub(crate) send_copy_slot_size: u32,
    /// Minimum total send size (bytes) for the zero-copy guard send path.
    ///
    /// Guard sends (`send_parts()` with `.guard()` parts) whose total length is
    /// **less than** this threshold are gathered into a `SendCopyPool` slot and
    /// submitted as a plain copy `Send` instead of `SendMsgZc`. For small sends
    /// the ZC bookkeeping (in-flight slab entry plus a second completion for the
    /// kernel's ZC notification) costs more than the memcpy it avoids.
    ///
    /// `0` disables the fallback (guard sends always use zero-copy).
    /// Sends at or above the threshold, or that don't fit a send pool slot,
    /// use the zero-copy path as before. Default: `4096`.
    pub(crate) send_zc_threshold: u32,
    /// Number of InFlightSendSlab slots for in-flight scatter-gather sends
    /// (i.e., `send_parts()` calls that include at least one guard).
    /// Each slot is held until all ZC notifications arrive.
    pub(crate) send_slab_slots: u16,
    /// Deadline-based flush interval in microseconds during CQE processing.
    /// When non-SQPOLL, if this many microseconds elapse since the last submit
    /// while processing a CQE batch, pending SQEs are flushed mid-iteration.
    /// 0 = disabled. Ignored when SQPOLL is active (kernel handles it).
    pub(crate) flush_interval_us: u64,
    /// Maximum time in microseconds that `submit_and_wait` will block before
    /// returning to call `on_tick`. Prevents the event loop from stalling when
    /// there are no pending completions (e.g., client-only mode between phases).
    /// 0 = no timeout (block indefinitely until a CQE arrives).
    /// Default: 1000 (1ms).
    pub(crate) tick_timeout_us: u64,
    /// Optional TLS configuration. When set, all accepted connections use TLS.
    pub(crate) tls: Option<TlsConfig>,
    /// Optional TLS client configuration for outbound `connect_tls()` calls.
    pub(crate) tls_client: Option<TlsClientConfig>,
    /// Enable TCP_NODELAY on all connections (accepted and outbound).
    pub(crate) tcp_nodelay: bool,
    /// Enable SO_TIMESTAMPING for kernel-level receive timestamps.
    /// When enabled, connections use `RecvMsgMulti` instead of `RecvMulti`
    /// to receive ancillary data containing kernel RX timestamps.
    #[cfg(feature = "timestamps")]
    pub(crate) timestamps: bool,
    /// Maximum number of SQEs per IOSQE_IO_LINK chain. 0 disables chaining.
    /// When disabled, sends exceeding MAX_IOVECS fall back to sequential
    /// round-trips (one SQE at a time via on_send_complete).
    /// Default: 16.
    pub(crate) max_chain_length: u16,
    /// Maximum number of standalone async tasks (not bound to connections)
    /// per worker. Used with [`spawn()`](crate::spawn).
    /// Default: 256.
    pub(crate) standalone_task_capacity: u32,
    /// Maximum number of concurrent timer slots per worker.
    /// Used by [`sleep()`](crate::sleep) and [`timeout()`](crate::timeout).
    /// Default: 256.
    pub(crate) timer_slots: u32,
    /// UDP bind addresses. Each worker creates its own socket with SO_REUSEPORT.
    /// Empty = no UDP sockets.
    pub(crate) udp_bind: Vec<SocketAddr>,
    /// Optional peer to `connect(2)` each UDP socket to, parallel to
    /// `udp_bind`. `None` leaves the socket unconnected (the usual UDP
    /// server case); `Some(peer)` calls `connect()` so the kernel filters
    /// incoming datagrams to that peer and the runtime can use the lighter
    /// `RecvUdp`/`SendUdp` opcodes instead of `RecvMsgUdp`/`SendMsgUdp`.
    /// Saves ~4 microseconds per round trip on single-shot client workloads.
    /// Must have the same length as `udp_bind` (enforced at validation).
    pub(crate) udp_connect_peers: Vec<Option<SocketAddr>>,
    /// Number of concurrent in-flight UDP sends per socket. Each slot owns a
    /// pre-allocated `sockaddr_storage` + `iovec` + `msghdr` triple used to
    /// submit a `sendmsg` SQE; the slot is returned to the freelist on CQE.
    /// Exhaustion returns [`crate::error::UdpSendError::PoolExhausted`].
    /// Default: 64.
    pub(crate) udp_send_slots: u16,
    /// Maximum number of datagrams buffered per UDP socket awaiting a
    /// consumer. The runtime fills this queue from `recvmsg` completions;
    /// the application's `on_udp_bind` future drains it via
    /// [`UdpCtx::recv_from`](crate::UdpCtx::recv_from). When the queue is
    /// full, additional datagrams are dropped and
    /// `udp::DATAGRAMS_DROPPED` is incremented — this guards against
    /// unbounded memory growth when the handler future stalls, panics,
    /// or returns early.
    ///
    /// Default: 1024.
    pub(crate) udp_recv_queue_capacity: usize,
    /// Enable UDP Generic Receive Offload (GRO) on bound UDP sockets.
    ///
    /// When set, the runtime calls `setsockopt(SOL_UDP, UDP_GRO)` so the
    /// kernel coalesces consecutive same-flow datagrams into one `recvmsg`
    /// delivery, carrying the per-segment size in a control message. The
    /// runtime splits the coalesced payload back into individual datagrams
    /// transparently, so [`UdpCtx::recv_batch`](crate::UdpCtx::recv_batch) /
    /// [`recv_batch_timed`](crate::UdpCtx::recv_batch_timed) callbacks still
    /// fire once per datagram. This cuts per-datagram syscall / wake overhead
    /// dramatically for high-pps flows (e.g. QUIC at large payloads).
    ///
    /// A coalesced datagram can be up to ~64 KiB, and on io_uring the
    /// recvmsg header + sockaddr + control + payload share one provided
    /// buffer, so enabling GRO requires `udp_recv_buffer.buffer_size` to be
    /// large enough to hold a full coalesced datagram (validated at startup);
    /// otherwise the kernel truncates and the datagram is dropped. Has no
    /// effect on `connect(2)`-ed UDP sockets (they use the lighter `recv`
    /// path, which carries no control message). Linux-only — a no-op on
    /// other platforms. Default: false.
    pub(crate) udp_gro: bool,
    /// Optional NVMe passthrough configuration. When set, enables NVMe device
    /// management and `IORING_OP_URING_CMD` submission for direct NVMe I/O.
    pub(crate) nvme: Option<crate::nvme::NvmeConfig>,
    /// Optional direct I/O configuration. When set, enables `O_DIRECT` file I/O
    /// via io_uring `IORING_OP_READ` / `IORING_OP_WRITE`, bypassing the page cache.
    pub(crate) direct_io: Option<crate::direct_io::DirectIoConfig>,
    /// Optional buffered filesystem I/O configuration. When set, enables async
    /// file open/read/write/stat/rename/unlink/mkdir via io_uring.
    pub(crate) fs: Option<crate::fs::FsConfig>,
    /// Number of dedicated DNS resolver threads. The resolver pool runs
    /// `getaddrinfo` on background threads, keeping blocking DNS isolated
    /// from the io_uring event loop.
    ///
    /// 0 = disabled (no resolver pool; [`resolve()`](crate::resolve) will
    /// return an error). Default: 2.
    pub(crate) resolver_threads: usize,
    /// Number of dedicated process spawner threads. The spawner pool runs
    /// `posix_spawnp` + `pidfd_open` on background threads, keeping blocking
    /// process creation isolated from the io_uring event loop.
    ///
    /// 0 = disabled (no spawner pool; [`Command::spawn()`](crate::process::Command::spawn)
    /// will return an error). Default: 1.
    pub(crate) spawner_threads: usize,
    /// Number of dedicated blocking threads. The blocking pool runs
    /// user-provided closures on low-priority (`SCHED_IDLE`) background threads,
    /// keeping CPU-bound or blocking work isolated from the io_uring event loop.
    ///
    /// 0 = disabled (no blocking pool; [`spawn_blocking()`](crate::spawn_blocking)
    /// will return an error). Default: 4.
    pub(crate) blocking_threads: usize,
    /// Number of dedicated disk I/O threads (mio backend only). The disk I/O
    /// pool executes blocking filesystem syscalls (pread, pwrite, fsync, stat,
    /// rename, unlink, mkdir) on background threads, enabling async file I/O
    /// on non-Linux platforms.
    ///
    /// 0 = disabled (filesystem/direct I/O operations return `Unsupported`).
    /// Default: 2.
    pub(crate) disk_io_threads: usize,
    /// Maximum time in milliseconds to wait for a TLS close_notify send to
    /// complete before force-closing the connection. When a TLS connection
    /// sends close_notify but the send never completes (e.g., due to pool
    /// exhaustion), this prevents the connection from hanging indefinitely.
    /// Default: 5000.
    pub(crate) close_notify_timeout_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sq_entries: 256,
            sqpoll: false,
            sqpoll_idle_ms: 1000,
            sqpoll_cpu: None,
            recv_buffer: RecvBufferConfig::default(),
            udp_recv_buffer: RecvBufferConfig {
                ring_size: 128,
                buffer_size: 2048,
                bgid: 1,
            },
            registered_regions: Vec::new(),
            max_registered_regions: 64,
            worker: WorkerConfig::default(),
            backlog: 1024,
            max_connections: 16000,
            recv_accumulator_capacity: 4096,
            recv_accumulator_max: usize::MAX,
            recv_segment_reserve: 64,
            forward_hold_cap: 64,
            accept_queue_capacity: 1024,
            conn_chunk_size: 1,
            send_copy_count: 1024,
            send_copy_slot_size: 16384,
            send_zc_threshold: 4096,
            send_slab_slots: 512,
            flush_interval_us: 100,
            tick_timeout_us: 1000,
            tls: None,
            tls_client: None,
            tcp_nodelay: true,
            #[cfg(feature = "timestamps")]
            timestamps: false,
            max_chain_length: 16,
            standalone_task_capacity: 256,
            timer_slots: 256,
            udp_bind: Vec::new(),
            udp_connect_peers: Vec::new(),
            udp_send_slots: 64,
            udp_recv_queue_capacity: 1024,
            udp_gro: false,
            nvme: None,
            direct_io: None,
            fs: Some(crate::fs::FsConfig::default()),
            resolver_threads: 2,
            spawner_threads: 1,
            blocking_threads: 4,
            disk_io_threads: 2,
            close_notify_timeout_ms: 5000,
        }
    }
}

impl Config {
    /// The zero-copy guard send threshold in bytes. See
    /// [`ConfigBuilder::send_zc_threshold`].
    pub fn send_zc_threshold(&self) -> u32 {
        self.send_zc_threshold
    }

    /// Validate configuration values. Returns an error if any value is out of range.
    pub fn validate(&self) -> Result<(), crate::error::Error> {
        if !self.recv_buffer.ring_size.is_power_of_two() {
            return Err(crate::error::Error::RingSetup(
                "recv_buffer.ring_size must be a power of two".into(),
            ));
        }
        if self.recv_buffer.buffer_size == 0 {
            return Err(crate::error::Error::RingSetup(
                "recv_buffer.buffer_size must be > 0".into(),
            ));
        }
        if self.max_connections == 0 || self.max_connections >= (1 << 24) {
            return Err(crate::error::Error::RingSetup(
                "max_connections must be > 0 and < 2^24".into(),
            ));
        }
        // A zero-capacity crossbeam channel is a rendezvous channel: try_send
        // only succeeds while a receiver is blocked in recv(), and workers
        // only ever try_recv — so every accept would fail.
        if self.accept_queue_capacity == 0 {
            return Err(crate::error::Error::RingSetup(
                "accept_queue_capacity must be > 0".into(),
            ));
        }
        if self.timer_slots == 0 || self.timer_slots > 65535 {
            return Err(crate::error::Error::RingSetup(
                "timer_slots must be > 0 and <= 65535".into(),
            ));
        }
        if self.send_slab_slots == 0 {
            return Err(crate::error::Error::RingSetup(
                "send_slab_slots must be > 0".into(),
            ));
        }
        if self.send_copy_slot_size == 0 {
            return Err(crate::error::Error::RingSetup(
                "send_copy_slot_size must be > 0".into(),
            ));
        }
        if self.send_copy_count == 0 {
            return Err(crate::error::Error::RingSetup(
                "send_copy_count must be > 0".into(),
            ));
        }
        // The provided ring can hold at most u16::MAX buffers, so a reserve above
        // that is nonsensical (it would force-copy every segmented delivery). A
        // reserve >= the configured ring_size is *allowed* (it simply makes
        // segmented delivery always copy) — it is not coupled here so small rings
        // stay valid.
        if self.recv_segment_reserve > u16::MAX as u32 {
            return Err(crate::error::Error::RingSetup(
                "recv_segment_reserve must be <= 65535 (the maximum provided-ring size)".into(),
            ));
        }
        // A cap of 0 would throttle a forward before it can hold its first
        // buffer, deadlocking the forward (nothing to write, never re-armed).
        if self.forward_hold_cap == 0 {
            return Err(crate::error::Error::RingSetup(
                "forward_hold_cap must be >= 1".into(),
            ));
        }
        if self.sq_entries == 0 || !self.sq_entries.is_power_of_two() {
            return Err(crate::error::Error::RingSetup(
                "sq_entries must be > 0 and a power of two".into(),
            ));
        }
        if self.standalone_task_capacity >= (1 << 31) {
            return Err(crate::error::Error::RingSetup(
                "standalone_task_capacity must be < 2^31".into(),
            ));
        }
        if self.registered_regions.len() > self.max_registered_regions as usize {
            return Err(crate::error::Error::RingSetup(format!(
                "registered_regions ({}) exceed max_registered_regions ({})",
                self.registered_regions.len(),
                self.max_registered_regions,
            )));
        }
        if !self.udp_bind.is_empty() && self.udp_send_slots == 0 {
            return Err(crate::error::Error::RingSetup(
                "udp_send_slots must be > 0 when udp_bind is non-empty".into(),
            ));
        }
        if !self.udp_connect_peers.is_empty() && self.udp_connect_peers.len() != self.udp_bind.len()
        {
            return Err(crate::error::Error::RingSetup(
                "udp_connect_peers length must match udp_bind length".into(),
            ));
        }
        if !self.udp_bind.is_empty() && self.udp_recv_queue_capacity == 0 {
            return Err(crate::error::Error::RingSetup(
                "udp_recv_queue_capacity must be > 0 when udp_bind is non-empty".into(),
            ));
        }
        if !self.udp_bind.is_empty() {
            if !self.udp_recv_buffer.ring_size.is_power_of_two() {
                return Err(crate::error::Error::RingSetup(
                    "udp_recv_buffer.ring_size must be a power of two".into(),
                ));
            }
            // Ceiling is 256 KiB so UDP GRO (which coalesces up to ~64 KiB of
            // payload plus the recvmsg header / sockaddr / control region into
            // one provided buffer) can be sized to fit. Non-GRO callers
            // typically stay near a single MTU.
            if self.udp_recv_buffer.buffer_size == 0 || self.udp_recv_buffer.buffer_size > (1 << 18)
            {
                return Err(crate::error::Error::RingSetup(
                    "udp_recv_buffer.buffer_size must be > 0 and <= 262144".into(),
                ));
            }
            // Each datagram occupies one buffer that also holds the
            // io_uring_recvmsg_out header (16 bytes) + sockaddr_storage
            // (128 bytes). Below that floor the kernel can't fit even a
            // zero-byte datagram plus the metadata.
            if self.udp_recv_buffer.buffer_size < 160 {
                return Err(crate::error::Error::RingSetup(
                    "udp_recv_buffer.buffer_size must be >= 160 to hold recvmsg header + sockaddr"
                        .into(),
                ));
            }
            // GRO coalesces up to ~64 KiB into a single delivery; the buffer
            // must hold that plus the recvmsg header (16) + sockaddr (128) +
            // control region, or the kernel truncates and the datagram is
            // dropped silently. Require headroom past 64 KiB.
            if self.udp_gro && self.udp_recv_buffer.buffer_size < (1 << 16) + 512 {
                return Err(crate::error::Error::RingSetup(
                    "udp_recv_buffer.buffer_size must be >= 66048 when udp_gro is enabled \
                     (a coalesced GRO datagram is up to ~64 KiB plus recvmsg metadata)"
                        .into(),
                ));
            }
            if self.udp_recv_buffer.bgid == self.recv_buffer.bgid {
                return Err(crate::error::Error::RingSetup(
                    "udp_recv_buffer.bgid must differ from recv_buffer.bgid".into(),
                ));
            }
        }
        if self.close_notify_timeout_ms == 0 || self.close_notify_timeout_ms > 60000 {
            return Err(crate::error::Error::RingSetup(
                "close_notify_timeout_ms must be > 0 and <= 60000".into(),
            ));
        }
        Ok(())
    }
}

/// Configuration for the provided buffer ring (multishot recv).
#[derive(Clone)]
pub(crate) struct RecvBufferConfig {
    /// Number of buffers in the ring (must be power of 2).
    pub ring_size: u16,
    /// Size of each buffer in bytes.
    pub buffer_size: u32,
    /// Buffer group ID for the provided buffer ring.
    pub bgid: u16,
}

impl Default for RecvBufferConfig {
    fn default() -> Self {
        Self {
            ring_size: 256,
            buffer_size: 16384,
            bgid: 0,
        }
    }
}

/// Configuration for the thread-per-core worker model.
#[derive(Clone)]
pub(crate) struct WorkerConfig {
    /// Number of worker threads.
    ///
    /// `0` (the default) auto-detects and uses the number of **physical CPU
    /// cores** — not logical CPUs. On SMT/hyperthreaded hardware this is half
    /// the value returned by `nproc` or `available_parallelism()`. Ringline's
    /// io_uring event loops are CPU-bound; two hyperthreads on the same
    /// physical core share execution units and caches, so spawning one worker
    /// per logical CPU induces contention without additional throughput.
    ///
    /// Set explicitly to override (e.g. `threads = 1` for a single-threaded
    /// server, or a larger value when you have many connections and the
    /// per-worker CPU budget is low).
    pub threads: usize,
    /// Whether to pin each worker to a CPU core.
    pub pin_to_core: bool,
    /// Starting CPU core index for pinning.
    pub core_offset: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            threads: 0,
            pin_to_core: true,
            core_offset: 0,
        }
    }
}

/// Builder for [`Config`] with discoverable methods and `build()` validation.
///
/// # Example
///
/// ```rust
/// use ringline::ConfigBuilder;
///
/// let config = ConfigBuilder::default()
///     .workers(4)
///     .max_connections(8000)
///     .sq_entries(512)
///     .tcp_nodelay(true)
///     .recv_buffer(256, 4096)
///     .send_pool(512, 8192)
///     .timer_slots(1024)
///     .build()
///     .expect("invalid config");
/// ```
#[derive(Default)]
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    /// Create a new builder with default config values.
    pub fn new() -> Self {
        Self::default()
    }

    // ── Worker settings ──────────────────────────────────────────────

    /// Set the number of worker threads. 0 = number of CPUs.
    pub fn workers(mut self, n: usize) -> Self {
        self.config.worker.threads = n;
        self
    }

    /// Enable or disable CPU core pinning.
    pub fn pin_to_core(mut self, enable: bool) -> Self {
        self.config.worker.pin_to_core = enable;
        self
    }

    /// Set the starting CPU core index for pinning.
    ///
    /// When `core_offset + worker_threads` fits within the machine's
    /// physical core count, this indexes *physical* cores and each
    /// worker is pinned to a distinct physical core's first SMT sibling
    /// (so hyperthread-adjacent CPU enumerations don't stack two workers
    /// on one core). Larger offsets are treated as raw logical CPU ids,
    /// which keeps deliberate hyperthread layouts expressible.
    pub fn core_offset(mut self, offset: usize) -> Self {
        self.config.worker.core_offset = offset;
        self
    }

    // ── Connection settings ──────────────────────────────────────────

    /// Set the maximum number of direct file descriptors (connections).
    pub fn max_connections(mut self, n: u32) -> Self {
        self.config.max_connections = n;
        self
    }

    /// Set the TCP listen backlog.
    pub fn backlog(mut self, n: i32) -> Self {
        self.config.backlog = n;
        self
    }

    /// Set the number of connections assigned to each worker before moving to
    /// the next one. `1` (the default) gives classic round-robin.
    pub fn conn_chunk_size(mut self, n: usize) -> Self {
        self.config.conn_chunk_size = n;
        self
    }

    /// Set the bound on the per-worker accept channel. Default: 1024.
    pub fn accept_queue_capacity(mut self, n: usize) -> Self {
        self.config.accept_queue_capacity = n;
        self
    }

    /// Enable or disable TCP_NODELAY on all connections.
    pub fn tcp_nodelay(mut self, enable: bool) -> Self {
        self.config.tcp_nodelay = enable;
        self
    }

    // ── io_uring settings ────────────────────────────────────────────

    /// Set the number of SQ entries. CQ will be 4x this. Must be a power of 2.
    pub fn sq_entries(mut self, n: u32) -> Self {
        self.config.sq_entries = n;
        self
    }

    /// Enable SQPOLL mode (kernel-side submission polling).
    pub fn sqpoll(mut self, enable: bool) -> Self {
        self.config.sqpoll = enable;
        self
    }

    /// Set SQPOLL idle timeout in milliseconds (default: 1000).
    pub fn sqpoll_idle_ms(mut self, ms: u32) -> Self {
        self.config.sqpoll_idle_ms = ms;
        self
    }

    /// Pin SQPOLL kernel thread to a specific CPU core.
    pub fn sqpoll_cpu(mut self, cpu: u32) -> Self {
        self.config.sqpoll_cpu = Some(cpu);
        self
    }

    // ── Buffer settings ──────────────────────────────────────────────

    /// Set recv buffer configuration.
    pub fn recv_buffer(mut self, ring_size: u16, buffer_size: u32) -> Self {
        self.config.recv_buffer.ring_size = ring_size;
        self.config.recv_buffer.buffer_size = buffer_size;
        self
    }

    /// Set the recv buffer group ID (bgid) for the TCP provided buffer ring.
    pub fn recv_buffer_bgid(mut self, bgid: u16) -> Self {
        self.config.recv_buffer.bgid = bgid;
        self
    }

    /// Set the initial capacity for per-connection recv accumulators.
    pub fn recv_accumulator_capacity(mut self, n: usize) -> Self {
        self.config.recv_accumulator_capacity = n;
        self
    }

    /// Set the upper bound on a single per-connection recv accumulator.
    /// `usize::MAX` (the default) disables the cap.
    pub fn recv_accumulator_max(mut self, n: usize) -> Self {
        self.config.recv_accumulator_max = n;
        self
    }

    /// Set the aggregate low-water reserve for segmented recv.
    ///
    /// When the shared per-worker recv ring's free-buffer count drops to this
    /// value or below, arriving buffers for `Segmented` connections are
    /// force-copied into owned `Bytes` (and their bids returned to the ring
    /// immediately) instead of being pinned as zero-copy held segments — so
    /// connections holding segments cannot deplete the ring and `ENOBUFS`-starve
    /// well-behaved connections under fan-in. Above the reserve, delivery stays
    /// zero-copy. `0` force-copies only when the ring is fully drained. Tune
    /// relative to the `recv_buffer` ring size; must be `<= 65535`.
    ///
    /// Default: 64 (a quarter of the default 256-buffer recv ring).
    pub fn recv_segment_reserve(mut self, reserve: u32) -> Self {
        self.config.recv_segment_reserve = reserve;
        self
    }

    /// Set the per-connection held-buffer cap for Mode A `forward_to`
    /// connections.
    ///
    /// When a forwarding connection has this many provided buffers held awaiting
    /// writes to the sink, the runtime cancels its multishot recv (closing its
    /// TCP receive window so the source stops sending) and re-arms it once the
    /// hold drains below the cap as writes complete — bounding one slow forward
    /// to `forward_hold_cap` held buffers so it cannot deplete the shared
    /// per-worker recv ring and starve other connections. Larger values allow
    /// more recv in flight (higher single-forward throughput) at the cost of more
    /// pinned ring buffers / held heap under a slow sink. Must be `>= 1`.
    ///
    /// Default: 64 (2× `MAX_IOVECS`).
    pub fn forward_hold_cap(mut self, cap: usize) -> Self {
        self.config.forward_hold_cap = cap;
        self
    }

    /// Set the number and size of copy-send pool slots.
    pub fn send_pool(mut self, count: u16, slot_size: u32) -> Self {
        self.config.send_copy_count = count;
        self.config.send_copy_slot_size = slot_size;
        self
    }

    /// Set the zero-copy guard send threshold in bytes (0 = always zero-copy).
    pub fn send_zc_threshold(mut self, bytes: u32) -> Self {
        self.config.send_zc_threshold = bytes;
        self
    }

    /// Set the number of scatter-gather send slab slots.
    pub fn send_slab_slots(mut self, n: u16) -> Self {
        self.config.send_slab_slots = n;
        self
    }

    // ── Task/timer settings ──────────────────────────────────────────

    /// Set the maximum number of standalone async tasks per worker.
    pub fn standalone_task_capacity(mut self, n: u32) -> Self {
        self.config.standalone_task_capacity = n;
        self
    }

    /// Set the maximum number of concurrent timer slots per worker.
    pub fn timer_slots(mut self, n: u32) -> Self {
        self.config.timer_slots = n;
        self
    }

    // ── Timing settings ──────────────────────────────────────────────

    /// Set the tick timeout in microseconds. 0 = block indefinitely.
    pub fn tick_timeout_us(mut self, us: u64) -> Self {
        self.config.tick_timeout_us = us;
        self
    }

    /// Set the deadline-based flush interval in microseconds. 0 = disabled.
    pub fn flush_interval_us(mut self, us: u64) -> Self {
        self.config.flush_interval_us = us;
        self
    }

    // ── Timestamp settings ────────────────────────────────────────────

    /// Enable SO_TIMESTAMPING for kernel-level receive timestamps.
    #[cfg(feature = "timestamps")]
    pub fn timestamps(mut self, enable: bool) -> Self {
        self.config.timestamps = enable;
        self
    }

    // ── Chain settings ───────────────────────────────────────────────

    /// Set the maximum number of SQEs per IO_LINK chain. 0 disables chaining.
    pub fn max_chain_length(mut self, n: u16) -> Self {
        self.config.max_chain_length = n;
        self
    }

    // ── UDP settings ─────────────────────────────────────────────────

    /// Add a UDP bind address. Can be called multiple times.
    pub fn udp_bind(mut self, addr: SocketAddr) -> Self {
        self.config.udp_bind.push(addr);
        self.config.udp_connect_peers.push(None);
        self
    }

    /// Add a UDP bind address that is then `connect(2)`ed to `peer`. The
    /// kernel filters incoming datagrams to `peer` and the runtime can use
    /// the lighter `RecvUdp`/`SendUdp` opcodes instead of the
    /// `RecvMsgUdp`/`SendMsgUdp` pair. Saves ~4 microseconds per round trip
    /// on single-shot client workloads.
    pub fn udp_bind_connected(mut self, local: SocketAddr, peer: SocketAddr) -> Self {
        self.config.udp_bind.push(local);
        self.config.udp_connect_peers.push(Some(peer));
        self
    }

    /// Set the number of concurrent in-flight UDP sends per socket.
    pub fn udp_send_slots(mut self, n: u16) -> Self {
        self.config.udp_send_slots = n;
        self
    }

    /// Set the UDP recv buffer configuration (ring_size, buffer_size).
    /// The `bgid` is left at its default; override it with
    /// [`udp_recv_buffer_bgid`](Self::udp_recv_buffer_bgid).
    pub fn udp_recv_buffer(mut self, ring_size: u16, buffer_size: u32) -> Self {
        self.config.udp_recv_buffer.ring_size = ring_size;
        self.config.udp_recv_buffer.buffer_size = buffer_size;
        self
    }

    /// Set the UDP recv buffer group ID (bgid). Must differ from the TCP
    /// `recv_buffer` bgid when UDP is in use.
    pub fn udp_recv_buffer_bgid(mut self, bgid: u16) -> Self {
        self.config.udp_recv_buffer.bgid = bgid;
        self
    }

    /// Set the per-UDP-socket recv queue capacity.
    ///
    /// Datagrams that arrive while the queue is full are dropped and
    /// counted in `udp::DATAGRAMS_DROPPED`. Default: 1024.
    ///
    /// On the io_uring backend each queued datagram pins one provided
    /// ring buffer until it is consumed, so the effective capacity is
    /// clamped to `udp_recv_buffer` ring_size — raise both knobs
    /// together for deeper queues. The mio backend copies datagrams into
    /// owned buffers and honors the full configured depth.
    pub fn udp_recv_queue_capacity(mut self, capacity: usize) -> Self {
        self.config.udp_recv_queue_capacity = capacity;
        self
    }

    /// Enable UDP GRO on bound UDP sockets. Remember
    /// to size `udp_recv_buffer` to hold a full coalesced datagram (~64 KiB).
    pub fn udp_gro(mut self, enabled: bool) -> Self {
        self.config.udp_gro = enabled;
        self
    }

    // ── Optional subsystems ──────────────────────────────────────────

    /// Set NVMe passthrough configuration.
    pub fn nvme(mut self, config: crate::nvme::NvmeConfig) -> Self {
        self.config.nvme = Some(config);
        self
    }

    /// Set direct I/O configuration.
    pub fn direct_io(mut self, config: crate::direct_io::DirectIoConfig) -> Self {
        self.config.direct_io = Some(config);
        self
    }

    /// Set filesystem I/O configuration.
    pub fn fs(mut self, config: crate::fs::FsConfig) -> Self {
        self.config.fs = Some(config);
        self
    }

    /// Disable filesystem I/O support entirely.
    ///
    /// `fs` defaults to enabled (unlike `nvme`/`direct_io`), so its
    /// per-worker file table and command slab are otherwise always
    /// allocated. Workloads that never touch [`crate::fs`] can reclaim
    /// that footprint; fs operations will then fail with "filesystem I/O
    /// not configured".
    pub fn no_fs(mut self) -> Self {
        self.config.fs = None;
        self
    }

    /// Set the number of DNS resolver threads. 0 = disabled.
    pub fn resolver_threads(mut self, threads: usize) -> Self {
        self.config.resolver_threads = threads;
        self
    }

    /// Set the number of process spawner threads. 0 = disabled.
    pub fn spawner_threads(mut self, threads: usize) -> Self {
        self.config.spawner_threads = threads;
        self
    }

    /// Set the number of blocking threads. 0 = disabled.
    pub fn blocking_threads(mut self, threads: usize) -> Self {
        self.config.blocking_threads = threads;
        self
    }

    /// Set the number of disk I/O threads (mio backend only). 0 = disabled.
    pub fn disk_io_threads(mut self, threads: usize) -> Self {
        self.config.disk_io_threads = threads;
        self
    }

    /// Set the TLS close_notify timeout in milliseconds. Default: 5000.
    pub fn close_notify_timeout_ms(mut self, ms: u64) -> Self {
        self.config.close_notify_timeout_ms = ms;
        self
    }

    /// Set TLS server configuration.
    pub fn tls(mut self, config: TlsConfig) -> Self {
        self.config.tls = Some(config);
        self
    }

    /// Set TLS client configuration for outbound connections.
    pub fn tls_client(mut self, config: TlsClientConfig) -> Self {
        self.config.tls_client = Some(config);
        self
    }

    // ── Memory regions ───────────────────────────────────────────────

    /// Set the user-registered memory regions occupying the initial
    /// registered-buffer slots.
    pub fn registered_regions(mut self, regions: Vec<MemoryRegion>) -> Self {
        self.config.registered_regions = regions;
        self
    }

    /// Set the maximum number of fixed-buffer slots to reserve. Must be
    /// `>= registered_regions.len()`. Default: 64.
    pub fn max_registered_regions(mut self, n: u16) -> Self {
        self.config.max_registered_regions = n;
        self
    }

    // ── Terminal ─────────────────────────────────────────────────────

    /// Validate and build the final [`Config`].
    pub fn build(self) -> Result<Config, crate::error::Error> {
        self.config.validate()?;
        Ok(self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a Config with a single field override.
    /// Avoids clippy::field_reassign_with_default on the multi-field Config struct.
    fn config_with(f: impl FnOnce(&mut Config)) -> Config {
        let mut c = Config::default();
        f(&mut c);
        c
    }

    #[test]
    fn validate_default_config_passes() {
        Config::default()
            .validate()
            .expect("default config should be valid");
    }

    #[test]
    fn validate_timer_slots_zero_rejected() {
        assert!(config_with(|c| c.timer_slots = 0).validate().is_err());
    }

    #[test]
    fn validate_send_slab_slots_zero_rejected() {
        assert!(config_with(|c| c.send_slab_slots = 0).validate().is_err());
    }

    #[test]
    fn validate_accept_queue_capacity_zero_rejected() {
        // bounded(0) is a rendezvous channel; workers only try_recv, so
        // every accept would fail with the queue reported as full.
        assert!(
            config_with(|c| c.accept_queue_capacity = 0)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn validate_buffer_size_zero_rejected() {
        assert!(
            config_with(|c| c.recv_buffer.buffer_size = 0)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn validate_buffer_size_accepted_65535() {
        assert!(
            config_with(|c| c.recv_buffer.buffer_size = 65535)
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validate_buffer_size_accepted_65536() {
        // 65536 is the first value that previously caused a crash; it must now be
        // accepted (the remaining-bytes counter is stored in the driver, not in the
        // 16-bit CQE payload).
        assert!(
            config_with(|c| c.recv_buffer.buffer_size = 65536)
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validate_buffer_size_accepted_large() {
        assert!(
            config_with(|c| c.recv_buffer.buffer_size = 131072)
                .validate()
                .is_ok()
        );
    }

    fn udp_config_with(f: impl FnOnce(&mut Config)) -> Config {
        config_with(|c| {
            c.udp_bind = vec!["127.0.0.1:0".parse().unwrap()];
            f(c);
        })
    }

    #[test]
    fn validate_udp_gro_requires_large_buffer() {
        // Default udp buffer (2048) is far too small for a coalesced datagram.
        let err = udp_config_with(|c| c.udp_gro = true)
            .validate()
            .unwrap_err();
        assert!(format!("{err}").contains("udp_gro"));
    }

    #[test]
    fn validate_udp_gro_accepts_large_buffer() {
        udp_config_with(|c| {
            c.udp_gro = true;
            c.udp_recv_buffer.buffer_size = 1 << 17;
        })
        .validate()
        .expect("udp_gro with a 128 KiB buffer should be valid");
    }

    #[test]
    fn validate_udp_buffer_ceiling_raised() {
        // The ceiling moved from 64 KiB to 256 KiB to accommodate GRO.
        udp_config_with(|c| c.udp_recv_buffer.buffer_size = 1 << 18)
            .validate()
            .expect("256 KiB udp buffer should be accepted");
        assert!(
            udp_config_with(|c| c.udp_recv_buffer.buffer_size = (1 << 18) + 1)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn validate_ring_size_not_power_of_two_rejected() {
        assert!(
            config_with(|c| c.recv_buffer.ring_size = 3)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn validate_sq_entries_not_power_of_two_rejected() {
        assert!(config_with(|c| c.sq_entries = 3).validate().is_err());
    }

    #[test]
    fn validate_sq_entries_zero_rejected() {
        assert!(config_with(|c| c.sq_entries = 0).validate().is_err());
    }

    #[test]
    fn validate_max_connections_zero_rejected() {
        assert!(config_with(|c| c.max_connections = 0).validate().is_err());
    }

    #[test]
    fn validate_max_connections_too_large_rejected() {
        assert!(
            config_with(|c| c.max_connections = 1 << 24)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn validate_timer_slots_too_large_rejected() {
        assert!(config_with(|c| c.timer_slots = 65536).validate().is_err());
    }

    #[test]
    fn validate_send_copy_slot_size_zero_rejected() {
        assert!(
            config_with(|c| c.send_copy_slot_size = 0)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn validate_send_copy_count_zero_rejected() {
        assert!(config_with(|c| c.send_copy_count = 0).validate().is_err());
    }

    #[test]
    fn validate_standalone_task_capacity_too_large_rejected() {
        assert!(
            config_with(|c| c.standalone_task_capacity = 1 << 31)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn send_zc_threshold_default() {
        assert_eq!(Config::default().send_zc_threshold, 4096);
    }

    #[test]
    fn send_zc_threshold_builder_zero() {
        let cfg = ConfigBuilder::default()
            .send_zc_threshold(0)
            .build()
            .expect("zero send_zc_threshold should be valid");
        assert_eq!(cfg.send_zc_threshold, 0);
    }
}
