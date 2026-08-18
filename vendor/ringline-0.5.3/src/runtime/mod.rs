//! Async runtime for ringline: task executor, waker, and I/O primitives.
//!
//! # Portability boundary
//!
//! This module is designed so that the core async machinery is portable
//! across I/O backends (io_uring, mio/epoll, kqueue):
//!
//! - **Portable** (no io_uring dependency):
//!   - `task` — `TaskSlab`, `TaskSlot` (slab of per-connection futures)
//!   - `waker` — `conn_waker()`, thread-local `READY_QUEUE`
//!   - `mod.rs` — `Executor`, `IoResult` (waiter flags, ready queue, result storage)
//!   - `handler` — `AsyncEventHandler` trait (references `DriverCtx` by borrowed ref only)
//!
//! - **Backend-specific** (tied to the concrete `Driver` type):
//!   - `io` — `ConnCtx`, futures (`WithDataFuture`, `SendFuture`, `ConnectFuture`)
//!     accesses `Driver` via thread-local pointer
//!
//! A mio backend would provide an alternative `driver.rs`, `async_event_loop.rs`,
//! and `runtime/io.rs` while reusing everything else unchanged.

pub(crate) mod cancellation;
pub mod channel;
pub(crate) mod handler;
pub(crate) mod io;
pub(crate) mod join;
pub(crate) mod select;
pub(crate) mod stream;
pub(crate) mod task;
pub(crate) mod waker;

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::io as stdio;

use self::task::{StandaloneTaskSlab, TaskSlab};
use self::waker::drain_ready_queue;

/// I/O result stored per-connection for async task wakeup.
pub(crate) enum IoResult {
    /// Send completed with total bytes or error.
    Send(stdio::Result<u32>),
    /// Connect completed with success or error.
    Connect(stdio::Result<()>),
}

/// A recv sink that allows CQE data to be written directly to a target buffer,
/// bypassing the per-connection accumulator.
///
/// # Safety
///
/// The pointer is set by the async task before yielding and cleared after wakeup.
/// ringline is single-threaded per worker — CQE processing and task polling never
/// interleave — so the raw pointer is safe to dereference during CQE processing.
pub(crate) struct RecvSink {
    pub(crate) ptr: *mut u8,
    pub(crate) cap: usize,
    pub(crate) pos: usize,
}

/// A queued UDP datagram waiting for an async consumer.
///
/// On the io_uring backend the buffer is borrowed from the kernel-provided buffer
/// ring (`Kernel` variant) and the consumer must push the `bid` to the driver's
/// `udp_pending_replenish` after reading the data so the buffer goes back to the
/// kernel. On the mio backend the buffer is owned (`Owned` variant) and the
/// `Vec<u8>` is dropped normally.
pub(crate) struct PendingUdpDatagram {
    pub(crate) peer: std::net::SocketAddr,
    pub(crate) buf: PendingUdpBuf,
    /// Wall-clock instant the driver first observed this datagram in
    /// the CQE drain (io_uring) or `recv_from` poll (mio). Used by
    /// `recv_batch_timed` callers (typically protocol drivers like
    /// `quinn-proto`) so RTT measurements don't include the executor
    /// wake + task poll latency that elapses between arrival and the
    /// callback firing.
    pub(crate) recv_at: std::time::Instant,
    /// UDP GRO segment size. When non-zero, `buf` holds several datagrams
    /// coalesced by the kernel and the drain path splits it into chunks of
    /// this size (the last chunk may be shorter), invoking the recv callback
    /// once per chunk. Zero means a single, un-coalesced datagram.
    pub(crate) segment_size: u32,
    /// Bytes already handed out by segment-wise consumers
    /// (`recv_from`/`with_datagram` on a GRO-coalesced entry). Zero for
    /// whole-entry consumers (`recv_batch`, non-GRO paths).
    pub(crate) consumed: u32,
}

pub(crate) enum PendingUdpBuf {
    /// User-owned buffer (mio backend or any path that needed to copy).
    /// Only constructed by the mio backend; on io_uring this variant is dead
    /// code (the kernel-buffer path is always used).
    #[cfg_attr(has_io_uring, allow(dead_code))]
    Owned(Vec<u8>),
    /// Reference into a kernel-provided buffer (io_uring backend, zero-copy).
    ///
    /// The pointer is into `ProvidedBufRing::buf_backing`, which is allocated
    /// once and not resized, so it remains valid until `bid` is replenished.
    /// `payload_len` is the datagram payload length (excludes recvmsg header).
    #[cfg(has_io_uring)]
    Kernel {
        bid: u16,
        ptr: *const u8,
        payload_len: u32,
    },
}

impl PendingUdpDatagram {
    /// Borrow the payload bytes. Valid until the buffer is released.
    // On mio-only builds the enum has a single variant, so these matches are
    // infallible. clippy complains; cfg-gating the lint avoids contorting
    // the code with `if let` chains that only exist to please mio builds.
    #[cfg_attr(not(has_io_uring), allow(clippy::infallible_destructuring_match))]
    pub(crate) fn data(&self) -> &[u8] {
        match &self.buf {
            PendingUdpBuf::Owned(v) => v.as_slice(),
            #[cfg(has_io_uring)]
            PendingUdpBuf::Kernel {
                ptr, payload_len, ..
            } => unsafe { std::slice::from_raw_parts(*ptr, *payload_len as usize) },
        }
    }

    /// Invoke `f` once per contained datagram: once for an un-coalesced
    /// entry, or once per `segment_size` chunk (last chunk possibly shorter)
    /// for a GRO-coalesced entry. Centralises the split so both drain loops
    /// share it and neither risks a `chunks(0)` panic.
    pub(crate) fn for_each_segment<F: FnMut(&[u8])>(&self, mut f: F) {
        let data = self.data();
        if self.segment_size == 0 {
            f(data);
        } else {
            for chunk in data.chunks(self.segment_size as usize) {
                f(chunk);
            }
        }
    }

    /// Byte range of the next un-consumed datagram: the whole payload for
    /// an un-coalesced entry, one `segment_size` chunk for a GRO entry.
    pub(crate) fn next_segment_range(&self) -> (usize, usize) {
        let len = self.data().len();
        if self.segment_size == 0 {
            (0, len)
        } else {
            let start = (self.consumed as usize).min(len);
            let end = (start + self.segment_size as usize).min(len);
            (start, end)
        }
    }

    /// Whether all contained datagrams have been handed out.
    pub(crate) fn exhausted(&self) -> bool {
        self.segment_size == 0 || self.consumed as usize >= self.data().len()
    }

    /// If this entry holds a kernel-provided buffer, return the bid so the
    /// caller can push it to `udp_pending_replenish`. Returns `None` for
    /// owned-buffer entries (mio).
    #[cfg_attr(not(has_io_uring), allow(clippy::match_single_binding))]
    pub(crate) fn bid_to_release(&self) -> Option<u16> {
        match &self.buf {
            PendingUdpBuf::Owned(_) => None,
            #[cfg(has_io_uring)]
            PendingUdpBuf::Kernel { bid, .. } => Some(*bid),
        }
    }

    /// Consume the entry as an owned `(Vec<u8>, SocketAddr)` pair. Allocates a
    /// fresh `Vec` for the kernel-buffer variant; the caller must still push
    /// `bid_to_release()` to `udp_pending_replenish` to free the kernel
    /// buffer.
    #[cfg_attr(not(has_io_uring), allow(clippy::infallible_destructuring_match))]
    pub(crate) fn into_owned(self) -> (Vec<u8>, std::net::SocketAddr) {
        let peer = self.peer;
        let v = match self.buf {
            PendingUdpBuf::Owned(v) => v,
            #[cfg(has_io_uring)]
            PendingUdpBuf::Kernel {
                ptr, payload_len, ..
            } => unsafe { std::slice::from_raw_parts(ptr, payload_len as usize).to_vec() },
        };
        (v, peer)
    }
}

thread_local! {
    /// The current task ID being polled. Set by the executor before each poll.
    /// Connection tasks: conn_index (bits 0..23).
    /// Standalone tasks: task_idx | STANDALONE_BIT.
    /// Used by SleepFuture to know which task to wake on timer completion.
    pub(crate) static CURRENT_TASK_ID: Cell<u32> = const { Cell::new(0) };
}

/// Pool of timer slots backed by stable memory for the I/O backend.
///
/// Each `sleep()` call allocates a slot and metadata. Generation counters
/// prevent stale completions from waking the wrong task after slot reuse.
pub(crate) struct TimerSlotPool {
    /// Timespec values — must remain at stable addresses for io_uring.
    #[cfg(has_io_uring)]
    pub(crate) timespecs: Vec<io_uring::types::Timespec>,
    /// Deadline instants for the mio backend timer expiry.
    #[cfg(not(has_io_uring))]
    pub(crate) deadlines: Vec<Option<std::time::Instant>>,
    /// Min-heap of (deadline, slot, generation) for O(log n) expiry checks
    /// on the mio backend (replaces full-pool scans per loop iteration).
    /// Entries are lazily invalidated: a popped/peeked entry only counts if
    /// the slot's generation and exact deadline still match.
    #[cfg(not(has_io_uring))]
    expiry_heap: std::collections::BinaryHeap<std::cmp::Reverse<(std::time::Instant, u32, u16)>>,
    /// Which task (with STANDALONE_BIT encoding) to wake when this timer fires.
    pub(crate) waker_ids: Vec<u32>,
    /// Whether the CQE/event has arrived for this timer.
    pub(crate) fired: Vec<bool>,
    /// Generation counter per slot to prevent stale event races.
    pub(crate) generations: Vec<u16>,
    /// Free slot indices for O(1) allocation.
    free_list: Vec<u32>,
}

impl TimerSlotPool {
    /// Create a new pool with the given capacity.
    pub(crate) fn new(capacity: u32) -> Self {
        let cap = capacity as usize;
        let mut free_list = Vec::with_capacity(cap);
        for i in 0..capacity {
            free_list.push(i);
        }
        TimerSlotPool {
            #[cfg(has_io_uring)]
            timespecs: vec![io_uring::types::Timespec::new(); cap],
            #[cfg(not(has_io_uring))]
            deadlines: vec![None; cap],
            #[cfg(not(has_io_uring))]
            expiry_heap: std::collections::BinaryHeap::new(),
            waker_ids: vec![0; cap],
            fired: vec![false; cap],
            generations: vec![0; cap],
            free_list,
        }
    }

    /// Allocate a timer slot. Returns `(slot_index, generation)` or None if full.
    pub(crate) fn allocate(&mut self, waker_id: u32) -> Option<(u32, u16)> {
        let slot = self.free_list.pop()?;
        let idx = slot as usize;
        self.waker_ids[idx] = waker_id;
        self.fired[idx] = false;
        let generation = self.generations[idx];
        Some((slot, generation))
    }

    /// Release a timer slot back to the free list.
    pub(crate) fn release(&mut self, slot: u32) {
        let idx = slot as usize;
        if idx < self.generations.len() {
            self.generations[idx] = self.generations[idx].wrapping_add(1);
            // Clear the mio deadline: the expiry scan reads the slot's
            // *current* generation, so a stale deadline on a free slot would
            // pass fire()'s generation check, spuriously wake the old
            // waker_id, and keep shortening the poll timeout until it passes.
            #[cfg(not(has_io_uring))]
            {
                self.deadlines[idx] = None;
            }
            self.free_list.push(slot);
        }
    }

    /// Mark a timer as fired. Returns the waker_id if generation matches.
    pub(crate) fn fire(&mut self, slot: u32, generation: u16) -> Option<u32> {
        let idx = slot as usize;
        if idx >= self.generations.len() || self.generations[idx] != generation {
            return None; // stale CQE
        }
        self.fired[idx] = true;
        Some(self.waker_ids[idx])
    }

    /// Total number of slots in the pool.
    pub(crate) fn capacity(&self) -> usize {
        self.generations.len()
    }

    /// Check if a timer slot has fired.
    pub(crate) fn is_fired(&self, slot: u32) -> bool {
        self.fired.get(slot as usize).copied().unwrap_or(false)
    }

    /// Encode `(slot_index, generation)` into a 32-bit payload for UserData.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub(crate) fn encode_payload(slot: u32, generation: u16) -> u32 {
        (slot & 0xFFFF) | ((generation as u32) << 16)
    }

    /// Decode payload back to `(slot_index, generation)`.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub(crate) fn decode_payload(payload: u32) -> (u32, u16) {
        let slot = payload & 0xFFFF;
        let generation = (payload >> 16) as u16;
        (slot, generation)
    }

    /// Store a relative duration into the timer slot and return a raw pointer
    /// to the timespec for io_uring submission.
    #[cfg(has_io_uring)]
    pub(crate) fn set_relative(
        &mut self,
        slot: u32,
        duration: std::time::Duration,
    ) -> *const io_uring::types::Timespec {
        let idx = slot as usize;
        self.timespecs[idx] = io_uring::types::Timespec::new()
            .sec(duration.as_secs())
            .nsec(duration.subsec_nanos());
        &self.timespecs[idx] as *const _
    }

    /// Store an absolute deadline into the timer slot and return a raw pointer
    /// to the timespec for io_uring submission.
    #[cfg(has_io_uring)]
    pub(crate) fn set_absolute(
        &mut self,
        slot: u32,
        secs: u64,
        nsecs: u32,
    ) -> *const io_uring::types::Timespec {
        let idx = slot as usize;
        self.timespecs[idx] = io_uring::types::Timespec::new().sec(secs).nsec(nsecs);
        &self.timespecs[idx] as *const _
    }

    /// Store a relative duration as a deadline (mio backend).
    #[cfg(not(has_io_uring))]
    pub(crate) fn set_relative(&mut self, slot: u32, duration: std::time::Duration) {
        let idx = slot as usize;
        let deadline = std::time::Instant::now() + duration;
        self.deadlines[idx] = Some(deadline);
        self.expiry_heap
            .push(std::cmp::Reverse((deadline, slot, self.generations[idx])));
    }

    /// Store an absolute deadline (mio backend).
    /// The secs/nsecs are CLOCK_MONOTONIC values; convert to Instant.
    #[cfg(not(has_io_uring))]
    pub(crate) fn set_absolute(&mut self, slot: u32, secs: u64, nsecs: u32) {
        let idx = slot as usize;
        // Approximate: compute offset from current monotonic clock to the deadline.
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        unsafe {
            libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
        }
        let now_ns = ts.tv_sec as u128 * 1_000_000_000 + ts.tv_nsec as u128;
        let deadline_ns = secs as u128 * 1_000_000_000 + nsecs as u128;
        let deadline = if deadline_ns > now_ns {
            std::time::Instant::now()
                + std::time::Duration::from_nanos((deadline_ns - now_ns) as u64)
        } else {
            std::time::Instant::now()
        };
        self.deadlines[idx] = Some(deadline);
        self.expiry_heap
            .push(std::cmp::Reverse((deadline, slot, self.generations[idx])));
    }

    /// A heap entry is live iff the slot still holds this exact deadline in
    /// the same generation and hasn't fired. Anything else is a stale entry
    /// left behind by release/re-arm and is skipped (lazy deletion).
    #[cfg(not(has_io_uring))]
    fn heap_entry_live(&self, deadline: std::time::Instant, slot: u32, generation: u16) -> bool {
        let idx = slot as usize;
        idx < self.generations.len()
            && self.generations[idx] == generation
            && !self.fired[idx]
            && self.deadlines[idx] == Some(deadline)
    }

    /// Earliest live deadline, discarding stale heap heads.
    #[cfg(not(has_io_uring))]
    pub(crate) fn next_deadline(&mut self) -> Option<std::time::Instant> {
        while let Some(&std::cmp::Reverse((deadline, slot, generation))) = self.expiry_heap.peek() {
            if self.heap_entry_live(deadline, slot, generation) {
                return Some(deadline);
            }
            self.expiry_heap.pop();
        }
        None
    }

    /// Pop the next timer whose deadline is <= `now`. Returns
    /// `(slot, generation)` ready to pass to [`fire`](Self::fire).
    #[cfg(not(has_io_uring))]
    pub(crate) fn pop_expired(&mut self, now: std::time::Instant) -> Option<(u32, u16)> {
        while let Some(&std::cmp::Reverse((deadline, slot, generation))) = self.expiry_heap.peek() {
            if !self.heap_entry_live(deadline, slot, generation) {
                self.expiry_heap.pop();
                continue;
            }
            if deadline > now {
                return None;
            }
            self.expiry_heap.pop();
            return Some((slot, generation));
        }
        None
    }
}

/// Per-worker async executor. Owns the task slab and coordinates
/// CQE-driven wakeups with future polling.
pub(crate) struct Executor {
    pub(crate) task_slab: TaskSlab,
    /// Standalone tasks not bound to any connection.
    pub(crate) standalone_slab: StandaloneTaskSlab,
    /// Timer slot pool for sleep/timeout.
    pub(crate) timer_pool: TimerSlotPool,
    /// Connection indices (and standalone task indices with STANDALONE_BIT) ready to poll.
    pub(crate) ready_queue: VecDeque<u32>,
    /// The task currently being polled (its future is checked out of the
    /// slab, so its slot reads Empty). `wake_task` consults this to record
    /// self-wakes instead of dropping them.
    pub(crate) currently_polling: Option<u32>,
    /// Set by `wake_task` when the currently-polling task wakes itself;
    /// consumed by the poll loop to re-queue the task after parking.
    pub(crate) woken_while_polling: bool,
    /// Scratch for draining the thread-local waker queue.
    waker_drain_scratch: VecDeque<u32>,
    /// Per-connection: task is awaiting recv data.
    pub(crate) recv_waiters: Vec<bool>,
    /// Per-connection: task is awaiting send completion.
    pub(crate) send_waiters: Vec<bool>,
    /// Per-connection: task is awaiting connect result.
    pub(crate) connect_waiters: Vec<bool>,
    /// Per-connection: CQE result storage for send/connect.
    pub(crate) io_results: Vec<Option<IoResult>>,
    /// Maps conn_index → owning task ID. For accepted connections, `owner_task[i] = Some(i)`
    /// (self-owned). For outbound connections created via `ConnCtx::connect()`,
    /// `owner_task[i] = Some(calling_task_id)` where `calling_task_id` is the task
    /// that initiated the connect. This indirection allows `wake_recv`/`wake_send`/
    /// `wake_connect` to wake the correct task even when the connection index differs
    /// from the task index.
    pub(crate) owner_task: Vec<Option<u32>>,
    /// Per-connection recv sink for direct-to-buffer writes.
    pub(crate) recv_sinks: Vec<Option<RecvSink>>,
    /// Per-UDP-socket datagram recv queue for async tasks.
    ///
    /// Holds buffer references into the kernel-provided UDP recv ring (zero-copy)
    /// rather than owned `Vec<u8>`s. The consumer pops, reads the slice, and
    /// pushes the `bid` to `Driver::udp_pending_replenish` to return the buffer
    /// to the kernel ring.
    pub(crate) udp_recv_queues: Vec<VecDeque<PendingUdpDatagram>>,
    /// Hard cap on each `udp_recv_queues[i]`. Datagrams arriving when the
    /// queue is full are dropped (`udp::DATAGRAMS_DROPPED` is incremented).
    /// This prevents unbounded memory growth when the consumer future
    /// stalls or has exited.
    pub(crate) udp_recv_queue_capacity: usize,
    /// Per-UDP-socket: task ID waiting for recv_from (None = no waiter).
    pub(crate) udp_recv_waiters: Vec<Option<u32>>,
    /// Per-UDP-socket: task ID waiting for a free send slot (None = no waiter).
    /// Driven by the io_uring backend's slot ring; the mio backend leaves this
    /// empty because sends are synchronous and never suspend.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub(crate) udp_send_ready_waiters: Vec<Option<u32>>,
    /// Disk I/O: maps command slab_idx → task_id to wake on completion.
    pub(crate) disk_io_waiters: HashMap<u32, u32>,
    /// Disk I/O: maps command slab_idx → i32 result from CQE.
    pub(crate) disk_io_results: HashMap<u32, i32>,
    /// Disk I/O buffer graveyard: holds buffers whose owning future was dropped
    /// before the CQE arrived. Entry is removed (and the buffer freed) when
    /// `wake_disk_io` fires, ensuring the kernel's pointer remains valid for
    /// the entire op even after the future goes away.
    pub(crate) disk_io_graveyard: HashMap<u32, bytes::BytesMut>,
    /// Filesystem stat results: maps slab_idx → Metadata (populated by handle_fs for Statx ops).
    pub(crate) fs_stat_results: HashMap<u32, crate::fs::Metadata>,
    /// Pending DNS resolve requests: request_id -> (task_id to wake, result slot).
    pub(crate) pending_resolves: HashMap<u64, (u32, Option<stdio::Result<std::net::SocketAddr>>)>,
    /// Monotonic counter for resolve request IDs.
    pub(crate) next_resolve_id: u64,
    /// Pending process spawn requests: request_id -> (task_id to wake, result slot).
    pub(crate) pending_spawns:
        HashMap<u64, (u32, Option<stdio::Result<crate::spawner::SpawnResult>>)>,
    /// Monotonic counter for spawn request IDs.
    pub(crate) next_spawn_id: u64,
    /// Pidfd poll waiters: seq -> task_id to wake on pidfd readability.
    pub(crate) pidfd_waiters: HashMap<u32, u32>,
    /// Pidfd poll results: seq -> CQE result (stored until polled by WaitFuture).
    pub(crate) pidfd_results: HashMap<u32, i32>,
    /// Monotonic counter for pidfd poll sequence numbers.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub(crate) next_pidfd_seq: u32,
    /// Pending blocking requests: request_id -> (task_id to wake, result slot).
    pub(crate) pending_blocking: HashMap<u64, (u32, Option<Box<dyn std::any::Any + Send>>)>,
    /// Monotonic counter for blocking request IDs.
    pub(crate) next_blocking_id: u64,
    /// Per-batch dedup bitset for connection-task ready-queue entries.
    ///
    /// Indexed by `conn_index`. Set to `true` when that id is first seen in
    /// `poll_ready_tasks`; cleared only for the entries touched, never
    /// globally. Sized to `max_connections` at construction — zero per-call
    /// allocation.
    pub(crate) poll_dedup_conn: Vec<bool>,
    /// Per-batch dedup bitset for standalone-task ready-queue entries.
    ///
    /// Indexed by `task_idx & !STANDALONE_BIT`. Same lifecycle as
    /// `poll_dedup_conn`. Sized to `standalone_task_capacity`.
    pub(crate) poll_dedup_standalone: Vec<bool>,
}

impl Executor {
    /// Create a new executor with the given capacities.
    pub(crate) fn new(
        max_connections: u32,
        standalone_capacity: u32,
        timer_slots: u32,
        udp_count: u32,
        udp_recv_queue_capacity: usize,
    ) -> Self {
        let cap = max_connections as usize;
        let udp = udp_count as usize;
        Executor {
            task_slab: TaskSlab::new(max_connections),
            standalone_slab: StandaloneTaskSlab::new(standalone_capacity),
            timer_pool: TimerSlotPool::new(timer_slots),
            ready_queue: VecDeque::with_capacity(64),
            currently_polling: None,
            woken_while_polling: false,
            waker_drain_scratch: VecDeque::with_capacity(64),
            recv_waiters: vec![false; cap],
            send_waiters: vec![false; cap],
            connect_waiters: vec![false; cap],
            io_results: {
                let mut v = Vec::with_capacity(cap);
                for _ in 0..cap {
                    v.push(None);
                }
                v
            },
            owner_task: vec![None; cap],
            recv_sinks: {
                let mut v = Vec::with_capacity(cap);
                for _ in 0..cap {
                    v.push(None);
                }
                v
            },
            udp_recv_queues: (0..udp).map(|_| VecDeque::new()).collect(),
            udp_recv_queue_capacity,
            udp_recv_waiters: vec![None; udp],
            udp_send_ready_waiters: vec![None; udp],
            disk_io_waiters: HashMap::new(),
            disk_io_results: HashMap::new(),
            disk_io_graveyard: HashMap::new(),
            fs_stat_results: HashMap::new(),
            pending_resolves: HashMap::new(),
            next_resolve_id: 0,
            pending_spawns: HashMap::new(),
            next_spawn_id: 0,
            pidfd_waiters: HashMap::new(),
            pidfd_results: HashMap::new(),
            next_pidfd_seq: 0,
            pending_blocking: HashMap::new(),
            next_blocking_id: 0,
            poll_dedup_conn: vec![false; cap],
            poll_dedup_standalone: vec![false; standalone_capacity as usize],
        }
    }

    /// Drain the thread-local waker queue, transitioning each woken task
    /// Parked → Ready and queueing it for poll.
    ///
    /// The transition matters: a std `Waker` pushes only the raw id onto the
    /// thread-local queue. Draining that id straight into `ready_queue`
    /// (as this used to) left the slot Parked, so `take_ready()` returned
    /// `None` and the wake was lost forever — stored wakers never worked
    /// for parked tasks. Routing through `wake_task` performs the slab
    /// transition and also handles wakes of the currently-polling task.
    pub(crate) fn collect_wakeups(&mut self) {
        drain_ready_queue(&mut self.waker_drain_scratch);
        while let Some(id) = self.waker_drain_scratch.pop_front() {
            let _ = self.wake_task(id);
        }
    }

    /// Reset all per-connection state for a connection that was closed.
    pub(crate) fn remove_connection(&mut self, conn_index: u32) {
        let idx = conn_index as usize;
        // Clear recv sink before removing the task — the task owns the memory
        // the sink points to, so the sink must be invalidated first.
        if idx < self.recv_sinks.len() {
            self.recv_sinks[idx] = None;
        }
        self.task_slab.remove(conn_index);
        if idx < self.recv_waiters.len() {
            // If a *standalone* task was awaiting recv/send/connect on this
            // connection, it isn't removed by `task_slab.remove`. Push it
            // back onto the ready queue so its future polls once more and
            // sees the new generation via the `ConnCtx`-stored gen check
            // — at which point it returns `ConnectionAborted` instead of
            // sitting parked forever. (Connection-bound tasks have already
            // been dropped by `task_slab.remove`.)
            let any_waiter =
                self.recv_waiters[idx] || self.send_waiters[idx] || self.connect_waiters[idx];
            if any_waiter
                && let Some(task_id) = self.owner_task[idx]
                && (task_id & waker::STANDALONE_BIT != 0 || task_id != conn_index)
            {
                // Push to the ready queue; the standalone task slab still
                // holds the future, and the next poll will short-circuit on
                // the generation mismatch.
                let _ = self.wake_task(task_id);
            }
            self.recv_waiters[idx] = false;
            self.send_waiters[idx] = false;
            self.connect_waiters[idx] = false;
            self.io_results[idx] = None;
            self.owner_task[idx] = None;
        }
    }

    /// Wake a task by its ID (connection task or standalone task).
    ///
    /// Handles both connection tasks (plain index) and standalone tasks
    /// (index | STANDALONE_BIT). Returns true if the task was parked and
    /// is now ready.
    pub(crate) fn wake_task(&mut self, task_id: u32) -> bool {
        // A task waking itself from inside its own poll: the future is
        // checked out of the slab (slot reads Empty), so slab.wake() would
        // silently drop the wake and the task would park forever. Record it;
        // the poll loop re-queues the task after parking it.
        if self.currently_polling == Some(task_id) {
            self.woken_while_polling = true;
            return true;
        }
        if task_id & waker::STANDALONE_BIT != 0 {
            let idx = task_id & !waker::STANDALONE_BIT;
            if self.standalone_slab.wake(idx) {
                self.ready_queue.push_back(task_id);
                return true;
            }
        } else if self.task_slab.wake(task_id) {
            self.ready_queue.push_back(task_id);
            return true;
        }
        false
    }

    /// Wake a task that was waiting for recv data.
    ///
    /// Resolves through `owner_task` so that outbound connections correctly
    /// wake the task that owns them (which may differ from the conn_index).
    pub(crate) fn wake_recv(&mut self, conn_index: u32) {
        let idx = conn_index as usize;
        if idx < self.recv_waiters.len() && self.recv_waiters[idx] {
            self.recv_waiters[idx] = false;
            let task_id = self.owner_task[idx].unwrap_or(conn_index);
            self.wake_task(task_id);
        }
    }

    /// Wake a task that was waiting for send completion.
    pub(crate) fn wake_send(&mut self, conn_index: u32, result: stdio::Result<u32>) {
        let idx = conn_index as usize;
        if idx < self.send_waiters.len() && self.send_waiters[idx] {
            self.send_waiters[idx] = false;
            self.io_results[idx] = Some(IoResult::Send(result));
            let task_id = self.owner_task[idx].unwrap_or(conn_index);
            self.wake_task(task_id);
        }
    }

    /// Wake a task that was waiting for a UDP datagram.
    pub(crate) fn wake_udp_recv(&mut self, udp_index: u32) {
        let idx = udp_index as usize;
        if idx < self.udp_recv_waiters.len()
            && let Some(task_id) = self.udp_recv_waiters[idx].take()
        {
            self.wake_task(task_id);
        }
    }

    /// Wake a task that was waiting for a free UDP send slot.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub(crate) fn wake_udp_send_ready(&mut self, udp_index: u32) {
        let idx = udp_index as usize;
        if idx < self.udp_send_ready_waiters.len()
            && let Some(task_id) = self.udp_send_ready_waiters[idx].take()
        {
            self.wake_task(task_id);
        }
    }

    /// Wake a task that was waiting for connect completion.
    pub(crate) fn wake_connect(&mut self, conn_index: u32, result: stdio::Result<()>) {
        let idx = conn_index as usize;
        if idx < self.connect_waiters.len() && self.connect_waiters[idx] {
            self.connect_waiters[idx] = false;
            self.io_results[idx] = Some(IoResult::Connect(result));
            let task_id = self.owner_task[idx].unwrap_or(conn_index);
            self.wake_task(task_id);
        }
    }

    /// Wake a task that was waiting for a disk I/O completion.
    ///
    /// Stores the CQE result and wakes the task if one is registered.
    /// Disk I/O waiters are keyed by slab_idx (not conn_index), so
    /// `remove_connection()` does not need to clear them — the task
    /// holds the `DiskIoFuture` and will consume the result.
    ///
    /// If the owning future was dropped before completion, its buffer was
    /// parked in `disk_io_graveyard`. We free it here (the kernel is now
    /// done with the pointer) and discard the result, since no future
    /// remains to read it.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub(crate) fn wake_disk_io(&mut self, seq: u32, result: i32) {
        if self.disk_io_graveyard.remove(&seq).is_some() {
            return;
        }
        self.disk_io_results.insert(seq, result);
        if let Some(task_id) = self.disk_io_waiters.remove(&seq) {
            self.wake_task(task_id);
        }
    }

    /// Deliver a process spawn response and wake the waiting task.
    pub(crate) fn deliver_spawn(
        &mut self,
        request_id: u64,
        result: stdio::Result<crate::spawner::SpawnResult>,
    ) {
        if let Some((task_id, slot)) = self.pending_spawns.get_mut(&request_id) {
            *slot = Some(result);
            let task_id = *task_id;
            self.wake_task(task_id);
        } else {
            // The SpawnFuture was dropped before the response arrived —
            // close the pidfd instead of leaking it.
            if let Ok(r) = result {
                unsafe {
                    libc::close(r.pidfd);
                }
            }
        }
    }

    /// Wake a task waiting for pidfd poll completion (child process exit).
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub(crate) fn wake_pidfd(&mut self, seq: u32, result: i32) {
        self.pidfd_results.insert(seq, result);
        if let Some(task_id) = self.pidfd_waiters.remove(&seq) {
            self.wake_task(task_id);
        }
    }

    /// Deliver a blocking response and wake the waiting task.
    pub(crate) fn deliver_blocking(
        &mut self,
        request_id: u64,
        result: Box<dyn std::any::Any + Send>,
    ) {
        if let Some((task_id, slot)) = self.pending_blocking.get_mut(&request_id) {
            *slot = Some(result);
            let task_id = *task_id;
            self.wake_task(task_id);
        }
    }

    /// Deliver a DNS resolve response and wake the waiting task.
    pub(crate) fn deliver_resolve(
        &mut self,
        request_id: u64,
        result: stdio::Result<std::net::SocketAddr>,
    ) {
        if let Some((task_id, slot)) = self.pending_resolves.get_mut(&request_id) {
            *slot = Some(result);
            let task_id = *task_id;
            self.wake_task(task_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_wakeups_transitions_parked_to_ready() {
        // A std Waker pushes only the raw id onto the thread-local queue.
        // collect_wakeups must transition the slot Parked -> Ready or the
        // wake is lost (take_ready returns None for Parked slots).
        let mut exec = Executor::new(8, 8, 8, 0, 0);
        exec.task_slab.spawn(3, Box::pin(async {}));
        let fut = exec.task_slab.take_ready(3).unwrap();
        exec.task_slab.park(3, fut);

        waker::conn_waker(3).wake_by_ref();
        exec.collect_wakeups();

        assert_eq!(exec.ready_queue.len(), 1);
        assert!(
            exec.task_slab.take_ready(3).is_some(),
            "waker-woken parked task must be Ready"
        );
    }

    #[test]
    fn wake_task_during_own_poll_is_recorded() {
        // While a task is being polled its future is checked out (slot
        // Empty); a self-wake must be recorded for post-park re-queue
        // instead of being dropped.
        let mut exec = Executor::new(8, 8, 8, 0, 0);
        exec.task_slab.spawn(2, Box::pin(async {}));
        let fut = exec.task_slab.take_ready(2).unwrap();

        exec.currently_polling = Some(2);
        assert!(exec.wake_task(2), "self-wake must be accepted");
        assert!(exec.woken_while_polling);
        exec.currently_polling = None;

        // The poll loop parks and re-queues:
        exec.task_slab.park(2, fut);
        assert!(exec.wake_task(2));
        assert!(exec.task_slab.take_ready(2).is_some());
    }

    #[test]
    fn remove_connection_wakes_cross_index_connection_task_owner() {
        // Proxy pattern: connection task 3 (inbound) awaits recv on
        // upstream connection 5. Teardown of 5 must re-wake task 3 even
        // though it is not a standalone task.
        let mut exec = Executor::new(8, 8, 8, 0, 0);
        exec.task_slab.spawn(3, Box::pin(async {}));
        let fut = exec.task_slab.take_ready(3).unwrap();
        exec.task_slab.park(3, fut);

        exec.recv_waiters[5] = true;
        exec.owner_task[5] = Some(3);
        exec.remove_connection(5);

        assert!(
            exec.task_slab.take_ready(3).is_some(),
            "cross-index owner task must be re-woken on teardown"
        );
    }

    #[cfg(not(has_io_uring))]
    #[test]
    fn timer_release_clears_mio_deadline() {
        // The mio expiry scan reads the slot's current generation, so a
        // deadline left on a free slot would self-satisfy fire()'s check.
        let mut pool = TimerSlotPool::new(4);
        let (slot, _gen) = pool.allocate(42).unwrap();
        pool.deadlines[slot as usize] =
            Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
        pool.release(slot);
        assert!(
            pool.deadlines[slot as usize].is_none(),
            "released slot must not retain a deadline"
        );
    }

    #[test]
    fn next_segment_range_walks_gro_entry() {
        // 2.5 segments of 1000: ranges must advance one segment at a time.
        let mut e = owned_datagram(vec![0u8; 2500], 1000);
        assert_eq!(e.next_segment_range(), (0, 1000));
        e.consumed = 1000;
        assert!(!e.exhausted());
        assert_eq!(e.next_segment_range(), (1000, 2000));
        e.consumed = 2000;
        assert_eq!(e.next_segment_range(), (2000, 2500));
        e.consumed = 2500;
        assert!(e.exhausted());
    }

    #[test]
    fn next_segment_range_uncoalesced_is_whole_payload() {
        let e = owned_datagram(vec![7u8; 1200], 0);
        assert_eq!(e.next_segment_range(), (0, 1200));
        assert!(e.exhausted());
    }

    fn owned_datagram(payload: Vec<u8>, segment_size: u32) -> PendingUdpDatagram {
        PendingUdpDatagram {
            peer: "127.0.0.1:0".parse().unwrap(),
            buf: PendingUdpBuf::Owned(payload),
            recv_at: std::time::Instant::now(),
            segment_size,
            consumed: 0,
        }
    }

    #[test]
    fn for_each_segment_no_gro_yields_whole_payload() {
        let entry = owned_datagram(vec![7u8; 1200], 0);
        let mut lens = Vec::new();
        entry.for_each_segment(|s| lens.push(s.len()));
        assert_eq!(lens, vec![1200]);
    }

    #[test]
    fn for_each_segment_splits_coalesced_payload() {
        // 2.5 segments of 1000 → chunks 1000, 1000, 500.
        let entry = owned_datagram(vec![0u8; 2500], 1000);
        let mut lens = Vec::new();
        entry.for_each_segment(|s| lens.push(s.len()));
        assert_eq!(lens, vec![1000, 1000, 500]);
    }

    #[test]
    fn for_each_segment_exact_multiple() {
        let entry = owned_datagram(vec![0u8; 3000], 1500);
        let mut count = 0;
        entry.for_each_segment(|s| {
            assert_eq!(s.len(), 1500);
            count += 1;
        });
        assert_eq!(count, 2);
    }

    #[test]
    fn executor_new() {
        let exec = Executor::new(16, 8, 8, 0, 0);
        assert!(exec.ready_queue.is_empty());
        assert_eq!(exec.recv_waiters.len(), 16);
        assert_eq!(exec.send_waiters.len(), 16);
        assert_eq!(exec.connect_waiters.len(), 16);
        assert_eq!(exec.io_results.len(), 16);
        assert_eq!(exec.owner_task.len(), 16);
    }

    #[test]
    fn remove_connection_clears_state() {
        let mut exec = Executor::new(4, 4, 4, 0, 0);
        exec.recv_waiters[1] = true;
        exec.send_waiters[1] = true;
        exec.connect_waiters[1] = true;
        exec.io_results[1] = Some(IoResult::Send(Ok(42)));
        exec.owner_task[1] = Some(0);

        exec.remove_connection(1);
        assert!(!exec.recv_waiters[1]);
        assert!(!exec.send_waiters[1]);
        assert!(!exec.connect_waiters[1]);
        assert!(exec.io_results[1].is_none());
        assert!(exec.owner_task[1].is_none());
    }

    #[test]
    fn wake_task_connection_task() {
        let mut exec = Executor::new(4, 4, 4, 0, 0);
        // Spawn a task at index 1 (simulated by setting it up as Ready then parking).
        exec.task_slab
            .spawn(1, Box::pin(std::future::pending::<()>()));
        let fut = exec.task_slab.take_ready(1).unwrap();
        exec.task_slab.park(1, fut);

        assert!(exec.wake_task(1));
        assert_eq!(exec.ready_queue.len(), 1);
        assert_eq!(exec.ready_queue[0], 1);
    }

    #[test]
    fn wake_task_standalone_task() {
        let mut exec = Executor::new(4, 4, 4, 0, 0);
        let idx = exec
            .standalone_slab
            .spawn(Box::pin(std::future::pending::<()>()))
            .unwrap();
        let fut = exec.standalone_slab.take_ready(idx).unwrap();
        exec.standalone_slab.park(idx, fut);

        let task_id = idx | waker::STANDALONE_BIT;
        assert!(exec.wake_task(task_id));
        assert_eq!(exec.ready_queue.len(), 1);
        assert_eq!(exec.ready_queue[0], task_id);
    }

    #[test]
    fn owner_task_routes_recv_wakeup() {
        let mut exec = Executor::new(16, 4, 4, 0, 0);

        // Task at index 5 owns connection 12 (outbound connect scenario).
        exec.task_slab
            .spawn(5, Box::pin(std::future::pending::<()>()));
        let fut = exec.task_slab.take_ready(5).unwrap();
        exec.task_slab.park(5, fut);

        exec.owner_task[12] = Some(5);
        exec.recv_waiters[12] = true;

        exec.wake_recv(12);

        // The task at index 5 should be woken, not index 12.
        assert_eq!(exec.ready_queue.len(), 1);
        assert_eq!(exec.ready_queue[0], 5);
        assert!(!exec.recv_waiters[12]);
    }

    #[test]
    fn owner_task_routes_send_wakeup() {
        let mut exec = Executor::new(16, 4, 4, 0, 0);

        exec.task_slab
            .spawn(3, Box::pin(std::future::pending::<()>()));
        let fut = exec.task_slab.take_ready(3).unwrap();
        exec.task_slab.park(3, fut);

        exec.owner_task[10] = Some(3);
        exec.send_waiters[10] = true;

        exec.wake_send(10, Ok(42));

        assert_eq!(exec.ready_queue.len(), 1);
        assert_eq!(exec.ready_queue[0], 3);
        assert!(!exec.send_waiters[10]);
        assert!(matches!(exec.io_results[10], Some(IoResult::Send(Ok(42)))));
    }

    #[test]
    fn owner_task_routes_connect_wakeup() {
        let mut exec = Executor::new(16, 4, 4, 0, 0);

        exec.task_slab
            .spawn(2, Box::pin(std::future::pending::<()>()));
        let fut = exec.task_slab.take_ready(2).unwrap();
        exec.task_slab.park(2, fut);

        exec.owner_task[8] = Some(2);
        exec.connect_waiters[8] = true;

        exec.wake_connect(8, Ok(()));

        assert_eq!(exec.ready_queue.len(), 1);
        assert_eq!(exec.ready_queue[0], 2);
        assert!(!exec.connect_waiters[8]);
    }

    #[test]
    fn owner_task_none_falls_back_to_conn_index() {
        let mut exec = Executor::new(4, 4, 4, 0, 0);

        // owner_task is None — should fall back to using conn_index directly.
        exec.task_slab
            .spawn(1, Box::pin(std::future::pending::<()>()));
        let fut = exec.task_slab.take_ready(1).unwrap();
        exec.task_slab.park(1, fut);

        exec.recv_waiters[1] = true;
        exec.wake_recv(1);

        assert_eq!(exec.ready_queue.len(), 1);
        assert_eq!(exec.ready_queue[0], 1);
    }

    // ── TimerSlotPool tests ────────────────────────────────────────

    #[test]
    fn timer_allocate_and_fire() {
        let mut pool = TimerSlotPool::new(4);

        let (slot, generation) = pool.allocate(42).unwrap();
        assert!(!pool.is_fired(slot));

        let waker_id = pool.fire(slot, generation).unwrap();
        assert_eq!(waker_id, 42);
        assert!(pool.is_fired(slot));
    }

    #[test]
    fn timer_fire_stale_generation_returns_none() {
        let mut pool = TimerSlotPool::new(4);

        let (slot, generation) = pool.allocate(10).unwrap();
        pool.release(slot); // increments generation

        // Fire with old generation — should return None (stale).
        assert!(pool.fire(slot, generation).is_none());
    }

    #[test]
    fn timer_release_increments_generation() {
        let mut pool = TimerSlotPool::new(4);

        let (slot, gen0) = pool.allocate(1).unwrap();
        pool.release(slot);

        let (slot2, gen1) = pool.allocate(2).unwrap();
        assert_eq!(slot2, slot); // same slot reused
        assert_eq!(gen1, gen0 + 1);

        pool.release(slot2);
    }

    #[test]
    fn timer_generation_wraps_at_u16_max() {
        let mut pool = TimerSlotPool::new(1);

        let (slot, _) = pool.allocate(1).unwrap();
        pool.generations[slot as usize] = u16::MAX;
        pool.release(slot);

        let (slot2, generation) = pool.allocate(2).unwrap();
        assert_eq!(slot2, slot);
        assert_eq!(generation, 0); // wrapped from u16::MAX
    }

    #[test]
    fn timer_encode_decode_round_trip() {
        let slot = 1234u32;
        let generation = 5678u16;
        let payload = TimerSlotPool::encode_payload(slot, generation);
        let (decoded_slot, decoded_gen) = TimerSlotPool::decode_payload(payload);
        assert_eq!(decoded_slot, slot);
        assert_eq!(decoded_gen, generation);
    }

    #[test]
    fn timer_encode_decode_boundary_values() {
        // Max slot (16 bits) and max generation (16 bits).
        let payload = TimerSlotPool::encode_payload(0xFFFF, 0xFFFF);
        let (slot, generation) = TimerSlotPool::decode_payload(payload);
        assert_eq!(slot, 0xFFFF);
        assert_eq!(generation, 0xFFFF);

        // Zero values.
        let payload = TimerSlotPool::encode_payload(0, 0);
        let (slot, generation) = TimerSlotPool::decode_payload(payload);
        assert_eq!(slot, 0);
        assert_eq!(generation, 0);
    }

    #[test]
    fn timer_exhaust_pool() {
        let mut pool = TimerSlotPool::new(2);

        let (s0, _) = pool.allocate(1).unwrap();
        let (s1, _) = pool.allocate(2).unwrap();
        assert!(pool.allocate(3).is_none()); // full

        pool.release(s0);
        assert!(pool.allocate(4).is_some()); // one freed
        assert!(pool.allocate(5).is_none()); // full again

        pool.release(s1);
    }

    #[test]
    fn timer_is_fired_out_of_bounds() {
        let pool = TimerSlotPool::new(2);
        assert!(!pool.is_fired(99)); // out of bounds returns false
    }

    #[test]
    fn timer_fire_out_of_bounds_returns_none() {
        let mut pool = TimerSlotPool::new(2);
        assert!(pool.fire(99, 0).is_none());
    }

    #[test]
    fn timer_allocate_resets_fired_flag() {
        let mut pool = TimerSlotPool::new(2);

        let (slot, generation) = pool.allocate(1).unwrap();
        pool.fire(slot, generation).unwrap();
        assert!(pool.is_fired(slot));

        pool.release(slot);
        let (slot2, _) = pool.allocate(2).unwrap();
        assert_eq!(slot2, slot);
        assert!(!pool.is_fired(slot2)); // fired flag reset on allocate
    }
}
