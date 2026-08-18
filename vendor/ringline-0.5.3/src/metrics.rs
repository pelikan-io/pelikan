//! ringline runtime metrics.
//!
//! Per-worker counters for connections, bytes, ring utilization, and pool
//! exhaustion. Automatically exposed via Prometheus when registered with
//! the admin server.

use metriken::{Gauge, ShardedCounterGroup, metric};

// ── Sharded counter groups ──────────────────────────────────────

#[metric(
    name = "ringline/connections",
    description = "Connection lifecycle counters"
)]
pub static CONNECTIONS: ShardedCounterGroup = ShardedCounterGroup::new(2);

#[metric(name = "ringline/bytes", description = "Byte transfer counters")]
pub static BYTES: ShardedCounterGroup = ShardedCounterGroup::new(3);

#[metric(name = "ringline/ring", description = "Ring utilization counters")]
pub static RING: ShardedCounterGroup = ShardedCounterGroup::new(5);

#[metric(name = "ringline/pool", description = "Pool exhaustion counters")]
pub static POOL: ShardedCounterGroup = ShardedCounterGroup::new(7);

#[metric(name = "ringline/udp", description = "UDP counters")]
pub static UDP: ShardedCounterGroup = ShardedCounterGroup::new(4);

// ── Gauge (not sharded) ─────────────────────────────────────────

#[metric(
    name = "ringline/connections/active",
    description = "Currently active connections"
)]
pub static CONNECTIONS_ACTIVE: Gauge = Gauge::new();

// ── Index constants ─────────────────────────────────────────────

/// Counter slot indices for connection metrics.
pub mod conn {
    pub const ACCEPTED: usize = 0;
    pub const CLOSED: usize = 1;
}

/// Counter slot indices for byte metrics.
pub mod bytes {
    pub const RECEIVED: usize = 0;
    pub const SENT: usize = 1;
    /// Bytes received via fallback one-shot recvs (also counted in
    /// `RECEIVED`); the fraction of traffic arriving through the
    /// degraded path when the provided ring is smaller than a response.
    pub const FALLBACK_RECEIVED: usize = 2;
}

/// Counter slot indices for ring utilization metrics.
pub mod ring {
    pub const CQE_PROCESSED: usize = 0;
    pub const SQE_SUBMIT_FAILURES: usize = 1;
    pub const CLOSE_SUBMIT_FAILURES: usize = 2;
    pub const RECV_ARM_FAILURES: usize = 3;
    /// A CQE arrived with an `OpTag` that `OpTag::from_u8` doesn't
    /// recognise. Indicates either a corrupted user_data or a future
    /// reorder of the `OpTag` enum that left a stale value in flight.
    pub const CQE_UNKNOWN_TAG: usize = 4;
}

/// Counter slot indices for pool exhaustion metrics.
pub mod pool {
    pub const SEND_EXHAUSTED: usize = 0;
    pub const TIMER_EXHAUSTED: usize = 1;
    pub const BUFFER_RING_EMPTY: usize = 2;
    /// A TCP send returned `-EAGAIN` from the kernel — the send buffer
    /// was full and ringline armed a `POLLOUT` retry. High counts mean
    /// the peer is consuming bytes more slowly than the producer
    /// generates them; tune `tcp_*_buffer_size` or apply
    /// application-level backpressure.
    pub const SEND_EAGAIN: usize = 3;
    /// A connection's multishot recv completed with `ENOBUFS` and the
    /// connection was parked until provided-ring buffers are returned
    /// (see `recv_starved` in the uring driver). While parked the socket
    /// is not being drained, so the kernel receive buffer fills and the
    /// advertised TCP window closes — sustained counts with large
    /// payloads mean single responses exceed the provided ring
    /// (`ConfigBuilder::recv_buffer`) and throughput is gated on buffer
    /// recycling rather than on the wire.
    pub const RECV_PARKED: usize = 4;
    /// A fallback one-shot recv was submitted for a connection parked on
    /// ENOBUFS with a partial message accumulated — the graceful-
    /// degradation path that keeps draining the socket when a single
    /// response exceeds the provided ring.
    pub const RECV_FALLBACK: usize = 5;
    /// A Mode A `forward_to` connection reached its `forward_hold_cap` held-buffer
    /// backlog and had its multishot recv cancelled (TCP window closed) to
    /// backpressure the source — re-armed once writes drain the hold below the
    /// cap. Sustained counts mean the sink is slower than the source for large
    /// objects; unlike `RECV_PARKED` (ENOBUFS starvation) this is *deliberate*
    /// per-connection backpressure that prevents one slow forward from depleting
    /// the shared recv ring.
    pub const FORWARD_THROTTLED: usize = 6;
}

/// Counter slot indices for UDP metrics.
pub mod udp {
    pub const DATAGRAMS_RECEIVED: usize = 0;
    pub const DATAGRAMS_SENT: usize = 1;
    pub const SEND_ERRORS: usize = 2;
    /// Datagrams dropped by the runtime because the per-socket recv queue
    /// reached `Config::udp_recv_queue_capacity`. Usually means the
    /// handler future has stopped consuming (panicked, returned early,
    /// or stalled).
    pub const DATAGRAMS_DROPPED: usize = 3;
}

/// Initialize per-entry metadata (labels) for all counter groups.
///
/// Call once at startup before metrics are scraped.
pub fn init_metadata() {
    CONNECTIONS.insert_metadata(conn::ACCEPTED, "op".into(), "accepted".into());
    CONNECTIONS.insert_metadata(conn::CLOSED, "op".into(), "closed".into());

    BYTES.insert_metadata(bytes::RECEIVED, "op".into(), "received".into());
    BYTES.insert_metadata(bytes::SENT, "op".into(), "sent".into());
    BYTES.insert_metadata(
        bytes::FALLBACK_RECEIVED,
        "op".into(),
        "fallback_received".into(),
    );

    RING.insert_metadata(ring::CQE_PROCESSED, "op".into(), "cqe_processed".into());
    RING.insert_metadata(
        ring::SQE_SUBMIT_FAILURES,
        "op".into(),
        "sqe_submit_failures".into(),
    );
    RING.insert_metadata(
        ring::CLOSE_SUBMIT_FAILURES,
        "op".into(),
        "close_submit_failures".into(),
    );
    RING.insert_metadata(
        ring::RECV_ARM_FAILURES,
        "op".into(),
        "recv_arm_failures".into(),
    );
    RING.insert_metadata(ring::CQE_UNKNOWN_TAG, "op".into(), "cqe_unknown_tag".into());

    POOL.insert_metadata(pool::SEND_EXHAUSTED, "op".into(), "send_exhausted".into());
    POOL.insert_metadata(pool::TIMER_EXHAUSTED, "op".into(), "timer_exhausted".into());
    POOL.insert_metadata(
        pool::BUFFER_RING_EMPTY,
        "op".into(),
        "buffer_ring_empty".into(),
    );
    POOL.insert_metadata(pool::SEND_EAGAIN, "op".into(), "send_eagain".into());
    POOL.insert_metadata(pool::RECV_PARKED, "op".into(), "recv_parked".into());
    POOL.insert_metadata(pool::RECV_FALLBACK, "op".into(), "recv_fallback".into());
    POOL.insert_metadata(
        pool::FORWARD_THROTTLED,
        "op".into(),
        "forward_throttled".into(),
    );

    UDP.insert_metadata(
        udp::DATAGRAMS_RECEIVED,
        "op".into(),
        "datagrams_received".into(),
    );
    UDP.insert_metadata(udp::DATAGRAMS_SENT, "op".into(), "datagrams_sent".into());
    UDP.insert_metadata(udp::SEND_ERRORS, "op".into(), "send_errors".into());
    UDP.insert_metadata(
        udp::DATAGRAMS_DROPPED,
        "op".into(),
        "datagrams_dropped".into(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every declared slot index must be in bounds for its group —
    /// `ShardedCounterGroup::increment` silently returns `false` on an
    /// out-of-range index, so an undersized group means a counter that
    /// never counts (this caught `RING` sized 4 with 5 declared slots).
    #[test]
    fn declared_indices_are_in_bounds() {
        for idx in [conn::ACCEPTED, conn::CLOSED] {
            assert!(
                CONNECTIONS.increment(idx),
                "CONNECTIONS[{idx}] out of bounds"
            );
        }
        for idx in [bytes::RECEIVED, bytes::SENT, bytes::FALLBACK_RECEIVED] {
            assert!(BYTES.increment(idx), "BYTES[{idx}] out of bounds");
        }
        for idx in [
            ring::CQE_PROCESSED,
            ring::SQE_SUBMIT_FAILURES,
            ring::CLOSE_SUBMIT_FAILURES,
            ring::RECV_ARM_FAILURES,
            ring::CQE_UNKNOWN_TAG,
        ] {
            assert!(RING.increment(idx), "RING[{idx}] out of bounds");
        }
        for idx in [
            pool::SEND_EXHAUSTED,
            pool::TIMER_EXHAUSTED,
            pool::BUFFER_RING_EMPTY,
            pool::SEND_EAGAIN,
            pool::RECV_PARKED,
            pool::RECV_FALLBACK,
            pool::FORWARD_THROTTLED,
        ] {
            assert!(POOL.increment(idx), "POOL[{idx}] out of bounds");
        }
        for idx in [
            udp::DATAGRAMS_RECEIVED,
            udp::DATAGRAMS_SENT,
            udp::SEND_ERRORS,
            udp::DATAGRAMS_DROPPED,
        ] {
            assert!(UDP.increment(idx), "UDP[{idx}] out of bounds");
        }
    }
}
