//! Pure ring backpressure decision.
//!
//! Given the recv ring's live free-buffer count and a low-water reserve, decide
//! whether a fresh delivery may be handed out as a held (zero-copy) segment or
//! must be force-copied (Mode C) so the buffer returns to the ring immediately.
//! This keeps transient consumers making progress under ring pressure: once the
//! shared per-worker ring's free count drops to the reserve, everyone copies at
//! delivery, so a well-behaved connection is never starved of a buffer by
//! connections holding segments.
//!
//! No I/O; unit-tested on both backends. Consulted by the io_uring segmented
//! recv hold branch (`handle_recv_multi`): once the shared ring's `free()` drops
//! to the reserve, deliveries force-copy (Mode C) instead of pinning. The mio
//! backend has no provided-buffer ring, so it does not consult this (dead there
//! outside the unit tests).
#![cfg_attr(not(has_io_uring), allow(dead_code))]

/// Whether a fresh recv delivery may be handed out zero-copy (held) or must be
/// copied at delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Delivery {
    /// Free buffers are above the reserve — a held (zero-copy) segment is allowed.
    ZeroCopyOk,
    /// At/below the reserve (or the ring is empty) — copy at delivery and
    /// replenish the bid immediately so holders cannot deplete the ring.
    ForceCopy,
}

/// Decide delivery mode from live ring occupancy.
///
/// `free` = buffers currently available in the ring; `reserve` = low-water mark
/// below which held (zero-copy) segments are forbidden. Force-copy iff
/// `free <= reserve` (so `reserve == 0` still forces when the ring is empty).
#[inline]
pub(crate) fn delivery_decision(free: u32, reserve: u32) -> Delivery {
    if free > reserve {
        Delivery::ZeroCopyOk
    } else {
        Delivery::ForceCopy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_copy_ok_when_free_above_reserve() {
        assert_eq!(delivery_decision(64, 8), Delivery::ZeroCopyOk);
        assert_eq!(delivery_decision(9, 8), Delivery::ZeroCopyOk);
    }

    #[test]
    fn force_copy_at_or_below_reserve() {
        assert_eq!(delivery_decision(8, 8), Delivery::ForceCopy);
        assert_eq!(delivery_decision(0, 8), Delivery::ForceCopy);
    }

    #[test]
    fn zero_reserve_forces_only_when_empty() {
        assert_eq!(delivery_decision(1, 0), Delivery::ZeroCopyOk);
        assert_eq!(delivery_decision(0, 0), Delivery::ForceCopy); // no buffer at all
    }
}
