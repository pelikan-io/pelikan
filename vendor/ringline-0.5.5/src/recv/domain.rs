//! Per-connection recv delivery domain (segmented-recv, see
//! `docs/segmented-recv-design.md`).
//!
//! Selects how a connection's received provided buffers are delivered:
//! - `CopyOrConsume` (the default) — the established path: bytes are copied into
//!   the `RecvAccumulator` (or held in the single-buffer zero-copy slot) and
//!   surfaced through `with_data`/`with_bytes`.
//! - `Segmented` — buffers are held in-place (bid NOT replenished, no
//!   accumulator copy) for later zero-copy / copy-at-delivery segment reads
//!   (Mode B/C). TLS connections are excluded from segmented delivery.
//!
//! This is a minimal per-connection selector; the holding machinery lives on the
//! `Driver` (`segment_hold`). The reader that consumes held buffers lands in a
//! later increment — until then `Segmented` is set only in tests.

/// How received provided buffers are delivered for a connection.
///
/// Consumed only by the io_uring backend's recv path; under the mio backend the
/// enum is unused (there is no provided-buffer ring), hence the `dead_code`
/// allow — matching the "inert until a consumer exists" pattern in
/// `recv::occupancy`.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RecvDomain {
    /// Copy into the accumulator / single-buffer zero-copy hold, surfaced via
    /// `with_data`/`with_bytes`. The default for every connection.
    #[default]
    CopyOrConsume,
    /// Hold received provided buffers in-place for segmented delivery (Mode B/C).
    /// Constructed only in tests until the segment reader lands (later increment).
    Segmented,
}
