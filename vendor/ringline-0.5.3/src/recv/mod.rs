//! Recv-side helpers shared across backends.
//!
//! Pure, backend-agnostic decision logic for the segmented-recv work (see
//! `docs/segmented-recv-design.md`). Nothing here performs I/O; the io_uring and
//! mio backends consume these decisions. Kept separate so the logic is unit-
//! testable on both backends (including macOS/mio, where the io_uring path
//! cannot be built).

pub(crate) mod domain;
pub(crate) mod occupancy;
