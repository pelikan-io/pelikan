# Add result-aware receives and FIFO backpressured sends

## Base and scope

This patch applies to the exact crates.io Ringline 0.5.5 source at archive VCS
revision `cd79112f037282d40d6e510161e3dc9aa01aceca`. Version 0.5.5 already
contains the transactional startup work from ringline-rs/ringline#309; this
patch does not change listener creation, startup transactionality, or fallback policy.
It does preserve the exact worker error or panic that triggered rollback.

The changes are generic Ringline runtime APIs with no Pelikan-specific protocol
or configuration behavior.

## Problem

`ConnCtx::with_data` intentionally maps clean EOF and terminal receive errors to
the same zero-length callback. Handlers that need transport metrics or recovery
cannot distinguish them.

Existing send APIs either submit eagerly or wait for socket completion after
submission. A handler that must bound copied-send memory needs an async admission
point before any prefix is submitted. Admission must remain correct across
multi-slot sends, cancellation, future movement between worker-local tasks,
partial writes, half-close, and connection-slot reuse.

## Implementation

- Add `ConnCtx::with_data_result`, preserving exact non-`WouldBlock` receive
  errors while retaining `with_data` compatibility and clean EOF as `Ok(0)`.
- Make multi-slot copy-pool reservation transactional: reserve every slot before
  committing a logical send, or release all tentative slots and submit nothing.
- Add `ConnCtx::send_backpressured`. Construction is inert; first poll registers
  the actual polling task in a worker-local FIFO and later polls refresh the
  operation owner.
- Reject a buffer larger than total copy-pool capacity before writing any byte.
- Carry a stable logical-operation ID through Mio pending writes and io_uring
  pool/slab operations. Completion and abandonment are ID-scoped so stale
  completion cannot satisfy a newer send on the same connection.
- Retain Mio permits until the final socket write. Wake the next waiter on
  permit return, CQ/SQ progress, cancellation, half-close, and teardown.
- Preserve exact terminal write/receive errors and route coalesced-POLLOUT
  cleanup to the correct bounded operation.
- Preserve worker setup errors and panic payloads through launch rollback instead
  of collapsing them to a generic setup failure.

## Compatibility

The patch adds APIs without changing existing `send`, `send_await`, `with_data`,
or listener-startup behavior. Both io_uring and forced-Mio backends implement the same
logical-send contract. The future is worker-local by construction.

## Tests

- transactional multi-slot reservation under pool pressure;
- FIFO bounded sends and exact wire content with a one-slot pool;
- oversize rejection before any prefix reaches the wire;
- inert/unpolled registration and owner refresh after a future moves tasks;
- queued cancellation, submitted cancellation, stale completion isolation, and
  shutdown with a parked waiter;
- Mio partial write and half-close completion with permit return;
- io_uring negative coalesced-POLLOUT cleanup and operation identity;
- real TCP reset surfaced through `with_data_result`;
- worker bootstrap panic payload returned by `launch`;
- reported startup-channel error wins over a different joined-worker error.

## Verification

Standalone patch: `ringline-v0.5.5-runtime-followups.patch`

SHA-256: `a4b2c83d0f40fed074c038d1f53ce5c8f8e01ba6960602d09257bfa8f148f05f`

```console
cargo test --features force-mio --lib
cargo test --features force-mio with_data_result_surfaces_tcp_reset
cargo test --features force-mio backpressured_send
cargo test --no-default-features --features force-mio backpressured_send_refreshes_owner_after_first_poll_move
cargo test --no-default-features --features force-mio mio_half_close_completes_submitted_send_and_cancels_capacity_waiter
cargo test --lib coalesced_pollout_error_releases_resources_and_completes_bounded_operation
cargo clippy --all-targets -- -D warnings
cargo clippy --no-default-features --features force-mio --all-targets -- -D warnings
cargo check --all-targets
cargo check --no-default-features --features force-mio --all-targets
```
