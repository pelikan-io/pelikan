# Ringline 0.5.3 lifecycle, backpressure, and receive-error follow-ups

## Provenance

The startup transaction is the code merged in ringline-rs/ringline#309; no crates.io release contains it yet. The same standalone patch carries generic follow-ups for upstream review: atomic copy-send reservation, exact worker panic payload propagation, FIFO async send-capacity backpressure, and result-aware transport receive errors. None contains Pelikan protocol or fallback policy.

## Problem

Ringline currently creates its listening socket and starts the acceptor before every worker has completed fallible backend initialization. A later worker failure can therefore occur after clients have completed handshakes, and queued `RawFd` values can be abandoned when their receiving worker fails.

## Lifecycle invariant

Every worker must complete fallible event-loop initialization before Ringline creates a listening socket. A failed launch must leave no listener or acceptor running, no worker threads running, and no launch-owned descriptors open.

## Implementation

- Split each backend event loop into a fallible `prepare_run` phase followed by `run`.
- Move the event loop into its final binding before io_uring queues an SQE that references its inline eventfd buffer.
- Report worker readiness only after `prepare_run` succeeds.
- Retain only bind intent and worker senders during worker startup.
- After all workers report ready, bind and listen, then spawn the acceptor.
- Roll back worker, bind/listen, or acceptor-spawn failure by setting shutdown, waking and joining started workers, and closing any listener created during commit.
- Keep Mio worker read descriptors in `OwnedFd` until they transfer to a successfully constructed driver; io_uring ownership remains with `WakeHandle`.
- Reserve every copy-pool slot for a logical multi-chunk send before submitting its first SQE, so pressure cannot commit a truncated prefix.
- Carry worker setup errors and caught panic text through the startup channel and rollback result.
- Add `ConnCtx::send_backpressured`, whose construction is inert and whose first poll registers the actual polling task in a worker-local FIFO. Its owned registration token unregisters without driver TLS, waits for enough configured pool slots, and rejects over-capacity buffers before submission.
- Assign each admitted bounded send a unique logical-operation ID and carry it through Mio pending writes and io_uring pool/slab submission. Completion and abandonment are ID-scoped, so a canceled submitted send cannot resolve or clear a newer send on the same connection.
- Retain Mio permits through the final socket write. Mio half-close completes each submitted bounded send with its exact write result, fails capacity-only waiters, returns permits, and wakes the next FIFO head.
- Wake bounded senders on Mio permit release and io_uring CQ/SQ progress; connection teardown completes or abandons each exact logical operation and wakes the next FIFO head.
- Add `ConnCtx::with_data_result`, preserving exact non-`WouldBlock` TCP receive errors while retaining `with_data` clean-EOF compatibility.

## Tests

- A unique Unix listener path is absent while worker startup is blocked and reusable after failure.
- Repeated startup failures do not grow `/proc/self/fd`; the assertion runs in an isolated child test process.
- A pool too small for a multi-slot send leaves every slot free (no partial reservation/submission).
- A worker startup panic is returned with its original string payload.
- Two real bounded sends contend for a one-slot Mio pool and arrive exactly once in FIFO order; an oversize send writes no prefix, and shutdown drops a demonstrably parked waiter without hanging.
- Constructing or moving an unpolled future does not register a FIFO position; registration captures the first polling task. Dropping a queued registration without driver TLS unregisters only that waiter and advances FIFO safely.
- Canceling a submitted send before completion cannot donate its length/error to the next logical send on the connection.
- Mio half-close completes a submitted bounded send, fails a capacity waiter, releases its permits, and leaves neither future stuck.
- A real TCP reset is surfaced by `with_data_result`.

## Compatibility

The startup change remains internal. The follow-up adds compatible `send_backpressured` and `with_data_result` APIs without changing existing `send` or `with_data` behavior, and its plain-TCP paths apply to both Mio and io_uring backends.

Listener errors now surface after worker initialization and rollback. This intentionally increases startup-failure latency. Errors from `run()` after the listener becomes live remain observable through worker join handles.

## Verification

Standalone patch: `ringline-v0.5.3-startup-transaction.patch`

SHA-256: `a7eae83333e5b2a8f73336ea554a2647e56ba2d7a9d456a47b1b0308dfb8a036`

Commands:

```console
cargo test startup_gate_tests --lib -- --test-threads=1
cargo test --features force-mio startup_gate_tests --lib -- --test-threads=1
cargo test --features force-mio chunk_reservation_is_transactional_under_pool_pressure
cargo test --features force-mio worker_startup_panic_payload_is_preserved
cargo test --features force-mio backpressured_send
cargo test --features force-mio with_data_result_surfaces_tcp_reset
cargo test --features force-mio shutdown_drops_parked_backpressured_send_without_hanging
cargo test --no-default-features --features force-mio
cargo test --no-default-features --features force-mio mio_half_close_completes_submitted_send_and_cancels_capacity_waiter
cargo clippy --all-targets -- -D warnings
cargo clippy --no-default-features --features force-mio --all-targets -- -D warnings
cargo check --all-targets
cargo check --no-default-features --features force-mio --all-targets
```

The lifecycle, bounded-send, receive-error, and panic-detail tests pass with force-Mio. Default io_uring targets compile and pass warning-denied clippy; the host used for this patch cannot execute native io_uring. The startup transaction merged as ringline-rs/ringline#309; the generic follow-ups above still need a separate upstream submission.
