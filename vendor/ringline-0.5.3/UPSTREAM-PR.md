# Ringline 0.5.3 lifecycle and send-pressure follow-ups

## Provenance

The startup transaction is the code merged in ringline-rs/ringline#309; no crates.io release contains it yet. The same standalone patch now also carries two generic follow-ups for upstream review: atomic copy-send pool reservation and exact worker panic payload propagation. None contains Pelikan protocol or fallback policy.

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

## Tests

- A unique Unix listener path is absent while worker startup is blocked and reusable after failure.
- Repeated startup failures do not grow `/proc/self/fd`; the assertion runs in an isolated child test process.
- A pool too small for a multi-slot send leaves every slot free (no partial reservation/submission).
- A worker startup panic is returned with its original string payload.

## Compatibility

The change is internal to Ringline startup. It does not change the public API or client-only behavior, and it applies to both Mio and io_uring backends.

Listener errors now surface after worker initialization and rollback. This intentionally increases startup-failure latency. Errors from `run()` after the listener becomes live remain observable through worker join handles.

## Verification

Standalone patch: `ringline-v0.5.3-startup-transaction.patch`

SHA-256: `5fb1907bf2846defd493f524e30d6e134e5a41737ff5c5bb91e6b74c49f99a36`

Commands:

```console
cargo test startup_gate_tests --lib -- --test-threads=1
cargo test --features force-mio startup_gate_tests --lib -- --test-threads=1
cargo test --features force-mio chunk_reservation_is_transactional_under_pool_pressure
cargo test --features force-mio worker_startup_panic_payload_is_preserved
cargo test --features force-mio --lib
cargo clippy --all-targets -- -D warnings
cargo clippy --features force-mio --all-targets -- -D warnings
cargo check --all-targets
cargo check --features force-mio --all-targets
```

The focused lifecycle, pressure, and panic-detail tests pass with force-Mio. The startup transaction merged as ringline-rs/ringline#309; the two follow-up hunks above still need a separate upstream submission.
