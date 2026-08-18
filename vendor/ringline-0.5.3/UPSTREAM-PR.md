# Make listener startup transactional across worker initialization

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

## Tests

- A unique Unix listener path is absent while worker startup is blocked and reusable after failure.
- Repeated startup failures do not grow `/proc/self/fd`; the assertion runs in an isolated child test process.

## Compatibility

The change is internal to Ringline startup. It does not change the public API or client-only behavior, and it applies to both Mio and io_uring backends.

Listener errors now surface after worker initialization and rollback. This intentionally increases startup-failure latency. Errors from `run()` after the listener becomes live remain observable through worker join handles.

## Verification

Standalone patch: `ringline-v0.5.3-startup-transaction.patch`

SHA-256: `2a0eb83566c9599821d292ac5febcc77c1bcbed350601c7fc54da2911be36166`

Commands:

```console
cargo test startup_gate_tests --lib -- --test-threads=1
cargo test --features force-mio startup_gate_tests --lib -- --test-threads=1
cargo test --features force-mio --lib
cargo clippy --all-targets -- -D warnings
cargo clippy --features force-mio --all-targets -- -D warnings
cargo check --all-targets
cargo check --features force-mio --all-targets
```

The focused lifecycle tests pass repeatedly on both backends, the force-Mio library suite passes, and both all-target and strict Clippy checks pass. The reviewed upstream change is open as ringline-rs/ringline#309.
