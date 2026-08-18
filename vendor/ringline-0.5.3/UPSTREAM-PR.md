# Make listener startup transactional across worker initialization

## Problem

Ringline currently creates its listening socket and starts the acceptor before every worker has completed fallible backend initialization. A later worker failure can therefore occur after clients have completed handshakes, and queued `RawFd` values can be abandoned when their receiving worker fails.

## Lifecycle invariant

Every worker must complete fallible event-loop initialization before Ringline creates a listening socket. A failed launch must leave no listener or acceptor running, no worker threads running, and no launch-owned descriptors open.

## Implementation

- Split each backend event loop into a fallible `prepare_run` phase followed by `run`.
- Report worker readiness only after `prepare_run` succeeds.
- Retain only bind intent and worker senders during worker startup.
- After all workers report ready, bind and listen, then spawn the acceptor.
- Roll back worker, bind/listen, or acceptor-spawn failure by setting shutdown, waking and joining started workers, and closing any listener created during commit.
- Keep Mio worker read descriptors under RAII ownership until they transfer to a successfully constructed driver.

## Tests

- A client connection attempt fails while worker startup is blocked.
- The listener address can be rebound immediately after startup failure.
- Repeated startup failures do not grow `/proc/self/fd`.

## Compatibility

The change is internal to Ringline startup. It does not change the public API or client-only behavior, and it applies to both Mio and io_uring backends.

## Verification

Standalone patch: `ringline-v0.5.3-startup-transaction.patch`

SHA-256: `8299062416229d2018e697bb77e69b5e875c62111326700d6cc9a3127ae1d947`

Commands:

```console
cargo test startup_gate_tests --lib -- --test-threads=1
cargo test --features force-mio startup_gate_tests --lib -- --test-threads=1
cargo test --features force-mio --lib
cargo check --all-targets
cargo check --features force-mio --all-targets
```

The focused lifecycle tests pass on both backends, the force-Mio library suite passes 185/185, and both all-target checks pass. On the verification host, the default io_uring library suite reached 200 passing tests and 127 failures because io_uring setup returns `EINVAL`; those host capability failures are unrelated to this change.
