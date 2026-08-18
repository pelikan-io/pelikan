# Make listener startup transactional across worker initialization

## Problem

Ringline currently starts the acceptor before every worker has completed fallible backend initialization. A later worker failure can therefore occur after connections have been accepted, and queued `RawFd` values can be abandoned when their receiving worker fails.

## Lifecycle invariant

Every worker must complete fallible event-loop initialization before Ringline performs the first `accept`. A failed launch must leave no acceptor running, no worker threads running, and no launch-owned descriptors open.

## Implementation

- Split each backend event loop into a fallible `prepare_run` phase followed by `run`.
- Report worker readiness only after `prepare_run` succeeds.
- Bind and retain a pending acceptor during launch, then spawn it only after all workers report ready.
- Roll back partial startup by setting shutdown, waking and joining started workers, and closing the listener.
- Keep Mio worker read descriptors under RAII ownership until they transfer to a successfully constructed driver.

## Tests

- A worker startup failure cannot queue an accepted connection.
- The listener address can be rebound immediately after startup failure.
- Repeated startup failures do not grow `/proc/self/fd`.

## Compatibility

The change is internal to Ringline startup. It does not change the public API or client-only behavior, and it applies to both Mio and io_uring backends.

## Verification

Standalone patch: `ringline-v0.5.3-startup-transaction.patch`

SHA-256: `294e39560a38976d56e9f967eb39efc0cdc47a2ee61f40553c4827912997cfa6`

Commands:

```console
cargo test worker_startup_failure --lib -- --test-threads=1
cargo test --features force-mio worker_startup_failure --lib -- --test-threads=1
cargo test --features force-mio --lib
cargo check --all-targets
cargo check --features force-mio --all-targets
```

The focused lifecycle tests pass on both backends, the force-Mio library suite passes 185/185, and both all-target checks pass. On the verification host, the default io_uring library suite reached 200 passing tests and 127 failures because io_uring setup returns `EINVAL`; those host capability failures are unrelated to this change.
