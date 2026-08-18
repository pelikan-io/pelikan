# Ringline Cache-Server Network I/O Design

## Summary

Add Ringline as an optional Linux network I/O backend for Pelikan cache
servers. `mio` remains the default backend and the portable fallback.
`pelikan-net` owns backend selection and the backend-specific runtime facade;
`pelikan-core-server` supplies the protocol and storage adapter used by either
runtime.

The first increment supports plain inbound TCP cache-server traffic. Admin,
proxy, outbound proxy connections, and TLS continue to use mio. TLS support for
the Ringline cache-server path is a follow-up increment.

## Motivation

Pelikan currently implements readiness-driven network loops directly with
`mio::Poll`. Ringline provides a completion-driven, thread-per-core async
runtime based on io_uring. It can use Linux-specific facilities such as
multishot receive, provided buffers, and zero-copy sends, but requires a recent
Linux kernel and has a different scheduling and ownership model from mio.

Ringline is not treated as a replacement for mio. Pelikan must retain mio for
non-Linux systems and as a safe default on Linux.

## Scope

In scope:

- A cache-server configuration option selecting `mio` or `ringline`.
- Mio as the default and fallback backend.
- A backend boundary in `pelikan-net`.
- Plain TCP for pingserver, RDS, and Segcache through the shared core server.
- Single-worker and multi-worker storage topologies.
- Existing protocol parsing, response composition, metrics, signals, and
  graceful shutdown semantics.
- Shared behavioral tests for both backends.

Out of scope:

- Proxy frontend or backend traffic.
- Admin network I/O migration.
- Ringline TLS support in this increment.
- Runtime migration of established connections between backends.
- Making Pelikan's application code generally async.

## Architecture

### Backend boundary

`pelikan-net` exposes an `IoBackend` selection type and backend resolution. It
continues to expose the current mio-backed socket, event, and waker API without
breaking existing callers. On supported Linux builds it additionally exposes a
small Ringline server-runtime facade that hides Ringline configuration,
startup, connection handles, notification primitives, and shutdown handles.

The abstraction is above raw readiness events. Ringline must not emulate
`mio::Poll`, `Registry`, `Event`, or `Source`: doing so would impose a readiness
state machine on a completion runtime and complicate buffer ownership without
preserving Ringline's benefits.

Backend selection is bound once during process startup. There is no per-request
backend dispatch or in-process adaptation. This keeps each deployed instance
monomorphic and characterizable while allowing the operator to select a tested
configuration, following P17 and P18 of `docs/PRINCIPLES.md`.

`pelikan-core-server::ProcessBuilder` resolves the configured backend and owns
one of two internal cache-server process implementations:

- The mio implementation is the current listener, session queue, worker, and
  optional storage-worker pipeline.
- The Ringline implementation uses a cache-server handler adapter to connect
  Ringline's per-connection tasks to Pelikan's protocol, request, response, and
  storage traits.

Admin and OS signal handling continue to use the current implementation. The
Ringline process implementation connects those signals to the Ringline facade's
shutdown and storage-control mechanisms.

### Build and platform boundaries

The Ringline dependency and implementation are feature- and target-gated.
Non-Linux targets do not compile Ringline-specific code. Public selection and
resolution types remain available so the same configuration can be deployed on
multiple platforms.

The mio implementation remains present in Linux binaries so initialization can
fall back before traffic is accepted.

## Scheduling Model

Both backends must keep an I/O worker from blocking while it waits for storage
or network progress, but they express the invariant differently.

The mio implementation is an explicit callback/state machine:

1. `Poll` reports readiness or a queue waker event.
2. Pelikan looks up a session by token and performs bounded work.
3. Pending state is retained in session and queue structures.
4. Control returns to `Poll` until the next event advances the session.

The Ringline implementation uses async task suspension:

1. Each accepted connection runs as a task.
2. The task parses and submits a request.
3. Awaiting network or multi-worker storage completion yields the task.
4. Ringline runs other ready tasks on the same worker thread.
5. Completion wakes the suspended task, which resumes composing or sending.

A blocking channel receive inside a Ringline worker is prohibited because it
would stall all connection tasks assigned to that thread. Inline cache
execution in single-worker mode remains synchronous, matching current behavior.

## Connection and Storage Data Flow

### Per-connection state

Each Ringline connection task owns its Pelikan parsing state. Receive data is
offered incrementally to the protocol adapter. Incomplete input remains
associated with that connection, and pipelined complete requests can be
processed without waiting for another receive completion.

Ringline-owned receive buffers do not escape the connection task or callback.
Requests must satisfy the same ownership guarantees as requests produced by the
current `Session` implementation before they can be queued or executed.

Responses use the existing `Compose` behavior, with output adapted to
Ringline's send API. Hangup responses are sent when possible and then close the
connection, matching the mio path.

### Single-worker mode

The single Ringline worker owns the storage value. A connection task parses a
request, executes it synchronously against worker-owned storage, records the
request log, composes the response, and submits the send. Storage expiration is
driven on that worker at the same effective cadence as the existing loop.

### Multi-worker mode

Pelikan's storage value remains owned by one dedicated storage thread. Ringline
workers submit `(request, connection identity)` messages through bounded
queues. A connection awaiting its response yields rather than blocking the
worker.

The `pelikan-net` Ringline facade provides the async side of this bridge. A
pending response registers a task waker in worker-local state. The storage
thread enqueues the response and signals Ringline's worker notification file
descriptor; the worker's `on_notify` callback then makes the response available
and wakes the corresponding task on the owning worker thread. Queue capacity
remains bounded and queue saturation applies explicit backpressure or a
connection-scoped error. No blocking receive is performed on a Ringline worker,
and no lock is acquired on the per-request data path.

Connection identities include generation or equivalent stale-detection data so
a delayed response cannot be delivered to a reused connection slot.

## Configuration and Fallback

Cache-server configuration adds an I/O backend field accepting `mio` and
`ringline`. The default is `mio`; existing configurations are unchanged.

When Ringline is requested:

- A supported Linux build attempts to construct the Ringline runtime before
  accepting traffic.
- Unsupported kernel capabilities or initialization failures are logged with
  their cause and select mio instead.
- A non-Linux build logs that Ringline is unavailable and selects mio.
- The requested and resolved backend are observable through startup logs and an
  admin metric or info field.

Fallback occurs only during startup. A failure after Ringline begins serving
causes orderly process shutdown and a reported error. Pelikan does not attempt
to migrate live connections to mio.

## Error Handling and Shutdown

Ordinary parse failures, client disconnects, send failures, queue pressure, and
invalid delayed completions remain connection-scoped. They close or reject the
affected connection and update metrics equivalent to the mio path.

Unrecoverable Ringline worker or driver failures are process-scoped. They stop
acceptance, request graceful shutdown, and surface the failure rather than
silently continuing with fewer workers.

On shutdown, the admin signal path asks the Ringline facade to stop accepting
new connections, wake suspended tasks, close or drain active connections
according to Ringline's supported semantics, join Ringline workers, and then
join the unchanged admin and storage threads. `FlushAll` continues to execute
on the sole storage owner.

## Metrics and Observability

Existing request, response, connection, error, and queue metrics retain their
meaning across both backends. Backend-specific runtime metrics may be added
with distinct names, but should not replace backend-independent operational
counters.

Every terminal error branch must propagate, increment a counter, or emit a log;
errors with a useful reason both increment the class counter and retain the
reason in a waitless log. This applies the P27-P29 observability contract
without putting blocking observability on the data plane.

Startup logs and admin output expose both the configured backend and the active
backend, including fallback reason when applicable.

## Testing

Unit tests cover:

- Configuration parsing and mio default selection.
- Linux and non-Linux backend resolution.
- Initialization failure classification and fallback.
- The bounded async storage-response bridge, including wakeup, saturation,
  cancellation, and stale connection identities.

Shared cache-server conformance tests run equivalent exchanges against both
backends:

- Partial and pipelined requests.
- Large requests and responses.
- Client disconnects and hangup responses.
- Queue pressure and delayed storage responses.
- `FlushAll` and graceful shutdown.
- Single-worker and multi-worker configurations.
- Equivalent core metrics.
- Connection-establishment bursts and repeated startup/shutdown cycles.

Existing mio tests remain unchanged and passing. Ringline runtime tests execute
only on Linux hosts with required kernel capabilities; absence of those
capabilities is a reported skip, not a false pass. Linux compile checks still
cover Ringline code where runtime support is unavailable.

Mio and Ringline benchmarks are recorded separately. Performance is evaluated
as evidence for later rollout decisions, not as an acceptance gate for the
initial integration.

Because this changes the runtime thread model, the generated threading and
request-dataflow diagrams and their source assertions must be updated with
`cargo xtask diagrams`; generated SVGs are never edited by hand.

## Follow-up: TLS

After plain TCP conformance is established, a separate increment extends the
Ringline facade with TLS configuration and adds the same shared cache-server
tests over TLS. Mio remains the TLS implementation on unsupported systems.
Proxy integration remains out of scope until separately designed.
