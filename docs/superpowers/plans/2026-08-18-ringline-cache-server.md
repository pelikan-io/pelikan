# Ringline Cache-Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Ringline as a runtime-selectable Linux I/O backend for plain TCP cache servers while retaining mio as the default and startup fallback.

**Architecture:** `pelikan-net` owns backend resolution, Ringline launch, per-worker state injection, and same-thread async completion primitives. `pelikan-core-server` adds a Ringline process implementation that adapts existing `Protocol`, `Compose`, `Execute`, `EntryStore`, admin, and signal behavior; the existing mio path remains intact.

**Tech Stack:** Rust 2021, mio 1.x, ringline 0.5.3, io_uring on Linux, serde/TOML, crossbeam-channel, existing Pelikan protocol/session/storage crates.

**Spec:** `docs/superpowers/specs/2026-08-18-ringline-cache-server-design.md`

## Global Constraints

- Mio is the default backend and remains compiled on every supported platform.
- Ringline is selectable only for cache-server plain TCP traffic in this increment.
- Ringline-specific dependencies and modules are gated with `cfg(target_os = "linux")`.
- Selecting Ringline on an unsupported platform or when startup initialization fails falls back to mio before accepting traffic.
- Runtime failures after Ringline accepts traffic shut the process down; they never migrate live connections.
- Admin sockets, TLS, proxy frontend traffic, and proxy backend traffic remain on mio.
- Multi-worker storage stays single-owner on its dedicated thread.
- A Ringline worker must never perform a blocking receive while waiting for storage.
- Ringline task wakers are same-thread only; cross-thread storage completion must first notify the owning Ringline worker and wake the task from `on_notify`.
- The per-request Ringline path uses worker-local state and acquires no mutex or other blocking lock.
- Backend choice is resolved once at startup; no per-request backend dispatch or autonomous runtime adaptation is introduced.
- Every terminal error path propagates, increments a metric, or logs; error payloads are retained in waitless logs when a counter alone cannot explain the failure.
- Threading and request-path changes require regenerating source-anchored diagrams with `cargo xtask diagrams`.
- Pin Ringline to crate version `0.5.3`; do not depend on an unpinned Git branch.

---

## File Structure

- `src/net/src/backend.rs`: public requested/resolved backend types, fallback reasons, and resolver.
- `src/net/src/ringline/mod.rs`: Linux-only Ringline facade and re-exports used by core-server.
- `src/net/src/ringline/launch.rs`: guarded per-worker state slots and Ringline launch wrapper.
- `src/net/src/ringline/completion.rs`: same-thread task completion table used after worker notification.
- `src/core/server/src/ringline/mod.rs`: Ringline process builder, process handles, and handler wiring.
- `src/core/server/src/ringline/session.rs`: protocol parse/compose state independent of mio sockets.
- `src/core/server/src/ringline/single.rs`: single-worker handler with inline storage.
- `src/core/server/src/ringline/multi.rs`: connection workers, storage thread, response routing, and notifications.
- Existing `listener.rs` and `workers/` remain the mio implementation and are not structurally rewritten.

### Task 1: Typed Backend Configuration and Resolution

**Files:**
- Modify: `src/config/src/server.rs`
- Modify: `src/config/src/segcache.rs`
- Modify: `src/config/src/rds.rs`
- Modify: `src/config/src/pingserver.rs`
- Create: `src/net/src/backend.rs`
- Modify: `src/net/src/lib.rs`

**Interfaces:**
- Produces: `Server::io_backend(&self) -> &str`
- Produces: `IoBackend::parse(&str) -> Result<IoBackend, InvalidIoBackend>`
- Produces: `resolve_backend(requested: IoBackend, ringline_available: bool) -> BackendResolution`
- Produces: `BackendResolution { requested: IoBackend, active: IoBackend, fallback: Option<FallbackReason> }`

- [ ] **Step 1: Add failing configuration tests**

In `src/config/src/server.rs`, add tests proving a missing key defaults to mio, valid values round-trip, and an arbitrary value is retained for validation by `pelikan-net`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_backend_defaults_to_mio() {
        let server: Server = toml::from_str("").unwrap();
        assert_eq!(server.io_backend(), "mio");
    }

    #[test]
    fn io_backend_reads_ringline() {
        let server: Server = toml::from_str("io_backend = 'ringline'").unwrap();
        assert_eq!(server.io_backend(), "ringline");
    }
}
```

- [ ] **Step 2: Run the config test and verify failure**

Run: `cargo test -p config server::tests::io_backend -- --nocapture`

Expected: compilation fails because `Server::io_backend` does not exist.

- [ ] **Step 3: Add the server setting**

Add a default function and field without changing existing defaults:

```rust
const SERVER_IO_BACKEND: &str = "mio";

fn io_backend() -> String {
    SERVER_IO_BACKEND.to_string()
}

#[serde(default = "io_backend")]
io_backend: String,

pub fn io_backend(&self) -> &str {
    &self.io_backend
}
```

Update `Server::default()` to initialize `io_backend`. Extend the existing rendered-config assertions in Segcache, RDS, and Pingserver to require `io_backend = "mio"`.

- [ ] **Step 4: Add failing backend resolver tests**

Create `src/net/src/backend.rs` with tests for parsing, default resolution, supported Ringline selection, and fallback:

```rust
#[test]
fn parses_supported_names() {
    assert_eq!(IoBackend::parse("mio").unwrap(), IoBackend::Mio);
    assert_eq!(IoBackend::parse("ringline").unwrap(), IoBackend::Ringline);
    assert!(IoBackend::parse("other").is_err());
}

#[test]
fn resolves_ringline_when_available() {
    let resolution = resolve_backend(IoBackend::Ringline, true);
    assert_eq!(resolution.active, IoBackend::Ringline);
    assert_eq!(resolution.fallback, None);
}

#[test]
fn falls_back_when_ringline_is_unavailable() {
    let resolution = resolve_backend(IoBackend::Ringline, false);
    assert_eq!(resolution.active, IoBackend::Mio);
    assert_eq!(resolution.fallback, Some(FallbackReason::Unavailable));
}
```

- [ ] **Step 5: Implement backend types and exports**

Implement:

```rust
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoBackend {
    #[default]
    Mio,
    Ringline,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidIoBackend(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FallbackReason {
    Unavailable,
    Initialization(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendResolution {
    pub requested: IoBackend,
    pub active: IoBackend,
    pub fallback: Option<FallbackReason>,
}
```

Implement `Display` for user-facing logs and `std::error::Error` for `InvalidIoBackend`. Export these from `src/net/src/lib.rs`.

- [ ] **Step 6: Run focused tests**

Run: `cargo test -p config server::tests -p pelikan-net backend::tests -- --nocapture`

Expected: all new tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/config/src/server.rs src/config/src/segcache.rs src/config/src/rds.rs src/config/src/pingserver.rs src/net/src/backend.rs src/net/src/lib.rs
git commit -m "feat: add cache server io backend selection"
```

### Task 2: Ringline Dependency and Linux Runtime Facade

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/net/Cargo.toml`
- Create: `src/net/src/ringline/mod.rs`
- Create: `src/net/src/ringline/launch.rs`
- Modify: `src/net/src/lib.rs`
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: `IoBackend`, `FallbackReason`
- Produces: `RinglineRuntimeConfig { workers: usize, max_connections: u32, recv_buffers: u16, recv_buffer_size: u32, pin_to_core: bool }`
- Produces: `launch<A>(addr: SocketAddr, config: RinglineRuntimeConfig, handlers: Vec<A>) -> Result<RinglineRuntime, ringline::Error>`
- Produces: `RinglineRuntime::{shutdown(&self), bound_addr(&self), join(self)}`
- Produces: `take_worker_bootstrap<A: Send + 'static>(worker_id: usize) -> A` for `AsyncEventHandler::create_for_worker`

- [ ] **Step 1: Add target-gated dependency and compile smoke test**

Add to workspace dependencies:

```toml
ringline = "=0.5.3"
```

Add to `src/net/Cargo.toml`:

```toml
[target.'cfg(target_os = "linux")'.dependencies]
ringline = { workspace = true }
```

In `src/net/src/ringline/mod.rs`, add a compile test that constructs the minimum Ringline configuration:

```rust
#[test]
fn runtime_config_builds_ringline_config() {
    let config = RinglineRuntimeConfig {
        workers: 1,
        max_connections: 128,
        recv_buffers: 64,
        recv_buffer_size: 4096,
        pin_to_core: false,
    };
    assert!(config.build().is_ok());
}
```

- [ ] **Step 2: Verify the smoke test fails**

Run: `cargo test -p pelikan-net ringline::tests::runtime_config_builds_ringline_config -- --nocapture`

Expected: compilation fails because the module and configuration type do not exist.

- [ ] **Step 3: Implement runtime configuration mapping**

Implement `RinglineRuntimeConfig::build()` using:

```rust
ringline::ConfigBuilder::new()
    .workers(self.workers)
    .pin_to_core(self.pin_to_core)
    .max_connections(self.max_connections)
    .recv_buffer(self.recv_buffers, self.recv_buffer_size)
    .build()
```

Validate integer conversions before calling Ringline. Export the module only on Linux:

```rust
#[cfg(target_os = "linux")]
pub mod ringline;
```

- [ ] **Step 4: Add failing handler-slot tests**

Ringline 0.5.3 calls `A::create_for_worker(worker_id)` without accepting a handler factory. Add tests that preload distinct states, take them by worker id, reject a second concurrent launch, and clear slots after launch failure:

```rust
#[test]
fn bootstraps_are_taken_by_worker_id_once() {
    let guard = HandlerSlots::install(vec!["zero", "one"]).unwrap();
    assert_eq!(take_worker_bootstrap::<&'static str>(1), "one");
    assert_eq!(take_worker_bootstrap::<&'static str>(0), "zero");
    drop(guard);
}

#[test]
fn only_one_slot_set_can_be_installed() {
    let _guard = HandlerSlots::install(vec![1_u8]).unwrap();
    assert!(HandlerSlots::install(vec![2_u8]).is_err());
}
```

- [ ] **Step 5: Implement guarded type-erased handler slots**

Use one process-global `Mutex<Option<Vec<Option<Box<dyn Any + Send>>>>>`. `HandlerSlots::install` acquires the launch guard and stores exactly one bootstrap value per configured worker. `take_worker_bootstrap<A>` removes and downcasts the requested slot, with explicit panic messages for a missing id or type mismatch. The mutex is used only during startup and never on a request path. The guard clears slots on every drop path.

Document the constraint: Pelikan launches one cache-server runtime per process, and slots exist only during synchronous Ringline startup while `create_for_worker` runs.

- [ ] **Step 6: Add launch wrapper and runtime handle**

Implement `launch<A>` so it:

1. Rejects `handlers.len() != config.workers`.
2. Installs handler slots.
3. Builds Ringline configuration.
4. Calls `RinglineBuilder::new(config).bind(addr).launch::<A>()`.
5. Drops the slot guard after all Ringline workers report startup.
6. Wraps `ShutdownHandle` and worker joins in `RinglineRuntime`.

`RinglineRuntime::join` must join every worker and convert a worker panic to `io::ErrorKind::Other`. `shutdown` delegates to the exact shutdown method provided by Ringline 0.5.3 and remains idempotent.

- [ ] **Step 7: Run facade tests and Linux check**

Run: `cargo test -p pelikan-net ringline -- --nocapture`

Run: `cargo check -p pelikan-net --all-targets`

Expected: tests and compile checks pass.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock src/net/Cargo.toml src/net/src/lib.rs src/net/src/ringline
git commit -m "feat: add linux ringline runtime facade"
```

### Task 3: Same-Thread Async Completion Bridge

**Files:**
- Create: `src/net/src/ringline/completion.rs`
- Modify: `src/net/src/ringline/mod.rs`

**Interfaces:**
- Produces: `CompletionId { slot: u32, generation: u32 }`
- Produces: `CompletionTable<T>::insert() -> (CompletionId, Completion<T>)`
- Produces: `CompletionTable<T>::complete(id: CompletionId, value: T) -> Result<(), T>`
- Produces: `CompletionTable<T>::cancel(id: CompletionId) -> bool`
- Produces: `Completion<T>: Future<Output = Result<T, CompletionCanceled>>`

- [ ] **Step 1: Write failing completion tests**

Add tests using a counting `RawWaker`:

```rust
#[test]
fn pending_completion_is_woken_on_same_thread() {
    let table = CompletionTable::new();
    let (id, mut future) = table.insert();
    assert!(poll_once(&mut future).is_pending());
    table.complete(id, 42).unwrap();
    assert_eq!(poll_once(&mut future), Poll::Ready(Ok(42)));
}

#[test]
fn stale_generation_cannot_complete_reused_slot() {
    let table = CompletionTable::new();
    let (old_id, old_future) = table.insert();
    drop(old_future);
    let (new_id, mut new_future) = table.insert();
    assert_ne!(old_id.generation, new_id.generation);
    assert_eq!(table.complete(old_id, 1), Err(1));
    table.complete(new_id, 2).unwrap();
    assert_eq!(poll_once(&mut new_future), Poll::Ready(Ok(2)));
}

#[test]
fn dropping_future_cancels_slot() {
    let table = CompletionTable::<u8>::new();
    let (id, future) = table.insert();
    drop(future);
    assert_eq!(table.complete(id, 7), Err(7));
}
```

- [ ] **Step 2: Verify tests fail**

Run: `cargo test -p pelikan-net ringline::completion::tests -- --nocapture`

Expected: compilation fails because the completion types do not exist.

- [ ] **Step 3: Implement completion state machine**

Use `Rc<RefCell<Slab<Entry<T>>>>` because insertion, polling, completion, and waking all occur on one Ringline worker. Each entry stores its generation, optional value, optional `Waker`, and cancellation state. `Future::poll` replaces the stored waker only when `will_wake` is false. `complete` validates generation, stores the value, and calls the saved waker from the Ringline worker thread.

Do not make `CompletionTable` or `Completion` `Send`. Add module documentation that storage threads may not call `complete`; they enqueue raw responses and signal Ringline's `WakeHandle`, after which `on_notify` calls `complete`.

- [ ] **Step 4: Run completion tests**

Run: `cargo test -p pelikan-net ringline::completion::tests -- --nocapture`

Expected: all completion, cancellation, and stale-generation tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/net/src/ringline/completion.rs src/net/src/ringline/mod.rs
git commit -m "feat: add ringline task completion bridge"
```

### Task 4: Backend-Independent Protocol Session Adapter

**Files:**
- Create: `src/core/server/src/ringline/session.rs`
- Create: `src/core/server/src/ringline/mod.rs`
- Modify: `src/core/server/src/lib.rs`
- Modify: `src/core/server/Cargo.toml`

**Interfaces:**
- Consumes: `Protocol<Request, Response>`, `Compose`, `ParseOk`
- Produces: `RinglineSession<P, Request, Response>::new(protocol: P) -> Self`
- Produces: `parse(&mut self, data: &[u8]) -> io::Result<Parsed<Request>>`
- Produces: `compose(&mut self, response: &Response) -> &[u8]`
- Produces: `Parsed::{Complete { request, consumed }, NeedMore}`

- [ ] **Step 1: Add failing parser adapter tests**

Use a small test protocol implementing `Protocol<Vec<u8>, Vec<u8>>` and `Compose`. Cover complete, partial, pipelined, invalid, and response composition behavior:

```rust
#[test]
fn parses_one_frame_and_reports_consumed_bytes() {
    let mut session = RinglineSession::new(LineProtocol);
    let parsed = session.parse(b"one\ntwo\n").unwrap();
    assert_eq!(parsed.request(), b"one");
    assert_eq!(parsed.consumed(), 4);
}

#[test]
fn incomplete_frame_needs_more_data() {
    let mut session = RinglineSession::new(LineProtocol);
    assert!(matches!(session.parse(b"one"), Ok(Parsed::NeedMore)));
}

#[test]
fn composes_response_into_reused_buffer() {
    let mut session = RinglineSession::new(LineProtocol);
    assert_eq!(session.compose(&b"ok".to_vec()), b"ok");
}
```

- [ ] **Step 2: Verify adapter tests fail**

Run: `cargo test -p server ringline::session::tests -- --nocapture`

Expected: compilation fails because the Ringline session module does not exist.

- [ ] **Step 3: Implement adapter**

Map `io::ErrorKind::WouldBlock` from `Protocol::parse_request` to `Parsed::NeedMore`; preserve all other errors. Use `bytes::BytesMut` as the reusable composition buffer and call `Response::compose`. Record request-start timestamps and expose `response_completed(bytes)` so existing latency metrics can be updated after Ringline send completion.

Add `bytes = { workspace = true }` to core-server dependencies. Gate the module with `cfg(target_os = "linux")`.

- [ ] **Step 4: Run adapter and existing session tests**

Run: `cargo test -p server ringline::session::tests -- --nocapture`

Run: `cargo test -p session`

Expected: both suites pass.

- [ ] **Step 5: Commit**

```bash
git add src/core/server/Cargo.toml src/core/server/src/lib.rs src/core/server/src/ringline
git commit -m "feat: adapt pelikan protocols to ringline connections"
```

### Task 5: Single-Worker Ringline Cache Runtime

**Files:**
- Create: `src/core/server/src/ringline/single.rs`
- Modify: `src/core/server/src/ringline/mod.rs`
- Modify: `src/core/server/src/process.rs`
- Modify: `src/core/server/src/lib.rs`

**Interfaces:**
- Consumes: `take_worker_bootstrap`, `RinglineRuntimeConfig`, `RinglineSession`
- Produces: `SingleHandler<P, Request, Response, Storage>: ringline::AsyncEventHandler`
- Produces: `RinglineProcessBuilder::single(...) -> io::Result<Self>`
- Produces: process enum dispatch preserving `Process::{shutdown, wait}`

- [ ] **Step 1: Add failing handler unit tests**

Factor request execution into a synchronous method testable without a live driver:

```rust
#[test]
fn complete_request_executes_and_composes() {
    let handler = SingleHandler::for_test(LineProtocol, EchoStorage);
    let outcome = handler.process(b"hello\n").unwrap();
    assert_eq!(outcome.consumed, 6);
    assert_eq!(outcome.response, b"hello");
    assert!(!outcome.hangup);
}

#[test]
fn incomplete_request_does_not_execute() {
    let handler = SingleHandler::for_test(LineProtocol, CountingStorage::new());
    assert!(matches!(handler.process(b"hello").unwrap(), ProcessOutcome::NeedMore));
    assert_eq!(handler.execution_count(), 0);
}
```

- [ ] **Step 2: Verify handler tests fail**

Run: `cargo test -p server ringline::single::tests -- --nocapture`

Expected: compilation fails because `SingleHandler` does not exist.

- [ ] **Step 3: Implement the handler state and request loop**

Make `SingleHandler` a zero-sized `Send` value. In `create_for_worker`, take `WorkerBootstrap<P, Storage>` from the guarded startup slots and install it into a worker-thread-local, type-erased `RefCell<Option<Box<dyn Any>>>`. `on_accept` clones only the protocol into the connection future; synchronous request execution accesses storage through the worker-local cell. This is sound because Ringline polls every connection future on its owning worker thread and ensures storage never crosses threads or sits behind a mutex. The closure:

1. Parses at most one request.
2. Executes it against storage.
3. Calls `Klog::klog`.
4. Composes into the connection session's reusable buffer.
5. Returns `ringline::ParseResult::Consumed(n)` or `NeedMore`.

After the closure returns a complete response, call `conn.send(&bytes)?.await` so send-pool pressure yields rather than closing a healthy connection. Apply `should_hangup` after the response completion. No worker-local borrow may be held across `.await`.

Implement `create_for_worker(worker_id)` with `pelikan_net::ringline::take_worker_bootstrap(worker_id)` followed by worker-local installation. Add a test that recursively enters storage execution and verifies the adapter reports a controlled borrow error rather than deadlocking.

- [ ] **Step 4: Add failing process-selection tests**

Extract a pure selection function and test that mio is default, explicit Ringline selects the Ringline builder on Linux, and unsupported resolution selects mio:

```rust
#[test]
fn resolved_mio_builds_existing_process() {
    assert_eq!(process_kind(BackendResolution::mio()), ProcessKind::Mio);
}

#[cfg(target_os = "linux")]
#[test]
fn resolved_ringline_builds_ringline_process() {
    assert_eq!(process_kind(BackendResolution::ringline()), ProcessKind::Ringline);
}
```

- [ ] **Step 5: Refactor process ownership into backend variants**

Rename the current concrete builder/process internally to `MioProcessBuilder` and `MioProcess`, then expose compatibility enums under the existing public names:

```rust
pub enum ProcessBuilder<P, Request, Response, Storage> {
    Mio(MioProcessBuilder<P, Request, Response, Storage>),
    #[cfg(target_os = "linux")]
    Ringline(RinglineProcessBuilder<P, Request, Response, Storage>),
}

pub enum Process {
    Mio(MioProcess),
    #[cfg(target_os = "linux")]
    Ringline(RinglineProcess),
}
```

Keep `ProcessBuilder::new`, `version`, `spawn`, `Process::shutdown`, and `Process::wait` source-compatible. Parse `config.server().io_backend()` and log both requested and active backends.

- [ ] **Step 6: Implement Ringline startup and pre-accept fallback**

Build one `SingleHandler` and call the `pelikan-net` launch facade. If Ringline configuration or launch returns an initialization error, log the exact error and build the previously retained mio builder. Do not bind the mio listener before the Ringline attempt, because both would contend for the same address.

This requires delaying backend-specific listener construction until `spawn`; store backend-neutral address/config values in the outer builder. Keep admin initialization independent so only one admin socket is bound.

- [ ] **Step 7: Run single-worker tests and mio regression tests**

Run: `cargo test -p server ringline::single -- --nocapture`

Run: `cargo test -p pelikan-pingserver`

Run: `cargo test -p pelikan-rds --test integration -- --nocapture`

Run: `cargo test -p pelikan-segcache --test integration -- --nocapture`

Expected: new unit tests pass and existing mio integrations remain green.

- [ ] **Step 8: Commit**

```bash
git add src/core/server/src/lib.rs src/core/server/src/process.rs src/core/server/src/ringline
git commit -m "feat: run single-worker cache servers on ringline"
```

### Task 6: Multi-Worker Storage Bridge

**Files:**
- Create: `src/core/server/src/ringline/multi.rs`
- Modify: `src/core/server/src/ringline/mod.rs`
- Modify: `src/core/server/src/workers/storage.rs`
- Modify: `src/core/server/src/process.rs`

**Interfaces:**
- Consumes: `CompletionTable<ResponseEnvelope<Response>>`, Ringline `WakeHandle`
- Produces: `StorageRequest<Request> { worker_id, completion_id, request }`
- Produces: `StorageResponse<Response> { completion_id, request, response }`
- Produces: `MultiHandler<P, Request, Response>: AsyncEventHandler`
- Produces: `RinglineStorageWorker<Request, Response, Storage>`

- [ ] **Step 1: Add failing response-routing tests**

Test the worker-local notification path without sockets:

```rust
#[test]
fn notify_completes_only_matching_connection_future() {
    let mut handler = MultiHandler::for_test(LineProtocol);
    let (first_id, mut first) = handler.completions.insert();
    let (_second_id, mut second) = handler.completions.insert();
    handler.responses.push(StorageResponse::new(first_id, b"one".to_vec()));
    handler.drain_notifications();
    assert_eq!(poll_once(&mut first), Poll::Ready(Ok(b"one".to_vec())));
    assert!(poll_once(&mut second).is_pending());
}

#[test]
fn canceled_connection_discards_delayed_response() {
    let mut handler = MultiHandler::for_test(LineProtocol);
    let (id, future) = handler.completions.insert();
    drop(future);
    handler.responses.push(StorageResponse::new(id, b"late".to_vec()));
    assert_eq!(handler.drain_notifications(), DrainStats { delivered: 0, stale: 1 });
}
```

- [ ] **Step 2: Verify routing tests fail**

Run: `cargo test -p server ringline::multi::tests -- --nocapture`

Expected: compilation fails because multi-worker types do not exist.

- [ ] **Step 3: Implement bounded request and response queues**

Create one bounded crossbeam request queue into the storage thread and one bounded response queue per Ringline worker. Each storage response queue is paired with that Ringline worker's `WakeHandle` obtained from `RinglineRuntime`.

The storage loop uses blocking receive because it is a dedicated storage thread. For each request it executes storage, records expiration on its existing cadence, sends the response to the originating worker queue, and calls `WakeHandle::wake()`.

Never move a Ringline `Waker`, `Completion`, `Rc`, or `RefCell` to the storage thread.

- [ ] **Step 4: Implement async request flow and `on_notify`**

For each parsed request, `MultiHandler::on_accept` allocates a completion id, calls nonblocking `try_send`, and awaits the worker-local `Completion`. Queue saturation returns a connection-scoped overload error and increments the queue-pressure metric.

`MultiHandler` is also zero-sized and installs protocol, completion table, and response receiver into worker-thread-local state during `create_for_worker`. `MultiHandler::on_notify` drains only its worker's response queue and invokes `CompletionTable::complete` on the Ringline worker thread. This same-thread call safely wakes the suspended connection task without a data-plane lock.

Preserve request logging by returning the request with the response envelope, as the current multi-worker path does.

- [ ] **Step 5: Wire multi-worker construction and shutdown**

When `worker.threads() > 1`, prebuild one `MultiHandler` per Ringline worker and one `RinglineStorageWorker`. Launch Ringline, attach each returned worker wake handle to the matching response sender, and start the storage thread before reporting startup success.

Shutdown stops acceptance, notifies every Ringline worker, sends `Signal::Shutdown` to storage, joins Ringline workers, and joins storage. `Signal::FlushAll` is routed only to storage.

- [ ] **Step 6: Run bridge and existing multi-worker tests**

Run: `cargo test -p server ringline::multi -- --nocapture`

Run: `cargo test -p pelikan-rds --test integration_multi -- --nocapture`

Run: `cargo test -p pelikan-segcache --test integration_multi -- --nocapture`

Expected: routing tests and current mio multi-worker integrations pass.

- [ ] **Step 7: Commit**

```bash
git add src/core/server/src/process.rs src/core/server/src/ringline src/core/server/src/workers/storage.rs
git commit -m "feat: bridge ringline workers to cache storage"
```

### Task 7: Ringline Integration Conformance and Fallback Observability

**Files:**
- Modify: `src/server/rds/tests/common.rs`
- Modify: `src/server/rds/tests/integration.rs`
- Modify: `src/server/rds/tests/integration_multi.rs`
- Modify: `src/server/segcache/tests/common.rs`
- Modify: `src/server/segcache/tests/integration.rs`
- Modify: `src/server/segcache/tests/integration_multi.rs`
- Modify: `src/core/server/src/lib.rs`
- Modify: `src/core/server/src/process.rs`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `scripts/gen-threading-diagram.py`
- Modify: `scripts/gen-request-dataflow-diagram.py`
- Modify: generated files under `docs/diagrams/` produced by `cargo xtask diagrams`
- Modify: `.github/workflows/cargo.yml`

**Interfaces:**
- Consumes: unchanged cache-server public construction API and `server.io_backend`
- Produces: backend-parameterized integration harnesses
- Produces: `SERVER_IO_BACKEND_ACTIVE` and `SERVER_IO_BACKEND_FALLBACK` metrics

- [ ] **Step 1: Parameterize existing integration harnesses**

Add a helper that sets the selected backend before spawning:

```rust
fn config_for_backend(backend: &str, threads: usize) -> TestConfig {
    let mut config = TestConfig::default();
    config.server_mut().set_io_backend(backend);
    config.worker_mut().set_threads(threads);
    config
}
```

Add `ServerConfig::server_mut` and `Server::set_io_backend` for programmatic tests. Retain existing mio tests under their current names and add Linux-only Ringline variants. Each fallback or skipped capability path must assert and print the terminal error reason; unsupported capability is the only accepted skip class.

- [ ] **Step 2: Add Ringline conformance cases**

Run the existing protocol exchange suites for `ringline` with one and two workers. Add focused cases for:

```rust
#[test]
fn partial_request_is_completed_after_second_write() {
    exchange_fragments(&[b"get ", b"missing\r\n"], b"$-1\r\n");
}

#[test]
fn pipelined_requests_preserve_response_order() {
    exchange_once(
        b"set first one\r\nset second two\r\nget first\r\nget second\r\n",
        b"+OK\r\n+OK\r\n$3\r\none\r\n$3\r\ntwo\r\n",
    );
}

#[test]
fn graceful_shutdown_closes_listener_and_joins_workers() {
    let started = Instant::now();
    server.shutdown();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(TcpStream::connect(server_addr).is_err());
}

#[test]
fn connection_burst_does_not_block_existing_clients() {
    let existing = connected_client(server_addr);
    let burst: Vec<_> = (0..128).map(|_| connected_client(server_addr)).collect();
    assert_eq!(exchange(&existing, b"get missing\r\n"), b"$-1\r\n");
    drop(burst);
}

#[test]
fn repeated_startup_shutdown_releases_runtime_resources() {
    for _ in 0..8 {
        let (server, addr) = spawn_on_ephemeral_port("ringline");
        assert_eq!(exchange(&connected_client(addr), b"get missing\r\n"), b"$-1\r\n");
        server.shutdown();
    }
}
```

Implement `exchange_fragments`, `exchange_once`, `exchange`,
`connected_client`, and `spawn_on_ephemeral_port` in each protocol harness
using `TcpStream::{set_read_timeout, set_write_timeout, write_all, read_exact}`.
Use protocol-native requests and expected responses in the Segcache harness;
do not share RESP wire literals with Memcache tests.

Use the harness's existing socket exchange helpers and explicit timeouts. If Ringline reports unsupported kernel capabilities, print one skip reason and return before assertions; do not catch protocol or runtime failures as skips.

- [ ] **Step 3: Add backend metrics and fallback tests**

Define an active-backend gauge with `0 = mio`, `1 = ringline` and a fallback counter. Test the pure metric update function:

```rust
#[test]
fn fallback_records_mio_active_and_increments_reason() {
    record_resolution(&BackendResolution {
        requested: IoBackend::Ringline,
        active: IoBackend::Mio,
        fallback: Some(FallbackReason::Unavailable),
    });
    assert_eq!(SERVER_IO_BACKEND_ACTIVE.value(), 0);
    assert_eq!(SERVER_IO_BACKEND_FALLBACK.value(), 1);
}
```

Log `requested_backend`, `active_backend`, and fallback cause in one structured startup event.

- [ ] **Step 4: Regenerate source-anchored runtime diagrams**

Update the diagram generators and their source assertions to show the launch-time mio/Ringline split, Ringline acceptor and worker threads, and the unchanged admin/storage plane. Update `docs/ARCHITECTURE.md` so the prose describes backend selection without implying runtime adaptation.

Run: `cargo xtask diagrams`

Run: `git diff --exit-code -- docs/diagrams` immediately after a second `cargo xtask diagrams` invocation.

Expected: the first run updates generated artifacts; the second run is deterministic and produces no diff.

- [ ] **Step 5: Add CI compile coverage**

In the actual Rust CI workflow, add a Linux job or step that runs:

```bash
cargo check --workspace --all-targets
cargo test -p pelikan-net
cargo test -p server
```

Keep Ringline runtime integration execution conditional on the host capability probe used by the tests.

- [ ] **Step 6: Run focused conformance tests**

Run: `cargo test -p pelikan-rds --test integration -- --nocapture`

Run: `cargo test -p pelikan-rds --test integration_multi -- --nocapture`

Run: `cargo test -p pelikan-segcache --test integration -- --nocapture`

Run: `cargo test -p pelikan-segcache --test integration_multi -- --nocapture`

Expected: mio passes everywhere; Ringline passes on a capable Linux host or reports a single capability-specific skip.

- [ ] **Step 7: Commit**

```bash
git add src/config/src/server.rs src/core/server/src/lib.rs src/core/server/src/process.rs src/server/rds/tests src/server/segcache/tests docs/ARCHITECTURE.md docs/diagrams scripts .github/workflows
git commit -m "test: cover ringline cache server conformance"
```

### Task 8: Full Verification and Documentation

**Files:**
- Modify: `README.md` or the existing operator configuration document selected during execution
- Modify: `docs/superpowers/specs/2026-08-18-ringline-cache-server-design.md` only if implementation facts require a correction

**Interfaces:**
- Consumes: completed Ringline cache-server backend
- Produces: operator-facing configuration, platform requirements, fallback behavior, and scope statement

- [ ] **Step 1: Document configuration and boundaries**

Add a configuration example:

```toml
[server]
io_backend = "ringline" # Linux only; defaults and falls back to "mio"
```

State the exact Ringline version, Linux/kernel requirements reported by Ringline 0.5.3, startup-only fallback behavior, and that TLS/admin/proxy remain on mio.

- [ ] **Step 2: Run formatting and lint checks**

Run: `cargo fmt --all -- --check`

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: both commands exit successfully.

- [ ] **Step 3: Run the full test suite**

Run: `cargo test --workspace`

Expected: all tests pass, with only capability-specific Ringline integration skips on unsupported hosts.

- [ ] **Step 4: Verify portable compilation**

Run an available non-Linux target check, preferring:

```bash
cargo check --workspace --target x86_64-apple-darwin
```

If that target is not installed, run `cargo metadata --no-deps` plus a Linux build with Ringline modules disabled through the target cfg test harness, and record that cross-target compilation still requires CI confirmation.

- [ ] **Step 5: Review diff and dependency graph**

Run:

```bash
git diff --check main...HEAD
cargo tree -p pelikan-net
git status --short
```

Confirm Ringline appears only in the Linux target dependency graph, mio remains present, no proxy code changed, and no generated artifacts are tracked.

- [ ] **Step 6: Commit documentation**

```bash
git add README.md docs Cargo.lock
git commit -m "docs: describe ringline cache server backend"
```

- [ ] **Step 7: Request final code review**

Use `superpowers:requesting-code-review` against `main...feature/ringline-cache-io`, address findings, and rerun every verification command affected by changes.
