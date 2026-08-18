# Concurrent Segcache Conversion — Design

**Date:** 2026-08-18
**Status:** Approved

## Goal

Convert pelikan-segcache (and, by shared infrastructure, pelikan-rds) to the new
concurrent segcache engine from [cache-rs](https://github.com/pelikan-io/cache-rs),
and rearchitect `core/server` so worker threads share the cache directly instead of
routing requests through a dedicated storage thread.

## Background

Pelikan currently depends on `segcache 0.3.0` from crates.io, which has a
single-threaded `&mut self` API. Because the engine could not be shared across
threads, `core/server` grew two thread models:

- **Single-worker:** one worker owns the storage thread-locally and executes
  requests in place.
- **Multi-worker:** a dedicated storage thread owns the cache; workers parse
  requests, ship them over queues to the storage thread, and receive responses
  back — two queue hops and a cross-thread wakeup per request.

The cache-rs segcache (main branch, concurrency roadmap PRs #29–#41) is now safe
for concurrent use: a lock-free N-choice hashtable, per-segment claim/drain
protocol for writers and eviction, seqlocked in-place numeric operations, and an
Arc-shareable `&self` API that is `Sync`. `get`, `insert`, `cas`, `delete`,
`expire`, `clear`, `wrapping_add`, and `saturating_sub` all take `&self`.

This removes the reason the storage thread exists.

## Decisions

1. **Full conversion.** Adapt to the new API and change `core/server` so workers
   share `Arc<Storage>` and execute requests in place. Not just a dependency swap.
2. **Replace the storage-thread model entirely.** Delete the queue-based multi
   model; segcache is the only real storage in entrystore, so the old path has no
   remaining user.
3. **One unified worker model.** The single-worker special case collapses too;
   single-worker is just N=1 of the shared model.
4. **Dedicated maintenance thread** drives `expire()` and handles
   `FlushAll`/`Shutdown` signals. Keeps eager expiration off the request path.
5. **Publish `segcache 0.4.0` first.** The `&self` API is a breaking change;
   pelikan depends on the published crate, keeping pelikan's crates publishable
   and CI reproducible.

## Design

### 1. Prerequisite: segcache 0.4.0 release (cache-rs repo)

- Bump the segcache crate version to 0.4.0 on cache-rs main and publish to
  crates.io. All concurrency code is already pushed to origin/main; only docs
  are local-only. `keyvalue 0.3.0` is already published.
- Pelikan workspace: `segcache = "0.4.0"`.
- Remove the stray untracked `src/storage/segcache/` directory (leftover
  Cargo.lock only).

API deltas pelikan must absorb:

| Method | 0.3.0 (published) | 0.4.0 (concurrent) |
|---|---|---|
| `get`, `get_no_freq_incr` | `&mut self -> Option<Item>` | `&self -> Option<Item>` (Item RAII-pins its segment) |
| `insert`, `cas` | `&mut self -> Result<(), _>` | `&self -> Result<(), _>` |
| `delete` | `&mut self -> bool` | `&self -> bool` |
| `wrapping_add`, `saturating_sub` | `&mut self -> Result<Item, _>` | `&self -> Result<u64, _>` (returns new value) |
| `expire`, `clear` | `&mut self` | `&self -> usize` (segments freed) |

`Item` still exposes `key()`, `value()`, `cas()`, `optional()`; the `Value`
enum (`Bytes`/`U64`) is unchanged. The entrystore `debug` feature continues to
map onto segcache's `debug` metafeature.

### 2. Trait conversion: `&mut self` → `&self`

Changed in place — no parallel "shared" trait variants:

- `protocol_common::Execute::execute(&self, request) -> Response`
- `entrystore::EntryStore::{expire, clear}(&self)`
- `protocol_memcache::Storage` — all methods take `&self`
- `protocol_resp::Storage` — all methods take `&self`

Implementors:

- `entrystore::Seg` (memcache + resp impls): mostly mechanical. `incr`/`decr`
  simplify — the new numeric ops return the new value directly instead of an
  `Item` to unpack.
- `entrystore::Noop` (pingserver): trivial.
- `core/proxy` imports `Execute` but has no call sites; no functional change.

Responses copy item data during composition, so segment pins are request-scoped
and never outlive `execute()`.

### 3. Unified worker model (`core/server`)

- Delete `workers/single.rs`, `workers/multi.rs`, `workers/storage.rs`, and the
  `Workers`/`WorkersBuilder` enums.
- One `Worker` type, modeled on today's `SingleWorker` event loop
  (fill session → parse → `storage.execute(&request)` → compose/flush), holding
  `Arc<Storage>` with bounds `Storage: 'static + Execute<Request, Response> +
  EntryStore + Send + Sync`.
- `worker.threads = N` spawns N workers, each with an `Arc` clone. No config
  format changes; `threads = 1` is the same model with one worker.
- Deleted along with the storage thread: the worker↔storage data queues, the
  `QUEUE_RETRIES` response shuttle, and the `storage_event_loop` /
  `storage_queue_depth` metrics. Worker metrics are unchanged.
- Workers no longer call `expire()` in their event loop and ignore
  `Signal::FlushAll` (the maintenance thread owns both); they still act on
  `Signal::Shutdown`.

### 4. Maintenance thread

New `workers/maintenance.rs` (thread name `pelikan_maint`):

- Owns an `Arc<Storage>` clone plus its own Poll/Waker registered with the
  admin signal fan-out.
- Loop: poll with timeout → `storage.expire()` each pass → drain signal queue:
  `FlushAll` → `storage.clear()`, `Shutdown` → return.
- Expiration cadence matches today's storage thread (every loop pass, bounded
  by the poll timeout).
- Net thread count: unchanged in multi-worker mode (maintenance replaces
  storage); +1 thread in single-worker mode.

### 5. Process wiring

`process.rs`:

- Signal queues fan out to listener + maintenance + workers.
- Session queues go from listener to workers only.
- Maintenance is spawned alongside workers; shutdown joins it like any other
  worker thread.

### 6. Documentation

- Rewrite the thread-model doc comment in `core/server/src/lib.rs` (the two
  ASCII diagrams become one).
- Update `docs/ARCHITECTURE.md` and regenerate the diagram set via
  `cargo xtask diagrams` (architecture-diagram skill): the storage thread and
  its queues vanish from the runtime charts.
- Add a `docs/journal/` entry for the effort per repo convention.

## Testing

- `cargo test --workspace`, `cargo clippy --all-targets --all-features`,
  `cargo fmt --all` — all clean.
- Existing integration tests validate both modes: `tests/integration.rs`
  (single worker) and `tests/integration_multi.rs` (multi worker, now truly
  concurrent storage). Same for rds.
- Benchmarks (`benches/benchmark.rs`) continue to compile against the `&self`
  `Execute` trait.
- Engine-level concurrency correctness (loom models, drain protocol) is
  validated in cache-rs, not re-proven here.

## Accepted semantic changes

- **CAS vs eviction race:** a `cas` racing an eviction-relocation of the
  checked item can fail with `EXISTS` where the old engine would have
  succeeded. Fail-safe direction; documented in the engine.
- **add/replace are not atomic under concurrent workers** (decision
  2026-08-18): entrystore implements them as check-then-act
  (`get_no_freq_incr` → `insert`). With workers sharing the engine, two
  concurrent `add`s on one key can both return `STORED`, and `replace` can
  race a concurrent `delete`. Accepted for now; the correct fix is
  engine-level conditional-insert primitives (insert-if-absent /
  insert-if-present) in cache-rs — related groundwork exists there as the
  fresh-key insert de-duplication spec — after which entrystore switches to
  them. Note: add-based distributed-locking patterns can double-win until
  then.
- **cas with past-timestamp TTL has a non-atomic delete-after tail:** the
  engine `cas` is atomic, but the follow-up delete used to emulate
  immediate expiry can remove a value a concurrent `set` stored in between.
  Same category and same eventual fix as add/replace; obscure path.
- **Expiration driver:** expiration moves from the storage thread (multi) or
  the worker loop (single) to the maintenance thread. Cadence is equivalent;
  single-worker deployments gain a thread but lose per-loop expire work on the
  request path.

## Payoff

- Multi-worker requests no longer pay two queue hops and a storage-thread
  wakeup; storage operations execute in parallel across workers on the
  lock-free engine.
- `core/server` loses an entire thread model and its queue plumbing — one
  worker implementation instead of three files.
