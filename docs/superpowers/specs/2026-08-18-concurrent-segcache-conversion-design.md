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
4. **Lazy-on-get + eager-on-pressure expiration; no maintenance thread**
   (revised 2026-08-18, superseding the original dedicated-maintenance-thread
   decision after it briefly landed). The engine's eviction path already
   reclaims whole expired segments before real eviction ("eager on
   pressure"), and `incr`/`decr` already treat expired items as missing
   ("lazy"). The missing piece was a lazy deadline check in `get` — added to
   cache-rs (segcache 0.4.1) mirroring `numeric_update`, and extended to
   `cas` and `delete` for full memcached lazy-expiry parity. With that,
   pelikan needs no periodic `expire()` at all: no maintenance thread, no
   expire calls in worker loops, and `EntryStore::expire` is removed as dead
   API. `FlushAll` is handled by every worker (the admin signal is broadcast;
   each calls `clear()`, and the duplicate clears are cheap no-ops).
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

### 4. Expiration model (revised: no maintenance thread)

Prerequisite in cache-rs (segcache 0.4.1): a lazy deadline check in the read
path — `get_pinned` (covering `get` and `get_no_freq_incr`) returns `None`
for items whose pinned segment is past `create_at + ttl`, and `cas`/`delete`
gain the same check — mirroring the existing `numeric_update` behavior and
matching memcached's expired-items-act-missing semantics.

With that in place:

- **Correctness** comes from the lazy checks: expired items are never served,
  regardless of when their segments are reclaimed.
- **Memory reclamation** comes from write pressure: the engine's eviction
  path drops whole expired segments as its cheap first path before any real
  eviction. Under low load, expired segments may linger in memory — accepted;
  there is no demand for that memory until there is write pressure, at which
  point it is reclaimed first.
- Pelikan calls `expire()` nowhere. `EntryStore::expire` is removed from the
  trait (and its implementors) as dead API; the engine keeps its public
  `expire()` for callers that want eager reclamation.
- `FlushAll` is handled by each worker: the admin signal fan-out is a
  broadcast, every worker receives it, and each calls `storage.clear()` —
  the second..Nth calls find already-drained buckets and are cheap no-ops.
- Net thread count: one fewer than the original multi-worker model (the
  storage thread is not replaced by anything).

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
- **Expiration driver:** no periodic `expire()` at all. Expired items become
  invisible via lazy deadline checks in the engine's read/mutate paths
  (segcache 0.4.1); expired segments are reclaimed by the eviction path under
  write pressure. Under low load expired segments linger in memory (metrics
  show them as used) — accepted, since that memory has no competing demand
  until write pressure exists.

## Payoff

- Multi-worker requests no longer pay two queue hops and a storage-thread
  wakeup; storage operations execute in parallel across workers on the
  lock-free engine.
- `core/server` loses an entire thread model and its queue plumbing — one
  worker implementation instead of three files.
