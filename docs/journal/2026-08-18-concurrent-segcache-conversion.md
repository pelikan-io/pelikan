---
beta_skills: [architecture-diagram]
---

# Concurrent segcache conversion

## What

Convert pelikan-segcache (and, by shared infrastructure, pelikan-rds) to
the concurrent segcache engine and collapse `core/server` to one unified
worker model: admin, listener, and N workers sharing the engine via `Arc`
and executing requests in place. Spec and plan:
`docs/superpowers/specs/2026-08-18-concurrent-segcache-conversion-design.md`,
`docs/superpowers/plans/2026-08-18-concurrent-segcache-conversion.md`.

## Decided

- **The storage thread existed only because the engine was
  single-threaded.** segcache 0.3.0 had a `&mut self` API, so multi-worker
  configs shipped every parsed request over a queue to a dedicated storage
  thread and shipped the response back — two queue hops and a cross-thread
  wakeup per request. The cache-rs concurrency roadmap (items 7a–7f:
  lock-free hashtable, per-segment claim/drain, seqlocked numeric ops,
  `&self`/`Sync` API) removed that reason, so the thread — and the whole
  single-vs-multi model split — went with it. Single worker is now just
  N = 1.
- **Staged conversion, each stage green on its own:** publish the engine
  (segcache 0.4.0) → bump the dependency (with an interim git pin) →
  convert the storage traits to `&self` in place (`Execute`,
  `EntryStore`, `protocol_memcache::Storage`, `protocol_resp::Storage`) →
  unify the workers behind `Arc<Storage>` → docs/diagrams/journal.
- **Design revision mid-flight: no maintenance thread.** The original
  design replaced the storage thread's periodic `expire()` with a
  dedicated maintenance thread, and that briefly landed. It was then
  removed after realizing the engine's expiration model makes any periodic
  pass unnecessary: eviction already reclaims whole expired segments
  before real eviction (eager on pressure), and segcache 0.4.1 added the
  missing lazy deadline check on `get`/`cas`/`delete` (mirroring
  `numeric_update`), so expired items act missing on access. Pelikan now
  calls `expire()` nowhere and `EntryStore::expire` is deleted as dead
  API. Under low load expired segments linger in memory — accepted; that
  memory has no competing demand until write pressure exists, at which
  point it is reclaimed first.
- **Review caught a real flush_all wake defect.** The admin thread
  broadcast `FlushAll` to the worker signal queues without waking the
  workers, and workers drained signals only after waker events — so a
  flush could sit unprocessed until unrelated traffic woke each worker.
  Fixed by waking workers on the broadcast and draining the signal queue
  once per event-loop iteration.
- **Accepted semantic changes** (documented in the spec): a `cas` racing
  eviction-relocation can fail `EXISTS` where it used to succeed
  (fail-safe); `add`/`replace` are check-then-act and can race under
  concurrent workers (two `add`s can both win) pending engine-level
  conditional-insert primitives in cache-rs; broadcast `flush_all` has a
  small smear window where a write acked between the first and last
  worker's `clear()` can be destroyed; and no periodic expire — expired
  memory is reclaimed under write pressure, not on a timer.
- **Publishing chain mattered:** segcache 0.4.0 could not be published
  until crates.io ownership of its `keyvalue 0.3.0` dependency was
  resolved; the branch carried a temporary git pin until keyvalue and
  segcache 0.4.0 landed, and 0.4.1 followed with the lazy-expiry checks.

## Open

- PR against pelikan-io/pelikan (plan Task E3): final verification,
  adversarial self-review, and a PR description that calls out the
  operational changes (dropped `storage_event_loop`/`storage_queue_depth`
  metrics, per-worker flush_all log lines, `pelikan_work` →
  `pelikan_work_0` thread rename, accepted semantic changes).
- add/replace atomicity waits on cache-rs conditional-insert primitives
  (insert-if-absent / insert-if-present); entrystore switches to them when
  they exist.
- Exactly-once flush via an admin-held clear handle, if the flush smear
  window ever matters in practice.

## Skill Feedback

### architecture-diagram (beta)

- **Friction** — asked to show the Arc-shared engine on the threading
  chart, but the chart's vocabulary covers threads, queues, and externals;
  there is no idiom for a shared non-thread datastructure. Rather than
  invent a shared-state box, the storage chips were duplicated into every
  worker box with the sharing stated in caption prose — honest, but the
  sharing itself is carried by words, not geometry.
- **Confirmation** — "assert absences too" held: the deleted storage
  thread became negative claims (no `_storage` spawn, no worker↔storage
  queues, no `expire()` in the worker loop) so the missing box stays
  honestly missing. "Chips bridge the halves" also held: the model change
  reads as chip migration — entrystore/segcache chips moving into every
  worker box — with no new visual vocabulary needed.

## Appendix: Skills Invoked

Roster for the docs/journal sessions only; earlier implementation sessions
ran under separate context and their skill use is not fully recorded here.

- `architecture-diagram` (beta) — collapsed the runtime charts from three
  panels to two and retargeted the source claims to the unified worker
  model.
- `journal` — this entry, under the skill-use convention.
