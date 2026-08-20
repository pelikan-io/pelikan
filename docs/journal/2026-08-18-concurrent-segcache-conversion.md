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
- **The pre-PR adversarial review found six critical engine bugs, and
  that is the headline result of this effort.** Being the first workload
  to drive the engine concurrently from many threads is what exposed
  them; the engine's own unit and loom coverage had not, largely because
  its tests exercised each mechanism in isolation (unique keys per
  operation, cache sized so eviction never runs) and the bugs all lived
  in the overlaps. In rough order of severity: concurrent `incr`/`decr`
  lost updates (a non-atomic read-modify-write whose "writers are
  serialized externally" precondition this conversion deleted — 8
  threads × 10k increments landed 47,336 of 80,000); a replace whose old
  value shared the tail segment with its new reservation deadlocked
  against any drain of that segment, wedging two threads and the bucket
  chain lock; `cas` racing `incr` returned false `STORED` and destroyed
  acked increments (~13%); merge relocation raw-`memcpy`'d numeric items
  outside the seqlock, which could orphan an in-flight increment or
  publish a permanently odd-version item that wedged every later access
  to that key; an acked `delete` could be resurrected by merge
  relocation; and live keys read as missing during merge drains, which
  also let `add` clobber them. All six were reproduced with a failing
  test before being fixed.
- **Fix, then re-review the fix.** Each fix got an adversarial pass of
  its own, and that is what caught the fourth bug above: the cas-integrity
  fix's "no residual window" claim was false, because the party it forgot
  was the one the deadlock fix had just promoted to first-class. Two of
  the fix rounds also discarded their own first design after analysis
  (a double-apply window; an ABA hazard introduced by a fix for a
  different ABA hazard). The rule that fell out and is now written into
  the engine: *a publish superseding an item may proceed unpinned iff it
  touches no segment bytes; a publish that must re-verify or freeze item
  state requires a remover pin and fails safe with `EXISTS`.*
- **Two independent reviews converged on the same deadlock.** A separate
  effort in cache-rs found it from the engine side (issue #49) and fixed
  it by never waiting — publishing through an unpinned slot CAS — while
  this one found it from the integration side and fixed it by rolling
  back and restarting. Reconciling them was worth more than either fix:
  never-wait is the stronger property and better for `insert`, but is
  unsound for token-gated `cas` on this base, because acquiring the
  numeric version lock *writes* into segment bytes that nothing is
  pinning against recycle. Rollback-restart shipped; the never-wait
  design is recorded in cache-rs #56 with that constraint attached.
  Merging the two branches naively would also have silently
  double-subtracted the item gauges — two correct-looking fixes for the
  same drift, in different places, that git auto-merges without a
  conflict.

- **A seventh engine bug surfaced after the release, from a CI flake we
  didn't write off.** A version-bump-only PR failed one job in the test
  guarding the very invariant the drain fix had just established, then
  passed on re-run. Triage found a distinct mechanism: the hashtable's
  key verification (`SegmentsVerifier::verify`) compares key bytes by
  reading raw segment memory at the location loaded from the bucket slot,
  with no pin and no generation tag — so a reader that stalls while a
  merge relocates the item and the segment is recycled and rewritten
  verifies against the *new* occupant, concludes "different key", and the
  lookup returns `None`. It returns before any retry logic, so the drain
  fix's unbounded pin retry never runs. Reproduced ~1 in 2,400 runs at
  24-way parallelism (a serial loop is nearly useless); ~1.3e-9 per
  lookup under continuous full-heap turnover. The guard already existed
  in the same file on the write path and had simply never been
  generalized to the five read helpers. Two lessons worth keeping: a
  green-on-re-run failure in a test that guards a just-fixed invariant
  deserves triage, not a re-run; and a fix written for one path should be
  audited against every path sharing its hazard, which is now written
  into the engine as one named invariant the six sites reference.
- **Pelikan's exposure to that bug is narrower than the engine's.**
  Pelikan calls `get_no_freq_incr` only in `add` and `replace`, and never
  `try_into_numeric` or `contains` — so the worst engine failure mode (a
  live counter silently reset to its initial value via the
  incr-with-initial path) does not apply here. What did apply: `add`
  could clobber a live key, `replace` could return NOT_STORED for one,
  `delete` could report NOT_FOUND without deleting, and `cas` could
  return NOT_FOUND where memcached requires EXISTS. A spurious `get`
  miss is legal for a cache; the false *absence* leaking into
  check-then-act commands is what made it a contract violation.

- **The durable outcome is a verification standard, not the bug list.**
  Four patterns came out of chasing these, and each was earned by a claim
  that turned out to be unbacked:
  - **A test must be proven able to fail.** #60 shipped a control test
    that passed unchanged against deliberately broken code — it asserted
    coverage it did not have. Every model and guard test since is
    neutered on purpose to watch it go red first. The same check applied
    per-function ("break this and see if anything notices") found a
    production path — S3-FIFO promotion — that could degrade to a
    complete no-op with the entire suite still green.
  - **Cost properties belong in deterministic checkers, not benchmarks.**
    The #65 fix asserts convergence as a loom invariant on *lookup
    count*, falsifiable two independent ways. Expressed as a benchmark it
    would have been at the mercy of the noise floor; expressed as an
    invariant, machine load is irrelevant. Correctness models are
    routine; a cost model is not, and it is the better home for a
    property like "this retry converges".
  - **A performance claim must carry its resolution.** Read-path numbers
    were cited from `get/*` benchmarks that never insert and therefore
    measure only the miss path — structurally incapable of detecting a
    hit-path regression, which is what they were being cited for. Renamed
    to `get_miss/*`, with verified `get_hit/*` groups added; a hit costs
    ~2.5x a miss. And on a shared machine the same code and benchmark
    produced an A/A control of ±1.26% quiet versus −13.0%..+2.6% under
    load, so every number now ships with its control spread and load
    average. "Unmeasured, control spread ±X%" is a result; a bare
    percentage is not.
  - **Prefer structure over discipline — but check which way it points.**
    The most-repeated decision of the effort: when a rule has to be
    remembered at every call site, move it somewhere the compiler or the
    type enforces it. Carry the tag inside `Metadata` so pack/unpack
    round-trip it, rather than preserving bits at each site (the rule was
    already forgotten once, in the change that introduced it). Compensate
    per-item rather than in a tail block an early return can skip. Bump
    the generation on the state transition, not in a queue helper a path
    can legitimately bypass. Make the maximum segment id unissuable
    rather than documenting the cliff. But it is a heuristic with a
    direction, not a law: the two retry arms in `get_pinned` look alike
    and terminate for genuinely different reasons — one because a drain
    is bounded work that must finish, the other because a bound says so —
    and unifying them would have hidden the asymmetry that makes each
    correct. There, the *distinct* code is what carries the invariant.
  - **The A/A control has a blind spot: it cannot see code-layout
    noise**, because both arms share a layout. Measured afterwards: six
    padding-only variants of one binary — semantically identical, hot-path
    instructions identical, differing only by `global_asm!(".space N")` —
    span **0.6–1.3 ns (1.6–3.4%)** on the same benchmark where the A/A
    control read **0.097 ns (0.25%)**. The control was advertising a
    resolution roughly **10× better than achievable**, and both of us
    trusted it. Consequence: every "effect is below our ±X% control"
    conclusion this week was measured against a floor that was too
    optimistic — the conclusions survive (a wider floor makes "below
    resolution" *more* true) but the stated precision did not. For
    effects near ~1 ns the protocol is to build each variant as several
    padding-only layouts and compare per-layout minima; better still,
    attribute from **disassembly rather than timing** where the question
    permits it, which settles instruction-form questions exactly and with
    no benchmark at all. Same family as the rest: a real measurement
    that could not observe the thing it was cited for.
  - **A dilution control beats arguing about confidence intervals.** Two
    benchmarks with a known working-set ratio give an *internal
    consistency* test, not just a floor: a genuine fixed cost of ~1.6 ns
    must show roughly half the relative effect on a 255-byte hit path
    that it shows on a 1-byte one. When a measurement showed *more*
    (+2.76% vs +1.82%), that was physically impossible for a real fixed
    cost, so the sweep was measuring the machine — settled without
    adjudicating a single overlapping interval. Strictly stronger than
    the A/A control, which tells you the floor but cannot catch two
    effects that both clear the floor while contradicting each other. A
    reviewer can dispute whether ±3.72% is "too wide"; nobody can
    dispute that a fixed cost cannot dilute upward.

    The control has **three** consistent outcomes, not two: the diluted
    effect appears at roughly the predicted smaller size; it appears
    *larger*, which disqualifies the sweep; or it is **correctly
    invisible** — the predicted dilution falls below that group's own
    resolution, so it could not have been distinguished from zero either
    way. The third is a pass, but only if you check that group's
    resolution rather than reading "no effect" as a contradiction. A
    later measurement landed exactly there: +1.2 ns on a 40 ns baseline
    predicts ~1.45% on an 82 ns one, against a 2.15% control on that
    group — invisible by construction, and so evidence *for* the fixed
    cost rather than against it.
  - **Attach the cheap check to the claim.** Every miss this week was a
    plausible statement nobody spent thirty seconds testing: "loom can't
    model this" (it could — the seam was already in production code),
    "16 lifecycles bounds the aliasing window" (a lost election bumps the
    counter for free), "these busy-loops are the other session's live
    work" (`ps -o ppid` showed a dead parent, 5.5 hours of orphans).

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
- Engine follow-ups tracked in cache-rs, none blocking: a `delete` racing
  a merge copy aborts the remainder of that segment's copy
  (eviction-legal amplification); unpinned unlinks retain an ABA class
  (#50) that wants generation-tagged locations or a key-verifying remove;
  the never-wait insert design (#56); and an item-gauge under-count under
  S3Fifo promotion, fixed in a follow-up PR from the parallel effort.
- Numeric values are stored typed when they parse as `u64`, so `set`ting
  `007` reads back `7` — a memcached byte-transparency violation that
  predates this work and survives it.
- **Three engine timing metrics report garbage, and pelikan exposes them**
  (cache-rs #75, found while trying to build an assertion on one).
  `EVICT_TIME`, `EXPIRE_TIME` and `CLEAR_TIME` measure sub-millisecond
  operations against a clocksource `Instant` with **1-second
  resolution**, so `elapsed()` yields either `0` — when the operation
  fits inside one coarse second — or exactly `1_000_000_000` ns when it
  straddles a tick. Pelikan's admin enumerates metriken's global
  registry, so all three appear in `/metrics` verbatim.
  `evict_time` is the damaging one: eviction runs constantly under
  pressure, so a fraction of evictions each contribute a spurious full
  second and the total becomes a random walk. `evict_time /
  segment_evict` looks like an average eviction latency and is
  meaningless. That is worse than a metric that visibly reads zero — a
  zero prompts an investigation, a plausible number does not.
  `clear_time` is the benign instance only because flushes are rare
  enough that it mostly reads 0. Fix is upstream (`std::time::Instant`
  at the three sites); the coarse clock is correct for TTL and expiry
  *deadlines*, which is what it exists for, and the two concerns sharing
  the type name `Instant` is what made this invisible at the call site.

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
