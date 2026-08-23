---
status: open
opened: 2026-08-18
updated: 2026-08-22
prs: [187]
beta_skills: [review-guide]
---

# Ringline cache-server backend

## Goal

Add Ringline as an optional Linux network backend for Pelikan cache servers
without replacing Mio as the portable default. Keep backend policy in the
server layer and backend mechanics behind `pelikan-net`.

This entry uses the single-PR lifecycle. The implementation is complete, but
the entry remains open until PR #187 lands.

## Decision Criteria

The change is ready when:

- a Linux build can opt into Ringline while a Mio-only Linux or non-Linux build
  does not resolve Ringline-only dependencies;
- Pingserver, RDS, and Segcache can run plain TCP on either backend;
- startup failures can fall back before any listener accepts traffic, while a
  failure after the runtime becomes live shuts the process down;
- Mio and Ringline workers execute against the same process-owned cache engine;
- an admin flush clears that engine synchronously before returning `OK`;
- configured backend, active backend, and exact fallback cause are observable;
- native and forced-Mio Ringline paths have protocol, lifecycle, and concurrency
  coverage.

## Scope

PR #187 covers the Pingserver, RDS, and Segcache data planes. Mio remains the
default and remains the implementation for admin traffic and TLS. Proxy support
and a Ringline TLS path are separate efforts.

Ringline and its support dependency are both feature-gated and Linux-target
gated. Enabling the feature on a non-Linux target retains Mio without resolving
Ringline.

## Evidence

The implementation before this journal-only cleanup was commit
`007e802b8c668496eb0096fefb0ad9d0739a50ea` in PR #187.

Verification included locked workspace tests, doctests, release builds, strict
Clippy with the repository's existing large-enum allowance, deterministic
diagram generation, and native and forced-Mio Ringline integration suites. The
integration suites cover single- and multi-worker servers, partial requests,
pipelines, 64 KiB values, disconnects, cancellation, TLS fallback, listener
release, restart loops, and concurrent flush traffic.

GitHub CI was green at that implementation head. Native io_uring execution was
not available on the development host; those runs verified exact
capability-classified fallback. The forced-Mio Ringline feature exercised the
Ringline runtime and worker lifecycle without depending on host io_uring
support.

The journal-only CI run 32621999549 exposed a false positive in the concurrent
flush test on macOS. A writer sampled the pre-flush phase before sending, then
could be descheduled until after the clear and incorrectly label its surviving
write as an uncleared key. Early samples now require the write to complete
before flush start; four fresh forced-Ringline runs passed after the correction.

The eight-worker Segcache integration originally failed under GitHub's
`RLIMIT_NOFILE=65536`: Ringline's default 16,000 connections per worker
required about 128,128 descriptors. A `server.ringline_max_connections`
setting now keeps the production default at 16,000 while integration fixtures
use 256, requiring about 2,112 descriptors and still covering their 129-connection
peak.

## Design and Implementation

### Backend selection and lifecycle

The server parses the requested backend and delegates construction to
`pelikan-net`. Mio is the default. Ringline is selected only when configured,
compiled, and supported by the target.

Fallback is a startup transaction. Ringline must finish fallible worker
initialization before it creates the data listener. A configuration or runtime
failure before the listener exists can return ownership to the Mio builder.
Once Ringline is live, failures terminate the server instead of moving active
connections between runtimes.

Logs and admin stats expose the requested backend, active backend, and exact
fallback cause. Configuration errors remain initialization failures; only
typed runtime capability failures qualify for the supported fallback path.

### Shared cache storage

The first multi-worker design used a dedicated Ringline storage thread with
request and response queues. That design was removed after Pelikan's cache
engine became internally synchronized.

The process now creates exactly one `Arc<Storage>` before backend selection.
Mio workers, Ringline worker bootstraps, the fallback path, and the admin flush
handle all clone that same Arc. Every data worker executes cache operations
directly. There is no Ringline storage worker, cross-thread completion bridge,
storage request queue, or bridge-specific metric.

The admin flush handle calls `clear` synchronously and queues `OK` only after
the clear returns. Tests exercise the ordering with a blocking flush and verify
that every worker observes the cleared shared engine.

### Ringline startup transaction

Ringline 0.5.3 originally started its acceptor before every worker completed
fallible initialization. A later worker failure could expose clients to a
runtime that would roll back, and queued raw descriptors could be abandoned.

The upstream fix delays listener creation and acceptor startup until every
worker reports ready. Failure rollback wakes and joins started workers and
closes launch-owned descriptors. The generic change merged as Ringline PR #309,
commit `a04751f0041c0ffc485a706bb124ec40b27823e1`. No published Ringline release
contained it when this entry was updated, so Pelikan still carries the audited
0.5.3 source and patch provenance under `vendor/`.

The vendored runtime also contains follow-up send and receive lifecycle work:
bounded FIFO send reservation, oversize rejection before writing bytes, permit
retention through Mio flush, operation IDs that reject stale completions, and
generation-tagged receive errors.

### Metrics dependency convergence

Ringline 0.5.3 uses `metriken-core` 0.2. Pelikan and Segcache previously used
the 0.1 domain, which would split the process-global metric registry.

A temporary compatibility facade compiled but was not correct: its histogram
API interpreted percentiles as fractions while Pelikan supplied percentages.
The admin path ignored the resulting errors and emitted no percentile
snapshots. The facade was abandoned. The workspace now resolves registry
`metriken` 0.9.2, `metriken-core` 0.2.1, and registry `segcache` 0.4.4
exactly once each; the vendored Segcache copy was removed.

### Architecture documentation

Generated diagrams distinguish Mio's listener and callback workers from
Ringline's acceptor, bounded accepted-descriptor wake path, async worker tasks,
and control thread. Both branches converge on one backend-neutral shared
storage panel. Generator assertions cover Ringline's io_uring and Mio task
paths, and repeated generation produces byte-identical SVGs.

## Outcome

The implementation satisfies the decision criteria at the current PR head.
PR #187 remains open pending review and merge, so this journal entry remains
`open`.

## Derived Documents

- `README.md` documents backend selection, platform constraints, fallback,
  operational limits, and the per-worker connection budget.
- `docs/ARCHITECTURE.md` documents the backend fork and shared cache engine.
- `docs/diagrams/dataflow.svg` and `docs/diagrams/threading.svg` are generated
  from source-checked topology claims.
- `vendor/README.md` records Ringline archive provenance and patch checksums.

The temporary Ringline plans and specification were absorbed into this entry
and removed in the same PR update.

## Deferred or Reopen Items

- Mark this entry `shipped` after PR #187 merges and record the merge commit.
- Replace the vendored Ringline source when a release contains the required
  startup and runtime behavior.
- Add Ringline TLS support as a separate design and implementation.
- Evaluate proxy support separately; proxy behavior did not change here.
- Run native io_uring conformance on a capable Linux host in addition to the
  forced-Mio Ringline coverage.

## Skill Feedback

### review-guide (beta)

- **Friction** — The existing PR body had become stale after the storage and
  dependency pivots. The skill’s full-guide structure was disproportionate for
  this cleanup, so the body update retains its useful reviewer-facing sections
  without expanding every prescribed section.
- **Confirmation** — Ranking reviewer attention exposed two claims that a diff
  could not safely correct by itself: the removed storage bridge and the
  removed vendored Segcache copy. Re-reading the implementation before editing
  the body prevented those obsolete claims from surviving.

## Appendix: Skills Invoked

- `engineering-journal` — durable effort record and lifecycle index.
- `technical-prose` — word-level pass over the journal and PR material.
- `sweep-comments` — ordering-comment audit for the CI regression fix.
- `review-guide` (beta) — reviewer attention, test gaps, and production risks.
- `superpowers:brainstorming` — backend scope and lifecycle design.
- `superpowers:systematic-debugging` — startup, metrics, CI, and lifecycle failures.
- `superpowers:test-driven-development` — implementation and regression fixes.
- `superpowers:using-git-worktrees` — isolated feature and upstream work.
- `superpowers:writing-plans` — staged implementation planning.
- `superpowers:subagent-driven-development` — parallel implementation and verification.
- `superpowers:requesting-code-review` and `superpowers:receiving-code-review` — independent review
  and correction rounds.
- `superpowers:verification-before-completion` — proportional and full release gates.
- `superpowers:finishing-a-development-branch` — PR readiness and integration checks.

The roster is incomplete because the effort crossed compacted sessions and
multiple handoffs; only skills confirmed in retained history are listed.
