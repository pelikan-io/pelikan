# Documentation: merits and architecture readability

## What

Strengthen the README's case for pelikan with concrete, attributed claims
(drawn from pelikan.io and the NSDI'21 Segcache paper), and restructure
docs/ARCHITECTURE.md from a directory listing with charts appended into a
narrative reading path.

## Decided

- **Merits are numbers with sources, not adjectives.** The README's value
  bullets and a new "Why Pelikan" section carry the measurable claims —
  5 B/object metadata (vs 56 for Memcached), 22–60% memory reduction vs
  state of the art, ~8× Memcached throughput at 24 threads, p999 < 5 ms at
  1M QPS/host under ADQ — each attributed to the Segcache blog/paper or the
  ADQ evaluation. Mechanism claims (lockless queues, plane separation) are
  framed as design statements, distinct from measured ones.
- **ARCHITECTURE.md reads in three views**: what the code is (build chart +
  composition table), what runs (threading chart + thread model selection),
  what happens to a request (dataflow chart + stage verbs) — then layer
  detail, design principles, and a closing section on how the generated
  charts stay honest. Each chart leads its section instead of trailing the
  document.
- **Blind simulation caught real defects** the author read past: the
  admin-HTTP-endpoint vs protocol-http-unused apparent contradiction
  (resolved: the endpoint uses an embedded server, now stated), "lockless
  data plane" vs amortized storage locking phrasing tension (reconciled),
  the rds-vs-segcache storage question the composition table left open
  (both wire `entrystore::Seg`; now stated), and the unstated
  single-vs-multi worker selection (`[worker] threads`, default 1; now
  stated).

## Open

- The README telnet transcript hardcodes `VERSION 0.3.2`, which will drift
  silently on release bumps — accepted for now as a low-risk, real
  transcript; a release-time check could catch it.
- Human review of the restructure (gated surfaces: README merits section,
  ARCHITECTURE reading order) — pending maintainer review on the PR.

## Appendix: Skills Invoked

- `document-feature` — governed the effort: charter recheck, frozen tasks
  per audience, blind simulation plus separate critic, human review gate.
- `journal` — this entry, under the skill-use convention.
