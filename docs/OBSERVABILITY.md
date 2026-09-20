# Observability work

This work is maintained separately from the Ringline backend migration. The
branch is based on that migration so its source checks can inspect the actual
upstream Ringline metric types. After the backend PR merges, rebase this work
onto `main` and retarget its PR.

## Current implementation

The [observability diagram](diagrams/observability.svg) and
[architecture explanation](ARCHITECTURE.md#observability-across-threads) describe
the code as it exists. They show metric recording, admin exports, logging
queues, and known gaps. `cargo xtask diagrams` checks the source claims and
regenerates the chart. Every behavior change below must update those claims,
the chart, and its textual explanation together.

## Tasks

- [x] Add a code-checked diagram of metrics and logging across threads.
- [ ] Obtain maintainer visual review of the rendered diagram.
- [ ] Restore histogram snapshot refresh. Choose and document a sampling
  interval and the meaning of the exported window; do not make data-plane
  workers wait for an admin scrape. Verify that samples recorded after an
  initial export become visible in later exports and that idle windows behave
  as specified. Cover ASCII, JSON, and Prometheus output.
- [ ] Export Ringline's sharded counter groups. Inspect the `metriken`
  counter-group interface, preserve entry metadata, and define consistent
  names and aggregation across ASCII, JSON, and Prometheus output. Verify
  values from multiple worker shards and prevent duplicate metric identities.
- [x] Add and run a primitive per-worker histogram benchmark. Compare shared
  atomics, worker-owned atomics, and local recording with bounded buffer
  publication at 1, 2, and 4 P cores, with collection disabled and enabled.
  Validate all bucket counts against expected samples. See
  [methodology and decision gates](HISTOGRAM_ASSESSMENT.md); host-specific
  measurements are reported in the PR.
- [ ] Complete the end-to-end per-worker histogram assessment. Compare the
  current shared atomic histogram, per-worker atomic shards, and non-atomic
  local recording with
  safe snapshot publication. Include concentrated and broad sample
  distributions, worker-count scaling, recording cost, memory, collection
  cost, and request throughput/tail latency. Record benchmark environment and
  limitations. No histogram implementation has been selected yet.
- [ ] If measurements justify sharding, design its collection and lifecycle
  contract before implementation. Merge bucket counts before calculating
  percentiles; define window skew, worker exit, stale workers, and bounded
  memory. Pure thread-local recording requires explicit ownership transfer
  for snapshots; double buffering alone does not permit concurrent unsynchronized
  reads. Consider a reusable `metriken` implementation upstream.

## Scope boundary

The Ringline PR retains only the receive-metric adaptation required by the
upstream API change, including its EOF/reset regressions. Snapshot refresh,
counter-group export, histogram experiments, and observability documentation
belong to this work. The gaps documented here existed before the Ringline
dependency update.
