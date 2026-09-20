# Per-worker histogram assessment

The experiment in `src/session/examples/histogram_scaling.rs` compares three
recording and collection designs. It does not change production metrics.

| Variant | Recording | Collection |
| --- | --- | --- |
| Shared | All workers increment one `metriken::AtomicHistogram` | Load its buckets and calculate aggregate quantiles |
| Sharded | Each worker increments its own `metriken::AtomicHistogram` | Load each shard, merge bucket counts, calculate aggregate quantiles |
| Local | Each worker exclusively owns a non-atomic `Histogram` | Transfer completed buffers to the collector; merge and recycle them |

All variants use the `(7, 32)` geometry of Pelikan's `REQUEST_LATENCY`.
The atomic variants retain metriken's initialization/access wrapper. Local
recording holds its histogram directly in worker state, without a thread-local
lookup per observation. A production implementation must account for any
additional lookup or registry overhead.

## Reproduction

On Linux, select four distinct physical worker cores and a separate collector
CPU. The defaults (`0,2,4,6` and `8`) describe the development i5-13500H only;
inspect `lscpu -e` and allowed CPU affinity before using another host.

```sh
cargo build --release -p session --example histogram_scaling --locked
HIST_CPUS=0,2,4,6 HIST_COLLECTOR_CPU=8 \
  target/release/examples/histogram_scaling 750 5 > histogram-results.csv
```

Arguments are measured milliseconds per case and repetitions. Run the
benchmark after compilation and without concurrent builds. Each repetition
rotates variant order. There is no frequency lock or machine isolation in the
harness; record CPU, kernel, Rust version, background load, and build profile
with results. Measurements belong to the PR assessment with their environment
and limitations, not to a portable performance guarantee.

The matrix uses 1, 2, and 4 workers; concentrated values (10,000–10,007, in one
bucket); and a broader deterministic sequence spanning powers of two. Inputs
are generated before timing. The two collection settings are disabled and a
nominal 10 ms collector interval. Workers check time and publication requests
once per 256 records in every variant. All samples count toward the result;
the input sequence is repeated rather than generated randomly in the hot loop.

At completion, the harness verifies every bucket against the exact expected
distribution, including samples remaining in worker buffers. Missing,
duplicated, or incorrectly merged samples abort the run.

## What the measurements mean

- `mrecords_per_s` is aggregate recorded samples divided by the longest worker
  recording duration. `worker_ns_per_record` is the mean of each worker's
  duration divided by its sample count. These are different denominators.
- `mean_collection_us` includes bucket loading/merging, buffer recycling, and
  p50/p99/p999 calculation. It excludes sleeping. Collection-disabled runs
  still perform a final correctness collection; use enabled runs for periodic
  collection comparisons.
- `collector_cpu_ms` is thread CPU time over the run, including loop overhead
  and final draining, not wall-clock sleep time.
- `max_publication_age_us` measures elapsed time from a local buffer's handoff
  to its consumption. It is not end-to-end metric freshness: samples may have
  accumulated for a whole publication interval before handoff.
- `resident_histogram_bytes` estimates algorithmic histogram storage: shared
  or sharded atomic state plus a merged result and one temporary load; or two
  local buffers per worker plus a merged result. It excludes allocator and
  channel overhead, benchmark inputs, unused harness objects, and production
  registry/snapshot structures. This is not an RSS measurement.

This isolates histogram operations. It does **not** measure server request
throughput, request tail latency, NUMA scaling, many simultaneous metric
families, or arbitrary worker counts. A recording speedup does not imply the
same server speedup. Local 4-core results are screening evidence for choosing
the next prototype, not a deployment recommendation.

## Local publication contract in the prototype

Each worker has an active histogram and one spare. The collector requests a
publication by advancing an epoch. At a batch boundary, the worker tries to
take a spare, swaps buffers, and sends the completed histogram through a
bounded channel. The collector merges it, clears it, and returns it through
another bounded channel. Ownership moves with the buffer, so the collector
never reads buckets being modified by a worker.

If no spare is available, the worker continues accumulating without waiting.
Requests coalesce until the spare returns. No samples are dropped, but the
observation interval can grow. The collector polls every 10 ms, so a
publication may wait another interval before it is consumed. No exact global
window is promised. All variants calculate cumulative quantiles in this
experiment; production interval-delta semantics remain a separate decision.

The timed recording loop uses only non-waiting channel operations. Final
publication may wait after timing ends to validate and retain all samples.
Worker shutdown, stalled workers, queue latency, and stale-snapshot reporting
still require a production lifecycle design. Non-waiting channel operations
also do not establish that the queue implementation itself is lock-free.

## Decision gates

1. Determine whether worker sharding materially improves recording cost over
   shared atomics for concentrated and broad distributions.
2. Measure the further improvement from non-atomic recording against sharded
   atomics, including publication work and memory, before accepting its added
   lifecycle complexity.
3. Prototype the strongest candidate in a separate, opt-in server experiment;
   measure request throughput and tail latency with realistic sampling rates
   and scraper load. Keep the shared implementation as the control.
4. Before production adoption, specify collection windows, stale-worker
   reporting, worker exit, and a bounded-memory overload policy. Merge bucket
   counts before computing percentiles. Consider a reusable metriken design.

Histogram refresh and export of Ringline's counter groups remain independent
correctness tasks in [the observability work list](OBSERVABILITY.md).
