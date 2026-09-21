# Per-worker histogram assessment

The experiment in `src/session/examples/histogram_scaling.rs` compares four
recording and collection designs. It does not change production metrics.

| Variant | Recording | Collection |
| --- | --- | --- |
| Shared | All workers increment one `metriken::AtomicHistogram` | Load its buckets and calculate aggregate quantiles |
| Sharded | Each worker increments its own `metriken::AtomicHistogram` | Load each shard, merge bucket counts, calculate aggregate quantiles |
| Relaxed | One writer per shard uses `AtomicU64::load/store(Relaxed)` | Read cumulative buckets with relaxed loads; never reset live buckets |
| Local | Each worker exclusively owns a non-atomic `Histogram` | Transfer completed buffers to the collector; merge and recycle them |

All variants use the `(7, 32)` geometry of Pelikan's `REQUEST_LATENCY`.
Shared and Sharded use relaxed atomic **read-modify-write** increments and
retain metriken's initialization/access wrapper. Relaxed avoids that wrapper
and the read-modify-write operation. It specializes histogram 1.5's private
bucket-index calculation for `(7, 32)`; tests compare both ends of every bucket
against the library. Mapping remains inside the timed loop. This is a design
comparison, not an isolated measurement of instruction costs. Local
recording holds its histogram directly in worker state, without a thread-local
lookup per observation. A production implementation must account for any
additional lookup or registry overhead.

## Sidecar synchronization default

Prefer relaxed atomic access for independent observational values that do not
publish application state. The Relaxed variant has exactly one writer per
shard; concurrent collection only reads. A load followed by a store is not a
multi-writer increment. Multiple writers still require `fetch_add` or another
correct update protocol. Initialization, ownership transfer, and shutdown
retain the synchronization their lifecycle requires.

Each relaxed bucket read is atomic, but a sweep is not one simultaneous
histogram snapshot. Cross-bucket timing differences are tolerated here. A
production interval exporter should subtract successive cumulative snapshots,
not reset buckets while the worker is updating them. Counter wrap, worker
replacement, and snapshot timestamps need explicit handling. Tests exercise
nondecreasing reads (without wrap) and exact final counts after writer join.

This is the preferred baseline candidate before accepting buffer handoff's
additional lifecycle machinery, not a production implementation selection.
Hardware/cache-coherence and compiler costs remain; relaxed does not mean
zero-cost. Plain concurrent non-atomic reads/writes are not a substitute.

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

To assess the usual collection cadence, set the request interval separately
from the collector's 10 ms queue-drain cadence. These runs use four workers,
three repetitions, and enough recording time for two periodic requests per
case (plus a final drain):

```sh
HIST_INTERVAL_MS=1000 HIST_WORKERS=4 HIST_COLLECTION=on \
  target/release/examples/histogram_scaling 2200 3 > histogram-1s.csv
HIST_INTERVAL_MS=5000 HIST_WORKERS=4 HIST_COLLECTION=on \
  target/release/examples/histogram_scaling 10200 3 > histogram-5s.csv
cargo run --release -p session --example histogram_windows > histogram-windows.csv
```

`HIST_WORKERS` is a comma-separated worker-count matrix. `HIST_COLLECTION=on`
selects enabled collection only; otherwise both settings run. Defaults retain
the original 1/2/4-worker, 10 ms matrix. For longer intervals, recording must
last longer than the requested interval; prefer multiple intervals per case.

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
  p50/p99/p999 calculation on sweeps that load or receive data. Local replies
  can arrive on separate sweeps, so this is not necessarily cost per complete
  global snapshot. It excludes sleeping. Collection-disabled runs
  still perform a final correctness collection; use enabled runs for periodic
  collection comparisons.
- `collector_cpu_ms` is thread CPU time over the run, including loop overhead
  and final draining, not wall-clock sleep time.
- `max_publication_age_us` measures elapsed time from a local buffer's handoff
  to its consumption. It is not end-to-end metric freshness: samples may have
  accumulated for a whole publication interval before handoff.
- `max_cutoff_lag_us` measures how long a worker takes to cut and publish a
  buffer after the collector requests it. It excludes the collector's own
  timer lateness relative to an ideal wall-clock schedule and excludes final
  shutdown publications. `requests` counts actual periodic requests.
  Both publication timing fields apply only to Local; zeros for directly
  loaded atomic variants mean not applicable, not zero end-to-end age.
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
observation interval can grow. The collector polls every 10 ms independently
of the configured publication-request interval, so a publication can wait for
the next poll before it is consumed. Slowing the request interval to five
seconds does not deliberately make queue draining wait five seconds. No exact global
window is promised. All variants calculate cumulative quantiles in this
experiment; production interval-delta semantics remain a separate decision.

The timed recording loop uses only non-waiting channel operations. Final
publication may wait after timing ends to validate and retain all samples.
Worker shutdown, stalled workers, queue latency, and stale-snapshot reporting
still require a production lifecycle design. Non-waiting channel operations
also do not establish that the queue implementation itself is lock-free.

## Burst-boundary sensitivity

`histogram_windows` is a deterministic replay, not a measured runtime latency
distribution. Four workers each record 100,000 samples/second: ordinary values
are 100 µs, with a 20 ms burst of 10,000 µs values. Burst starts range from
20 ms before to 20 ms after a collection boundary. Windows are 1 or 5 seconds.

The control merges exact-window buckets. The alternative shifts each worker's
window by an evenly staggered offset between zero and a chosen maximum
(1, 5, or 20 ms). Both window ends shift together, preserving duration and
sample count. The replay compares p50, p99, and p999 after merging buckets.
Histogram bucket upper bounds are reported, including 10,047 µs for the
10,000 µs sample value. Tests verify the boundary-crossing behavior.

A separate zero-cutoff-skew row represents delivering the same snapshot 20 ms
later: every percentile is unchanged. Delivery age and window-cutoff skew are
different quantities. The chosen cutoff offsets are sensitivity scenarios,
not claims that the live prototype incurred that skew. A burst constructed
right at a percentile threshold can move that percentile substantially even
with a small cutoff shift; this does not estimate how often that occurs in
production.

An exporter should expose the sampled window/cutoff timestamp. Until all
required publications arrive, it must not label a previous or partial window
as a newly completed global snapshot. Slowing collection reduces relative
delivery delay, but does not remove this requirement.

## Decision gates

1. Determine whether worker sharding materially improves recording cost over
   shared atomics for concentrated and broad distributions.
2. Compare single-writer relaxed load/store against sharded read-modify-write
   atomics first. Measure any further improvement from non-atomic recording,
   including publication work and memory, before accepting its added lifecycle
   complexity.
3. Prototype the strongest candidate in a separate, opt-in server experiment;
   measure request throughput and tail latency with realistic sampling rates
   and scraper load. Keep the shared implementation as the control.
4. Before production adoption, specify collection windows, stale-worker
   reporting, worker exit, and a bounded-memory overload policy. Merge bucket
   counts before computing percentiles. Consider a reusable metriken design.

Histogram refresh and export of Ringline's counter groups remain independent
correctness tasks in [the observability work list](OBSERVABILITY.md).
