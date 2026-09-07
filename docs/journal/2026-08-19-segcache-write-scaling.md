# Segcache write-heavy scalability (experiment for PR #189)

## What

PR #189 removed the storage thread: N workers share one `Arc<Segcache>` and
execute in place (merged as `790490b`; see
`2026-08-18-concurrent-segcache-conversion.md`). That is unambiguously good for
reads and unknown for writes, so this effort measured write-heavy contention on
the shared engine, then bracketed the architectures that could fix it.

Harness: `segbench`, N OS threads on one `Arc<Segcache>` calling `get`/`insert`
directly — no sockets, no protocol parsing. It lives in cache-rs as the
`benchmarks/segbench` workspace member (pelikan-io/cache-rs#88), where it takes
a path dependency on segcache and measures the tree rather than a published
release; it is not duplicated here. Sweep CSVs and rendered charts are
regenerable output and are not checked in. The tables below are the record.

- Cores: the 8 E cores only, `taskset -c 8-15`. No SMT, so a thread count is a
  core count.
- Cache: 1 GiB heap, 1 MiB segments, Merge eviction, hash_power 22.
- Data: 1M keys (16 B / 128 B), TTL 0, prefilled. Uniform or Zipf(s=0.99).
- 2 s warmup + 8 s measure, 3 repeats, median. Run-to-run spread under 2%.
- Host: i5-13500H, 32 GB, Linux 6.8, rustc 1.97.1. Engine: segcache 0.4.4.

**On the discarded first pass.** Everything here was measured a second time.
The original sweeps ran 1–16 *logical CPUs* — 4 P cores, their 4 SMT siblings,
and 8 E cores — which plots three kinds of resource on one "threads" axis and
makes the curve say more about the core mix than about the engine. Those
numbers are superseded and are not reproduced here. Pinning to one core type is
the single most important thing to get right in this harness.

## Decided

**1. Reads scale; writes do not.** Sweeping 0% and 100% writes separately, not
only a mixture, is what exposed this. Full write-up: cache-rs
`docs/journal/2026-08-25-segcache-read-write-scaling.md`.

| Cores | read uni | read zipf | write uni | write zipf | 50/50 uni |
|---|---|---|---|---|---|
| 1 | 2.66 (1.00x) | 1.92 (1.00x) | 0.98 (1.00x) | 0.95 (1.00x) | 1.44 (1.00x) |
| 4 | 11.02 (4.14x) | 6.88 (3.58x) | 0.98 (0.99x) | 1.07 (1.13x) | 1.82 (1.27x) |
| 8 | 21.80 (8.19x) | 11.37 (5.91x) | 0.81 (0.82x) | 0.91 (0.95x) | 1.64 (1.14x) |

Reads return 8.19x of 8 equal cores. Writes never rise: the best speedup
anywhere is 0.99x uniform, 1.13x zipf, and the eighth core costs throughput.
The write path is a serial resource reachable from any core.

**2. A 50/50 mixture cannot show that.** Its apparent peak — 1.89 Mops/s at six
cores, falling to 1.65 at eight — is the read half scaling while the write half
sits near 1 Mops/s. Decomposing the mixture against the read-only curve
(`0.5/R + 0.5/W = 1/M`) recovers the same flat write capacity independently, so
two routes agree without either assuming a mechanism. Every remaining table
here is a 50/50 mixture and should be read as an *architecture comparison*, not
as write scaling.

**3. The bottleneck is structural, not key-local.** Zipf is never worse than
uniform, so it is not hot-key contention. Tail reservation is real but partial:
`mode=stripe` gives each thread its own tier-1 TTL bucket, which converts the
turn-over into a flat line and is a large win under skew, yet still plateaus.

| Cores | base uni | striped uni | base zipf | striped zipf |
|---|---|---|---|---|
| 1 | 1.44 | 1.45 | 1.24 | 1.25 |
| 2 | 1.71 | 1.77 | 1.69 | 1.73 |
| 4 | 1.88 | 1.96 | 2.02 | 2.25 |
| 6 | 1.89 | 1.92 | 2.02 | 2.38 |
| 8 | 1.65 | 1.90 | 1.85 | 2.45 |

At eight cores striping is +15% uniform and +32% zipf, and it removes the
turn-over. The residual ceiling is the shared free-segment supply and
merge-eviction pipeline, which striping cannot reach.

**4. Single-writer ownership is the structural fix; batching decides whether it
pays.** `mode=shard` is the no-routing upper bound (T private engines, keys
partitioned). `mode=delegate [batch]` is the practical bracket: half the cores
become workers routing by key hash to owner threads over bounded channels, with
global zipf so hot-shard imbalance is included.

| Cores | shard uni | shard zipf | deleg-b16 uni | deleg-b16 zipf | deleg-b1 uni |
|---|---|---|---|---|---|
| 1 | 1.43 | 1.26 | — | — | — |
| 2 | 2.50 | 2.29 | 1.42 | 1.81 | 1.03 |
| 4 | 3.91 | 3.90 | 2.08 | 2.56 | 1.70 |
| 6 | 4.91 | 4.96 | 2.53 | 3.10 | 2.08 |
| 8 | 5.51 | 5.63 | 2.87 | 3.41 | 1.83 |

Sharding scales monotonically to 3.3x the shared engine by removing every
shared write structure at once, and **zipf beats uniform in every sharded
configuration** — a single-writer shard turns hot-key writes into
cache-resident serial updates, so the hardest multi-writer case becomes the
easiest single-writer one.

Unbatched delegation is the cautionary result. At two cores it runs 1.03
against the shared engine's 1.71: the per-op handoff costs more than the
contention it removes, and it stays behind striping until six cores. Batch 16
fixes that, and real servers get the batching free by draining a connection's
pipeline per handoff.

**5. The partitioned layout is most of the win, and the wrapper is free.**
`mode=part P` is P engines behind a routing function, every thread reading and
writing every partition — layout without ownership or queues.

| Cores | base | P=1 | P=4 | P=16 | base zipf | P=16 zipf |
|---|---|---|---|---|---|---|
| 1 | 1.44 | 1.43 | 1.46 | 1.51 | 1.24 | 1.31 |
| 2 | 1.71 | 1.76 | 1.86 | 2.02 | 1.69 | 2.03 |
| 4 | 1.88 | 2.00 | 2.28 | 2.50 | 2.02 | 2.94 |
| 6 | 1.89 | 1.98 | 2.36 | 2.68 | 2.02 | 3.35 |
| 8 | 1.65 | 1.72 | 2.37 | 2.72 | 1.85 | 3.55 |

P=1 tracks base within 0.8% at one core, so the routing wrapper itself costs
nothing. P=16 is +4.6% at one core — smaller per-partition structures run
hotter — so there is no low-core regression to trade away. At eight cores the
layout alone reaches 1.65x/1.92x the shared engine, beating engine-level tail
striping with zero engine changes, because it partitions the free pool and
reclaim pipeline that striping cannot touch.

**6. Owner count is a real tuning parameter only once workers are busy.**
Eight cores, batch 16, uniform; `overhead_ns` is calibrated per-op worker
busy-work standing in for parse and socket cost.

| Owners | Workers | free workers | 500 ns/op workers |
|---|---|---|---|
| 2 | 6 | 1.99 | 2.00 |
| 3 | 5 | 2.43 | 2.40 |
| 4 | 4 | 2.87 | 2.38 |
| 6 | 2 | 3.22 | 1.62 |

With free workers, more owners is monotonically better — the workers are idle,
so every core is better spent applying writes. Model realistic worker cost and
the top end inverts: the optimum flattens at three to four owners, and six
owners **collapses by half** (3.22 to 1.62) because two busy workers cannot
feed six. An owner-sizing rule derived without a worker cost model will
over-provision owners.

Separately, `hybrid` mode on the *shared* engine tops out at 2.55 and gains
1.8% past three owners: they queue behind the same reclaim pipeline. That is a
third independent confirmation that segment recycling is the binding
constraint.

**7. Skewed reads have their own ceiling, and no write-side remedy touches it.**
Zipf reads reach 5.91x against uniform's 8.19x. Shuffling the prefill order —
same workload, hot keys spread across segments instead of packed into the ones
filled first — lifts them to 7.81x (+30%), while uniform reads move 0.5% and
zipf writes 0.0%. `Segcache::get` pins the item's segment with a SeqCst
`fetch_add` on that segment header's `ref_count`, so a read is a full-barrier
RMW on a line shared by every reader of that segment. Sharding, delegation, and
partitioning all leave it untouched.

**8. Architecture on record for write-heavy fleets:** reads execute in place
against the shared engine, writes batch-delegated to per-shard single-writer
owners, reply released after apply. The semantics argument — why direct readers
need no new machinery, and how read-your-writes and per-key linearizability
survive the split — moved to cache-rs `docs/handoff.md`, since it is about
segcache's publish protocol rather than anything pelikan implements. It is
framed there as a *reference* thread architecture: cache-rs owns no threads,
pelikan does.

## Open

- **Engine, in order:** tail striping per TTL bucket (cheap, converts the
  turn-over to flat); then parallel reclaim — per-stripe free lists, concurrent
  merge drains — which is where the residual ceiling lives.
- **Engine, newly identified:** per-segment reader pin traffic.
  `get_no_freq_incr` avoids the frequency CAS but not the pin, so the pin is
  what needs striping, an epoch scheme, or hazard pointers.
- **Not swept:** segment size, which is a parameter of the pin effect — pin
  contention scales with how many hot keys share a segment.
- **Not swept:** anything above eight cores. Sharding, the partitioned layout,
  and batched delegation are all still climbing at eight, so their ceilings are
  unmeasured; the shared engine and per-op delegation have already turned over.
- **For pelikan:** write-heavy configurations gain nothing past ~4 workers on
  the shared engine as it stands. Nothing here argued against #189, which is
  never worse than the serialized storage thread it replaced; the finding is a
  ceiling to raise next, not a regression to undo.
- **Caveat carried:** TTL 0 puts every writer on one TTL bucket — realistic for
  a single-default-TTL fleet, and the worst case for tail contention.

## Appendix: Skills Invoked

The earlier sessions of this effort predate the skill-roster convention and
left no record; this list covers the 2026-08-25/26 sessions only.

- `journal` — this entry.
- `engineering-journal` — the paired cache-rs entry.
- `technical-prose` — word-level pass over both entries.
- `pr` — PR #192.
- `artifact-design`, `dataviz` — the published chart sets.
