# Concurrent integration testing of the shared-`Arc<Seg>` workers

> **Status: uncommitted draft.** Written by an agent session that was asked not
> to commit to `concurrent-segcache`. Review and commit (or discard) as you see
> fit. The findings below are all reproducible: the harness is preserved
> alongside this entry in `2026-08-19-concurrent-driver/` (also uncommitted) —
> `conc.py` is the multi-connection driver, `runconc.sh` launches one server and
> one phase, `runall.sh` runs the matrix, `stress.sh` is the oversubscription
> loop. `conc.py` is the piece worth turning into a real test target.

## What

Integration-test pelikan against cache-rs `main` (`2788fcf`, the pending 0.4.3
line) before it publishes, and against published segcache 0.4.2 as a control.
Everything ran in throwaway git worktrees with a temporary `[patch.crates-io]`
pin; nothing was committed to the branch.

## Found

- **`integration_multi` has never tested a multi-worker system.** It starts N
  workers and then opens **one connection at a time**, issuing ~25 requests
  sequentially with 10ms sleeps. Sessions are handed to workers uniformly at
  random (`queues.rs::try_send_any`), so with a single live connection exactly
  one worker ever handles traffic. The whole premise of this branch — N workers
  sharing one `Arc<Seg>` and executing requests in place — was not exercised by
  the test named for it. Every "green, single- and multi-worker" claim on this
  branch, including in the previous journal entry, rests on that test.
  - The RESP suite is thinner still: three assertions total (`get miss`,
    `set and get`, `version`) for a protocol adapter we ship.
  - `flush_all` — the one genuinely N-worker-concurrent operation in pelikan —
    has no integration coverage at all.
- **Under real concurrent load, the engine holds.** A multi-connection driver
  (32 connections, hot contended keys, both engines, 2 and 8 workers, with
  per-thread CPU sampled to *prove* the workers were all active) found zero
  genuine defects: no lost `incr` updates across 48,000 increments on 4 hot
  keys, no `cas` succeeding against a stale token across ~7,000 successful
  compare-and-swaps with ~18,600 `EXISTS` rejections, no phantom values, no
  false misses across 48,000 read-your-writes, and no protocol errors.
  `2788fcf` and 0.4.2 are indistinguishable on every measure, including
  engine gauge values through merge eviction and item relocation.
- **The `add` check-then-act race is real and its rate scales with worker
  count** — measured at ~2-13% of rounds with 2 workers and ~33% with 8
  (24 racing connections, 300 rounds). This is the accepted behaviour already
  recorded in the design spec, now with a number attached.
- **The `flush_all` window is set by the duration of `clear()`, not by wake
  latency and not by inter-worker skew.** This corrects both the spec's stated
  mechanism and an earlier draft of this entry. Established by instrumenting
  the admin broadcast and every worker's `clear()` with lock-free atomic
  timestamps (`flushtrace-instrumentation.patch`; an earlier `eprintln!`
  version perturbed the very interleaving it measured and had to be discarded).

  | build | workers | worker wake latency | inter-worker skew | `clear()` duration | window |
  |---|---|---|---|---|---|
  | release | 1 | 51 µs | – | 5.65 ms | 0.64 ms |
  | release | 2 | 35 µs | 5 µs | 8.44 ms | 0.37 ms |
  | release | 4 | 40 µs | 9 µs | 8.59 ms | 8.64 ms |
  | release | 8 | 35–82 µs | 29 µs | 6.2–7.4 ms | 6.7–8.2 ms |
  | debug | 8 | 75–152 µs | 57–69 µs | 41–46 ms | 41–47 ms |

  - **The wake fix (`b4c50a7`) works correctly at every worker count.** Every
    worker receives the broadcast within **35–152 µs**, with inter-worker skew
    of at most ~70 µs. No worker ever waits on the 100 ms poll timeout. The
    "37–49 ms" figure in the first draft of this entry was a **debug-build**
    `clear()` duration, reported without noting the profile — in release the
    same measurement is 6–8 ms.
  - **`clear()` costs ~6–8 ms in release regardless of heap size or how much
    is cached.** `TtlBuckets::clear` walks all `TOTAL_BUCKETS = 256 × 4 = 1024`
    buckets and takes each bucket's `std::sync::Mutex` chain lock. 64MB, 256MB
    and 1GB heaps all measured the same, so this is a fixed per-flush cost, not
    proportional to the data being discarded.
  - **The window is the sweep, not the skew.** Writes landing in a bucket the
    sweep has not yet reached are acked and then destroyed by that same sweep.
    At 1–2 workers every connection stalls behind its own worker's sweep, so
    almost nothing is acked mid-sweep and the window stays sub-millisecond; at
    4–8 workers a worker that finishes early resumes serving while others are
    still sweeping, so the window widens to the full sweep duration. The
    spec's "between the first and last worker's clear" names a real
    multi-worker effect but attributes it to a skew that is ~70 µs, three
    orders of magnitude too small to explain the observed window.
  - *Ack-before-effect* is a separate, **inherited** behaviour — see below.

- **Ack-before-effect predates the conversion; the duplicate clear does not.**
  Verified against the branch point (`6dc2e98`) rather than assumed:
  - Pre-conversion, the admin handler was
    `let _ = self.signal_queue_tx.try_send_all(Signal::FlushAll); session.send(Ok)?;`
    — it replied `OK` on enqueue, before any clear, exactly as it does now. So
    ack-before-effect is **inherited**, and the PR should describe it as such.
    The old path had **no `wake()` at all**, so the storage thread saw the
    signal only on its next poll; `b4c50a7` made this strictly better.
  - Pre-conversion, `workers/multi.rs` **explicitly ignored** the signal
    (`Signal::FlushAll => {}`) and only the dedicated storage thread cleared;
    `workers/single.rs` cleared once because it owned the storage. **Exactly
    one `clear()` ever ran.** The N-redundant-clear behaviour, and with it the
    "destroyed by a later duplicate clear" race, is **new to this conversion**.

- **Every detector in the driver was proven non-vacuous by fault injection.**
  `conc.py --broken` injects the exact defect each phase claims to catch and
  the phase must fail; on the real code it must pass:

  | phase | injected defect | clean | broken |
  |---|---|---|---|
  | `incr` | networked read-modify-write instead of `incr` | 0 lost | 4/4 keys lost |
  | `cas` | blind `set`, token ignored | 0 stale | 4/4 keys lost |
  | `mixed` | write a value never recorded | 0 phantom | 2113 phantom |
  | `ryw` | read an unwritten key / expect an unwritten value | 0 | 2400 misses + 2400 mismatches |
  | `add` | `set` instead of `replace` on an absent key | 0/40 | 40/40 resurrected |
  | `resp` | as `mixed` + `ryw`, over RESP | 0 | 3158 phantom + 3242 misses |

  Residual gap, stated rather than papered over: the RESP *mismatch* arm did
  not fire under injection (the injection produces misses and phantoms), so it
  is covered only by sharing its comparison with `ryw`'s proven arm.

## Decided

- Nothing shipped. These are findings for the branch owner, not changes.
- Recommended before 0.4.3 / before the PR: give `integration_multi` genuinely
  concurrent clients, add `flush_all` coverage, and widen the RESP suite.
  Restate the branch's multi-worker claims in terms of what is actually
  covered, and rewrite the spec's flush_all paragraph around the sweep
  duration rather than inter-worker skew.
- **`conc.py` should become a real test target**, and it is close to being one.
  What it would take: port the phases to Rust as a `harness = false` test
  beside `integration_multi` (they are plain TCP and `std::thread`, no
  dependencies); replace the fixed 10 s startup sleep with a connect-retry
  loop; keep the `--broken` fault-injection mode as a self-test so the suite
  proves its own detectors on every run; and pick op counts that keep it
  under a few seconds per phase. The `flush` phase should assert the window is
  bounded rather than print it, so a regression that widens it fails CI.
- Worth considering separately: since storage is a shared `Arc<Seg>`, only one
  `clear()` is semantically needed. Broadcasting to all N workers buys N-1
  redundant 6–8 ms sweeps and creates the duplicate-clear race that the spec
  then has to document as accepted.

## Cost note — `kill $LOADPIDS` does not word-split in zsh

Root cause of the orphaned busy-loops that spun on this machine for ~5.5 hours
and were misattributed to live work by two sessions.

The idiom is the standard one:

```sh
LOADPIDS=""
for i in $(seq 1 256); do
  busy_loop & LOADPIDS="$LOADPIDS $!"
done
trap 'kill $LOADPIDS 2>/dev/null' EXIT INT TERM
```

In **bash** this works: unquoted `$LOADPIDS` undergoes word splitting, so `kill`
receives 256 arguments. In **zsh** it does not — unquoted parameter expansion is
*not* word-split by default, so `kill` receives one argument (`" 29135 29136
…"`), fails, and every loop is orphaned to PID 1. The trap fires and reports
success; nothing indicates a leak. This reproduced exactly during this session:
an inline zsh probe leaked all 256 loops while the identical logic in a `bash`
script left zero.

Mitigations, in order of reliability:

1. **Make the load generator self-limiting** so it dies regardless of whether
   any cleanup runs. This is the one that actually saved the session:
   ```sh
   bash -c 'end=$((SECONDS+900)); while : ; do [ $SECONDS -ge $end ] && break; done' &
   ```
   A trap is best-effort — it cannot fire on `SIGKILL`, on a parent that dies
   first, or when the kill itself silently fails. A self-limiting worker needs
   no cleanup to be correct.
2. Run any such harness under `bash` explicitly (`bash harness.sh`), never as an
   inline `zsh -c` one-liner. Note that Claude Code's Bash tool runs `zsh` on
   this machine, so inline compound commands hit exactly this trap.
3. Prefer a pid **array** (`LOADPIDS+=($!)` / `kill "${LOADPIDS[@]}"`), which is
   correct in both shells.
4. Always verify rather than assume: `pgrep -f 'while :' | wc -l` must be `0`
   after cleanup, and it belongs in the harness itself, not only in the report.
