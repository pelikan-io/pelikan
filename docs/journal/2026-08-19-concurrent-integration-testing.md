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
- **The `flush_all` smear window is much wider than the spec's paper
  description, and has two distinct components.**
  - *Ack-before-effect.* The admin thread replies `OK` once the broadcast is
    **queued**, not once any worker has cleared. Even with a **single** worker,
    writes acked after the client received `OK` were destroyed. The client
    contract "flush returned OK, so my subsequent writes survive" does not hold
    at any worker count.
  - *Inter-worker smear*, which scales badly. Workers drain the signal queue
    once per event-loop iteration, and an idle worker sits in `poll` for up to
    the 100ms worker timeout, so the last worker's `clear()` is bounded by that
    timeout rather than by anything tight. Measured window from broadcast to
    last destroyed acked write:

    | workers | smear window | acked-then-destroyed writes |
    |---|---|---|
    | 1 | 0.33 ms | 4 |
    | 2 | 0.47–0.56 ms | 6–7 |
    | 4 | 0.41 ms | 7 |
    | 8 | **37–49 ms** (5 consecutive runs) | 5–20 |

    Two orders of magnitude wider at 8 workers, reproducibly.

## Decided

- Nothing shipped. These are findings for the branch owner, not changes.
- Recommended before 0.4.3 / before the PR: give `integration_multi` genuinely
  concurrent clients, add `flush_all` coverage, and widen the RESP suite.
  Restate the branch's multi-worker claims in terms of what is actually
  covered.

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
