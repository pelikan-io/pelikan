# `flush_all`: one sweep, on the control plane, before the ack

## What

`flush_all` had two defects on `concurrent-segcache`, both quantified in the
previous entry (`2026-08-19-concurrent-integration-testing.md`) and the design
spec: the admin broadcast `Signal::FlushAll` to all N workers and each called
`storage.clear()` on the *shared* `Arc<Seg>` (N-1 wasted ~6-8 ms sweeps, new to
this conversion), and it acked `OK` as soon as the broadcast was queued, so
writes the client issued after being told `OK` were destroyed by the sweep that
followed (inherited from the branch point, present even at `threads = 1`).

## Decided

**The admin thread holds a clear handle, sweeps itself, and only then acks.**
`ProcessBuilder::new` hoists `Arc::new(storage)` and hands `AdminBuilder` an
`Arc<dyn Fn() + Send + Sync>` closing over it. The handle is `Option`al so the
proxy, which has no storage, keeps the old broadcast path. Workers ignore
`Signal::FlushAll`, and the server no longer sends it.

Type erasure rather than making `Admin` generic over `Storage`: one call site
is not worth a `Storage` parameter through every admin-side type.

**Considered and rejected: at-most-once on the data plane** — a single control
message that exactly one worker pops, or a CAS-guarded flush generation. It
gets one sweep and reuses the existing queues, but (1) it does not fix the
contract, since the admin still acks on enqueue — and we measured the
ack-before-effect at `threads = 1`, where no duplication exists, so duplication
was never the cause of that half; recovering ack-after-effect would need a
completion signal back to admin, which is more machinery than the handle; and
(2) whichever worker wins the pop stops serving its sessions for the whole
6-8 ms sweep. On admin, no data-plane thread blocks at all.

The property is **exactly-once on the control plane** — not "at most once".
The admin performs the clear unconditionally and synchronously; there is no
election and no possibility of zero.

**Accepted, deliberately:** the admin thread is blocked for the sweep and
serves no other admin request meanwhile. `flush_all` is rare and inherently
heavy, and the control plane is where that cost belongs.

**Not overclaimed:** writes still in flight *during* the sweep can still be
destroyed. That is unavoidable without quiescing the workers and matches
memcached, where writes concurrent with a flush are undefined. The contract
this establishes is the narrower, client-usable one: **after the client
receives `OK`, subsequent writes survive.**

### Measured (release, `conc.py flush`, 8 writer connections)

| workers | smear window before | after | writes destroyed after `OK`, before | after |
|---|---|---|---|---|
| 1 | 0.16-0.23 ms | 0.14-0.26 ms | 0-5 | 0 |
| 2 | 0.19-0.26 ms | 0.24-0.38 ms | 1-4 | 0 |
| 4 | 8.41 ms | 0.22-0.30 ms | 4 | 0 |
| 8 | 6.43 ms | 0.23-0.30 ms | 13 | 0 |

Admin round trip went from 0.17-0.23 ms to 6.5-9.6 ms — one sweep, and the
same one sweep at 8 workers as at 1, which is the exactly-once evidence.
Three reps per worker count after the fix.

### Tests

- `flush_all_tests` in `src/server/segcache/tests/common.rs`, run by both
  `integration` and `integration_multi`. Writers record a key as "post-`OK`"
  only when they observe the ack flag *before* sending that write, so the
  ordering is sound in the one direction that matters; five rounds, since the
  pre-fix defect is a race. Also asserts that keys written before the flush
  are gone, so a no-op flush cannot pass.
- `integration_multi` now runs **8** workers, not 2. At two workers a single
  connection is almost always served by the same one, so nothing about sharing
  the storage was exercised; eight also makes the flush race observable.
- **Proven red against the pre-fix code: 6 runs, 6 failures** — e.g.
  `flush_all acked before it took effect: 10 of 57351 writes issued AFTER the
  admin returned OK were destroyed by the sweep`. Green in 3/3 release runs
  and in `cargo test --workspace` after the fix. At one worker the pre-fix
  code passes (every connection stalls behind that worker's own sweep), so the
  multi-worker binary is the red-capable detector.
- `conc.py`'s `flush` phase now **asserts** the bound instead of printing it,
  and gained a `--broken` injection (ack on enqueue) like every other phase.
  Self-test: clean `rc=0`, broken `rc=1` with 8 destroyed.

## Open

- Nothing outstanding for `flush_all`. The engine-level check-then-act race on
  `add`/`replace` remains as recorded in the spec.

## Appendix: Skills Invoked

- None. (This effort ran from a written brief rather than a skill.)
