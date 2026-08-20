# Locally patched crates

These crates are copied from their published crates.io archives and selected
with the workspace's `[patch.crates-io]` table. Keep each patch minimal so it
can be rebased onto a future upstream release or submitted independently.

## `ringline` 0.5.3

- Upstream repository: <https://github.com/brayniac/ringline>
- Published version: `0.5.3`
- crates.io archive checksum: `93b934743f292f7ec5edc97def629f0609fbd857e5aa4cd6b472c9e4e3583d9a`
- Archive VCS revision: `da05b68890f22e6a511165eaa93e331588c218f1`
- Archive path: `ringline`
- Local change: make server launch transactional, reporting success only after
  worker initialization and deferring bind/listen until the startup commit;
  rollback closes launch-owned descriptors and joins started workers. The
  reviewed correction binds the io_uring event loop before submitting an SQE
  that references inline storage and uses `OwnedFd` for Mio read-end ownership.
  Generic follow-ups reserve all copy-send slots before committing a logical
  send, add lazy FIFO async capacity backpressure with current-local-task wake
  routing, ID-keyed completion and cancellation, preserve exact half-close
  results, deliver exact transport receive errors, and preserve worker panic
  payloads through startup rollback.

The Ringline changes are generic and contain no Pelikan-specific fallback or
configuration behavior. The startup transaction merged as ringline-rs/ringline#309
but is not released on crates.io; send reservation/backpressure, receive-error
propagation, logical-send identity (including coalesced-POLLOUT error cleanup),
movable-future owner refresh, and panic-detail propagation are documented
follow-ups for a separate upstream submission. See
`ringline-0.5.3/UPSTREAM-PR.md` for the standalone patch artifact and
verification commands.

- Standalone patch SHA-256:
  `3755ff1bd37608c8b6b482f516186c8ccdfd3d96f951e56a5f8e87b45e42f706`

Compared with the published archive, this vendor tree omits `Cargo.lock`,
`Cargo.toml.orig`, and `ROADMAP.md`; it adds `UPSTREAM-PR.md` and the standalone
`ringline-v0.5.3-startup-transaction.patch`. Cargo's `.cargo-ok` unpack-cache
marker is not part of the archive and is not vendored.
