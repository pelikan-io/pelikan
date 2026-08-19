# Locally patched crates

These crates are copied from their published crates.io archives and selected
with the workspace's `[patch.crates-io]` table. Keep each patch minimal so it
can be rebased onto a future upstream release or submitted independently.

## `segcache` 0.3.0

- Upstream repository: <https://github.com/pelikan-io/cache-rs>
- Published version: `0.3.0`
- crates.io archive checksum: `511afdec590e313aa76a32e095840c623a9f10a5a33c67bd846901c2a8853ea5`
- Archive VCS revision: `4ce3405708e6831afa3c0abe3056090889e05aa5`
- Archive path: `crates/segcache`
- Local change: require `metriken` 0.9 instead of 0.7. No metric definitions,
  names, labels, values, features, or runtime code are changed.

This keeps Segcache's default `metrics` feature enabled while the Pelikan
workspace uses a single `metriken-core` 0.2 registration domain.

Compared with the published archive, this vendor tree omits the package-local
`Cargo.lock` and publication-source `Cargo.toml.orig`; it adds no files. Cargo's
`.cargo-ok` unpack-cache marker is not part of the archive and is not vendored.

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
  send, add FIFO async capacity backpressure with exact transport receive-error
  delivery, and preserve exact worker panic payloads through startup rollback.

The Ringline changes are generic and contain no Pelikan-specific fallback or
configuration behavior. The startup transaction merged as ringline-rs/ringline#309
but is not released on crates.io; send reservation/backpressure, receive-error
propagation, and panic-detail propagation are documented follow-ups for a separate upstream submission. See
`ringline-0.5.3/UPSTREAM-PR.md` for the standalone patch artifact and
verification commands.

Compared with the published archive, this vendor tree omits `Cargo.lock`,
`Cargo.toml.orig`, and `ROADMAP.md`; it adds `UPSTREAM-PR.md` and the standalone
`ringline-v0.5.3-startup-transaction.patch`. Cargo's `.cargo-ok` unpack-cache
marker is not part of the archive and is not vendored.
