# Locally patched crates

These crates are copied from their published crates.io archives and selected
with the workspace's `[patch.crates-io]` table. Keep each patch minimal so it
can be rebased onto a future upstream release or submitted independently.

## `segcache` 0.3.0

- Upstream repository: <https://github.com/pelikan-io/cache-rs>
- Published version: `0.3.0`
- Archive VCS revision: `4ce3405708e6831afa3c0abe3056090889e05aa5`
- Archive path: `crates/segcache`
- Local change: require `metriken` 0.9 instead of 0.7. No metric definitions,
  names, labels, values, features, or runtime code are changed.

This keeps Segcache's default `metrics` feature enabled while the Pelikan
workspace uses a single `metriken-core` 0.2 registration domain.

## `ringline` 0.5.3

- Upstream repository: <https://github.com/brayniac/ringline>
- Published version: `0.5.3`
- Archive VCS revision: `da05b68890f22e6a511165eaa93e331588c218f1`
- Archive path: `ringline`
- Local change: make server launch transactional, reporting success and
  startup rollback closes launch-owned descriptors and joins started workers.

The Ringline change is generic and contains no Pelikan-specific fallback or
configuration behavior. See `ringline-0.5.3/UPSTREAM-PR.md` for the standalone
patch artifact, draft PR text, and upstream verification commands.
  starting the acceptor only after every worker completes initialization;
