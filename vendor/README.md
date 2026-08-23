# Locally patched crates

These crates are copied from their published crates.io archives and selected
with the workspace's `[patch.crates-io]` table. Keep each patch minimal so it
can be rebased onto a future upstream release or submitted independently.

## `ringline` 0.5.5

- Upstream repository: <https://github.com/brayniac/ringline>
- Published version: `0.5.5`
- crates.io archive checksum: `2943e42082464b95e21eb43008547c8f1dbeff260b3fed1df56f725effb16277`
- Archive VCS revision: `cd79112f037282d40d6e510161e3dc9aa01aceca`
- Archive path: `ringline`
- Local changes: add result-aware receive access and FIFO bounded sends with
  transactional copy-pool admission, exact logical-operation identity,
  cancel-safe task ownership, permit retention through socket completion,
  backend-specific teardown/error propagation, and exact worker-startup panic
  diagnostics.

Ringline 0.5.5 already contains the transactional listener startup merged as
ringline-rs/ringline#309. Pelikan does not carry a duplicate startup patch.
The remaining changes are generic Ringline runtime behavior and contain no
Pelikan protocol, configuration, or fallback policy. See
`ringline-0.5.5/UPSTREAM-PR.md` for the standalone patch artifact and
verification commands.

Compared with the published archive, this vendor tree omits `Cargo.lock`,
`Cargo.toml.orig`, and `ROADMAP.md`; it adds `UPSTREAM-PR.md` and the standalone
`ringline-v0.5.5-runtime-followups.patch`. Cargo's `.cargo-ok` unpack-cache
marker is not part of the archive and is not vendored.
