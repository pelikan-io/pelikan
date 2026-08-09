# CI build speedups

## What

Investigated why every PR push runs CI twice and what else slows the build.
Measured from actual run history: pushes to same-repo PR branches fire both
`push` and `pull_request` events (verified: head SHA `a272ed16` produced four
simultaneous workflow runs), duplicating ~18 jobs / ~70 raw runner-minutes per
push — more in billed minutes since four of the duplicated jobs run on macOS.

## Decided

- Scope `push` triggers to `main` in both workflows; `pull_request` covers PR
  branches (fork PRs already only get `pull_request` runs).
- Add concurrency cancellation to `cargo.yml` (`fuzz.yml` already had it), so
  rapid successive pushes stop stacking complete 35-minute run-sets.
- Add `protocol/resp` to the fuzz warm-up build loop: the fuzz jobs restore
  the shared cache with `save-if: false`, so resp — absent from the loop that
  populates the cache — was compiled cold on every run, forever.
- Drop the rust-cache restore from the `audit` job: `cargo audit` only reads
  `Cargo.lock`; restoring a build cache there was pure overhead.
- Bump `codeql-action/upload-sarif` v2 → v3 (v2 is past end of support).

## Open

Deliberately not done here, as they change artifacts or have tradeoffs:

- Release profile carries `debug = true` + fat LTO + `codegen-units = 1`,
  making the release build+test jobs the critical path (8-13 min). Options: a
  CI-only `[profile.ci-release]` with thin LTO, or `debug = "line-tables-only"`
  in release. Changes shipped artifacts — maintainer decision.
- rust-cache saves from every branch may evict main's caches under the 10GB
  cap; `save-if: github.ref == 'refs/heads/main'` on big jobs is an option.
- `clippy` runs with `continue-on-error: true`, so clippy failures never gate
  `check-success` — only SARIF annotations. Verify that's intentional.
- The `check-{os}` jobs are subsumed by `build-{os}-debug`; their only value
  is ~1-minute fast feedback. The macOS leg is 10x-billed.
