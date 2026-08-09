# README accuracy pass

## What

Bring `README.md` back in line with the current codebase using the
`document-feature` skill: verified every claim against rendered `--help`, a
live server session, and the Cargo workspace, then fixed what drifted.

## Decided

- Products list now includes all four binaries (`pelikan-rds` and
  `pelikan-pingproxy` were missing).
- The C toolchain/cmake requirement stays — the reason changed (AWS-LC via
  rustls, not BoringSSL), so the rationale was corrected rather than the
  requirement dropped.
- Default ports (12321 data, 9999 admin, 9998 HTTP admin in the sample
  config), shutdown behavior, and a test/port-conflict warning are now stated
  as facts instead of being inferable only from example transcripts.
- The `stats` example stays compact: the blank lines in real output come from
  a `join("\r\n")` bug on already-terminated lines
  (`src/protocol/admin/src/admin.rs:193`), so documenting them would enshrine
  a bug. `END` terminator added to the example instead.

## Open

- Bug: spurious blank line between every `STAT` row in admin `stats` output
  (`src/protocol/admin/src/admin.rs:193`) — fix separately.
- Papercut: pingproxy package name is `pingproxy` while its bin is
  `pelikan-pingproxy`; `cargo build -p pelikan-pingproxy` fails.
- Repo has no `CONTRIBUTING.md` or issue/PR templates despite inviting
  contributions.
