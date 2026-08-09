# Agent Instructions Restructure Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move pelikan's conditional agent-facing guidance out of CLAUDE.md into shipped, pluggable skills under `.agent/` (symlinked into `.claude/`), add an in-repo enforced engineering journal, and leave CLAUDE.md as a thin "load-bearing plus index" residue.

**Architecture:** `.agent/skills/*` and `.agent/hooks/*` become the source of truth; `.claude/skills/*` and `.claude/hooks/*` become symlinks into them, registered via `.claude/settings.json`. `docs/ARCHITECTURE.md` absorbs the detailed workspace-layout prose currently inline in CLAUDE.md (and corrects two stale references found during research — see Task 6). `docs/journal/` becomes the enforced-but-non-blocking engineering journal location.

**Tech Stack:** Markdown (skills, docs), Bash (hook script), JSON (`.claude/settings.json`), git (mv + symlinks).

**Reference:** `docs/plans/2026-08-08-agent-instructions-restructure-design.md` — the validated design this plan implements. Read it first if anything below is ambiguous.

**Branch:** work happens on `agent-instructions-restructure` (already checked out; contains the design-doc commit `d86c075`).

---

### Task 1: Scaffold `.agent/`, migrate `pr` and `release` skills

**Files:**
- Create: `.agent/skills/pr/SKILL.md` (moved from `.claude/skills/pr/SKILL.md`)
- Create: `.agent/skills/release/SKILL.md` (moved from `.claude/skills/release/SKILL.md`)
- Create (symlink): `.claude/skills/pr` -> `../../.agent/skills/pr`
- Create (symlink): `.claude/skills/release` -> `../../.agent/skills/release`

**Step 1: Move the two existing skills with git mv**

```bash
mkdir -p .agent/skills
git mv .claude/skills/pr .agent/skills/pr
git mv .claude/skills/release .agent/skills/release
```

**Step 2: Verify the move**

Run: `git status`
Expected: two renames shown (`.claude/skills/pr/SKILL.md` -> `.agent/skills/pr/SKILL.md`, same for `release`), nothing else changed.

**Step 3: Replace the old paths with symlinks**

```bash
cd .claude/skills
ln -s ../../.agent/skills/pr pr
ln -s ../../.agent/skills/release release
cd ../..
```

**Step 4: Verify the symlinks resolve**

Run: `readlink -f .claude/skills/pr/SKILL.md && readlink -f .claude/skills/release/SKILL.md`
Expected: both resolve to the absolute paths of `.agent/skills/pr/SKILL.md` and `.agent/skills/release/SKILL.md`.

Run: `cat .claude/skills/pr/SKILL.md | head -3`
Expected: prints the `pr` skill's frontmatter (proves the symlink reads through correctly).

**Step 5: Commit**

```bash
git add .agent/skills/pr .agent/skills/release .claude/skills/pr .claude/skills/release
git commit -m "chore: move pr/release skills under .agent/, symlink from .claude/"
```

---

### Task 2: Add the `run` skill

**Files:**
- Create: `.agent/skills/run/SKILL.md`
- Create (symlink): `.claude/skills/run` -> `../../.agent/skills/run`

**Step 1: Write the skill**

Create `.agent/skills/run/SKILL.md`:

```markdown
---
name: run
description: Launch a pelikan product binary with its example config
---

Launch one of pelikan's product binaries.

## Products

| Binary | Purpose | Example config |
|---|---|---|
| `pelikan-segcache` | Memcached-compatible server, Segcache storage | `config/segcache.toml` (or `config/twemcache-tls.toml` for TLS) |
| `pelikan-pingserver` | Ping protocol server for testing/benchmarking | `config/pingserver.toml` (or `config/pingserver-tls.toml` for TLS) |
| `pelikan-rds` | RESP (Redis protocol) server | `config/rds.toml` |
| `pelikan-pingproxy` | Ping protocol proxy | `config/pingproxy.toml` |

## Steps

1. Build if needed: `cargo build --workspace --release`
2. Run with a config file: `target/release/<binary> config/<name>.toml`
   - Without a config file argument, the binary starts with built-in defaults.
3. `target/release/<binary> --help` lists all flags and overrides.

## Notes

- Admin port defaults to 9999 (stats/management), separate from the data port.
- See the `test` skill for running the integration test harnesses instead of a live binary.
```

**Step 2: Symlink from `.claude/`**

```bash
ln -s ../../.agent/skills/run .claude/skills/run
```

**Step 3: Verify**

Run: `cat .claude/skills/run/SKILL.md | head -3`
Expected: prints the `run` skill's frontmatter.

**Step 4: Commit**

```bash
git add .agent/skills/run .claude/skills/run
git commit -m "feat: add run skill for launching product binaries"
```

---

### Task 3: Add the `test` skill

**Files:**
- Create: `.agent/skills/test/SKILL.md`
- Create (symlink): `.claude/skills/test` -> `../../.agent/skills/test`

**Step 1: Write the skill**

Create `.agent/skills/test/SKILL.md`:

```markdown
---
name: test
description: Run pelikan's unit, integration, and fuzz tests
---

Run tests across the pelikan workspace.

## Unit tests

Alongside source files in each crate:
```bash
cargo test --workspace          # everything
cargo test -p pelikan-segcache  # one product crate, e.g. segcache
```

## Integration tests

Custom harnesses, not plain `#[test]` functions — currently exist for `segcache` and `rds`:
- `src/server/segcache/tests/integration.rs` — single-threaded instance
- `src/server/segcache/tests/integration_multi.rs` — multi-threaded instance
- `src/server/segcache/tests/common.rs` — shared test utilities
- `src/server/rds/tests/integration.rs`, `integration_multi.rs`, `common.rs` — same pattern for RDS

They build and run as part of `cargo test -p pelikan-segcache` / `-p pelikan-rds`.

## Fuzz tests (requires nightly)

Fuzz targets exist for the protocol parsers:
- `src/protocol/admin/fuzz/`
- `src/protocol/memcache/fuzz/`
- `src/protocol/ping/fuzz/`
- `src/protocol/resp/fuzz/`

```bash
cd src/protocol/<name>/fuzz
cargo +nightly fuzz list          # see available targets
cargo +nightly fuzz run <target>
```

## Notes

- The segment-based storage engine itself now lives in the external `cache-rs` repository (consumed as the `segcache` dependency by `src/entrystore/`) — it has no fuzz targets or tests in this repo.
```

**Step 2: Symlink from `.claude/`**

```bash
ln -s ../../.agent/skills/test .claude/skills/test
```

**Step 3: Verify**

Run: `cat .claude/skills/test/SKILL.md | head -3`
Expected: prints the `test` skill's frontmatter.

**Step 4: Commit**

```bash
git add .agent/skills/test .claude/skills/test
git commit -m "feat: add test skill for unit/integration/fuzz workflows"
```

---

### Task 4: Add the `journal` skill and `docs/journal/` convention

**Files:**
- Create: `.agent/skills/journal/SKILL.md`
- Create (symlink): `.claude/skills/journal` -> `../../.agent/skills/journal`
- Create: `docs/journal/README.md`

**Step 1: Write the skill**

Create `.agent/skills/journal/SKILL.md`:

```markdown
---
name: journal
description: Scaffold a docs/journal/ entry for a non-trivial engineering effort
---

Create or continue an entry in `docs/journal/` — pelikan's in-repo, contributor-portable
engineering journal. Use when starting, continuing, handing off, or closing a non-trivial
effort, or when preserving a negative result worth remembering.

## Convention

- Path: `docs/journal/YYYY-MM-DD-<slug>.md`
- One file per effort; append to the same file as it continues rather than creating a new
  one each session.
- Sections:
  - `## What` — the effort or problem, one or two sentences
  - `## Decided` — what was decided and why
  - `## Open` — what's still unresolved

## Steps

1. Check `docs/journal/` for an existing entry covering this effort; continue it if found.
2. Otherwise create `docs/journal/<today>-<slug>.md` using the convention above.
3. Keep it short — this is a log, not a design brief. If the effort warrants a fuller
   paired problem/design brief, that's a separate, heavier step (the `frame-problem` /
   `propose-design` skills, if `skills-mcp` is connected) — not a replacement for this file.

## Notes

- A pre-commit hook (`.agent/hooks/pre-commit-check.sh`) nudges — non-blocking — when a
  commit touches `src/` without a corresponding `docs/journal/*.md` entry staged.
- This is separate from, not a replacement for, the MCP `engineering-journal` /
  knowledge-iop vault skills — those stay optional, for maintainers who want to mirror
  significant entries cross-project.
```

**Step 2: Symlink from `.claude/`**

```bash
ln -s ../../.agent/skills/journal .claude/skills/journal
```

**Step 3: Create the journal directory with its README**

Create `docs/journal/README.md`:

```markdown
# Journal

In-repo engineering journal. See the `journal` skill (`.agent/skills/journal/SKILL.md`)
for the convention and workflow. One file per non-trivial effort:
`YYYY-MM-DD-<slug>.md`.
```

**Step 4: Verify**

Run: `cat .claude/skills/journal/SKILL.md | head -3 && ls docs/journal/`
Expected: prints frontmatter, then lists `README.md`.

**Step 5: Commit**

```bash
git add .agent/skills/journal .claude/skills/journal docs/journal/README.md
git commit -m "feat: add journal skill and docs/journal/ convention"
```

---

### Task 5: Add the pre-commit journal-nudge hook

**Files:**
- Create: `.agent/hooks/pre-commit-check.sh`
- Create (symlink): `.claude/hooks/pre-commit-check.sh` -> `../../.agent/hooks/pre-commit-check.sh`
- Create: `.claude/settings.json`

**Step 1: Write the hook script**

Create `.agent/hooks/pre-commit-check.sh`:

```bash
#!/usr/bin/env bash
# PreToolUse hook: nudge toward docs/journal/ when a non-trivial src/ change
# is committed without one. Non-blocking — always exits 0.
set -euo pipefail

command -v jq >/dev/null 2>&1 || exit 0

input="$(cat)"
tool_name="$(echo "$input" | jq -r '.tool_name // empty')"
command_str="$(echo "$input" | jq -r '.tool_input.command // empty')"

[ "$tool_name" = "Bash" ] || exit 0
echo "$command_str" | grep -Eq 'git[[:space:]]+commit' || exit 0

staged="$(git diff --cached --name-only 2>/dev/null || true)"
[ -n "$staged" ] || exit 0

nontrivial_src="$(echo "$staged" | grep -E '^src/' | grep -Ev '^src/.*Cargo\.(toml|lock)$' || true)"
[ -n "$nontrivial_src" ] || exit 0

has_journal="$(echo "$staged" | grep -E '^docs/journal/.*\.md$' || true)"
[ -n "$has_journal" ] && exit 0

echo "Reminder: this commit touches src/ but no docs/journal/*.md entry is staged. Consider running the 'journal' skill to log this effort (non-blocking)." >&2
exit 0
```

Make it executable:

```bash
chmod +x .agent/hooks/pre-commit-check.sh
```

**Step 2: Symlink from `.claude/`**

```bash
mkdir -p .claude/hooks
ln -s ../../.agent/hooks/pre-commit-check.sh .claude/hooks/pre-commit-check.sh
```

**Step 3: Register the hook in `.claude/settings.json`**

Create `.claude/settings.json`:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": ".claude/hooks/pre-commit-check.sh"
          }
        ]
      }
    ]
  }
}
```

**Step 4: Verify the JSON is well-formed**

Run: `jq . .claude/settings.json`
Expected: pretty-printed JSON, no parse error.

**Step 5: Verify the hook logic manually (two cases)**

Case A — non-trivial src/ change, no journal entry staged, expect a reminder:

```bash
echo '// scratch' > src/common/src/.hook-test-scratch.rs   # new file, so `git add` actually stages it
git add src/common/src/.hook-test-scratch.rs
echo '{"tool_name":"Bash","tool_input":{"command":"git commit -m test"}}' | .agent/hooks/pre-commit-check.sh
git reset src/common/src/.hook-test-scratch.rs
rm src/common/src/.hook-test-scratch.rs
```
Expected: the reminder line prints to stderr, exit code 0.

Case B — same src/ change, but a journal entry is also staged, expect silence:

```bash
echo '// scratch' > src/common/src/.hook-test-scratch.rs
git add src/common/src/.hook-test-scratch.rs
touch docs/journal/.hook-test-scratch.md
git add docs/journal/.hook-test-scratch.md
echo '{"tool_name":"Bash","tool_input":{"command":"git commit -m test"}}' | .agent/hooks/pre-commit-check.sh
git reset src/common/src/.hook-test-scratch.rs docs/journal/.hook-test-scratch.md
rm src/common/src/.hook-test-scratch.rs docs/journal/.hook-test-scratch.md
```
Expected: no output, exit code 0.

**Step 6: Commit**

```bash
git add .agent/hooks/pre-commit-check.sh .claude/hooks/pre-commit-check.sh .claude/settings.json
git commit -m "feat: add non-blocking pre-commit journal nudge hook"
```

---

### Task 6: Extract `docs/ARCHITECTURE.md`

While gathering file paths for Tasks 2–3, `src/storage/segcache/` and `src/storage/datatier/`
were found not to exist — they were moved to the external `cache-rs` repo in commits
`f11be9b` and `8b43ea6`. CLAUDE.md's current Architecture section still lists them. This
task both extracts the section into its own doc and corrects that staleness.

**Files:**
- Create: `docs/ARCHITECTURE.md`

**Step 1: Write the corrected architecture doc**

Create `docs/ARCHITECTURE.md`:

```markdown
# Pelikan Architecture

## Workspace Structure

The workspace is organized in layers:

**Core Infrastructure** (`src/`)
- `common/` - Shared types and traits across servers
- `config/` - TOML-based configuration parsing
- `logger/` - Centralized logging with tracing
- `net/` - Networking abstractions, event loops, TLS support
- `session/` - Session management
- `entrystore/` - Entry storage type collection; wraps the external `cache-rs` `segcache`
  crate (segment-based storage engine, NSDI'21 paper) as its backing store

**Protocol Layer** (`src/protocol/`)
- `admin/` - Admin ASCII protocol for stats and management
- `memcache/` - Memcache ASCII protocol
- `ping/` - Simple ping/pong protocol
- `resp/` - Redis RESP protocol with sorted set support
- `http/` - HTTP protocol parser
- `common/` - Shared protocol traits

**Storage Support** (`src/storage/`)
- `bloom/` - Bloom filter implementations
- `types/` - Shared storage type definitions
- The segment-based storage engine itself (formerly `src/storage/segcache/` and
  `src/storage/datatier/` in this repo) now lives in the external `cache-rs` repository
  and is pulled in as the `segcache` dependency by `src/entrystore/`.

**Server Core** (`src/core/`)
- `admin/` - Admin thread infrastructure
- `server/` - Event loops, thread management, signal handling
- `proxy/` - Proxy thread infrastructure

**Server Products** (`src/server/`)
- `segcache/` - Memcache-compatible server
- `pingserver/` - Multi-protocol ping server
- `rds/` - RESP protocol server

**Proxies** (`src/proxy/`)
- `ping/` - Ping protocol proxy

## Key Design Patterns

- **Lockless data structures**: Worker threads never block
- **Control/data plane separation**: Admin port (default 9999) for management, data port
  for cache operations
- **Per-module config and metrics**: Each component has independent configuration and
  observability
- **Protocol/storage pluggability**: Easy to add new protocols or storage backends
```

**Step 2: Verify**

Run: `grep -c "src/storage/segcache\|src/storage/datatier" docs/ARCHITECTURE.md`
Expected: `0` as a standalone claim of current existence — the only mentions should be in
the "formerly ... now lives in ... cache-rs" sentence. (Eyeball the file to confirm; the
grep is just a sanity check that the stale paths weren't copied verbatim as still-current.)

**Step 3: Commit**

```bash
git add docs/ARCHITECTURE.md
git commit -m "docs: extract architecture doc, fix stale storage/segcache references"
```

---

### Task 7: Thin CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Replace CLAUDE.md's contents**

Replace the full file with:

```markdown
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in
this repository.

## Project Overview

Pelikan is a Rust framework for developing high-performance cache services. It
emphasizes modularity through reusable components, enabling rapid development of
different caching solutions. The project includes multiple server products that share
common infrastructure.

## Products

- `pelikan-segcache` - Memcached-compatible server with Segcache storage (TTL-centric,
  high memory efficiency)
- `pelikan-pingserver` - Ping server for testing and benchmarking
- `pelikan-rds` - RESP (Redis protocol) server
- `pelikan-pingproxy` - Ping protocol proxy

See the `run` skill for how to launch them and the `test` skill for how to test them.

## Build Commands

```bash
cargo build --workspace --release   # release build
cargo build --workspace             # debug build
cargo test --workspace              # all tests
cargo fmt --all                     # format
cargo clippy --all-targets --all-features   # lint
```

## Architecture

Multi-crate Cargo workspace, layered: core infrastructure, protocol layer, storage
support, server core, server products, proxies. See `docs/ARCHITECTURE.md` for the full
breakdown.

## Skills

Essential (shipped in `.agent/skills/`, symlinked into `.claude/skills/`, always
available regardless of MCP setup):
- `run` — launch a product binary with its example config
- `test` — run unit, integration, and fuzz tests
- `journal` — scaffold a `docs/journal/` entry for a non-trivial engineering effort
  (nudged, non-blocking, by `.agent/hooks/pre-commit-check.sh`)
- `pr` — create a feature branch, commit, push, open a PR
- `release` — create a release PR with version bump and changelog update

Recommended (used only if your environment has `skills-mcp` connected; never assume
they're present):
- `catchup`, `sweep-comments`, `plan-feature` — general repo/dev habit skills
- knowledge-iop vault bundle (`engineering-journal`, `frame-problem`, `propose-design`,
  `record-decision`, `vault-search`) — additionally requires the user's personal vault

## Build Requirements

- Rust stable toolchain

## Platform Support

- Linux and macOS
- x86_64 and ARM64 architectures
```

**Step 2: Verify**

Run: `wc -l CLAUDE.md`
Expected: well under half the original line count (original was ~95 lines; this should
land around 55-60, mostly from the Products/Skills lists).

Run: `git diff CLAUDE.md | head -80`
Expected: diff shows the Running Products, Testing, Configuration, and Dependencies
sections removed, and the new Skills section added.

**Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: thin CLAUDE.md to load-bearing facts plus a skills index"
```

---

### Task 8: Final verification pass

**Step 1: Confirm no Rust code changed**

Run: `git diff main --stat -- '*.rs' '*.toml'`
Expected: empty output — this branch only touches `.agent/`, `.claude/`, and `docs/`.

**Step 2: Confirm the workspace still builds and lints clean (nothing should have changed here, but this is a Rust repo — verify before calling it done)**

Run: `cargo build --workspace 2>&1 | tail -20`
Expected: builds successfully (or was already building successfully before this branch;
this is a regression check, not expected to be affected by doc/skill changes).

**Step 3: Confirm all five essential skills resolve through their symlinks**

Run: `for s in run test journal pr release; do echo "== $s =="; head -3 .claude/skills/$s/SKILL.md; done`
Expected: five frontmatter blocks print without error.

**Step 4: Review full branch diff against main**

Run: `git diff main --stat`
Expected: shows the design doc, this plan doc, `docs/ARCHITECTURE.md`,
`docs/journal/README.md`, the CLAUDE.md rewrite, and the `.agent/`/`.claude/` skill and
hook files (moved + new + symlinked).

**Step 5: Report status to the user**

Summarize what's on the branch and that it's ready for the `pr` skill when the user
wants to open the PR — do not push or open the PR as part of this plan; that was
explicitly deferred ("new branch in preparation for PR, but not yet").
