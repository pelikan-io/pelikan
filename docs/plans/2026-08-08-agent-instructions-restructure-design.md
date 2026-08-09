# Restructuring agent instructions: thin CLAUDE.md, pluggable skills, enforced journaling

## Context

CLAUDE.md currently holds most of pelikan's agent-facing guidance as always-loaded
prose: build commands, product list with run instructions, architecture layers,
testing organization, dependency list. This works, but has two costs:

- Everything in it is paid on every turn, whether or not it's relevant.
- It's delivered as context attached to conversation history rather than a
  protected system prompt slot, so on long sessions it's subject to the same
  auto-compaction/summarization as any other prior message — unlike a skill,
  which is fetched fresh (full recency weight) only when actually invoked.

The goal is to move conditional, workflow-shaped knowledge out of CLAUDE.md and
into pluggable skills, while keeping CLAUDE.md as a small "load-bearing plus
index" residue, and to make engineering journaling an enforced, portable
project practice rather than an ad hoc `docs/notes/*.txt` habit.

## Sorting principle

Three buckets, sorted by how much it costs to miss the rule and whether the
model needs to notice it at all:

1. **Hooks** — must never be missed, fire on the triggering event itself
   (a tool call, a commit), don't depend on the model reading anything.
2. **Skills** — conditional, workflow-shaped knowledge, safe to fetch fresh
   on demand, only needed when doing that specific thing.
3. **CLAUDE.md residue ("load-bearing plus index")** — small facts that must
   be present at session start and tolerate occasional degradation on long
   sessions, plus a short index pointing at the skills/docs that cover
   everything conditional.

## Content migration

| Current CLAUDE.md content | Destination |
|---|---|
| Project one-liner | CLAUDE.md residue |
| Raw `cargo build/test/fmt/clippy` commands | CLAUDE.md residue (trivial enough that a skill would be YAGNI) |
| Build Requirements / Platform Support | CLAUDE.md residue (tiny, rarely changes) |
| Architecture (Workspace Structure detail) | Short pointer in CLAUDE.md; full detail stays in a referenced doc, not necessarily a skill (it's not an action/workflow) |
| Running Products (4 binaries + configs) | New `run` skill |
| Testing (unit/integration/fuzz organization) | New `test` skill |
| Dependencies list | Dropped — derivable from `Cargo.toml`, not worth maintaining a second copy |
| (new) Engineering journaling | New `journal` skill + pre-commit hook (see below) |

## Skill index (essential vs. recommended)

**Essential** — shipped in the repo, always available to any contributor,
regardless of their MCP setup:
- `run` — launch product binaries
- `test` — run unit/integration/fuzz tests
- `journal` — scaffold an in-repo engineering journal entry
- `pr`, `release` — existing

**Recommended** — available only if the contributor's environment has
`skills-mcp` connected; CLAUDE.md documents them as "prefer if available,
fall back otherwise," never as required:
- `catchup`, `sweep-comments`, `plan-feature` — general habit skills
- knowledge-iop vault bundle (`engineering-journal`, `frame-problem`,
  `propose-design`, `record-decision`, `vault-search`) — additionally
  depends on the user's personal vault, not just `skills-mcp`

The essential/recommended split tracks portability (shipped-with-repo vs.
personal-MCP-dependent), not usefulness — an MCP skill can be a great fit
and still can't be "essential" for a public repo whose contributors won't
all share the same MCP connection.

## Engineering journaling: enforced, in-repo, portable

Decision: enforce journaling for **any contributor**, not just the maintainer.
This rules out relying on the personal knowledge-iop vault (most contributors
won't have it) and requires an in-repo, vault-independent mechanism.

- **Convention**: `docs/journal/YYYY-MM-DD-<slug>.md`, plain markdown, checked
  into git — same spirit as grow-a-cache's `docs/v0/`…`docs/v3/`, flat rather
  than milestone-numbered. Lightweight sections: what effort/problem this is,
  what was decided and why, what's still open. Deliberately less formal than
  the vault's paired problem/design-brief structure, so skipping it feels
  like the worse option.
- **`journal` skill**: scaffolds a new entry. Covers the same triggers as the
  MCP `engineering-journal` skill (start/continue/handoff/close a non-trivial
  effort, preserve a negative result) but writes to the repo, not the vault.
- **Hook**: `PreToolUse` on `git commit` (covers PR-open transitively, since
  the `pr` skill always commits before opening a PR). Heuristic: diff touches
  `src/` beyond a trivial dependency bump or CI-only change, and no
  `docs/journal/*.md` file is part of the change → nudge via stderr,
  **non-blocking**, matching grow-a-cache's existing precedent rather than a
  new stricter pattern.
- This is separate from, not a replacement for, the MCP vault skills — those
  stay in the Recommended tier as an opt-in layer if the maintainer wants to
  mirror significant entries into knowledge-iop later. Not built in now
  (YAGNI until asked for).

## Directory layout: `.agent/` as source of truth, `.claude/` as symlinks

```
.agent/
  skills/
    run/SKILL.md
    test/SKILL.md
    journal/SKILL.md
    pr/SKILL.md          <- moved from .claude/skills/pr
    release/SKILL.md     <- moved from .claude/skills/release
  hooks/
    pre-commit-check.sh

.claude/
  skills/
    run     -> ../../.agent/skills/run
    test    -> ../../.agent/skills/test
    journal -> ../../.agent/skills/journal
    pr      -> ../../.agent/skills/pr
    release -> ../../.agent/skills/release
  hooks/
    pre-commit-check.sh -> ../../.agent/hooks/pre-commit-check.sh
  settings.json          <- registers the PreToolUse hook; path resolves
                             through the symlink
```

Rationale: matches the portability signal already present in the skill
catalog's `document-feature-skill`/`engineering-journal-skill` templates,
which declare `agent-skills-common-subset`/`codex`/`claude-code`
compatibility — content lives once under `.agent/`, tool-specific directories
just point at it. CLAUDE.md already scopes pelikan to Linux/macOS, so
symlinks-in-git aren't a portability concern.

Consequence: `pr` and `release` are currently real files under
`.claude/skills/`, not symlinks. Adopting this layout means `git mv`-ing them
into `.agent/skills/` and replacing the old paths with symlinks — not just
adding the new skills alongside the old ones.

## Out of scope for this pass

- Mirroring in-repo journal entries into the personal knowledge-iop vault
  (opt-in, later, if wanted).
- Making the pre-commit hook blocking rather than a nudge.
- Any skill/doc content for architecture beyond a short CLAUDE.md pointer.
