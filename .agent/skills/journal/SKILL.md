---
name: journal
description: Scaffold a docs/journal/ entry for a non-trivial engineering effort
---

Create or continue an entry in `docs/journal/` — pelikan's in-repo, contributor-portable engineering journal. Use when starting, continuing, handing off, or closing a non-trivial effort, or when preserving a negative result worth remembering.

## Convention

- Path: `docs/journal/YYYY-MM-DD-<slug>.md`
- One file per effort; append to the same file as it continues rather than creating a new one each session.
- Sections:
  - `## What` — the effort or problem, one or two sentences
  - `## Decided` — what was decided and why
  - `## Open` — what's still unresolved

## Steps

1. Check `docs/journal/` for an existing entry covering this effort; continue it if found.
2. Otherwise create `docs/journal/<today>-<slug>.md` using the convention above.
3. Keep it short — this is a log, not a design brief. If the effort warrants a fuller paired problem/design brief, that's a separate, heavier step (the `frame-problem` / `propose-design` skills, if `skills-mcp` is connected) — not a replacement for this file.

## Notes

- A pre-commit hook (`.agent/hooks/pre-commit-check.sh`) nudges — non-blocking — when a commit touches `src/` without a corresponding `docs/journal/*.md` entry staged.
- This is separate from, not a replacement for, the MCP `engineering-journal` / knowledge-iop vault skills — those stay optional, for maintainers who want to mirror significant entries cross-project.
