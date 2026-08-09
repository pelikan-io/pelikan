#!/usr/bin/env bash
# PreToolUse hook: nudge toward docs/journal/ when a non-trivial src/ change
# is committed without one. Non-blocking — always exits 0.
set -euo pipefail

command -v jq >/dev/null 2>&1 || exit 0

input="$(cat)"
parsed="$(printf '%s' "$input" | jq -r '[.tool_name // "", .tool_input.command // ""] | @tsv' 2>/dev/null)" || exit 0
IFS=$'\t' read -r tool_name command_str <<< "$parsed"

[ "$tool_name" = "Bash" ] || exit 0
# Tolerate flags between `git` and `commit` (e.g. `git -C . commit`); loose by
# design — false positives just mean an extra harmless check runs below.
printf '%s\n' "$command_str" | grep -Eq 'git\b.*\bcommit\b' || exit 0

staged="$(git diff --cached --name-only 2>/dev/null || true)"

# `git commit -a`/`--all`/`-am`/`-a -m` stages tracked modifications as part
# of the commit itself, so `--cached` alone misses them; union in unstaged
# tracked changes in that case. (Compound `git add ... && git commit ...`
# commands are intentionally not handled — out of scope.)
if printf '%s\n' "$command_str" | grep -Eq -- '(^|[[:space:]])-[a-zA-Z]*a[a-zA-Z]*([[:space:]]|$)|(^|[[:space:]])--all([[:space:]]|$)'; then
    unstaged="$(git diff --name-only 2>/dev/null || true)"
    staged="$(printf '%s\n%s\n' "$staged" "$unstaged" | sort -u)"
fi

[ -n "$staged" ] || exit 0

nontrivial_src="$(printf '%s\n' "$staged" | grep -E '^src/' | grep -Ev '^src/.*Cargo\.(toml|lock)$' || true)"
[ -n "$nontrivial_src" ] || exit 0

has_journal="$(printf '%s\n' "$staged" | grep -E '^docs/journal/.*\.md$' || true)"
[ -n "$has_journal" ] && exit 0

echo "Reminder: this commit touches src/ but no docs/journal/*.md entry is staged. Consider running the 'journal' skill to log this effort (non-blocking)." >&2
exit 0
