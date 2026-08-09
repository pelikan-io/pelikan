#!/usr/bin/env bash
# PreToolUse hook: nudge toward docs/journal/ when a non-trivial src/ change
# is committed without one. Non-blocking — always exits 0.
set -euo pipefail

command -v jq >/dev/null 2>&1 || exit 0

input="$(cat)"
tool_name="$(echo "$input" | jq -r '.tool_name // empty' 2>/dev/null)" || exit 0
command_str="$(echo "$input" | jq -r '.tool_input.command // empty' 2>/dev/null)" || exit 0

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
