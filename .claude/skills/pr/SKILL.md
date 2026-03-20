---
name: pr
description: Create a feature branch, commit changes, push, and open a PR against pelikan-io/pelikan
---

Create a PR from the current uncommitted changes. Handles branching, committing, pushing, and opening the PR against the upstream repo.

## Arguments

The skill accepts an optional branch name argument:
- If provided, use it as the branch name (e.g., `/pr fix-socket-perms`)
- If not provided, generate a descriptive kebab-case branch name from the changes

## Steps

1. **Verify prerequisites**:
   - Working directory must have uncommitted changes (staged or unstaged)
   - Must not already be on a feature branch with unpushed commits for a different change

   ```bash
   git status
   git diff
   git diff --staged
   ```

   If there are no changes, stop and tell the user.

2. **Analyze the changes**:
   - Read the diff to understand what changed
   - Check recent commit messages for style conventions:
     ```bash
     git log --oneline -10
     ```

3. **Create feature branch**:
   - If currently on `main`, create a new branch:
     ```bash
     git checkout -b <branch-name>
     ```
   - If already on a feature branch, use the current branch

4. **Stage and commit**:
   - Stage the relevant changed files by name (avoid `git add -A`)
   - Write a commit message following the repo's conventional commit style (`type(scope): description`)
   - Common types: `feat`, `fix`, `refactor`, `chore`, `docs`, `test`
   - Include `Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>` trailer
   - Use a HEREDOC for the commit message

5. **Push to origin**:
   ```bash
   git push -u origin <branch-name>
   ```

6. **Open PR against upstream**:
   - The upstream repo is `pelikan-io/pelikan`
   - The fork remote is `origin` (determine the owner from `git remote -v`)
   - Use `gh pr create` with `--repo pelikan-io/pelikan` and `--head <fork-owner>:<branch-name>`

   ```bash
   gh pr create \
     --repo pelikan-io/pelikan \
     --head <fork-owner>:<branch-name> \
     --title "<conventional commit style title>" \
     --body "$(cat <<'EOF'
   ## Summary
   <1-3 bullet points describing the changes>

   ## Test plan
   <bulleted checklist of testing done or needed>

   🤖 Generated with [Claude Code](https://claude.com/claude-code)
   EOF
   )"
   ```

7. **Report the PR URL** to the user.

## Notes

- Keep PR titles under 70 characters
- PR title should follow the same conventional commit format as the commit message
- If `cargo clippy`, `cargo test`, or `cargo fmt --check` haven't been run yet during this session, run them before committing
- Never force push or amend existing commits
- The fork owner can be determined from the origin remote URL (e.g., `git@github.com:brayniac/pelikan` → `brayniac`)
