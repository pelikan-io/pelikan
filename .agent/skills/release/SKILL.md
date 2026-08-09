---
name: release
description: Create a release PR with version bump and changelog update
---

Create a release PR that bumps the version and updates the changelog.

## Arguments

The skill accepts a version level argument:
- `patch` - 0.3.2 -> 0.3.3
- `minor` - 0.3.2 -> 0.4.0
- `major` - 0.3.2 -> 1.0.0
- Or an explicit version like `0.4.0`

Example: `/release minor`

## Steps

1. **Verify prerequisites**:
   - Must be on `main` branch
   - Working directory must be clean
   - Must be up to date with origin/main

   ```bash
   git fetch origin
   if [ "$(git branch --show-current)" != "main" ]; then
     echo "Error: Must be on main branch"
     exit 1
   fi
   if [ -n "$(git status --porcelain)" ]; then
     echo "Error: Working directory not clean"
     exit 1
   fi
   if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
     echo "Error: Not up to date with origin/main"
     exit 1
   fi
   ```

2. **Run local checks**:
   ```bash
   cargo clippy --workspace
   cargo test --workspace
   ```
   If checks fail, stop and report the errors.

3. **Determine the new version**:
   - Get current version from workspace `Cargo.toml`
   - Calculate new version based on the level argument

4. **Create release branch**:
   ```bash
   NEW_VERSION="X.Y.Z"  # from step 3
   git checkout -b release/v${NEW_VERSION}
   ```

5. **Bump version**:
   - Update the `version` field in the workspace `[workspace.package]` section of `Cargo.toml`

6. **Commit changes**:
   ```bash
   git add Cargo.toml Cargo.lock
   git commit -m "release: prepare v${NEW_VERSION}"
   ```

7. **Push and create PR**:
   ```bash
   git push -u origin release/v${NEW_VERSION}

   gh pr create \
     --repo pelikan-io/pelikan \
     --head <fork-owner>:release/v${NEW_VERSION} \
     --title "release: v${NEW_VERSION}" \
     --body "$(cat <<'EOF'
   ## Release v${NEW_VERSION}

   This PR prepares the release of v${NEW_VERSION}.

   ### Changes
   - Version bump in Cargo.toml

   ---
   🤖 Generated with [Claude Code](https://claude.com/claude-code)
   EOF
   )"
   ```

8. **Report the PR URL** to the user.

## Notes

- The fork owner can be determined from the origin remote URL (e.g., `git@github.com:brayniac/pelikan` → `brayniac`)
- Never force push or amend existing commits
