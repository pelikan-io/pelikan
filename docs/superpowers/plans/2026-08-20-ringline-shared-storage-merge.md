# Ringline Shared Storage Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebase PR #187 onto current Pelikan main while replacing its dedicated Ringline storage thread with the concurrent Arc-shared storage model introduced by #189.

**Architecture:** `ProcessBuilder` creates one `Arc<Storage>` before backend selection. Mio and Ringline workers execute directly against clones of that same engine, while admin `FlushAll` clears it synchronously. Ringline retains async network task/completion machinery but removes all storage request/response queues, storage-worker lifecycle, and bridge-only metrics.

**Tech Stack:** Rust 2021, Mio, Ringline 0.5.3, Segcache 0.4.4, metriken 0.9.2, Cargo workspaces, generated SVG diagrams.

**Spec:** `docs/superpowers/specs/2026-08-18-ringline-cache-server-design.md`

## Global Constraints

- Mio remains the default and non-Linux/TLS fallback.
- Ringline remains opt-in and Linux-only; `ringline-force-mio` still tests the logical Ringline backend with Ringline's Mio driver.
- Exactly one `Arc<Storage>` exists per cache-server process and is shared by data workers and admin flush handling.
- No Pelikan-owned storage thread, storage request/response queue, or data-path storage lock remains.
- `FlushAll` clears the shared engine exactly once before acknowledging.
- Segcache comes from crates.io at 0.4.4; metriken resolves once at 0.9.2 and metriken-core once at 0.2.1.
- Generated SVG files are changed only through `cargo xtask diagrams`.
- Proxy behavior remains out of scope.

---

### Task 1: Resolve Core Process and Dependency Conflicts

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/core/server/src/process.rs`
- Modify: `src/core/server/src/workers/mod.rs`
- Modify: `src/entrystore/src/segcache/memcache.rs`
- Modify: `src/server/pingserver/src/main.rs`

**Interfaces:**
- Produces: `ProcessBuilder` backend selection operating on one `Arc<Storage>`.
- Produces: Mio `WorkersBuilder` using main's shared worker implementation.
- Consumes: main's `FlushHandle` and `pelikan_main!` macro.
- Preserves: registry `segcache = "0.4.4"`, `metriken = "0.9.2"`.

- [ ] **Step 1: Merge current `origin/main` and capture the existing conflict set**

Run: `git merge --no-edit origin/main`
Expected: conflicts in the known process, worker, dependency, test, and diagram files.

- [ ] **Step 2: Resolve dependency and core conflicts**

Keep Segcache 0.4.4 and metriken 0.9.2; regenerate the lockfile instead of hand-splicing it. Take main's worker-only Mio topology. In `ProcessBuilder::new`, wrap storage in `Arc` before backend selection, attach a `FlushHandle` clone to admin, and pass Arc clones to either backend. Preserve Ringline backend resolution, fallback classification, observability, and live-runtime shutdown behavior. Keep main's `&self` Segcache execution methods and its standardized product main macro while launching through `Pingserver::new(config).wait()`.

- [ ] **Step 3: Run the focused compile gate**

Run: `cargo check --workspace --all-targets`
Expected: failures, if any, are confined to Ringline handlers that still expect owned storage or the obsolete storage bridge.

- [ ] **Step 4: Commit the core merge resolution**

Run: `git add Cargo.toml Cargo.lock src/core/server/src/process.rs src/core/server/src/workers src/entrystore/src/segcache/memcache.rs src/server/pingserver/src/main.rs && git commit`
Expected commit subject: `refactor: share storage across io backends`.

### Task 2: Remove the Ringline Dedicated Storage Bridge

**Files:**
- Modify: `src/core/server/src/ringline/mod.rs`
- Modify: `src/core/server/src/ringline/single.rs`
- Delete or rewrite: `src/core/server/src/ringline/multi.rs`
- Modify: related Ringline process tests under `src/core/server/src/ringline/`
- Modify only if unused after removal: `src/net/src/ringline/completion.rs`

**Interfaces:**
- Consumes: `Arc<Storage>` from Task 1.
- Produces: Ringline worker bootstraps that clone the Arc and execute protocol storage calls directly.
- Removes: `RinglineStorageWorker`, `MultiHandler`, storage request/response envelopes, storage queue wake handles, and bridge-only metrics.
- Preserves: bounded network send backpressure, request logging, expiry, cancellation, hangup, startup fallback, and runtime monitoring.

- [ ] **Step 1: Add or adapt failing tests for shared identity and direct multi-worker execution**

Tests must prove multiple Ringline worker bootstraps observe the same `Arc`, no storage thread is started, and admin flush affects data seen by every worker.

- [ ] **Step 2: Run focused tests to verify RED**

Run: `cargo test -p server ringline --features ringline-force-mio`
Expected: fail because the old multi-worker path still constructs queues/storage worker or cannot accept Arc storage.

- [ ] **Step 3: Implement direct shared execution**

Unify single- and multi-worker handlers where practical. Store `Arc<Storage>` in each worker-local bootstrap and rely on Storage's internal synchronization. Remove the dedicated thread lifecycle, queues, notifications, completion routing, and metrics; do not introduce replacement locking.

- [ ] **Step 4: Run focused tests to verify GREEN**

Run: `cargo test -p server --features ringline-force-mio`
Run: `cargo test -p pelikan-net --features ringline-force-mio`
Expected: all tests pass with no storage-bridge thread or metric references.

- [ ] **Step 5: Commit the bridge removal**

Expected commit subject: `refactor: execute ringline requests on shared storage`.

### Task 3: Merge Cache-Server Conformance and Flush Semantics

**Files:**
- Modify: `src/server/segcache/tests/common.rs`
- Modify: `src/server/segcache/tests/integration.rs`
- Modify: `src/server/segcache/tests/integration_multi.rs`
- Modify as required: RDS and Pingserver integration fixtures.

**Interfaces:**
- Consumes: main's concurrent `flush_all` stress client and PR #187's ephemeral live-address/backend-resolution fixture.
- Produces: the same conformance suite for Mio, native Ringline/fallback, and forced-Ringline.
- Preserves: port-zero listener ownership with no bind-close-rebind race.

- [ ] **Step 1: Resolve tests by composing, not choosing, conflict sides**

Retain dynamic data/admin addresses, readiness polling, exact fallback classification, TLS fallback, restart cleanup, large request/response and cancellation cases. Add main's concurrent write/flush tests, parameterized with live addresses. Use eight workers in the multi-worker suite.

- [ ] **Step 2: Run Mio tests**

Run: `cargo test -p pelikan-segcache --test integration`
Run: `cargo test -p pelikan-segcache --test integration_multi`
Expected: all protocol and concurrent flush cases pass.

- [ ] **Step 3: Run forced-Ringline tests**

Run: `cargo test -p pelikan-segcache --test integration --features ringline-force-mio`
Run: `cargo test -p pelikan-segcache --test integration_multi --features ringline-force-mio`
Expected: active backend is Ringline, no fallback occurs, shared-storage and flush cases pass.

- [ ] **Step 4: Commit conformance resolution**

Expected commit subject: `test: cover shared storage across ringline workers`.

### Task 4: Reconcile Architecture Documentation and Generated Diagrams

**Files:**
- Modify: `docs/ARCHITECTURE.md`
- Modify: `xtask/src/dataflow.rs`
- Modify: `xtask/src/threading.rs`
- Generate: `docs/diagrams/dataflow.svg`
- Generate: `docs/diagrams/threading.svg`

**Interfaces:**
- Consumes: final source topology from Tasks 1-3.
- Produces: assertions and diagrams showing backend-specific I/O scheduling feeding one Arc-shared storage engine.
- Removes: all dedicated `pelikan_storage` thread and storage queue edges.

- [ ] **Step 1: Resolve architecture prose and generator claims**

Preserve main's concurrent shared-engine explanation and PR #187's backend selection/fallback text. Describe Mio callbacks versus Ringline tasks without presenting different storage ownership models. Update source assertions to current file names and direct shared execution paths.

- [ ] **Step 2: Verify generator assertions fail before stale diagrams are accepted**

Run: `cargo xtask diagrams`
Expected before final resolution: failure on stale source claims or a generated diff replacing obsolete storage-thread topology.

- [ ] **Step 3: Generate diagrams from resolved sources**

Run: `cargo fmt --all`
Run: `cargo xtask diagrams`
Run again: `cargo xtask diagrams && git diff --exit-code docs/diagrams/`
Expected: second generation is byte-identical.

- [ ] **Step 4: Commit documentation and diagrams**

Expected commit subject: `docs: show shared cache storage topology`.

### Task 5: Final Verification, Review, and PR Update

**Files:**
- Verify all changed files.
- Update PR #187 branch only after review passes.

- [ ] **Step 1: Run dependency and provenance checks**

Run: `cargo tree -p pelikan-segcache --locked`
Run: `cargo metadata --locked --format-version 1`
Expected: registry Segcache 0.4.4; exactly one metriken 0.9.2 and metriken-core 0.2.1; no vendored Segcache references.

- [ ] **Step 2: Run CI-equivalent gates**

Run: `cargo check --workspace --all-targets`
Run: `cargo test --workspace --tests --lib --bins --examples --locked`
Run: `cargo test --workspace --doc --locked -- --test-threads 16`
Run: `cargo build --workspace --all-targets --locked --release`
Run: affected-package strict Clippy with only the documented pre-existing large-enum lint allowed.
Expected: zero failures.

- [ ] **Step 3: Run independent whole-branch review**

Review current `origin/main..HEAD` for shared-storage correctness, fallback lifecycle, flush ordering, concurrency safety, dependency provenance, platform gating, tests, and documentation. Resolve every Critical or Important finding and re-review.

- [ ] **Step 4: Push normally and monitor CI**

Run: `git push origin feature/ringline-cache-io`
Expected: PR #187 is mergeable and the previously failing Ubuntu check completes successfully.
