# Concurrent Segcache Conversion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert pelikan to the concurrent segcache engine (cache-rs), replacing the storage-thread architecture with a unified Arc-shared worker model plus a maintenance thread.

**Architecture:** Publish `keyvalue 0.3.0` and `segcache 0.4.0` from cache-rs, then convert pelikan in three buildable stages: (1) dependency bump + entrystore internal adaptation, (2) trait conversion `&mut self` → `&self`, (3) core/server rearchitecture. Every commit builds and passes the full test suite.

**Tech Stack:** Rust stable, cargo workspace, mio event loops, crates.io publishing via `cargo publish`, `gh` CLI for the cache-rs PR.

**Spec:** `docs/superpowers/specs/2026-08-18-concurrent-segcache-conversion-design.md`

**Repos involved:**
- `/Users/brian/workspace/brayniac/cache-rs` — release prep (Phase A)
- `/Users/brian/workspace/brayniac/pelikan` — conversion, branch `concurrent-segcache` (Phases B–E)

---

## Phase A: cache-rs release (keyvalue 0.3.0, segcache 0.4.0)

> **Status 2026-08-18:** A1/A2 complete (PR #42 merged as `c1b3599`, clean clone validated). A3/A4 **blocked on crates.io ownership**: `keyvalue` is owned solely by thinkingfish (Yao Yue); brayniac cannot publish it. The user is resolving ownership out-of-band. Interim: Phase B uses a temporary `[patch.crates-io]` git pin to `c1b3599` so development proceeds; Task E3 removes the patch once both crates are published (hard prerequisite for merging the PR).

Published state on crates.io: `keyvalue 0.2.0`, `segcache 0.3.0` (deps on keyvalue ^0.2.0). Local/origin main has `keyvalue 0.3.0` (unpublished, has the seqlock numeric slot) and `segcache 0.3.0` (needs bump to 0.4.0 — the `&self` API is breaking). The full test suite passes on local main (verified 2026-08-18, exit 0).

### Task A1: Version-bump PR for segcache 0.4.0

**Files:**
- Modify: `/Users/brian/workspace/brayniac/cache-rs/crates/segcache/Cargo.toml` (version field)

- [ ] **Step 1: Create a branch and bump the version**

In `/Users/brian/workspace/brayniac/cache-rs`:

```bash
git checkout -b segcache-0.4.0 origin/main
```

Edit `crates/segcache/Cargo.toml`:

```toml
[package]
name = "segcache"
version = "0.4.0"
```

(only the `version` line changes, from `0.3.0`)

- [ ] **Step 2: Verify the workspace still builds and tests pass**

```bash
cargo build --workspace && cargo test -p segcache --release 2>&1 | tail -5
```

Expected: `test result: ok`

- [ ] **Step 3: Commit and open the PR (repo convention: version bumps go through PRs, cf. #26/#27)**

```bash
git add crates/segcache/Cargo.toml Cargo.lock
git commit -m "Bump segcache version to 0.4.0

The Arc-shareable &self API (roadmap items 7a-7f) is a breaking change."
git push -u origin segcache-0.4.0
gh pr create --title "Bump segcache version to 0.4.0" --body "The Arc-shareable &self API (roadmap items 7a-7f) is a breaking change over the published 0.3.0."
```

- [ ] **Step 4: Merge the PR after CI passes**

```bash
gh pr checks --watch && gh pr merge --squash
```

Expected: PR merged into origin/main.

### Task A2: Clean checkout of origin/main for publishing

Publish from a pristine clone so the published bits correspond exactly to public main (the local checkout carries unpushed docs commits).

- [ ] **Step 1: Fresh clone into the scratchpad**

```bash
cd /private/tmp/claude-501/-Users-brian-workspace-brayniac-pelikan/2c0aa821-fe61-4a68-9156-8b83fd550bed/scratchpad
git clone https://github.com/pelikan-io/cache-rs cache-rs-release
cd cache-rs-release
git log --oneline -3
```

Expected: HEAD is the merged version-bump commit; `crates/segcache/Cargo.toml` says `0.4.0`, `crates/keyvalue/Cargo.toml` says `0.3.0`.

- [ ] **Step 2: Run the test suite in the clean clone**

```bash
cargo test --workspace --release 2>&1 | tail -5
```

Expected: `test result: ok`

### Task A3: Publish keyvalue 0.3.0

keyvalue must go first — segcache 0.4.0 depends on `keyvalue = "0.3.0"`, which crates.io does not have yet.

- [ ] **Step 1: Dry run**

```bash
cd /private/tmp/claude-501/-Users-brian-workspace-brayniac-pelikan/2c0aa821-fe61-4a68-9156-8b83fd550bed/scratchpad/cache-rs-release
cargo publish -p keyvalue --dry-run
```

Expected: `Uploading keyvalue v0.3.0` … finishes with no errors (upload skipped in dry run).

- [ ] **Step 2: GATE — confirm with the user before the irreversible publish**

Do not run the real publish without an explicit go-ahead in this session.

- [ ] **Step 3: Publish**

```bash
cargo publish -p keyvalue
```

Expected: `Uploaded keyvalue v0.3.0` / `Published keyvalue v0.3.0`.

- [ ] **Step 4: Verify on the index**

```bash
curl -s https://index.crates.io/ke/yv/keyvalue | tail -1 | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['vers'])"
```

Expected: `0.3.0` (may take ~a minute to appear).

### Task A4: Publish segcache 0.4.0

- [ ] **Step 1: Dry run**

```bash
cargo publish -p segcache --dry-run
```

Expected: no errors. (If it fails because keyvalue 0.3.0 hasn't propagated to the index yet, wait a minute and retry.)

- [ ] **Step 2: GATE — confirm with the user before the irreversible publish**

- [ ] **Step 3: Publish**

```bash
cargo publish -p segcache
```

Expected: `Published segcache v0.4.0`.

- [ ] **Step 4: Verify the published dep requirement**

```bash
curl -s https://index.crates.io/se/gc/segcache | tail -1 | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d['vers'], [(x['name'], x['req']) for x in d['deps'] if x['name']=='keyvalue'])"
```

Expected: `0.4.0 [('keyvalue', '^0.3.0')]`

---

## Phase B: pelikan — dependency bump + entrystore adaptation (buildable commit 1)

All remaining work happens in `/Users/brian/workspace/brayniac/pelikan` on branch `concurrent-segcache`. This stage bumps the dep and adapts entrystore to the changed return types, while **keeping every trait signature `&mut self`** — `&mut self` methods can call the engine's `&self` methods, so the tree builds and all existing tests pass unchanged.

API deltas absorbed here:
- `wrapping_add`/`saturating_sub` return `Result<u64, SegcacheError>` (the new value) instead of `Result<Item, SegcacheError>`
- `expire`/`clear` return `usize` (ignorable; not `#[must_use]`)

### Task B1: Bump the workspace dependency and remove the stray directory

**Files:**
- Modify: `Cargo.toml` (workspace root, line with `segcache = "0.3.0"`)
- Delete: `src/storage/segcache/` (untracked leftover containing only a Cargo.lock)

- [ ] **Step 1: Remove the stray untracked directory**

```bash
rm -r src/storage/segcache
```

- [ ] **Step 2: Bump the dependency**

In the root `Cargo.toml` `[workspace.dependencies]`:

```toml
segcache = "0.4.0"
```

(replaces `segcache = "0.3.0"`)

- [ ] **Step 2b (temporary, while crates.io publish is blocked): add a git pin**

At the end of the root `Cargo.toml`:

```toml
# TEMPORARY: segcache 0.4.0 is not yet on crates.io (keyvalue ownership is
# being resolved). Remove this patch before merging (see Task E3).
[patch.crates-io]
segcache = { git = "https://github.com/pelikan-io/cache-rs", rev = "c1b3599f8a0d552501c99db96dcda2a195ab62bf" }
```

- [ ] **Step 3: Update the lockfile and attempt a build**

```bash
cargo update -p segcache && cargo build --workspace 2>&1 | tail -20
```

Expected: **FAIL** in `entrystore` — `incr`/`decr` in `src/entrystore/src/segcache/memcache.rs` call `.value()` on a `u64`. This failure scopes Task B2.

### Task B2: Adapt entrystore incr/decr to the new numeric-op returns

**Files:**
- Modify: `src/entrystore/src/segcache/memcache.rs:270-292` (`incr` and `decr`)

- [ ] **Step 1: Replace the two method bodies**

```rust
    fn incr(&mut self, incr: &Incr) -> Response {
        match self.data.wrapping_add(incr.key(), incr.value()) {
            Ok(v) => Response::numeric(v, incr.noreply()),
            Err(SegcacheError::NotFound) => Response::not_found(incr.noreply()),
            Err(SegcacheError::NotNumeric) => Response::error(),
            Err(_) => Response::server_error(""),
        }
    }

    fn decr(&mut self, decr: &Decr) -> Response {
        match self.data.saturating_sub(decr.key(), decr.value()) {
            Ok(v) => Response::numeric(v, decr.noreply()),
            Err(SegcacheError::NotFound) => Response::not_found(decr.noreply()),
            Err(SegcacheError::NotNumeric) => Response::error(),
            Err(_) => Response::server_error(""),
        }
    }
```

(Receivers stay `&mut self` in this phase; only the match arms change — the engine now returns the new value directly, so the `Item`/`Value::U64` unpacking disappears.)

- [ ] **Step 2: Build the workspace**

```bash
cargo build --workspace 2>&1 | tail -5
```

Expected: success. If `expire()`/`clear()` call sites warn or error anywhere, they don't — `usize` returns are silently droppable; no changes needed there.

- [ ] **Step 3: Run the full test suite**

```bash
cargo test --workspace 2>&1 | tail -15
```

Expected: all tests pass, including `integration` and `integration_multi` for segcache and rds (these are `harness = false` binaries; each prints `passed!` and exits 0).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "deps: bump segcache to 0.4.0 (concurrent engine)

Adapt incr/decr to the new numeric-op API which returns the new value
directly instead of an Item. Storage engine is now the concurrent
segcache from cache-rs; threading model unchanged in this commit."
```

---

## Phase C: pelikan — trait conversion `&mut self` → `&self` (buildable commit 2)

Convert the four traits and all implementors. The existing single/multi/storage workers still compile: they hold storage by value/`&mut` and calling `&self` methods through a `&mut` receiver is fine. No behavior change in this commit.

### Task C1: Convert `protocol_common::Execute`

**Files:**
- Modify: `src/protocol/common/src/lib.rs:24-26`

- [ ] **Step 1: Change the trait**

```rust
pub trait Execute<Request, Response: Compose> {
    fn execute(&self, request: &Request) -> Response;
}
```

### Task C2: Convert `entrystore::EntryStore`

**Files:**
- Modify: `src/entrystore/src/lib.rs:18-27`

- [ ] **Step 1: Change the trait**

```rust
pub trait EntryStore {
    /// Eager expiration of items/values from storage. Not all storage types
    /// will be able to efficiently implement this function. The default
    /// implementation is a no-op. Types which can efficiently implement eager
    /// expiration should implement their own handling logic for this function.
    fn expire(&self) {}

    /// Remove all existing values from the entry store.
    fn clear(&self);
}
```

### Task C3: Convert `protocol_memcache::Storage` and `protocol_resp::Storage`

**Files:**
- Modify: `src/protocol/memcache/src/storage/mod.rs:7-21`
- Modify: `src/protocol/resp/src/storage/mod.rs:7-10`

- [ ] **Step 1: memcache Storage — every method takes `&self`**

```rust
pub trait Storage {
    fn add(&self, request: &Add) -> Response;
    fn append(&self, request: &Append) -> Response;
    fn cas(&self, request: &Cas) -> Response;
    fn decr(&self, request: &Decr) -> Response;
    fn delete(&self, request: &Delete) -> Response;
    fn flush_all(&self, request: &FlushAll) -> Response;
    fn get(&self, request: &Get) -> Response;
    fn gets(&self, request: &Get) -> Response;
    fn incr(&self, request: &Incr) -> Response;
    fn prepend(&self, request: &Prepend) -> Response;
    fn quit(&self, request: &Quit) -> Response;
    fn replace(&self, request: &Replace) -> Response;
    fn set(&self, request: &Set) -> Response;
}
```

- [ ] **Step 2: resp Storage**

```rust
pub trait Storage {
    fn get(&self, request: &Get) -> Response;
    fn set(&self, request: &Set) -> Response;
}
```

### Task C4: Convert all implementors

**Files:**
- Modify: `src/entrystore/src/segcache/mod.rs:62-70` (EntryStore for Seg)
- Modify: `src/entrystore/src/segcache/memcache.rs` (Execute + Storage impls: every `&mut self` → `&self`)
- Modify: `src/entrystore/src/segcache/resp.rs` (same)
- Modify: `src/entrystore/src/noop/mod.rs:23-27` (EntryStore for Noop)
- Modify: `src/entrystore/src/noop/ping.rs` (Execute for Noop)

- [ ] **Step 1: Mechanical receiver change in all five files**

Every `fn …(&mut self, …)` in these impls becomes `fn …(&self, …)`. The bodies are already compatible (`self.data.…` calls target the new `&self` engine API). `EntryStore for Seg` becomes:

```rust
impl EntryStore for Seg {
    fn expire(&self) {
        self.data.expire();
    }

    fn clear(&self) {
        self.data.clear();
    }
}
```

`EntryStore for Noop`:

```rust
impl EntryStore for Noop {
    fn expire(&self) {}

    fn clear(&self) {}
}
```

- [ ] **Step 2: Build — expect remaining `&mut`-receiver call sites to be fine**

```bash
cargo build --workspace 2>&1 | tail -10
```

Expected: success. `core/server` workers call `self.storage.execute(&request)` / `.expire()` / `.clear()` through `&mut self` receivers, which coerce to `&self`. If clippy later flags variables that no longer need `mut`, fix in Task C5.

- [ ] **Step 3: Test and lint**

```bash
cargo test --workspace 2>&1 | tail -15 && cargo clippy --all-targets --all-features 2>&1 | tail -5
```

Expected: tests pass; clippy may warn about unused `mut` (e.g. `let mut storage` bindings) — remove any flagged `mut`s until clippy is clean.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor!: storage traits take &self

Execute, EntryStore, and the memcache/resp Storage traits no longer
require exclusive access; the concurrent segcache engine is internally
synchronized. Threading model unchanged in this commit."
```

---

## Phase D: pelikan — unified worker model + maintenance thread (buildable commit 3)

Replace the three worker implementations with one `Worker` holding `Arc<Storage>`, plus a `Maintenance` thread that owns expiration and flush. **`process.rs` keeps working unchanged** — the `WorkersBuilder` public surface (`new`, `wakers`, `worker_wakers`, `build`, `spawn`) is preserved, with the maintenance waker taking the slot the storage waker occupied (first in `wakers()`).

**Files:**
- Delete: `src/core/server/src/workers/single.rs`
- Delete: `src/core/server/src/workers/multi.rs`
- Delete: `src/core/server/src/workers/storage.rs`
- Create: `src/core/server/src/workers/worker.rs`
- Create: `src/core/server/src/workers/maintenance.rs`
- Rewrite: `src/core/server/src/workers/mod.rs`
- Modify: `src/core/server/src/lib.rs` (remove `QUEUE_RETRIES` if it has no remaining users)

### Task D1: Create `worker.rs`

This is today's `single.rs` with four changes: types renamed `SingleWorker*` → `Worker*`, storage field is `Arc<Storage>`, no `expire()` call in the loop, and `FlushAll` is ignored (maintenance owns it).

- [ ] **Step 1: Write `src/core/server/src/workers/worker.rs`**

```rust
// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::collections::VecDeque;

pub struct WorkerBuilder<Proto, Request, Response, Storage> {
    nevent: usize,
    protocol: Proto,
    pending: VecDeque<Token>,
    poll: Poll,
    sessions: Slab<ServerSession<Proto, Response, Request>>,
    storage: Arc<Storage>,
    timeout: Duration,
    waker: Arc<Waker>,
}

impl<Proto, Request, Response, Storage> WorkerBuilder<Proto, Request, Response, Storage> {
    pub fn new<T: WorkerConfig>(
        config: &T,
        protocol: Proto,
        storage: Arc<Storage>,
    ) -> Result<Self> {
        let config = config.worker();

        let poll = Poll::new()?;

        let waker = Arc::new(Waker::from(
            pelikan_net::Waker::new(poll.registry(), WAKER_TOKEN).unwrap(),
        ));

        let nevent = config.nevent();
        let timeout = Duration::from_millis(config.timeout() as u64);

        Ok(Self {
            nevent,
            protocol,
            pending: VecDeque::new(),
            poll,
            sessions: Slab::new(),
            storage,
            timeout,
            waker,
        })
    }

    pub fn waker(&self) -> Arc<Waker> {
        self.waker.clone()
    }

    pub fn build(
        self,
        session_queue: Queues<Session, Session>,
        signal_queue: Queues<(), Signal>,
    ) -> Worker<Proto, Request, Response, Storage> {
        Worker {
            nevent: self.nevent,
            protocol: self.protocol,
            pending: self.pending,
            poll: self.poll,
            session_queue,
            sessions: self.sessions,
            signal_queue,
            storage: self.storage,
            timeout: self.timeout,
            waker: self.waker,
        }
    }
}

pub struct Worker<Proto, Request, Response, Storage> {
    nevent: usize,
    protocol: Proto,
    pending: VecDeque<Token>,
    poll: Poll,
    session_queue: Queues<Session, Session>,
    sessions: Slab<ServerSession<Proto, Response, Request>>,
    signal_queue: Queues<(), Signal>,
    storage: Arc<Storage>,
    timeout: Duration,
    waker: Arc<Waker>,
}

impl<Proto, Request, Response, Storage> Worker<Proto, Request, Response, Storage>
where
    Proto: Protocol<Request, Response> + Clone,
    Request: Klog + Klog<Response = Response>,
    Response: Compose,
    Storage: EntryStore + Execute<Request, Response>,
{
    /// Return the `Session` to the `Listener` to handle flush/close
    fn close(&mut self, token: Token) {
        if self.sessions.contains(token.0) {
            let mut session = self.sessions.remove(token.0).into_inner();
            let _ = self.poll.registry().deregister(&mut session);
            let _ = self.session_queue.try_send_any(session);
            let _ = self.session_queue.wake();
        }
    }

    /// Handle up to one request for a session
    fn read(&mut self, token: Token) -> Result<()> {
        let session = self
            .sessions
            .get_mut(token.0)
            .ok_or_else(|| Error::other("non-existant session"))?;

        // fill the session
        map_result(session.fill())?;

        // process up to one pending request
        match session.receive() {
            Ok(request) => {
                let response = self.storage.execute(&request);
                PROCESS_REQ.increment();
                if response.should_hangup() {
                    let _ = session.send(response);
                    return Err(Error::other("should hangup"));
                }
                request.klog(&response);
                match session.send(response) {
                    Ok(_) => {
                        // attempt to flush immediately if there's now data in
                        // the write buffer
                        if session.write_pending() > 0 {
                            match session.flush() {
                                Ok(_) => Ok(()),
                                Err(e) => map_err(e),
                            }?;
                        }

                        // reregister to get writable event
                        if session.write_pending() > 0 {
                            let interest = session.interest();
                            if self
                                .poll
                                .registry()
                                .reregister(session, token, interest)
                                .is_err()
                            {
                                return Err(Error::other("failed to reregister"));
                            }
                        }

                        // if there's still data to read, put the token on the
                        // pending queue
                        if session.remaining() > 0 {
                            self.pending.push_back(token);
                        }

                        Ok(())
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::WouldBlock {
                            Ok(())
                        } else {
                            Err(e)
                        }
                    }
                }
            }
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    fn write(&mut self, token: Token) -> Result<()> {
        let session = self
            .sessions
            .get_mut(token.0)
            .ok_or_else(|| Error::other("non-existant session"))?;

        match session.flush() {
            Ok(_) => Ok(()),
            Err(e) => map_err(e),
        }
    }

    /// Run the worker in a loop, handling new events.
    pub fn run(&mut self) {
        let mut events = Events::with_capacity(self.nevent);

        loop {
            WORKER_EVENT_LOOP.increment();

            // we need another wakeup if there are still pending reads
            if !self.pending.is_empty() {
                let _ = self.waker.wake();
            }

            // get events with timeout
            if self.poll.poll(&mut events, Some(self.timeout)).is_err() {
                error!("Error polling");
            }

            let count = events.iter().count();
            WORKER_EVENT_TOTAL.add(count as _);
            if count == self.nevent {
                WORKER_EVENT_MAX_REACHED.increment();
            } else {
                let _ = WORKER_EVENT_DEPTH.increment(count as _);
            }

            // process all events
            for event in events.iter() {
                let token = event.token();

                match token {
                    WAKER_TOKEN => {
                        self.waker.reset();
                        // handle outstanding reads
                        for _ in 0..self.pending.len() {
                            if let Some(token) = self.pending.pop_front() {
                                if self.read(token).is_err() {
                                    self.close(token);
                                }
                            }
                        }

                        // handle up to one new session
                        if let Some(mut session) =
                            self.session_queue.try_recv().map(|v| v.into_inner())
                        {
                            let s = self.sessions.vacant_entry();
                            let interest = session.interest();
                            if session
                                .register(self.poll.registry(), Token(s.key()), interest)
                                .is_ok()
                            {
                                s.insert(ServerSession::new(session, self.protocol.clone()));
                            } else {
                                let _ = self.session_queue.try_send_any(session);
                            }

                            // trigger a wake-up in case there are more sessions
                            let _ = self.waker.wake();
                        }

                        // check if we received any signals from the admin thread
                        while let Some(signal) = self.signal_queue.try_recv() {
                            match signal.into_inner() {
                                Signal::FlushAll => {
                                    // the maintenance thread handles flush
                                }
                                Signal::Shutdown => {
                                    // if we received a shutdown, we can return
                                    // and stop processing events
                                    return;
                                }
                            }
                        }
                    }
                    _ => {
                        if event.is_error() {
                            WORKER_EVENT_ERROR.increment();

                            self.close(token);
                            continue;
                        }

                        if event.is_writable() {
                            WORKER_EVENT_WRITE.increment();

                            if self.write(token).is_err() {
                                self.close(token);
                                continue;
                            }
                        }

                        if event.is_readable() {
                            WORKER_EVENT_READ.increment();

                            if self.read(token).is_err() {
                                self.close(token);
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
}
```

Note the bound `Storage: EntryStore + Execute<…>` — `EntryStore` is not strictly used by the worker anymore but stays on `Workers`' spawn bounds; keep the worker bound minimal: if the build succeeds without `EntryStore` here, drop it from this impl.

### Task D2: Create `maintenance.rs`

- [ ] **Step 1: Write `src/core/server/src/workers/maintenance.rs`**

```rust
// Copyright 2026 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[metric(
    name = "maintenance_event_loop",
    description = "the number of times the maintenance event loop has run"
)]
pub static MAINTENANCE_EVENT_LOOP: Counter = Counter::new();

pub struct MaintenanceBuilder<Storage> {
    poll: Poll,
    storage: Arc<Storage>,
    timeout: Duration,
    waker: Arc<Waker>,
}

impl<Storage> MaintenanceBuilder<Storage> {
    pub fn new<T: WorkerConfig>(config: &T, storage: Arc<Storage>) -> Result<Self> {
        let config = config.worker();

        let poll = Poll::new()?;

        let waker = Arc::new(Waker::from(
            pelikan_net::Waker::new(poll.registry(), WAKER_TOKEN).unwrap(),
        ));

        let timeout = Duration::from_millis(config.timeout() as u64);

        Ok(Self {
            poll,
            storage,
            timeout,
            waker,
        })
    }

    pub fn waker(&self) -> Arc<Waker> {
        self.waker.clone()
    }

    pub fn build(self, signal_queue: Queues<(), Signal>) -> Maintenance<Storage> {
        Maintenance {
            poll: self.poll,
            signal_queue,
            storage: self.storage,
            timeout: self.timeout,
            waker: self.waker,
        }
    }
}

pub struct Maintenance<Storage> {
    poll: Poll,
    signal_queue: Queues<(), Signal>,
    storage: Arc<Storage>,
    timeout: Duration,
    waker: Arc<Waker>,
}

impl<Storage: EntryStore> Maintenance<Storage> {
    /// Run the maintenance thread in a loop, driving eager expiration and
    /// handling control-plane signals.
    pub fn run(&mut self) {
        let mut events = Events::with_capacity(1);

        loop {
            MAINTENANCE_EVENT_LOOP.increment();

            self.storage.expire();

            // wait for a signal wakeup or timeout
            if self.poll.poll(&mut events, Some(self.timeout)).is_err() {
                error!("Error polling");
            }

            if !events.is_empty() {
                self.waker.reset();
            }

            // check if we received any signals from the admin thread
            while let Some(signal) = self.signal_queue.try_recv() {
                match signal.into_inner() {
                    Signal::FlushAll => {
                        warn!("received flush_all");
                        self.storage.clear();
                    }
                    Signal::Shutdown => {
                        return;
                    }
                }
            }
        }
    }
}
```

### Task D3: Rewrite `workers/mod.rs`

- [ ] **Step 1: Delete the old implementations**

```bash
git rm src/core/server/src/workers/single.rs src/core/server/src/workers/multi.rs src/core/server/src/workers/storage.rs
```

- [ ] **Step 2: Rewrite `src/core/server/src/workers/mod.rs`**

Keep the seven `worker_*` metrics and `map_result` exactly as they are today (lines 17–65); replace everything from the `Workers` enum down:

```rust
// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use protocol_common::Protocol;
use std::thread::JoinHandle;

mod maintenance;
mod worker;

use maintenance::*;
use worker::*;

// [ ... the seven worker_* #[metric] statics, unchanged ... ]

fn map_result(result: Result<usize>) -> Result<()> {
    match result {
        Ok(0) => Err(Error::other("client hangup")),
        Ok(_) => Ok(()),
        Err(e) => map_err(e),
    }
}

pub struct Workers<Proto, Request, Response, Storage> {
    workers: Vec<Worker<Proto, Request, Response, Storage>>,
    maintenance: Maintenance<Storage>,
}

impl<Proto, Request, Response, Storage> Workers<Proto, Request, Response, Storage>
where
    Proto: 'static + Protocol<Request, Response> + Clone + Send,
    Request: 'static + Klog + Klog<Response = Response> + Send,
    Response: 'static + Compose + Send,
    Storage: 'static + EntryStore + Execute<Request, Response> + Send + Sync,
{
    pub fn spawn(self) -> Vec<JoinHandle<()>> {
        let mut maintenance = self.maintenance;
        let mut join_handles = vec![std::thread::Builder::new()
            .name(format!("{THREAD_PREFIX}_maint"))
            .spawn(move || maintenance.run())
            .unwrap()];

        for (id, mut worker) in self.workers.into_iter().enumerate() {
            join_handles.push(
                std::thread::Builder::new()
                    .name(format!("{THREAD_PREFIX}_work_{id}"))
                    .spawn(move || worker.run())
                    .unwrap(),
            )
        }

        join_handles
    }
}

pub struct WorkersBuilder<Proto, Request, Response, Storage> {
    workers: Vec<WorkerBuilder<Proto, Request, Response, Storage>>,
    maintenance: MaintenanceBuilder<Storage>,
}

impl<Proto, Request, Response, Storage> WorkersBuilder<Proto, Request, Response, Storage>
where
    Proto: Protocol<Request, Response> + Clone,
    Response: Compose,
    Storage: Execute<Request, Response> + EntryStore,
{
    pub fn new<T: WorkerConfig>(config: &T, protocol: Proto, storage: Storage) -> Result<Self> {
        let threads = config.worker().threads();
        let storage = Arc::new(storage);

        let mut workers = vec![];
        for _ in 0..threads {
            workers.push(WorkerBuilder::new(config, protocol.clone(), storage.clone())?)
        }

        Ok(Self {
            workers,
            maintenance: MaintenanceBuilder::new(config, storage)?,
        })
    }

    pub fn worker_wakers(&self) -> Vec<Arc<Waker>> {
        self.workers.iter().map(|w| w.waker()).collect()
    }

    pub fn wakers(&self) -> Vec<Arc<Waker>> {
        let mut wakers = vec![self.maintenance.waker()];
        for worker in &self.workers {
            wakers.push(worker.waker());
        }
        wakers
    }

    pub fn build(
        self,
        mut session_queues: Vec<Queues<Session, Session>>,
        mut signal_queues: Vec<Queues<(), Signal>>,
    ) -> Workers<Proto, Request, Response, Storage> {
        // The maintenance thread precedes the worker threads in the set of
        // wakers, so its signal queue is the first element of
        // `signal_queues`. We remove it and build the maintenance thread so
        // we can loop through the remaining queues when building the
        // workers.
        let maintenance = self.maintenance.build(signal_queues.remove(0));

        let mut workers = Vec::new();
        for builder in self.workers {
            workers.push(builder.build(session_queues.remove(0), signal_queues.remove(0)));
        }

        Workers {
            workers,
            maintenance,
        }
    }
}
```

(`process.rs` needs no changes: `wakers()` still lists the signal-fanout receivers with the maintenance/storage slot first, `worker_wakers()` still lists session receivers, and `spawn()` has the same signature.)

- [ ] **Step 3: Remove `QUEUE_RETRIES` from `src/core/server/src/lib.rs` if unused**

```bash
grep -rn "QUEUE_RETRIES" src/core/server/src/
```

If the only hit is the definition in `lib.rs:125` (plus its comment block on 122–124), delete those lines. If `listener.rs` uses it, leave it.

- [ ] **Step 4: Build and lint**

```bash
cargo build --workspace 2>&1 | tail -10 && cargo clippy --all-targets --all-features 2>&1 | tail -5
```

Expected: clean. Likely follow-ups if not: unused imports in `workers/mod.rs` (the old file imported `multi::*`/`single::*`/`storage::*`), and the `EntryStore` bound on `Worker`'s impl block if the compiler flags it as unused (drop it there, keep it on `Workers`).

- [ ] **Step 5: Run the full test suite — both thread models**

```bash
cargo test --workspace 2>&1 | tail -15
```

Expected: all pass. `integration` exercises one worker + maintenance; `integration_multi` exercises two workers sharing the engine concurrently (first time this path truly runs concurrent storage). The admin tests within them cover `flush_all` via the signal path, which now lands in the maintenance thread.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat!: unified Arc-shared worker model

Workers share the concurrent segcache engine via Arc and execute
requests in place; the dedicated storage thread and its per-request
queue round-trip are gone. A maintenance thread drives eager TTL
expiration and handles flush_all. Single-worker mode is now just N=1
of the same model."
```

---

## Phase D-rev: lazy expiry in the engine; drop the maintenance thread

> **Status 2026-08-18:** Added after Phase D landed (commit `37378e3`). Design
> revision approved by the user: the engine gains lazy deadline checks
> (segcache 0.4.1), after which pelikan needs no periodic `expire()` and the
> maintenance thread is deleted. Supersedes the maintenance-thread parts of
> Tasks D2/D3. The spec's section 4 has the full rationale.

### Task A5: cache-rs — lazy deadline checks + segcache 0.4.1 release

In /Users/brian/workspace/brayniac/cache-rs (branch from origin/main):

- [ ] `get_pinned`: after the post-pin revalidation succeeds (key still maps to
  `location`, segment pinned so its header is authoritative), check
  `self.remaining_ttl(seg_id)`; on `Err` drop the guard and return `None`.
  Covers `get` and `get_no_freq_incr`.
- [ ] `cas`: after resolving the current item's location/seg_id, check
  `remaining_ttl` and return `Err(SegcacheError::NotFound)` for expired items
  (memcached: cas on expired key → NOT_FOUND).
- [ ] `delete`: expired item → return `false` (memcached: NOT_FOUND).
- [ ] TDD: failing tests first (insert with short TTL, wait past deadline, no
  `expire()` call, assert get→None / cas→NotFound / delete→false), following
  the existing TTL test patterns in the crate.
- [ ] Docs: `get`/`cas`/`delete` doc comments + `docs/segcache.md` gain the
  lazy-expiry statement.
- [ ] PR per repo convention, CI green, merge; then version-bump PR
  `segcache 0.4.1` (behavioral fix toward documented TTL semantics), merge.
- [ ] Publish 0.4.1 from a refreshed clean clone (dry-run first). User already
  approved this release in the design revision.

### Task D4: pelikan — remove the maintenance thread

- [ ] Bump workspace `segcache = "0.4.1"`, `cargo update -p segcache`.
- [ ] Delete `src/core/server/src/workers/maintenance.rs`; remove the
  `maintenance` module/field/builder from `workers/mod.rs` (`Workers` holds
  only `Vec<Worker>`; `wakers()` = worker wakers only — process.rs wiring
  still works since it only strips index 0 for the listener).
- [ ] `worker.rs`: `Signal::FlushAll` handler becomes `self.storage.clear()`
  (broadcast means every worker clears; duplicates are cheap no-ops).
- [ ] Remove `EntryStore::expire` from the trait and its implementors (Seg,
  Noop) — nothing calls it anymore. Keep `clear`.
- [ ] Full build/clippy/test gate as in Phase D; verify a manual `flush_all`
  smoke test against a running server; commit.

---

## Phase E: docs, diagrams, journal (final commits)

### Task E1: Rewrite the thread-model documentation

**Files:**
- Modify: `src/core/server/src/lib.rs:5-95` (module doc comment)
- Modify: `docs/ARCHITECTURE.md` (storage-thread references)

- [ ] **Step 1: Replace the two ASCII thread-model diagrams in `lib.rs` with one**

The doc comment describes a single model: `admin`, `listener`, N `worker` threads sharing the cache datastructure (`Arc`-shared, internally synchronized), and a `maintenance` thread driving eager expiration and handling `flush_all`. Remove the "Single worker thread model" / "Multiple worker thread model" split and the storage-thread paragraphs; describe the worker count as a scaling knob, not a mode switch.

- [ ] **Step 2: Update `docs/ARCHITECTURE.md`**

```bash
grep -n "storage thread\|storage worker\|StorageWorker" docs/ARCHITECTURE.md
```

Rewrite each hit to match the new model (workers share the concurrent engine; maintenance thread owns expiration).

- [ ] **Step 3: Regenerate the diagram set**

Invoke the `architecture-diagram` skill (per repo convention) and run:

```bash
cargo xtask diagrams
```

Expected: runtime threading/dataflow charts no longer show a storage thread or worker↔storage queues.

- [ ] **Step 3b: Reviewer drive-bys (from Phase C quality review)**

- `src/entrystore/src/lib.rs:8` — doc comment references `protocol::memcache::MemcacheStorage`, which doesn't exist; fix the reference.
- `src/protocol/http/src/lib.rs:34-38` — the http `Storage` trait still takes `&mut self` and has no implementors; add a `// NOTE:` that it predates the &self conversion and should be converted when http is next touched (or convert it if trivial).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: thread model reflects Arc-shared workers"
```

### Task E2: Journal entry

- [ ] **Step 1: Invoke the `journal` skill** to scaffold `docs/journal/` entry covering: why the storage thread existed, what the concurrent engine changed, the staged conversion (deps → traits → threading), and the accepted semantic changes (CAS-vs-eviction; add/replace check-then-act non-atomicity and the cas delete-after tail, accepted 2026-08-18 pending engine conditional-insert primitives).

- [ ] **Step 2: Commit**

```bash
git add docs/journal && git commit -m "docs: journal the concurrent segcache conversion"
```

## Phase F: engine hardening (added 2026-08-18, blocks the PR)

> The pre-PR adversarial review found five critical engine-level bugs, all
> newly reachable now that pelikan shares the cache concurrently (default
> Merge eviction policy, plain numeric ops). User decision: fix all five in
> cache-rs before shipping. Three work areas, each TDD'd with reproducing
> tests; a finding that cannot be reproduced is reported back as a possible
> false positive, not "fixed".

- [ ] **F1 — pin-failure protocol** (findings: false absence during merge
  drains; acked delete resurrected by merge relocation; replace-vs-drain
  deadlock): `get_pinned` retries the lookup on pin failure (bounded, like
  `numeric_update`); `delete` on remover-pin failure still unlinks via
  `hashtable.remove` (merge drains relocate rather than sweep); `insert`'s
  replace arm and `replace_at` break the deadlock by rolling back the
  reservation when the old item's segment == the reserved segment and the
  remover pin fails (the drain is provably waiting on our WriterPin).
- [ ] **F2 — numeric atomicity** (finding: concurrent incr/decr lose
  updates): `keyvalue::seqlocked_update`'s non-atomic RMW becomes atomic
  (`fetch_add` for wrapping add; CAS loop for saturating sub; version
  `fetch_add(2)` after). keyvalue patch release.
- [ ] **F3 — cas publish integrity** (finding: cas racing incr/decr returns
  false STORED, destroying acked increments): re-verify the full token
  (including the numeric seqlock version) under the pin immediately before
  the location slot-CAS publish; fail `Exists` on mismatch.
- [ ] **F4 — git-pin iteration** (user decision 2026-08-18: work on git
  dependencies while hardening, defer releases): re-add the
  `[patch.crates-io]` pin in pelikan pointing at cache-rs main, advancing
  the rev as each engine fix merges; run the full pelikan gate against each
  rev. Adversarial re-review of the engine deltas once F1–F3 are all in.
  Iterate further as needed — releases wait until everything is fleshed out.
- [ ] **F5 — release + ship**: when hardening settles: keyvalue 0.3.1 +
  segcache 0.4.2 bump PRs, publish from refreshed clean clone, drop the
  pelikan git pin (swap to published 0.4.2), full gate, then Task E3.

Non-critical findings recorded for follow-up (cache-rs issues, not this PR):
cas-vs-delete returns EXISTS where serialized execution gave NOT_FOUND;
16-bit generation ABA (theoretical); eviction overshoot (up to 3 segments per
insert); numeric canonicalization breaks byte-transparency for "007"/"+42"
(pre-existing on main); `ERROR` vs memcached's `CLIENT_ERROR` for
non-numeric incr (pre-existing).

### Task E3: Final verification and PR

- [ ] **Step 0: Remove the temporary git pin (blocks merge until publishes land)**

Once keyvalue 0.3.0 and segcache 0.4.0 are on crates.io (Tasks A3/A4), delete the `[patch.crates-io]` block from the root `Cargo.toml`, then:

```bash
cargo update -p segcache && cargo test --workspace 2>&1 | tail -10
git add Cargo.toml Cargo.lock && git commit -m "deps: use published segcache 0.4.0, drop git pin"
```

Expected: lockfile now sources segcache from `registry+https://github.com/rust-lang/crates.io-index`; all tests pass.

- [ ] **Step 1: Full verification (superpowers:verification-before-completion)**

```bash
cargo fmt --all -- --check && cargo clippy --all-targets --all-features 2>&1 | tail -3 && cargo test --workspace 2>&1 | tail -10
```

Expected: fmt clean, clippy clean, all tests pass.

- [ ] **Step 2: Adversarial self-review**

Invoke the `pr-adversarial-review` skill on the branch diff (repo's standard pre-PR step). Pay attention to: signal-queue index ordering in `WorkersBuilder::build` vs `wakers()`, shutdown joining the maintenance thread, and worker `FlushAll` no-op.

- [ ] **Step 3: Open the PR**

Invoke the `pr` skill to push `concurrent-segcache` and open a PR against pelikan-io/pelikan, referencing the spec and this plan. The PR description must call out the operational changes (Phase D quality review, M3): the `storage_event_loop`/`storage_queue_depth` metrics are gone (external dashboards may chart them); `flush_all` now logs one `warn!` line per worker instead of one; the single-worker thread name changed from `pelikan_work` to `pelikan_work_0` (ops tooling matching on thread names); and the accepted semantic changes from the spec (add/replace races, flush smear, no periodic expire — memory for expired segments is reclaimed under write pressure).

---

## Out of scope

- Pelikan's own version bump / release (use the `release` skill separately after merge).
- cache-rs CLAUDE.md staleness (it still says "All reads require `&mut self`… intentional") — worth a docs PR in cache-rs, but not part of this conversion.
- Append/prepend support, `try_into_numeric` adoption for incr-with-initial semantics — future work enabled by the new engine.
