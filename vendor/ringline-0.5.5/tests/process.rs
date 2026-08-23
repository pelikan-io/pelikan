#![allow(clippy::manual_async_fn)]
#![cfg(has_io_uring)]
//! Integration tests for async process spawning.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};

use ringline::process::Command;
use ringline::{AsyncEventHandler, Config, ConfigBuilder, ConnCtx, RinglineBuilder};

fn test_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .max_connections(64)
        .send_pool(64, 16384)
        .resolver_threads(0)
}

fn test_config() -> Config {
    test_config_builder().build().expect("valid config")
}

// ── Spawn + wait (success) ──────────────────────────────────────────

static SPAWN_OK: AtomicU32 = AtomicU32::new(0);

struct SpawnTrueHandler;

impl AsyncEventHandler for SpawnTrueHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let child = Command::new("true").spawn().unwrap().await.unwrap();
            let status = child.wait().unwrap().await.unwrap();
            if status.success() {
                SPAWN_OK.store(1, Ordering::SeqCst);
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        SpawnTrueHandler
    }
}

#[test]
fn process_spawn_true() {
    SPAWN_OK.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<SpawnTrueHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(SPAWN_OK.load(Ordering::SeqCst), 1);
}

// ── Spawn + wait (failure exit) ─────────────────────────────────────

static SPAWN_FAIL_CODE: AtomicI32 = AtomicI32::new(-1);

struct SpawnFalseHandler;

impl AsyncEventHandler for SpawnFalseHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let child = Command::new("false").spawn().unwrap().await.unwrap();
            let status = child.wait().unwrap().await.unwrap();
            SPAWN_FAIL_CODE.store(status.code(), Ordering::SeqCst);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        SpawnFalseHandler
    }
}

#[test]
fn process_spawn_false() {
    SPAWN_FAIL_CODE.store(-1, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<SpawnFalseHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    let code = SPAWN_FAIL_CODE.load(Ordering::SeqCst);
    assert_ne!(code, 0, "expected non-zero exit code from `false`");
}

// ── Spawn with args ─────────────────────────────────────────────────

static SPAWN_ARGS: AtomicU32 = AtomicU32::new(0);

struct SpawnArgsHandler;

impl AsyncEventHandler for SpawnArgsHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let child = Command::new("test")
                .arg("1")
                .arg("-eq")
                .arg("1")
                .spawn()
                .unwrap()
                .await
                .unwrap();
            let status = child.wait().unwrap().await.unwrap();
            if status.success() {
                SPAWN_ARGS.store(1, Ordering::SeqCst);
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        SpawnArgsHandler
    }
}

#[test]
fn process_spawn_with_args() {
    SPAWN_ARGS.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<SpawnArgsHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(SPAWN_ARGS.load(Ordering::SeqCst), 1);
}

// ── Kill ────────────────────────────────────────────────────────────

static KILL_OK: AtomicU32 = AtomicU32::new(0);

struct KillHandler;

impl AsyncEventHandler for KillHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let child = Command::new("sleep")
                .arg("60")
                .spawn()
                .unwrap()
                .await
                .unwrap();
            child.kill().unwrap();
            let status = child.wait().unwrap().await.unwrap();
            // Killed by signal — exit code is non-zero.
            if !status.success() {
                KILL_OK.store(1, Ordering::SeqCst);
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        KillHandler
    }
}

#[test]
fn process_kill() {
    KILL_OK.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<KillHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(KILL_OK.load(Ordering::SeqCst), 1);
}

// ── Spawner disabled ────────────────────────────────────────────────

static SPAWNER_DISABLED: AtomicU32 = AtomicU32::new(0);

struct SpawnerDisabledHandler;

impl AsyncEventHandler for SpawnerDisabledHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            match Command::new("true").spawn() {
                Err(_) => SPAWNER_DISABLED.store(1, Ordering::SeqCst),
                Ok(_) => SPAWNER_DISABLED.store(99, Ordering::SeqCst),
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        SpawnerDisabledHandler
    }
}

#[test]
fn process_spawner_disabled() {
    SPAWNER_DISABLED.store(0, Ordering::SeqCst);

    let config = test_config_builder()
        .spawner_threads(0)
        .build()
        .expect("valid config");
    let (_shutdown, handles) = RinglineBuilder::new(config)
        .launch::<SpawnerDisabledHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(SPAWNER_DISABLED.load(Ordering::SeqCst), 1);
}

// ── Command::args builder ───────────────────────────────────────────

static ARGS_BUILDER: AtomicU32 = AtomicU32::new(0);

struct ArgsBuilderHandler;

impl AsyncEventHandler for ArgsBuilderHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            // Use .args() with an iterator.
            let child = Command::new("test")
                .args(["1", "-eq", "1"])
                .spawn()
                .unwrap()
                .await
                .unwrap();
            let status = child.wait().unwrap().await.unwrap();
            if status.success() {
                ARGS_BUILDER.store(1, Ordering::SeqCst);
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        ArgsBuilderHandler
    }
}

#[test]
fn process_args_builder() {
    ARGS_BUILDER.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<ArgsBuilderHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(ARGS_BUILDER.load(Ordering::SeqCst), 1);
}
