//! Regression: dropping a `ShutdownHandle` without calling `.shutdown()`
//! must trigger graceful worker shutdown so the RAII idiom
//! `drop(shutdown); for h in handles { h.join() }` actually returns.
//!
//! Before the fix, `Drop` was a no-op and the workers ran forever — the
//! `cargo bench -p ringline --bench buffer` suite reliably hung after
//! the first benchmark group because it dropped the handle between
//! sizes and then waited on `JoinHandle::join()`.

#![cfg(target_os = "linux")]
#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, Config, ConfigBuilder, ConnCtx, RinglineBuilder};

struct NoopHandler;

impl AsyncEventHandler for NoopHandler {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn create_for_worker(_id: usize) -> Self {
        NoopHandler
    }
}

fn test_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .max_registered_regions(4)
}

fn test_config() -> Config {
    test_config_builder().build().expect("valid config")
}

#[test]
fn drop_handle_triggers_shutdown_and_join_returns() {
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<NoopHandler>()
        .expect("launch");

    // Drop the handle WITHOUT calling shutdown() — this is what RAII
    // users (and `ringline/benches/buffer.rs` between iterations) do.
    drop(shutdown);

    // Move joins onto a helper thread so a regression can't hang the
    // whole test binary — the deadline below converts a hang into a
    // failed assertion.
    let joiner = std::thread::spawn(move || {
        for h in handles {
            h.join().expect("worker panicked").expect("worker error");
        }
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while !joiner.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        joiner.is_finished(),
        "workers did not exit within 5s after ShutdownHandle drop \
         — Drop probably no longer signals shutdown",
    );
    joiner.join().expect("joiner thread panicked");
}

#[test]
fn explicit_shutdown_then_drop_is_idempotent() {
    // Verifies that the Drop impl doesn't double-close or otherwise
    // misbehave when the caller already invoked shutdown() explicitly
    // — i.e. existing callers that follow the previous pattern keep
    // working.
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<NoopHandler>()
        .expect("launch");

    shutdown.shutdown();
    // Drop after explicit shutdown — should be a clean no-op-ish second
    // call to shutdown().
    drop(shutdown);

    let joiner = std::thread::spawn(move || {
        for h in handles {
            h.join().expect("worker panicked").expect("worker error");
        }
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !joiner.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        joiner.is_finished(),
        "workers did not exit within 5s after explicit shutdown + drop",
    );
    joiner.join().expect("joiner thread panicked");
}
