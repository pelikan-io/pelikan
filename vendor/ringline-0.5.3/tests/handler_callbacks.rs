//! Regression test: `ringline::spawn()` and other `with_state`-based
//! APIs must work from inside `AsyncEventHandler::on_tick` and
//! `on_notify`. The event loop must set the `CURRENT_DRIVER` thread-local
//! before calling these handler hooks.

#![cfg(target_os = "linux")]
#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, ConnCtx, DriverCtx, RinglineBuilder};

struct OnTickSpawner {
    spawn_attempted: Arc<AtomicBool>,
    spawn_ok: Arc<AtomicBool>,
    task_ran: Arc<AtomicUsize>,
}

impl AsyncEventHandler for OnTickSpawner {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_tick(&mut self, _ctx: &mut DriverCtx<'_>) {
        if self.spawn_attempted.swap(true, Ordering::AcqRel) {
            return;
        }
        let task_ran = self.task_ran.clone();
        match ringline::spawn(async move {
            task_ran.fetch_add(1, Ordering::AcqRel);
        }) {
            Ok(_) => self.spawn_ok.store(true, Ordering::Release),
            Err(_) => self.spawn_ok.store(false, Ordering::Release),
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        OnTickSpawner {
            spawn_attempted: Arc::new(AtomicBool::new(false)),
            spawn_ok: Arc::new(AtomicBool::new(false)),
            task_ran: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Static handles so the test can observe the per-worker handler state.
/// One worker only, set in `create_for_worker` above only the first time.
mod tick_observer {
    use super::*;
    use std::sync::OnceLock;

    pub static OBSERVER: OnceLock<(Arc<AtomicBool>, Arc<AtomicUsize>)> = OnceLock::new();

    pub fn record(spawn_ok: Arc<AtomicBool>, task_ran: Arc<AtomicUsize>) {
        let _ = OBSERVER.set((spawn_ok, task_ran));
    }
}

struct ObservedTickSpawner {
    inner: OnTickSpawner,
}

impl AsyncEventHandler for ObservedTickSpawner {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx<'_>) {
        self.inner.on_tick(ctx);
    }

    fn create_for_worker(id: usize) -> Self {
        let inner = OnTickSpawner::create_for_worker(id);
        tick_observer::record(inner.spawn_ok.clone(), inner.task_ran.clone());
        ObservedTickSpawner { inner }
    }
}

#[test]
fn spawn_from_on_tick_succeeds() {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .max_registered_regions(4)
        .build()
        .expect("valid config");

    let (shutdown, handles) = RinglineBuilder::new(config)
        .launch::<ObservedTickSpawner>()
        .unwrap();

    // Wait briefly for the worker's first on_tick to run.
    let mut waited = 0;
    let (spawn_ok, task_ran) = loop {
        if let Some(observed) = tick_observer::OBSERVER.get()
            && observed.0.load(Ordering::Acquire)
            && observed.1.load(Ordering::Acquire) > 0
        {
            break observed.clone();
        }
        std::thread::sleep(Duration::from_millis(20));
        waited += 1;
        assert!(waited < 200, "on_tick never spawned a task");
    };

    assert!(
        spawn_ok.load(Ordering::Acquire),
        "ringline::spawn() returned Err inside on_tick — driver_state was not set",
    );
    assert!(
        task_ran.load(Ordering::Acquire) >= 1,
        "spawned task did not run",
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Same coverage but for `on_notify`. We trigger it by waking the worker
/// via its `WakeHandle`.
struct OnNotifySpawner {
    spawn_ok: Arc<AtomicBool>,
    task_ran: Arc<AtomicUsize>,
}

mod notify_observer {
    use super::*;
    use std::sync::OnceLock;
    pub static OBSERVER: OnceLock<(Arc<AtomicBool>, Arc<AtomicUsize>)> = OnceLock::new();
    pub fn record(spawn_ok: Arc<AtomicBool>, task_ran: Arc<AtomicUsize>) {
        let _ = OBSERVER.set((spawn_ok, task_ran));
    }
}

impl AsyncEventHandler for OnNotifySpawner {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_notify(&mut self, _ctx: &mut DriverCtx<'_>) {
        let task_ran = self.task_ran.clone();
        if ringline::spawn(async move {
            task_ran.fetch_add(1, Ordering::AcqRel);
        })
        .is_ok()
        {
            self.spawn_ok.store(true, Ordering::Release);
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        let h = OnNotifySpawner {
            spawn_ok: Arc::new(AtomicBool::new(false)),
            task_ran: Arc::new(AtomicUsize::new(0)),
        };
        notify_observer::record(h.spawn_ok.clone(), h.task_ran.clone());
        h
    }
}

#[test]
fn spawn_from_on_notify_succeeds() {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .max_registered_regions(4)
        .build()
        .expect("valid config");

    let (shutdown, handles) = RinglineBuilder::new(config)
        .launch::<OnNotifySpawner>()
        .unwrap();

    // Force a wakeup so on_notify runs at least once.
    let waker = shutdown
        .worker_wake_handle(0)
        .expect("worker 0 wake handle");
    waker.wake();

    let mut waited = 0;
    let (spawn_ok, task_ran) = loop {
        if let Some(observed) = notify_observer::OBSERVER.get()
            && observed.0.load(Ordering::Acquire)
            && observed.1.load(Ordering::Acquire) > 0
        {
            break observed.clone();
        }
        std::thread::sleep(Duration::from_millis(20));
        waited += 1;
        if waited >= 50 {
            // Wake again in case the first wake landed before the
            // handler was installed.
            waker.wake();
        }
        assert!(waited < 200, "on_notify never spawned a task");
    };

    assert!(
        spawn_ok.load(Ordering::Acquire),
        "ringline::spawn() returned Err inside on_notify — driver_state was not set",
    );
    assert!(
        task_ran.load(Ordering::Acquire) >= 1,
        "spawned task did not run",
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}
