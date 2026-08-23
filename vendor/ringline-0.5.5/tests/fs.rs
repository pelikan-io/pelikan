#![allow(clippy::manual_async_fn)]
//! Integration tests for the async fs module.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};

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

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("ringline-fs-test-{}-{name}", std::process::id()))
}

// ── Create + write + read ───────────────────────────────────────────

static FS_READ_RESULT: AtomicU32 = AtomicU32::new(0);

struct FsReadWriteHandler;

impl AsyncEventHandler for FsReadWriteHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let path = temp_path("rw.txt");

            // Create and write.
            let file = ringline::fs::create(&path).unwrap().await.unwrap();
            let data = b"hello ringline fs";
            let n = unsafe {
                ringline::fs::write(file, 0, data.as_ptr(), data.len() as u32)
                    .unwrap()
                    .await
            };
            assert!(n.is_ok());

            // Fsync.
            ringline::fs::fsync(file).unwrap().await.ok();

            // Close and reopen for read.
            ringline::fs::close(file).unwrap();

            let file = ringline::fs::open(&path, ringline::fs::OpenFlags::READ, 0)
                .unwrap()
                .await
                .unwrap();
            let mut buf = [0u8; 64];
            let result = unsafe {
                ringline::fs::read(file, 0, buf.as_mut_ptr(), buf.len() as u32)
                    .unwrap()
                    .await
            };
            match result {
                Ok(n) if n > 0 && &buf[..n as usize] == b"hello ringline fs" => {
                    FS_READ_RESULT.store(1, Ordering::SeqCst);
                }
                _ => {}
            }

            ringline::fs::close(file).unwrap();
            let _ = std::fs::remove_file(&path);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        FsReadWriteHandler
    }
}

#[test]
fn fs_create_write_read() {
    FS_READ_RESULT.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<FsReadWriteHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(FS_READ_RESULT.load(Ordering::SeqCst), 1);
}

// ── Stat ────────────────────────────────────────────────────────────

static FS_STAT_RESULT: AtomicU32 = AtomicU32::new(0);

struct FsStatHandler;

impl AsyncEventHandler for FsStatHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let path = temp_path("stat.txt");

            // Create a file with known content.
            std::fs::write(&path, b"stat test data").unwrap();

            let meta = ringline::fs::stat(&path).unwrap().await.unwrap();
            if meta.size == 14 && meta.is_file && !meta.is_dir {
                FS_STAT_RESULT.store(1, Ordering::SeqCst);
            }

            let _ = std::fs::remove_file(&path);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        FsStatHandler
    }
}

#[test]
fn fs_stat_file() {
    FS_STAT_RESULT.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<FsStatHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(FS_STAT_RESULT.load(Ordering::SeqCst), 1);
}

// ── Rename ──────────────────────────────────────────────────────────

static FS_RENAME_RESULT: AtomicU32 = AtomicU32::new(0);

struct FsRenameHandler;

impl AsyncEventHandler for FsRenameHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let old = temp_path("rename-old.txt");
            let new = temp_path("rename-new.txt");

            std::fs::write(&old, b"rename me").unwrap();
            let _ = std::fs::remove_file(&new);

            let result = ringline::fs::rename(&old, &new).unwrap().await;
            if result.is_ok() && !old.exists() && new.exists() {
                let data = std::fs::read(&new).unwrap();
                if data == b"rename me" {
                    FS_RENAME_RESULT.store(1, Ordering::SeqCst);
                }
            }

            let _ = std::fs::remove_file(&new);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        FsRenameHandler
    }
}

#[test]
fn fs_rename_file() {
    FS_RENAME_RESULT.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<FsRenameHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(FS_RENAME_RESULT.load(Ordering::SeqCst), 1);
}

// ── Remove ──────────────────────────────────────────────────────────

static FS_REMOVE_RESULT: AtomicU32 = AtomicU32::new(0);

struct FsRemoveHandler;

impl AsyncEventHandler for FsRemoveHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let path = temp_path("remove.txt");
            std::fs::write(&path, b"delete me").unwrap();

            let result = ringline::fs::remove(&path).unwrap().await;
            if result.is_ok() && !path.exists() {
                FS_REMOVE_RESULT.store(1, Ordering::SeqCst);
            }

            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        FsRemoveHandler
    }
}

#[test]
fn fs_remove_file() {
    FS_REMOVE_RESULT.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<FsRemoveHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(FS_REMOVE_RESULT.load(Ordering::SeqCst), 1);
}

// ── Mkdir ───────────────────────────────────────────────────────────

static FS_MKDIR_RESULT: AtomicU32 = AtomicU32::new(0);

struct FsMkdirHandler;

impl AsyncEventHandler for FsMkdirHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let path = temp_path("testdir");
            let _ = std::fs::remove_dir(&path);

            let result = ringline::fs::mkdir(&path, 0o755).unwrap().await;
            if result.is_ok() && path.exists() {
                let meta = ringline::fs::stat(&path).unwrap().await.unwrap();
                if meta.is_dir {
                    FS_MKDIR_RESULT.store(1, Ordering::SeqCst);
                }
            }

            let _ = std::fs::remove_dir(&path);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        FsMkdirHandler
    }
}

#[test]
fn fs_mkdir_and_stat() {
    FS_MKDIR_RESULT.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<FsMkdirHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(FS_MKDIR_RESULT.load(Ordering::SeqCst), 1);
}

// ── Safe owned-buffer API: read_into / write_from ───────────────────

static FS_SAFE_RESULT: AtomicU32 = AtomicU32::new(0);

struct FsSafeRoundtripHandler;

impl AsyncEventHandler for FsSafeRoundtripHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let path = temp_path("safe-rw.txt");
            let payload: &[u8] = b"safe api roundtrip data";

            // write_from: hand the runtime an owned BytesMut, get it back.
            let file = ringline::fs::create(&path).unwrap().await.unwrap();
            let mut wbuf = bytes::BytesMut::with_capacity(payload.len());
            wbuf.extend_from_slice(payload);
            let (wres, wbuf) = ringline::fs::write_from(file, 0, wbuf).unwrap().await;
            assert_eq!(wres.unwrap(), payload.len());
            // Buffer is returned with len unchanged.
            assert_eq!(&wbuf[..], payload);
            ringline::fs::close(file).unwrap();

            // read_into: kernel fills spare capacity, future yields updated buf.
            let file = ringline::fs::open(&path, ringline::fs::OpenFlags::READ, 0)
                .unwrap()
                .await
                .unwrap();
            let rbuf = bytes::BytesMut::with_capacity(64);
            let (rres, rbuf) = ringline::fs::read_into(file, 0, rbuf).unwrap().await;
            let n = rres.unwrap();
            if n == payload.len() && &rbuf[..n] == payload {
                FS_SAFE_RESULT.store(1, Ordering::SeqCst);
            }

            ringline::fs::close(file).unwrap();
            let _ = std::fs::remove_file(&path);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        FsSafeRoundtripHandler
    }
}

#[test]
fn fs_safe_api_roundtrip() {
    FS_SAFE_RESULT.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<FsSafeRoundtripHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(FS_SAFE_RESULT.load(Ordering::SeqCst), 1);
}

// ── Drop in-flight: buffer must be parked, no UAF, follow-up read works ─

static FS_DROP_RESULT: AtomicU32 = AtomicU32::new(0);

struct FsDropInFlightHandler;

impl AsyncEventHandler for FsDropInFlightHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let path = temp_path("safe-drop.txt");
            let payload: &[u8] = b"abandon the first read";

            std::fs::write(&path, payload).unwrap();
            let file = ringline::fs::open(&path, ringline::fs::OpenFlags::READ, 0)
                .unwrap()
                .await
                .unwrap();

            // Submit a read and immediately drop the future without awaiting.
            // The buffer must be parked in the runtime until the kernel CQE
            // arrives; if it isn't, the kernel may scribble into freed memory.
            {
                let _fut =
                    ringline::fs::read_into(file, 0, bytes::BytesMut::with_capacity(payload.len()))
                        .unwrap();
                // _fut dropped here without poll.
            }

            // Issue a second read on the same file. If the graveyard logic
            // is broken, this is where memory corruption would surface.
            let rbuf = bytes::BytesMut::with_capacity(payload.len());
            let (rres, rbuf) = ringline::fs::read_into(file, 0, rbuf).unwrap().await;
            let n = rres.unwrap();
            if n == payload.len() && &rbuf[..n] == payload {
                FS_DROP_RESULT.store(1, Ordering::SeqCst);
            }

            ringline::fs::close(file).unwrap();
            let _ = std::fs::remove_file(&path);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        FsDropInFlightHandler
    }
}

#[test]
fn fs_safe_api_drop_in_flight() {
    FS_DROP_RESULT.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<FsDropInFlightHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(FS_DROP_RESULT.load(Ordering::SeqCst), 1);
}
