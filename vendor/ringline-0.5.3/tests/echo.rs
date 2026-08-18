#![allow(clippy::manual_async_fn)]
//! Integration tests: echo server using real TCP connections.
//!
//! Each test launches a ringline server, connects via std TCP, sends data,
//! and verifies the echoed response.

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::pin::Pin;
use std::time::Duration;

use ringline::{AsyncEventHandler, Config, ConfigBuilder, ConnCtx, ParseResult, RinglineBuilder};
use std::sync::atomic::{AtomicU32, Ordering};

// ── Async echo handler ─────────────────────────────────────────────

struct AsyncEcho;

impl AsyncEventHandler for AsyncEcho {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            loop {
                let n = conn
                    .with_data(|data| {
                        let _ = conn.send_nowait(data);
                        ParseResult::Consumed(data.len())
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        AsyncEcho
    }
}

// ── Burst sender (exercises the coalesced send path) ───────────────

const BURST_N: u32 = 100;
const BURST_MSG: usize = 64;

/// On accept, fires `BURST_N` distinct small messages back-to-back without
/// awaiting between them, so they queue on the connection and drain through the
/// coalesced `sendmsg` path. Message `i` is `BURST_MSG` bytes all set to
/// `(i % 251)`, so the client can verify order and detect any reordering or
/// truncation.
struct BurstSender;

impl AsyncEventHandler for BurstSender {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            for i in 0..BURST_N {
                let msg = [(i % 251) as u8; BURST_MSG];
                // Retry only if the copy pool is transiently exhausted.
                while conn.send_nowait(&msg).is_err() {
                    ringline::sleep(Duration::from_micros(50)).await;
                }
            }
            // Keep the connection open until the client finishes reading.
            loop {
                let n = conn.with_data(|d| ParseResult::Consumed(d.len())).await;
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        BurstSender
    }
}

// ── Zero-copy recv-forward echo handler ────────────────────────────

/// Echo via the multi-buffer zero-copy recv-forward path: held provided recv
/// buffers are scatter-gathered back in one `sendmsg`, no accumulator copy.
struct RecvForwardEcho;

impl AsyncEventHandler for RecvForwardEcho {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            conn.enable_recv_forward();
            loop {
                conn.recv_ready().await;
                let n = match conn.forward_held() {
                    Ok(f) => f.await.unwrap_or(0),
                    Err(_) => break,
                };
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        RecvForwardEcho
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn test_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .max_connections(64)
        .send_pool(64, 16384)
}

fn test_config() -> Config {
    test_config_builder().build().expect("valid config")
}

/// Find an available port by binding to :0.
fn free_port() -> u16 {
    // Tests run on many threads; the naive bind(:0)-drop-rebind pattern
    // races (the kernel can hand the same port to two tests before either
    // rebinds), which shows up as AddrInUse launch failures or clients
    // connecting to another test's server. A process-global claimed set
    // makes each handed-out port unique within the test binary.
    use std::sync::Mutex;
    static CLAIMED: Mutex<Option<std::collections::HashSet<u16>>> = Mutex::new(None);
    loop {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let mut guard = CLAIMED.lock().unwrap();
        if guard.get_or_insert_with(Default::default).insert(port) {
            return port;
        }
    }
}

fn wait_for_server(addr: &str) {
    for _ in 0..200 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server did not start on {addr}");
}

fn echo_round_trip(addr: &str, msg: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(msg).unwrap();
    stream.flush().unwrap();

    let mut buf = vec![0u8; msg.len()];
    let mut total = 0;
    while total < msg.len() {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }
    buf.truncate(total);
    buf
}

// ── Tests ───────────────────────────────────────────────────────────

#[test]
fn coalesced_sends_preserve_order() {
    // Many small sends queued on one connection drain through the coalesced
    // sendmsg path; verify they arrive in order, byte-for-byte, none dropped.
    let config = test_config_builder()
        .send_pool(512, 16384)
        .build()
        .expect("valid config");
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<BurstSender>()
        .expect("launch failed");
    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let total = BURST_N as usize * BURST_MSG;
    let mut buf = vec![0u8; total];
    let mut read = 0;
    while read < total {
        match stream.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(n) => read += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }
    assert_eq!(read, total, "received {read} of {total} bytes");
    for i in 0..BURST_N {
        let off = i as usize * BURST_MSG;
        let expected = (i % 251) as u8;
        for (j, &b) in buf[off..off + BURST_MSG].iter().enumerate() {
            assert_eq!(
                b, expected,
                "message {i} byte {j}: got {b}, expected {expected} (reordering or corruption)"
            );
        }
    }

    drop(stream);
    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn recv_forward_echo_preserves_order_across_buffers() {
    // Drive the zero-copy recv-forward path with many distinct messages, each
    // larger than one provided recv buffer (so a message spans buffers and
    // multiple buffers are held), and verify the echo is byte-for-byte in order.
    let config = test_config_builder()
        .recv_buffer(256, 4096)
        .sq_entries(256)
        .build()
        .expect("valid config");
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<RecvForwardEcho>()
        .expect("launch failed");
    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    const N: usize = 20;
    const MSG: usize = 16384;
    let mut payload = Vec::with_capacity(N * MSG);
    for i in 0..N {
        payload.extend(std::iter::repeat_n((i % 251) as u8, MSG));
    }
    stream.write_all(&payload).unwrap();
    stream.flush().unwrap();

    let total = N * MSG;
    let mut buf = vec![0u8; total];
    let mut read = 0;
    while read < total {
        match stream.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(n) => read += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }
    assert_eq!(read, total, "received {read} of {total} bytes");
    assert!(
        buf == payload,
        "echoed bytes differ (reordering or corruption)"
    );

    drop(stream);
    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn shutdown_handle_reports_bound_addr() {
    // Bind to :0 so the kernel picks a port, and verify ShutdownHandle
    // surfaces the resolved address (not the wildcard the user passed in).
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    let bound = shutdown
        .bound_addr()
        .expect("bound_addr should be Some after a TCP bind");
    assert_eq!(bound.ip().to_string(), "127.0.0.1");
    assert_ne!(bound.port(), 0, "kernel-assigned port must be non-zero");

    // Sanity: the reported port actually accepts connections.
    wait_for_server(&bound.to_string());

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn echo_small_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let msg = b"Hello, ringline!";
    let response = echo_round_trip(&addr, msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn echo_large_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // 8KB message — larger than typical TCP segment
    let msg: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
    let response = echo_round_trip(&addr, &msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn echo_multiple_connections() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut join_handles = Vec::new();
    for i in 0..4 {
        let addr = addr.clone();
        join_handles.push(std::thread::spawn(move || {
            let msg = format!("connection {i}");
            let response = echo_round_trip(&addr, msg.as_bytes());
            assert_eq!(response, msg.as_bytes());
        }));
    }
    for h in join_handles {
        h.join().unwrap();
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn echo_sequential_sends() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    for i in 0..10 {
        let msg = format!("msg-{i}\n");
        stream.write_all(msg.as_bytes()).unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0u8; msg.len()];
        let mut total = 0;
        while total < msg.len() {
            match stream.read(&mut buf[total..]) {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => panic!("read error: {e}"),
            }
        }
        assert_eq!(&buf[..total], msg.as_bytes(), "mismatch on send {i}");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn async_echo_small_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let msg = b"Hello, async ringline!";
    let response = echo_round_trip(&addr, msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn async_echo_large_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let msg: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
    let response = echo_round_trip(&addr, &msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn async_echo_multiple_connections() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut join_handles = Vec::new();
    for i in 0..4 {
        let addr = addr.clone();
        join_handles.push(std::thread::spawn(move || {
            let msg = format!("async conn {i}");
            let response = echo_round_trip(&addr, msg.as_bytes());
            assert_eq!(response, msg.as_bytes());
        }));
    }
    for h in join_handles {
        h.join().unwrap();
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn connection_close_on_client_disconnect() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Open and immediately close 10 connections.
    for _ in 0..10 {
        let stream = TcpStream::connect(&addr).unwrap();
        drop(stream);
    }

    // Give the server time to process the closes.
    std::thread::sleep(Duration::from_millis(200));

    // Verify the server is still alive by connecting again.
    let msg = b"still alive";
    let response = echo_round_trip(&addr, msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn graceful_shutdown() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Open a connection, send data, verify echo.
    let msg = b"pre-shutdown";
    let response = echo_round_trip(&addr, msg);
    assert_eq!(response, msg);

    // Trigger shutdown.
    shutdown.shutdown();

    // Workers should exit cleanly.
    for h in handles {
        let result = h.join().expect("worker panicked");
        result.expect("worker returned error");
    }
}

// ── Shutdown-write test ─────────────────────────────────────────────

/// Handler that echoes back data then half-closes the write side.
struct ShutdownWriteEcho;

impl AsyncEventHandler for ShutdownWriteEcho {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| {
                    let _ = conn.send_nowait(data);
                    ParseResult::Consumed(data.len())
                })
                .await;
            if n > 0 {
                conn.shutdown_write();
            }
            // Keep the task alive to receive more (should get EOF).
            let _ = conn.with_data(|_data| ParseResult::Consumed(0)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        ShutdownWriteEcho
    }
}

#[test]
fn async_shutdown_write_triggers_eof() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<ShutdownWriteEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Send data.
    let msg = b"shutdown test";
    stream.write_all(msg).unwrap();
    stream.flush().unwrap();

    // Read the echo.
    let mut buf = vec![0u8; msg.len()];
    let mut total = 0;
    while total < msg.len() {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }
    assert_eq!(&buf[..total], msg);

    // After echo, server does shutdown_write — we should get EOF.
    let mut extra = [0u8; 1];
    match stream.read(&mut extra) {
        Ok(0) => {} // EOF — correct!
        Ok(_) => panic!("expected EOF after shutdown_write"),
        Err(e) => panic!("unexpected error: {e}"),
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Request-shutdown test ───────────────────────────────────────────

/// Handler that shuts down the worker after receiving any data.
struct RequestShutdownHandler;

impl AsyncEventHandler for RequestShutdownHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            conn.with_data(|data| {
                // Echo back, then request shutdown.
                let _ = conn.send_nowait(data);
                conn.request_shutdown();
                ParseResult::Consumed(data.len())
            })
            .await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        RequestShutdownHandler
    }
}

#[test]
fn async_request_shutdown_exits_cleanly() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<RequestShutdownHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Send a message — the handler will request shutdown after echoing.
    // The response may arrive or the connection may reset (race between
    // the queued send and shutdown closing the socket), so tolerate both.
    {
        let mut stream = TcpStream::connect(&addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let _ = stream.write_all(b"trigger-shutdown");
        let _ = stream.flush();
        let mut buf = [0u8; 64];
        let _ = stream.read(&mut buf);
    }

    // Workers should exit on their own (request_shutdown triggers it).
    for h in handles {
        let result = h.join().expect("worker panicked");
        result.expect("worker returned error");
    }

    // ShutdownHandle is now redundant, but drop it cleanly.
    drop(shutdown);
}

// ── Spawn standalone task test ──────────────────────────────────────

static SPAWN_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Handler that spawns a standalone task from on_accept.
struct SpawnTestHandler;

impl AsyncEventHandler for SpawnTestHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            // Spawn a standalone task that increments the counter.
            ringline::spawn(async {
                SPAWN_COUNTER.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();

            // Echo one message to signal readiness.
            conn.with_data(|data| {
                let _ = conn.send_nowait(data);
                ParseResult::Consumed(data.len())
            })
            .await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SpawnTestHandler
    }
}

#[test]
fn async_spawn_standalone_task() {
    // Reset counter.
    SPAWN_COUNTER.store(0, Ordering::SeqCst);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<SpawnTestHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Connect 3 times — each accept spawns a standalone task.
    for _ in 0..3 {
        echo_round_trip(&addr, b"spawn-test");
    }

    // Give standalone tasks time to run.
    std::thread::sleep(Duration::from_millis(100));

    // Verify the standalone tasks ran.
    let count = SPAWN_COUNTER.load(Ordering::SeqCst);
    assert!(count >= 3, "expected at least 3, got {count}");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Sleep test ──────────────────────────────────────────────────────

/// Handler that sleeps before echoing back.
struct SleepEchoHandler;

impl AsyncEventHandler for SleepEchoHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            loop {
                let n = conn
                    .with_data(|data| {
                        let len = data.len();
                        // Sleep 50ms then echo.
                        let data_copy = data.to_vec();
                        let conn2 = conn;
                        ringline::spawn(async move {
                            ringline::sleep(Duration::from_millis(50)).await;
                            let _ = conn2.send_nowait(&data_copy);
                        })
                        .unwrap();
                        ParseResult::Consumed(len)
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SleepEchoHandler
    }
}

#[test]
fn async_sleep_completes() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<SleepEchoHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let start = std::time::Instant::now();
    let response = echo_round_trip(&addr, b"hello sleep");
    let elapsed = start.elapsed();

    assert_eq!(response, b"hello sleep");
    // Should take at least ~50ms due to sleep.
    assert!(
        elapsed >= Duration::from_millis(30),
        "elapsed only {elapsed:?}, expected at least 30ms"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Timeout test ────────────────────────────────────────────────────

/// Handler that tests timeout — a fast operation should succeed,
/// then the handler echoes a response indicating success.
struct TimeoutTestHandler;

impl AsyncEventHandler for TimeoutTestHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            conn.with_data(|data| -> ParseResult {
                let msg = std::str::from_utf8(data).unwrap_or("");
                if msg == "test-timeout-ok" {
                    // Timeout wrapping an immediate future should succeed.
                    let conn2 = conn;
                    ringline::spawn(async move {
                        let result =
                            ringline::timeout(Duration::from_secs(10), async { 42u32 }).await;
                        match result {
                            Ok(42) => {
                                let _ = conn2.send_nowait(b"OK");
                            }
                            _ => {
                                let _ = conn2.send_nowait(b"FAIL");
                            }
                        }
                    })
                    .unwrap();
                } else if msg == "test-timeout-expire" {
                    // Timeout wrapping a long sleep should expire.
                    let conn2 = conn;
                    ringline::spawn(async move {
                        let result = ringline::timeout(
                            Duration::from_millis(20),
                            ringline::sleep(Duration::from_secs(10)),
                        )
                        .await;
                        match result {
                            Err(_elapsed) => {
                                let _ = conn2.send_nowait(b"ELAPSED");
                            }
                            Ok(()) => {
                                let _ = conn2.send_nowait(b"FAIL");
                            }
                        }
                    })
                    .unwrap();
                }
                ParseResult::Consumed(data.len())
            })
            .await;
            // Keep the task alive so the spawned tasks can send.
            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        TimeoutTestHandler
    }
}

#[test]
fn async_timeout_ok() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<TimeoutTestHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Test: timeout wrapping an immediate future should return Ok.
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"test-timeout-ok").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 16];
    let mut total = 0;
    while total < 2 {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read error: {e}"),
        }
    }
    assert_eq!(&buf[..total], b"OK");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn async_timeout_expires() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<TimeoutTestHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Test: timeout wrapping a long sleep should return Err(Elapsed).
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"test-timeout-expire").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 16];
    let mut total = 0;
    while total < 7 {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read error: {e}"),
        }
    }
    assert_eq!(&buf[..total], b"ELAPSED");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Cross-connection I/O tests ──────────────────────────────────────

/// Forwarder handler: on accept, connects to a backend, forwards data
/// through it (echo), and sends the response back to the client.
/// This exercises the owner_task wakeup chain: client task at index N
/// owns backend connection at index M, and with_data/send on the backend
/// connection must correctly wake the client task.
struct ForwarderHandler {
    backend_addr: SocketAddr,
}

use std::net::SocketAddr;

static FORWARDER_BACKEND_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();

impl AsyncEventHandler for ForwarderHandler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        let backend_addr = self.backend_addr;
        async move {
            // Connect to the backend echo server.
            let backend = match client.connect(backend_addr) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        let _ = client.send_nowait(format!("-ERR connect: {e}\r\n").as_bytes());
                        return;
                    }
                },
                Err(e) => {
                    let _ = client.send_nowait(format!("-ERR connect: {e}\r\n").as_bytes());
                    return;
                }
            };

            // Forward loop: read from client, send to backend, read echo, send back.
            loop {
                let mut data_copy = Vec::new();
                let n = client
                    .with_data(|data| {
                        data_copy = data.to_vec();
                        ParseResult::Consumed(data.len())
                    })
                    .await;
                if n == 0 {
                    break;
                }

                // Forward to backend.
                if backend.send_nowait(&data_copy).is_err() {
                    break;
                }

                // Read echo from backend.
                let mut echo = Vec::new();
                let target_len = data_copy.len();
                while echo.len() < target_len {
                    let remaining = target_len - echo.len();
                    let got = backend
                        .with_data(|data| {
                            let take = data.len().min(remaining);
                            echo.extend_from_slice(&data[..take]);
                            ParseResult::Consumed(take)
                        })
                        .await;
                    if got == 0 {
                        break;
                    }
                }

                // Send back to client.
                if client.send_nowait(&echo).is_err() {
                    break;
                }
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        let addr = *FORWARDER_BACKEND_ADDR.get().expect("backend addr not set");
        ForwarderHandler { backend_addr: addr }
    }
}

#[test]
fn async_outbound_connect_and_echo() {
    // 1. Start a backend echo server.
    let backend_port = free_port();
    let backend_addr = format!("127.0.0.1:{backend_port}");

    let (backend_shutdown, backend_handles) = RinglineBuilder::new(test_config())
        .bind(backend_addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("backend launch failed");

    wait_for_server(&backend_addr);

    // 2. Start the forwarder server.
    FORWARDER_BACKEND_ADDR
        .set(backend_addr.parse().unwrap())
        .ok();

    let forwarder_port = free_port();
    let forwarder_addr = format!("127.0.0.1:{forwarder_port}");

    let (fwd_shutdown, fwd_handles) = RinglineBuilder::new(test_config())
        .bind(forwarder_addr.parse().unwrap())
        .launch::<ForwarderHandler>()
        .expect("forwarder launch failed");

    wait_for_server(&forwarder_addr);

    // 3. Connect to the forwarder, send data, verify echo.
    let msg = b"cross-connection echo test!";
    let response = echo_round_trip(&forwarder_addr, msg);
    assert_eq!(response, msg, "forwarder did not echo correctly");

    // Larger message.
    let large_msg: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
    let response = echo_round_trip(&forwarder_addr, &large_msg);
    assert_eq!(response, large_msg, "forwarder did not echo large message");

    fwd_shutdown.shutdown();
    for h in fwd_handles {
        h.join().unwrap().unwrap();
    }

    backend_shutdown.shutdown();
    for h in backend_handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that tries to connect to a non-listening address.
struct ConnectRefusedHandler;

static CONNECT_REFUSED_PORT: AtomicU32 = AtomicU32::new(0);

impl AsyncEventHandler for ConnectRefusedHandler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            // Wait for trigger byte from client before connecting.
            client
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;

            let port = CONNECT_REFUSED_PORT.load(Ordering::SeqCst);
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            let result = match client.connect(addr) {
                Ok(fut) => match fut.await {
                    Ok(_) => "CONNECTED".to_string(),
                    Err(e) => format!("ERR:{}", e.kind()),
                },
                Err(e) => format!("SUBMIT_ERR:{e}"),
            };

            let _ = client.send_nowait(result.as_bytes());
            // Keep connection alive so the send completes.
            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        ConnectRefusedHandler
    }
}

#[test]
fn async_outbound_connect_refused() {
    // Bind to a port, then drop the listener so nothing is listening.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_port = listener.local_addr().unwrap().port();
    drop(listener);

    CONNECT_REFUSED_PORT.store(dead_port as u32, Ordering::SeqCst);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<ConnectRefusedHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    // Trigger the handler.
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 128];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                // Check if we have a complete response.
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.starts_with("ERR:")
                    || s.starts_with("CONNECTED")
                    || s.starts_with("SUBMIT_ERR:")
                {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let response = std::str::from_utf8(&buf[..total]).unwrap();
    assert!(
        response.starts_with("ERR:"),
        "expected connect error, got: {response}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that opens multiple outbound connections from a single task.
struct MultiOutboundHandler;

static MULTI_OUTBOUND_BACKEND_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();

impl AsyncEventHandler for MultiOutboundHandler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        let backend_addr = *MULTI_OUTBOUND_BACKEND_ADDR
            .get()
            .expect("backend addr not set");
        async move {
            // Open two backend connections from the same client task.
            let backend1 = match client.connect(backend_addr) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        let _ = client.send_nowait(format!("ERR1:{e}").as_bytes());
                        return;
                    }
                },
                Err(e) => {
                    let _ = client.send_nowait(format!("ERR1:{e}").as_bytes());
                    return;
                }
            };

            let backend2 = match client.connect(backend_addr) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        let _ = client.send_nowait(format!("ERR2:{e}").as_bytes());
                        return;
                    }
                },
                Err(e) => {
                    let _ = client.send_nowait(format!("ERR2:{e}").as_bytes());
                    return;
                }
            };

            // Send "AA" through backend1, "BB" through backend2.
            if backend1.send_nowait(b"AA").is_err() {
                let _ = client.send_nowait(b"SEND_ERR1");
                return;
            }
            let mut echo1 = Vec::new();
            while echo1.len() < 2 {
                let remaining = 2 - echo1.len();
                let got = backend1
                    .with_data(|data| {
                        let take = data.len().min(remaining);
                        echo1.extend_from_slice(&data[..take]);
                        ParseResult::Consumed(take)
                    })
                    .await;
                if got == 0 {
                    break;
                }
            }

            if backend2.send_nowait(b"BB").is_err() {
                let _ = client.send_nowait(b"SEND_ERR2");
                return;
            }
            let mut echo2 = Vec::new();
            while echo2.len() < 2 {
                let remaining = 2 - echo2.len();
                let got = backend2
                    .with_data(|data| {
                        let take = data.len().min(remaining);
                        echo2.extend_from_slice(&data[..take]);
                        ParseResult::Consumed(take)
                    })
                    .await;
                if got == 0 {
                    break;
                }
            }

            // Combine and send back.
            let mut result = echo1;
            result.extend_from_slice(&echo2);
            let _ = client.send_nowait(&result);
            // Keep connection alive so the send completes.
            ringline::sleep(Duration::from_secs(5)).await;
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        MultiOutboundHandler
    }
}

#[test]
fn async_multiple_outbound_from_one_task() {
    // Start backend echo server.
    let backend_port = free_port();
    let backend_addr = format!("127.0.0.1:{backend_port}");

    let (backend_shutdown, backend_handles) = RinglineBuilder::new(test_config())
        .bind(backend_addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("backend launch failed");

    wait_for_server(&backend_addr);

    MULTI_OUTBOUND_BACKEND_ADDR
        .set(backend_addr.parse().unwrap())
        .ok();

    // Start the multi-outbound server.
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<MultiOutboundHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Connect and trigger the handler.
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    // The handler fires on accept; we just need to read the result.
    // But the handler awaits with_data from client first — send a trigger byte.
    // Actually, looking at the handler, it connects on accept, no trigger needed.
    // But it does need to use with_data — wait, no it doesn't. Let me re-check...
    // The handler connects immediately on accept and doesn't read from client first.

    let mut buf = [0u8; 64];
    let mut total = 0;
    while total < 4 {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert_eq!(result, "AABB", "expected AABB, got: {result}");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }

    backend_shutdown.shutdown();
    for h in backend_handles {
        h.join().unwrap().unwrap();
    }
}

// ── Select tests ────────────────────────────────────────────────────

/// Handler that uses select to monitor two backend connections.
/// Connects to two backend echo servers, sends data to one, and uses
/// select to determine which responds first.
struct SelectTwoHandler;

static SELECT_BACKEND1_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();
static SELECT_BACKEND2_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();

impl AsyncEventHandler for SelectTwoHandler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        let addr1 = *SELECT_BACKEND1_ADDR.get().expect("backend1 addr not set");
        let addr2 = *SELECT_BACKEND2_ADDR.get().expect("backend2 addr not set");
        async move {
            // Connect to both backends.
            let backend1 = match client.connect(addr1) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        let _ = client.send_nowait(format!("ERR1:{e}").as_bytes());
                        return;
                    }
                },
                Err(e) => {
                    let _ = client.send_nowait(format!("ERR1:{e}").as_bytes());
                    return;
                }
            };
            let backend2 = match client.connect(addr2) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        let _ = client.send_nowait(format!("ERR2:{e}").as_bytes());
                        return;
                    }
                },
                Err(e) => {
                    let _ = client.send_nowait(format!("ERR2:{e}").as_bytes());
                    return;
                }
            };

            // Send data to backend1 only.
            if backend1.send_nowait(b"HELLO").is_err() {
                let _ = client.send_nowait(b"SEND_ERR");
                return;
            }

            // Select on both — backend1 should win since we sent data there.
            // Use separate buffers since each closure needs its own &mut.
            let mut buf1 = Vec::new();
            let mut buf2 = Vec::new();
            match ringline::select(
                backend1.with_data(|data| {
                    buf1.extend_from_slice(data);
                    ParseResult::Consumed(data.len())
                }),
                backend2.with_data(|data| {
                    buf2.extend_from_slice(data);
                    ParseResult::Consumed(data.len())
                }),
            )
            .await
            {
                ringline::Either::Left(_) => {
                    let _ = client.send_nowait(b"LEFT:");
                    let _ = client.send_nowait(&buf1);
                }
                ringline::Either::Right(_) => {
                    let _ = client.send_nowait(b"RIGHT:");
                    let _ = client.send_nowait(&buf2);
                }
            }

            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SelectTwoHandler
    }
}

#[test]
fn async_select_two_connections() {
    // Start two backend echo servers.
    let backend1_port = free_port();
    let backend1_addr = format!("127.0.0.1:{backend1_port}");
    let (b1_shutdown, b1_handles) = RinglineBuilder::new(test_config())
        .bind(backend1_addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("backend1 launch failed");
    wait_for_server(&backend1_addr);

    let backend2_port = free_port();
    let backend2_addr = format!("127.0.0.1:{backend2_port}");
    let (b2_shutdown, b2_handles) = RinglineBuilder::new(test_config())
        .bind(backend2_addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("backend2 launch failed");
    wait_for_server(&backend2_addr);

    SELECT_BACKEND1_ADDR
        .set(backend1_addr.parse().unwrap())
        .ok();
    SELECT_BACKEND2_ADDR
        .set(backend2_addr.parse().unwrap())
        .ok();

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<SelectTwoHandler>()
        .expect("launch failed");
    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut buf = [0u8; 64];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.starts_with("LEFT:") || s.starts_with("RIGHT:") || s.starts_with("ERR") {
                    // Wait for the full response.
                    if s.len() >= 10 || s.starts_with("ERR") {
                        break;
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert!(
        result.starts_with("LEFT:HELLO"),
        "expected LEFT:HELLO, got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    b1_shutdown.shutdown();
    for h in b1_handles {
        h.join().unwrap().unwrap();
    }
    b2_shutdown.shutdown();
    for h in b2_handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that uses select to show the second branch can win.
/// Sends data to backend2 (not backend1), so Right should win.
struct SelectSecondWinsHandler;

static SELECT2_BACKEND1_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();
static SELECT2_BACKEND2_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();

impl AsyncEventHandler for SelectSecondWinsHandler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        let addr1 = *SELECT2_BACKEND1_ADDR.get().expect("backend1 addr not set");
        let addr2 = *SELECT2_BACKEND2_ADDR.get().expect("backend2 addr not set");
        async move {
            let backend1 = match client.connect(addr1) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        let _ = client.send_nowait(format!("ERR:{e}").as_bytes());
                        return;
                    }
                },
                Err(e) => {
                    let _ = client.send_nowait(format!("ERR:{e}").as_bytes());
                    return;
                }
            };
            let backend2 = match client.connect(addr2) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        let _ = client.send_nowait(format!("ERR:{e}").as_bytes());
                        return;
                    }
                },
                Err(e) => {
                    let _ = client.send_nowait(format!("ERR:{e}").as_bytes());
                    return;
                }
            };

            // Send data to backend2 only.
            if backend2.send_nowait(b"WORLD").is_err() {
                let _ = client.send_nowait(b"SEND_ERR");
                return;
            }

            let mut buf1 = Vec::new();
            let mut buf2 = Vec::new();
            match ringline::select(
                backend1.with_data(|data| {
                    buf1.extend_from_slice(data);
                    ParseResult::Consumed(data.len())
                }),
                backend2.with_data(|data| {
                    buf2.extend_from_slice(data);
                    ParseResult::Consumed(data.len())
                }),
            )
            .await
            {
                ringline::Either::Left(_) => {
                    let _ = client.send_nowait(b"LEFT:");
                    let _ = client.send_nowait(&buf1);
                }
                ringline::Either::Right(_) => {
                    let _ = client.send_nowait(b"RIGHT:");
                    let _ = client.send_nowait(&buf2);
                }
            }

            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SelectSecondWinsHandler
    }
}

#[test]
fn async_select_second_wins() {
    let b1_port = free_port();
    let b1_addr = format!("127.0.0.1:{b1_port}");
    let (b1_shutdown, b1_handles) = RinglineBuilder::new(test_config())
        .bind(b1_addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("backend1 launch failed");
    wait_for_server(&b1_addr);

    let b2_port = free_port();
    let b2_addr = format!("127.0.0.1:{b2_port}");
    let (b2_shutdown, b2_handles) = RinglineBuilder::new(test_config())
        .bind(b2_addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("backend2 launch failed");
    wait_for_server(&b2_addr);

    SELECT2_BACKEND1_ADDR.set(b1_addr.parse().unwrap()).ok();
    SELECT2_BACKEND2_ADDR.set(b2_addr.parse().unwrap()).ok();

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<SelectSecondWinsHandler>()
        .expect("launch failed");
    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut buf = [0u8; 64];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if (s.starts_with("LEFT:") || s.starts_with("RIGHT:")) && s.len() >= 11 {
                    break;
                }
                if s.starts_with("ERR") || s.starts_with("SEND_ERR") {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert!(
        result.starts_with("RIGHT:WORLD"),
        "expected RIGHT:WORLD, got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    b1_shutdown.shutdown();
    for h in b1_handles {
        h.join().unwrap().unwrap();
    }
    b2_shutdown.shutdown();
    for h in b2_handles {
        h.join().unwrap().unwrap();
    }
}

// ── Select with sleep test (timer slot leak check) ──────────────────

/// Handler that uses select(with_data, sleep) as a manual timeout.
/// Runs many iterations to confirm no timer slot leaks from dropped SleepFutures.
struct SelectSleepHandler;

impl AsyncEventHandler for SelectSleepHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            // Run 300 iterations of select(with_data, sleep).
            // Each iteration where data arrives drops the SleepFuture,
            // which must correctly cancel the io_uring timeout and release
            // the timer slot. If slots leak, we'll exhaust the pool and panic.
            for _ in 0..300 {
                match ringline::select(
                    conn.with_data(|data| {
                        let _ = conn.send_nowait(data);
                        ParseResult::Consumed(data.len())
                    }),
                    ringline::sleep(Duration::from_secs(60)),
                )
                .await
                {
                    ringline::Either::Left(0) => break,
                    ringline::Either::Left(_) => {} // got data, sleep was dropped
                    ringline::Either::Right(()) => {
                        // Timeout — shouldn't happen with 60s timeout.
                        let _ = conn.send_nowait(b"TIMEOUT");
                        break;
                    }
                }
            }
            let _ = conn.send_nowait(b"DONE");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SelectSleepHandler
    }
}

#[test]
fn async_select_with_sleep() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    // Use a config with limited timer slots to make leaks detectable.
    let config = test_config_builder()
        .timer_slots(16)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<SelectSleepHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Send 300 messages — each triggers a select(with_data, sleep) where
    // data wins and the SleepFuture is dropped.
    for i in 0..300 {
        let msg = format!("msg-{i}\n");
        stream.write_all(msg.as_bytes()).unwrap();
        stream.flush().unwrap();

        // Read the echo.
        let mut buf = vec![0u8; msg.len()];
        let mut total = 0;
        while total < msg.len() {
            match stream.read(&mut buf[total..]) {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => panic!("read error on iteration {i}: {e}"),
            }
        }
        assert_eq!(
            &buf[..total],
            msg.as_bytes(),
            "echo mismatch on iteration {i}"
        );
    }

    // Close the connection — handler should send "DONE".
    drop(stream);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── select3 test ────────────────────────────────────────────────────

/// Handler that uses select3 with two data sources + sleep.
struct Select3Handler;

static SELECT3_BACKEND_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();

impl AsyncEventHandler for Select3Handler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        let backend_addr = *SELECT3_BACKEND_ADDR.get().expect("backend addr not set");
        async move {
            let backend = match client.connect(backend_addr) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        let _ = client.send_nowait(format!("ERR:{e}").as_bytes());
                        return;
                    }
                },
                Err(e) => {
                    let _ = client.send_nowait(format!("ERR:{e}").as_bytes());
                    return;
                }
            };

            // Send data to backend so it echoes.
            if backend.send_nowait(b"ECHO3").is_err() {
                let _ = client.send_nowait(b"SEND_ERR");
                return;
            }

            // select3: client data (none sent), backend echo, long sleep.
            // Backend should win since we sent data there.
            let mut client_buf = Vec::new();
            let mut backend_buf = Vec::new();
            match ringline::select3(
                client.with_data(|data| {
                    client_buf.extend_from_slice(data);
                    ParseResult::Consumed(data.len())
                }),
                backend.with_data(|data| {
                    backend_buf.extend_from_slice(data);
                    ParseResult::Consumed(data.len())
                }),
                ringline::sleep(Duration::from_secs(60)),
            )
            .await
            {
                ringline::Either3::First(_) => {
                    let _ = client.send_nowait(b"FIRST");
                }
                ringline::Either3::Second(_) => {
                    let _ = client.send_nowait(b"SECOND:");
                    let _ = client.send_nowait(&backend_buf);
                }
                ringline::Either3::Third(()) => {
                    let _ = client.send_nowait(b"THIRD");
                }
            }

            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        Select3Handler
    }
}

#[test]
fn async_select3_basic() {
    let b_port = free_port();
    let b_addr = format!("127.0.0.1:{b_port}");
    let (b_shutdown, b_handles) = RinglineBuilder::new(test_config())
        .bind(b_addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("backend launch failed");
    wait_for_server(&b_addr);

    SELECT3_BACKEND_ADDR.set(b_addr.parse().unwrap()).ok();

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<Select3Handler>()
        .expect("launch failed");
    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut buf = [0u8; 64];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.starts_with("SECOND:") && s.len() >= 12 {
                    break;
                }
                if s.starts_with("FIRST") || s.starts_with("THIRD") || s.starts_with("ERR") {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert!(
        result.starts_with("SECOND:ECHO3"),
        "expected SECOND:ECHO3, got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    b_shutdown.shutdown();
    for h in b_handles {
        h.join().unwrap().unwrap();
    }
}

// ── spawn / cancel tests ────────────────────────────────────────

/// Handler that tests spawn exhaustion.
struct TrySpawnHandler;

impl AsyncEventHandler for TrySpawnHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return; // probe connection from wait_for_server
            }

            // First spawn should succeed.
            let result1 = ringline::spawn(async {
                ringline::sleep(Duration::from_secs(60)).await;
            });

            // Slab capacity is 1, so second spawn should fail.
            let result2 = ringline::spawn(async {
                ringline::sleep(Duration::from_secs(60)).await;
            });

            match (result1, result2) {
                (Ok(task_id), Err(_)) => {
                    let _ = conn.send_nowait(b"OK");
                    // Clean up: cancel the first task.
                    task_id.cancel();
                }
                (Ok(_), Ok(_)) => {
                    let _ = conn.send_nowait(b"BOTH_OK");
                }
                _ => {
                    let _ = conn.send_nowait(b"FAIL");
                }
            }

            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        TrySpawnHandler
    }
}

#[test]
fn async_spawn_exhaustion() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .standalone_task_capacity(1)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<TrySpawnHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 32];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s == "OK" || s == "BOTH_OK" || s == "FAIL" {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert_eq!(result, "OK", "expected OK, got: {result}");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that tests cancelling a running task.
struct CancelTaskHandler;

impl AsyncEventHandler for CancelTaskHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return; // probe connection from wait_for_server
            }

            // Spawn a long-running task.
            let task_id = ringline::spawn(async {
                ringline::sleep(Duration::from_secs(60)).await;
            })
            .unwrap();

            // Cancel it immediately.
            task_id.cancel();

            // The slot should be free — spawn a replacement.
            let result = ringline::spawn(async {
                // Quick task — completes immediately.
            });

            match result {
                Ok(_) => {
                    let _ = conn.send_nowait(b"OK");
                }
                Err(_) => {
                    let _ = conn.send_nowait(b"FAIL");
                }
            }

            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        CancelTaskHandler
    }
}

#[test]
fn async_cancel_running_task() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .standalone_task_capacity(1)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<CancelTaskHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 32];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s == "OK" || s == "FAIL" {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert_eq!(
        result, "OK",
        "expected OK (slot freed after cancel), got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that tests cancelling an already-completed task (should be a no-op).
struct CancelCompletedHandler;

impl AsyncEventHandler for CancelCompletedHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return; // probe connection from wait_for_server
            }

            // Spawn a task that completes immediately.
            let task_id = ringline::spawn(async {}).unwrap();

            // Give it a chance to complete.
            ringline::sleep(Duration::from_millis(50)).await;

            // Cancel after completion — should not panic.
            task_id.cancel();

            let _ = conn.send_nowait(b"OK");
            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        CancelCompletedHandler
    }
}

#[test]
fn async_cancel_completed_task() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<CancelCompletedHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 32];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s == "OK" {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert_eq!(
        result, "OK",
        "expected OK (cancel completed task is no-op), got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Multi-worker tests ──────────────────────────────────────────────

fn multi_worker_config(threads: usize) -> Config {
    test_config_builder()
        .workers(threads)
        .build()
        .expect("valid config")
}

#[test]
fn multi_worker_echo() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(multi_worker_config(2))
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Connect 4 clients sequentially — acceptor round-robins across 2 workers.
    for i in 0..4 {
        let msg = format!("multi-worker-{i}");
        let response = echo_round_trip(&addr, msg.as_bytes());
        assert_eq!(response, msg.as_bytes(), "mismatch on connection {i}");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn multi_worker_async_echo() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(multi_worker_config(2))
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Connect 4 clients sequentially — acceptor round-robins across 2 workers.
    for i in 0..4 {
        let msg = format!("multi-worker-async-{i}");
        let response = echo_round_trip(&addr, msg.as_bytes());
        assert_eq!(response, msg.as_bytes(), "mismatch on connection {i}");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn multi_worker_graceful_shutdown() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(multi_worker_config(4))
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Open a few connections to exercise the workers.
    for i in 0..4 {
        let msg = format!("shutdown-{i}");
        let response = echo_round_trip(&addr, msg.as_bytes());
        assert_eq!(response, msg.as_bytes());
    }

    // Trigger shutdown — all 4 worker threads must join cleanly.
    shutdown.shutdown();
    for (i, h) in handles.into_iter().enumerate() {
        let result = h.join().unwrap_or_else(|_| panic!("worker {i} panicked"));
        result.unwrap_or_else(|e| panic!("worker {i} returned error: {e}"));
    }
}

// ── Awaitable send tests ────────────────────────────────────────────

/// Handler that tests send_await: sends a known payload via send_await
/// and reports the byte count from the SendFuture.
struct SendAwaitHandler;

impl AsyncEventHandler for SendAwaitHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let payload = b"SEND_AWAIT_OK";
            match conn.send(payload) {
                Ok(fut) => match fut.await {
                    Ok(bytes) => {
                        let msg = format!("OK:{bytes}");
                        let _ = conn.send_nowait(msg.as_bytes());
                    }
                    Err(e) => {
                        let _ = conn.send_nowait(format!("ERR:{e}").as_bytes());
                    }
                },
                Err(e) => {
                    let _ = conn.send_nowait(format!("SUBMIT_ERR:{e}").as_bytes());
                }
            }
            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SendAwaitHandler
    }
}

#[test]
fn async_send_await_basic() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<SendAwaitHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    // Trigger the handler.
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    // Read "SEND_AWAIT_OK" followed by "OK:13".
    let mut buf = [0u8; 128];
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.contains("OK:") && s.len() >= 16 {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert!(
        result.starts_with("SEND_AWAIT_OK"),
        "expected SEND_AWAIT_OK prefix, got: {result}"
    );
    assert!(
        result.contains("OK:13"),
        "expected OK:13 (send_await byte count), got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[cfg(has_io_uring)]
/// Handler that tests send_chain_await.
struct SendChainAwaitHandler;

#[cfg(has_io_uring)]
impl AsyncEventHandler for SendChainAwaitHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            // Build a chained send with two copy parts and await completion.
            let part1 = b"HELLO";
            let part2 = b"WORLD";
            match conn.send_chain(|b| b.copy(part1).copy(part2).finish()) {
                Ok(fut) => match fut.await {
                    Ok(bytes) => {
                        let msg = format!("OK:{bytes}");
                        let _ = conn.send_nowait(msg.as_bytes());
                    }
                    Err(e) => {
                        let _ = conn.send_nowait(format!("ERR:{e}").as_bytes());
                    }
                },
                Err(e) => {
                    let _ = conn.send_nowait(format!("SUBMIT_ERR:{e}").as_bytes());
                }
            }
            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SendChainAwaitHandler
    }
}

#[test]
#[cfg(has_io_uring)]
fn async_send_chain_await_basic() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<SendChainAwaitHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    // Read "HELLOWORLD" followed by "OK:<bytes>".
    let mut buf = [0u8; 128];
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.contains("OK:") && s.len() >= 13 {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert!(
        result.starts_with("HELLOWORLD"),
        "expected HELLOWORLD prefix, got: {result}"
    );
    assert!(
        result.contains("OK:10"),
        "expected OK:10 (5+5 bytes chain send), got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── try_sleep / try_timeout exhaustion tests ────────────────────────

/// Handler that tests try_sleep exhaustion.
struct TrySleepHandler;

impl AsyncEventHandler for TrySleepHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let exhausted = {
                // Allocate all timer slots (config has timer_slots = 2).
                let _s1 = ringline::try_sleep(Duration::from_secs(60));
                let _s2 = ringline::try_sleep(Duration::from_secs(60));

                // Third attempt should fail with TimerExhausted.
                ringline::try_sleep(Duration::from_secs(60)).is_err()
                // _s1, _s2 dropped here — slots released.
            };

            if exhausted {
                let _ = conn.send_nowait(b"EXHAUSTED");
            } else {
                let _ = conn.send_nowait(b"NOT_EXHAUSTED");
            }

            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        TrySleepHandler
    }
}

#[test]
fn async_try_sleep_exhaustion() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .timer_slots(2)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<TrySleepHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 32];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s == "EXHAUSTED" || s == "NOT_EXHAUSTED" {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert_eq!(
        result, "EXHAUSTED",
        "expected EXHAUSTED (timer pool full), got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that tests try_timeout exhaustion.
struct TryTimeoutHandler;

impl AsyncEventHandler for TryTimeoutHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let exhausted = {
                // Allocate all timer slots (config has timer_slots = 2).
                let _t1 =
                    ringline::try_timeout(Duration::from_secs(60), std::future::pending::<()>());
                let _t2 =
                    ringline::try_timeout(Duration::from_secs(60), std::future::pending::<()>());

                // Third attempt should fail with TimerExhausted.
                ringline::try_timeout(Duration::from_secs(60), std::future::pending::<()>())
                    .is_err()
                // _t1, _t2 dropped here — timer slots released.
            };

            if exhausted {
                let _ = conn.send_nowait(b"EXHAUSTED");
            } else {
                let _ = conn.send_nowait(b"NOT_EXHAUSTED");
            }

            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        TryTimeoutHandler
    }
}

#[test]
fn async_try_timeout_exhaustion() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .timer_slots(2)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<TryTimeoutHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 32];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s == "EXHAUSTED" || s == "NOT_EXHAUSTED" {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert_eq!(
        result, "EXHAUSTED",
        "expected EXHAUSTED (timer pool full), got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════
// Phase 5 tests: join, absolute timers, UDP
// ═══════════════════════════════════════════════════════════════════

// ── join / join3 ──────────────────────────────────────────────────

/// Handler that joins two send_await calls and reports byte counts.
struct JoinHandler;

impl AsyncEventHandler for JoinHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            // Join two send calls.
            let fut_a = async {
                match conn.send(b"HELLO") {
                    Ok(f) => f.await,
                    Err(e) => Err(e),
                }
            };
            let fut_b = async {
                match conn.send(b"WORLD") {
                    Ok(f) => f.await,
                    Err(e) => Err(e),
                }
            };
            let (a, b) = ringline::join(fut_a, fut_b).await;
            let msg = format!("JOIN:{}:{}", a.unwrap_or(0), b.unwrap_or(0));
            let _ = conn.send_nowait(msg.as_bytes());

            // Wait for send to drain before closing.
            ringline::sleep(Duration::from_millis(20)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        JoinHandler
    }
}

#[test]
fn async_join_basic() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<JoinHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 128];
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.contains("JOIN:") {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    // Both sends should report 5 bytes each: "HELLO" and "WORLD".
    assert!(
        result.contains("JOIN:5:5"),
        "expected JOIN:5:5, got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that joins three futures: send_await + sleep + with_data.
struct Join3Handler;

impl AsyncEventHandler for Join3Handler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let fut_a = async {
                match conn.send(b"ABC") {
                    Ok(f) => f.await.unwrap_or(0),
                    Err(_) => 0,
                }
            };
            let fut_b = async {
                ringline::sleep(Duration::from_millis(20)).await;
                42u32
            };
            let fut_c = async {
                // This will wait for new data from the client.
                let n = conn
                    .with_data(|data| ParseResult::Consumed(data.len()))
                    .await;
                n as u32
            };

            let (a, b, c) = ringline::join3(fut_a, fut_b, fut_c).await;
            let msg = format!("JOIN3:{a}:{b}:{c}");
            let _ = conn.send_nowait(msg.as_bytes());

            ringline::sleep(Duration::from_millis(20)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        Join3Handler
    }
}

#[test]
fn async_join3_mixed() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<Join3Handler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // First write triggers the handler (consumed by initial with_data).
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    // Brief delay, then send second payload for the join3 with_data branch.
    std::thread::sleep(Duration::from_millis(30));
    stream.write_all(b"PAYLOAD").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 128];
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.contains("JOIN3:") {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    // a=3 (send "ABC"), b=42 (sleep completed), c=7 (with_data received "PAYLOAD")
    // Note: "ABC" may appear before JOIN3 in the output since it's a real send.
    assert!(
        result.contains("JOIN3:3:42:7"),
        "expected JOIN3:3:42:7, got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Absolute timers ───────────────────────────────────────────────

/// Handler that uses sleep_until with a deadline.
struct SleepUntilHandler;

impl AsyncEventHandler for SleepUntilHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let before = std::time::Instant::now();
            let deadline = ringline::Deadline::after(Duration::from_millis(50));
            ringline::sleep_until(deadline).await;
            let elapsed = before.elapsed();

            let msg = if elapsed >= Duration::from_millis(30) {
                "SLEEP_UNTIL_OK"
            } else {
                "SLEEP_UNTIL_TOO_FAST"
            };
            let _ = conn.send_nowait(msg.as_bytes());
            ringline::sleep(Duration::from_millis(20)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SleepUntilHandler
    }
}

#[test]
fn async_sleep_until_basic() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<SleepUntilHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 64];
    let mut total = 0;
    let deadline_t = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline_t {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.contains("SLEEP_UNTIL") {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert_eq!(result, "SLEEP_UNTIL_OK", "got: {result}");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that uses timeout_at with a short deadline around a long sleep.
struct TimeoutAtHandler;

impl AsyncEventHandler for TimeoutAtHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let deadline = ringline::Deadline::after(Duration::from_millis(20));
            let result =
                ringline::timeout_at(deadline, ringline::sleep(Duration::from_secs(10))).await;

            let msg = match result {
                Err(_elapsed) => "TIMEOUT_AT_EXPIRED",
                Ok(()) => "TIMEOUT_AT_NOT_EXPIRED",
            };
            let _ = conn.send_nowait(msg.as_bytes());
            ringline::sleep(Duration::from_millis(20)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        TimeoutAtHandler
    }
}

#[test]
fn async_timeout_at_expires() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<TimeoutAtHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 64];
    let mut total = 0;
    let deadline_t = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline_t {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.contains("TIMEOUT_AT") {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert_eq!(result, "TIMEOUT_AT_EXPIRED", "got: {result}");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── UDP ───────────────────────────────────────────────────────────

/// Async handler that echoes UDP datagrams via UdpCtx.
struct UdpEchoAsync;

impl AsyncEventHandler for UdpEchoAsync {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            loop {
                let n = conn
                    .with_data(|data| ParseResult::Consumed(data.len()))
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn on_udp_bind(
        &self,
        udp: ringline::UdpCtx,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async move {
            loop {
                let (data, peer) = udp.recv_from().await;
                let _ = udp.send_to(peer, &data);
            }
        }))
    }
    fn create_for_worker(_id: usize) -> Self {
        UdpEchoAsync
    }
}

#[test]
fn async_udp_echo() {
    let udp_port = free_port();
    let udp_addr: std::net::SocketAddr = format!("127.0.0.1:{udp_port}").parse().unwrap();

    let tcp_port = free_port();
    let tcp_addr = format!("127.0.0.1:{tcp_port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(tcp_addr.parse().unwrap())
        .bind_udp(udp_addr)
        .launch::<UdpEchoAsync>()
        .expect("launch failed");

    wait_for_server(&tcp_addr);
    std::thread::sleep(Duration::from_millis(50));

    let client = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let msg = b"ASYNC_UDP_ECHO";
    client.send_to(msg, udp_addr).unwrap();

    let mut buf = [0u8; 64];
    let (n, _peer) = client.recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..n], msg, "async UDP echo mismatch");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════
// Free connect() + on_start() tests
// ═══════════════════════════════════════════════════════════════════

// ── Standalone task using free connect() ─────────────────────────

/// Handler where on_accept spawns a standalone task that uses the free
/// ringline::connect() (not ConnCtx::connect) to reach a backend echo server.
struct StandaloneConnectHandler;

static STANDALONE_CONNECT_BACKEND: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();

impl AsyncEventHandler for StandaloneConnectHandler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        let backend_addr = *STANDALONE_CONNECT_BACKEND
            .get()
            .expect("backend addr not set");
        async move {
            // Wait for trigger from client.
            let n = client
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            // Spawn a standalone task that connects to the backend.
            // ConnCtx is Copy — standalone tasks can use it for send().
            ringline::spawn(async move {
                let backend = match ringline::connect(backend_addr) {
                    Ok(fut) => match fut.await {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            let _ = client.send_nowait(format!("CONNECT_ERR:{e}").as_bytes());
                            return;
                        }
                    },
                    Err(e) => {
                        let _ = client.send_nowait(format!("SUBMIT_ERR:{e}").as_bytes());
                        return;
                    }
                };

                // Send data to backend, read echo.
                if backend.send_nowait(b"STANDALONE").is_err() {
                    return;
                }

                let mut echo = Vec::new();
                while echo.len() < 10 {
                    let remaining = 10 - echo.len();
                    let got = backend
                        .with_data(|data| {
                            let take = data.len().min(remaining);
                            echo.extend_from_slice(&data[..take]);
                            ParseResult::Consumed(take)
                        })
                        .await;
                    if got == 0 {
                        break;
                    }
                }

                // Report to client.
                let _ = client.send_nowait(&echo);
            })
            .unwrap();

            // Keep connection alive so standalone task can send.
            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        StandaloneConnectHandler
    }
}

#[test]
fn async_standalone_connect() {
    // Start backend echo server.
    let backend_port = free_port();
    let backend_addr = format!("127.0.0.1:{backend_port}");
    let (b_shutdown, b_handles) = RinglineBuilder::new(test_config())
        .bind(backend_addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("backend launch failed");
    wait_for_server(&backend_addr);

    STANDALONE_CONNECT_BACKEND
        .set(backend_addr.parse().unwrap())
        .ok();

    // Start the handler server.
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<StandaloneConnectHandler>()
        .expect("launch failed");
    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 64];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                if total >= 10 {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let result = std::str::from_utf8(&buf[..total]).unwrap();
    assert_eq!(
        result, "STANDALONE",
        "expected STANDALONE echo, got: {result}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    b_shutdown.shutdown();
    for h in b_handles {
        h.join().unwrap().unwrap();
    }
}

// ── Server-speaks-first greeting ─────────────────────────────────

/// Server that sends a greeting immediately on accept (MySQL/SMTP style),
/// before the client sends anything.
struct GreetingServer;

impl AsyncEventHandler for GreetingServer {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let _ = conn.send_nowait(b"WELCOME!");
            // Keep the connection open until the peer disconnects.
            loop {
                let n = conn
                    .with_data(|data| ParseResult::Consumed(data.len()))
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        GreetingServer
    }
}

static GREETING_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();
static GREETING_RESULT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Client that connects and expects the server's greeting to arrive intact.
/// Regression: on the mio backend, greeting bytes delivered in the same
/// event batch as the connect-writable event were read into the accumulator
/// and then destroyed by the connect-success accumulator reset — and
/// edge-triggered mio never re-delivered them, so the client stalled.
struct GreetingClientHandler;

impl AsyncEventHandler for GreetingClientHandler {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let addr = *GREETING_ADDR.get().expect("greeting addr not set");
        Some(Box::pin(async move {
            let conn = match ringline::connect(addr) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        GREETING_RESULT.set(format!("CONNECT_ERR:{e}")).ok();
                        ringline::request_shutdown().ok();
                        return;
                    }
                },
                Err(e) => {
                    GREETING_RESULT.set(format!("SUBMIT_ERR:{e}")).ok();
                    ringline::request_shutdown().ok();
                    return;
                }
            };

            let mut greeting = Vec::new();
            while greeting.len() < 8 {
                let remaining = 8 - greeting.len();
                let got = conn
                    .with_data(|data| {
                        let take = data.len().min(remaining);
                        greeting.extend_from_slice(&data[..take]);
                        ParseResult::Consumed(take)
                    })
                    .await;
                if got == 0 {
                    break;
                }
            }
            GREETING_RESULT
                .set(String::from_utf8_lossy(&greeting).to_string())
                .ok();
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        GreetingClientHandler
    }
}

#[test]
fn async_server_speaks_first_greeting() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (s_shutdown, s_handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<GreetingServer>()
        .expect("server launch failed");
    wait_for_server(&addr);

    GREETING_ADDR.set(addr.parse().unwrap()).ok();

    let (_c_shutdown, c_handles) = RinglineBuilder::new(test_config())
        .launch::<GreetingClientHandler>()
        .expect("client launch failed");
    for h in c_handles {
        h.join().unwrap().unwrap();
    }

    let result = GREETING_RESULT.get().expect("client did not set result");
    assert_eq!(result, "WELCOME!", "greeting lost or corrupted: {result}");

    s_shutdown.shutdown();
    for h in s_handles {
        h.join().unwrap().unwrap();
    }
}

// ── Client-only mode via on_start() ─────────────────────────────

/// Handler that uses on_start() for client-only mode: connects to a
/// backend, sends data, reads echo, then shuts down.
struct OnStartClientHandler;

static ON_START_BACKEND_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();
static ON_START_RESULT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

impl AsyncEventHandler for OnStartClientHandler {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        // No inbound connections expected in client-only mode.
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let backend_addr = *ON_START_BACKEND_ADDR.get().expect("backend addr not set");
        Some(Box::pin(async move {
            let backend = match ringline::connect(backend_addr) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        ON_START_RESULT.set(format!("CONNECT_ERR:{e}")).ok();
                        ringline::request_shutdown().ok();
                        return;
                    }
                },
                Err(e) => {
                    ON_START_RESULT.set(format!("SUBMIT_ERR:{e}")).ok();
                    ringline::request_shutdown().ok();
                    return;
                }
            };

            if backend.send_nowait(b"ON_START").is_err() {
                ON_START_RESULT.set("SEND_ERR".to_string()).ok();
                ringline::request_shutdown().ok();
                return;
            }

            let mut echo = Vec::new();
            while echo.len() < 8 {
                let remaining = 8 - echo.len();
                let got = backend
                    .with_data(|data| {
                        let take = data.len().min(remaining);
                        echo.extend_from_slice(&data[..take]);
                        ParseResult::Consumed(take)
                    })
                    .await;
                if got == 0 {
                    break;
                }
            }

            ON_START_RESULT
                .set(String::from_utf8_lossy(&echo).to_string())
                .ok();
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        OnStartClientHandler
    }
}

#[test]
fn async_on_start_client_only() {
    // Start backend echo server.
    let backend_port = free_port();
    let backend_addr = format!("127.0.0.1:{backend_port}");
    let (b_shutdown, b_handles) = RinglineBuilder::new(test_config())
        .bind(backend_addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("backend launch failed");
    wait_for_server(&backend_addr);

    ON_START_BACKEND_ADDR
        .set(backend_addr.parse().unwrap())
        .ok();

    // Launch client-only (no .bind()).
    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<OnStartClientHandler>()
        .expect("launch failed");

    // Wait for the on_start task to complete and shut down the worker.
    for h in handles {
        h.join().unwrap().unwrap();
    }

    let result = ON_START_RESULT.get().expect("on_start did not set result");
    assert_eq!(result, "ON_START", "expected ON_START echo, got: {result}");

    b_shutdown.shutdown();
    for h in b_handles {
        h.join().unwrap().unwrap();
    }
}

// ── Free connect() to dead port returns error ────────────────────

/// Handler where on_accept spawns a standalone task that tries to
/// connect to a dead port via ringline::connect().
struct StandaloneConnectRefusedHandler;

static STANDALONE_REFUSED_PORT: AtomicU32 = AtomicU32::new(0);

impl AsyncEventHandler for StandaloneConnectRefusedHandler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = client
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let port = STANDALONE_REFUSED_PORT.load(Ordering::SeqCst);
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            ringline::spawn(async move {
                let result = match ringline::connect(addr) {
                    Ok(fut) => match fut.await {
                        Ok(_) => "CONNECTED".to_string(),
                        Err(e) => format!("ERR:{}", e.kind()),
                    },
                    Err(e) => format!("SUBMIT_ERR:{e}"),
                };

                let _ = client.send_nowait(result.as_bytes());
            })
            .unwrap();

            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        StandaloneConnectRefusedHandler
    }
}

#[test]
fn async_standalone_connect_refused() {
    // Bind to a port then drop it so nothing is listening.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_port = listener.local_addr().unwrap().port();
    drop(listener);

    STANDALONE_REFUSED_PORT.store(dead_port as u32, Ordering::SeqCst);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<StandaloneConnectRefusedHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 128];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if s.starts_with("ERR:")
                    || s.starts_with("CONNECTED")
                    || s.starts_with("SUBMIT_ERR:")
                {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }

    let response = std::str::from_utf8(&buf[..total]).unwrap();
    assert!(
        response.starts_with("ERR:"),
        "expected connect error, got: {response}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Peer close delivers EOF to accepted connection ──────────────────

/// Server that accepts a connection, reads one message, echoes it, then
/// waits for the client to disconnect. Verifies with_data returns 0 (EOF).
struct PeerCloseHandler;

static PEER_CLOSE_RESULT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

impl AsyncEventHandler for PeerCloseHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            // Read until EOF. Each chunk is echoed back.
            loop {
                let n = conn
                    .with_data(|data| {
                        let _ = conn.send_nowait(data);
                        ParseResult::Consumed(data.len())
                    })
                    .await;
                if n == 0 {
                    // EOF — peer closed the connection. This is the success case.
                    PEER_CLOSE_RESULT.set("OK".to_string()).ok();
                    return;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        PeerCloseHandler
    }
}

#[test]
fn async_peer_close_delivers_eof() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<PeerCloseHandler>()
        .expect("launch failed");
    wait_for_server(&addr);

    // Connect with std TCP, send data, read echo, then close.
    {
        let mut stream = TcpStream::connect(&addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(b"hello").unwrap();
        stream.flush().unwrap();

        let mut buf = [0u8; 5];
        let mut total = 0;
        while total < 5 {
            match stream.read(&mut buf[total..]) {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("read error: {e}"),
            }
        }
        assert_eq!(&buf[..total], b"hello");
        // stream drops here, closing the TCP connection
    }

    // Give the server time to process the close.
    std::thread::sleep(Duration::from_millis(200));

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }

    let result = PEER_CLOSE_RESULT.get().expect("handler did not set result");
    assert_eq!(result, "OK", "expected EOF after peer close, got: {result}");
}

// ── Send pool exhaustion ────────────────────────────────────────────

#[cfg(has_io_uring)]
/// Handler that fires many send_nowait calls to exhaust the send pool.
struct PoolExhaustionHandler;

#[cfg(has_io_uring)]
static POOL_EXHAUSTION_RESULT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

#[cfg(has_io_uring)]
impl AsyncEventHandler for PoolExhaustionHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            // Read one message to know the client is connected.
            conn.with_data(|data| ParseResult::Consumed(data.len()))
                .await;

            // Fire many send_nowait calls rapidly. With a tiny pool, this
            // should eventually return Err (pool exhausted).
            let mut got_error = false;
            let payload = [0xABu8; 512];
            for _ in 0..1000 {
                if let Err(_e) = conn.send_nowait(&payload) {
                    got_error = true;
                    break;
                }
            }

            if got_error {
                POOL_EXHAUSTION_RESULT.set("OK".to_string()).ok();
            } else {
                POOL_EXHAUSTION_RESULT.set("NO_ERROR".to_string()).ok();
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        PoolExhaustionHandler
    }
}

#[cfg(has_io_uring)]
#[test]
fn async_send_pool_exhaustion() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        // Very small send pool to trigger exhaustion quickly.
        .send_pool(4, 16384)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<PoolExhaustionHandler>()
        .expect("launch failed");
    wait_for_server(&addr);

    // Connect and send a trigger message. Don't read — let the server's
    // sends queue up and exhaust the pool.
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"go").unwrap();
    stream.flush().unwrap();

    // Wait for the handler to complete.
    std::thread::sleep(Duration::from_millis(500));

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }

    let result = POOL_EXHAUSTION_RESULT
        .get()
        .expect("handler did not set result");
    assert_eq!(
        result, "OK",
        "expected pool exhaustion error, got: {result}"
    );
}

// ── Scatter-gather send_parts test ──────────────────────────────────

#[cfg(has_io_uring)]
/// Handler that uses send_parts with multiple copy segments.
struct SendPartsHandler;

#[cfg(has_io_uring)]
impl AsyncEventHandler for SendPartsHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            // Build a scatter-gather send with 3 copy parts.
            let part1 = b"SCATTER";
            let part2 = b"-";
            let part3 = b"GATHER";
            match conn
                .send_parts()
                .build(|b| b.copy(part1).copy(part2).copy(part3).submit())
            {
                Ok(()) => {}
                Err(e) => {
                    let _ = conn.send_nowait(format!("ERR:{e}").as_bytes());
                }
            }
            ringline::sleep(Duration::from_secs(5)).await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SendPartsHandler
    }
}

#[cfg(has_io_uring)]
#[test]
fn async_send_parts_scatter_gather() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<SendPartsHandler>()
        .expect("launch failed");
    wait_for_server(&addr);

    // Send trigger, then read the scatter-gather response.
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"go").unwrap();
    stream.flush().unwrap();

    let expected = b"SCATTER-GATHER";
    let mut buf = vec![0u8; expected.len()];
    let mut total = 0;
    while total < expected.len() {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read error: {e}"),
        }
    }
    assert_eq!(
        &buf[..total],
        expected,
        "expected scatter-gather response, got: {}",
        String::from_utf8_lossy(&buf[..total])
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Outbound connect EOF delivery ───────────────────────────────────

static OUTBOUND_EOF_ADDR: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();
static OUTBOUND_EOF_RESULT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Client that connects outbound to a std TCP server, sends data, reads
/// echo, then waits for EOF. Exercises the outbound plaintext recv_mode fix.
struct OutboundEofClient;

impl AsyncEventHandler for OutboundEofClient {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let server_addr = *OUTBOUND_EOF_ADDR.get().expect("addr not set");
        Some(Box::pin(async move {
            let conn = match ringline::connect(server_addr) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        OUTBOUND_EOF_RESULT.set(format!("CONNECT_ERR:{e}")).ok();
                        ringline::request_shutdown().ok();
                        return;
                    }
                },
                Err(e) => {
                    OUTBOUND_EOF_RESULT.set(format!("SUBMIT_ERR:{e}")).ok();
                    ringline::request_shutdown().ok();
                    return;
                }
            };

            // Send data and read echo, with a timeout.
            let _ = conn.send_nowait(b"hello");
            let mut echoed = Vec::new();
            let echo_fut = conn.with_data(|data| {
                echoed.extend_from_slice(data);
                ParseResult::Consumed(data.len())
            });
            let n = match ringline::timeout(Duration::from_secs(5), echo_fut).await {
                Ok(n) => n,
                Err(_) => {
                    OUTBOUND_EOF_RESULT.set("ECHO_TIMEOUT".to_string()).ok();
                    ringline::request_shutdown().ok();
                    return;
                }
            };
            if n == 0 || echoed != b"hello" {
                OUTBOUND_EOF_RESULT
                    .set(format!(
                        "ECHO_FAIL:n={n},data={}",
                        String::from_utf8_lossy(&echoed)
                    ))
                    .ok();
                ringline::request_shutdown().ok();
                return;
            }

            // Now wait for EOF — the std server thread closes after echoing.
            let eof_fut = conn.with_data(|data| ParseResult::Consumed(data.len()));
            match ringline::timeout(Duration::from_secs(5), eof_fut).await {
                Ok(0) => {
                    OUTBOUND_EOF_RESULT.set("OK".to_string()).ok();
                }
                Ok(n) => {
                    OUTBOUND_EOF_RESULT.set(format!("UNEXPECTED:{n}")).ok();
                }
                Err(_) => {
                    OUTBOUND_EOF_RESULT.set("TIMEOUT".to_string()).ok();
                }
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        OutboundEofClient
    }
}

#[test]
fn async_outbound_connect_receives_eof() {
    let port = free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // Start a simple std TCP echo-once server in a thread.
    let listener = std::net::TcpListener::bind(addr).unwrap();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).unwrap();
        stream.write_all(&buf[..n]).unwrap();
        stream.flush().unwrap();
        // Close — this sends FIN to the client.
        drop(stream);
    });

    OUTBOUND_EOF_ADDR.set(addr).ok();

    let (_c_shutdown, c_handles) = RinglineBuilder::new(test_config())
        .launch::<OutboundEofClient>()
        .expect("client launch failed");

    for h in c_handles {
        h.join().unwrap().unwrap();
    }

    server_thread.join().unwrap();

    let result = OUTBOUND_EOF_RESULT
        .get()
        .expect("on_start did not set result");
    assert_eq!(
        result, "OK",
        "expected EOF on outbound connect, got: {result}"
    );
}

// ── Buffer ring exhaustion stress test ──────────────────────────────

#[cfg(has_io_uring)]
#[test]
fn buffer_ring_exhaustion_recovers() {
    // Use a tiny buffer ring (4 buffers) to force ENOBUFS under
    // concurrent connection load, then verify all data echoes correctly.
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .recv_buffer(4, 4096)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");
    wait_for_server(&addr);

    // Open 8 connections simultaneously and send data on all of them.
    let mut threads = Vec::new();
    for i in 0..8 {
        let addr = addr.clone();
        threads.push(std::thread::spawn(move || {
            let mut stream = TcpStream::connect(&addr).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();

            // Send a message identifying this connection.
            let msg = format!("connection-{i}-payload");
            stream.write_all(msg.as_bytes()).unwrap();
            stream.flush().unwrap();

            // Read back the echo.
            let mut buf = vec![0u8; msg.len()];
            let mut total = 0;
            while total < msg.len() {
                match stream.read(&mut buf[total..]) {
                    Ok(0) => break,
                    Ok(n) => total += n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => panic!("conn {i} read error: {e}"),
                }
            }
            assert_eq!(&buf[..total], msg.as_bytes(), "conn {i} echo mismatch");
        }));
    }

    for t in threads {
        t.join().expect("connection thread panicked");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Connect timeout test ────────────────────────────────────────────

static TIMEOUT_RESULT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

struct ConnectTimeoutClient;

impl AsyncEventHandler for ConnectTimeoutClient {
    fn on_accept(&self, _conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn std::future::Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            // Connect to a black-hole address with a 50ms timeout.
            // 192.0.2.1 is TEST-NET-1 (RFC 5737) — routable but unreachable.
            let addr: SocketAddr = "192.0.2.1:12345".parse().unwrap();
            match ringline::connect_with_timeout(addr, 50) {
                Ok(fut) => match fut.await {
                    Ok(_) => {
                        TIMEOUT_RESULT.set("UNEXPECTED_OK".into()).ok();
                    }
                    Err(e) => {
                        if e.kind() == io::ErrorKind::TimedOut {
                            TIMEOUT_RESULT.set("TIMED_OUT".into()).ok();
                        } else {
                            TIMEOUT_RESULT.set(format!("ERR:{e}")).ok();
                        }
                    }
                },
                Err(e) => {
                    TIMEOUT_RESULT.set(format!("SUBMIT_ERR:{e}")).ok();
                }
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        ConnectTimeoutClient
    }
}

#[test]
fn async_connect_timeout_fires() {
    let (_c_shutdown, c_handles) = RinglineBuilder::new(test_config())
        .launch::<ConnectTimeoutClient>()
        .expect("client launch failed");

    for h in c_handles {
        h.join().unwrap().unwrap();
    }

    let result = TIMEOUT_RESULT.get().expect("on_start did not set result");
    // Accept either TimedOut or a connection error (some networks reject
    // immediately instead of black-holing).
    assert!(
        result == "TIMED_OUT" || result.starts_with("ERR:"),
        "expected timeout or connection error, got: {result}"
    );
}

// ── spawn_with_handle / JoinHandle tests ────────────────────────────

/// Send a trigger byte and read the full response (up to 256 bytes).
fn trigger_and_read(addr: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"x").unwrap();
    stream.flush().unwrap();

    let mut buf = vec![0u8; 256];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }
    buf.truncate(total);
    buf
}

static JOIN_RESULT: AtomicU32 = AtomicU32::new(0);

/// Handler that uses spawn_with_handle to await a spawned task's result.
struct JoinHandleHandler;

impl AsyncEventHandler for JoinHandleHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            // Spawn a task that computes a value after a short sleep.
            let handle = ringline::spawn_with_handle(async {
                ringline::sleep(Duration::from_millis(10)).await;
                99u32
            })
            .unwrap();

            let value = handle.await;
            JOIN_RESULT.store(value, Ordering::SeqCst);
            let _ = conn.send_nowait(b"done");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        JoinHandleHandler
    }
}

#[test]
fn spawn_with_handle_awaits_result() {
    JOIN_RESULT.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<JoinHandleHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"done");
    assert_eq!(JOIN_RESULT.load(Ordering::SeqCst), 99);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that spawns a task returning a value synchronously (no .await).
struct ImmediateJoinHandler;

static IMMEDIATE_RESULT: AtomicU32 = AtomicU32::new(0);

impl AsyncEventHandler for ImmediateJoinHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            // Task completes synchronously on first poll — result should
            // be available immediately when JoinHandle is next polled.
            let handle = ringline::spawn_with_handle(async { 42u32 }).unwrap();
            // Yield once so the child gets polled.
            ringline::sleep(Duration::from_millis(1)).await;
            let value = handle.await;
            IMMEDIATE_RESULT.store(value, Ordering::SeqCst);
            let _ = conn.send_nowait(b"ok");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        ImmediateJoinHandler
    }
}

#[test]
fn spawn_with_handle_immediate_completion() {
    IMMEDIATE_RESULT.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<ImmediateJoinHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"ok");
    assert_eq!(IMMEDIATE_RESULT.load(Ordering::SeqCst), 42);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that drops the JoinHandle without awaiting — task should still run.
struct DetachHandler;

static DETACH_RAN: AtomicU32 = AtomicU32::new(0);

impl AsyncEventHandler for DetachHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            {
                let _handle = ringline::spawn_with_handle(async {
                    DETACH_RAN.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap();
                // _handle dropped here without await
            }

            // Give the detached task a tick to run.
            ringline::sleep(Duration::from_millis(20)).await;
            let _ = conn.send_nowait(b"ok");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        DetachHandler
    }
}

#[test]
fn spawn_with_handle_detach_on_drop() {
    DETACH_RAN.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<DetachHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"ok");
    assert!(DETACH_RAN.load(Ordering::SeqCst) >= 1);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that spawns a long-sleeping task and aborts it.
struct AbortHandler;

impl AsyncEventHandler for AbortHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let handle = ringline::spawn_with_handle(async {
                ringline::sleep(Duration::from_secs(60)).await;
                42u32
            })
            .unwrap();

            handle.abort();

            // Verify we can spawn another task (slot was freed).
            let ok = ringline::spawn(async {}).is_ok();
            let _ = conn.send_nowait(if ok { b"ok" as &[u8] } else { b"fail" });
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        AbortHandler
    }
}

#[test]
fn spawn_with_handle_abort() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AbortHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"ok");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Handler that spawns multiple tasks and awaits all of them.
struct MultiJoinHandler;

static MULTI_SUM: AtomicU32 = AtomicU32::new(0);

impl AsyncEventHandler for MultiJoinHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let h1 = ringline::spawn_with_handle(async { 10u32 }).unwrap();
            let h2 = ringline::spawn_with_handle(async { 20u32 }).unwrap();
            let h3 = ringline::spawn_with_handle(async { 30u32 }).unwrap();

            let (a, b) = ringline::join(h1, h2).await;
            let c = h3.await;
            MULTI_SUM.store(a + b + c, Ordering::SeqCst);
            let _ = conn.send_nowait(b"ok");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        MultiJoinHandler
    }
}

#[test]
fn spawn_with_handle_multiple_join() {
    MULTI_SUM.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<MultiJoinHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"ok");
    assert_eq!(MULTI_SUM.load(Ordering::SeqCst), 60);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── oneshot channel tests ───────────────────────────────────────────

static ONESHOT_RESULT: AtomicU32 = AtomicU32::new(0);

/// Spawn a task that sends a value on a oneshot, await it from the connection task.
struct OneshotHandler;

impl AsyncEventHandler for OneshotHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let (tx, rx) = ringline::oneshot::channel::<u32>();
            ringline::spawn(async move {
                ringline::sleep(Duration::from_millis(10)).await;
                let _ = tx.send(77);
            })
            .unwrap();

            match rx.await {
                Ok(val) => ONESHOT_RESULT.store(val, Ordering::SeqCst),
                Err(_) => ONESHOT_RESULT.store(999, Ordering::SeqCst),
            }
            let _ = conn.send_nowait(b"done");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        OneshotHandler
    }
}

#[test]
fn oneshot_channel_async_wakeup() {
    ONESHOT_RESULT.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<OneshotHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"done");
    assert_eq!(ONESHOT_RESULT.load(Ordering::SeqCst), 77);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Sender dropped without sending — receiver gets RecvError.
static ONESHOT_CLOSED: AtomicU32 = AtomicU32::new(0);

struct OneshotClosedHandler;

impl AsyncEventHandler for OneshotClosedHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let (tx, rx) = ringline::oneshot::channel::<u32>();
            ringline::spawn(async move {
                drop(tx); // Drop without sending.
            })
            .unwrap();

            // Give the spawned task a tick to run.
            ringline::sleep(Duration::from_millis(10)).await;
            match rx.await {
                Ok(_) => ONESHOT_CLOSED.store(0, Ordering::SeqCst),
                Err(_) => ONESHOT_CLOSED.store(1, Ordering::SeqCst),
            }
            let _ = conn.send_nowait(b"done");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        OneshotClosedHandler
    }
}

#[test]
fn oneshot_channel_sender_dropped() {
    ONESHOT_CLOSED.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<OneshotClosedHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"done");
    assert_eq!(ONESHOT_CLOSED.load(Ordering::SeqCst), 1);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── mpsc channel tests ──────────────────────────────────────────────

static MPSC_SUM: AtomicU32 = AtomicU32::new(0);

/// Multiple senders, single receiver via mpsc.
struct MpscHandler;

impl AsyncEventHandler for MpscHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let (tx, rx) = ringline::mpsc::channel::<u32>(16);

            // Spawn 3 senders.
            for i in 0..3 {
                let tx = tx.clone();
                ringline::spawn(async move {
                    ringline::sleep(Duration::from_millis(5)).await;
                    let _ = tx.try_send(10 * (i + 1));
                })
                .unwrap();
            }
            // Drop original sender so only the clones remain.
            drop(tx);

            // Receive until channel closes.
            let mut sum = 0u32;
            while let Some(val) = rx.recv().await {
                sum += val;
            }
            MPSC_SUM.store(sum, Ordering::SeqCst);
            let _ = conn.send_nowait(b"done");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        MpscHandler
    }
}

#[test]
fn mpsc_channel_multiple_senders() {
    MPSC_SUM.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<MpscHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"done");
    // 10 + 20 + 30 = 60
    assert_eq!(MPSC_SUM.load(Ordering::SeqCst), 60);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// async send with backpressure — channel capacity 1, multiple sends.
static MPSC_BACKPRESSURE: AtomicU32 = AtomicU32::new(0);

struct MpscBackpressureHandler;

impl AsyncEventHandler for MpscBackpressureHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let (tx, rx) = ringline::mpsc::channel::<u32>(1);

            // Sender task: send 5 values through a capacity-1 channel.
            ringline::spawn(async move {
                for i in 1..=5 {
                    tx.send(i).await.unwrap();
                }
            })
            .unwrap();

            // Receiver: drain all values.
            let mut sum = 0u32;
            while let Some(val) = rx.recv().await {
                sum += val;
            }
            MPSC_BACKPRESSURE.store(sum, Ordering::SeqCst);
            let _ = conn.send_nowait(b"done");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        MpscBackpressureHandler
    }
}

#[test]
fn mpsc_channel_backpressure() {
    MPSC_BACKPRESSURE.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<MpscBackpressureHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"done");
    // 1 + 2 + 3 + 4 + 5 = 15
    assert_eq!(MPSC_BACKPRESSURE.load(Ordering::SeqCst), 15);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── signal handling tests ───────────────────────────────────────────

/// wait_on_signal shuts down workers when SIGTERM is sent to self.
#[test]
fn signal_wait_on_signal_shutdown() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Verify server is running.
    let got = echo_round_trip(&addr, b"hi");
    assert_eq!(got, b"hi");

    // Send SIGTERM to self from a background thread after a short delay.
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(100));
        unsafe {
            libc::kill(libc::getpid(), libc::SIGTERM);
        }
    });

    let sig = shutdown.wait_on_signal();
    assert_eq!(sig, ringline::Signal::Terminate);

    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── DNS resolver tests ──────────────────────────────────────────────

static RESOLVE_RESULT: AtomicU32 = AtomicU32::new(0);

/// Handler that resolves "localhost" and verifies the result.
struct ResolveHandler;

impl AsyncEventHandler for ResolveHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            match ringline::resolve("localhost", 80) {
                Ok(fut) => match fut.await {
                    Ok(addr) => {
                        if addr.ip().is_loopback() && addr.port() == 80 {
                            RESOLVE_RESULT.store(1, Ordering::SeqCst);
                        }
                        let _ = conn.send_nowait(b"ok");
                    }
                    Err(_) => {
                        let _ = conn.send_nowait(b"err");
                    }
                },
                Err(_) => {
                    let _ = conn.send_nowait(b"no-resolver");
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        ResolveHandler
    }
}

#[test]
fn resolve_localhost() {
    RESOLVE_RESULT.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<ResolveHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"ok");
    assert_eq!(RESOLVE_RESULT.load(Ordering::SeqCst), 1);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Resolve an invalid hostname — should return an error.
static RESOLVE_ERR: AtomicU32 = AtomicU32::new(0);

struct ResolveErrorHandler;

impl AsyncEventHandler for ResolveErrorHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            match ringline::resolve("nonexistent.invalid", 80) {
                Ok(fut) => match fut.await {
                    Ok(_) => {
                        let _ = conn.send_nowait(b"unexpected-ok");
                    }
                    Err(_) => {
                        RESOLVE_ERR.store(1, Ordering::SeqCst);
                        let _ = conn.send_nowait(b"ok");
                    }
                },
                Err(_) => {
                    let _ = conn.send_nowait(b"no-resolver");
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        ResolveErrorHandler
    }
}

#[test]
fn resolve_invalid_hostname() {
    // GitHub Actions macOS runners resolve `.invalid` hostnames: their
    // resolver returns an address for an RFC 6761 name that must never
    // resolve, so `resolve("nonexistent.invalid")` succeeds and the handler
    // replies "unexpected-ok" instead of taking the error path this test
    // asserts on. Skip only in that specific environment — the test still
    // runs on Linux CI (where `.invalid` correctly fails) and on local
    // macOS (normal resolvers return NXDOMAIN).
    if cfg!(target_os = "macos") && std::env::var_os("GITHUB_ACTIONS").is_some() {
        eprintln!(
            "skipping resolve_invalid_hostname: GitHub Actions macOS DNS \
             resolves .invalid hostnames"
        );
        return;
    }

    RESOLVE_ERR.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<ResolveErrorHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"ok");
    assert_eq!(RESOLVE_ERR.load(Ordering::SeqCst), 1);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Resolver disabled (0 threads) — resolve() returns error.
struct ResolveDisabledHandler;

impl AsyncEventHandler for ResolveDisabledHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            match ringline::resolve("localhost", 80) {
                Ok(_) => {
                    let _ = conn.send_nowait(b"unexpected");
                }
                Err(_) => {
                    let _ = conn.send_nowait(b"ok");
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        ResolveDisabledHandler
    }
}

#[test]
fn resolve_disabled_returns_error() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .resolver_threads(0)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<ResolveDisabledHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"ok");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Unix domain socket tests ────────────────────────────────────────

/// UDS echo server: bind_unix, connect via std UnixStream, echo round trip.
#[test]
fn unix_socket_echo() {
    use std::os::unix::net::UnixStream;

    let dir = std::env::temp_dir();
    let sock_path = dir.join(format!("ringline-test-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock_path);

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind_unix(&sock_path)
        .launch::<AsyncEcho>()
        .expect("launch failed");

    // Wait for the socket file to appear.
    for _ in 0..200 {
        if sock_path.exists() {
            std::thread::sleep(Duration::from_millis(10));
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(sock_path.exists(), "socket file not created");

    let mut stream = UnixStream::connect(&sock_path).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let msg = b"hello unix";
    stream.write_all(msg).unwrap();
    stream.flush().unwrap();

    let mut buf = vec![0u8; msg.len()];
    stream.read_exact(&mut buf).unwrap();
    assert_eq!(buf, msg);

    // Multi-round trip.
    for i in 0..5 {
        let payload = format!("uds-msg-{i}");
        stream.write_all(payload.as_bytes()).unwrap();
        stream.flush().unwrap();
        let mut buf = vec![0u8; payload.len()];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(buf, payload.as_bytes());
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    let _ = std::fs::remove_file(&sock_path);
}

/// peer_addr returns PeerAddr::Tcp for TCP connections (regression).
#[test]
fn peer_addr_tcp_regression() {
    static TCP_PEER: AtomicU32 = AtomicU32::new(0);

    struct PeerAddrHandler;
    impl AsyncEventHandler for PeerAddrHandler {
        fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
            async move {
                if let Some(ringline::PeerAddr::Tcp(_)) = conn.peer_addr() {
                    TCP_PEER.store(1, Ordering::SeqCst);
                }
                let _ = conn
                    .with_data(|data| {
                        let _ = conn.send_nowait(data);
                        ParseResult::Consumed(data.len())
                    })
                    .await;
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            PeerAddrHandler
        }
    }

    TCP_PEER.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<PeerAddrHandler>()
        .expect("launch failed");

    wait_for_server(&addr);
    let _ = echo_round_trip(&addr, b"x");
    assert_eq!(TCP_PEER.load(Ordering::SeqCst), 1);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── CancellationToken tests ─────────────────────────────────────────

static CANCEL_RESULT: AtomicU32 = AtomicU32::new(0);

/// Cancellation token interrupts a long-running task via select.
struct CancellationHandler;

impl AsyncEventHandler for CancellationHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let token = ringline::CancellationToken::new();
            let child = token.child_token();

            // Spawn a task that waits for cancellation.
            let handle = ringline::spawn_with_handle(async move {
                child.cancelled().await;
                42u32
            })
            .unwrap();

            // Cancel after a short delay.
            ringline::sleep(Duration::from_millis(10)).await;
            token.cancel();

            // The spawned task should now complete.
            let val = handle.await;
            CANCEL_RESULT.store(val, Ordering::SeqCst);
            let _ = conn.send_nowait(b"done");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        CancellationHandler
    }
}

#[test]
fn cancellation_token_wakes_task() {
    CANCEL_RESULT.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<CancellationHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"done");
    assert_eq!(CANCEL_RESULT.load(Ordering::SeqCst), 42);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Select between data and cancellation — cancellation wins.
static SELECT_CANCEL: AtomicU32 = AtomicU32::new(0);

struct SelectCancelHandler;

impl AsyncEventHandler for SelectCancelHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }

            let token = ringline::CancellationToken::new();

            // Cancel immediately — the select should pick cancellation
            // over a long sleep.
            token.cancel();

            let result =
                ringline::select(ringline::sleep(Duration::from_secs(60)), token.cancelled()).await;

            match result {
                ringline::Either::Right(()) => SELECT_CANCEL.store(1, Ordering::SeqCst),
                _ => SELECT_CANCEL.store(99, Ordering::SeqCst),
            }
            let _ = conn.send_nowait(b"done");
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SelectCancelHandler
    }
}

#[test]
fn cancellation_token_with_select() {
    SELECT_CANCEL.store(0, Ordering::SeqCst);
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<SelectCancelHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let got = trigger_and_read(&addr);
    assert_eq!(&got, b"done");
    assert_eq!(SELECT_CANCEL.load(Ordering::SeqCst), 1);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Zero-copy forward tests ────────────────────────────────────────
//
// These tests exercise the forward_recv_buf path, which sends directly
// from the kernel recv buffer without copying into the send pool.

struct ForwardEcho;

impl AsyncEventHandler for ForwardEcho {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            loop {
                let n = conn
                    .with_data(|data| {
                        if let Err(e) = conn.forward_recv_buf(data) {
                            eprintln!("echo: forward_recv_buf failed: {e}");
                            return ParseResult::NeedMore;
                        }
                        ParseResult::Consumed(data.len())
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        ForwardEcho
    }
}

#[test]
fn forward_echo_small_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<ForwardEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let msg = b"Hello, zero-copy forward!";
    let response = echo_round_trip(&addr, msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn forward_echo_large_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .recv_buffer(64, 32768)
        .send_pool(64, 32768)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<ForwardEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // 32KB — exercises the full zero-copy recv + forward path.
    let msg: Vec<u8> = (0..32768).map(|i| (i % 256) as u8).collect();
    let response = echo_round_trip(&addr, &msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn forward_echo_message_larger_than_buffer() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    // Buffer is 4KB but message is 8KB — forces accumulator fallback
    // (forward_recv_buf falls back to send_nowait when data is from accumulator).
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<ForwardEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let msg: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
    let response = echo_round_trip(&addr, &msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn forward_echo_multiple_connections() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<ForwardEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut join_handles = Vec::new();
    for conn_id in 0..10u8 {
        let addr = addr.clone();
        join_handles.push(std::thread::spawn(move || {
            let mut stream = TcpStream::connect(&addr).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream.set_nodelay(true).unwrap();

            for round in 0..20u8 {
                let msg = vec![conn_id ^ round; 64];
                stream.write_all(&msg).unwrap();

                let mut buf = vec![0u8; 64];
                let mut total = 0;
                while total < 64 {
                    match stream.read(&mut buf[total..]) {
                        Ok(0) => panic!("unexpected EOF"),
                        Ok(n) => total += n,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => panic!("read error: {e}"),
                    }
                }
                assert_eq!(buf, msg, "data mismatch conn={conn_id} round={round}");
            }
        }));
    }

    for h in join_handles {
        h.join().unwrap();
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn forward_echo_sequential_sends() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<ForwardEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Many sequential round-trips on one connection to stress the
    // pending recv buf → replenish → reuse cycle.
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.set_nodelay(true).unwrap();

    for i in 0..500u16 {
        let msg = format!("msg-{i:04}");
        stream.write_all(msg.as_bytes()).unwrap();

        let mut buf = vec![0u8; msg.len()];
        let mut total = 0;
        while total < msg.len() {
            match stream.read(&mut buf[total..]) {
                Ok(0) => panic!("unexpected EOF at msg {i}"),
                Ok(n) => total += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => panic!("read error at msg {i}: {e}"),
            }
        }
        assert_eq!(buf, msg.as_bytes(), "data mismatch at msg {i}");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Connection-task panic recovery ─────────────────────────────────────

/// Handler that panics if any received data starts with `die`, otherwise
/// echoes. Lets one test run both paths against the same worker.
struct PanickingThenEcho;

#[allow(clippy::manual_async_fn)]
impl AsyncEventHandler for PanickingThenEcho {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            loop {
                let n = conn
                    .with_data(|data| {
                        if data.starts_with(b"die") {
                            panic!("intentional panic in connection task");
                        }
                        let _ = conn.send_nowait(data);
                        ParseResult::Consumed(data.len())
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        PanickingThenEcho
    }
}

#[test]
fn connection_task_panic_does_not_kill_worker() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<PanickingThenEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Connection 1: trigger panic. Connection should be torn down; we
    // expect the read side to see EOF.
    {
        let mut s = TcpStream::connect(&addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        s.write_all(b"die-now").unwrap();
        let mut buf = [0u8; 16];
        // The worker should close us; read returns 0 (EOF) or an error.
        let _ = s.read(&mut buf);
    }

    // Connection 2: must succeed — proves the worker is still alive.
    {
        let mut s = TcpStream::connect(&addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        s.write_all(b"hello").unwrap();
        let mut buf = vec![0u8; 5];
        let mut total = 0;
        while total < 5 {
            match s.read(&mut buf[total..]) {
                Ok(0) => panic!("worker died after panic — second connection got EOF"),
                Ok(n) => total += n,
                Err(e) => panic!("worker died after panic: {e}"),
            }
        }
        assert_eq!(&buf, b"hello");
    }

    shutdown.shutdown();
    for h in handles {
        // The worker should exit cleanly; the panic was caught.
        let _ = h.join();
    }
}
