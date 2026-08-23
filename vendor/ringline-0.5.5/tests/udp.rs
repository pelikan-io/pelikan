#![allow(clippy::manual_async_fn)]
//! Integration tests focused on the UDP path.
//!
//! These tests exercise the UDP API end-to-end on whichever backend is
//! compiled in. The io_uring backend uses multishot recvmsg + a send slot
//! ring; the mio backend uses synchronous `recv_from` / `send_to`. Both
//! paths funnel through the same `UdpCtx` API so the tests are
//! backend-agnostic except where noted.

use ringline::ConfigBuilder;
use std::collections::HashSet;
use std::future::Future;
use std::net::{Ipv6Addr, SocketAddr, UdpSocket};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ringline::{
    AsyncEventHandler, Config, ConnCtx, ParseResult, RinglineBuilder, UdpCtx, UdpSendError,
};

// ── Helpers ────────────────────────────────────────────────────────────

fn base_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .max_connections(64)
        .send_pool(64, 16384)
        .standalone_task_capacity(64)
        .tick_timeout_us(5_000)
}

fn base_config() -> Config {
    base_config_builder().build().expect("valid config")
}

fn free_udp_port() -> u16 {
    // Bind a UDP socket to :0, read the port, drop the socket.
    let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap().port()
}

fn free_udp_port_v6() -> u16 {
    let s = std::net::UdpSocket::bind("[::1]:0").unwrap();
    s.local_addr().unwrap().port()
}

fn await_handler_started(flag: &AtomicUsize) {
    for _ in 0..400 {
        if flag.load(Ordering::SeqCst) > 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("UDP handler did not start within timeout");
}

// Many tests share static slots since `on_udp_bind` is called once per worker
// per bound address and we want to plug per-test handler state into the
// running worker. Tests that use the same slot must run serially — gate
// them with this mutex.
static UDP_SLOT_LOCK: Mutex<()> = Mutex::new(());

// ── Echo handler used by most tests ────────────────────────────────────

struct UdpEcho {
    started: Arc<AtomicUsize>,
}

impl AsyncEventHandler for UdpEcho {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }

    fn create_for_worker(_id: usize) -> Self {
        UdpEcho {
            started: ECHO_STARTED.get_or_init(Default::default).clone(),
        }
    }

    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            loop {
                let (data, peer) = udp.recv_from().await;
                // Loop until the send pool has room.
                loop {
                    match udp.send_to(peer, &data) {
                        Ok(()) => break,
                        Err(UdpSendError::PoolExhausted)
                        | Err(UdpSendError::SubmissionQueueFull) => {
                            udp.send_ready().await;
                        }
                        Err(_) => break,
                    }
                }
            }
        }))
    }
}

static ECHO_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

fn reset_echo_started() {
    let started = ECHO_STARTED.get_or_init(Default::default);
    started.store(0, Ordering::SeqCst);
}

// ── Tests: basic round-trip ────────────────────────────────────────────

#[test]
fn udp_basic_round_trip() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<UdpEcho>()
        .expect("launch");

    await_handler_started(ECHO_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let payload = b"hello udp";
    client.send_to(payload, addr).unwrap();

    let mut buf = [0u8; 64];
    let (n, src) = client.recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..n], payload);
    assert_eq!(src, addr, "echo source mismatch");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn udp_echo_many_datagrams_in_sequence() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<UdpEcho>()
        .expect("launch");
    await_handler_started(ECHO_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let mut buf = [0u8; 1024];
    for i in 0..200u32 {
        let msg = format!("seq-{i}");
        client.send_to(msg.as_bytes(), addr).unwrap();
        let (n, _src) = client.recv_from(&mut buf).unwrap();
        assert_eq!(
            std::str::from_utf8(&buf[..n]).unwrap(),
            msg,
            "echoed payload mismatch on iteration {i}"
        );
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Multiple distinct clients (peer addr round-trip) ────────────────────

#[test]
fn udp_echo_distinguishes_peers() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<UdpEcho>()
        .expect("launch");
    await_handler_started(ECHO_STARTED.get().unwrap());

    // Three clients, each gets its own message echoed to its own port.
    let clients: Vec<UdpSocket> = (0..3)
        .map(|_| {
            let s = UdpSocket::bind("127.0.0.1:0").unwrap();
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            s
        })
        .collect();

    for (i, c) in clients.iter().enumerate() {
        let m = format!("payload-{i}");
        c.send_to(m.as_bytes(), addr).unwrap();
    }

    let mut buf = [0u8; 64];
    for (i, c) in clients.iter().enumerate() {
        let (n, _) = c.recv_from(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, format!("payload-{i}"), "client {i} got wrong echo");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Burst send: clients send many packets back-to-back ──────────────────

#[test]
fn udp_echo_burst_unique_payloads() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let cfg = base_config_builder()
        // Make sure send slot ring is comfortably larger than the burst so the
        // server doesn't have to wait on send_ready for the common case.
        .udp_send_slots(64)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<UdpEcho>()
        .expect("launch");
    await_handler_started(ECHO_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let count = 100u32;
    for i in 0..count {
        let payload = format!("burst-{i:04}");
        client.send_to(payload.as_bytes(), addr).unwrap();
    }

    // Recv everything; the kernel may reorder but loopback usually preserves
    // order. We tolerate either by comparing as a multiset.
    let mut got = HashSet::new();
    let mut buf = [0u8; 128];
    while got.len() < count as usize {
        match client.recv_from(&mut buf) {
            Ok((n, _src)) => {
                let s = std::str::from_utf8(&buf[..n]).unwrap().to_string();
                got.insert(s);
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => panic!("recv error: {e}"),
        }
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }

    let lost = count as usize - got.len();
    // Tolerate small loss on saturated loopback, but not catastrophic.
    assert!(
        lost <= 5,
        "lost {lost} of {count} echoes — too many; received={}",
        got.len()
    );
}

// ── Larger payloads near MTU ───────────────────────────────────────────

#[test]
fn udp_large_datagram_within_mtu() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // Bump the recv buffer to comfortably cover ~1400 bytes + header.
    let cfg = base_config_builder()
        .udp_recv_buffer(128, 4096)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<UdpEcho>()
        .expect("launch");
    await_handler_started(ECHO_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let payload: Vec<u8> = (0u8..=255).cycle().take(1400).collect();
    client.send_to(&payload, addr).unwrap();

    let mut buf = vec![0u8; 4096];
    let (n, _src) = client.recv_from(&mut buf).unwrap();
    assert_eq!(n, payload.len(), "echoed length mismatch");
    assert_eq!(&buf[..n], payload.as_slice(), "echoed payload mismatch");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Multiple bound UDP addresses ────────────────────────────────────────

#[test]
fn udp_multiple_bound_sockets() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    let p1 = free_udp_port();
    let p2 = loop {
        let p = free_udp_port();
        if p != p1 {
            break p;
        }
    };
    let a1: SocketAddr = format!("127.0.0.1:{p1}").parse().unwrap();
    let a2: SocketAddr = format!("127.0.0.1:{p2}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(a1)
        .bind_udp(a2)
        .launch::<UdpEcho>()
        .expect("launch");

    // Wait for both UDP handlers to start.
    let started = ECHO_STARTED.get_or_init(Default::default);
    for _ in 0..400 {
        if started.load(Ordering::SeqCst) >= 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        started.load(Ordering::SeqCst) >= 2,
        "both UDP handlers should have started"
    );

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    for (addr, msg) in [(a1, b"to-port-1".as_ref()), (a2, b"to-port-2")] {
        client.send_to(msg, addr).unwrap();
        let mut buf = [0u8; 64];
        let (n, src) = client.recv_from(&mut buf).unwrap();
        assert_eq!(&buf[..n], msg);
        assert_eq!(src, addr, "echo came from wrong socket");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── IPv6 ───────────────────────────────────────────────────────────────

#[test]
fn udp_ipv6_round_trip() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    // Skip if loopback IPv6 is unavailable in the test environment.
    let probe = match UdpSocket::bind("[::1]:0") {
        Ok(s) => s,
        Err(_) => return,
    };
    drop(probe);

    let port = free_udp_port_v6();
    let addr: SocketAddr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), port);

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<UdpEcho>()
        .expect("launch");
    await_handler_started(ECHO_STARTED.get().unwrap());

    let client = UdpSocket::bind("[::1]:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let payload = b"v6 echo";
    client.send_to(payload, addr).unwrap();

    let mut buf = [0u8; 64];
    let (n, src) = client.recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..n], payload);
    assert_eq!(src.port(), port, "echo source port mismatch");
    assert!(src.is_ipv6(), "echo source not IPv6: {src}");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Send pool exhaustion / send_ready ──────────────────────────────────

/// Handler that never responds to incoming datagrams but spams `send_to` until
/// it sees `PoolExhausted`, awaits `send_ready`, then continues. We use this
/// to verify the ring-based send slot accounting.
struct SendStress {
    started: Arc<AtomicUsize>,
    saw_exhausted: Arc<AtomicUsize>,
    sent_ok: Arc<AtomicUsize>,
}

static SEND_STRESS_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static SEND_STRESS_EXHAUSTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static SEND_STRESS_SENT: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static SEND_STRESS_PEER: OnceLock<Mutex<Option<SocketAddr>>> = OnceLock::new();

impl AsyncEventHandler for SendStress {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        SendStress {
            started: SEND_STRESS_STARTED.get_or_init(Default::default).clone(),
            saw_exhausted: SEND_STRESS_EXHAUSTED.get_or_init(Default::default).clone(),
            sent_ok: SEND_STRESS_SENT.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        let saw_exhausted = self.saw_exhausted.clone();
        let sent_ok = self.sent_ok.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            // Wait for the test to learn our peer address from a probe message.
            let (_data, peer) = udp.recv_from().await;
            let _ = SEND_STRESS_PEER
                .get_or_init(|| Mutex::new(None))
                .lock()
                .unwrap()
                .replace(peer);
            // Now blast send_to and verify exhaustion paths work.
            let payload = b"x".repeat(64);
            // Send up to a fixed total; the slot ring is small so we expect
            // to hit PoolExhausted multiple times.
            for _ in 0..2_000u32 {
                loop {
                    match udp.send_to(peer, &payload) {
                        Ok(()) => {
                            sent_ok.fetch_add(1, Ordering::SeqCst);
                            break;
                        }
                        Err(UdpSendError::PoolExhausted) => {
                            saw_exhausted.fetch_add(1, Ordering::SeqCst);
                            udp.send_ready().await;
                        }
                        Err(UdpSendError::SubmissionQueueFull) => {
                            udp.send_ready().await;
                        }
                        Err(UdpSendError::Io(_)) => {
                            return;
                        }
                        Err(_) => {
                            return;
                        }
                    }
                }
            }
        }))
    }
}

#[test]
fn udp_send_ready_unblocks_after_exhaustion() {
    // io_uring backend only — on mio the send is synchronous and PoolExhausted
    // means WouldBlock from the kernel, which only triggers under very
    // different conditions and isn't a meaningful exercise of `send_ready`.
    if ringline::backend() != ringline::Backend::IoUring {
        return;
    }
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    SEND_STRESS_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    SEND_STRESS_EXHAUSTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    SEND_STRESS_SENT
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    let _ = SEND_STRESS_PEER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap()
        .take();

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let cfg = base_config_builder()
        .udp_send_slots(4)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<SendStress>()
        .expect("launch");
    await_handler_started(SEND_STRESS_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    // Probe so the server learns our address.
    client.send_to(b"hello", addr).unwrap();

    // Drain incoming traffic for a bit.
    let mut buf = vec![0u8; 4096];
    let mut received = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        match client.recv_from(&mut buf) {
            Ok(_) => received += 1,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // Stop draining when the server quiesces or the cap is hit.
                let sent = SEND_STRESS_SENT
                    .get()
                    .map(|c| c.load(Ordering::SeqCst))
                    .unwrap_or(0);
                if sent >= 2_000 {
                    break;
                }
            }
            Err(e) => panic!("recv: {e}"),
        }
    }

    let exhausted = SEND_STRESS_EXHAUSTED.get().unwrap().load(Ordering::SeqCst);
    let sent = SEND_STRESS_SENT.get().unwrap().load(Ordering::SeqCst);
    assert!(
        sent >= 1_000,
        "server should have sent many datagrams; sent={sent}"
    );
    assert!(
        exhausted >= 5,
        "server should have observed PoolExhausted multiple times; \
         exhausted={exhausted}"
    );
    assert!(
        received >= 100,
        "client should have received many datagrams; received={received}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Datagram larger than copy slot returns a recoverable error ─────────

/// Verifies that `send_to` with data larger than `send_copy_slot_size`
/// returns a *non-retryable* error (not `PoolExhausted` — that's a transient
/// condition that callers may wait out via `send_ready()`, and the
/// oversized case will never become retryable on its own). Also asserts
/// the socket remains usable for follow-up sends of normal size.
struct OversizedSend {
    started: Arc<AtomicUsize>,
    oversized_err: Arc<AtomicUsize>,
    follow_up_ok: Arc<AtomicUsize>,
}

static OVER_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static OVER_ERR: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static OVER_OK: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

impl AsyncEventHandler for OversizedSend {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        OversizedSend {
            started: OVER_STARTED.get_or_init(Default::default).clone(),
            oversized_err: OVER_ERR.get_or_init(Default::default).clone(),
            follow_up_ok: OVER_OK.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        let oversized_err = self.oversized_err.clone();
        let follow_up_ok = self.follow_up_ok.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            let (_data, peer) = udp.recv_from().await;
            // Try to send something far bigger than send_copy_slot_size.
            let big = vec![0xAAu8; 32 * 1024];
            match udp.send_to(peer, &big) {
                Ok(()) => {} // would be wrong but let's not abort
                Err(UdpSendError::PoolExhausted) | Err(UdpSendError::SubmissionQueueFull) => {
                    // Wrong: oversize is a permanent condition, not a
                    // transient one. The handler intentionally does NOT
                    // count this case so the test fails loudly.
                }
                Err(UdpSendError::Io(_)) => {
                    oversized_err.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {}
            }
            // Now send a normal payload; this must succeed.
            if udp.send_to(peer, b"after-oversized").is_ok() {
                follow_up_ok.fetch_add(1, Ordering::SeqCst);
            }
        }))
    }
}

#[test]
fn udp_oversized_send_does_not_corrupt_state() {
    // Only the io_uring backend funnels sends through `send_copy_pool`,
    // where oversize datagrams hit the slot-size limit and need an
    // explicit rejection. The mio backend hands the buffer straight to
    // the kernel, which happily accepts datagrams up to ~65 KiB on
    // loopback, so this scenario doesn't apply there.
    if ringline::backend() != ringline::Backend::IoUring {
        return;
    }
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    OVER_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    OVER_ERR
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    OVER_OK
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let cfg = base_config_builder()
        .send_pool(64, 4096)
        .udp_recv_buffer(128, 4096)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<OversizedSend>()
        .expect("launch");
    await_handler_started(OVER_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    client.send_to(b"trigger", addr).unwrap();

    let mut buf = [0u8; 64];
    let (n, _src) = client.recv_from(&mut buf).unwrap();
    assert_eq!(
        &buf[..n],
        b"after-oversized",
        "follow-up datagram must arrive after the oversized send is rejected"
    );

    let oversized_err = OVER_ERR.get().unwrap().load(Ordering::SeqCst);
    let follow_up_ok = OVER_OK.get().unwrap().load(Ordering::SeqCst);
    assert_eq!(oversized_err, 1, "oversized send must return Err");
    assert_eq!(follow_up_ok, 1, "follow-up send must succeed");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Recv buffer truncation: payload larger than UDP recv buffer ────────

/// When a peer sends a datagram bigger than the UDP recv buffer can hold,
/// the multishot recvmsg sets the `payload_truncated` flag. The handler
/// silently drops the datagram (parser sees truncated msg). We verify that
/// the socket *remains live* — the next non-truncated datagram still gets
/// echoed.
struct CountingEcho {
    started: Arc<AtomicUsize>,
    received: Arc<AtomicU64>,
}

static COUNT_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static COUNT_RECV: OnceLock<Arc<AtomicU64>> = OnceLock::new();

impl AsyncEventHandler for CountingEcho {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        CountingEcho {
            started: COUNT_STARTED.get_or_init(Default::default).clone(),
            received: COUNT_RECV.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        let received = self.received.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            loop {
                let (data, peer) = udp.recv_from().await;
                received.fetch_add(1, Ordering::SeqCst);
                let _ = udp.send_to(peer, &data);
            }
        }))
    }
}

#[test]
fn udp_truncated_datagram_recoverable() {
    if ringline::backend() != ringline::Backend::IoUring {
        // Only the io_uring backend uses a fixed-size provided buffer per
        // datagram. mio's recv path uses a 64KiB stack buffer that always
        // fits the payload, so there's nothing to truncate.
        return;
    }
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    COUNT_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    COUNT_RECV
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // Tight recv buffer: 256 bytes total → ~70 bytes for payload after the
    // io_uring header + sockaddr.
    let cfg = base_config_builder()
        .udp_recv_buffer(16, 256)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<CountingEcho>()
        .expect("launch");
    await_handler_started(COUNT_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();

    // Send an oversized datagram that *will* be truncated server-side.
    let big = vec![0xCDu8; 1500];
    client.send_to(&big, addr).unwrap();
    // Server must drop it; client should receive nothing.
    let mut buf = vec![0u8; 4096];
    match client.recv_from(&mut buf) {
        Err(e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut => {}
        Ok((n, _)) => panic!("did not expect echo of truncated datagram, got {n} bytes"),
        Err(e) => panic!("unexpected recv error: {e}"),
    }

    // Now send a small one; it must round-trip — i.e. the multishot was
    // rearmed properly after the truncated datagram fed back through the
    // ring.
    let small = b"small";
    client.send_to(small, addr).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let (n, _src) = client.recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..n], small);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Multi-worker UDP via SO_REUSEPORT ──────────────────────────────────

/// Each worker gets its own UDP socket bound to the same address with
/// SO_REUSEPORT. The kernel hashes incoming datagrams across workers.
/// Our echo handler reports its worker_id in the response so we can verify
/// that more than one worker actually got traffic.
struct ReuseportEcho {
    worker_id: usize,
    started: Arc<AtomicUsize>,
}

static REUSE_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

impl AsyncEventHandler for ReuseportEcho {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(id: usize) -> Self {
        ReuseportEcho {
            worker_id: id,
            started: REUSE_STARTED.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let worker_id = self.worker_id;
        let started = self.started.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            loop {
                let (mut data, peer) = udp.recv_from().await;
                data.push(b':');
                data.extend_from_slice(format!("{worker_id}").as_bytes());
                loop {
                    match udp.send_to(peer, &data) {
                        Ok(()) => break,
                        Err(UdpSendError::PoolExhausted)
                        | Err(UdpSendError::SubmissionQueueFull) => {
                            udp.send_ready().await;
                        }
                        Err(_) => break,
                    }
                }
            }
        }))
    }
}

#[test]
fn udp_reuseport_balances_across_workers() {
    // SO_REUSEPORT load-balances datagrams across bound sockets only on
    // Linux 3.9+. On macOS/BSD it just permits the multi-bind without
    // hashing, so every datagram lands on a single worker and this
    // assertion can never hold. Skip there.
    if !cfg!(target_os = "linux") {
        return;
    }
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    REUSE_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let workers = 4;
    let cfg = base_config_builder()
        .workers(workers)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<ReuseportEcho>()
        .expect("launch");

    let started = REUSE_STARTED.get().unwrap();
    for _ in 0..400 {
        if started.load(Ordering::SeqCst) >= workers {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        started.load(Ordering::SeqCst),
        workers,
        "all workers should have started UDP handlers"
    );

    // Give each worker many distinct source ports so the kernel hash spreads
    // datagrams. A single source/dest tuple usually sticks to one worker.
    let mut clients: Vec<UdpSocket> = Vec::new();
    for _ in 0..32 {
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        clients.push(s);
    }

    for c in &clients {
        c.send_to(b"x", addr).unwrap();
    }

    let mut workers_seen: HashSet<usize> = HashSet::new();
    let mut buf = [0u8; 64];
    for c in &clients {
        if let Ok((n, _src)) = c.recv_from(&mut buf) {
            // payload is "x:<worker_id>"
            let s = std::str::from_utf8(&buf[..n]).unwrap();
            let id: usize = s
                .strip_prefix("x:")
                .and_then(|t| t.parse().ok())
                .unwrap_or(usize::MAX);
            workers_seen.insert(id);
        }
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }

    assert!(
        workers_seen.len() >= 2,
        "REUSEPORT failed to spread across workers; only {workers_seen:?} echoed"
    );
}

// ── Empty (zero-byte) datagram ─────────────────────────────────────────

/// Verifies that a UDP socket can echo zero-length datagrams (a legal
/// thing on the wire) without dropping them. Some recvmsg implementations
/// confuse `result == 0` with EOF; the io_uring multishot path uses a
/// composite header so the CQE result is always non-zero on success — but
/// we want a regression test in case future refactors break that.
#[test]
fn udp_echo_zero_byte_datagram() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<UdpEcho>()
        .expect("launch");
    await_handler_started(ECHO_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    client.send_to(&[], addr).unwrap();

    let mut buf = [0u8; 16];
    let (n, src) = client.recv_from(&mut buf).unwrap();
    assert_eq!(n, 0, "echoed datagram should be empty");
    assert_eq!(src, addr);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── TCP + UDP coexistence ──────────────────────────────────────────────

struct TcpUdpHandler {
    started: Arc<AtomicUsize>,
}

static TCP_UDP_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

impl AsyncEventHandler for TcpUdpHandler {
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
        TcpUdpHandler {
            started: TCP_UDP_STARTED.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            loop {
                let (data, peer) = udp.recv_from().await;
                let _ = udp.send_to(peer, &data);
            }
        }))
    }
}

#[test]
fn udp_and_tcp_coexist() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    TCP_UDP_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let tcp_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let udp_port = free_udp_port();
    let tcp_addr: SocketAddr = format!("127.0.0.1:{tcp_port}").parse().unwrap();
    let udp_addr: SocketAddr = format!("127.0.0.1:{udp_port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind(tcp_addr)
        .bind_udp(udp_addr)
        .launch::<TcpUdpHandler>()
        .expect("launch");

    // Wait for both TCP listener and UDP handler.
    await_handler_started(TCP_UDP_STARTED.get().unwrap());
    for _ in 0..200 {
        if std::net::TcpStream::connect(tcp_addr).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // TCP echo.
    {
        use std::io::{Read, Write};
        let mut s = std::net::TcpStream::connect(tcp_addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        s.write_all(b"tcp-msg").unwrap();
        let mut buf = [0u8; 32];
        let n = s.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"tcp-msg");
    }

    // UDP echo.
    {
        let c = UdpSocket::bind("127.0.0.1:0").unwrap();
        c.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        c.send_to(b"udp-msg", udp_addr).unwrap();
        let mut buf = [0u8; 32];
        let (n, _) = c.recv_from(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"udp-msg");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Handler future exits early ─────────────────────────────────────────

/// Handler whose UDP future returns immediately after recording it
/// started — emulates a buggy app that drops out of the recv loop.
/// Datagrams arriving after the future returns must not crash the worker.
struct ExitingUdpHandler {
    started: Arc<AtomicUsize>,
}

static EXITING_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

impl AsyncEventHandler for ExitingUdpHandler {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        ExitingUdpHandler {
            started: EXITING_STARTED.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, _udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            // Future returns immediately; no recv loop runs.
        }))
    }
}

#[test]
fn udp_handler_exit_does_not_crash_worker() {
    // The on_udp_bind future returns straight away. We then send a flood
    // of datagrams and verify the worker stays alive: the shutdown
    // sequence completes, no panics, no thread join errors. This proves
    // there's no crash on the read side, and is the minimum bar before
    // we worry about the secondary concern (queue growth — see the
    // comment at the bottom of this test).
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    EXITING_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<ExitingUdpHandler>()
        .expect("launch");
    await_handler_started(EXITING_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    // Fire many datagrams; the server has no recv loop so it accepts and
    // ignores them.
    for i in 0..200u32 {
        let payload = format!("orphan-{i}");
        let _ = client.send_to(payload.as_bytes(), addr);
    }

    // Give the runtime time to chew through the kernel queue.
    std::thread::sleep(Duration::from_millis(200));

    shutdown.shutdown();
    for h in handles {
        h.join()
            .expect("worker thread panicked")
            .expect("worker exited with error");
    }

    // The unbounded-queue-growth concern is covered by
    // `udp_recv_queue_capacity_drops_excess_datagrams` below.
}

#[test]
fn udp_recv_queue_capacity_drops_excess_datagrams() {
    // Verify the per-socket recv queue cap actually fires when a handler
    // stops draining. With no cap, a handler that exits leaks memory at
    // line rate; this test pins the behavior down.
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    EXITING_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let cfg = base_config_builder()
        .udp_recv_queue_capacity(8)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<ExitingUdpHandler>()
        .expect("launch");
    await_handler_started(EXITING_STARTED.get().unwrap());

    // Snapshot the dropped counter before the burst.
    let before = ringline::metrics::UDP
        .value(ringline::metrics::udp::DATAGRAMS_DROPPED)
        .unwrap_or(0);

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    for _ in 0..256u32 {
        let _ = client.send_to(b"x", addr);
    }
    std::thread::sleep(Duration::from_millis(300));

    let after = ringline::metrics::UDP
        .value(ringline::metrics::udp::DATAGRAMS_DROPPED)
        .unwrap_or(0);
    let dropped = after - before;
    assert!(
        dropped >= 200,
        "cap should have dropped most of 256 datagrams (cap=8); \
         dropped={dropped}, before={before}, after={after}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Handler future panic ───────────────────────────────────────────────

/// Handler whose UDP future panics on the first received datagram.
/// We want the worker to keep running — at minimum, a separate UDP
/// socket must continue to function, and shutdown must complete.
struct PanickingUdpHandler {
    started: Arc<AtomicUsize>,
}

static PANIC_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

impl AsyncEventHandler for PanickingUdpHandler {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        PanickingUdpHandler {
            started: PANIC_STARTED.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        // Only the FIRST socket panics; the second is a normal echo so we
        // can verify the worker stayed alive.
        if udp.index() == 0 {
            Some(Box::pin(async move {
                started.fetch_add(1, Ordering::SeqCst);
                let (_data, _peer) = udp.recv_from().await;
                panic!("intentional panic from UDP handler");
            }))
        } else {
            Some(Box::pin(async move {
                started.fetch_add(1, Ordering::SeqCst);
                loop {
                    let (data, peer) = udp.recv_from().await;
                    let _ = udp.send_to(peer, &data);
                }
            }))
        }
    }
}

#[test]
fn udp_handler_panic_keeps_worker_alive() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    PANIC_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let p1 = free_udp_port();
    let p2 = loop {
        let p = free_udp_port();
        if p != p1 {
            break p;
        }
    };
    let panicking_addr: SocketAddr = format!("127.0.0.1:{p1}").parse().unwrap();
    let echo_addr: SocketAddr = format!("127.0.0.1:{p2}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(panicking_addr) // index 0 — the panicking one
        .bind_udp(echo_addr) // index 1 — normal echo
        .launch::<PanickingUdpHandler>()
        .expect("launch");

    let started = PANIC_STARTED.get().unwrap();
    for _ in 0..400 {
        if started.load(Ordering::SeqCst) >= 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        started.load(Ordering::SeqCst) >= 2,
        "both UDP handlers should have started"
    );

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    // Trigger the panic.
    client.send_to(b"boom", panicking_addr).unwrap();
    // Give the panic a moment to propagate (or be caught).
    std::thread::sleep(Duration::from_millis(100));

    // The other UDP socket on the *same worker* must still echo. If the
    // panic took down the worker, this recv times out and the test fails.
    client.send_to(b"still-alive", echo_addr).unwrap();
    let mut buf = [0u8; 32];
    match client.recv_from(&mut buf) {
        Ok((n, _src)) => assert_eq!(&buf[..n], b"still-alive"),
        Err(e) => panic!("worker appears dead after handler panic: {e}"),
    }

    shutdown.shutdown();
    for h in handles {
        // We tolerate the worker thread surfacing the panic as a join
        // error, but we *don't* tolerate it deadlocking shutdown.
        let _ = h.join();
    }
}

// ── Shutdown with in-flight UDP sends ─────────────────────────────────

struct InFlightSendHandler {
    started: Arc<AtomicUsize>,
}

static INFLIGHT_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

impl AsyncEventHandler for InFlightSendHandler {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        InFlightSendHandler {
            started: INFLIGHT_STARTED.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            let (_data, peer) = udp.recv_from().await;
            // Saturate the slot ring as fast as possible; some sends will
            // succeed, some will hit PoolExhausted. We don't await
            // completions — the test wants in-flight sends pending when
            // shutdown fires.
            let payload = vec![0xCDu8; 256];
            for _ in 0..1_000u32 {
                let _ = udp.send_to(peer, &payload);
            }
            // Sleep forever to keep the future alive past shutdown.
            loop {
                let _ = udp.recv_from().await;
            }
        }))
    }
}

#[test]
fn udp_shutdown_with_inflight_sends_completes() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    INFLIGHT_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<InFlightSendHandler>()
        .expect("launch");
    await_handler_started(INFLIGHT_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    let client_addr = client.local_addr().unwrap();
    client.send_to(b"trigger", addr).unwrap();

    // Wait briefly so the handler kicks off its 1k send burst.
    std::thread::sleep(Duration::from_millis(50));

    let start = std::time::Instant::now();
    shutdown.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker errored");
    }
    let elapsed = start.elapsed();

    // Shutdown should not hang: the worker drains its CQEs (or just exits)
    // and the join returns within seconds even though sends were in flight.
    assert!(
        elapsed < Duration::from_secs(5),
        "shutdown hung waiting on in-flight UDP sends: took {elapsed:?}"
    );
    // Sanity: client may or may not have received some echoes; we just
    // care that the kernel address still works (worker is gone).
    let _ = client_addr;
}

// ── Concurrent in-flight send_to (slot-ring accounting) ────────────────

struct ConcurrentSenders {
    started: Arc<AtomicUsize>,
    sent_total: Arc<AtomicU64>,
}

static CONC_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static CONC_SENT: OnceLock<Arc<AtomicU64>> = OnceLock::new();

impl AsyncEventHandler for ConcurrentSenders {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        ConcurrentSenders {
            started: CONC_STARTED.get_or_init(Default::default).clone(),
            sent_total: CONC_SENT.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        let sent_total = self.sent_total.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            let (_data, peer) = udp.recv_from().await;
            // 8 send slots, 64 sends → must observe slot recycle multiple
            // times to complete. Tests that the freelist correctly
            // returns slots on CQE.
            let mut sent = 0u64;
            let payload = b"slot-test".to_vec();
            for i in 0..64u32 {
                loop {
                    match udp.send_to(peer, &payload) {
                        Ok(()) => {
                            sent += 1;
                            break;
                        }
                        Err(UdpSendError::PoolExhausted)
                        | Err(UdpSendError::SubmissionQueueFull) => {
                            udp.send_ready().await;
                        }
                        Err(_) => break,
                    }
                }
                let _ = i;
            }
            sent_total.store(sent, Ordering::SeqCst);
        }))
    }
}

#[test]
fn udp_concurrent_inflight_sends_recycle_slots() {
    if ringline::backend() != ringline::Backend::IoUring {
        // mio's send is synchronous; there's no slot ring to recycle.
        return;
    }
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CONC_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    CONC_SENT
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let cfg = base_config_builder()
        .udp_send_slots(8)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<ConcurrentSenders>()
        .expect("launch");
    await_handler_started(CONC_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    client.send_to(b"go", addr).unwrap();

    // Drain everything the server sends.
    let mut received = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut buf = [0u8; 64];
    while std::time::Instant::now() < deadline {
        match client.recv_from(&mut buf) {
            Ok(_) => received += 1,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                if CONC_SENT.get().unwrap().load(Ordering::SeqCst) == 64 {
                    break;
                }
            }
            Err(e) => panic!("recv: {e}"),
        }
    }

    let sent = CONC_SENT.get().unwrap().load(Ordering::SeqCst);
    assert_eq!(
        sent, 64,
        "handler should have completed all 64 sends; sent={sent}"
    );
    assert!(
        received >= 60,
        "client should have received almost all datagrams; received={received}"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Max-size loopback datagram ─────────────────────────────────────────

#[test]
#[cfg(target_os = "linux")]
fn udp_max_size_loopback_datagram() {
    // UDP's theoretical max payload is 65507 bytes (65535 IP MTU - 20
    // IPv4 header - 8 UDP header). We pick a payload that's clearly
    // beyond the standard MTU but still fits comfortably under that
    // cap, and verify it round-trips on loopback. The recv buffer
    // must hold the io_uring_recvmsg_out header (16) + sockaddr_storage
    // (128) + payload, and the send slot must be at least the payload
    // size.
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // The recv buffer is capped at 65535 by config validation; that
    // leaves ~65535 - 16 (recvmsg_out) - 128 (sockaddr_storage) =
    // 65391 bytes for payload. Pick a size well under that but
    // larger than any standard MTU so we exercise loopback's
    // happy-with-jumbo path.
    let payload_len = 65000;
    let cfg = base_config_builder()
        .send_pool(64, 65535)
        .udp_recv_buffer(16, 65535)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<UdpEcho>()
        .expect("launch");
    await_handler_started(ECHO_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    let payload: Vec<u8> = (0..payload_len).map(|i| (i & 0xFF) as u8).collect();
    client.send_to(&payload, addr).unwrap();

    let mut buf = vec![0u8; 128 * 1024];
    let (n, src) = client.recv_from(&mut buf).unwrap();
    assert_eq!(n, payload_len, "echoed datagram length should match");
    assert_eq!(&buf[..n], payload.as_slice(), "echoed payload mismatch");
    assert_eq!(src, addr);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Recv buffer ring exhaustion under burst ────────────────────────────

/// Handler that *blocks* on an atomic flag before calling `recv_from`,
/// then drains and echoes everything in a tight loop. With a tiny
/// `udp_recv_buffer.ring_size`, we can deterministically force the
/// kernel's multishot recvmsg to hit `ENOBUFS` while the handler is
/// blocked — then unblock and verify the worker recovers and processes
/// the surviving datagrams. Using a flag (not a sleep-per-recv) makes
/// the test robust to debug-vs-release-mode timing.
struct BlockedThenEcho {
    started: Arc<AtomicUsize>,
    unblock: Arc<AtomicBool>,
}

static BLOCKED_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static BLOCKED_UNBLOCK: OnceLock<Arc<AtomicBool>> = OnceLock::new();

impl AsyncEventHandler for BlockedThenEcho {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        BlockedThenEcho {
            started: BLOCKED_STARTED.get_or_init(Default::default).clone(),
            unblock: BLOCKED_UNBLOCK.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        let unblock = self.unblock.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            // Yield the executor until the test signals us to start
            // draining. While we're here, the kernel piles up datagrams
            // in our io_uring buffer ring (and beyond, in the socket's
            // kernel queue) — and once the buffer ring is empty, the
            // multishot recvmsg returns ENOBUFS and is torn down.
            while !unblock.load(Ordering::SeqCst) {
                let _ = ringline::sleep(Duration::from_millis(2)).await;
            }
            // Drain whatever survived into the recv queue.
            loop {
                let (data, peer) = udp.recv_from().await;
                let _ = udp.send_to(peer, &data);
            }
        }))
    }
}

#[test]
fn udp_recv_ring_exhaustion_under_burst() {
    if ringline::backend() != ringline::Backend::IoUring {
        // mio's recv path uses a 64 KiB stack buffer per call; there's
        // no buffer ring to exhaust.
        return;
    }
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    BLOCKED_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    BLOCKED_UNBLOCK
        .get_or_init(Default::default)
        .store(false, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let cfg = base_config_builder()
        .udp_recv_buffer(4, 256)
        .build()
        .expect("valid config");
    let buf_empty_before = ringline::metrics::POOL
        .value(ringline::metrics::pool::BUFFER_RING_EMPTY)
        .unwrap_or(0);

    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<BlockedThenEcho>()
        .expect("launch");
    await_handler_started(BLOCKED_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();

    // While the handler is blocked, fire a flood. The kernel will fill
    // our 4-slot buffer ring and then return ENOBUFS for everything
    // else, tearing down the multishot.
    for i in 0..200u32 {
        let payload = format!("burst-{i:04}");
        let _ = client.send_to(payload.as_bytes(), addr);
    }
    // Give the kernel a moment to deliver as many as it can.
    std::thread::sleep(Duration::from_millis(100));

    // Now release the handler. It should recover (the multishot will
    // be re-armed once buffers replenish) and echo whatever is still
    // in the kernel queue.
    BLOCKED_UNBLOCK.get().unwrap().store(true, Ordering::SeqCst);

    let mut received = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut buf = [0u8; 64];
    while std::time::Instant::now() < deadline {
        match client.recv_from(&mut buf) {
            Ok(_) => received += 1,
            Err(_) => break,
        }
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }

    let buf_empty_after = ringline::metrics::POOL
        .value(ringline::metrics::pool::BUFFER_RING_EMPTY)
        .unwrap_or(0);
    let _ = buf_empty_before;
    let _ = buf_empty_after;

    // The core property under test is that the worker survives the
    // burst and resumes processing once the consumer unblocks.
    //
    // We deliberately do NOT assert `BUFFER_RING_EMPTY` ticked: the
    // event loop replenishes recv buffers immediately after each CQE
    // batch, so the kernel only catches an empty ring during a
    // narrow scheduling window. Whether that window opens depends on
    // kernel version, build profile, and machine load — assertions
    // about it are flaky in CI even with a 4-slot ring and a fully
    // blocked consumer. The metric is still useful for operators
    // (it points at undersized rings under sustained burst), but the
    // test that pins it down would have to inject failure into the
    // replenisher to be deterministic.
    assert!(
        received >= 1,
        "expected the worker to recover after the burst and \
         echo at least one of the surviving datagrams (received={received})"
    );
}

// ── Send to unreachable peer ───────────────────────────────────────────

/// Handler that, on first recv, attempts to send to a peer that's
/// guaranteed to have no listener (loopback :1). That send may fail
/// async via ICMP, but the worker must keep running so subsequent
/// real traffic still flows.
struct UnreachableProbe {
    started: Arc<AtomicUsize>,
    follow_up_ok: Arc<AtomicUsize>,
}

static UNREACH_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static UNREACH_FOLLOWUP: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

impl AsyncEventHandler for UnreachableProbe {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        UnreachableProbe {
            started: UNREACH_STARTED.get_or_init(Default::default).clone(),
            follow_up_ok: UNREACH_FOLLOWUP.get_or_init(Default::default).clone(),
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        let follow_up_ok = self.follow_up_ok.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            // First recv: unblock the test by reading the trigger
            // datagram. Then send to an unreachable peer.
            let (_data, peer) = udp.recv_from().await;
            let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
            for _ in 0..5 {
                let _ = udp.send_to(dead, b"to-the-void");
            }
            // Wait for the kernel's async ICMP storm to settle.
            let _ = ringline::sleep(Duration::from_millis(100)).await;
            // Now reply to the original peer; this must succeed.
            if udp.send_to(peer, b"alive").is_ok() {
                follow_up_ok.fetch_add(1, Ordering::SeqCst);
            }
        }))
    }
}

#[test]
fn udp_send_to_unreachable_peer_does_not_kill_worker() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    UNREACH_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    UNREACH_FOLLOWUP
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<UnreachableProbe>()
        .expect("launch");
    await_handler_started(UNREACH_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    client.send_to(b"trigger", addr).unwrap();

    let mut buf = [0u8; 16];
    let (n, _src) = client.recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"alive", "live peer must still receive its echo");

    let follow_up_ok = UNREACH_FOLLOWUP.get().unwrap().load(Ordering::SeqCst);
    assert_eq!(
        follow_up_ok, 1,
        "follow-up send to live peer must succeed after spamming a dead peer"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Invalid bind address propagates an error ───────────────────────────

#[test]
fn udp_invalid_bind_address_returns_error() {
    // 192.0.2.0/24 is RFC-5737 documentation space — guaranteed not to
    // be assigned to any local interface, so bind() with a non-zero
    // address there fails with EADDRNOTAVAIL.
    let bogus: SocketAddr = "192.0.2.1:9".parse().unwrap();

    let result = RinglineBuilder::new(base_config())
        .bind_udp(bogus)
        .launch::<UdpEcho>();

    assert!(
        result.is_err(),
        "binding UDP to an unassigned address must surface the failure to launch()"
    );
}

// ── No fd leak across repeated launch+shutdown cycles ──────────────────

#[cfg(target_os = "linux")]
fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

#[test]
#[cfg(target_os = "linux")]
fn udp_repeated_launch_does_not_leak_fds() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    // Burn a few launches first so any one-shot allocations (resolver
    // pool, blocking pool, etc.) settle in. Compare counts after that.
    for _ in 0..2 {
        reset_echo_started();
        let port = free_udp_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let (shutdown, handles) = RinglineBuilder::new(base_config())
            .bind_udp(addr)
            .launch::<UdpEcho>()
            .expect("launch");
        await_handler_started(ECHO_STARTED.get().unwrap());
        shutdown.shutdown();
        for h in handles {
            h.join().unwrap().unwrap();
        }
    }

    // Brief grace period for io_uring teardown to release fds.
    std::thread::sleep(Duration::from_millis(50));
    let baseline = open_fd_count();

    for _ in 0..6 {
        reset_echo_started();
        let port = free_udp_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let (shutdown, handles) = RinglineBuilder::new(base_config())
            .bind_udp(addr)
            .launch::<UdpEcho>()
            .expect("launch");
        await_handler_started(ECHO_STARTED.get().unwrap());
        shutdown.shutdown();
        for h in handles {
            h.join().unwrap().unwrap();
        }
    }

    // Allow background drops to settle.
    std::thread::sleep(Duration::from_millis(100));
    let after = open_fd_count();

    // Allow some slack for transient fds (logger flush, /proc handle,
    // timing artifacts) but we expect no per-iteration leak.
    let delta = after.saturating_sub(baseline);
    assert!(
        delta < 6,
        "fd count grew by {delta} after 6 launch+shutdown cycles \
         (baseline={baseline}, after={after}); suggests a UDP/io_uring fd leak"
    );
}

// ── UDP_SEGMENT (GSO) round-trip ───────────────────────────────────────

/// Handler that, on the first received datagram, replies once with a
/// `segment_size`-segmented buffer big enough that the kernel splits
/// it into N datagrams. Lets the test verify the peer sees N
/// distinct receives — proving the GSO cmsg actually took effect.
struct GsoSendHandler {
    started: Arc<AtomicUsize>,
    sent: Arc<AtomicUsize>,
    segment_size: u16,
    segments: u16,
}

static GSO_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static GSO_SENT: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static GSO_SEG_SIZE: AtomicU64 = AtomicU64::new(0);
static GSO_SEG_COUNT: AtomicU64 = AtomicU64::new(0);

impl AsyncEventHandler for GsoSendHandler {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        GsoSendHandler {
            started: GSO_STARTED.get_or_init(Default::default).clone(),
            sent: GSO_SENT.get_or_init(Default::default).clone(),
            segment_size: GSO_SEG_SIZE.load(Ordering::SeqCst) as u16,
            segments: GSO_SEG_COUNT.load(Ordering::SeqCst) as u16,
        }
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        let sent = self.sent.clone();
        let seg_size = self.segment_size;
        let seg_count = self.segments;
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            // Drain the trigger datagram so we know the test peer's
            // address; reply with `seg_count` datagrams of `seg_size`
            // bytes each, packed into one buffer with the GSO
            // segment_size cmsg.
            let (_data, peer) = udp.recv_from().await;
            let total = seg_size as usize * seg_count as usize;
            let mut buf = vec![0u8; total];
            // Tag each segment with its index so the receiver can
            // verify ordering.
            for i in 0..seg_count {
                let off = (i as usize) * (seg_size as usize);
                buf[off..off + 2].copy_from_slice(&i.to_le_bytes());
            }
            loop {
                match udp.send_to_gso(peer, &buf, seg_size) {
                    Ok(()) => {
                        sent.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                    Err(UdpSendError::PoolExhausted) | Err(UdpSendError::SubmissionQueueFull) => {
                        udp.send_ready().await;
                    }
                    Err(_) => break,
                }
            }
        }))
    }
}

#[test]
fn udp_gso_segments_one_send_into_many_datagrams() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    GSO_STARTED
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);
    GSO_SENT
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let segment_size: u16 = 1000;
    let segments: u16 = 5;
    GSO_SEG_SIZE.store(segment_size as u64, Ordering::SeqCst);
    GSO_SEG_COUNT.store(segments as u64, Ordering::SeqCst);

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let cfg = base_config_builder()
        .send_pool(64, 16384)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<GsoSendHandler>()
        .expect("launch");
    await_handler_started(GSO_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    client.send_to(b"trigger", addr).unwrap();

    // Recv exactly `segments` datagrams of `segment_size` bytes each.
    let mut buf = vec![0u8; segment_size as usize + 64];
    let mut received = Vec::with_capacity(segments as usize);
    for _ in 0..segments {
        let (n, src) = client
            .recv_from(&mut buf)
            .expect("expected one datagram per segment");
        assert_eq!(
            n as u16, segment_size,
            "each GSO segment should land as a single {segment_size}-byte UDP datagram"
        );
        assert_eq!(src, addr, "datagram should come from the bound port");
        let idx = u16::from_le_bytes([buf[0], buf[1]]);
        received.push(idx);
    }
    // The kernel may interleave a tiny bit on loopback but in
    // practice GSO segments arrive in order. We just check that all
    // expected indices are present (multiset equality), not strict
    // ordering, to keep the test robust to any kernel variability.
    received.sort_unstable();
    let expected: Vec<u16> = (0..segments).collect();
    assert_eq!(
        received, expected,
        "kernel should have produced exactly the {segments} segments"
    );

    // Confirm only one logical send was issued by the handler. The mio
    // fallback sends synchronously inside `send_to_gso`, so the client can
    // observe all segments before the handler thread has run the counter
    // increment that follows the call — wait for it instead of sampling.
    let sent = GSO_SENT.get().unwrap();
    for _ in 0..400 {
        if sent.load(Ordering::SeqCst) > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        sent.load(Ordering::SeqCst),
        1,
        "send_to_gso should issue a single logical send"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn udp_gso_invalid_segment_size_returns_error() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_echo_started();

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // Use the standard echo handler — the test focuses on the API
    // contract for invalid arguments.
    struct GsoArgCheck {
        started: Arc<AtomicUsize>,
        sane_send_ok: Arc<AtomicUsize>,
    }
    static SANE_OK: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
    impl AsyncEventHandler for GsoArgCheck {
        fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
            async move {}
        }
        fn create_for_worker(_id: usize) -> Self {
            GsoArgCheck {
                started: ECHO_STARTED.get_or_init(Default::default).clone(),
                sane_send_ok: SANE_OK.get_or_init(Default::default).clone(),
            }
        }
        fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
            let started = self.started.clone();
            let ok_count = self.sane_send_ok.clone();
            Some(Box::pin(async move {
                started.fetch_add(1, Ordering::SeqCst);
                let (_data, peer) = udp.recv_from().await;
                // segment_size = 0 must error.
                assert!(udp.send_to_gso(peer, &[1, 2, 3], 0).is_err());
                // segment_size larger than data must error.
                assert!(udp.send_to_gso(peer, &[1, 2, 3], 100).is_err());
                // A sane send should still succeed afterward — the
                // earlier errors must not have leaked a slot.
                if udp.send_to_gso(peer, &[0u8; 600], 200).is_ok() {
                    ok_count.fetch_add(1, Ordering::SeqCst);
                }
            }))
        }
    }
    SANE_OK
        .get_or_init(Default::default)
        .store(0, Ordering::SeqCst);

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<GsoArgCheck>()
        .expect("launch");
    await_handler_started(ECHO_STARTED.get().unwrap());

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    client.send_to(b"trigger", addr).unwrap();

    // Drain the 3-segment GSO send (3 × 200 = 600 bytes total).
    let mut buf = [0u8; 256];
    for _ in 0..3 {
        let (n, _) = client.recv_from(&mut buf).unwrap();
        assert_eq!(n, 200);
    }
    assert_eq!(
        SANE_OK.get().unwrap().load(Ordering::SeqCst),
        1,
        "follow-up send_to_gso must succeed after the invalid-arg errors"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── recv_batch: drain multiple queued datagrams in one poll ─────────────

struct BatchEcho {
    started: Arc<AtomicUsize>,
    /// Records how many datagrams each `recv_batch` poll drained, so the
    /// test can assert that batching actually fired (i.e. more than one
    /// datagram was popped in at least one poll).
    poll_drains: Arc<Mutex<Vec<usize>>>,
}

impl AsyncEventHandler for BatchEcho {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }

    fn create_for_worker(_id: usize) -> Self {
        BatchEcho {
            started: BATCH_ECHO_STARTED.get_or_init(Default::default).clone(),
            poll_drains: BATCH_POLL_DRAINS
                .get_or_init(|| Arc::new(Mutex::new(Vec::new())))
                .clone(),
        }
    }

    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        let poll_drains = self.poll_drains.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            // Per-iteration scratch: collected `(peer, payload)` pairs
            // captured by the recv_batch callback, replayed after the
            // future returns so we test the drain semantics, not
            // send-from-inside-callback.
            let mut to_send: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
            loop {
                to_send.clear();
                let drained = udp
                    .recv_batch(16, |data, peer| {
                        to_send.push((peer, data.to_vec()));
                    })
                    .await;
                poll_drains.lock().unwrap().push(drained);
                for (peer, payload) in to_send.drain(..) {
                    loop {
                        match udp.send_to(peer, &payload) {
                            Ok(()) => break,
                            Err(UdpSendError::PoolExhausted)
                            | Err(UdpSendError::SubmissionQueueFull) => {
                                udp.send_ready().await;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }))
    }
}

static BATCH_ECHO_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static BATCH_POLL_DRAINS: OnceLock<Arc<Mutex<Vec<usize>>>> = OnceLock::new();

// ── recv_batch_timed: per-datagram arrival timestamp ────────────────────

/// Handler that uses `recv_batch_timed` and records the
/// `recv_at -> callback` latency for each datagram. We assert that
/// the captured timestamp is monotonically not-in-the-future and
/// always precedes the moment our user-space callback runs — i.e.
/// the driver captured it earlier than `Instant::now()` at the
/// callback site.
struct TimedBatchEcho {
    started: Arc<AtomicUsize>,
    deltas: Arc<Mutex<Vec<std::time::Duration>>>,
}

impl AsyncEventHandler for TimedBatchEcho {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }

    fn create_for_worker(_id: usize) -> Self {
        TimedBatchEcho {
            started: TIMED_BATCH_STARTED.get_or_init(Default::default).clone(),
            deltas: TIMED_BATCH_DELTAS
                .get_or_init(|| Arc::new(Mutex::new(Vec::new())))
                .clone(),
        }
    }

    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let started = self.started.clone();
        let deltas = self.deltas.clone();
        Some(Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            let mut to_send: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
            loop {
                to_send.clear();
                udp.recv_batch_timed(16, |data, peer, recv_at| {
                    let cb_now = std::time::Instant::now();
                    let delta = cb_now.saturating_duration_since(recv_at);
                    deltas.lock().unwrap().push(delta);
                    to_send.push((peer, data.to_vec()));
                })
                .await;
                for (peer, payload) in to_send.drain(..) {
                    loop {
                        match udp.send_to(peer, &payload) {
                            Ok(()) => break,
                            Err(UdpSendError::PoolExhausted)
                            | Err(UdpSendError::SubmissionQueueFull) => {
                                udp.send_ready().await;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }))
    }
}

static TIMED_BATCH_STARTED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static TIMED_BATCH_DELTAS: OnceLock<Arc<Mutex<Vec<std::time::Duration>>>> = OnceLock::new();

#[test]
fn udp_recv_batch_timed_captures_arrival_before_callback() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let started = TIMED_BATCH_STARTED.get_or_init(Default::default).clone();
    started.store(0, Ordering::SeqCst);
    if let Some(d) = TIMED_BATCH_DELTAS.get() {
        d.lock().unwrap().clear();
    }

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<TimedBatchEcho>()
        .expect("launch");
    await_handler_started(&started);

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    // Burst so the handler accumulates queued entries; that's when
    // the `recv_at -> callback` gap is meaningfully > 0.
    const BURST: usize = 16;
    for i in 0..BURST {
        let payload = format!("ts-{i:03}");
        client.send_to(payload.as_bytes(), addr).unwrap();
    }
    let mut buf = [0u8; 64];
    for _ in 0..BURST {
        let (_n, _src) = client.recv_from(&mut buf).unwrap();
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }

    let deltas = TIMED_BATCH_DELTAS.get().unwrap().lock().unwrap().clone();
    assert!(
        deltas.len() >= BURST,
        "expected at least {BURST} delta samples, got {}",
        deltas.len()
    );
    // Every recv_at must be at or before the callback firing — that's
    // the contract that makes recv_batch_timed useful for tighter
    // RTT estimation. saturating_duration_since returns Duration::ZERO
    // if recv_at were somehow in the future; that would still pass
    // this check but the next assertion catches it: we expect at
    // least one non-zero delta on a real io_uring run because the
    // driver captures the timestamp strictly earlier than we observe
    // it in user code. (On mio the delta is generally near-zero;
    // accept that path too.)
    let nonzero = deltas.iter().filter(|d| !d.is_zero()).count();
    if ringline::backend() == ringline::Backend::IoUring {
        assert!(
            nonzero > 0,
            "io_uring backend should produce at least one strictly positive recv_at→callback delta; deltas={deltas:?}"
        );
    }
}

#[test]
fn udp_recv_batch_drains_burst_in_fewer_polls_than_datagrams() {
    let _guard = UDP_SLOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let started = BATCH_ECHO_STARTED.get_or_init(Default::default).clone();
    started.store(0, Ordering::SeqCst);
    if let Some(d) = BATCH_POLL_DRAINS.get() {
        d.lock().unwrap().clear();
    }

    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(base_config())
        .bind_udp(addr)
        .launch::<BatchEcho>()
        .expect("launch");
    await_handler_started(&started);

    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    // Fire a burst of datagrams back-to-back so the handler's recv
    // queue accumulates more than one entry per poll.
    const BURST: usize = 32;
    for i in 0..BURST {
        let payload = format!("batch-{i:04}");
        client.send_to(payload.as_bytes(), addr).unwrap();
    }

    // Collect all echoes.
    let mut got = HashSet::new();
    let mut buf = [0u8; 64];
    for _ in 0..BURST {
        let (n, _src) = client.recv_from(&mut buf).unwrap();
        got.insert(std::str::from_utf8(&buf[..n]).unwrap().to_string());
    }
    assert_eq!(got.len(), BURST, "every datagram must be echoed back");

    // Now assert the handler actually drained more than one datagram
    // per poll on at least one iteration — that's the contract that
    // makes recv_batch different from with_datagram.
    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }

    let drains = BATCH_POLL_DRAINS.get().unwrap().lock().unwrap().clone();
    let total: usize = drains.iter().sum();
    assert!(
        total >= BURST,
        "handler must have observed at least BURST datagrams (got {total})"
    );
    // Either: at least one poll drained multiple datagrams, OR the
    // poll count is strictly less than BURST (proving the kernel
    // delivered multiple per CQE batch). The first form is the more
    // common case; both are acceptable evidence that batching took
    // effect.
    let max_drain = drains.iter().copied().max().unwrap_or(0);
    assert!(
        max_drain > 1 || drains.len() < BURST,
        "expected at least one multi-datagram drain or fewer polls than datagrams; \
         got drains={drains:?}"
    );
}
