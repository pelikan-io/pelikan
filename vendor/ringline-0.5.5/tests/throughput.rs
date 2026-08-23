#![allow(clippy::manual_async_fn)]
//! Throughput regression tests for TCP and UDP.
//!
//! Each test runs the same workload through ringline and through a
//! plain `std::net` server in a separate thread, then compares the
//! wall-clock time. The std::net path is the baseline — ringline must
//! be in the same ballpark, otherwise we've regressed below the
//! "vanilla blocking" implementation, which would make the runtime
//! pointless.
//!
//! These tests aren't precise benchmarks (kernel scheduling, CI
//! variance, build profile), so the assertions are loose: ringline must
//! complete and not be dramatically slower than std::net. The numbers
//! are also printed via `eprintln!` so a human reading test output can
//! spot drift.

use ringline::ConfigBuilder;
use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures_util::{AsyncReadExt, AsyncWriteExt};
use ringline::{
    AsyncEventHandler, Config, ConnCtx, ConnStream, RinglineBuilder, UdpCtx, UdpSendError,
};

// ── Shared test config ────────────────────────────────────────────────

fn test_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        // Sized so an 8 MiB sustained echo fits without stalling on
        // resource limits — the slot count covers the worst-case in-flight
        // pipeline given default 16 KiB slots, the SQ holds enough entries
        // to keep io_uring busy, and the recv buffer ring matches.
        .sq_entries(2048)
        .recv_buffer(512, 16 * 1024)
        .max_connections(64)
        .send_pool(2048, 16384)
        .standalone_task_capacity(32)
}

fn test_config() -> Config {
    test_config_builder().build().expect("valid config")
}

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

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_for_tcp(addr: &str) {
    for _ in 0..200 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("TCP server did not come up at {addr}");
}

// ── TCP echo through std::net (baseline) ───────────────────────────────

/// Run a chunked echo against a `std::net` server thread. Returns
/// elapsed time + bytes received.
fn tcp_echo_round_trip_std(payload: &[u8], chunk_size: usize) -> (Duration, Vec<u8>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("std listen");
    let addr = listener.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    // Trivial echo server: read up to N bytes, write them back, repeat
    // until EOF.
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("std accept");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let mut buf = vec![0u8; 64 * 1024];
        while !stop_clone.load(Ordering::Relaxed) {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if stream.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let addr_str = addr.to_string();
    let result = run_tcp_echo_chunked(&addr_str, payload, chunk_size);
    stop.store(true, Ordering::Relaxed);
    let _ = server.join();
    result
}

/// Chunked request-reply driver: writes `chunk_size` bytes, reads the
/// echo back, repeats until the full payload has round-tripped.
/// Keeping at most one chunk in flight stops either side's TCP buffers
/// from filling, which is the symptom of the `EAGAIN`-drops-queue
/// limitation in the runtime that an unbounded fire-and-forget writer
/// would hit.
fn run_tcp_echo_chunked(addr: &str, payload: &[u8], chunk_size: usize) -> (Duration, Vec<u8>) {
    let mut client = TcpStream::connect(addr).expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    client.set_nodelay(true).ok();

    let payload_len = payload.len();
    let mut received = Vec::with_capacity(payload_len);
    let mut buf = vec![0u8; chunk_size];
    let started = Instant::now();
    let mut sent = 0;
    while sent < payload_len {
        let end = (sent + chunk_size).min(payload_len);
        client.write_all(&payload[sent..end]).expect("write");
        let want = end - sent;
        let mut got = 0;
        while got < want {
            match client.read(&mut buf[got..want]) {
                Ok(0) => panic!("unexpected EOF"),
                Ok(n) => {
                    received.extend_from_slice(&buf[got..got + n]);
                    got += n;
                }
                Err(e) => panic!("read error: {e}"),
            }
        }
        sent = end;
    }
    let elapsed = started.elapsed();
    client.shutdown(std::net::Shutdown::Both).ok();
    (elapsed, received)
}

// ── TCP echo through ringline ──────────────────────────────────────────

/// Echoes via [`ConnStream`] so write calls block on send-pool
/// availability rather than silently dropping bytes — required for
/// sustained-throughput round-trips. (`with_data` + `send_nowait`
/// loses bytes when the pool fills, which is fine for tiny payloads
/// but breaks 8 MiB transfers.)
struct AsyncTcpEcho;

impl AsyncEventHandler for AsyncTcpEcho {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let mut stream = ConnStream::new(conn);
            let mut buf = vec![0u8; 32 * 1024];
            loop {
                let n = match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                if stream.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        AsyncTcpEcho
    }
}

/// Run the same workload through a ringline server. Returns the same
/// (duration, received) tuple so callers can compare to the std::net
/// baseline.
fn tcp_echo_round_trip_ringline(payload: &[u8], chunk_size: usize) -> (Duration, Vec<u8>) {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let bind: SocketAddr = addr.parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(bind)
        .launch::<AsyncTcpEcho>()
        .expect("ringline launch");
    wait_for_tcp(&addr);

    let result = run_tcp_echo_chunked(&addr, payload, chunk_size);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    result
}

#[test]
fn tcp_throughput_round_trip_vs_std_net() {
    // Chunked request-reply: write 64 KiB, read 64 KiB back, repeat
    // 128 times = 8 MiB total. With at most one chunk in flight on the
    // wire neither side's TCP buffers fill, which keeps the workload
    // inside ringline's happy path. (Fire-and-forget large writes hit
    // a separate limitation: when the kernel returns `EAGAIN` on send
    // because the send buffer filled, `handle_send` currently drops
    // the connection's pending queue instead of awaiting socket
    // writability. The right fix is multishot poll-on-POLLOUT + retry,
    // tracked separately. This test exercises the throughput path we
    // can compare apples-to-apples against std::net today.)
    let total_bytes = 8 * 1024 * 1024;
    let chunk_size = 64 * 1024;
    let payload: Vec<u8> = (0..total_bytes).map(|i| (i & 0xFF) as u8).collect();

    // Run ringline first to warm caches; debug-built test binaries
    // pay a notable cost on the first launch.
    let (rl_dur, rl_recv) = tcp_echo_round_trip_ringline(&payload, chunk_size);
    assert_eq!(
        rl_recv.len(),
        payload.len(),
        "ringline TCP echo length mismatch"
    );
    assert_eq!(rl_recv, payload, "ringline TCP echo payload mismatch");

    let (std_dur, std_recv) = tcp_echo_round_trip_std(&payload, chunk_size);
    assert_eq!(std_recv.len(), payload.len(), "std::net length mismatch");
    assert_eq!(std_recv, payload, "std::net payload mismatch");

    eprintln!(
        "TCP {} MiB echo (chunked {} KiB): ringline={:?} ({:.0} MB/s), \
         std::net={:?} ({:.0} MB/s), ratio={:.2}x",
        total_bytes / 1024 / 1024,
        chunk_size / 1024,
        rl_dur,
        total_bytes as f64 / 1e6 / rl_dur.as_secs_f64(),
        std_dur,
        total_bytes as f64 / 1e6 / std_dur.as_secs_f64(),
        rl_dur.as_secs_f64() / std_dur.as_secs_f64()
    );

    // Generous bound — we're flagging regressions, not benchmarking.
    // Ringline being more than 5× slower than blocking std::net means
    // something got broken.
    assert!(
        rl_dur < std_dur * 5,
        "ringline TCP echo is way slower than std::net baseline: \
         ringline={rl_dur:?}, std::net={std_dur:?}"
    );
}

// ── UDP echo through std::net (baseline) ───────────────────────────────

/// Run `count` synchronous request-reply round-trips against a
/// `std::net` UDP server thread. Returns elapsed time. Synchronous
/// (one-in-flight) so neither side's UDP socket buffer overruns —
/// this measures *per-round-trip latency* dominated by syscall + ack
/// overhead, which is what the runtime actually controls.
fn udp_request_reply_std(count: usize, payload_size: usize) -> Duration {
    let server = UdpSocket::bind("127.0.0.1:0").expect("std udp bind");
    let server_addr = server.local_addr().unwrap();
    server.set_nonblocking(true).ok();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    let server_thread = std::thread::spawn(move || {
        let mut buf = vec![0u8; 65536];
        while !stop_clone.load(Ordering::Relaxed) {
            match server.recv_from(&mut buf) {
                Ok((n, peer)) => {
                    let _ = server.send_to(&buf[..n], peer);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_micros(50));
                }
                Err(_) => break,
            }
        }
    });

    let elapsed = run_udp_request_reply(server_addr, count, payload_size);
    stop.store(true, Ordering::Relaxed);
    let _ = server_thread.join();
    elapsed
}

// ── UDP echo through ringline ──────────────────────────────────────────

struct AsyncUdpEcho;

impl AsyncEventHandler for AsyncUdpEcho {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {}
    }
    fn create_for_worker(_id: usize) -> Self {
        AsyncUdpEcho
    }
    fn on_udp_bind(&self, udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async move {
            UDP_HANDLER_STARTED.fetch_add(1, Ordering::SeqCst);
            loop {
                let (data, peer) = udp.recv_from().await;
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

static UDP_HANDLER_STARTED: AtomicUsize = AtomicUsize::new(0);

fn udp_request_reply_ringline(count: usize, payload_size: usize) -> Duration {
    UDP_HANDLER_STARTED.store(0, Ordering::SeqCst);
    let port = free_udp_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let cfg = test_config_builder()
        .udp_send_slots(64)
        .udp_recv_queue_capacity(4096)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(cfg)
        .bind_udp(addr)
        .launch::<AsyncUdpEcho>()
        .expect("ringline udp launch");

    for _ in 0..400 {
        if UDP_HANDLER_STARTED.load(Ordering::SeqCst) > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        UDP_HANDLER_STARTED.load(Ordering::SeqCst) > 0,
        "ringline UDP handler did not start"
    );

    let elapsed = run_udp_request_reply(addr, count, payload_size);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    elapsed
}

/// Synchronous request-reply pump: send one datagram, recv its reply,
/// repeat. Avoids kernel-buffer overruns (each side has at most one
/// in-flight datagram) so loss is ~zero on loopback and the timing
/// reflects per-round-trip handler turnaround time.
fn run_udp_request_reply(server_addr: SocketAddr, count: usize, payload_size: usize) -> Duration {
    let client = UdpSocket::bind("127.0.0.1:0").expect("udp client bind");
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let mut payload = vec![0u8; payload_size];
    let mut buf = vec![0u8; payload_size + 64];
    let started = Instant::now();
    for i in 0..count {
        payload[..4].copy_from_slice(&(i as u32).to_le_bytes());
        client.send_to(&payload, server_addr).expect("send");
        let (_n, _src) = client.recv_from(&mut buf).expect("recv");
    }
    started.elapsed()
}

#[test]
fn udp_throughput_request_reply_vs_std_net() {
    // 1 000 synchronous round-trips of 256-byte datagrams. With one
    // datagram in flight at a time neither kernel queue fills, so
    // delivery is reliable and the timing measures per-round-trip
    // handler overhead end-to-end.
    let count = 1_000;
    let payload_size = 256;

    let rl_dur = udp_request_reply_ringline(count, payload_size);
    let std_dur = udp_request_reply_std(count, payload_size);

    eprintln!(
        "UDP {count} × {payload_size}B req/reply: \
         ringline={rl_dur:?} ({:.0} rtt/s), \
         std::net={std_dur:?} ({:.0} rtt/s), \
         ratio={:.2}x",
        count as f64 / rl_dur.as_secs_f64(),
        count as f64 / std_dur.as_secs_f64(),
        rl_dur.as_secs_f64() / std_dur.as_secs_f64()
    );

    // Hard floor on completing the workload at all.
    assert!(
        rl_dur < Duration::from_secs(30),
        "ringline UDP req/reply took too long: {rl_dur:?} for {count} round-trips"
    );
    // Loose bound — single-flight UDP req/reply on loopback is dominated
    // by kernel syscall overhead; both implementations should be in
    // the same order of magnitude.
    assert!(
        rl_dur < std_dur * 5,
        "ringline UDP req/reply is way slower than std::net: \
         ringline={rl_dur:?}, std::net={std_dur:?}"
    );
}

// ── TCP fire-and-forget large transfer (EAGAIN backpressure path) ──────

/// Push the entire payload in one direction without waiting for echo
/// chunks, which forces the kernel TCP send buffer to fill (the peer
/// drains in the background). The runtime must arm `POLLOUT` and
/// retransparently retry sends that returned `-EAGAIN`; if it dropped
/// the queue (the old behavior), the receiver gets fewer bytes than
/// were sent.
fn tcp_fire_and_forget_ringline(payload: &[u8]) -> Vec<u8> {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let bind: SocketAddr = addr.parse().unwrap();

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(bind)
        .launch::<AsyncTcpEcho>()
        .expect("ringline launch");
    wait_for_tcp(&addr);

    let mut client = TcpStream::connect(&addr).expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    client.set_nodelay(true).ok();

    // Writer: shoves the full payload in; blocks on kernel send buffer.
    let writer = {
        let mut writer = client.try_clone().expect("clone");
        let payload = payload.to_vec();
        std::thread::spawn(move || {
            writer.write_all(&payload).expect("write");
            writer.shutdown(std::net::Shutdown::Write).ok();
        })
    };

    // Reader: drain echo until the server FINs.
    let mut received = Vec::with_capacity(payload.len());
    let mut buf = vec![0u8; 64 * 1024];
    while received.len() < payload.len() {
        match client.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => received.extend_from_slice(&buf[..n]),
            Err(e) => panic!("read error: {e}"),
        }
    }
    writer.join().unwrap();
    drop(client);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    received
}

#[test]
fn tcp_fire_and_forget_8mib_survives_kernel_buffer_full() {
    // 8 MiB unbounded write through one connection. The kernel TCP
    // send buffer (≈200 KiB by default) fills well before the writer
    // finishes, so the server's send CQEs return `-EAGAIN`. A correct
    // runtime arms `POLLOUT`, waits for writability, and retries the
    // send; ringline's previous behavior dropped the connection's
    // queued sends on EAGAIN, which would show up here as the receiver
    // getting < 8 MiB before the FIN.
    let payload: Vec<u8> = (0..8 * 1024 * 1024).map(|i| (i & 0xFF) as u8).collect();
    let received = tcp_fire_and_forget_ringline(&payload);
    assert_eq!(
        received.len(),
        payload.len(),
        "fire-and-forget echo lost data: got {} bytes, expected {}",
        received.len(),
        payload.len()
    );
    assert_eq!(received, payload, "echoed payload mismatch");

    // We don't assert `SEND_EAGAIN` ticked: with the close-time
    // deferral the kernel often manages to drain everything without
    // hitting `-EAGAIN` in the first place. The metric is still
    // wired (and exercised by tighter tests in the runtime crate
    // when we can force the kernel buffer to fill) but the
    // user-visible regression here is "data is preserved", not "the
    // EAGAIN-retry path fires".
}
