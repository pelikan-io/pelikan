use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use ringline::{AsyncEventHandler, Config, ConnCtx, ParseResult, RinglineBuilder};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

// ── Echo server handler ──────────────────────────────────────────────

#[allow(clippy::manual_async_fn)]
struct EchoHandler;

#[allow(clippy::manual_async_fn)]
impl AsyncEventHandler for EchoHandler {
    fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
        async move {
            loop {
                let n = conn
                    .with_data(|data| {
                        conn.send_nowait(data).ok();
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
        EchoHandler
    }
}

// ── Buffer size benchmarks ───────────────────────────────────────────

fn bench_ringline_buffer_sizes(c: &mut Criterion) {
    // Server ports must stay BELOW Linux's default ephemeral-port range
    // (/proc/sys/net/ipv4/ip_local_port_range, typically 32768-60999).
    // Each iteration does tens of thousands of client TCP connects from
    // this same process; their source ports get drawn from the
    // ephemeral range and linger in TIME_WAIT for ~60s after close. If
    // a server port happens to fall in that range, a later iteration
    // trying to bind() it sees EADDRINUSE — Linux's SO_REUSEADDR
    // doesn't help here because the conflicting TIME_WAIT sockets are
    // owned by the client side (`TcpStream::connect`), which doesn't
    // set the option.
    //
    // The previous scheme `base_port + size` put four of six ports
    // inside the ephemeral range (33024, 36096, 48384, and the
    // wrap-to-32000 case at size=65536), and the bench reliably hung
    // at iteration 3 once #181 unblocked the underlying shutdown.
    //
    // 29800..29806 keeps every port well below the ephemeral range and
    // doesn't collide with the ports used by connect.rs (29500-29501)
    // or echo.rs (29600-29607).
    let sizes = [64, 256, 1024, 4096, 16384, 65536];
    const PORT_BASE: u16 = 29800;

    for (idx, size) in sizes.iter().copied().enumerate() {
        let data = vec![0u8; size];
        let port = PORT_BASE + idx as u16;
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // ── Ringline server benchmark ──────────────────────────────
        let config = Config::default();
        let (shutdown, handles) = RinglineBuilder::new(config)
            .bind(addr)
            .launch::<EchoHandler>()
            .unwrap_or_else(|e| panic!("Failed to start server on {}: {:?}", addr, e));

        // Give server time to start
        thread::sleep(Duration::from_millis(500));

        // Warmup
        for _ in 0..100 {
            let mut stream = std::net::TcpStream::connect(addr)
                .unwrap_or_else(|_| panic!("Failed to connect to {}", addr));
            stream
                .write_all(&data)
                .unwrap_or_else(|_| panic!("Failed to write to {}", addr));
            let mut buf = vec![0u8; size];
            stream
                .read_exact(&mut buf)
                .unwrap_or_else(|_| panic!("Failed to read from {}", addr));
        }

        let mut group = c.benchmark_group(format!("ringline_buffer_{}", size));
        group.bench_function(BenchmarkId::from_parameter("ringline_server"), |b| {
            b.iter(|| {
                let mut stream = std::net::TcpStream::connect(addr)
                    .unwrap_or_else(|_| panic!("Failed to connect to {}", addr));
                stream
                    .write_all(&data)
                    .unwrap_or_else(|_| panic!("Failed to write to {}", addr));
                let mut buf = vec![0u8; size];
                stream
                    .read_exact(&mut buf)
                    .unwrap_or_else(|_| panic!("Failed to read from {}", addr));
                black_box(buf);
            });
        });
        group.finish();

        // Give server time to shut down
        drop(shutdown);
        for h in handles {
            h.join().ok();
        }
        thread::sleep(Duration::from_millis(500));
    }
}

criterion_group!(benches, bench_ringline_buffer_sizes);
criterion_main!(benches);
