use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ringline::{AsyncEventHandler, Config, ConnCtx, ParseResult, RinglineBuilder};
use std::net::{SocketAddr, TcpStream};
// use std::sync::Arc;
// use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

// ── Echo server handler ──────────────────────────────────────────────

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

fn start_server(addr: SocketAddr) -> thread::JoinHandle<()> {
    let config = Config::default();
    let (_shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr)
        .launch::<EchoHandler>()
        .unwrap();

    thread::spawn(move || {
        for h in handles {
            h.join().ok();
        }
    })
}

// ── Connect benchmarks ───────────────────────────────────────────────

fn bench_connect(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29500".parse().unwrap();
    let _server = start_server(addr);

    // Warmup
    for _ in 0..100 {
        let _ = TcpStream::connect(addr).unwrap();
    }

    c.bench_function("connect", |b| {
        b.iter(|| {
            let _ = TcpStream::connect(addr).unwrap();
        });
    });
}

fn bench_connect_with_timeout(c: &mut Criterion) {
    let unreachable_addr: SocketAddr = "192.0.2.1:12345".parse().unwrap(); // TEST-NET-1

    c.bench_function("connect_with_timeout", |b| {
        b.iter(|| {
            let _ = TcpStream::connect_timeout(&unreachable_addr, Duration::from_millis(100));
        });
    });
}

fn bench_connect_concurrent(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29501".parse().unwrap();
    let _server = start_server(addr);

    let mut group = c.benchmark_group("connect_concurrent");
    for num_concurrent in [1, 10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("connect_concurrent", num_concurrent),
            &num_concurrent,
            |b, &n| {
                b.iter(|| {
                    let mut handles = Vec::new();
                    for _ in 0..n {
                        let connect_addr = addr;
                        let handle = thread::spawn(move || {
                            let _ = TcpStream::connect(connect_addr);
                        });
                        handles.push(handle);
                    }
                    for h in handles {
                        h.join().ok();
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_connect,
    bench_connect_with_timeout,
    bench_connect_concurrent
);
criterion_main!(benches);
