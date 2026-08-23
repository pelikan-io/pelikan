use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ringline::{AsyncEventHandler, Config, ConnCtx, ParseResult, RinglineBuilder};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// ── Ringline echo server handler ─────────────────────────────────────

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

fn start_ringline_server(addr: SocketAddr) -> thread::JoinHandle<()> {
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

fn start_tokio_server(addr: SocketAddr) -> thread::JoinHandle<()> {
    let listener = TcpListener::bind(addr).unwrap();
    let _addr = listener.local_addr().unwrap();
    let running = Arc::new(AtomicBool::new(true));
    let running_thread = running.clone();

    thread::spawn(move || {
        while running_thread.load(Ordering::Relaxed) {
            if let Ok((mut stream, _)) = listener.accept() {
                thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = stream.read(&mut buf) {
                        if n == 0 {
                            break;
                        }
                        stream.write_all(&buf[..n]).ok();
                    }
                });
            }
        }
    })
}

// ── Echo benchmarks (single connection) ──────────────────────────────

fn bench_ringline_echo_64b(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29600".parse().unwrap();
    let _server = start_ringline_server(addr);

    // Warmup
    for _ in 0..100 {
        let mut stream = TcpStream::connect(addr).unwrap();
        let data = b"Hello, world!";
        stream.write_all(data).unwrap();
        let mut buf = [0u8; 1024];
        stream.read_exact(&mut buf[..data.len()]).unwrap();
    }

    c.bench_function("ringline_echo_64b", |b| {
        b.iter(|| {
            let mut stream = TcpStream::connect(addr).unwrap();
            let data = b"Hello, world!";
            stream.write_all(data).unwrap();
            let mut buf = [0u8; 1024];
            stream.read_exact(&mut buf[..data.len()]).unwrap();
            black_box(buf);
        });
    });
}

fn bench_tokio_echo_64b(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29601".parse().unwrap();
    let _server = start_tokio_server(addr);

    // Warmup
    for _ in 0..100 {
        let mut stream = TcpStream::connect(addr).unwrap();
        let data = b"Hello, world!";
        stream.write_all(data).unwrap();
        let mut buf = [0u8; 1024];
        stream.read_exact(&mut buf[..data.len()]).unwrap();
    }

    c.bench_function("tokio_echo_64b", |b| {
        b.iter(|| {
            let mut stream = TcpStream::connect(addr).unwrap();
            let data = b"Hello, world!";
            stream.write_all(data).unwrap();
            let mut buf = [0u8; 1024];
            stream.read_exact(&mut buf[..data.len()]).unwrap();
            black_box(buf);
        });
    });
}

fn bench_ringline_echo_4kb(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29602".parse().unwrap();
    let _server = start_ringline_server(addr);

    let data = vec![0u8; 4096];

    // Warmup
    for _ in 0..100 {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(&data).unwrap();
        let mut buf = vec![0u8; 4096];
        stream.read_exact(&mut buf).unwrap();
    }

    c.bench_function("ringline_echo_4kb", |b| {
        b.iter(|| {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream.write_all(&data).unwrap();
            let mut buf = vec![0u8; 4096];
            stream.read_exact(&mut buf).unwrap();
            black_box(buf);
        });
    });
}

fn bench_tokio_echo_4kb(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29603".parse().unwrap();
    let _server = start_tokio_server(addr);

    let data = vec![0u8; 4096];

    // Warmup
    for _ in 0..100 {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(&data).unwrap();
        let mut buf = vec![0u8; 4096];
        stream.read_exact(&mut buf).unwrap();
    }

    c.bench_function("tokio_echo_4kb", |b| {
        b.iter(|| {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream.write_all(&data).unwrap();
            let mut buf = vec![0u8; 4096];
            stream.read_exact(&mut buf).unwrap();
            black_box(buf);
        });
    });
}

// ── Pipeline benchmarks (multiple requests) ──────────────────────────

fn bench_ringline_pipeline_100(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29604".parse().unwrap();
    let _server = start_ringline_server(addr);

    let requests = vec![b"req1"; 100];

    // Warmup
    for _ in 0..10 {
        let mut stream = TcpStream::connect(addr).unwrap();
        for req in &requests {
            stream.write_all(*req).unwrap();
        }
        let mut buf = [0u8; 1024];
        for _ in 0..requests.len() {
            let _ = stream.read(&mut buf).unwrap();
        }
    }

    c.bench_function("ringline_pipeline_100", |b| {
        b.iter(|| {
            let mut stream = TcpStream::connect(addr).unwrap();
            for req in &requests {
                stream.write_all(*req).unwrap();
            }
            let mut buf = [0u8; 1024];
            for _ in 0..requests.len() {
                let _ = stream.read(&mut buf).unwrap();
            }
        });
    });
}

fn bench_tokio_pipeline_100(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29605".parse().unwrap();
    let _server = start_tokio_server(addr);

    let requests = vec![b"req1"; 100];

    // Warmup
    for _ in 0..10 {
        let mut stream = TcpStream::connect(addr).unwrap();
        for req in &requests {
            stream.write_all(*req).unwrap();
        }
        let mut buf = [0u8; 1024];
        for _ in 0..requests.len() {
            let _ = stream.read(&mut buf).unwrap();
        }
    }

    c.bench_function("tokio_pipeline_100", |b| {
        b.iter(|| {
            let mut stream = TcpStream::connect(addr).unwrap();
            for req in &requests {
                stream.write_all(*req).unwrap();
            }
            let mut buf = [0u8; 1024];
            for _ in 0..requests.len() {
                let _ = stream.read(&mut buf).unwrap();
            }
        });
    });
}

// ── Throughput benchmarks (measuring ops/sec) ────────────────────────

fn bench_ringline_throughput(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29606".parse().unwrap();
    let _server = start_ringline_server(addr);

    let data = vec![0u8; 1024];
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    // Run a warmup client
    let stop_warmup = stop.clone();
    let ops_warmup = ops.clone();
    let data_warmup = data.clone();
    thread::spawn(move || {
        while !stop_warmup.load(Ordering::Relaxed) {
            if let Ok(mut stream) = TcpStream::connect(addr) {
                let start = Instant::now();
                let mut local_ops = 0u64;
                while start.elapsed() < Duration::from_secs(1) {
                    if stream.write_all(&data_warmup).is_err() {
                        break;
                    }
                    let mut buf = vec![0u8; data_warmup.len()];
                    if stream.read_exact(&mut buf).is_err() {
                        break;
                    }
                    local_ops += 1;
                }
                ops_warmup.fetch_add(local_ops, Ordering::Relaxed);
            }
        }
    });

    thread::sleep(Duration::from_secs(2));
    ops.store(0, Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(100));

    // Measure throughput
    let stop_measure = stop.clone();
    let ops_measure = ops.clone();
    let data_measure = data.clone();

    c.bench_function("ringline_throughput", |b| {
        b.iter(|| {
            stop_measure.store(false, Ordering::Relaxed);
            ops_measure.store(0, Ordering::Relaxed);

            let stop_clone = stop_measure.clone();
            let ops_clone = ops_measure.clone();
            let data_clone = data_measure.clone();

            let handle = thread::spawn(move || {
                while !stop_clone.load(Ordering::Relaxed) {
                    if let Ok(mut stream) = TcpStream::connect(addr) {
                        let mut local_ops = 0u64;
                        while local_ops < 1000 {
                            if stream.write_all(&data_clone).is_err() {
                                break;
                            }
                            let mut buf = vec![0u8; data_clone.len()];
                            if stream.read_exact(&mut buf).is_err() {
                                break;
                            }
                            local_ops += 1;
                        }
                        ops_clone.fetch_add(local_ops, Ordering::Relaxed);
                    }
                }
            });

            thread::sleep(Duration::from_secs(1));
            stop_measure.store(true, Ordering::Relaxed);
            handle.join().ok();

            // Return ops/sec
            let total_ops = ops_measure.load(Ordering::Relaxed) as f64;
            black_box(total_ops);
        });
    });

    stop.store(true, Ordering::Relaxed);
}

fn bench_tokio_throughput(c: &mut Criterion) {
    let addr: SocketAddr = "127.0.0.1:29607".parse().unwrap();
    let _server = start_tokio_server(addr);

    let data = vec![0u8; 1024];
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    // Run a warmup client
    let stop_warmup = stop.clone();
    let ops_warmup = ops.clone();
    let data_warmup = data.clone();
    thread::spawn(move || {
        while !stop_warmup.load(Ordering::Relaxed) {
            if let Ok(mut stream) = TcpStream::connect(addr) {
                let start = Instant::now();
                let mut local_ops = 0u64;
                while start.elapsed() < Duration::from_secs(1) {
                    if stream.write_all(&data_warmup).is_err() {
                        break;
                    }
                    let mut buf = vec![0u8; data_warmup.len()];
                    if stream.read_exact(&mut buf).is_err() {
                        break;
                    }
                    local_ops += 1;
                }
                ops_warmup.fetch_add(local_ops, Ordering::Relaxed);
            }
        }
    });

    thread::sleep(Duration::from_secs(2));
    ops.store(0, Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(100));

    // Measure throughput
    let stop_measure = stop.clone();
    let ops_measure = ops.clone();
    let data_measure = data.clone();

    c.bench_function("tokio_throughput", |b| {
        b.iter(|| {
            stop_measure.store(false, Ordering::Relaxed);
            ops_measure.store(0, Ordering::Relaxed);

            let stop_clone = stop_measure.clone();
            let ops_clone = ops_measure.clone();
            let data_clone = data_measure.clone();

            let handle = thread::spawn(move || {
                while !stop_clone.load(Ordering::Relaxed) {
                    if let Ok(mut stream) = TcpStream::connect(addr) {
                        let mut local_ops = 0u64;
                        while local_ops < 1000 {
                            if stream.write_all(&data_clone).is_err() {
                                break;
                            }
                            let mut buf = vec![0u8; data_clone.len()];
                            if stream.read_exact(&mut buf).is_err() {
                                break;
                            }
                            local_ops += 1;
                        }
                        ops_clone.fetch_add(local_ops, Ordering::Relaxed);
                    }
                }
            });

            thread::sleep(Duration::from_secs(1));
            stop_measure.store(true, Ordering::Relaxed);
            handle.join().ok();

            // Return ops/sec
            let total_ops = ops_measure.load(Ordering::Relaxed) as f64;
            black_box(total_ops);
        });
    });

    stop.store(true, Ordering::Relaxed);
}

criterion_group!(
    benches,
    bench_ringline_echo_64b,
    bench_tokio_echo_64b,
    bench_ringline_echo_4kb,
    bench_tokio_echo_4kb,
    bench_ringline_pipeline_100,
    bench_tokio_pipeline_100,
    bench_ringline_throughput,
    bench_tokio_throughput
);

criterion_main!(benches);
