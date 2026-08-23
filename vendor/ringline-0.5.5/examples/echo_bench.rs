#![allow(clippy::manual_async_fn)]
//! Benchmark for the async AsyncEventHandler echo server.
//! Measures throughput (ops/sec), per-operation latency histograms
//! (p50/p90/p99/p999/max), and CPU usage across a matrix of connection
//! counts and message sizes.
//!
//! Usage:
//!   cargo run --release -p ringline --example echo_bench -- [OPTIONS]
//!
//! Options:
//!   --duration <secs>    Test duration per configuration (default: 3)
//!   --workers <n>        Number of server worker threads (default: 1)
//!   --port <n>           Base port (default: 17171)
//!   --quick              Run a single config only (4 clients, 64B)

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, Config, ConfigBuilder, ConnCtx, ParseResult, RinglineBuilder};

// ── Async echo handler ──────────────────────────────────────────────

struct AsyncEcho;

impl AsyncEventHandler for AsyncEcho {
    fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
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

    fn create_for_worker(_worker_id: usize) -> Self {
        AsyncEcho
    }
}

// ── Latency histogram ───────────────────────────────────────────────

struct LatencyHistogram {
    samples: Vec<u64>, // nanoseconds per op
}

impl LatencyHistogram {
    fn new() -> Self {
        LatencyHistogram {
            samples: Vec::with_capacity(1_000_000),
        }
    }

    fn record(&mut self, ns: u64) {
        self.samples.push(ns);
    }

    fn finalize(&mut self) -> LatencyStats {
        self.samples.sort_unstable();
        let n = self.samples.len();
        if n == 0 {
            return LatencyStats {
                p50: 0,
                p90: 0,
                p99: 0,
                p999: 0,
                max: 0,
                count: 0,
            };
        }
        LatencyStats {
            p50: self.samples[n * 50 / 100],
            p90: self.samples[n * 90 / 100],
            p99: self.samples[n * 99 / 100],
            p999: self.samples[n.saturating_sub(1).min(n * 999 / 1000)],
            max: self.samples[n - 1],
            count: n as u64,
        }
    }
}

#[derive(Clone)]
#[allow(dead_code)]
struct LatencyStats {
    p50: u64,
    p90: u64,
    p99: u64,
    p999: u64,
    max: u64,
    count: u64,
}

// ── CPU measurement ─────────────────────────────────────────────────

fn process_cpu_time_ns() -> u64 {
    let stat = match std::fs::read_to_string("/proc/self/stat") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let fields: Vec<&str> = stat.split_whitespace().collect();
    if fields.len() < 15 {
        return 0;
    }
    let utime: u64 = fields[13].parse().unwrap_or(0);
    let stime: u64 = fields[14].parse().unwrap_or(0);
    let ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;
    if ticks_per_sec == 0 {
        return 0;
    }
    (utime + stime) * 1_000_000_000 / ticks_per_sec
}

// ── Client ──────────────────────────────────────────────────────────

#[allow(dead_code)]
struct ClientResult {
    ops: u64,
    histogram: LatencyHistogram,
}

fn run_client(
    addr: &str,
    msg_size: usize,
    stop: Arc<AtomicBool>,
    ops_counter: Arc<AtomicU64>,
) -> ClientResult {
    let msg = vec![0xABu8; msg_size];
    let mut recv_buf = vec![0u8; msg_size];
    let mut histogram = LatencyHistogram::new();

    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  client connect failed: {e}");
            return ClientResult { ops: 0, histogram };
        }
    };
    stream.set_nodelay(true).ok();

    let mut local_ops: u64 = 0;

    while !stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();

        // Send
        if stream.write_all(&msg).is_err() {
            break;
        }

        // Recv — read exactly msg_size bytes
        let mut total_read = 0;
        while total_read < msg_size {
            match stream.read(&mut recv_buf[total_read..]) {
                Ok(0) => {
                    return ClientResult {
                        ops: local_ops,
                        histogram,
                    };
                }
                Ok(n) => total_read += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    return ClientResult {
                        ops: local_ops,
                        histogram,
                    };
                }
            }
        }

        let elapsed_ns = t0.elapsed().as_nanos() as u64;
        histogram.record(elapsed_ns);

        local_ops += 1;
        if local_ops & 0xFF == 0 {
            ops_counter.fetch_add(256, Ordering::Relaxed);
        }
    }

    ops_counter.fetch_add(local_ops & 0xFF, Ordering::Relaxed);
    ClientResult {
        ops: local_ops,
        histogram,
    }
}

// ── Benchmark runner ────────────────────────────────────────────────

#[derive(Clone)]
#[allow(dead_code)]
struct BenchResult {
    ops_per_sec: f64,
    ns_per_op: f64,
    latency: LatencyStats,
    cpu_ns: u64,
}

fn make_config_builder(workers: usize, msg_size: usize) -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(workers)
        .pin_to_core(false)
        .sq_entries(256)
        .recv_buffer(256, msg_size.next_power_of_two().max(4096) as u32)
        .max_connections(4096)
        .send_pool(512, msg_size.next_power_of_two().max(4096) as u32)
}

fn make_config(workers: usize, msg_size: usize) -> Config {
    make_config_builder(workers, msg_size)
        .build()
        .expect("valid config")
}

fn wait_for_server(addr: &str) {
    for _ in 0..100 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not start on {addr}");
}

fn run_bench(
    addr: &str,
    num_clients: usize,
    msg_size: usize,
    warmup: Duration,
    duration: Duration,
) -> BenchResult {
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    wait_for_server(addr);

    // Spawn client threads.
    let mut client_handles = Vec::with_capacity(num_clients);
    for _ in 0..num_clients {
        let addr = addr.to_string();
        let stop = stop.clone();
        let ops = ops.clone();
        client_handles.push(std::thread::spawn(move || {
            run_client(&addr, msg_size, stop, ops)
        }));
    }

    // Warmup phase.
    std::thread::sleep(warmup);
    ops.store(0, Ordering::Relaxed);

    // Measurement phase.
    let cpu_before = process_cpu_time_ns();
    let start = Instant::now();
    std::thread::sleep(duration);
    let elapsed = start.elapsed();
    let cpu_after = process_cpu_time_ns();
    stop.store(true, Ordering::Relaxed);

    // Collect.
    let mut merged = LatencyHistogram::new();
    for h in client_handles {
        if let Ok(result) = h.join() {
            for &sample in &result.histogram.samples {
                merged.record(sample);
            }
        }
    }

    let total_ops = ops.load(Ordering::Relaxed);
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    let ns_per_op = if total_ops > 0 {
        elapsed.as_nanos() as f64 / total_ops as f64
    } else {
        0.0
    };

    BenchResult {
        ops_per_sec,
        ns_per_op,
        latency: merged.finalize(),
        cpu_ns: cpu_after.saturating_sub(cpu_before),
    }
}

// ── Formatting helpers ──────────────────────────────────────────────

fn format_size(bytes: usize) -> String {
    if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{}B", bytes)
    }
}

fn format_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else {
        format!("{}ns", ns)
    }
}

// ── Main ────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut duration_secs = 3u64;
    let mut workers = 1usize;
    let mut base_port = 17171u16;
    let mut quick = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--duration" => {
                i += 1;
                duration_secs = args[i].parse().unwrap();
            }
            "--workers" => {
                i += 1;
                workers = args[i].parse().unwrap();
            }
            "--port" => {
                i += 1;
                base_port = args[i].parse().unwrap();
            }
            "--quick" => {
                quick = true;
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let duration = Duration::from_secs(duration_secs);
    let warmup = Duration::from_secs(1);

    // Configuration matrix.
    let client_counts: Vec<usize> = if quick { vec![4] } else { vec![1, 4, 16, 64] };
    let msg_sizes: Vec<usize> = if quick {
        vec![64]
    } else {
        vec![64, 512, 4096, 32768]
    };

    eprintln!(
        "Echo benchmark: {} worker(s), {}s per config",
        workers, duration_secs
    );
    eprintln!("  clients: {:?}", client_counts);
    eprintln!(
        "  sizes:   {:?}",
        msg_sizes
            .iter()
            .map(|s| format_size(*s))
            .collect::<Vec<_>>()
    );
    eprintln!();

    struct Row {
        clients: usize,
        msg_size: usize,
        result: BenchResult,
    }

    let mut results: Vec<Row> = Vec::new();
    let mut port_offset = 0u16;

    for &clients in &client_counts {
        for &msg_size in &msg_sizes {
            let port = base_port + port_offset;
            port_offset += 1;

            let addr = format!("127.0.0.1:{port}");

            eprintln!(
                "  {clients} clients x {}: on :{port}",
                format_size(msg_size)
            );

            let config = make_config(workers, msg_size);
            let (shutdown, handles) = RinglineBuilder::new(config)
                .bind(addr.parse().expect("invalid bind address"))
                .launch::<AsyncEcho>()
                .expect("failed to launch server");

            let bench_result = run_bench(&addr, clients, msg_size, warmup, duration);

            shutdown.shutdown();
            for h in handles {
                h.join().ok();
            }

            std::thread::sleep(Duration::from_millis(50));

            eprintln!(
                "    {:>9.0} ops/s  p50: {}  p99: {}",
                bench_result.ops_per_sec,
                format_ns(bench_result.latency.p50),
                format_ns(bench_result.latency.p99),
            );

            results.push(Row {
                clients,
                msg_size,
                result: bench_result,
            });
        }
    }

    // ── Summary table ───────────────────────────────────────────────
    eprintln!();
    eprintln!("## Results");
    eprintln!();
    eprintln!(
        "| Clients | MsgSize | ops/s      | p50        | p90        | p99        | p999       | max        |"
    );
    eprintln!(
        "|---------|---------|------------|------------|------------|------------|------------|------------|"
    );

    for row in &results {
        eprintln!(
            "| {:>7} | {:>7} | {:>10.0} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} |",
            row.clients,
            format_size(row.msg_size),
            row.result.ops_per_sec,
            format_ns(row.result.latency.p50),
            format_ns(row.result.latency.p90),
            format_ns(row.result.latency.p99),
            format_ns(row.result.latency.p999),
            format_ns(row.result.latency.max),
        );
    }

    // ── CPU usage ───────────────────────────────────────────────────
    eprintln!();
    eprintln!("## CPU Usage (process total, test interval)");
    eprintln!();
    for row in &results {
        let label = format!("{}c x {}", row.clients, format_size(row.msg_size));
        eprintln!(
            "  {label:>20}  {:.1}ms",
            row.result.cpu_ns as f64 / 1_000_000.0,
        );
    }
}
