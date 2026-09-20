//! Linux microbenchmark; no production histogram implementation is changed.
//! cargo run --release -p session --example histogram_scaling -- 750 5
//! Pinning defaults match the documented i5-13500H; override HIST_CPUS and
//! HIST_COLLECTOR_CPU for another host. CSV goes to stdout.
use metriken::histogram::{AtomicHistogram, Histogram};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
fn pin(cpu: usize) {
    assert!(cpu < libc::CPU_SETSIZE as usize);
    // SAFETY: initialized cpu_set_t, validated CPU index, current thread only.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set),
            0,
            "affinity failed: {}",
            std::io::Error::last_os_error()
        );
    }
}
#[cfg(not(target_os = "linux"))]
fn pin(_: usize) {
    panic!("this benchmark requires Linux CPU affinity");
}

#[cfg(target_os = "linux")]
fn cpu_ns() -> u64 {
    // SAFETY: valid timespec output pointer; thread CPU clock has no ownership.
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        assert_eq!(
            libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts),
            0
        );
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    }
}
#[cfg(not(target_os = "linux"))]
fn cpu_ns() -> u64 {
    0
}

fn empty() -> Histogram {
    Histogram::new(7, 32).unwrap()
}
fn merge(to: &mut Histogram, from: &Histogram) {
    for (a, b) in to.as_mut_slice().iter_mut().zip(from.as_slice()) {
        *a += *b;
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Shared,
    Sharded,
    Local,
}
#[derive(Debug)]
struct Publication {
    histogram: Histogram,
    at: Instant,
}

fn run(
    mode: Mode,
    broad: bool,
    cpus: &[usize],
    collector_cpu: usize,
    duration: Duration,
    collect: bool,
    repetition: usize,
) {
    let n = cpus.len();
    let shards: Arc<Vec<_>> = Arc::new(
        (0..if matches!(mode, Mode::Shared) { 1 } else { n })
            .map(|_| AtomicHistogram::new(7, 32))
            .collect(),
    );
    let epoch = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(n + 1));
    let mut endpoints = Vec::new();
    let mut collector_endpoints = Vec::new();
    for _ in 0..n {
        let (publish, receive) = mpsc::sync_channel::<Publication>(1);
        let (recycle, spare) = mpsc::sync_channel::<Histogram>(1);
        recycle.send(empty()).unwrap();
        endpoints.push((publish, spare));
        collector_endpoints.push((receive, recycle));
    }
    // Pregenerated identical inputs: no RNG cost in the measured loop.
    let values: Arc<Vec<u64>> = Arc::new(
        (0..4096_u64)
            .map(|i| {
                if broad {
                    let x = i.wrapping_mul(0x9e3779b97f4a7c15).rotate_left(17);
                    let shift = 8 + (x % 24);
                    (1_u64 << shift) + ((x >> 8) & ((1_u64 << shift) - 1))
                } else {
                    10_000 + i % 8
                }
            })
            .collect(),
    );
    let mut workers = Vec::new();
    for (id, (publish, spare)) in endpoints.into_iter().enumerate() {
        let (shards, epoch, start, values) =
            (shards.clone(), epoch.clone(), start.clone(), values.clone());
        let cpu = cpus[id];
        workers.push(std::thread::spawn(move || {
            pin(cpu);
            let mut active = empty();
            let mut seen = 0;
            let mut count = 0_u64;
            let shard = &shards[if matches!(mode, Mode::Shared) { 0 } else { id }];
            start.wait();
            let began = Instant::now();
            let deadline = began + duration;
            while Instant::now() < deadline {
                // Same batch/epoch-check frequency for all three variants.
                let requested = epoch.load(Ordering::Relaxed);
                if matches!(mode, Mode::Local) && requested != seen {
                    if let Ok(mut next) = spare.try_recv() {
                        std::mem::swap(&mut next, &mut active);
                        publish
                            .try_send(Publication {
                                histogram: next,
                                at: Instant::now(),
                            })
                            .unwrap();
                        seen = requested;
                    }
                }
                for j in 0..256 {
                    let value = black_box(values[(count as usize + j) & 4095]);
                    match mode {
                        Mode::Local => active.increment(value).unwrap(),
                        _ => shard.increment(value).unwrap(),
                    }
                }
                count += 256;
            }
            let elapsed = began.elapsed();
            // Final handoff is outside the timed recording loop. It may wait.
            if matches!(mode, Mode::Local) {
                publish
                    .send(Publication {
                        histogram: active,
                        at: Instant::now(),
                    })
                    .unwrap();
            }
            (count, elapsed.as_secs_f64())
        }));
    }
    let collector = {
        let (shards, epoch, start, stop) =
            (shards.clone(), epoch.clone(), start.clone(), stop.clone());
        std::thread::spawn(move || {
            pin(collector_cpu);
            let mut total = empty();
            let mut sweeps = 0_u64;
            let mut collection_ns = 0_u64;
            let mut max_age_us = 0_u128;
            let mut publications = 0_u64;
            start.wait();
            let cpu_start = cpu_ns();
            loop {
                // Even with collection disabled, local final handoffs must drain.
                std::thread::sleep(Duration::from_millis(10));
                let done = stop.load(Ordering::Acquire);
                let begin = Instant::now();
                if collect {
                    epoch.fetch_add(1, Ordering::Relaxed);
                }
                if matches!(mode, Mode::Local) {
                    for (receive, recycle) in &collector_endpoints {
                        while let Ok(mut item) = receive.try_recv() {
                            max_age_us = max_age_us.max(item.at.elapsed().as_micros());
                            merge(&mut total, &item.histogram);
                            publications += 1;
                            item.histogram.as_mut_slice().fill(0);
                            let _ = recycle.try_send(item.histogram);
                        }
                    }
                } else if collect || done {
                    total.as_mut_slice().fill(0);
                    for shard in shards.iter() {
                        merge(&mut total, &shard.load().unwrap_or_else(empty));
                    }
                }
                if collect || done {
                    black_box(total.quantiles(&[0.5, 0.99, 0.999]).unwrap());
                    collection_ns += begin.elapsed().as_nanos() as u64;
                    sweeps += 1;
                }
                if done {
                    break;
                }
            }
            (
                total,
                sweeps,
                collection_ns,
                cpu_ns() - cpu_start,
                max_age_us,
                publications,
            )
        })
    };
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    stop.store(true, Ordering::Release);
    let (total, sweeps, collection_ns, collector_cpu_ns, max_age_us, publications) =
        collector.join().unwrap();
    let recorded: u64 = results.iter().map(|r| r.0).sum();
    assert_eq!(
        total.as_slice().iter().sum::<u64>(),
        recorded,
        "lost or duplicated samples"
    );
    let mut expected = empty();
    for (count, _) in &results {
        for (i, value) in values.iter().enumerate() {
            expected
                .add(*value, count / 4096 + u64::from((i as u64) < count % 4096))
                .unwrap();
        }
    }
    assert_eq!(
        total.as_slice(),
        expected.as_slice(),
        "bucket counts differ"
    );
    let seconds = results.iter().map(|r| r.1).fold(0.0, f64::max);
    let ns_per_record = results.iter().map(|r| r.1 * 1e9 / r.0 as f64).sum::<f64>() / n as f64;
    let histogram_bytes = total.as_slice().len() * 8 + std::mem::size_of::<Histogram>();
    // Algorithmic histogram storage, excluding channels, allocator overhead,
    // inputs, and benchmark-only unused objects. Atomic collection needs a
    // merged result plus one temporary load; local needs two buffers per worker
    // plus the merged result. Sequential collection reuses the temporary space.
    let atomic_bytes = total.as_slice().len() * 8 + std::mem::size_of::<AtomicHistogram>();
    let resident = match mode {
        Mode::Shared => atomic_bytes + 2 * histogram_bytes,
        Mode::Sharded => n * atomic_bytes + 2 * histogram_bytes,
        Mode::Local => (2 * n + 1) * histogram_bytes,
    };
    println!("{mode:?},{},{n},{collect},{repetition},{recorded},{:.3},{ns_per_record:.3},{sweeps},{:.3},{:.3},{max_age_us},{publications},{histogram_bytes},{resident}",
        if broad { "broad" } else { "concentrated" }, recorded as f64 / seconds / 1e6,
        collection_ns as f64 / sweeps as f64 / 1000.0, collector_cpu_ns as f64 / 1e6);
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let ms: u64 = args.get(1).map(|x| x.parse().unwrap()).unwrap_or(750);
    let repetitions: usize = args.get(2).map(|x| x.parse().unwrap()).unwrap_or(5);
    assert!(ms > 0 && repetitions > 0);
    let cpus: Vec<usize> = std::env::var("HIST_CPUS")
        .unwrap_or("0,2,4,6".into())
        .split(',')
        .map(|x| x.parse().unwrap())
        .collect();
    assert!(cpus.len() >= 4);
    let collector = std::env::var("HIST_COLLECTOR_CPU")
        .unwrap_or("8".into())
        .parse()
        .unwrap();
    assert!(!cpus.contains(&collector));
    println!("mode,distribution,workers,collect,repetition,records,mrecords_per_s,worker_ns_per_record,sweeps,mean_collection_us,collector_cpu_ms,max_publication_age_us,publications,histogram_bytes,resident_histogram_bytes");
    for repetition in 0..repetitions {
        for n in [1, 2, 4] {
            for broad in [false, true] {
                for collect in [false, true] {
                    // Rotate variant order to reduce systematic ordering bias.
                    let modes = [Mode::Shared, Mode::Sharded, Mode::Local];
                    for offset in 0..3 {
                        run(
                            modes[(offset + repetition) % 3],
                            broad,
                            &cpus[..n],
                            collector,
                            Duration::from_millis(ms),
                            collect,
                            repetition,
                        );
                    }
                }
            }
        }
    }
}
