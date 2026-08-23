use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;

// ── Timer benchmarks ─────────────────────────────────────────────────

fn bench_std_sleep_1ms(c: &mut Criterion) {
    c.bench_function("std_sleep_1ms", |b| {
        b.iter(|| {
            std::thread::sleep(Duration::from_millis(1));
        });
    });
}

fn bench_std_sleep_10ms(c: &mut Criterion) {
    c.bench_function("std_sleep_10ms", |b| {
        b.iter(|| {
            std::thread::sleep(Duration::from_millis(10));
        });
    });
}

fn bench_std_sleep_100ms(c: &mut Criterion) {
    c.bench_function("std_sleep_100ms", |b| {
        b.iter(|| {
            std::thread::sleep(Duration::from_millis(100));
        });
    });
}

fn bench_instant_now(c: &mut Criterion) {
    c.bench_function("instant_now", |b| {
        b.iter(|| {
            let _ = std::time::Instant::now();
        });
    });
}

fn bench_duration_since(c: &mut Criterion) {
    let start = std::time::Instant::now();
    c.bench_function("duration_since", |b| {
        b.iter(|| {
            let _ = start.elapsed();
        });
    });
}

criterion_group!(
    benches,
    bench_std_sleep_1ms,
    bench_std_sleep_10ms,
    bench_std_sleep_100ms,
    bench_instant_now,
    bench_duration_since
);
criterion_main!(benches);
