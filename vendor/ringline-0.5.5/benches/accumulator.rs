//! Accumulator recv-parse cycle benchmarks.
//!
//! Models the two `with_bytes` access patterns whose copy behavior matters:
//!
//! - `pipelined`: a batch of B small responses lands in one recv; the parser
//!   consumes ONE response per take/put-back cycle (the fire/recv client
//!   pattern). Cost of interest: what happens to the (B-i) unconsumed
//!   responses on each cycle.
//! - `streaming`: one large value arrives in K chunks; every chunk triggers a
//!   take that parses to NeedMore until the last chunk completes it. Cost of
//!   interest: what happens to the accumulated prefix on each chunk.
//!
//! Uses the crate-internal `AccumulatorTable` API via the `testing` feature
//! re-export.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use ringline::bench_internal::AccumulatorTable;

/// B responses of `resp_len` bytes each land at once; consume one per cycle.
fn pipelined_cycles(table: &mut AccumulatorTable, batch: usize, resp_len: usize) {
    let payload = vec![0x2au8; batch * resp_len];
    assert!(table.append(0, &payload));

    for _ in 0..batch {
        let frozen = table.take_frozen(0);
        // "Parse" one response: consume resp_len, keep a slice like a real
        // parser keeps the value, put the remainder back.
        let value = frozen.slice(0..resp_len);
        black_box(&value);
        let remainder = frozen.slice(resp_len..);
        if !remainder.is_empty() {
            table.put_back(0, remainder);
        }
    }
    assert!(table.data(0).is_empty());
}

/// One `total` byte value arrives in `chunks` appends; each append is
/// followed by a take that fails to parse (NeedMore) until complete.
fn streaming_cycles(table: &mut AccumulatorTable, total: usize, chunks: usize) {
    let chunk = vec![0x2au8; total / chunks];
    for i in 0..chunks {
        assert!(table.append(0, &chunk));
        let frozen = table.take_frozen(0);
        if i + 1 < chunks {
            // NeedMore — put everything back.
            black_box(frozen.len());
            table.put_back(0, frozen);
        } else {
            black_box(frozen.len());
        }
    }
    assert!(table.data(0).is_empty());
}

fn bench_pipelined(c: &mut Criterion) {
    let mut group = c.benchmark_group("accumulator_pipelined");
    for &(batch, resp_len) in &[(16usize, 64usize), (64, 64), (256, 64), (64, 1024)] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(format!("b{batch}_r{resp_len}"), |b| {
            let mut table = AccumulatorTable::new(1, 16 * 1024);
            b.iter(|| pipelined_cycles(&mut table, batch, resp_len));
        });
    }
    group.finish();
}

fn bench_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("accumulator_streaming");
    for &(total, chunks) in &[
        (64usize * 1024, 4usize),
        (256 * 1024, 16),
        (1024 * 1024, 64),
    ] {
        group.throughput(Throughput::Bytes(total as u64));
        group.bench_function(format!("{}k_c{}", total / 1024, chunks), |b| {
            let mut table = AccumulatorTable::new(1, 16 * 1024);
            b.iter(|| streaming_cycles(&mut table, total, chunks));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_pipelined, bench_streaming);
criterion_main!(benches);
