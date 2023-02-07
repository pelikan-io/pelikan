// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::num::ParseIntError;
use storage_types::*;

use std::time::Duration;

fn parse_redis_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_signed_redis");
    group.measurement_time(Duration::from_secs(2));

    let examples = vec![
        ("max_value", "9223372036854775807"),
        ("min_value", "-9223372036854775807"),
        ("average_value 6 bytes", "123456"),
        ("average_value 7 bytes", "1234567"),
        ("average_value 8 bytes", "12345678"),
        ("zero_value", "0"),
    ];

    //passing cases
    group.throughput(Throughput::Elements(1));
    for (label, value) in examples {
        let bytes = value.as_bytes();
        group.bench_with_input(format!("parse i64: {}", label), &bytes, |b, &bytes| {
            b.iter(|| {
                let result = parse_signed_redis(bytes);
                assert!(result.is_some());
            })
        });
    }

    let examples = vec![("overflowed_value", "92233720368547758079223372036854775807")];
    //non-passing cases
    group.throughput(Throughput::Elements(1));
    for (label, value) in examples {
        let bytes = value.as_bytes();
        group.bench_with_input(
            format!("parse (failed) i64: {}", label),
            &bytes,
            |b, &bytes| {
                b.iter(|| {
                    let result = parse_signed_redis(bytes);
                    assert!(result.is_none());
                })
            },
        );
    }
}

criterion_group!(benches, parse_redis_benchmark);
criterion_main!(benches);
