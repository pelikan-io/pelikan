// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::num::ParseIntError;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::RngCore;
use rand::SeedableRng;
use storage_types::*;

use std::time::Duration;

// A very fast PRNG which is appropriate for testing
pub fn rng() -> impl RngCore {
    rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(0)
}

fn parse_redis_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_signed_redis");
    group.measurement_time(Duration::from_secs(30));


    let bytes = b"9223372036854775807";
    group.throughput(Throughput::Elements(1));
    group.bench_with_input("parse_numbers", &bytes, |b, &bytes| {
        b.iter(|| {
            let result = parse_signed_redis(bytes);
            if (result.is_none()) {
                println!("it's empty!")
            }
        })
    });

    let string = "9223372036854775807";
    group.bench_with_input("parse_numbers_std_lib", &string, |b, &bytes| {
        b.iter(|| {
            let result: Result<i64, ParseIntError> = bytes.parse();
            if (result.is_err()) {
                println!("it's empty!")
            }
        })
    });
}


criterion_group!(benches, parse_redis_benchmark);
criterion_main!(benches);
