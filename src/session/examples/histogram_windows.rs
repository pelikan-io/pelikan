//! Deterministic window-boundary sensitivity, not a live latency measurement.
//! cargo run --release -p session --example histogram_windows
use metriken::histogram::Histogram;

fn window(period_ms: i64, burst_offset_ms: i64, cutoff_delay_ms: i64) -> Histogram {
    let mut merged = Histogram::new(7, 32).unwrap();
    let burst_start = period_ms + burst_offset_ms;
    for worker in 0..4 {
        // Same delay at both ends gives each worker an equal-duration window.
        let shift = cutoff_delay_ms * worker / 3;
        let mut local = Histogram::new(7, 32).unwrap();
        for t in shift..period_ms + shift {
            let latency_us = if (burst_start..burst_start + 20).contains(&t) {
                10_000
            } else {
                100
            };
            // Constant 100k observations/s/worker; bins represent microseconds.
            local.add(latency_us, 100).unwrap();
        }
        for (a, b) in merged.as_mut_slice().iter_mut().zip(local.as_slice()) {
            *a += *b;
        }
    }
    assert_eq!(
        merged.as_slice().iter().sum::<u64>(),
        period_ms as u64 * 400
    );
    merged
}

fn quantiles(histogram: &Histogram) -> Vec<u64> {
    histogram
        .quantiles(&[0.5, 0.99, 0.999])
        .unwrap()
        .unwrap()
        .entries()
        .values()
        .map(|bucket| bucket.end())
        .collect()
}

fn main() {
    println!("period_ms,burst_offset_ms,cutoff_delay_ms,delivery_delay_ms,truth_p50_us,truth_p99_us,truth_p999_us,observed_p50_us,observed_p99_us,observed_p999_us");
    for period in [1000, 5000] {
        for offset in [-20, -10, 0, 10, 20] {
            let truth = window(period, offset, 0);
            let tq = quantiles(&truth);
            // Delivery lag alone preserves the distribution and all quantiles.
            println!(
                "{period},{offset},0,20,{},{},{},{},{},{}",
                tq[0], tq[1], tq[2], tq[0], tq[1], tq[2]
            );
            for skew in [1, 5, 20] {
                let observed = window(period, offset, skew);
                let oq = quantiles(&observed);
                println!(
                    "{period},{offset},{skew},20,{},{},{},{},{},{}",
                    tq[0], tq[1], tq[2], oq[0], oq[1], oq[2]
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_cutoffs_merge_to_the_exact_window() {
        let histogram = window(1000, -20, 0);
        let q = quantiles(&histogram);
        assert_eq!(q[0], 100);
        assert!(q[1] >= 10_000);
    }

    #[test]
    fn boundary_shift_can_change_p99_without_changing_sample_count() {
        let truth = window(1000, -10, 0);
        let observed = window(1000, -10, 20);
        // Truth contains exactly 1% slow samples. The shifted worker windows
        // include more of the boundary burst and move p99 into the slow bin.
        assert_eq!(quantiles(&truth)[1], 100);
        assert!(quantiles(&observed)[1] >= 10_000);
    }

    #[test]
    fn longer_windows_dilute_the_same_burst() {
        assert!(quantiles(&window(1000, -20, 0))[1] >= 10_000);
        assert_eq!(quantiles(&window(5000, -20, 0))[1], 100);
        assert!(quantiles(&window(5000, -20, 0))[2] >= 10_000);
    }
}
