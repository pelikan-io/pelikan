extern crate self as metriken;

pub use metriken_0_9::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[metric(
        name = "metriken_compat_counter",
        description = "compatibility-shim test counter"
    )]
    static TEST_COUNTER: Counter = Counter::new();

    #[metric(name = "metriken_compat_gauge")]
    static TEST_GAUGE: Gauge = Gauge::new();

    #[metric(name = "metriken_compat_histogram")]
    static TEST_HISTOGRAM: AtomicHistogram = AtomicHistogram::new(7, 32);

    #[test]
    fn reexports_existing_metric_api() {
        TEST_COUNTER.increment();
        TEST_GAUGE.set(7);
        TEST_HISTOGRAM.increment(3).unwrap();

        assert_eq!(TEST_COUNTER.value(), 1);
        assert_eq!(TEST_GAUGE.value(), 7);
        assert!(TEST_HISTOGRAM.load().is_some());

        let metric = metrics()
            .static_metrics()
            .iter()
            .find(|metric| metric.name() == "metriken_compat_counter")
            .unwrap();
        assert_eq!(
            metric.formatted(Format::Prometheus),
            "metriken_compat_counter"
        );

        let _: Lazy<usize> = Lazy::new(|| 1);
    }
}
