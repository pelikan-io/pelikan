use goodmetrics::{GaugeDimensions, GaugeFactory, HistogramHandle, SumHandle};

pub fn proxy_sum_gauge(g: &GaugeFactory, name: &'static str) -> SumHandle {
    g.dimensioned_gauge_sum("momento_proxy", name, Default::default())
}

fn proxy_request_latency_histogram(
    gauge_factory: &GaugeFactory,
    rpc: &'static str,
    result: &'static str,
) -> HistogramHandle {
    gauge_factory.dimensioned_gauge_histogram(
        "momento_proxy",
        "latency",
        GaugeDimensions::new([("rpc", rpc), ("result", result)]),
    )
}

pub fn proxy_request_latency_ok_histogram(g: &GaugeFactory, rpc: &'static str) -> HistogramHandle {
    proxy_request_latency_histogram(g, rpc, "ok")
}

pub fn proxy_request_latency_error_histogram(
    gauge_factory: &GaugeFactory,
    rpc: &'static str,
) -> HistogramHandle {
    proxy_request_latency_histogram(gauge_factory, rpc, "error")
}

pub fn proxy_request_latency_timeout_histogram(
    gauge_factory: &GaugeFactory,
    rpc: &'static str,
) -> HistogramHandle {
    proxy_request_latency_histogram(gauge_factory, rpc, "timeout")
}
