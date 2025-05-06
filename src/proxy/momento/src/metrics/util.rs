use goodmetrics::{GaugeDimensions, GaugeFactory, HistogramHandle, SumHandle};

pub fn proxy_sum_gauge(g: &GaugeFactory, name: &'static str) -> SumHandle {
    g.dimensioned_gauge_sum("momento_proxy", name, Default::default())
}

pub fn proxy_request_ok_gauge(g: &GaugeFactory, rpc: &'static str) -> SumHandle {
    g.dimensioned_gauge_sum(
        "momento_proxy",
        "momento_request",
        GaugeDimensions::new([("rpc", rpc), ("result", "ok")]),
    )
}

pub fn proxy_request_error_gauge(g: &GaugeFactory, rpc: &'static str) -> SumHandle {
    g.dimensioned_gauge_sum(
        "momento_proxy",
        "momento_request",
        GaugeDimensions::new([("rpc", rpc), ("result", "error")]),
    )
}

pub fn proxy_request_latency_gauge(g: &GaugeFactory, rpc: &'static str) -> HistogramHandle {
    g.dimensioned_gauge_histogram(
        "momento_proxy",
        "latency",
        GaugeDimensions::new([("rpc", rpc)]),
    )
}
