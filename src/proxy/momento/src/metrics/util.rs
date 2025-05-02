use goodmetrics::{GaugeDimensions, GaugeFactory, HistogramHandle, SumHandle};

pub fn proxy_sum_gauge(g: &GaugeFactory, name: &'static str) -> SumHandle {
    g.dimensioned_gauge_sum("momento_proxy", name, Default::default())
}

pub fn proxy_request_ok_gauge(g: &GaugeFactory, rpc: &'static str) -> SumHandle {
    g.dimensioned_gauge_sum("momento_proxy", "Ok", GaugeDimensions::new([("rpc", rpc)]))
}

pub fn proxy_request_error_gauge(g: &GaugeFactory, rpc: &'static str) -> SumHandle {
    g.dimensioned_gauge_sum(
        "momento_proxy",
        "Error",
        GaugeDimensions::new([("rpc", rpc)]),
    )
}

pub fn proxy_request_latency_gauge(g: &GaugeFactory, rpc: &'static str) -> HistogramHandle {
    g.dimensioned_gauge_histogram(
        "momento_proxy",
        "latency",
        GaugeDimensions::new([("rpc", rpc)]),
    )
}
