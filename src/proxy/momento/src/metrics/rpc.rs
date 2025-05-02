use std::future::Future;

use goodmetrics::{GaugeFactory, HistogramHandle, SumHandle, TimeGuard};

use super::util::{proxy_request_error_gauge, proxy_request_latency_gauge, proxy_request_ok_gauge};

#[derive(Clone, Debug)]
pub struct RpcMetrics {
    calls_ok: SumHandle,
    calls_error: SumHandle,
    request_latency: HistogramHandle,
}

impl RpcMetrics {
    pub fn new(gauge_factory: &GaugeFactory, rpc: &'static str) -> Self {
        Self {
            calls_ok: proxy_request_ok_gauge(gauge_factory, rpc),
            calls_error: proxy_request_error_gauge(gauge_factory, rpc),
            request_latency: proxy_request_latency_gauge(gauge_factory, rpc),
        }
    }

    pub fn record_api_call(&self) -> RpcCallGuard {
        RpcCallGuard::new(
            self.request_latency.time(),
            self.calls_ok.clone(),
            self.calls_error.clone(),
        )
    }
}

pub struct RpcCallGuard {
    _latency: TimeGuard,
    calls_ok: SumHandle,
    calls_error: SumHandle,
}

impl RpcCallGuard {
    pub fn new(latency: TimeGuard, calls_ok: SumHandle, calls_error: SumHandle) -> Self {
        Self {
            _latency: latency,
            calls_ok,
            calls_error,
        }
    }

    pub fn complete_ok(&self) {
        self.calls_ok.observe(1);
    }

    pub fn complete_error(&self) {
        self.calls_error.observe(1);
    }

    pub fn complete<T, E>(&self, result: &Result<T, E>) {
        match result {
            Ok(_) => self.complete_ok(),
            Err(_) => self.complete_error(),
        };
    }
}

pub async fn with_rpc_call_guard<T, E, F>(recorder: RpcCallGuard, fut: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let result = fut.await;
    recorder.complete(&result);
    result
}
