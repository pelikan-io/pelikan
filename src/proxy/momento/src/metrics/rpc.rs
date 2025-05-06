use std::{
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
};

use clocksource::coarse::Instant;
use goodmetrics::{GaugeFactory, HistogramHandle};

use super::util::{
    proxy_request_latency_error_histogram, proxy_request_latency_ok_histogram,
    proxy_request_latency_timeout_histogram,
};

#[derive(Clone, Debug)]
pub struct RpcMetrics {
    latency_ok: HistogramHandle,
    latency_error: HistogramHandle,
    latency_timeout: HistogramHandle,
}

impl RpcMetrics {
    pub fn new(gauge_factory: &GaugeFactory, rpc: &'static str) -> Self {
        Self {
            latency_ok: proxy_request_latency_ok_histogram(gauge_factory, rpc),
            latency_error: proxy_request_latency_error_histogram(gauge_factory, rpc),
            latency_timeout: proxy_request_latency_timeout_histogram(gauge_factory, rpc),
        }
    }

    pub fn record_api_call(&self) -> RpcCallGuard {
        RpcCallGuard::new(
            self.latency_ok.clone(),
            self.latency_error.clone(),
            self.latency_timeout.clone(),
        )
    }
}

pub struct RpcCallGuard {
    start_time: Instant,
    latency_ok: HistogramHandle,
    latency_error: HistogramHandle,
    latency_timeout: HistogramHandle,
    recorded: AtomicBool,
}

impl RpcCallGuard {
    pub fn new(
        latency_ok: HistogramHandle,
        latency_error: HistogramHandle,
        latency_timeout: HistogramHandle,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            latency_ok,
            latency_error,
            latency_timeout,
            recorded: AtomicBool::new(false),
        }
    }

    pub fn complete_ok(&mut self) {
        if let Ok(false) =
            self.recorded
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        {
            self.latency_ok
                .observe(self.start_time.elapsed().as_nanos() as i64);
        }
    }

    pub fn complete_error(&mut self) {
        if let Ok(false) =
            self.recorded
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        {
            self.latency_error
                .observe(self.start_time.elapsed().as_nanos() as i64);
        }
    }

    pub fn complete<T, E>(&mut self, result: &Result<T, E>) {
        match result {
            Ok(_) => self.complete_ok(),
            Err(_) => self.complete_error(),
        };
    }
}

impl Drop for RpcCallGuard {
    fn drop(&mut self) {
        if let Ok(false) =
            self.recorded
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        {
            self.latency_timeout
                .observe(self.start_time.elapsed().as_nanos() as i64);
        }
    }
}

pub async fn with_rpc_call_guard<T, E, F>(mut recorder: RpcCallGuard, fut: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let result = fut.await;
    recorder.complete(&result);
    result
}
