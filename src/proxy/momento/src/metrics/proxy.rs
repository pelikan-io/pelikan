use super::ConnectionGuard;
use goodmetrics::SumHandle;

use super::{RpcCallGuard, RpcMetrics};

pub trait ProxyMetricsApi: Send + Sync + 'static {
    fn increment_total_requests(&self);
    fn begin_connection(&self) -> ConnectionGuard;
    fn begin_get(&self) -> RpcCallGuard;
    fn begin_set(&self) -> RpcCallGuard;
    fn begin_delete(&self) -> RpcCallGuard;
}

#[derive(Clone, Debug)]
pub struct ProxyMetrics {
    pub(crate) total_requests: SumHandle,
    pub(crate) get: RpcMetrics,
    pub(crate) set: RpcMetrics,
    pub(crate) delete: RpcMetrics,
    pub(crate) current_connections: SumHandle,
}

impl ProxyMetricsApi for ProxyMetrics {
    fn begin_connection(&self) -> ConnectionGuard {
        ConnectionGuard::new(self.current_connections.clone())
    }

    fn increment_total_requests(&self) {
        self.total_requests.observe(1);
    }

    fn begin_get(&self) -> RpcCallGuard {
        self.get.record_api_call()
    }

    fn begin_set(&self) -> RpcCallGuard {
        self.set.record_api_call()
    }

    fn begin_delete(&self) -> RpcCallGuard {
        self.delete.record_api_call()
    }
}
