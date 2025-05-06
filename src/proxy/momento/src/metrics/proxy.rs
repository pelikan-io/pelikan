use std::sync::Arc;

use super::ConnectionGuard;
use goodmetrics::SumHandle;

use super::{RpcCallGuard, RpcMetrics};

pub trait ProxyMetrics: Clone + Send + Sync + 'static {
    fn increment_total_requests(&self);
    fn begin_connection(&self) -> ConnectionGuard;
    fn begin_get(&self) -> RpcCallGuard;
    fn begin_set(&self) -> RpcCallGuard;
    fn begin_delete(&self) -> RpcCallGuard;
}

#[derive(Clone, Debug)]
pub struct DefaultProxyMetrics {
    pub(crate) total_requests: SumHandle,
    pub(crate) get: RpcMetrics,
    pub(crate) set: RpcMetrics,
    pub(crate) delete: RpcMetrics,
    pub(crate) current_connections: SumHandle,
}

impl ProxyMetrics for DefaultProxyMetrics {
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

impl ProxyMetrics for Arc<DefaultProxyMetrics> {
    fn begin_connection(&self) -> ConnectionGuard {
        self.as_ref().begin_connection()
    }

    fn increment_total_requests(&self) {
        self.as_ref().increment_total_requests()
    }

    fn begin_get(&self) -> RpcCallGuard {
        self.as_ref().begin_get()
    }

    fn begin_set(&self) -> RpcCallGuard {
        self.as_ref().begin_set()
    }

    fn begin_delete(&self) -> RpcCallGuard {
        self.as_ref().begin_delete()
    }
}
