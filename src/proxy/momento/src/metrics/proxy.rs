use std::sync::Arc;

use super::ConnectionGuard;
use goodmetrics::SumHandle;

use super::{RpcCallGuard, RpcMetrics};

pub trait ProxyMetrics: Clone + Send + Sync + 'static {
    fn begin_connection(&self) -> ConnectionGuard;
    fn begin_get(&self) -> RpcCallGuard;
    fn begin_set(&self) -> RpcCallGuard;
    fn begin_delete(&self) -> RpcCallGuard;
    fn begin_unimplemented(&self) -> RpcCallGuard;
}

#[derive(Clone, Debug)]
pub struct DefaultProxyMetrics {
    pub(crate) get: RpcMetrics,
    pub(crate) set: RpcMetrics,
    pub(crate) delete: RpcMetrics,
    pub(crate) unimplemented: RpcMetrics,
    pub(crate) current_connections: SumHandle,
}

impl ProxyMetrics for DefaultProxyMetrics {
    fn begin_connection(&self) -> ConnectionGuard {
        ConnectionGuard::new(self.current_connections.clone())
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

    fn begin_unimplemented(&self) -> RpcCallGuard {
        self.unimplemented.record_api_call()
    }
}

impl ProxyMetrics for Arc<DefaultProxyMetrics> {
    fn begin_connection(&self) -> ConnectionGuard {
        self.as_ref().begin_connection()
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

    fn begin_unimplemented(&self) -> RpcCallGuard {
        self.as_ref().begin_unimplemented()
    }
}
