use metriken::*;

pub static PERCENTILES: &[(&str, f64)] = &[
    ("p25", 25.0),
    ("p50", 50.0),
    ("p75", 75.0),
    ("p90", 90.0),
    ("p99", 99.0),
    ("p999", 99.9),
    ("p9999", 99.99),
];

#[metric(name = "admin_request_parse")]
pub static ADMIN_REQUEST_PARSE: Counter = Counter::new();

#[metric(name = "admin_response_compose")]
pub static ADMIN_RESPONSE_COMPOSE: Counter = Counter::new();

#[metric(name = "backend_request")]
pub static BACKEND_REQUEST: Counter = Counter::new();

#[metric(name = "backend_ex")]
pub static BACKEND_EX: Counter = Counter::new();

#[metric(name = "backend_ex_rate_limited")]
pub static BACKEND_EX_RATE_LIMITED: Counter = Counter::new();

#[metric(name = "backend_ex_timeout")]
pub static BACKEND_EX_TIMEOUT: Counter = Counter::new();

#[metric(name = "ru_utime")]
pub static RU_UTIME: Counter = Counter::new();

#[metric(name = "ru_stime")]
pub static RU_STIME: Counter = Counter::new();

#[metric(name = "ru_maxrss")]
pub static RU_MAXRSS: Gauge = Gauge::new();

#[metric(name = "ru_ixrss")]
pub static RU_IXRSS: Gauge = Gauge::new();

#[metric(name = "ru_idrss")]
pub static RU_IDRSS: Gauge = Gauge::new();

#[metric(name = "ru_isrss")]
pub static RU_ISRSS: Gauge = Gauge::new();

#[metric(name = "ru_minflt")]
pub static RU_MINFLT: Counter = Counter::new();

#[metric(name = "ru_majflt")]
pub static RU_MAJFLT: Counter = Counter::new();

#[metric(name = "ru_nswap")]
pub static RU_NSWAP: Counter = Counter::new();

#[metric(name = "ru_inblock")]
pub static RU_INBLOCK: Counter = Counter::new();

#[metric(name = "ru_oublock")]
pub static RU_OUBLOCK: Counter = Counter::new();

#[metric(name = "ru_msgsnd")]
pub static RU_MSGSND: Counter = Counter::new();

#[metric(name = "ru_msgrcv")]
pub static RU_MSGRCV: Counter = Counter::new();

#[metric(name = "ru_nsignals")]
pub static RU_NSIGNALS: Counter = Counter::new();

#[metric(name = "ru_nvcsw")]
pub static RU_NVCSW: Counter = Counter::new();

#[metric(name = "ru_nivcsw")]
pub static RU_NIVCSW: Counter = Counter::new();

mod builder;
mod connection;
mod proxy;
mod rpc;
pub mod util;

pub use builder::ProxyMetricsBuilder;
pub use connection::ConnectionGuard;
pub use proxy::{ProxyMetrics, ProxyMetricsApi};
pub use rpc::{with_rpc_call_guard, RpcCallGuard, RpcMetrics};
