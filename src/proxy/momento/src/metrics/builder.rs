use goodmetrics::{
    default_gauge_factory,
    downstream::{get_client, OpenTelemetryDownstream, OpentelemetryBatcher},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use super::{proxy::DefaultProxyMetrics, util::proxy_sum_gauge, RpcMetrics};

pub struct ProxyMetricsBuilder {
    otel_endpoint: String,
    batch_interval: Duration,
    batch_capacity: usize,
}

impl ProxyMetricsBuilder {
    pub fn new<S: Into<String>>(otel_endpoint: S) -> Self {
        Self {
            otel_endpoint: otel_endpoint.into(),
            batch_interval: Duration::from_secs(1),
            batch_capacity: 128,
        }
    }

    pub async fn build(self) -> Arc<DefaultProxyMetrics> {
        let (batch_sender, batch_receiver) = mpsc::channel(self.batch_capacity);
        let gauge_factory = default_gauge_factory();

        // Set up the OTLP downstream
        let channel = get_client(
            self.otel_endpoint.as_str(),
            || None,
            goodmetrics::proto::opentelemetry::collector::metrics::v1::metrics_service_client::MetricsServiceClient::with_origin
        ).expect("connect to otel-collector");

        let otlp_downstream = OpenTelemetryDownstream::new(channel, None);
        tokio::spawn(otlp_downstream.send_batches_forever(batch_receiver));

        // Set up the OpenTelemetry batcher
        tokio::spawn(gauge_factory.clone().report_gauges_forever(
            self.batch_interval,
            batch_sender,
            OpentelemetryBatcher,
        ));

        let metrics = DefaultProxyMetrics {
            total_requests: proxy_sum_gauge(gauge_factory, "total_requests"),
            get: RpcMetrics::new(gauge_factory, "get"),
            set: RpcMetrics::new(gauge_factory, "set"),
            delete: RpcMetrics::new(gauge_factory, "delete"),
            current_connections: proxy_sum_gauge(gauge_factory, "current_connections"),
        };

        Arc::new(metrics)
    }
}
