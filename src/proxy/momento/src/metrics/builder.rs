use goodmetrics::{
    default_gauge_factory,
    downstream::{get_client, OpenTelemetryDownstream, OpentelemetryBatcher},
    pipeline::DimensionPosition,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_rustls::rustls::RootCertStore;
use tonic::metadata::MetadataValue;

use super::{proxy::DefaultProxyMetrics, util::proxy_sum_gauge, RpcMetrics};

pub struct ProxyMetricsBuilder {
    batch_interval: Duration,
    batch_capacity: usize,
}

impl ProxyMetricsBuilder {
    pub fn new() -> Self {
        Self {
            batch_interval: Duration::from_secs(1),
            batch_capacity: 128,
        }
    }

    pub async fn build(self) -> Arc<DefaultProxyMetrics> {
        let (batch_sender, batch_receiver) = mpsc::channel(self.batch_capacity);
        let gauge_factory = default_gauge_factory();

        let endpoint = get_environment_variable_or_none("MOMENTO_PROXY_OTLP_ENDPOINT");
        let api_token = get_environment_variable_or_none("MOMENTO_PROXY_OTLP_API_TOKEN");

        if endpoint.is_some() && api_token.is_some() {
            info!("Configuring OTLP downstream with provided endpoint and API token");

            // Set up the OTLP downstream
            let channel = get_client(
                endpoint.as_ref().unwrap(),
                || {
                    Some(RootCertStore {
                        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec()
                    })
                },
                goodmetrics::proto::opentelemetry::collector::metrics::v1::metrics_service_client::MetricsServiceClient::with_origin
            ).expect("Failed to create client");

            // Set up the OTLP downstream
            let otlp_downstream = OpenTelemetryDownstream::new_with_dimensions(
                channel,
                Some((
                    "api-token",
                    MetadataValue::try_from(api_token.as_ref().unwrap()).unwrap(),
                )),
                get_chronosphere_base_environment_dimensions(),
            );
            tokio::spawn(otlp_downstream.send_batches_forever(batch_receiver));

            // Set up the OpenTelemetry batcher
            tokio::spawn(gauge_factory.clone().report_gauges_forever(
                self.batch_interval,
                batch_sender,
                OpentelemetryBatcher,
            ));
        } else if endpoint.is_none() {
            info!("OTLP endpoint not provided: not configuring OTLP downstream. Set the MOMENTO_PROXY_OTLP_ENDPOINT environment variable to configure.");
        } else {
            info!("OTLP API token not provided, not configuring OTLP downstream. Set the MOMENTO_PROXY_OTLP_API_TOKEN environment variable to configure.");
        }

        let metrics = DefaultProxyMetrics {
            get: RpcMetrics::new(gauge_factory, "get"),
            set: RpcMetrics::new(gauge_factory, "set"),
            delete: RpcMetrics::new(gauge_factory, "delete"),
            unimplemented: RpcMetrics::new(gauge_factory, "unimplemented"),
            current_connections: proxy_sum_gauge(gauge_factory, "current_connections"),
        };

        Arc::new(metrics)
    }
}

fn get_chronosphere_base_environment_dimensions() -> DimensionPosition {
    DimensionPosition::from_iter(
        vec![
            // Chronosphere requires a standard Otel Collor `service.instance.id` and `service.name`
            // dimensions in order to ingest metrics, otherwise they are rejected.
            // We don't really "need" a distinct value, we just need something, which
            // will default to `unknown`. If we need something in the future, we can
            // add that as necessary.
            (
                "service.instance.id",
                get_environment_variable("SERVICE_INSTANCE_ID"),
            ),
            ("service.name", get_environment_variable("SERVICE_NAME")),
        ]
        .into_iter()
        .map(|(n, v)| (n.into(), v.into())),
    )
}

fn get_environment_variable(variable: &str) -> String {
    match std::env::var(variable) {
        Ok(val) => val,
        Err(_) => {
            info!(
                "Environment variable {} not set, defaulting to 'unknown'",
                variable
            );
            "unknown".to_string()
        }
    }
}

fn get_environment_variable_or_none(variable: &str) -> Option<String> {
    std::env::var(variable).ok()
}
