use crate::tokio::METRICS_SNAPSHOT;
use crate::*;
use ::config::AdminConfig;
use metriken::Value;
use std::net::ToSocketAddrs;
use std::sync::Arc;

/// The HTTP admin server.
pub async fn http(config: Arc<Config>) {
    let admin = filters::admin();

    let addr = format!("{}:{}", config.admin().host(), config.admin().port());

    let addr = addr
        .to_socket_addrs()
        .expect("bad listen address")
        .next()
        .expect("couldn't determine listen address");

    warp::serve(admin).run(addr).await;
}

mod filters {
    use super::*;
    use warp::Filter;

    /// The combined set of admin endpoint filters
    pub fn admin() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        prometheus_stats().or(human_stats()).or(json_stats())
    }

    /// Serves Prometheus / OpenMetrics text format metrics.
    ///
    /// GET /metrics
    pub fn prometheus_stats(
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("metrics")
            .and(warp::get())
            .and_then(handlers::prometheus_stats)
    }

    /// Serves a human readable metrics output.
    ///
    /// GET /vars
    pub fn human_stats(
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("vars")
            .and(warp::get())
            .and_then(handlers::human_stats)
    }

    /// Serves JSON metrics output that is compatible with Twitter Server /
    /// Finagle metrics endpoints. Multiple paths are provided for enhanced
    /// compatibility with metrics collectors.
    ///
    /// GET /metrics.json
    /// GET /vars.json
    /// GET /admin/metrics.json
    pub fn json_stats(
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("metrics.json")
            .and(warp::get())
            .and_then(handlers::json_stats)
            .or(warp::path!("vars.json")
                .and(warp::get())
                .and_then(handlers::json_stats))
            .or(warp::path!("admin" / "metrics.json")
                .and(warp::get())
                .and_then(handlers::json_stats))
    }
}

pub mod handlers {

    use super::*;
    use core::convert::Infallible;
    use std::time::UNIX_EPOCH;

    /// Serves Prometheus / OpenMetrics text format metrics. All metrics have
    /// type information, some have descriptions as well. Percentiles read from
    /// heatmaps are exposed with a `percentile` label where the value
    /// corresponds to the percentile in the range of 0.0 - 100.0.
    ///
    /// See: https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md
    ///
    /// ```text
    /// # TYPE some_counter counter
    /// # HELP some_counter An unsigned 64bit monotonic counter.
    /// counter 0
    /// # TYPE some_gauge gauge
    /// # HELP some_gauge A signed 64bit gauge.
    /// some_gauge 0
    /// # TYPE some_distribution{percentile="50.0"} gauge
    /// some_distribution{percentile="50.0"} 0
    /// ```
    pub async fn prometheus_stats() -> Result<impl warp::Reply, Infallible> {
        let mut data = Vec::new();

        let metrics_snapshot = METRICS_SNAPSHOT.read().await;

        let timestamp = metrics_snapshot
            .current
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        for metric in &metriken::metrics() {
            if metric.name().starts_with("log_") {
                continue;
            }

            let name = metric.name();

            match metric.value() {
                Some(Value::Counter(value)) => {
                    if let Some(description) = metric.description() {
                        data.push(format!(
                            "# TYPE {name} counter\n# HELP {name} {description}\n{name} {value}"
                        ));
                    } else {
                        data.push(format!("# TYPE {name} counter\n{name} {value}"));
                    }
                }
                Some(Value::Gauge(value)) => {
                    if let Some(description) = metric.description() {
                        data.push(format!(
                            "# TYPE {name} gauge\n# HELP {name} {description}\n{name} {value}"
                        ));
                    } else {
                        data.push(format!("# TYPE {name} gauge\n{name} {value}"));
                    }
                }
                Some(Value::Other(_)) => {
                    let percentiles = metrics_snapshot.percentiles(metric.name());

                    for (_label, percentile, value) in percentiles {
                        if let Some(description) = metric.description() {
                            data.push(format!(
                                "# TYPE {name} gauge\n# HELP {name} {description}\n{name}{{percentile=\"{:02}\"}} {value} {timestamp}",
                                percentile,
                            ));
                        } else {
                            data.push(format!(
                                "# TYPE {name} gauge\n{name}{{percentile=\"{:02}\"}} {value} {timestamp}",
                                percentile,
                            ));
                        }
                    }
                }
                _ => continue,
            }
        }

        data.sort();
        let mut content = data.join("\n");
        content += "\n";
        let parts: Vec<&str> = content.split('/').collect();
        Ok(parts.join("_"))
    }

    /// Serves JSON formatted metrics following the conventions of Finagle /
    /// TwitterServer. Percentiles read from heatmaps will have a percentile
    /// label appended to the metric name in the form `/p999` which would be the
    /// 99.9th percentile.
    ///
    /// ```text
    /// {"get/ok": 0,"client/request/p999": 0, ... }
    /// ```
    pub async fn json_stats() -> Result<impl warp::Reply, Infallible> {
        let data = human_formatted_stats().await;

        let mut content = "{".to_string();
        content += &data.join(",");
        content += "}";

        Ok(content)
    }

    /// Serves human readable stats. One metric per line with a `LF` as the
    /// newline character (Unix-style). Percentiles will have percentile labels
    /// appened with a `/` as a separator.
    ///
    /// ```
    /// get/ok: 0
    /// client/request/latency/p50: 0,
    /// ```
    pub async fn human_stats() -> Result<impl warp::Reply, Infallible> {
        let data = human_formatted_stats().await;

        let mut content = data.join("\n");
        content += "\n";
        Ok(content)
    }
}

// human formatted stats that can be exposed as human stats or converted to json
pub async fn human_formatted_stats() -> Vec<String> {
    let mut data = Vec::new();

    let metrics_snapshot = METRICS_SNAPSHOT.read().await;

    for metric in &metriken::metrics() {
        if metric.name().starts_with("log_") {
            continue;
        }

        let name = metric.name();

        match metric.value() {
            Some(Value::Counter(value)) => {
                data.push(format!("\"{name}\": {value}"));
            }
            Some(Value::Gauge(value)) => {
                data.push(format!("\"{name}\": {value}"));
            }
            Some(Value::Other(_)) => {
                let percentiles = metrics_snapshot.percentiles(metric.name());

                for (label, _percentile, value) in percentiles {
                    data.push(format!("\"{name}/{label}\": {value}",));
                }
            }
            _ => continue,
        }
    }

    data.sort();

    data
}
