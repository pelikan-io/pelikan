use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use logger::Drain;
use metriken::Lazy;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::sync::RwLock;
use tokio::time::sleep;

mod admin;
mod ascii;
mod grpc;
mod http2;
mod http3;
mod metrics;

static METRICS_SNAPSHOT: Lazy<Arc<RwLock<metrics::MetricsSnapshot>>> =
    Lazy::new(|| Arc::new(RwLock::new(Default::default())));

static RUNNING: AtomicBool = AtomicBool::new(true);

use crate::config::{Config, Protocol};

pub fn run(config: Config, mut log: Box<dyn Drain>) {
    let config = Arc::new(config);

    // initialize async runtime for control plane
    let control_runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("failed to initialize tokio runtime");

    // spawn logging thread
    control_runtime.spawn(async move {
        while RUNNING.load(Ordering::Relaxed) {
            sleep(Duration::from_millis(1)).await;
            let _ = log.flush();
        }
        let _ = log.flush();
    });

    // spawn thread to maintain histogram snapshots
    {
        let interval = config.metrics.interval();
        control_runtime.spawn(async move {
            while RUNNING.load(Ordering::Relaxed) {
                // acquire a lock and update the snapshots
                {
                    let mut snapshots = METRICS_SNAPSHOT.write().await;
                    snapshots.update();
                }

                // delay until next update
                sleep(interval).await;
            }
        });
    }

    // spawn the admin thread
    control_runtime.spawn(admin::http(config.clone()));

    // initialize async runtime for the data plane
    let data_runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.worker.threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general.protocol {
        Protocol::Ascii => {
            data_runtime.block_on(async move { ascii::run(config).await });
        }
        Protocol::Grpc => {
            data_runtime.spawn(async move { grpc::run(config).await });
        }
        Protocol::Http2 => {
            data_runtime.spawn(async move { http2::run(config).await });
        }
        Protocol::Http3 => {
            data_runtime.spawn(async move { http3::run(config).await });
        }
    }

    while RUNNING.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(250));
    }

    data_runtime.shutdown_timeout(std::time::Duration::from_millis(100));
}
