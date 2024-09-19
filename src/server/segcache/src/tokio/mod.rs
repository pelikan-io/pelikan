use std::sync::atomic::Ordering;
use std::time::Duration;
use logger::Drain;
use crate::config::{Config};
use crate::*;

use ::tokio::runtime::Builder;
use ::tokio::sync::RwLock;
use ::tokio::time::sleep;
use metriken::Lazy;

use std::sync::Arc;

mod admin;
mod ascii;
mod metrics;

static METRICS_SNAPSHOT: Lazy<Arc<RwLock<metrics::MetricsSnapshot>>> =
    Lazy::new(|| Arc::new(RwLock::new(Default::default())));

pub fn spawn(config: Config, mut log: Box<dyn Drain>) {
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

    // initialize storage
    let storage = Storage::new(&*config).expect("failed to initialize storage");

    // initialize parser
    let parser = Parser::new()
        .max_value_size(config.seg.segment_size() as usize)
        .time_type(config.time.time_type());


    // initialize async runtime for the data plane
    let data_runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.worker.threads())
        .build()
        .expect("failed to initialize tokio runtime");

    data_runtime.block_on(async move { ascii::run(config, storage, parser).await });

    while RUNNING.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(250));
    }

    data_runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    control_runtime.shutdown_timeout(std::time::Duration::from_millis(100));
}
