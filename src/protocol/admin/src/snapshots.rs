use std::time::SystemTime;
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;
use metriken::Lazy;
use crate::*;

type HistogramSnapshots = HashMap<String, metriken::histogram::Snapshot>;

pub static SNAPSHOTS: Lazy<Arc<RwLock<Snapshots>>> =
    Lazy::new(|| Arc::new(RwLock::new(Snapshots::new())));

pub struct Snapshots {
    timestamp: SystemTime,
    previous: HistogramSnapshots,
    deltas: HistogramSnapshots,
}

impl Default for Snapshots {
    fn default() -> Self {
        Self::new()
    }
}

impl Snapshots {
    pub fn new() -> Self {
        let timestamp = SystemTime::now();

        let mut current = HashMap::new();

        for metric in metriken::metrics().iter() {
            let any = if let Some(any) = metric.as_any() {
                any
            } else {
                continue;
            };

            let key = metric.name().to_string();

            let snapshot = if let Some(histogram) = any.downcast_ref::<metriken::AtomicHistogram>()
            {
                histogram.snapshot()
            } else if let Some(histogram) = any.downcast_ref::<metriken::RwLockHistogram>() {
                histogram.snapshot()
            } else {
                None
            };

            if let Some(snapshot) = snapshot {
                current.insert(key, snapshot);
            }
        }

        let deltas = current.clone();

        Self {
            timestamp,
            previous: current,
            deltas,
        }
    }

    pub fn update(&mut self) {
        self.timestamp = SystemTime::now();

        let mut current = HashMap::new();

        for metric in metriken::metrics().iter() {
            let any = if let Some(any) = metric.as_any() {
                any
            } else {
                continue;
            };

            let key = metric.name().to_string();

            let snapshot = if let Some(histogram) = any.downcast_ref::<metriken::AtomicHistogram>()
            {
                histogram.snapshot()
            } else if let Some(histogram) = any.downcast_ref::<metriken::RwLockHistogram>() {
                histogram.snapshot()
            } else {
                None
            };

            if let Some(snapshot) = snapshot {
                if let Some(previous) = self.previous.get(&key) {
                    self.deltas
                        .insert(key.clone(), snapshot.wrapping_sub(previous).unwrap());
                }

                current.insert(key, snapshot);
            }
        }

        self.previous = current;
    }

    pub fn percentiles(&self, metric: &str) -> Vec<(String, f64, u64)> {
        let mut result = Vec::new();

        let percentiles: Vec<f64> = PERCENTILES
            .iter()
            .map(|(_, percentile)| *percentile)
            .collect();

        if let Some(snapshot) = self.deltas.get(metric) {
            if let Ok(percentiles) = snapshot.percentiles(&percentiles) {
                for ((label, _), (percentile, bucket)) in PERCENTILES.iter().zip(percentiles.iter())
                {
                    result.push((label.to_string(), *percentile, bucket.end()));
                }
            }
        }

        result
    }

    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
}