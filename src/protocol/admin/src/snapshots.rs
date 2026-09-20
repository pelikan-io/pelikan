use crate::*;
use metriken::Lazy;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

type HistogramSnapshots = HashMap<String, metriken::histogram::Histogram>;

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
                histogram.load()
            } else if let Some(histogram) = any.downcast_ref::<metriken::RwLockHistogram>() {
                histogram.load()
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
                histogram.load()
            } else if let Some(histogram) = any.downcast_ref::<metriken::RwLockHistogram>() {
                histogram.load()
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
            .map(|(_, percentile)| percentile / 100.0)
            .collect();

        if let Some(snapshot) = self.deltas.get(metric) {
            if let Ok(Some(quantiles)) = snapshot.quantiles(&percentiles) {
                for ((label, percentile), bucket) in
                    PERCENTILES.iter().zip(quantiles.entries().values())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_percentile_snapshot_preserves_labels_and_percentage_values() {
        let mut histogram = metriken::histogram::Histogram::new(7, 16).unwrap();
        for value in [10, 20, 30, 40] {
            histogram.increment(value).unwrap();
        }

        let mut deltas = HashMap::new();
        deltas.insert("request_latency".to_string(), histogram);
        let snapshots = Snapshots {
            timestamp: SystemTime::now(),
            previous: HashMap::new(),
            deltas,
        };

        let percentiles = snapshots.percentiles("request_latency");
        assert_eq!(percentiles.len(), PERCENTILES.len());
        assert_eq!(
            percentiles
                .iter()
                .map(|(label, percentile, _)| (label.as_str(), *percentile))
                .collect::<Vec<_>>(),
            PERCENTILES
        );
        assert_eq!(percentiles[0].2, 10);
        assert_eq!(percentiles[1].2, 20);
        assert_eq!(percentiles[2].2, 30);
        assert_eq!(percentiles[3].2, 40);
        assert_eq!(percentiles[4].2, 40);
    }
}
