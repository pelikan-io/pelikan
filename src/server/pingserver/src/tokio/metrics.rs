use metriken::{histogram, AtomicHistogram, RwLockHistogram, Value};
use std::collections::HashMap;
use std::time::SystemTime;

pub static PERCENTILES: &[(&str, f64)] = &[
    ("p25", 25.0),
    ("p50", 50.0),
    ("p75", 75.0),
    ("p90", 90.0),
    ("p99", 99.0),
    ("p999", 99.9),
    ("p9999", 99.99),
];

pub struct MetricsSnapshot {
    pub current: SystemTime,
    pub previous: SystemTime,
    pub counters: CountersSnapshot,
    pub histograms: HistogramsSnapshot,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsSnapshot {
    pub fn new() -> Self {
        let now = SystemTime::now();

        Self {
            current: now,
            previous: now,
            counters: Default::default(),
            histograms: Default::default(),
        }
    }

    pub fn update(&mut self) {
        self.previous = self.current;
        self.current = SystemTime::now();

        self.counters.update();
        self.histograms.update();
    }

    pub fn percentiles(&self, name: &str) -> Vec<(String, f64, u64)> {
        self.histograms.percentiles(name)
    }
}

pub struct HistogramsSnapshot {
    pub previous: HashMap<String, histogram::Histogram>,
    pub deltas: HashMap<String, histogram::Histogram>,
}

impl Default for HistogramsSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl HistogramsSnapshot {
    pub fn new() -> Self {
        let mut current = HashMap::new();

        for metric in &metriken::metrics() {
            match metric.value() {
                Some(Value::Other(other)) => {
                    let histogram = if let Some(histogram) = other.downcast_ref::<AtomicHistogram>()
                    {
                        histogram.load()
                    } else if let Some(histogram) = other.downcast_ref::<RwLockHistogram>() {
                        histogram.load()
                    } else {
                        None
                    };

                    if let Some(histogram) = histogram {
                        current.insert(metric.name().to_string(), histogram);
                    }
                }
                _ => continue,
            }
        }

        let deltas = current.clone();

        Self {
            previous: current,
            deltas,
        }
    }

    pub fn update(&mut self) {
        for metric in &metriken::metrics() {
            match metric.value() {
                Some(Value::Other(other)) => {
                    let histogram = if let Some(histogram) = other.downcast_ref::<AtomicHistogram>()
                    {
                        histogram.load()
                    } else if let Some(histogram) = other.downcast_ref::<RwLockHistogram>() {
                        histogram.load()
                    } else {
                        None
                    };

                    if let Some(histogram) = histogram {
                        let name = metric.name().to_string();

                        if let Some(previous) = self.previous.get(&name) {
                            self.deltas
                                .insert(name.clone(), histogram.wrapping_sub(previous).unwrap());
                        }

                        self.previous.insert(name, histogram);
                    }
                }
                _ => continue,
            }
        }
    }

    pub fn percentiles(&self, metric: &str) -> Vec<(String, f64, u64)> {
        let mut result = Vec::new();

        let percentiles: Vec<f64> = PERCENTILES
            .iter()
            .map(|(_, percentile)| *percentile)
            .collect();

        if let Some(snapshot) = self.deltas.get(metric) {
            if let Ok(Some(percentiles)) = snapshot.percentiles(&percentiles) {
                for ((label, _), (percentile, bucket)) in PERCENTILES.iter().zip(percentiles.iter())
                {
                    result.push((label.to_string(), *percentile, bucket.end()));
                }
            }
        }

        result
    }
}

#[derive(Clone)]
pub struct CountersSnapshot {
    pub current: HashMap<String, u64>,
    pub previous: HashMap<String, u64>,
}

impl Default for CountersSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl CountersSnapshot {
    pub fn new() -> Self {
        let mut current = HashMap::new();
        let previous = HashMap::new();

        for metric in metriken::metrics().iter() {
            let any = if let Some(any) = metric.as_any() {
                any
            } else {
                continue;
            };

            let metric = metric.name().to_string();

            if let Some(_counter) = any.downcast_ref::<metriken::Counter>() {
                current.insert(metric.clone(), 0);
            }
        }
        Self { current, previous }
    }

    pub fn update(&mut self) {
        for metric in metriken::metrics().iter() {
            let any = if let Some(any) = metric.as_any() {
                any
            } else {
                continue;
            };

            if let Some(counter) = any.downcast_ref::<metriken::Counter>() {
                if let Some(old_value) = self
                    .current
                    .insert(metric.name().to_string(), counter.value())
                {
                    self.previous.insert(metric.name().to_string(), old_value);
                }
            }
        }
    }
}
