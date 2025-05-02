use goodmetrics::SumHandle;

pub struct ConnectionGuard {
    counter: SumHandle,
}

impl ConnectionGuard {
    /// Create a new guard, immediately bumping the counter.
    pub fn new(counter: SumHandle) -> Self {
        counter.observe(1);
        Self { counter }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // -1 when this guard goes out of scope
        self.counter.observe(-1);
    }
}
