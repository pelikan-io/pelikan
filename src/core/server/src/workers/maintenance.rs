// Copyright 2026 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[metric(
    name = "maintenance_event_loop",
    description = "the number of times the maintenance event loop has run"
)]
pub static MAINTENANCE_EVENT_LOOP: Counter = Counter::new();

pub struct MaintenanceBuilder<Storage> {
    poll: Poll,
    storage: Arc<Storage>,
    timeout: Duration,
    waker: Arc<Waker>,
}

impl<Storage> MaintenanceBuilder<Storage> {
    pub fn new<T: WorkerConfig>(config: &T, storage: Arc<Storage>) -> Result<Self> {
        let config = config.worker();

        let poll = Poll::new()?;

        let waker = Arc::new(Waker::from(
            pelikan_net::Waker::new(poll.registry(), WAKER_TOKEN).unwrap(),
        ));

        let timeout = Duration::from_millis(config.timeout() as u64);

        Ok(Self {
            poll,
            storage,
            timeout,
            waker,
        })
    }

    pub fn waker(&self) -> Arc<Waker> {
        self.waker.clone()
    }

    pub fn build(self, signal_queue: Queues<(), Signal>) -> Maintenance<Storage> {
        Maintenance {
            poll: self.poll,
            signal_queue,
            storage: self.storage,
            timeout: self.timeout,
            waker: self.waker,
        }
    }
}

pub struct Maintenance<Storage> {
    poll: Poll,
    signal_queue: Queues<(), Signal>,
    storage: Arc<Storage>,
    timeout: Duration,
    waker: Arc<Waker>,
}

impl<Storage: EntryStore> Maintenance<Storage> {
    /// Run the maintenance thread in a loop, driving eager expiration and
    /// handling control-plane signals.
    pub fn run(&mut self) {
        let mut events = Events::with_capacity(1);

        loop {
            MAINTENANCE_EVENT_LOOP.increment();

            self.storage.expire();

            // wait for a signal wakeup or timeout
            if self.poll.poll(&mut events, Some(self.timeout)).is_err() {
                error!("Error polling");
            }

            if !events.is_empty() {
                self.waker.reset();
            }

            // check if we received any signals from the admin thread
            while let Some(signal) = self.signal_queue.try_recv() {
                match signal.into_inner() {
                    Signal::FlushAll => {
                        warn!("received flush_all");
                        self.storage.clear();
                    }
                    Signal::Shutdown => {
                        return;
                    }
                }
            }
        }
    }
}
