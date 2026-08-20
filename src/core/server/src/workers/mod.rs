// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use protocol_common::Protocol;
use std::thread::JoinHandle;

mod worker;

use worker::{Worker, WorkerBuilder};

#[metric(
    name = "worker_event_depth",
    description = "distribution of the number of events received per iteration of the event loop"
)]
pub static WORKER_EVENT_DEPTH: AtomicHistogram = AtomicHistogram::new(7, 17);

#[metric(
    name = "worker_event_error",
    description = "the number of error events received"
)]
pub static WORKER_EVENT_ERROR: Counter = Counter::new();

#[metric(
    name = "worker_event_loop",
    description = "the number of times the event loop has run"
)]
pub static WORKER_EVENT_LOOP: Counter = Counter::new();

#[metric(
    name = "worker_event_max_reached",
    description = "the number of times the maximum number of events was returned"
)]
pub static WORKER_EVENT_MAX_REACHED: Counter = Counter::new();

#[metric(
    name = "worker_event_read",
    description = "the number of read events received"
)]
pub static WORKER_EVENT_READ: Counter = Counter::new();

#[metric(
    name = "worker_event_total",
    description = "the total number of events received"
)]
pub static WORKER_EVENT_TOTAL: Counter = Counter::new();

#[metric(
    name = "worker_event_write",
    description = "the number of write events received"
)]
pub static WORKER_EVENT_WRITE: Counter = Counter::new();

fn map_result(result: Result<usize>) -> Result<()> {
    match result {
        Ok(0) => Err(Error::other("client hangup")),
        Ok(_) => Ok(()),
        Err(e) => map_err(e),
    }
}

pub struct Workers<Proto, Request, Response, Storage> {
    workers: Vec<Worker<Proto, Request, Response, Storage>>,
}

impl<Proto, Request, Response, Storage> Workers<Proto, Request, Response, Storage>
where
    Proto: 'static + Protocol<Request, Response> + Clone + Send,
    Request: 'static + Klog + Klog<Response = Response> + Send,
    Response: 'static + Compose + Send,
    Storage: 'static + EntryStore + Execute<Request, Response> + Send + Sync,
{
    pub fn spawn(self) -> Vec<JoinHandle<()>> {
        let mut join_handles = Vec::new();

        for (id, mut worker) in self.workers.into_iter().enumerate() {
            join_handles.push(
                std::thread::Builder::new()
                    .name(format!("{THREAD_PREFIX}_work_{id}"))
                    .spawn(move || worker.run())
                    .unwrap(),
            )
        }

        join_handles
    }
}

pub struct WorkersBuilder<Proto, Request, Response, Storage> {
    workers: Vec<WorkerBuilder<Proto, Request, Response, Storage>>,
}

impl<Proto, Request, Response, Storage> WorkersBuilder<Proto, Request, Response, Storage>
where
    Proto: Protocol<Request, Response> + Clone,
    Response: Compose,
    Storage: Execute<Request, Response> + EntryStore,
{
    pub fn new<T: WorkerConfig>(
        config: &T,
        protocol: Proto,
        storage: Arc<Storage>,
    ) -> Result<Self> {
        let threads = config.worker().threads();

        let mut workers = vec![];
        for _ in 0..threads {
            workers.push(WorkerBuilder::new(
                config,
                protocol.clone(),
                storage.clone(),
            )?)
        }

        Ok(Self { workers })
    }

    pub fn worker_wakers(&self) -> Vec<Arc<Waker>> {
        self.workers.iter().map(|w| w.waker()).collect()
    }

    pub fn wakers(&self) -> Vec<Arc<Waker>> {
        self.worker_wakers()
    }

    pub fn build(
        self,
        mut session_queues: Vec<Queues<Session, Session>>,
        mut signal_queues: Vec<Queues<(), Signal>>,
    ) -> Workers<Proto, Request, Response, Storage> {
        // The queues arrive in the same order as `wakers()`: one session
        // queue and one signal queue per worker.
        let mut workers = Vec::new();
        for builder in self.workers {
            workers.push(builder.build(session_queues.remove(0), signal_queues.remove(0)));
        }

        Workers { workers }
    }
}
