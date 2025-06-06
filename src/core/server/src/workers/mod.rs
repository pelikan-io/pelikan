// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use protocol_common::Protocol;
use std::thread::JoinHandle;

mod multi;
mod single;
mod storage;

use multi::*;
use single::*;
use storage::*;

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
        Ok(0) => Err(Error::new(ErrorKind::Other, "client hangup")),
        Ok(_) => Ok(()),
        Err(e) => map_err(e),
    }
}

// NOTE: as it is expected to have very few instances of this enum
// we suppress the warning about the large variant
#[allow(clippy::large_enum_variant)]
pub enum Workers<Parser, Request, Response, Storage> {
    Single {
        worker: SingleWorker<Parser, Request, Response, Storage>,
    },
    Multi {
        workers: Vec<MultiWorker<Parser, Request, Response>>,
        storage: StorageWorker<Request, Response, Storage, Token>,
    },
}

impl<Proto, Request, Response, Storage> Workers<Proto, Request, Response, Storage>
where
    Proto: 'static + Protocol<Request, Response> + Clone + Send,
    Request: 'static + Klog + Klog<Response = Response> + Send,
    Response: 'static + Compose + Send,
    Storage: 'static + EntryStore + Execute<Request, Response> + Send,
{
    pub fn spawn(self) -> Vec<JoinHandle<()>> {
        match self {
            Self::Single { mut worker } => {
                vec![std::thread::Builder::new()
                    .name(format!("{THREAD_PREFIX}_work"))
                    .spawn(move || worker.run())
                    .unwrap()]
            }
            Self::Multi {
                mut workers,
                mut storage,
            } => {
                let mut join_handles = vec![std::thread::Builder::new()
                    .name(format!("{THREAD_PREFIX}_storage"))
                    .spawn(move || storage.run())
                    .unwrap()];

                for (id, mut worker) in workers.drain(..).enumerate() {
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
    }
}

pub enum WorkersBuilder<Proto, Request, Response, Storage> {
    Single {
        worker: SingleWorkerBuilder<Proto, Request, Response, Storage>,
    },
    Multi {
        workers: Vec<MultiWorkerBuilder<Proto, Request, Response>>,
        storage: StorageWorkerBuilder<Request, Response, Storage>,
    },
}

impl<Proto, Request, Response, Storage> WorkersBuilder<Proto, Request, Response, Storage>
where
    Proto: Protocol<Request, Response> + Clone,
    Response: Compose,
    Storage: Execute<Request, Response> + EntryStore,
{
    pub fn new<T: WorkerConfig>(config: &T, protocol: Proto, storage: Storage) -> Result<Self> {
        let threads = config.worker().threads();

        if threads > 1 {
            let mut workers = vec![];
            for _ in 0..threads {
                workers.push(MultiWorkerBuilder::new(config, protocol.clone())?)
            }

            Ok(Self::Multi {
                workers,
                storage: StorageWorkerBuilder::new(config, storage)?,
            })
        } else {
            Ok(Self::Single {
                worker: SingleWorkerBuilder::new(config, protocol, storage)?,
            })
        }
    }

    pub fn worker_wakers(&self) -> Vec<Arc<Waker>> {
        match self {
            Self::Single { worker } => {
                vec![worker.waker()]
            }
            Self::Multi {
                workers,
                storage: _,
            } => workers.iter().map(|w| w.waker()).collect(),
        }
    }

    pub fn wakers(&self) -> Vec<Arc<Waker>> {
        match self {
            Self::Single { worker } => {
                vec![worker.waker()]
            }
            Self::Multi { workers, storage } => {
                let mut wakers = vec![storage.waker()];
                for worker in workers {
                    wakers.push(worker.waker());
                }
                wakers
            }
        }
    }

    pub fn build(
        self,
        session_queues: Vec<Queues<Session, Session>>,
        signal_queues: Vec<Queues<(), Signal>>,
    ) -> Workers<Proto, Request, Response, Storage> {
        let mut signal_queues = signal_queues;
        let mut session_queues = session_queues;
        match self {
            Self::Multi {
                storage,
                mut workers,
            } => {
                let storage_wakers = vec![storage.waker()];
                let worker_wakers: Vec<Arc<Waker>> = workers.iter().map(|v| v.waker()).collect();
                let (mut worker_data_queues, mut storage_data_queues) =
                    Queues::new(worker_wakers, storage_wakers, QUEUE_CAPACITY);

                // The storage thread precedes the worker threads in the set of
                // wakers, so its signal queue is the first element of
                // `signal_queues`. Its request queue is also the first (and
                // only) element of `request_queues`. We remove these and build
                // the storage so we can loop through the remaining signal
                // queues when launching the worker threads.
                let s = storage.build(storage_data_queues.remove(0), signal_queues.remove(0));

                let mut w = Vec::new();
                for worker_builder in workers.drain(..) {
                    w.push(worker_builder.build(
                        worker_data_queues.remove(0),
                        session_queues.remove(0),
                        signal_queues.remove(0),
                    ));
                }

                Workers::Multi {
                    storage: s,
                    workers: w,
                }
            }
            Self::Single { worker } => Workers::Single {
                worker: worker.build(session_queues.remove(0), signal_queues.remove(0)),
            },
        }
    }
}
