// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::map_result;
use crate::*;
use protocol_common::Protocol;
use session::ClientSession;
use std::collections::HashMap;
use std::collections::VecDeque;

#[metric(
    name = "backend_event_depth",
    description = "distribution of the number of events received per iteration of the event loop"
)]
pub static BACKEND_EVENT_DEPTH: AtomicHistogram = AtomicHistogram::new(7, 17);

#[metric(
    name = "backend_event_error",
    description = "the number of error events received"
)]
pub static BACKEND_EVENT_ERROR: Counter = Counter::new();

#[metric(
    name = "backend_event_loop",
    description = "the number of times the event loop has run"
)]
pub static BACKEND_EVENT_LOOP: Counter = Counter::new();

#[metric(
    name = "backend_event_max_reached",
    description = "the number of times the maximum number of events was returned"
)]
pub static BACKEND_EVENT_MAX_REACHED: Counter = Counter::new();

#[metric(
    name = "backend_event_read",
    description = "the number of read events received"
)]
pub static BACKEND_EVENT_READ: Counter = Counter::new();

#[metric(
    name = "backend_event_total",
    description = "the total number of events received"
)]
pub static BACKEND_EVENT_TOTAL: Counter = Counter::new();

#[metric(
    name = "backend_event_write",
    description = "the number of write events received"
)]
pub static BACKEND_EVENT_WRITE: Counter = Counter::new();

pub struct BackendWorkerBuilder<Proto, Request, Response> {
    free_queue: VecDeque<Token>,
    nevent: usize,
    protocol: Proto,
    poll: Poll,
    sessions: Slab<ClientSession<Proto, Request, Response>>,
    timeout: Duration,
    waker: Arc<Waker>,
}

impl<Proto, Request, Response> BackendWorkerBuilder<Proto, Request, Response>
where
    Proto: Clone + Protocol<Request, Response>,
    Request: Compose,
{
    pub fn new<T: BackendConfig>(config: &T, protocol: Proto) -> Result<Self> {
        let config = config.backend();

        let poll = Poll::new()?;

        let waker = Arc::new(Waker::from(
            pelikan_net::Waker::new(poll.registry(), WAKER_TOKEN).unwrap(),
        ));

        let nevent = config.nevent();
        let timeout = Duration::from_millis(config.timeout() as u64);

        let mut sessions = Slab::new();
        let mut free_queue = VecDeque::new();

        for endpoint in config.socket_addrs()? {
            let stream = TcpStream::connect(endpoint)?;
            let mut session = ClientSession::new(Session::from(stream), protocol.clone());
            let s = sessions.vacant_entry();
            let interest = session.interest();
            session
                .register(poll.registry(), Token(s.key()), interest)
                .expect("failed to register");
            free_queue.push_back(Token(s.key()));
            s.insert(session);
        }

        Ok(Self {
            free_queue,
            nevent,
            protocol,
            poll,
            sessions,
            timeout,
            waker,
        })
    }

    pub fn waker(&self) -> Arc<Waker> {
        self.waker.clone()
    }

    pub fn build(
        self,
        data_queue: Queues<(Request, Response, Token), (Request, Token)>,
        signal_queue: Queues<(), Signal>,
    ) -> BackendWorker<Proto, Request, Response> {
        BackendWorker {
            backlog: VecDeque::new(),
            data_queue,
            free_queue: self.free_queue,
            nevent: self.nevent,
            protocol: self.protocol,
            pending: HashMap::new(),
            poll: self.poll,
            sessions: self.sessions,
            signal_queue,
            timeout: self.timeout,
            waker: self.waker,
        }
    }
}

pub struct BackendWorker<Proto, Request, Response> {
    backlog: VecDeque<(Request, Token)>,
    data_queue: Queues<(Request, Response, Token), (Request, Token)>,
    free_queue: VecDeque<Token>,
    nevent: usize,
    protocol: Proto,
    pending: HashMap<Token, Token>,
    poll: Poll,
    sessions: Slab<ClientSession<Proto, Request, Response>>,
    signal_queue: Queues<(), Signal>,
    timeout: Duration,
    waker: Arc<Waker>,
}

impl<Proto, Request, Response> BackendWorker<Proto, Request, Response>
where
    Proto: Protocol<Request, Response> + Clone,
    Request: Compose,
{
    /// Return the `Session` to the `Listener` to handle flush/close
    fn close(&mut self, token: Token) {
        if self.sessions.contains(token.0) {
            let mut session = self.sessions.remove(token.0);
            let _ = session.flush();
        }
    }

    /// Handle up to one response for a session
    fn read(&mut self, token: Token) -> Result<()> {
        let session = self
            .sessions
            .get_mut(token.0)
            .ok_or_else(|| Error::new(ErrorKind::Other, "non-existant session"))?;

        // fill the session
        map_result(session.fill())?;

        // process up to one request
        match session.receive() {
            Ok((request, response)) => {
                if let Some(fe_token) = self.pending.remove(&token) {
                    self.free_queue.push_back(token);
                    self.data_queue
                        .try_send_to(0, (request, response, fe_token))
                        .map_err(|_| Error::new(ErrorKind::Other, "data queue is full"))
                } else {
                    panic!("corrupted state");
                }
            }
            Err(e) => map_err(e),
        }
    }

    /// Handle write by flushing the session
    fn write(&mut self, token: Token) -> Result<()> {
        let session = self
            .sessions
            .get_mut(token.0)
            .ok_or_else(|| Error::new(ErrorKind::Other, "non-existant session"))?;

        match session.flush() {
            Ok(_) => Ok(()),
            Err(e) => map_err(e),
        }
    }

    /// Run the worker in a loop, handling new events.
    pub fn run(&mut self) {
        // these are buffers which are re-used in each loop iteration to receive
        // events and queue messages
        let mut events = Events::with_capacity(self.nevent);
        let mut messages = Vec::with_capacity(QUEUE_CAPACITY);
        // let mut sessions = Vec::with_capacity(QUEUE_CAPACITY);

        loop {
            BACKEND_EVENT_LOOP.increment();

            // get events with timeout
            if self.poll.poll(&mut events, Some(self.timeout)).is_err() {
                error!("Error polling");
            }

            let count = events.iter().count();
            BACKEND_EVENT_TOTAL.add(count as _);
            if count == self.nevent {
                BACKEND_EVENT_MAX_REACHED.increment();
            } else {
                let _ = BACKEND_EVENT_DEPTH.increment(count as _);
            }

            // process all events
            for event in events.iter() {
                let token = event.token();
                match token {
                    WAKER_TOKEN => {
                        self.waker.reset();
                        // handle all pending messages on the data queue
                        self.data_queue.try_recv_all(&mut messages);
                        for (request, fe_token) in messages.drain(..).map(|v| v.into_inner()) {
                            if let Some(be_token) = self.free_queue.pop_front() {
                                let session = &mut self.sessions[be_token.0];
                                if session.send(request).is_err() {
                                    panic!("we don't handle this right now");
                                } else {
                                    self.pending.insert(be_token, fe_token);
                                }
                            } else {
                                self.backlog.push_back((request, token));
                            }
                        }

                        // check if we received any signals from the admin thread
                        while let Some(signal) =
                            self.signal_queue.try_recv().map(|v| v.into_inner())
                        {
                            match signal {
                                Signal::FlushAll => {}
                                Signal::Shutdown => {
                                    // if we received a shutdown, we can return
                                    // and stop processing events
                                    return;
                                }
                            }
                        }
                    }
                    _ => {
                        if event.is_error() {
                            BACKEND_EVENT_ERROR.increment();

                            self.close(token);
                            continue;
                        }

                        if event.is_writable() {
                            BACKEND_EVENT_WRITE.increment();

                            if self.write(token).is_err() {
                                self.close(token);
                                continue;
                            }
                        }

                        if event.is_readable() {
                            BACKEND_EVENT_READ.increment();

                            if self.read(token).is_err() {
                                self.close(token);
                                continue;
                            }
                        }
                    }
                }
            }

            // wakes the storage thread if necessary
            let _ = self.data_queue.wake();
        }
    }
}

pub struct BackendBuilder<Proto, Request, Response> {
    builders: Vec<BackendWorkerBuilder<Proto, Request, Response>>,
}

impl<BackendProto, BackendRequest, BackendResponse>
    BackendBuilder<BackendProto, BackendRequest, BackendResponse>
where
    BackendProto: Protocol<BackendRequest, BackendResponse> + Clone,
    BackendRequest: Compose,
{
    pub fn new<T: BackendConfig>(
        config: &T,
        protocol: BackendProto,
        threads: usize,
    ) -> Result<Self> {
        let mut builders = Vec::new();
        for _ in 0..threads {
            builders.push(BackendWorkerBuilder::new(config, protocol.clone())?);
        }
        Ok(Self { builders })
    }

    pub fn wakers(&self) -> Vec<Arc<Waker>> {
        self.builders.iter().map(|b| b.waker()).collect()
    }

    #[allow(clippy::type_complexity)]
    pub fn build(
        mut self,
        mut data_queues: Vec<
            Queues<(BackendRequest, BackendResponse, Token), (BackendRequest, Token)>,
        >,
        mut signal_queues: Vec<Queues<(), Signal>>,
    ) -> Vec<BackendWorker<BackendProto, BackendRequest, BackendResponse>> {
        self.builders
            .drain(..)
            .map(|b| b.build(data_queues.pop().unwrap(), signal_queues.pop().unwrap()))
            .collect()
    }
}
