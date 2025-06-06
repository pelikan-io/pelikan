// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::map_result;
use crate::*;
use protocol_common::Protocol;

#[metric(
    name = "frontend_event_depth",
    description = "distribution of the number of events received per iteration of the event loop"
)]
pub static FRONTEND_EVENT_DEPTH: AtomicHistogram = AtomicHistogram::new(7, 17);

#[metric(
    name = "frontend_event_error",
    description = "the number of error events received"
)]
pub static FRONTEND_EVENT_ERROR: Counter = Counter::new();

#[metric(
    name = "frontend_event_loop",
    description = "the number of times the event loop has run"
)]
pub static FRONTEND_EVENT_LOOP: Counter = Counter::new();

#[metric(
    name = "frontend_event_max_reached",
    description = "the number of times the maximum number of events was returned"
)]
pub static FRONTEND_EVENT_MAX_REACHED: Counter = Counter::new();

#[metric(
    name = "frontend_event_read",
    description = "the number of read events received"
)]
pub static FRONTEND_EVENT_READ: Counter = Counter::new();

#[metric(
    name = "frontend_event_total",
    description = "the total number of events received"
)]
pub static FRONTEND_EVENT_TOTAL: Counter = Counter::new();

#[metric(
    name = "frontend_event_write",
    description = "the number of write events received"
)]
pub static FRONTEND_EVENT_WRITE: Counter = Counter::new();

pub struct FrontendWorkerBuilder<
    FrontendProto,
    FrontendRequest,
    FrontendResponse,
    BackendRequest,
    BackendResponse,
> {
    nevent: usize,
    protocol: FrontendProto,
    poll: Poll,
    sessions: Slab<ServerSession<FrontendProto, FrontendResponse, FrontendRequest>>,
    timeout: Duration,
    waker: Arc<Waker>,
    _backend_request: PhantomData<BackendRequest>,
    _backend_response: PhantomData<BackendResponse>,
}

impl<FrontendProto, FrontendRequest, FrontendResponse, BackendRequest, BackendResponse>
    FrontendWorkerBuilder<
        FrontendProto,
        FrontendRequest,
        FrontendResponse,
        BackendRequest,
        BackendResponse,
    >
{
    pub fn new<T: FrontendConfig>(config: &T, protocol: FrontendProto) -> Result<Self> {
        let config = config.frontend();

        let poll = Poll::new()?;

        let waker = Arc::new(Waker::from(
            pelikan_net::Waker::new(poll.registry(), WAKER_TOKEN).unwrap(),
        ));

        let nevent = config.nevent();
        let timeout = Duration::from_millis(config.timeout() as u64);

        Ok(Self {
            nevent,
            protocol,
            poll,
            sessions: Slab::new(),
            timeout,
            waker,
            _backend_request: PhantomData,
            _backend_response: PhantomData,
        })
    }

    pub fn waker(&self) -> Arc<Waker> {
        self.waker.clone()
    }

    pub fn build(
        self,
        data_queue: Queues<(BackendRequest, Token), (BackendRequest, BackendResponse, Token)>,
        session_queue: Queues<Session, Session>,
        signal_queue: Queues<(), Signal>,
    ) -> FrontendWorker<
        FrontendProto,
        FrontendRequest,
        FrontendResponse,
        BackendRequest,
        BackendResponse,
    > {
        FrontendWorker {
            data_queue,
            nevent: self.nevent,
            protocol: self.protocol,
            poll: self.poll,
            session_queue,
            sessions: self.sessions,
            signal_queue,
            timeout: self.timeout,
            waker: self.waker,
        }
    }
}

pub struct FrontendWorker<
    FrontendProto,
    FrontendRequest,
    FrontendResponse,
    BackendRequest,
    BackendResponse,
> {
    data_queue: Queues<(BackendRequest, Token), (BackendRequest, BackendResponse, Token)>,
    nevent: usize,
    protocol: FrontendProto,
    poll: Poll,
    session_queue: Queues<Session, Session>,
    sessions: Slab<ServerSession<FrontendProto, FrontendResponse, FrontendRequest>>,
    signal_queue: Queues<(), Signal>,
    timeout: Duration,
    waker: Arc<Waker>,
}

impl<FrontendProto, FrontendRequest, FrontendResponse, BackendRequest, BackendResponse>
    FrontendWorker<
        FrontendProto,
        FrontendRequest,
        FrontendResponse,
        BackendRequest,
        BackendResponse,
    >
where
    FrontendProto: Protocol<FrontendRequest, FrontendResponse> + Clone,
    FrontendResponse: Compose,
    FrontendResponse: From<BackendResponse>,
    BackendRequest: From<FrontendRequest>,
    BackendRequest: Compose,
    BackendResponse: Compose,
{
    /// Return the `Session` to the `Listener` to handle flush/close
    fn close(&mut self, token: Token) {
        if self.sessions.contains(token.0) {
            let mut session = self.sessions.remove(token.0).into_inner();
            let _ = session.deregister(self.poll.registry());
            let _ = self.session_queue.try_send_any(session);
            let _ = self.session_queue.wake();
        }
    }

    /// Handle up to one request for a session
    fn read(&mut self, token: Token) -> Result<()> {
        let session = self
            .sessions
            .get_mut(token.0)
            .ok_or_else(|| Error::new(ErrorKind::Other, "non-existant session"))?;

        // fill the session
        map_result(session.fill())?;

        // process up to one request
        match session.receive() {
            Ok(request) => self
                .data_queue
                .try_send_to(0, (BackendRequest::from(request), token))
                .map_err(|_| Error::new(ErrorKind::Other, "data queue is full")),
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

        loop {
            FRONTEND_EVENT_LOOP.increment();

            // get events with timeout
            if self.poll.poll(&mut events, Some(self.timeout)).is_err() {
                error!("Error polling");
            }

            let count = events.iter().count();
            FRONTEND_EVENT_TOTAL.add(count as _);
            if count == self.nevent {
                FRONTEND_EVENT_MAX_REACHED.increment();
            } else {
                let _ = FRONTEND_EVENT_DEPTH.increment(count as _);
            }

            // process all events
            for event in events.iter() {
                let token = event.token();
                match token {
                    WAKER_TOKEN => {
                        self.waker.reset();
                        // handle up to one new session
                        if let Some(mut session) =
                            self.session_queue.try_recv().map(|v| v.into_inner())
                        {
                            let s = self.sessions.vacant_entry();
                            let interest = session.interest();
                            if session
                                .register(self.poll.registry(), Token(s.key()), interest)
                                .is_ok()
                            {
                                s.insert(ServerSession::new(session, self.protocol.clone()));
                            } else {
                                let _ = self.session_queue.try_send_any(session);
                            }

                            // trigger a wake-up in case there are more sessions
                            let _ = self.waker.wake();
                        }

                        // handle all pending messages on the data queue
                        self.data_queue.try_recv_all(&mut messages);
                        for (_request, response, token) in
                            messages.drain(..).map(|v| v.into_inner())
                        {
                            if let Some(session) = self.sessions.get_mut(token.0) {
                                if response.should_hangup() {
                                    let _ = session.send(FrontendResponse::from(response));
                                    self.close(token);
                                    continue;
                                } else if session.send(FrontendResponse::from(response)).is_err() {
                                    self.close(token);
                                    continue;
                                } else if session.write_pending() > 0 {
                                    let interest = session.interest();
                                    if session
                                        .reregister(self.poll.registry(), token, interest)
                                        .is_err()
                                    {
                                        self.close(token);
                                        continue;
                                    }
                                }
                                if session.remaining() > 0 && self.read(token).is_err() {
                                    self.close(token);
                                    continue;
                                }
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
                            FRONTEND_EVENT_ERROR.increment();

                            self.close(token);
                            continue;
                        }

                        if event.is_writable() {
                            FRONTEND_EVENT_WRITE.increment();

                            if self.write(token).is_err() {
                                self.close(token);
                                continue;
                            }
                        }

                        if event.is_readable() {
                            FRONTEND_EVENT_READ.increment();

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

pub struct FrontendBuilder<
    FrontendParser,
    FrontendRequest,
    FrontendResponse,
    BackendRequest,
    BackendResponse,
> {
    builders: Vec<
        FrontendWorkerBuilder<
            FrontendParser,
            FrontendRequest,
            FrontendResponse,
            BackendRequest,
            BackendResponse,
        >,
    >,
}

impl<FrontendProto, FrontendRequest, FrontendResponse, BackendRequest, BackendResponse>
    FrontendBuilder<
        FrontendProto,
        FrontendRequest,
        FrontendResponse,
        BackendRequest,
        BackendResponse,
    >
where
    FrontendProto: Protocol<FrontendRequest, FrontendResponse> + Clone,
    FrontendResponse: Compose,
    FrontendResponse: From<BackendResponse>,
    BackendRequest: From<FrontendRequest>,
    BackendRequest: Compose,
{
    pub fn new<T: FrontendConfig>(
        config: &T,
        protocol: FrontendProto,
        threads: usize,
    ) -> Result<Self> {
        let mut builders = Vec::new();
        for _ in 0..threads {
            builders.push(FrontendWorkerBuilder::new(config, protocol.clone())?);
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
            Queues<(BackendRequest, Token), (BackendRequest, BackendResponse, Token)>,
        >,
        mut session_queues: Vec<Queues<Session, Session>>,
        mut signal_queues: Vec<Queues<(), Signal>>,
    ) -> Vec<
        FrontendWorker<
            FrontendProto,
            FrontendRequest,
            FrontendResponse,
            BackendRequest,
            BackendResponse,
        >,
    > {
        self.builders
            .drain(..)
            .map(|b| {
                b.build(
                    data_queues.pop().unwrap(),
                    session_queues.pop().unwrap(),
                    signal_queues.pop().unwrap(),
                )
            })
            .collect()
    }
}
