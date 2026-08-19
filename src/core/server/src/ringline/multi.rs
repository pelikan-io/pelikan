use super::single::{
    record_receive, record_send, record_send_bytes, record_transport_receive_error,
};
use crate::workers::{STORAGE_EVENT_LOOP, STORAGE_QUEUE_DEPTH};
use crate::PROCESS_REQ;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use entrystore::EntryStore;
use logger::Klog;
use metriken::{metric, Counter};
use pelikan_net::ringline::{
    self, AsyncEventHandler, CompletionId, CompletionTable, ConnCtx, ParseResult, WakeHandle,
};
use protocol_common::{Compose, Execute, Protocol};
use std::any::Any;
use std::cell::RefCell;
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

#[metric(
    name = "ringline_storage_queue_full",
    description = "Ringline cache requests rejected because the storage queue is full"
)]
pub static RINGLINE_STORAGE_QUEUE_FULL: Counter = Counter::new();

#[metric(
    name = "ringline_storage_response_queue_full",
    description = "Ringline storage responses blocked by a full worker response queue"
)]
pub static RINGLINE_STORAGE_RESPONSE_QUEUE_FULL: Counter = Counter::new();

#[metric(
    name = "ringline_storage_response_queue_depth",
    description = "Depth of a Ringline worker response queue when storage enqueues a response"
)]
pub static RINGLINE_STORAGE_RESPONSE_QUEUE_DEPTH: metriken::AtomicHistogram =
    metriken::AtomicHistogram::new(7, 20);

thread_local! {
    static WORKER_STATE: RefCell<Option<Box<dyn Any>>> = RefCell::new(None);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DrainStats {
    pub(crate) delivered: usize,
    pub(crate) stale: usize,
}

pub(crate) struct StorageResponse<T> {
    completion_id: CompletionId,
    value: T,
}

impl<T> StorageResponse<T> {
    pub(crate) fn new(completion_id: CompletionId, value: T) -> Self {
        Self {
            completion_id,
            value,
        }
    }
}

pub(crate) struct ResponseEnvelope<Request, Response> {
    request: Request,
    response: Response,
}

pub(crate) struct StorageRequest<Request> {
    pub(crate) worker_id: usize,
    pub(crate) completion_id: CompletionId,
    pub(crate) request: Request,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubmitError {
    Full,
    Disconnected,
}

fn try_submit_request<Request>(
    sender: &Sender<StorageRequest<Request>>,
    request: StorageRequest<Request>,
) -> Result<(), SubmitError> {
    match sender.try_send(request) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => {
            RINGLINE_STORAGE_QUEUE_FULL.increment();
            Err(SubmitError::Full)
        }
        Err(TrySendError::Disconnected(_)) => Err(SubmitError::Disconnected),
    }
}

pub(crate) struct ResponseSender<T> {
    sender: Sender<StorageResponse<T>>,
    wake: AttachOnce<WakeCallback>,
}

type WakeCallback = Arc<dyn Fn() + Send + Sync>;

struct AttachOnce<T>(Arc<OnceLock<T>>);

impl<T> AttachOnce<T> {
    fn new() -> Self {
        Self(Arc::new(OnceLock::new()))
    }

    fn attach(&self, value: T) -> Result<(), T> {
        self.0.set(value)
    }

    fn get(&self) -> Option<&T> {
        self.0.get()
    }

    fn is_attached(&self) -> bool {
        self.0.get().is_some()
    }
}

impl<T> Clone for AttachOnce<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Clone for ResponseSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            wake: self.wake.clone(),
        }
    }
}

impl<T> ResponseSender<T> {
    pub(crate) fn attach(&self, wake: WakeHandle) -> Result<(), WakeHandle> {
        let preserved = wake.clone();
        self.wake
            .attach(Arc::new(move || wake.wake()))
            .map_err(|_| preserved)
    }

    fn wake(&self) -> Result<(), io::Error> {
        self.wake
            .get()
            .ok_or_else(|| io::Error::other("Ringline worker wake handle is not attached"))?
            .as_ref()();
        Ok(())
    }
}

#[cfg(test)]
fn response_channel_with_callback<T, Factory, Callback>(
    capacity: usize,
    factory: Factory,
) -> (ResponseSender<T>, Receiver<StorageResponse<T>>)
where
    Factory: FnOnce(Receiver<StorageResponse<T>>) -> Callback,
    Callback: Fn() + Send + Sync + 'static,
{
    let (sender, receiver) = response_channel(capacity);
    sender
        .wake
        .attach(Arc::new(factory(receiver.clone())))
        .unwrap_or_else(|_| panic!("test response wake already attached"));
    (sender, receiver)
}

pub(crate) fn response_channel<T>(
    capacity: usize,
) -> (ResponseSender<T>, Receiver<StorageResponse<T>>) {
    let (sender, receiver) = crossbeam_channel::bounded(capacity);
    (
        ResponseSender {
            sender,
            wake: AttachOnce::new(),
        },
        receiver,
    )
}

pub(crate) fn all_response_wakes_attached<T>(senders: &[ResponseSender<T>]) -> bool {
    senders.iter().all(|sender| sender.wake.is_attached())
}

pub(crate) struct MultiBootstrap<P, Request, Response> {
    protocol: P,
    worker_id: usize,
    requests: Sender<StorageRequest<Request>>,
    responses: Receiver<StorageResponse<ResponseEnvelope<Request, Response>>>,
}

struct WorkerState<P, Request, Response> {
    protocol: P,
    worker_id: usize,
    requests: Sender<StorageRequest<Request>>,
    responses: Receiver<StorageResponse<ResponseEnvelope<Request, Response>>>,
    completions: CompletionTable<ResponseEnvelope<Request, Response>>,
}

type HandlerTypes<P, Request, Response> = fn() -> (P, Request, Response);

pub(crate) struct MultiHandler<P, Request, Response> {
    _types: PhantomData<HandlerTypes<P, Request, Response>>,
}

impl<P, Request, Response> MultiHandler<P, Request, Response> {
    pub(crate) fn new(
        protocol: P,
        worker_id: usize,
        requests: Sender<StorageRequest<Request>>,
        responses: Receiver<StorageResponse<ResponseEnvelope<Request, Response>>>,
    ) -> (Self, MultiBootstrap<P, Request, Response>) {
        (
            Self {
                _types: PhantomData,
            },
            MultiBootstrap {
                protocol,
                worker_id,
                requests,
                responses,
            },
        )
    }

    fn install(bootstrap: MultiBootstrap<P, Request, Response>)
    where
        P: 'static,
        Request: 'static,
        Response: 'static,
    {
        WORKER_STATE.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(WorkerState {
                protocol: bootstrap.protocol,
                worker_id: bootstrap.worker_id,
                requests: bootstrap.requests,
                responses: bootstrap.responses,
                completions: CompletionTable::new(),
            }));
        });
    }

    fn with_state<T>(
        f: impl FnOnce(&mut WorkerState<P, Request, Response>) -> io::Result<T>,
    ) -> io::Result<T>
    where
        P: 'static,
        Request: 'static,
        Response: 'static,
    {
        WORKER_STATE.with(|slot| {
            let mut slot = slot
                .try_borrow_mut()
                .map_err(|_| io::Error::other("recursive Ringline multi-worker state access"))?;
            let state = slot
                .as_mut()
                .ok_or_else(|| io::Error::other("Ringline multi-worker state is not installed"))?
                .downcast_mut::<WorkerState<P, Request, Response>>()
                .ok_or_else(|| io::Error::other("Ringline multi-worker state type mismatch"))?;
            f(state)
        })
    }

    fn drain_state_notifications(state: &mut WorkerState<P, Request, Response>) -> DrainStats {
        let mut stats = DrainStats {
            delivered: 0,
            stale: 0,
        };
        while let Ok(response) = state.responses.try_recv() {
            if state
                .completions
                .complete(response.completion_id, response.value)
                .is_ok()
            {
                stats.delivered += 1;
            } else {
                stats.stale += 1;
            }
        }
        stats
    }
}

impl<P, Request, Response> AsyncEventHandler for MultiHandler<P, Request, Response>
where
    P: Protocol<Request, Response> + Clone + Send + 'static,
    Request: Klog<Response = Response> + Send + 'static,
    Response: Compose + Send + 'static,
{
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
        let setup = Self::with_state(|state| {
            Ok((
                state.protocol.clone(),
                state.worker_id,
                state.requests.clone(),
                state.completions.clone(),
            ))
        });
        async move {
            let (protocol, worker_id, requests, completions) = match setup {
                Ok(setup) => setup,
                Err(error) => {
                    error!("Ringline multi-worker connection setup failed: {error}");
                    return;
                }
            };
            let mut session = super::RinglineSession::new(protocol);
            let mut buffered_bytes = 0_usize;

            loop {
                let mut parsed = None;
                let mut terminal_error = None;
                let receive_result = conn
                    .with_data_result(|data| {
                        record_receive(data.len(), &mut buffered_bytes);
                        match session.parse_at(data, super::RequestStart::now()) {
                            Ok(super::Parsed::Complete { request, consumed }) => {
                                parsed = Some(request);
                                ParseResult::Consumed(consumed)
                            }
                            Ok(super::Parsed::NeedMore) => ParseResult::NeedMore,
                            Err(error) => {
                                terminal_error = Some(error);
                                ParseResult::Consumed(data.len())
                            }
                        }
                    })
                    .await;

                let consumed = match receive_result {
                    Ok(consumed) => consumed,
                    Err(error) => {
                        record_transport_receive_error();
                        error!("Ringline transport receive failed: {error}");
                        break;
                    }
                };
                buffered_bytes = buffered_bytes.saturating_sub(consumed);
                if let Some(error) = terminal_error {
                    error!("Ringline request processing failed: {error}");
                    break;
                }
                if consumed == 0 {
                    break;
                }
                let Some(request) = parsed else {
                    error!("Ringline consumed request bytes without producing a request");
                    break;
                };

                let (completion_id, completion) = completions.insert();
                match try_submit_request(
                    &requests,
                    StorageRequest {
                        worker_id,
                        completion_id,
                        request,
                    },
                ) {
                    Ok(()) => {}
                    Err(SubmitError::Full) => {
                        error!("Ringline storage request queue is full");
                        break;
                    }
                    Err(SubmitError::Disconnected) => {
                        error!("Ringline storage request queue is disconnected");
                        break;
                    }
                }

                let envelope = match completion.await {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        error!("Ringline storage completion canceled: {error}");
                        break;
                    }
                };
                envelope.request.klog(&envelope.response);
                let hangup = envelope.response.should_hangup();
                let response = session.compose(&envelope.response).to_vec();
                record_send();
                if response.is_empty() {
                    if hangup {
                        break;
                    }
                    continue;
                }
                let sent = match conn.send_backpressured(&response).await {
                    Ok(sent) => sent as usize,
                    Err(error) => {
                        error!("Ringline response send failed: {error}");
                        break;
                    }
                };
                record_send_bytes(sent);
                session.response_completed(sent);
                if sent != response.len() {
                    error!(
                        "Ringline response send completed partially: sent {sent} of {} bytes",
                        response.len()
                    );
                    break;
                }
                if hangup {
                    break;
                }
            }
        }
    }

    fn on_notify(&mut self, _ctx: &mut ringline::DriverCtx<'_>) {
        match Self::with_state(|state| Ok(Self::drain_state_notifications(state))) {
            Ok(stats) if stats.stale > 0 => {
                trace!("discarded {} stale Ringline storage responses", stats.stale);
            }
            Ok(_) => {}
            Err(error) => error!("Ringline response notification failed: {error}"),
        }
    }

    fn create_for_worker(worker_id: usize) -> Self {
        let bootstrap =
            ringline::take_worker_bootstrap::<MultiBootstrap<P, Request, Response>>(worker_id);
        Self::install(bootstrap);
        Self {
            _types: PhantomData,
        }
    }
}

pub(crate) struct RinglineStorageWorker<Request, Response, Storage> {
    requests: Receiver<StorageRequest<Request>>,
    responses: Vec<ResponseSender<ResponseEnvelope<Request, Response>>>,
    signals: Receiver<common::signal::Signal>,
    storage: Storage,
    timeout: Duration,
}

impl<Request, Response, Storage> RinglineStorageWorker<Request, Response, Storage>
where
    Storage: Execute<Request, Response> + EntryStore,
    Response: Compose,
{
    pub(crate) fn new(
        requests: Receiver<StorageRequest<Request>>,
        responses: Vec<ResponseSender<ResponseEnvelope<Request, Response>>>,
        signals: Receiver<common::signal::Signal>,
        storage: Storage,
        timeout: Duration,
    ) -> Self {
        Self {
            requests,
            responses,
            signals,
            storage,
            timeout,
        }
    }

    pub(crate) fn run(mut self) {
        let expiration = crossbeam_channel::tick(self.timeout);
        let mut requests = Vec::with_capacity(1024);
        loop {
            STORAGE_EVENT_LOOP.increment();
            crossbeam_channel::select! {
                recv(self.signals) -> signal => match signal {
                    Ok(common::signal::Signal::FlushAll) => self.storage.clear(),
                    Ok(common::signal::Signal::Shutdown) | Err(_) => return,
                },
                recv(self.requests) -> request => {
                    let Ok(request) = request else { return; };
                    requests.push(request);
                    requests.extend(self.requests.try_iter());
                    let _ = STORAGE_QUEUE_DEPTH.increment(requests.len() as u64);
                    self.storage.expire();
                    for request in requests.drain(..) {
                        let response = self.storage.execute(&request.request);
                        PROCESS_REQ.increment();
                        let Some(sender) = self.responses.get(request.worker_id) else {
                            error!("Ringline storage response has invalid worker id {}", request.worker_id);
                            continue;
                        };
                        let response = StorageResponse::new(
                            request.completion_id,
                            ResponseEnvelope { request: request.request, response },
                        );
                        match sender.sender.try_send(response) {
                            Ok(()) => {
                                let _ = RINGLINE_STORAGE_RESPONSE_QUEUE_DEPTH
                                    .increment(sender.sender.len() as u64);
                                if let Err(error) = sender.wake() {
                                    error!("failed to wake Ringline response worker: {error}");
                                    return;
                                }
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                error!("Ringline response queue disconnected");
                                return;
                            }
                            Err(TrySendError::Full(response)) => {
                                RINGLINE_STORAGE_RESPONSE_QUEUE_FULL.increment();
                                let _ = RINGLINE_STORAGE_RESPONSE_QUEUE_DEPTH
                                    .increment(sender.sender.len() as u64);
                                loop {
                                    crossbeam_channel::select! {
                                        send(sender.sender, response) -> result => {
                                            if result.is_err() {
                                                error!("Ringline response queue disconnected");
                                                return;
                                            }
                                            if let Err(error) = sender.wake() {
                                                error!("failed to wake Ringline response worker: {error}");
                                                return;
                                            }
                                            break;
                                        },
                                        recv(self.signals) -> signal => match signal {
                                            Ok(common::signal::Signal::FlushAll) => self.storage.clear(),
                                            Ok(common::signal::Signal::Shutdown) | Err(_) => return,
                                        },
                                    }
                                }
                            }
                        }
                    }
                },
                recv(expiration) -> _ => self.storage.expire(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DrainStats, StorageRequest, StorageResponse};
    use crate::workers::{STORAGE_EVENT_LOOP, STORAGE_QUEUE_DEPTH};
    use common::signal::Signal;
    use entrystore::EntryStore;
    use pelikan_net::ringline::Completion;
    use protocol_common::{BufMut, Compose, Execute};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::thread;
    use std::time::Duration;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn test_guard() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait_until(message: &str, condition: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !condition() {
            assert!(std::time::Instant::now() < deadline, "{message}");
            thread::yield_now();
        }
    }

    #[derive(Clone)]
    struct LineProtocol;

    #[derive(Debug, Eq, PartialEq)]
    struct TestResponse(u64);

    impl Compose for TestResponse {
        fn compose(&self, dst: &mut dyn BufMut) -> usize {
            let bytes = self.0.to_string();
            dst.put_slice(bytes.as_bytes());
            bytes.len()
        }
    }

    #[derive(Default)]
    struct StorageCounts {
        clear: AtomicUsize,
        execute: AtomicUsize,
        expire: AtomicUsize,
    }

    struct TestStorage(Arc<StorageCounts>);

    impl EntryStore for TestStorage {
        fn expire(&mut self) {
            self.0.expire.fetch_add(1, Ordering::SeqCst);
        }

        fn clear(&mut self) {
            self.0.clear.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Execute<u64, TestResponse> for TestStorage {
        fn execute(&mut self, request: &u64) -> TestResponse {
            self.0.execute.fetch_add(1, Ordering::SeqCst);
            TestResponse(*request + 100)
        }
    }

    fn completion(slot: u32) -> pelikan_net::ringline::CompletionId {
        pelikan_net::ringline::CompletionId {
            slot,
            generation: 0,
        }
    }

    fn poll_once<T>(
        future: &mut Completion<T>,
    ) -> Poll<Result<T, pelikan_net::ringline::CompletionCanceled>> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        Pin::new(future).poll(&mut context)
    }

    #[test]
    fn notify_completes_only_matching_connection_future() {
        let (response_tx, response_rx) = crossbeam_channel::bounded(4);
        let completions = pelikan_net::ringline::CompletionTable::new();
        let (first_id, mut first) = completions.insert();
        let (_second_id, mut second) = completions.insert();
        response_tx
            .send(StorageResponse::new(
                first_id,
                super::ResponseEnvelope {
                    request: (),
                    response: b"one".to_vec(),
                },
            ))
            .unwrap();
        let mut state = super::WorkerState {
            protocol: LineProtocol,
            worker_id: 0,
            requests: crossbeam_channel::bounded(1).0,
            responses: response_rx,
            completions,
        };

        super::MultiHandler::<LineProtocol, (), Vec<u8>>::drain_state_notifications(&mut state);

        let Poll::Ready(Ok(envelope)) = poll_once(&mut first) else {
            panic!("matching completion was not delivered");
        };
        assert_eq!(envelope.response, b"one".to_vec());
        assert!(poll_once(&mut second).is_pending());
    }

    #[test]
    fn canceled_connection_discards_delayed_response() {
        let (response_tx, response_rx) = crossbeam_channel::bounded(4);
        let completions = pelikan_net::ringline::CompletionTable::new();
        let (id, future) = completions.insert();
        drop(future);
        response_tx
            .send(StorageResponse::new(
                id,
                super::ResponseEnvelope {
                    request: (),
                    response: b"late".to_vec(),
                },
            ))
            .unwrap();
        let mut state = super::WorkerState {
            protocol: LineProtocol,
            worker_id: 0,
            requests: crossbeam_channel::bounded(1).0,
            responses: response_rx,
            completions,
        };

        assert_eq!(
            super::MultiHandler::<LineProtocol, (), Vec<u8>>::drain_state_notifications(&mut state),
            DrainStats {
                delivered: 0,
                stale: 1,
            }
        );
    }

    #[test]
    fn wake_cells_attach_once_and_require_every_worker_before_storage_start() {
        let first = super::AttachOnce::new();
        let second = super::AttachOnce::new();

        assert!(!first.is_attached());
        assert!(!second.is_attached());
        assert_eq!(first.attach(11), Ok(()));
        assert_eq!(first.attach(12), Err(12));
        assert_eq!(first.get(), Some(&11));
        assert!(!second.is_attached());
        assert_eq!(second.attach(22), Ok(()));
        assert!(first.is_attached() && second.is_attached());
    }

    #[test]
    fn saturated_request_queue_rejects_and_increments_pressure_metric() {
        let _guard = test_guard();
        let (sender, _receiver) = crossbeam_channel::bounded(1);
        sender
            .send(StorageRequest {
                worker_id: 0,
                completion_id: completion(0),
                request: 1_u64,
            })
            .unwrap();
        let before = super::RINGLINE_STORAGE_QUEUE_FULL.value();

        let result = super::try_submit_request(
            &sender,
            StorageRequest {
                worker_id: 0,
                completion_id: completion(1),
                request: 2_u64,
            },
        );

        assert_eq!(result, Err(super::SubmitError::Full));
        assert_eq!(super::RINGLINE_STORAGE_QUEUE_FULL.value() - before, 1);
    }

    #[test]
    fn responses_route_exactly_and_are_enqueued_before_worker_wake() {
        let _guard = test_guard();
        let counts = Arc::new(StorageCounts::default());
        let (request_tx, request_rx) = crossbeam_channel::bounded(4);
        let (signal_tx, signal_rx) = crossbeam_channel::bounded(4);
        let wake_zero = Arc::new(AtomicUsize::new(0));
        let wake_one = Arc::new(AtomicUsize::new(0));
        let (observed_zero_tx, observed_zero_rx) = crossbeam_channel::bounded(2);
        let (observed_one_tx, observed_one_rx) = crossbeam_channel::bounded(2);
        let wake_zero_assert = Arc::clone(&wake_zero);
        let (sender_zero, receiver_zero) =
            super::response_channel_with_callback(2, move |receiver| {
                let wakes = Arc::clone(&wake_zero);
                move || {
                    observed_zero_tx
                        .send(receiver.try_recv().expect("wake preceded enqueue"))
                        .unwrap();
                    wakes.fetch_add(1, Ordering::SeqCst);
                }
            });
        let wake_one_assert = Arc::clone(&wake_one);
        let (sender_one, receiver_one) =
            super::response_channel_with_callback(2, move |receiver| {
                let wakes = Arc::clone(&wake_one);
                move || {
                    observed_one_tx
                        .send(receiver.try_recv().expect("wake preceded enqueue"))
                        .unwrap();
                    wakes.fetch_add(1, Ordering::SeqCst);
                }
            });
        let worker = super::RinglineStorageWorker::new(
            request_rx,
            vec![sender_zero, sender_one],
            signal_rx,
            TestStorage(Arc::clone(&counts)),
            Duration::from_secs(60),
        );
        let join = thread::spawn(move || worker.run());

        request_tx
            .send(StorageRequest {
                worker_id: 1,
                completion_id: completion(11),
                request: 7,
            })
            .unwrap();
        request_tx
            .send(StorageRequest {
                worker_id: 0,
                completion_id: completion(10),
                request: 5,
            })
            .unwrap();

        let one = observed_one_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let zero = observed_zero_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert_eq!(one.completion_id, completion(11));
        assert_eq!(one.value.response, TestResponse(107));
        assert_eq!(zero.completion_id, completion(10));
        assert_eq!(zero.value.response, TestResponse(105));
        assert_eq!(wake_zero_assert.load(Ordering::SeqCst), 1);
        assert_eq!(wake_one_assert.load(Ordering::SeqCst), 1);
        assert!(receiver_zero.is_empty());
        assert!(receiver_one.is_empty());
        signal_tx.send(Signal::Shutdown).unwrap();
        join.join().unwrap();
    }

    #[test]
    fn saturated_response_queue_blocks_until_space_and_shutdown_interrupts_it() {
        let _guard = test_guard();
        let counts = Arc::new(StorageCounts::default());
        let (request_tx, request_rx) = crossbeam_channel::bounded(2);
        let (signal_tx, signal_rx) = crossbeam_channel::bounded(2);
        let wakes = Arc::new(AtomicUsize::new(0));
        let wake_callback = Arc::clone(&wakes);
        let pressure_before = super::RINGLINE_STORAGE_RESPONSE_QUEUE_FULL.value();
        let depth_before = super::RINGLINE_STORAGE_RESPONSE_QUEUE_DEPTH.load();
        let (sender, receiver) = super::response_channel_with_callback(1, move |_receiver| {
            let wakes = Arc::clone(&wake_callback);
            move || {
                wakes.fetch_add(1, Ordering::SeqCst);
            }
        });
        sender
            .sender
            .send(StorageResponse::new(
                completion(99),
                super::ResponseEnvelope {
                    request: 99,
                    response: TestResponse(199),
                },
            ))
            .unwrap();
        let worker = super::RinglineStorageWorker::new(
            request_rx,
            vec![sender],
            signal_rx,
            TestStorage(Arc::clone(&counts)),
            Duration::from_secs(60),
        );
        let join = thread::spawn(move || worker.run());
        request_tx
            .send(StorageRequest {
                worker_id: 0,
                completion_id: completion(1),
                request: 1,
            })
            .unwrap();
        wait_until("storage did not execute saturated response", || {
            counts.execute.load(Ordering::SeqCst) > 0
        });
        assert_eq!(wakes.load(Ordering::SeqCst), 0);

        signal_tx.send(Signal::Shutdown).unwrap();
        join.join().unwrap();
        assert_eq!(receiver.len(), 1);
        assert_eq!(
            super::RINGLINE_STORAGE_RESPONSE_QUEUE_FULL.value() - pressure_before,
            1
        );
        let depth_after = super::RINGLINE_STORAGE_RESPONSE_QUEUE_DEPTH.load().unwrap();
        let depth_delta = match depth_before {
            Some(before) => depth_after.wrapping_sub(&before).unwrap(),
            None => depth_after,
        };
        assert!(depth_delta
            .iter()
            .any(|bucket| bucket.count() == 1 && bucket.range().contains(&1)));
    }

    #[test]
    fn flush_during_saturated_response_send_preserves_the_blocked_response() {
        let _guard = test_guard();
        let counts = Arc::new(StorageCounts::default());
        let (request_tx, request_rx) = crossbeam_channel::bounded(2);
        let (signal_tx, signal_rx) = crossbeam_channel::bounded(2);
        let (sender, receiver) = super::response_channel_with_callback(1, |_receiver| || {});
        sender
            .sender
            .send(StorageResponse::new(
                completion(99),
                super::ResponseEnvelope {
                    request: 99,
                    response: TestResponse(199),
                },
            ))
            .unwrap();
        let worker = super::RinglineStorageWorker::new(
            request_rx,
            vec![sender],
            signal_rx,
            TestStorage(Arc::clone(&counts)),
            Duration::from_secs(60),
        );
        let join = thread::spawn(move || worker.run());
        request_tx
            .send(StorageRequest {
                worker_id: 0,
                completion_id: completion(1),
                request: 1,
            })
            .unwrap();
        wait_until("storage did not execute blocked flush response", || {
            counts.execute.load(Ordering::SeqCst) > 0
        });

        signal_tx.send(Signal::FlushAll).unwrap();
        wait_until("storage did not process FlushAll", || {
            counts.clear.load(Ordering::SeqCst) > 0
        });
        receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        let response = receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(response.completion_id, completion(1));
        assert_eq!(response.value.response, TestResponse(101));

        signal_tx.send(Signal::Shutdown).unwrap();
        join.join().unwrap();
    }

    #[test]
    fn storage_batches_preserve_metrics_expiration_cadence_and_flush_ownership() {
        let _guard = test_guard();
        let counts = Arc::new(StorageCounts::default());
        let (request_tx, request_rx) = crossbeam_channel::bounded(4);
        let (signal_tx, signal_rx) = crossbeam_channel::bounded(4);
        let (sender, receiver) = super::response_channel_with_callback(4, |_receiver| || {});
        let loops_before = STORAGE_EVENT_LOOP.value();
        let depth_before = STORAGE_QUEUE_DEPTH.load();
        let worker = super::RinglineStorageWorker::new(
            request_rx,
            vec![sender],
            signal_rx,
            TestStorage(Arc::clone(&counts)),
            Duration::from_secs(60),
        );
        request_tx
            .send(StorageRequest {
                worker_id: 0,
                completion_id: completion(1),
                request: 1,
            })
            .unwrap();
        request_tx
            .send(StorageRequest {
                worker_id: 0,
                completion_id: completion(2),
                request: 2,
            })
            .unwrap();
        let join = thread::spawn(move || worker.run());

        receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        signal_tx.send(Signal::FlushAll).unwrap();
        wait_until("storage did not process batch FlushAll", || {
            counts.clear.load(Ordering::SeqCst) > 0
        });
        signal_tx.send(Signal::Shutdown).unwrap();
        join.join().unwrap();

        assert_eq!(counts.execute.load(Ordering::SeqCst), 2);
        assert_eq!(counts.expire.load(Ordering::SeqCst), 1);
        assert_eq!(counts.clear.load(Ordering::SeqCst), 1);
        assert!(STORAGE_EVENT_LOOP.value() > loops_before);
        let depth_after = STORAGE_QUEUE_DEPTH.load().unwrap();
        let depth_delta = match depth_before {
            Some(before) => depth_after.wrapping_sub(&before).unwrap(),
            None => depth_after,
        };
        assert!(depth_delta
            .iter()
            .any(|bucket| bucket.count() == 1 && bucket.range().contains(&2)));
    }
}
