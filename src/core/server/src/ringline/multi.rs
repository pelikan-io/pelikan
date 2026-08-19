use super::single::{
    record_receive, record_send, record_send_bytes, record_transport_receive_error,
};
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

pub(crate) struct ResponseSender<T> {
    sender: Sender<StorageResponse<T>>,
    wake: AttachOnce<WakeHandle>,
}

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
        self.wake.attach(wake)
    }

    fn wake(&self) -> Result<(), io::Error> {
        self.wake
            .get()
            .ok_or_else(|| io::Error::other("Ringline worker wake handle is not attached"))?
            .wake();
        Ok(())
    }
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
                match requests.try_send(StorageRequest {
                    worker_id,
                    completion_id,
                    request,
                }) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        RINGLINE_STORAGE_QUEUE_FULL.increment();
                        error!("Ringline storage request queue is full");
                        break;
                    }
                    Err(TrySendError::Disconnected(_)) => {
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
        loop {
            crossbeam_channel::select! {
                recv(self.signals) -> signal => match signal {
                    Ok(common::signal::Signal::FlushAll) => self.storage.clear(),
                    Ok(common::signal::Signal::Shutdown) | Err(_) => return,
                },
                recv(self.requests) -> request => {
                    let Ok(request) = request else { return; };
                    self.storage.expire();
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
                },
                recv(expiration) -> _ => self.storage.expire(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DrainStats, StorageResponse};
    use pelikan_net::ringline::Completion;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    #[derive(Clone)]
    struct LineProtocol;

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
}
