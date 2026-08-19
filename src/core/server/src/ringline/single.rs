use super::{Parsed, RequestStart, RinglineSession};
use crate::PROCESS_REQ;
use entrystore::EntryStore;
use logger::Klog;
use pelikan_net::ringline::{self, AsyncEventHandler, ConnCtx, ParseResult};
use protocol_common::{Compose, Execute, Protocol};
use session::{SESSION_RECV, SESSION_RECV_BYTE, SESSION_RECV_EX, SESSION_SEND, SESSION_SEND_BYTE};
use std::any::Any;
use std::cell::RefCell;
use std::io;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

static FLUSH_REQUESTED: AtomicBool = AtomicBool::new(false);

pub(super) fn record_receive(observed: usize, buffered: &mut usize) {
    SESSION_RECV.increment();
    SESSION_RECV_BYTE.add(observed.saturating_sub(*buffered) as u64);
    *buffered = observed;
}

pub(super) fn record_transport_receive_error() {
    // Mio Session::fill counts every read attempt, then counts only a
    // non-WouldBlock transport failure as a receive exception.
    SESSION_RECV.increment();
    SESSION_RECV_EX.increment();
}

pub(super) fn record_send() {
    SESSION_SEND.increment();
}

pub(super) fn record_send_bytes(bytes: usize) {
    SESSION_SEND_BYTE.add(bytes as u64);
}

pub(crate) fn request_flush() {
    FLUSH_REQUESTED.store(true, Ordering::Release);
}
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;

thread_local! {
    static WORKER_STATE: RefCell<Option<Box<dyn Any>>> = RefCell::new(None);
}

pub(crate) struct WorkerBootstrap<P, Storage> {
    protocol: Option<P>,
    storage: Option<Storage>,
    committed: Arc<AtomicBool>,
    recovery: Sender<(P, Storage)>,
}

struct WorkerState<P, Storage> {
    bootstrap: WorkerBootstrap<P, Storage>,
}

impl<P, Storage> Drop for WorkerBootstrap<P, Storage> {
    fn drop(&mut self) {
        if !self.committed.load(Ordering::Acquire) {
            if let (Some(protocol), Some(storage)) = (self.protocol.take(), self.storage.take()) {
                if let Err(error) = self.recovery.send((protocol, storage)) {
                    error!("failed to return Ringline startup state for fallback: {error}");
                }
            }
        }
    }
}

pub(crate) struct StartupRecovery<P, Storage> {
    committed: Arc<AtomicBool>,
    receiver: Receiver<(P, Storage)>,
}

impl<P, Storage> StartupRecovery<P, Storage> {
    pub(crate) fn commit(self) {
        self.committed.store(true, Ordering::Release);
    }

    pub(crate) fn recover(self) -> io::Result<(P, Storage)> {
        self.receiver.recv().map_err(|error| {
            io::Error::other(format!(
                "Ringline startup did not return worker state for mio fallback: {error}"
            ))
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessOutcome {
    Complete {
        consumed: usize,
        response: Vec<u8>,
        hangup: bool,
    },
    NeedMore,
}

#[derive(Debug, Eq, PartialEq)]
enum SendAction {
    Send,
    Skip,
}

impl SendAction {
    fn for_response(response: &[u8]) -> Self {
        if response.is_empty() {
            Self::Skip
        } else {
            Self::Send
        }
    }
}

/// Single-worker Ringline handler.
///
/// All request state lives in worker thread-local storage. The function-pointer
/// phantom keeps this value zero-sized without making it inherit the Send
/// properties of the protocol, request, response, or storage types.
type HandlerTypes<P, Request, Response, Storage> = fn() -> (P, Request, Response, Storage);

pub struct SingleHandler<P, Request, Response, Storage> {
    _types: PhantomData<HandlerTypes<P, Request, Response, Storage>>,
}

impl<P, Request, Response, Storage> SingleHandler<P, Request, Response, Storage> {
    pub(crate) fn new(
        protocol: P,
        storage: Storage,
    ) -> (
        Self,
        WorkerBootstrap<P, Storage>,
        StartupRecovery<P, Storage>,
    ) {
        let committed = Arc::new(AtomicBool::new(false));
        let (recovery, receiver) = mpsc::channel();
        (
            Self {
                _types: PhantomData,
            },
            WorkerBootstrap {
                protocol: Some(protocol),
                storage: Some(storage),
                committed: Arc::clone(&committed),
                recovery,
            },
            StartupRecovery {
                committed,
                receiver,
            },
        )
    }

    fn install(bootstrap: WorkerBootstrap<P, Storage>)
    where
        P: 'static,
        Storage: 'static,
    {
        WORKER_STATE.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(WorkerState { bootstrap }));
        });
    }

    fn with_state<T>(f: impl FnOnce(&mut WorkerState<P, Storage>) -> io::Result<T>) -> io::Result<T>
    where
        P: 'static,
        Storage: 'static,
    {
        WORKER_STATE.with(|slot| {
            let mut slot = slot
                .try_borrow_mut()
                .map_err(|_| io::Error::other("recursive Ringline single-worker storage access"))?;
            let state = slot
                .as_mut()
                .ok_or_else(|| io::Error::other("Ringline single-worker state is not installed"))?
                .downcast_mut::<WorkerState<P, Storage>>()
                .ok_or_else(|| io::Error::other("Ringline single-worker state type mismatch"))?;
            f(state)
        })
    }

    fn protocol() -> io::Result<P>
    where
        P: Clone + 'static,
        Storage: 'static,
    {
        Self::with_state(|state| {
            state
                .bootstrap
                .protocol
                .as_ref()
                .cloned()
                .ok_or_else(|| io::Error::other("Ringline worker protocol is unavailable"))
        })
    }

    fn process_with_session(
        session: &mut RinglineSession<P, Request, Response>,
        data: &[u8],
    ) -> io::Result<ProcessOutcome>
    where
        P: Protocol<Request, Response> + 'static,
        Request: Klog<Response = Response>,
        Response: Compose,
        Storage: Execute<Request, Response> + 'static,
    {
        let parsed = session.parse_at(data, RequestStart::now())?;
        let Parsed::Complete { request, consumed } = parsed else {
            return Ok(ProcessOutcome::NeedMore);
        };

        let response = Self::with_state(|state| {
            let storage = state
                .bootstrap
                .storage
                .as_mut()
                .ok_or_else(|| io::Error::other("Ringline worker storage is unavailable"))?;
            Ok(storage.execute(&request))
        })?;
        PROCESS_REQ.increment();
        request.klog(&response);
        let hangup = response.should_hangup();
        let bytes = session.compose(&response).to_vec();

        Ok(ProcessOutcome::Complete {
            consumed,
            response: bytes,
            hangup,
        })
    }

    #[cfg(test)]
    fn for_test(protocol: P, storage: Storage) -> Self
    where
        P: 'static,
        Storage: 'static,
    {
        let (handler, bootstrap, recovery) = Self::new(protocol, storage);
        recovery.commit();
        Self::install(bootstrap);
        handler
    }

    #[cfg(test)]
    fn process(&self, data: &[u8]) -> io::Result<ProcessOutcome>
    where
        P: Protocol<Request, Response> + Clone + 'static,
        Request: Klog<Response = Response>,
        Response: Compose,
        Storage: Execute<Request, Response> + 'static,
    {
        let mut session = RinglineSession::new(Self::protocol()?);
        Self::process_with_session(&mut session, data)
    }
}

impl<P, Request, Response, Storage> AsyncEventHandler
    for SingleHandler<P, Request, Response, Storage>
where
    P: Protocol<Request, Response> + Clone + Send + 'static,
    Request: Klog<Response = Response> + 'static,
    Response: Compose + 'static,
    Storage: Execute<Request, Response> + EntryStore + Send + 'static,
{
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
        let protocol = Self::protocol();
        async move {
            let protocol = match protocol {
                Ok(protocol) => protocol,
                Err(error) => {
                    error!("Ringline connection setup failed: {error}");
                    return;
                }
            };
            let mut session = RinglineSession::new(protocol);
            let mut buffered_bytes = 0_usize;

            loop {
                let mut outcome = None;
                let mut terminal_error = None;
                let receive_result = conn
                    .with_data_result(|data| {
                        record_receive(data.len(), &mut buffered_bytes);
                        match Self::process_with_session(&mut session, data) {
                            Ok(ProcessOutcome::Complete {
                                consumed,
                                response,
                                hangup,
                            }) => {
                                outcome = Some((response, hangup));
                                ParseResult::Consumed(consumed)
                            }
                            Ok(ProcessOutcome::NeedMore) => ParseResult::NeedMore,
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

                let Some((response, hangup)) = outcome else {
                    error!("Ringline consumed request bytes without producing a response");
                    break;
                };

                record_send();
                if SendAction::for_response(&response) == SendAction::Skip {
                    if hangup {
                        break;
                    }
                    continue;
                }

                let sent = match conn.send_backpressured(&response).await {
                    Ok(sent) => sent,
                    Err(error) => {
                        error!("Ringline response send failed: {error}");
                        break;
                    }
                };
                let sent = sent as usize;
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
        if FLUSH_REQUESTED.swap(false, Ordering::AcqRel) {
            if let Err(error) = Self::with_state(|state| {
                state
                    .bootstrap
                    .storage
                    .as_mut()
                    .ok_or_else(|| io::Error::other("Ringline worker storage is unavailable"))?
                    .clear();
                Ok(())
            }) {
                error!("Ringline storage flush failed: {error}");
            }
        }
    }

    fn on_tick(&mut self, _ctx: &mut ringline::DriverCtx<'_>) {
        if let Err(error) = Self::with_state(|state| {
            state
                .bootstrap
                .storage
                .as_mut()
                .ok_or_else(|| io::Error::other("Ringline worker storage is unavailable"))?
                .expire();
            Ok(())
        }) {
            error!("Ringline storage expiration failed: {error}");
        }
    }

    fn create_for_worker(worker_id: usize) -> Self {
        let bootstrap = ringline::take_worker_bootstrap::<WorkerBootstrap<P, Storage>>(worker_id);
        Self::install(bootstrap);
        Self {
            _types: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProcessOutcome, SendAction, SingleHandler};
    use entrystore::EntryStore;
    use logger::Klog;
    use protocol_common::{BufMut, Compose, Execute, ParseOk, Protocol};
    use std::cell::{Cell, RefCell};
    use std::io::{self, ErrorKind};
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct LineProtocol;

    #[derive(Debug, PartialEq, Eq)]
    struct LineRequest(Vec<u8>);

    struct LineResponse {
        bytes: Vec<u8>,
        hangup: bool,
    }

    impl Klog for LineRequest {
        type Response = LineResponse;

        fn klog(&self, _response: &Self::Response) {}
    }

    impl Compose for LineResponse {
        fn compose(&self, dst: &mut dyn BufMut) -> usize {
            dst.put_slice(&self.bytes);
            self.bytes.len()
        }

        fn should_hangup(&self) -> bool {
            self.hangup
        }
    }

    impl Protocol<LineRequest, LineResponse> for LineProtocol {
        fn parse_request(&self, buffer: &[u8]) -> io::Result<ParseOk<LineRequest>> {
            if buffer.starts_with(b"!") {
                return Err(io::Error::new(ErrorKind::InvalidData, "invalid line"));
            }
            let Some(end) = buffer.iter().position(|byte| *byte == b'\n') else {
                return Err(io::Error::from(ErrorKind::WouldBlock));
            };

            Ok(ParseOk::new(LineRequest(buffer[..end].to_vec()), end + 1))
        }

        fn compose_request(
            &self,
            _request: &LineRequest,
            _buffer: &mut dyn BufMut,
        ) -> io::Result<usize> {
            unreachable!("server tests do not compose requests")
        }

        fn parse_response(
            &self,
            _request: &LineRequest,
            _buffer: &[u8],
        ) -> io::Result<ParseOk<LineResponse>> {
            unreachable!("server tests do not parse responses")
        }

        fn compose_response(
            &self,
            _request: &LineRequest,
            _response: &LineResponse,
            _buffer: &mut dyn BufMut,
        ) -> io::Result<usize> {
            unreachable!("server tests compose through Compose")
        }
    }

    struct EchoStorage {
        executions: Rc<Cell<usize>>,
    }

    impl EchoStorage {
        fn new(executions: Rc<Cell<usize>>) -> Self {
            Self { executions }
        }
    }

    impl EntryStore for EchoStorage {
        fn clear(&mut self) {}
    }

    impl Execute<LineRequest, LineResponse> for EchoStorage {
        fn execute(&mut self, request: &LineRequest) -> LineResponse {
            self.executions.set(self.executions.get() + 1);
            LineResponse {
                bytes: request.0.clone(),
                hangup: false,
            }
        }
    }

    #[test]
    fn complete_request_executes_and_composes() {
        let executions = Rc::new(Cell::new(0));
        let handler =
            SingleHandler::for_test(LineProtocol, EchoStorage::new(Rc::clone(&executions)));

        let ProcessOutcome::Complete {
            consumed,
            response,
            hangup,
        } = handler.process(b"hello\n").unwrap()
        else {
            panic!("complete line must produce a response");
        };

        assert_eq!(consumed, 6);
        assert_eq!(response, b"hello");
        assert!(!hangup);
        assert_eq!(executions.get(), 1);
    }

    #[test]
    fn incomplete_request_does_not_execute() {
        let executions = Rc::new(Cell::new(0));
        let handler =
            SingleHandler::for_test(LineProtocol, EchoStorage::new(Rc::clone(&executions)));

        assert!(matches!(
            handler.process(b"hello").unwrap(),
            ProcessOutcome::NeedMore
        ));
        assert_eq!(executions.get(), 0);
    }

    struct RecursiveStorage {
        nested_error: Rc<RefCell<Option<io::Error>>>,
        recurse: bool,
    }

    impl EntryStore for RecursiveStorage {
        fn clear(&mut self) {}
    }

    impl Execute<LineRequest, LineResponse> for RecursiveStorage {
        fn execute(&mut self, request: &LineRequest) -> LineResponse {
            if self.recurse {
                self.recurse = false;
                let handler =
                    SingleHandler::<LineProtocol, LineRequest, LineResponse, RecursiveStorage> {
                        _types: std::marker::PhantomData,
                    };
                *self.nested_error.borrow_mut() = handler.process(b"nested\n").err();
            }
            LineResponse {
                bytes: request.0.clone(),
                hangup: false,
            }
        }
    }

    #[test]
    fn recursive_storage_access_returns_controlled_borrow_error() {
        let nested_error = Rc::new(RefCell::new(None));
        let handler = SingleHandler::for_test(
            LineProtocol,
            RecursiveStorage {
                nested_error: Rc::clone(&nested_error),
                recurse: true,
            },
        );

        assert!(matches!(
            handler.process(b"outer\n"),
            Ok(ProcessOutcome::Complete { .. })
        ));
        let error = nested_error
            .borrow_mut()
            .take()
            .expect("recursive access must return an error");
        assert_eq!(
            error.to_string(),
            "recursive Ringline single-worker storage access"
        );
    }

    #[test]
    fn empty_response_skips_ringline_send() {
        assert_eq!(SendAction::for_response(&[]), SendAction::Skip);
    }

    #[cfg(feature = "ringline-force-mio")]
    mod live_metrics {
        use super::*;
        use session::{
            SESSION_RECV, SESSION_RECV_BYTE, SESSION_RECV_EX, SESSION_SEND, SESSION_SEND_BYTE,
            SESSION_SEND_EX,
        };
        struct SendEchoStorage;

        impl EntryStore for SendEchoStorage {
            fn clear(&mut self) {}
        }

        impl Execute<LineRequest, LineResponse> for SendEchoStorage {
            fn execute(&mut self, request: &LineRequest) -> LineResponse {
                LineResponse {
                    bytes: request.0.clone(),
                    hangup: false,
                }
            }
        }

        static LIVE_METRIC_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

        fn launch_live_handler() -> pelikan_net::ringline::RinglineRuntime {
            let (_handler, bootstrap, recovery) =
                SingleHandler::<LineProtocol, LineRequest, LineResponse, SendEchoStorage>::new(
                    LineProtocol,
                    SendEchoStorage,
                );
            let runtime = pelikan_net::ringline::launch_with_bootstraps::<
                SingleHandler<LineProtocol, LineRequest, LineResponse, SendEchoStorage>,
                _,
            >(
                "127.0.0.1:0".parse().unwrap(),
                pelikan_net::ringline::RinglineRuntimeConfig {
                    workers: 1,
                    max_connections: 16,
                    recv_buffers: 16,
                    recv_buffer_size: 4096,
                    pin_to_core: false,
                },
                vec![bootstrap],
            )
            .expect("live Ringline launch failed");
            recovery.commit();
            runtime
        }

        fn wait_for_counter(counter: &metriken::Counter, before: u64) {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while counter.value() == before && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert!(counter.value() > before, "metric did not advance");
        }

        #[test]
        fn live_protocol_error_is_not_counted_as_transport_receive_error() {
            use std::io::Write;

            let _guard = LIVE_METRIC_TEST_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let recv_before = SESSION_RECV.value();
            let recv_ex_before = SESSION_RECV_EX.value();
            let runtime = launch_live_handler();
            let mut stream = std::net::TcpStream::connect(runtime.bound_addr().unwrap()).unwrap();
            stream.write_all(b"!\n").unwrap();
            wait_for_counter(&SESSION_RECV, recv_before);
            assert_eq!(SESSION_RECV_EX.value(), recv_ex_before);
            drop(stream);
            runtime.join().unwrap();
        }

        #[cfg(unix)]
        #[test]
        fn live_tcp_reset_counts_one_transport_receive_error() {
            use std::os::fd::AsRawFd;

            let _guard = LIVE_METRIC_TEST_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let recv_before = SESSION_RECV.value();
            let recv_ex_before = SESSION_RECV_EX.value();
            let runtime = launch_live_handler();
            let stream = std::net::TcpStream::connect(runtime.bound_addr().unwrap()).unwrap();
            let linger = libc::linger {
                l_onoff: 1,
                l_linger: 0,
            };
            assert_eq!(
                unsafe {
                    libc::setsockopt(
                        stream.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_LINGER,
                        &linger as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&linger) as libc::socklen_t,
                    )
                },
                0
            );
            drop(stream);
            wait_for_counter(&SESSION_RECV_EX, recv_ex_before);
            assert_eq!(SESSION_RECV.value() - recv_before, 1);
            assert_eq!(SESSION_RECV_EX.value() - recv_ex_before, 1);
            runtime.join().unwrap();
        }

        #[test]
        fn live_success_matches_mio_receive_and_send_metrics() {
            use std::io::{Read, Write};

            let _guard = LIVE_METRIC_TEST_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let recv_before = SESSION_RECV.value();
            let recv_byte_before = SESSION_RECV_BYTE.value();
            let recv_ex_before = SESSION_RECV_EX.value();
            let send_before = SESSION_SEND.value();
            let send_byte_before = SESSION_SEND_BYTE.value();
            let send_ex_before = SESSION_SEND_EX.value();
            let runtime = launch_live_handler();
            let mut stream = std::net::TcpStream::connect(runtime.bound_addr().unwrap()).unwrap();
            stream.write_all(b"ok\n").unwrap();
            let mut response = [0; 2];
            stream.read_exact(&mut response).unwrap();
            assert_eq!(&response, b"ok");
            wait_for_counter(&SESSION_SEND_BYTE, send_byte_before);
            assert_eq!(SESSION_RECV.value() - recv_before, 1);
            assert_eq!(SESSION_RECV_BYTE.value() - recv_byte_before, 3);
            assert_eq!(SESSION_RECV_EX.value(), recv_ex_before);
            assert_eq!(SESSION_SEND.value() - send_before, 1);
            assert_eq!(SESSION_SEND_BYTE.value() - send_byte_before, 2);
            assert_eq!(SESSION_SEND_EX.value(), send_ex_before);
            drop(stream);
            runtime.join().unwrap();
        }
    }

    #[test]
    fn dropped_startup_bootstrap_returns_protocol_and_storage_for_fallback() {
        let executions = Rc::new(Cell::new(0));
        let (_handler, bootstrap, recovery) =
            SingleHandler::<LineProtocol, LineRequest, LineResponse, EchoStorage>::new(
                LineProtocol,
                EchoStorage::new(Rc::clone(&executions)),
            );

        drop(bootstrap);
        assert!(recovery.recover().is_ok());
    }
}
