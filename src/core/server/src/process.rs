// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use libc::c_int;
use protocol_common::Protocol;
use signal_hook::consts::signal::*;
use signal_hook::iterator::Signals;
use std::any::Any;
use std::io;
use std::thread::JoinHandle;

#[cfg(target_os = "linux")]
use crate::ringline::{
    all_response_wakes_attached, request_flush, response_channel, MultiHandler, ResponseEnvelope,
    RinglineStorageWorker, SingleHandler, StorageRequest,
};
#[cfg(target_os = "linux")]
use pelikan_net::ringline::{launch_with_bootstraps, RinglineRuntimeConfig};

#[derive(Debug, Eq, PartialEq)]
enum ProcessKind {
    Mio,
    #[cfg(target_os = "linux")]
    Ringline,
}

fn process_kind(resolution: pelikan_net::BackendResolution) -> ProcessKind {
    match resolution.active {
        pelikan_net::IoBackend::Mio => ProcessKind::Mio,
        #[cfg(target_os = "linux")]
        pelikan_net::IoBackend::Ringline => ProcessKind::Ringline,
        #[cfg(not(target_os = "linux"))]
        pelikan_net::IoBackend::Ringline => ProcessKind::Mio,
    }
}

#[derive(Clone)]
struct CacheConfig {
    server: config::Server,
    tls: config::Tls,
    worker: config::Worker,
}

impl ServerConfig for CacheConfig {
    fn server(&self) -> &config::Server {
        &self.server
    }

    fn server_mut(&mut self) -> &mut config::Server {
        &mut self.server
    }
}

impl TlsConfig for CacheConfig {
    fn tls(&self) -> &config::Tls {
        &self.tls
    }
}

impl WorkerConfig for CacheConfig {
    fn worker(&self) -> &config::Worker {
        &self.worker
    }

    fn worker_mut(&mut self) -> &mut config::Worker {
        &mut self.worker
    }
}

pub enum ProcessBuilder<Parser, Request, Response, Storage> {
    Mio(MioProcessBuilder<Parser, Request, Response, Storage>),
    #[cfg(target_os = "linux")]
    Ringline(RinglineProcessBuilder<Parser, Request, Response, Storage>),
}

pub struct MioProcessBuilder<Parser, Request, Response, Storage> {
    admin: AdminBuilder,
    config: CacheConfig,
    log_drain: LogDrain,
    protocol: Parser,
    storage: Storage,
    _request: PhantomData<Request>,
    _response: PhantomData<Response>,
}

#[cfg(target_os = "linux")]
pub struct RinglineProcessBuilder<Parser, Request, Response, Storage> {
    mio: MioProcessBuilder<Parser, Request, Response, Storage>,
}

impl<P, Request, Response, Storage> ProcessBuilder<P, Request, Response, Storage>
where
    P: 'static + Protocol<Request, Response> + Clone + Send,
    Request: 'static + Klog + Klog<Response = Response> + Send,
    Response: 'static + Compose + Send,
    Storage: 'static + Execute<Request, Response> + EntryStore + Send,
{
    pub fn new<T: AdminConfig + ServerConfig + TlsConfig + WorkerConfig>(
        config: &T,
        log_drain: LogDrain,
        protocol: P,
        storage: Storage,
    ) -> Result<Self> {
        let requested = pelikan_net::IoBackend::parse(config.server().io_backend())
            .map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
        let available = cfg!(target_os = "linux");
        let resolution = pelikan_net::resolve_backend(requested, available);
        let mio = MioProcessBuilder::new(config, log_drain, protocol, storage)?;

        match process_kind(resolution.clone()) {
            ProcessKind::Mio => {
                log_resolution(&resolution);
                Ok(Self::Mio(mio))
            }
            #[cfg(target_os = "linux")]
            ProcessKind::Ringline => match ringline_preflight(&mio) {
                Ok(()) => {
                    info!("cache server I/O backend requested=ringline active=pending");
                    Ok(Self::Ringline(RinglineProcessBuilder::single(mio)?))
                }
                Err(error) => {
                    log_ringline_fallback(&error);
                    Ok(Self::Mio(mio))
                }
            },
        }
    }

    pub fn version(mut self, version: &str) -> Self {
        match &mut self {
            Self::Mio(builder) => builder.admin.version(version),
            #[cfg(target_os = "linux")]
            Self::Ringline(builder) => builder.mio.admin.version(version),
        }
        self
    }

    pub fn spawn(self) -> Process {
        match self {
            Self::Mio(builder) => Process::Mio(builder.spawn()),
            #[cfg(target_os = "linux")]
            Self::Ringline(builder) => builder.spawn(),
        }
    }
}

static BACKEND_RESOLUTION: std::sync::Mutex<pelikan_net::BackendResolution> =
    std::sync::Mutex::new(pelikan_net::BackendResolution {
        requested: pelikan_net::IoBackend::Mio,
        active: pelikan_net::IoBackend::Mio,
        fallback: None,
    });

/// Returns the last terminal cache-server backend resolution, including cause.
pub fn backend_resolution() -> pelikan_net::BackendResolution {
    BACKEND_RESOLUTION
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

fn record_resolution(resolution: &pelikan_net::BackendResolution) {
    *BACKEND_RESOLUTION
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = resolution.clone();
    SERVER_IO_BACKEND_ACTIVE.set(match resolution.active {
        pelikan_net::IoBackend::Mio => 0,
        pelikan_net::IoBackend::Ringline => 1,
    });
    if resolution.fallback.is_some() {
        SERVER_IO_BACKEND_FALLBACK.increment();
    }
}

fn log_ringline_active() {
    log_resolution(&pelikan_net::BackendResolution {
        requested: pelikan_net::IoBackend::Ringline,
        active: pelikan_net::IoBackend::Ringline,
        fallback: None,
    });
}

#[cfg(target_os = "linux")]
fn ringline_fallback_reason(error: &pelikan_net::ringline::Error) -> pelikan_net::FallbackReason {
    match error {
        pelikan_net::ringline::Error::RingSetup(_) => {
            pelikan_net::FallbackReason::UnsupportedCapability(error.to_string())
        }
        pelikan_net::ringline::Error::Io(io_error)
            if matches!(
                io_error.raw_os_error(),
                Some(libc::EINVAL | libc::EPERM | libc::ENOSYS | libc::EOPNOTSUPP)
            ) =>
        {
            pelikan_net::FallbackReason::UnsupportedCapability(error.to_string())
        }
        _ => pelikan_net::FallbackReason::Initialization(error.to_string()),
    }
}

#[cfg(target_os = "linux")]
fn log_ringline_runtime_fallback(error: &pelikan_net::ringline::Error) {
    log_resolution(&pelikan_net::BackendResolution {
        requested: pelikan_net::IoBackend::Ringline,
        active: pelikan_net::IoBackend::Mio,
        fallback: Some(ringline_fallback_reason(error)),
    });
}

fn log_ringline_fallback(cause: impl std::fmt::Display) {
    log_resolution(&pelikan_net::BackendResolution {
        requested: pelikan_net::IoBackend::Ringline,
        active: pelikan_net::IoBackend::Mio,
        fallback: Some(pelikan_net::FallbackReason::Initialization(
            cause.to_string(),
        )),
    });
}

fn log_resolution(resolution: &pelikan_net::BackendResolution) {
    record_resolution(resolution);
    match &resolution.fallback {
        Some(reason) => info!(
            "cache server I/O backend requested_backend={} active_backend={} fallback_cause={reason}",
            resolution.requested, resolution.active
        ),
        None => info!(
            "cache server I/O backend requested_backend={} active_backend={}",
            resolution.requested, resolution.active
        ),
    }
}

impl<P, Request, Response, Storage> MioProcessBuilder<P, Request, Response, Storage>
where
    P: 'static + Protocol<Request, Response> + Clone + Send,
    Request: 'static + Klog + Klog<Response = Response> + Send,
    Response: 'static + Compose + Send,
    Storage: 'static + Execute<Request, Response> + EntryStore + Send,
{
    fn new<T: AdminConfig + ServerConfig + TlsConfig + WorkerConfig>(
        config: &T,
        log_drain: LogDrain,
        protocol: P,
        storage: Storage,
    ) -> Result<Self> {
        let admin = AdminBuilder::new(config)?;

        config.server().socket_addr().map_err(|error| {
            error!("{error}");
            Error::other("Bad listen address")
        })?;
        let _ = tls_acceptor(config.tls())?;

        Ok(Self {
            admin,
            config: CacheConfig {
                server: config.server().clone(),
                tls: config.tls().clone(),
                worker: config.worker().clone(),
            },
            log_drain,
            protocol,
            storage,
            _request: PhantomData,
            _response: PhantomData,
        })
    }

    fn spawn(self) -> MioProcess {
        let listener = ListenerBuilder::new(&self.config)
            .unwrap_or_else(|error| panic!("failed to initialize mio listener: {error}"));
        let workers = WorkersBuilder::new(&self.config, self.protocol, self.storage)
            .unwrap_or_else(|error| panic!("failed to initialize mio workers: {error}"));

        spawn_mio(self.admin, listener, workers, self.log_drain)
    }
}

fn spawn_mio<P, Request, Response, Storage>(
    admin: AdminBuilder,
    listener: ListenerBuilder,
    workers: WorkersBuilder<P, Request, Response, Storage>,
    log_drain: LogDrain,
) -> MioProcess
where
    P: 'static + Protocol<Request, Response> + Clone + Send,
    Request: 'static + Klog + Klog<Response = Response> + Send,
    Response: 'static + Compose + Send,
    Storage: 'static + Execute<Request, Response> + EntryStore + Send,
{
    let mut thread_wakers = vec![listener.waker()];
    thread_wakers.extend_from_slice(&workers.wakers());

    let (signal_tx, signal_rx) = bounded(QUEUE_CAPACITY);
    let (mut signal_queue_tx, mut signal_queue_rx) =
        Queues::new(vec![admin.waker()], thread_wakers, QUEUE_CAPACITY).unwrap();
    let (mut listener_session_queues, worker_session_queues) = Queues::new(
        vec![listener.waker()],
        workers.worker_wakers(),
        QUEUE_CAPACITY,
    )
    .unwrap();

    let mut admin = admin.build(log_drain, signal_rx, signal_queue_tx.remove(0));
    let mut listener = listener.build(signal_queue_rx.remove(0), listener_session_queues.remove(0));
    let workers = workers.build(worker_session_queues, signal_queue_rx);

    let admin = std::thread::Builder::new()
        .name(format!("{THREAD_PREFIX}_admin"))
        .spawn(move || admin.run())
        .unwrap();
    let listener = std::thread::Builder::new()
        .name(format!("{THREAD_PREFIX}_listener"))
        .spawn(move || listener.run())
        .unwrap();
    let workers = workers.spawn();

    spawn_signal_handler(signal_tx.clone());

    MioProcess {
        admin,
        listener,
        signal_tx,
        workers,
    }
}

#[cfg(target_os = "linux")]
fn ringline_preflight<P, Request, Response, Storage>(
    mio: &MioProcessBuilder<P, Request, Response, Storage>,
) -> Result<()> {
    if tls_acceptor(&mio.config.tls)?.is_some() {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "Ringline cache-server data listener supports plain TCP only",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
const RINGLINE_MAX_CONNECTIONS: u32 = 16_000;

#[cfg(target_os = "linux")]
impl<P, Request, Response, Storage> RinglineProcessBuilder<P, Request, Response, Storage>
where
    P: 'static + Protocol<Request, Response> + Clone + Send,
    Request: 'static + Klog + Klog<Response = Response> + Send,
    Response: 'static + Compose + Send,
    Storage: 'static + Execute<Request, Response> + EntryStore + Send,
{
    pub fn single(mio: MioProcessBuilder<P, Request, Response, Storage>) -> Result<Self> {
        ringline_preflight(&mio)?;
        Ok(Self { mio })
    }

    fn spawn(self) -> Process {
        let MioProcessBuilder {
            admin,
            config,
            log_drain,
            protocol,
            storage,
            ..
        } = self.mio;
        let addr = config
            .server
            .socket_addr()
            .expect("Ringline listen address was validated at builder creation");
        let max_connections = RINGLINE_MAX_CONNECTIONS;
        let workers = config.worker.threads();
        let runtime_config = RinglineRuntimeConfig {
            workers,
            max_connections,
            recv_buffers: 64,
            recv_buffer_size: 4096,
            pin_to_core: false,
        };
        if workers == 1 {
            return spawn_ringline_single(
                admin,
                config,
                log_drain,
                protocol,
                storage,
                addr,
                runtime_config,
            );
        }

        let fallback_protocol = protocol.clone();
        let (request_tx, request_rx) = bounded::<StorageRequest<Request>>(QUEUE_CAPACITY);
        let mut response_senders = Vec::with_capacity(workers);
        let mut bootstraps = Vec::with_capacity(workers);
        for worker_id in 0..workers {
            let (response_tx, response_rx) =
                response_channel::<ResponseEnvelope<Request, Response>>(QUEUE_CAPACITY);
            let (_handler, bootstrap) =
                MultiHandler::new(protocol.clone(), worker_id, request_tx.clone(), response_rx);
            response_senders.push(response_tx);
            bootstraps.push(bootstrap);
        }

        match launch_with_bootstraps::<MultiHandler<P, Request, Response>, _>(
            addr,
            runtime_config,
            bootstraps,
        ) {
            Ok(runtime) => {
                for (worker_id, sender) in response_senders.iter().enumerate() {
                    let Some(wake) = runtime.worker_wake_handle(worker_id) else {
                        let error = io::Error::other(format!(
                            "Ringline worker {worker_id} wake handle unavailable during startup"
                        ));
                        let _ = runtime.join();
                        log_ringline_fallback(&error);
                        return Process::Mio(
                            MioProcessBuilder {
                                admin,
                                config,
                                log_drain,
                                protocol: fallback_protocol,
                                storage,
                                _request: PhantomData,
                                _response: PhantomData,
                            }
                            .spawn(),
                        );
                    };
                    if sender.attach(wake).is_err() {
                        let error = io::Error::other(format!(
                            "Ringline worker {worker_id} wake handle was attached more than once"
                        ));
                        let _ = runtime.join();
                        log_ringline_fallback(&error);
                        return Process::Mio(
                            MioProcessBuilder {
                                admin,
                                config,
                                log_drain,
                                protocol: fallback_protocol,
                                storage,
                                _request: PhantomData,
                                _response: PhantomData,
                            }
                            .spawn(),
                        );
                    }
                }
                if !all_response_wakes_attached(&response_senders) {
                    let error = io::Error::other(
                        "Ringline storage cannot start before every worker wake handle is attached",
                    );
                    let _ = runtime.join();
                    log_ringline_fallback(&error);
                    return Process::Mio(
                        MioProcessBuilder {
                            admin,
                            config,
                            log_drain,
                            protocol: fallback_protocol,
                            storage,
                            _request: PhantomData,
                            _response: PhantomData,
                        }
                        .spawn(),
                    );
                }
                let (storage_signal_tx, storage_signal_rx) = bounded(QUEUE_CAPACITY);
                let storage_worker = RinglineStorageWorker::new(
                    request_rx,
                    response_senders,
                    storage_signal_rx,
                    storage,
                    Duration::from_millis(config.worker.timeout() as u64),
                );
                let pending_storage = PendingRinglineStorage {
                    signal_tx: storage_signal_tx,
                    run: Box::new(move || storage_worker.run()),
                };
                let process = spawn_ringline(admin, log_drain, runtime, Some(pending_storage))
                    .unwrap_or_else(|error| {
                        error!("Ringline control-plane initialization failed: {error}");
                        panic!("Ringline control-plane initialization failed: {error}")
                    });
                log_ringline_active();
                Process::Ringline(process)
            }
            Err(ringline_error) => {
                log_ringline_runtime_fallback(&ringline_error);
                Process::Mio(
                    MioProcessBuilder {
                        admin,
                        config,
                        log_drain,
                        protocol: fallback_protocol,
                        storage,
                        _request: PhantomData,
                        _response: PhantomData,
                    }
                    .spawn(),
                )
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn spawn_ringline_single<P, Request, Response, Storage>(
    admin: AdminBuilder,
    config: CacheConfig,
    log_drain: LogDrain,
    protocol: P,
    storage: Storage,
    addr: std::net::SocketAddr,
    runtime_config: RinglineRuntimeConfig,
) -> Process
where
    P: 'static + Protocol<Request, Response> + Clone + Send,
    Request: 'static + Klog + Klog<Response = Response> + Send,
    Response: 'static + Compose + Send,
    Storage: 'static + Execute<Request, Response> + EntryStore + Send,
{
    let (_handler, bootstrap, recovery) =
        SingleHandler::<P, Request, Response, Storage>::new(protocol, storage);
    match launch_with_bootstraps::<SingleHandler<P, Request, Response, Storage>, _>(
        addr,
        runtime_config,
        vec![bootstrap],
    ) {
        Ok(runtime) => {
            recovery.commit();
            log_ringline_active();
            Process::Ringline(
                spawn_ringline(admin, log_drain, runtime, None).unwrap_or_else(|error| {
                    error!("Ringline control-plane initialization failed: {error}");
                    panic!("Ringline control-plane initialization failed: {error}")
                }),
            )
        }
        Err(ringline_error) => {
            log_ringline_runtime_fallback(&ringline_error);
            let (protocol, storage) = recovery.recover().unwrap_or_else(|recovery_error| {
                panic!("Ringline initialization failed ({ringline_error}); mio fallback state recovery failed: {recovery_error}")
            });
            Process::Mio(
                MioProcessBuilder {
                    admin,
                    config,
                    log_drain,
                    protocol,
                    storage,
                    _request: PhantomData,
                    _response: PhantomData,
                }
                .spawn(),
            )
        }
    }
}

pub enum Process {
    Mio(MioProcess),
    #[cfg(target_os = "linux")]
    Ringline(RinglineProcess),
}

pub struct MioProcess {
    admin: JoinHandle<()>,
    listener: JoinHandle<()>,
    signal_tx: Sender<Signal>,
    workers: Vec<JoinHandle<()>>,
}

#[cfg(target_os = "linux")]
pub struct RinglineProcess {
    admin: JoinHandle<()>,
    bridge: JoinHandle<()>,
    signal_tx: Sender<Signal>,
}

#[cfg(target_os = "linux")]
fn spawn_ringline(
    admin: AdminBuilder,
    log_drain: LogDrain,
    runtime: pelikan_net::ringline::RinglineRuntime,
    pending_storage: Option<PendingRinglineStorage>,
) -> io::Result<RinglineProcess> {
    let mut bridge_poll = match Poll::new() {
        Ok(poll) => poll,
        Err(error) => return rollback_live_runtime(runtime, error),
    };
    let bridge_waker = match pelikan_net::Waker::new(bridge_poll.registry(), WAKER_TOKEN) {
        Ok(waker) => Arc::new(Waker::from(waker)),
        Err(error) => return rollback_live_runtime(runtime, error),
    };

    let (signal_tx, signal_rx) = bounded(QUEUE_CAPACITY);
    let (mut admin_signal_queues, mut bridge_signal_queues) = match Queues::new(
        vec![admin.waker()],
        vec![Arc::clone(&bridge_waker)],
        QUEUE_CAPACITY,
    ) {
        Ok(queues) => queues,
        Err(error) => return rollback_live_runtime(runtime, io::Error::other(error)),
    };
    let mut admin = admin.build(log_drain, signal_rx, admin_signal_queues.remove(0));

    let admin = match std::thread::Builder::new()
        .name(format!("{THREAD_PREFIX}_admin"))
        .spawn(move || admin.run())
    {
        Ok(admin) => admin,
        Err(error) => return rollback_live_runtime(runtime, error),
    };

    let bridge_signals = bridge_signal_queues.remove(0);
    let (runtime_tx, runtime_rx) = std::sync::mpsc::sync_channel::<(
        pelikan_net::ringline::RinglineShutdown,
        JoinHandle<io::Result<()>>,
        Option<LiveRinglineStorage>,
    )>(1);
    let bridge_signal_tx = signal_tx.clone();
    let bridge = match std::thread::Builder::new()
        .name(format!("{THREAD_PREFIX}_ringline_control"))
        .spawn(move || {
            let Ok((control, worker_monitor, mut storage)) = runtime_rx.recv() else {
                return;
            };
            let worker_wake = control.worker_wake_handle(0);
            let mut worker_monitor = Some(worker_monitor);
            loop {
                if worker_monitor.as_ref().is_some_and(JoinHandle::is_finished) {
                    stop_ringline_storage(&mut storage);
                    if let Some(monitor) = worker_monitor.take() {
                        log_ringline_monitor(monitor);
                    } else {
                        error!("Ringline worker monitor missing during termination");
                    }
                    if let Err(error) = bridge_signal_tx.try_send(Signal::Shutdown) {
                        error!("failed to report Ringline termination to admin: {error}");
                    }
                    return;
                }
                if let Err(error) = bridge_poll.poll(
                    &mut Events::with_capacity(1),
                    Some(Duration::from_millis(100)),
                ) {
                    error!("Ringline admin bridge poll failed: {error}");
                    stop_ringline_storage(&mut storage);
                    control.shutdown();
                    if let Some(monitor) = worker_monitor.take() {
                        log_ringline_monitor(monitor);
                    } else {
                        error!("Ringline worker monitor missing during shutdown");
                    }
                    if let Err(report_error) = bridge_signal_tx.try_send(Signal::Shutdown) {
                        error!("failed to report Ringline bridge failure to admin: {report_error}");
                    }
                    return;
                }
                bridge_waker.reset();

                while let Some(signal) = bridge_signals.try_recv().map(|item| item.into_inner()) {
                    match signal {
                        Signal::FlushAll => {
                            if let Some(storage) = &storage {
                                if let Err(error) = storage.signal_tx.try_send(Signal::FlushAll) {
                                    error!(
                                        "failed to forward FlushAll to Ringline storage: {error}"
                                    );
                                }
                            } else {
                                request_flush();
                                if let Some(wake) = &worker_wake {
                                    wake.wake();
                                } else {
                                    error!("Ringline worker wake handle unavailable for FlushAll");
                                }
                            }
                        }
                        Signal::Shutdown => {
                            control.shutdown();
                            stop_ringline_storage(&mut storage);
                            if let Some(monitor) = worker_monitor.take() {
                                log_ringline_monitor(monitor);
                            } else {
                                error!("Ringline worker monitor missing during shutdown");
                            }
                            return;
                        }
                    }
                }
            }
        }) {
        Ok(bridge) => bridge,
        Err(error) => {
            shutdown_signal(&signal_tx);
            let _ = admin.join();
            return rollback_live_runtime(runtime, error);
        }
    };

    let monitored = match runtime.monitor() {
        Ok(monitored) => monitored,
        Err(error) => {
            drop(runtime_tx);
            shutdown_signal(&signal_tx);
            let _ = bridge.join();
            let _ = admin.join();
            return Err(error);
        }
    };
    let live_storage = match pending_storage {
        Some(storage) => match storage.start() {
            Ok(storage) => Some(storage),
            Err(error) => {
                let (control, monitor) = monitored;
                control.shutdown();
                log_ringline_monitor(monitor);
                drop(runtime_tx);
                shutdown_signal(&signal_tx);
                let _ = bridge.join();
                let _ = admin.join();
                return Err(error);
            }
        },
        None => None,
    };
    if let Err(error) = runtime_tx.send((monitored.0, monitored.1, live_storage)) {
        let (control, monitor, mut storage) = error.0;
        control.shutdown();
        stop_ringline_storage(&mut storage);
        log_ringline_monitor(monitor);
        shutdown_signal(&signal_tx);
        let bridge_error = match bridge.join() {
            Ok(()) => io::Error::other("Ringline control bridge exited during startup"),
            Err(payload) => io::Error::other(format!(
                "Ringline control bridge panicked during startup: {}",
                panic_payload(payload)
            )),
        };
        if let Err(payload) = admin.join() {
            error!(
                "Ringline admin panicked during rollback: {}",
                panic_payload(payload)
            );
        }
        return Err(bridge_error);
    }

    if let Err(error) = try_spawn_signal_handler(signal_tx.clone()) {
        shutdown_signal(&signal_tx);
        let _ = bridge.join();
        let _ = admin.join();
        return Err(error);
    }

    Ok(RinglineProcess {
        admin,
        bridge,
        signal_tx,
    })
}

#[cfg(target_os = "linux")]
struct PendingRinglineStorage {
    signal_tx: Sender<Signal>,
    run: Box<dyn FnOnce() + Send>,
}

#[cfg(target_os = "linux")]
impl PendingRinglineStorage {
    fn start(self) -> io::Result<LiveRinglineStorage> {
        let join = std::thread::Builder::new()
            .name(format!("{THREAD_PREFIX}_storage"))
            .spawn(self.run)?;
        Ok(LiveRinglineStorage {
            signal_tx: self.signal_tx,
            join: Some(join),
        })
    }
}

#[cfg(target_os = "linux")]
struct LiveRinglineStorage {
    signal_tx: Sender<Signal>,
    join: Option<JoinHandle<()>>,
}

#[cfg(target_os = "linux")]
fn stop_ringline_storage(storage: &mut Option<LiveRinglineStorage>) {
    let Some(mut storage) = storage.take() else {
        return;
    };
    let _ = storage.signal_tx.send(Signal::Shutdown);
    if let Some(join) = storage.join.take() {
        if let Err(payload) = join.join() {
            error!(
                "Ringline storage worker panicked: {}",
                panic_payload(payload)
            );
        }
    }
}

#[cfg(target_os = "linux")]
fn fail_after_live<T>(error: io::Error, rollback: impl FnOnce()) -> io::Result<T> {
    rollback();
    Err(error)
}

#[cfg(target_os = "linux")]
fn rollback_live_runtime(
    runtime: pelikan_net::ringline::RinglineRuntime,
    error: io::Error,
) -> io::Result<RinglineProcess> {
    fail_after_live(error, || {
        if let Err(cleanup_error) = runtime.join() {
            error!("Ringline rollback after control-plane failure also failed: {cleanup_error}");
        }
    })
}

#[cfg(target_os = "linux")]
fn log_ringline_monitor(worker_monitor: JoinHandle<io::Result<()>>) {
    match worker_monitor.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => error!("Ringline runtime terminated: {error}"),
        Err(payload) => error!("Ringline monitor panicked: {}", panic_payload(payload)),
    }
}

fn try_spawn_signal_handler(signal_tx: Sender<Signal>) -> io::Result<()> {
    std::thread::Builder::new()
        .name(format!("{THREAD_PREFIX}_signal"))
        .spawn(move || signal_handler(&signal_tx))
        .map(drop)
}

fn spawn_signal_handler(signal_tx: Sender<Signal>) {
    if let Err(error) = std::thread::Builder::new()
        .name(format!("{THREAD_PREFIX}_signal"))
        .spawn(move || signal_handler(&signal_tx))
    {
        error!("failed to spawn signal handler: {error}");
    }
}

fn shutdown_signal(signal_tx: &Sender<Signal>) {
    if let Err(error) = signal_tx.try_send(Signal::Shutdown) {
        fatal!("error sending shutdown signal to thread: {error}");
    }
}

fn signal_handler(signal_tx: &Sender<Signal>) {
    const SIGNALS: &[c_int] = &[SIGHUP, SIGINT, SIGTERM, SIGQUIT];
    let mut signals = Signals::new(SIGNALS).expect("Couldn't instantiate Signals");

    for signal in &mut signals {
        match signal {
            SIGTERM | SIGINT | SIGQUIT => {
                shutdown_signal(signal_tx);
                break;
            }
            _ => (),
        }
    }
}

fn panic_payload(payload: Box<dyn Any + Send + 'static>) -> String {
    let payload = match payload.downcast::<String>() {
        Ok(message) => return *message,
        Err(payload) => payload,
    };
    payload
        .downcast::<&'static str>()
        .map(|message| (*message).to_string())
        .unwrap_or_else(|_| "non-string panic payload".to_string())
}

impl Process {
    pub fn shutdown(self) {
        match &self {
            Self::Mio(process) => shutdown_signal(&process.signal_tx),
            #[cfg(target_os = "linux")]
            Self::Ringline(process) => shutdown_signal(&process.signal_tx),
        }
        self.wait()
    }

    pub fn wait(self) {
        match self {
            Self::Mio(process) => process.wait(),
            #[cfg(target_os = "linux")]
            Self::Ringline(process) => process.wait(),
        }
    }
}

impl MioProcess {
    fn wait(self) {
        for thread in self.workers {
            if let Err(payload) = thread.join() {
                error!("mio worker panicked: {}", panic_payload(payload));
            }
        }
        if let Err(payload) = self.listener.join() {
            error!("mio listener panicked: {}", panic_payload(payload));
        }
        if let Err(payload) = self.admin.join() {
            error!("mio admin panicked: {}", panic_payload(payload));
        }
    }
}

#[cfg(target_os = "linux")]
impl RinglineProcess {
    fn wait(self) {
        if let Err(payload) = self.bridge.join() {
            error!("Ringline admin bridge panicked: {}", panic_payload(payload));
        }
        if let Err(payload) = self.admin.join() {
            error!("Ringline admin panicked: {}", panic_payload(payload));
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use super::RINGLINE_MAX_CONNECTIONS;
    use super::{
        backend_resolution, process_kind, record_resolution, ringline_fallback_reason, ProcessKind,
    };
    use crate::{SERVER_IO_BACKEND_ACTIVE, SERVER_IO_BACKEND_FALLBACK};
    use pelikan_net::{resolve_backend, BackendResolution, FallbackReason, IoBackend};
    #[cfg(target_os = "linux")]
    use std::cell::Cell;
    #[cfg(target_os = "linux")]
    use std::io;

    #[test]
    fn resolved_mio_builds_existing_process() {
        assert_eq!(
            process_kind(resolve_backend(IoBackend::Mio, false)),
            ProcessKind::Mio
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resolved_ringline_builds_ringline_process() {
        assert_eq!(
            process_kind(resolve_backend(IoBackend::Ringline, true)),
            ProcessKind::Ringline
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn post_live_construction_failure_runs_rollback_and_preserves_cause() {
        let rolled_back = Cell::new(false);
        let error =
            super::fail_after_live::<()>(io::Error::other("injected bridge spawn failure"), || {
                rolled_back.set(true)
            })
            .unwrap_err();

        assert!(rolled_back.get());
        assert_eq!(error.to_string(), "injected bridge spawn failure");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn ringline_connection_limit_uses_ringline_default_not_mio_event_batch() {
        assert_eq!(RINGLINE_MAX_CONNECTIONS, 16_000);
    }

    #[test]
    fn ring_setup_is_classified_as_unsupported_with_exact_cause() {
        let reason = ringline_fallback_reason(&pelikan_net::ringline::Error::RingSetup(
            "Operation not permitted (os error 1)".to_string(),
        ));
        assert_eq!(
            reason,
            FallbackReason::UnsupportedCapability(
                "ring setup: Operation not permitted (os error 1)".to_string()
            )
        );
    }

    #[test]
    fn kernel_einval_is_classified_as_unsupported_with_exact_cause() {
        let reason = ringline_fallback_reason(&pelikan_net::ringline::Error::Io(
            io::Error::from_raw_os_error(libc::EINVAL),
        ));
        assert_eq!(
            reason,
            FallbackReason::UnsupportedCapability(
                "I/O error: Invalid argument (os error 22)".to_string()
            )
        );
    }

    #[test]
    fn unrelated_initialization_error_is_not_an_unsupported_capability() {
        let reason = ringline_fallback_reason(&pelikan_net::ringline::Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "broken bootstrap",
        )));
        assert_eq!(
            reason,
            FallbackReason::Initialization("I/O error: broken bootstrap".to_string())
        );
    }

    #[test]
    fn fallback_records_mio_active_and_increments_counter() {
        let before = SERVER_IO_BACKEND_FALLBACK.value();
        record_resolution(&BackendResolution {
            requested: IoBackend::Ringline,
            active: IoBackend::Mio,
            fallback: Some(FallbackReason::Unavailable),
        });
        assert_eq!(SERVER_IO_BACKEND_ACTIVE.value(), 0);
        assert_eq!(SERVER_IO_BACKEND_FALLBACK.value(), before + 1);
        assert_eq!(
            backend_resolution(),
            BackendResolution {
                requested: IoBackend::Ringline,
                active: IoBackend::Mio,
                fallback: Some(FallbackReason::Unavailable),
            }
        );
    }

    #[test]
    fn unsupported_ringline_resolution_builds_mio_process() {
        assert_eq!(
            process_kind(resolve_backend(IoBackend::Ringline, false)),
            ProcessKind::Mio
        );
    }
}
