use std::any::Any;
use std::io;
use std::sync::{Arc, Mutex};

type WorkerBootstrapSlots = Vec<Option<Box<dyn Any + Send>>>;

static HANDLER_SLOTS: Mutex<Option<WorkerBootstrapSlots>> = Mutex::new(None);

/// Startup-only guard for the process-global per-worker bootstrap slots.
///
/// Pelikan launches one cache-server runtime per process. These slots exist
/// only while Ringline synchronously starts its workers and invokes
/// `AsyncEventHandler::create_for_worker`; they are never accessed on a
/// request path.
struct HandlerSlots;

impl HandlerSlots {
    fn install<A: Send + 'static>(handlers: Vec<A>) -> Result<Self, ::ringline::Error> {
        let mut slots = HANDLER_SLOTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slots.is_some() {
            return Err(::ringline::Error::Io(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Ringline worker bootstrap slots are already installed",
            )));
        }

        *slots = Some(
            handlers
                .into_iter()
                .map(|handler| Some(Box::new(handler) as Box<dyn Any + Send>))
                .collect(),
        );
        Ok(Self)
    }
}

impl Drop for HandlerSlots {
    fn drop(&mut self) {
        *HANDLER_SLOTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

/// Takes the bootstrap value installed for one Ringline worker.
pub fn take_worker_bootstrap<A: Send + 'static>(worker_id: usize) -> A {
    let bootstrap = HANDLER_SLOTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_mut()
        .and_then(|slots| slots.get_mut(worker_id))
        .and_then(Option::take)
        .unwrap_or_else(|| panic!("missing Ringline worker bootstrap for worker {worker_id}"));

    *bootstrap.downcast::<A>().unwrap_or_else(|_| {
        panic!("Ringline worker bootstrap type mismatch for worker {worker_id}")
    })
}

use std::net::SocketAddr;
use std::thread::JoinHandle;

use super::RinglineRuntimeConfig;

type WorkerJoin = JoinHandle<Result<(), ::ringline::Error>>;

/// Handle to a running Ringline runtime and its worker threads.
#[derive(Clone)]
pub struct RinglineShutdown {
    shutdown: Arc<::ringline::ShutdownHandle>,
}

pub struct RinglineRuntime {
    shutdown: Arc<::ringline::ShutdownHandle>,
    workers: Vec<WorkerJoin>,
}

impl RinglineShutdown {
    pub fn shutdown(&self) {
        self.shutdown.shutdown();
    }

    pub fn worker_wake_handle(&self, worker_id: usize) -> Option<::ringline::WakeHandle> {
        self.shutdown.worker_wake_handle(worker_id)
    }
}

impl RinglineRuntime {
    /// Signals every worker to shut down. Calling this more than once is safe.
    pub fn shutdown(&self) {
        self.shutdown.shutdown();
    }

    /// Returns the actual TCP listener address, including an assigned port.
    pub fn bound_addr(&self) -> Option<SocketAddr> {
        self.shutdown.bound_addr()
    }

    /// Returns a cloneable handle that wakes one Ringline worker.
    pub fn worker_wake_handle(&self, worker_id: usize) -> Option<::ringline::WakeHandle> {
        self.shutdown.worker_wake_handle(worker_id)
    }

    /// Starts a monitor that joins workers and shuts the listener on exit.
    pub fn monitor(self) -> (RinglineShutdown, JoinHandle<io::Result<()>>) {
        let Self { shutdown, workers } = self;
        let control = RinglineShutdown {
            shutdown: Arc::clone(&shutdown),
        };
        let monitor = std::thread::Builder::new()
            .name("pelikan_ringline_monitor".to_string())
            .spawn(move || {
                let result = join_workers(workers);
                shutdown.shutdown();
                result
            })
            .expect("failed to spawn Ringline monitor");
        (control, monitor)
    }

    /// Shuts down and joins every Ringline worker.
    pub fn join(self) -> io::Result<()> {
        let Self { shutdown, workers } = self;
        shutdown.shutdown();
        join_workers(workers)
    }
}

fn join_workers(workers: Vec<WorkerJoin>) -> io::Result<()> {
    let mut first_error = None;
    for worker in workers {
        let error = match worker.join() {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(io::Error::other(error)),
            Err(payload) => Some(worker_panic_error(payload)),
        };
        if first_error.is_none() {
            first_error = error;
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn worker_panic_error(payload: Box<dyn Any + Send + 'static>) -> io::Error {
    let payload = match payload.downcast::<String>() {
        Ok(message) => {
            return io::Error::other(format!("Ringline worker panicked: {message}"));
        }
        Err(payload) => payload,
    };

    match payload.downcast::<&'static str>() {
        Ok(message) => io::Error::other(format!("Ringline worker panicked: {message}")),
        Err(_) => io::Error::other("Ringline worker panicked with a non-string payload"),
    }
}

/// Starts one Ringline worker for each provided bootstrap value.
pub fn launch<A: ::ringline::AsyncEventHandler>(
    addr: SocketAddr,
    config: RinglineRuntimeConfig,
    handlers: Vec<A>,
) -> Result<RinglineRuntime, ::ringline::Error> {
    launch_with_bootstraps::<A, A>(addr, config, handlers)
}

/// Starts Ringline workers with distinct per-worker bootstrap values.
pub fn launch_with_bootstraps<A, B>(
    addr: SocketAddr,
    config: RinglineRuntimeConfig,
    bootstraps: Vec<B>,
) -> Result<RinglineRuntime, ::ringline::Error>
where
    A: ::ringline::AsyncEventHandler,
    B: Send + 'static,
{
    if bootstraps.len() != config.workers {
        return Err(::ringline::Error::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Ringline bootstrap count {} does not match worker count {}",
                bootstraps.len(),
                config.workers
            ),
        )));
    }

    let slots = HandlerSlots::install(bootstraps)?;
    let config = config.build()?;
    let (shutdown, workers) = ::ringline::RinglineBuilder::new(config)
        .bind(addr)
        .launch::<A>()?;
    drop(slots);

    Ok(RinglineRuntime {
        shutdown: Arc::new(shutdown),
        workers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn panic_message(payload: Box<dyn Any + Send>) -> String {
        match payload.downcast::<String>() {
            Ok(message) => *message,
            Err(payload) => payload
                .downcast::<&'static str>()
                .map(|message| (*message).to_string())
                .unwrap_or_else(|_| "non-string panic".to_string()),
        }
    }

    #[test]
    fn missing_worker_id_has_explicit_panic() {
        let _test_guard = TEST_LOCK.lock().unwrap();
        let guard = HandlerSlots::install(vec![1_u8]).unwrap();

        let panic = std::panic::catch_unwind(|| take_worker_bootstrap::<u8>(2)).unwrap_err();

        assert_eq!(
            panic_message(panic),
            "missing Ringline worker bootstrap for worker 2"
        );
        drop(guard);
    }

    #[test]
    fn bootstrap_type_mismatch_has_explicit_panic() {
        let _test_guard = TEST_LOCK.lock().unwrap();
        let guard = HandlerSlots::install(vec![1_u8]).unwrap();

        let panic = std::panic::catch_unwind(|| take_worker_bootstrap::<String>(0)).unwrap_err();

        assert_eq!(
            panic_message(panic),
            "Ringline worker bootstrap type mismatch for worker 0"
        );
        drop(guard);
    }
    #[test]
    fn bootstraps_are_taken_by_worker_id_once() {
        let _test_guard = TEST_LOCK.lock().unwrap();
        let guard = HandlerSlots::install(vec!["zero", "one"]).unwrap();
        assert_eq!(take_worker_bootstrap::<&'static str>(1), "one");
        assert_eq!(take_worker_bootstrap::<&'static str>(0), "zero");
        drop(guard);
    }

    #[test]
    fn only_one_slot_set_can_be_installed() {
        let _test_guard = TEST_LOCK.lock().unwrap();
        let _guard = HandlerSlots::install(vec![1_u8]).unwrap();
        assert!(HandlerSlots::install(vec![2_u8]).is_err());
    }
    struct TestHandler;

    impl ::ringline::AsyncEventHandler for TestHandler {
        #[allow(clippy::manual_async_fn)]
        fn on_accept(
            &self,
            _conn: ::ringline::ConnCtx,
        ) -> impl std::future::Future<Output = ()> + 'static {
            async {}
        }

        fn create_for_worker(worker_id: usize) -> Self {
            take_worker_bootstrap(worker_id)
        }
    }

    fn runtime_config(workers: usize, max_connections: u32) -> super::super::RinglineRuntimeConfig {
        super::super::RinglineRuntimeConfig {
            workers,
            max_connections,
            recv_buffers: 64,
            recv_buffer_size: 4096,
            pin_to_core: false,
        }
    }

    #[test]
    fn launch_rejects_handler_count_mismatch() {
        let _test_guard = TEST_LOCK.lock().unwrap();
        let result = launch::<TestHandler>(
            "127.0.0.1:0".parse().unwrap(),
            runtime_config(1, 128),
            vec![],
        );

        match result {
            Err(::ringline::Error::Io(error)) => {
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            }
            _ => panic!("handler count mismatch must be an invalid-input error"),
        }
    }

    #[test]
    fn launch_failure_clears_bootstrap_slots() {
        let _test_guard = TEST_LOCK.lock().unwrap();
        let result = launch(
            "127.0.0.1:0".parse().unwrap(),
            runtime_config(1, 0),
            vec![TestHandler],
        );
        assert!(result.is_err());

        let guard = HandlerSlots::install(vec![7_u8]).unwrap();
        drop(guard);
    }

    struct BootstrapHandler;

    impl ::ringline::AsyncEventHandler for BootstrapHandler {
        #[allow(clippy::manual_async_fn)]
        fn on_accept(
            &self,
            _conn: ::ringline::ConnCtx,
        ) -> impl std::future::Future<Output = ()> + 'static {
            async {}
        }

        fn create_for_worker(worker_id: usize) -> Self {
            assert_eq!(
                take_worker_bootstrap::<String>(worker_id),
                "worker bootstrap"
            );
            Self
        }
    }

    #[test]
    fn launch_with_bootstraps_separates_handler_from_worker_state() {
        let _test_guard = TEST_LOCK.lock().unwrap();
        let guard = HandlerSlots::install(vec!["worker bootstrap".to_string()]).unwrap();

        let _handler = <BootstrapHandler as ::ringline::AsyncEventHandler>::create_for_worker(0);

        drop(guard);
    }

    #[test]
    fn launch_with_bootstraps_rejects_bootstrap_count_mismatch() {
        let _test_guard = TEST_LOCK.lock().unwrap();
        let result = launch_with_bootstraps::<BootstrapHandler, String>(
            "127.0.0.1:0".parse().unwrap(),
            runtime_config(1, 128),
            vec![],
        );

        assert!(
            matches!(result, Err(::ringline::Error::Io(error)) if error.kind() == io::ErrorKind::InvalidInput)
        );
    }

    #[test]
    fn join_converts_worker_panic_to_io_error() {
        let worker = std::thread::spawn(|| -> Result<(), ::ringline::Error> {
            panic!("worker panic");
        });

        let error = join_workers(vec![worker]).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "Ringline worker panicked: worker panic");
    }
}
