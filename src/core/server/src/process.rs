// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use libc::c_int;
use signal_hook::consts::signal::*;
use signal_hook::iterator::Signals;
use std::thread::JoinHandle;

pub struct ProcessBuilder<Parser, Request, Response, Storage> {
    admin: AdminBuilder,
    listener: ListenerBuilder,
    log_drain: Box<dyn Drain>,
    workers: WorkersBuilder<Parser, Request, Response, Storage>,
}

impl<Parser, Request, Response, Storage> ProcessBuilder<Parser, Request, Response, Storage>
where
    Parser: 'static + Parse<Request> + Clone + Send,
    Request: 'static + Klog + Klog<Response = Response> + Send,
    Response: 'static + Compose + Send,
    Storage: 'static + Execute<Request, Response> + EntryStore + Send,
{
    pub fn new<T: AdminConfig + ServerConfig + TlsConfig + WorkerConfig>(
        config: &T,
        log_drain: Box<dyn Drain>,
        parser: Parser,
        storage: Storage,
    ) -> Result<Self> {
        let admin = AdminBuilder::new(config)?;
        let listener = ListenerBuilder::new(config)?;
        let workers = WorkersBuilder::new(config, parser, storage)?;

        Ok(Self {
            admin,
            listener,
            log_drain,
            workers,
        })
    }

    pub fn version(mut self, version: &str) -> Self {
        self.admin.version(version);
        self
    }

    pub fn spawn(self) -> Process {
        let mut thread_wakers = vec![self.listener.waker()];
        thread_wakers.extend_from_slice(&self.workers.wakers());

        // channel for the parent `Process` to send `Signal`s to the admin thread
        let (signal_tx, signal_rx) = bounded(QUEUE_CAPACITY);

        // queues for the `Admin` to send `Signal`s to all sibling threads
        let (mut signal_queue_tx, mut signal_queue_rx) =
            Queues::new(vec![self.admin.waker()], thread_wakers, QUEUE_CAPACITY);

        // queues for the `Listener` to send `Session`s to the worker threads
        let (mut listener_session_queues, worker_session_queues) = Queues::new(
            vec![self.listener.waker()],
            self.workers.worker_wakers(),
            QUEUE_CAPACITY,
        );

        let mut admin = self
            .admin
            .build(self.log_drain, signal_rx, signal_queue_tx.remove(0));

        let mut listener = self
            .listener
            .build(signal_queue_rx.remove(0), listener_session_queues.remove(0));

        let workers = self.workers.build(worker_session_queues, signal_queue_rx);

        let admin = std::thread::Builder::new()
            .name(format!("{THREAD_PREFIX}_admin"))
            .spawn(move || admin.run())
            .unwrap();

        let listener = std::thread::Builder::new()
            .name(format!("{THREAD_PREFIX}_listener"))
            .spawn(move || listener.run())
            .unwrap();

        let workers = workers.spawn();
        let cloned_signal_tx = signal_tx.clone();

        // NOTE: Signal handler join handle is not taken ownership of by [Process] as it's
        // considered something that has the same lifetime as the actual OS process as a whole
        // and there aren't any current use cases for blocking on join()'ing the thread
        // if we want to dynamically rebind signal handlers in the future we should reconsider this
        let _signal_handler = std::thread::Builder::new()
            .name(format!("{THREAD_PREFIX}_signal"))
            .spawn(move || Process::signal_handler(&cloned_signal_tx));

        Process {
            admin,
            listener,
            signal_tx,
            workers,
        }
    }
}

pub struct Process {
    admin: JoinHandle<()>,
    listener: JoinHandle<()>,
    signal_tx: Sender<Signal>,
    workers: Vec<JoinHandle<()>>,
}

impl Process {
    /// Attempts to gracefully shutdown the `Process` by sending a shutdown to
    /// each thread and then waiting to join those threads.
    ///
    /// Will terminate ungracefully if it encounters an error in sending a
    /// shutdown to any of the threads.
    ///
    /// This function will block until all threads have terminated.
    pub fn shutdown(self) {
        // this sends a shutdown to the admin thread, which will broadcast the
        // signal to all sibling threads in the process
        Process::shutdown_signal(&self.signal_tx);

        // wait and join all threads
        self.wait()
    }

    /// Communicates to the admin thread that shutdown should occur
    fn shutdown_signal(signal_tx: &Sender<Signal>) {
        if signal_tx.try_send(Signal::Shutdown).is_err() {
            fatal!("error sending shutdown signal to thread");
        }
    }

    /// Registers Process to listen to relevant signals
    /// and depending on the signal, may relay Pelikan [Signal] messages to admin channel
    fn signal_handler(signal_tx: &Sender<Signal>) {
        const SIGNALS: &[c_int] = &[SIGHUP, SIGINT, SIGTERM, SIGQUIT];
        let mut signals = Signals::new(SIGNALS).expect("Couldn't instantiate Signals");

        //Infinite iterator of signals
        for signal in &mut signals {
            match signal {
                SIGTERM | SIGINT | SIGQUIT => {
                    Process::shutdown_signal(signal_tx);
                    break;
                }
                _ => (),
            }
        }
    }

    /// Will block until all threads terminate. This should be used to keep the
    /// process alive while the child threads run.
    pub fn wait(self) {
        for thread in self.workers {
            let _ = thread.join();
        }
        let _ = self.listener.join();
        let _ = self.admin.join();
    }
}
