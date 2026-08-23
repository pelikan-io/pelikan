//! Dedicated process spawner pool.
//!
//! Runs `posix_spawnp` + `pidfd_open` on a small pool of background threads,
//! keeping blocking process creation isolated from the io_uring event loop.
//!
//! Workers submit requests via [`spawn()`](crate::process::spawn) and receive
//! results through a per-worker crossbeam channel + eventfd wakeup.

use std::ffi::CString;
use std::io;
use std::os::unix::io::RawFd;
use std::thread;

use crossbeam_channel::{Receiver, Sender};

unsafe extern "C" {
    static environ: *const *const libc::c_char;
}

/// A request from a worker to the spawner pool.
pub(crate) struct SpawnRequest {
    pub(crate) program: CString,
    pub(crate) args: Vec<CString>,
    pub(crate) request_id: u64,
    /// Per-worker response channel.
    pub(crate) response_tx: Sender<SpawnResponse>,
    /// Wake handle — used to wake the worker after sending the response.
    pub(crate) wake_handle: crate::wakeup::WakeFd,
}

/// A response from the spawner pool to a worker.
pub(crate) struct SpawnResponse {
    pub(crate) request_id: u64,
    pub(crate) result: io::Result<SpawnResult>,
}

/// Successful spawn result containing the child pid and pidfd.
pub(crate) struct SpawnResult {
    pub(crate) pid: u32,
    pub(crate) pidfd: RawFd,
}

/// A pool of threads that perform blocking process spawning.
///
/// Created once in [`launch_inner`](crate::worker) and shared (via `Arc`)
/// across all workers. Shutdown is driven by dropping the request sender,
/// which causes all pool threads to exit.
pub(crate) struct SpawnerPool {
    pub(crate) request_tx: Sender<SpawnRequest>,
    _threads: Vec<thread::JoinHandle<()>>,
}

impl SpawnerPool {
    /// Create the channel pair and spawn spawner threads.
    pub(crate) fn start(num_threads: usize) -> Self {
        let (request_tx, request_rx) = crossbeam_channel::unbounded::<SpawnRequest>();
        let mut threads = Vec::with_capacity(num_threads);

        for i in 0..num_threads {
            let rx = request_rx.clone();
            let handle = thread::Builder::new()
                .name(format!("ringline-spawner-{i}"))
                .spawn(move || spawner_thread(rx))
                .expect("failed to spawn spawner thread");
            threads.push(handle);
        }

        SpawnerPool {
            request_tx,
            _threads: threads,
        }
    }
}

/// Main loop for a spawner thread.
fn spawner_thread(rx: Receiver<SpawnRequest>) {
    while let Ok(req) = rx.recv() {
        let result = do_spawn(&req.program, &req.args);
        let _ = req.response_tx.send(SpawnResponse {
            request_id: req.request_id,
            result,
        });
        // Wake the requesting worker so it drains the response channel.
        req.wake_handle.wake();
    }
    // Channel closed -- pool is shutting down.
}

/// Perform blocking process spawn via `posix_spawnp` + `pidfd_open`.
fn do_spawn(program: &CString, args: &[CString]) -> io::Result<SpawnResult> {
    // Build argv array: [program, args..., null]
    let mut argv: Vec<*const libc::c_char> = Vec::with_capacity(args.len() + 2);
    argv.push(program.as_ptr());
    for arg in args {
        argv.push(arg.as_ptr());
    }
    argv.push(std::ptr::null());

    let mut pid: libc::pid_t = 0;

    // posix_spawnp searches PATH; environ inherits the parent's environment.
    let ret = unsafe {
        libc::posix_spawnp(
            &mut pid,
            program.as_ptr(),
            std::ptr::null(), // file_actions (none)
            std::ptr::null(), // attrp (default)
            argv.as_ptr() as *const *mut libc::c_char,
            environ as *const *mut libc::c_char, // inherit environment
        )
    };

    // posix_spawnp returns 0 on success, error code (not -1) on failure.
    if ret != 0 {
        return Err(io::Error::from_raw_os_error(ret));
    }

    // Get pidfd for the child (Linux-only via pidfd_open syscall).
    #[cfg(target_os = "linux")]
    let pidfd = {
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0 as libc::c_int) } as RawFd;
        if fd < 0 {
            // pidfd_open failed -- kill the child and return error.
            let err = io::Error::last_os_error();
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
            unsafe {
                libc::waitpid(pid, std::ptr::null_mut(), 0);
            }
            return Err(err);
        }
        fd
    };

    // Non-Linux: pidfd not available, use -1 as sentinel.
    #[cfg(not(target_os = "linux"))]
    let pidfd: RawFd = -1;

    Ok(SpawnResult {
        pid: pid as u32,
        pidfd,
    })
}
