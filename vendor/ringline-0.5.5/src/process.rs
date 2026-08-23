//! Async process spawning.
//!
//! Spawns child processes on a dedicated thread pool (via `posix_spawnp` +
//! `pidfd_open`) and uses io_uring `PollAdd` on the pidfd for async exit
//! notification.
//!
//! # Example
//!
//! ```rust,no_run
//! use ringline::process::Command;
//!
//! # async fn example() -> std::io::Result<()> {
//! let child = Command::new("echo")
//!     .arg("hello")
//!     .arg("world")
//!     .spawn()?
//!     .await?;
//! let status = child.wait()?.await?;
//! assert!(status.success());
//! # Ok(())
//! # }
//! ```

use std::ffi::CString;
use std::future::Future;
use std::io;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(has_io_uring)]
use crate::completion::{OpTag, UserData};
use crate::runtime::CURRENT_TASK_ID;
use crate::runtime::io::{try_with_state, with_state};

/// A spawned child process.
///
/// Obtained by awaiting the future returned by [`Command::spawn()`]. The child's
/// pidfd is held open until the `Child` is dropped.
pub struct Child {
    pid: u32,
    pidfd: RawFd,
}

impl Child {
    /// Get the process ID.
    pub fn id(&self) -> u32 {
        self.pid
    }

    /// Wait for the child to exit.
    ///
    /// Submits an io_uring `PollAdd` on the child's pidfd and returns a future
    /// that resolves when the child exits.
    #[cfg(has_io_uring)]
    pub fn wait(&self) -> io::Result<WaitFuture> {
        with_state(|driver, executor| {
            let seq = executor.next_pidfd_seq;
            executor.next_pidfd_seq += 1;
            let task_id = CURRENT_TASK_ID.with(|c| c.get());
            executor.pidfd_waiters.insert(seq, task_id);

            let ud = UserData::encode(OpTag::PidfdPoll, 0, seq);
            driver
                .ring
                .submit_poll_add(self.pidfd, libc::POLLIN as u32, ud.raw())?;
            Ok(WaitFuture { seq, pid: self.pid })
        })
    }

    /// Wait for the child to exit (not yet implemented on mio backend).
    #[cfg(not(has_io_uring))]
    pub fn wait(&self) -> io::Result<WaitFuture> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "process wait requires the io_uring backend",
        ))
    }

    /// Send `SIGKILL` to the child.
    pub fn kill(&self) -> io::Result<()> {
        let ret = unsafe { libc::kill(self.pid as i32, libc::SIGKILL) };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        // Close the pidfd.
        unsafe {
            libc::close(self.pidfd);
        }
    }
}

/// Future that resolves when a child process exits.
pub struct WaitFuture {
    seq: u32,
    pid: u32,
}

impl Drop for WaitFuture {
    fn drop(&mut self) {
        // Deregister so an abandoned wait (select/timeout loser) doesn't
        // leave a stale waiter/result entry behind forever.
        let _ = try_with_state(|_driver, executor| {
            executor.pidfd_waiters.remove(&self.seq);
            executor.pidfd_results.remove(&self.seq);
        });
    }
}

impl Future for WaitFuture {
    type Output = io::Result<ExitStatus>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_state(|_driver, executor| {
            if let Some(result) = executor.pidfd_results.remove(&self.seq) {
                // pidfd is readable -- child has exited.
                if result < 0 {
                    return Poll::Ready(Err(io::Error::from_raw_os_error(-result)));
                }
                // Call waitid to collect exit status. This returns immediately
                // because the pidfd poll already confirmed the child exited.
                let mut siginfo: libc::siginfo_t = unsafe { std::mem::zeroed() };
                let ret = unsafe {
                    libc::waitid(
                        libc::P_PID,
                        self.pid as libc::id_t,
                        &mut siginfo,
                        libc::WEXITED,
                    )
                };
                if ret < 0 {
                    return Poll::Ready(Err(io::Error::last_os_error()));
                }
                // Extract exit code from siginfo.
                let code = unsafe { siginfo.si_status() };
                Poll::Ready(Ok(ExitStatus { code }))
            } else {
                // Not yet ready -- re-register waiter.
                let task_id = CURRENT_TASK_ID.with(|c| c.get());
                executor.pidfd_waiters.insert(self.seq, task_id);
                Poll::Pending
            }
        })
    }
}

/// Exit status of a child process.
#[derive(Debug, Clone, Copy)]
pub struct ExitStatus {
    code: i32,
}

impl ExitStatus {
    /// Returns true if the process exited with code 0.
    pub fn success(&self) -> bool {
        self.code == 0
    }

    /// Returns the exit code.
    pub fn code(&self) -> i32 {
        self.code
    }
}

/// Builder for spawning a child process.
///
/// Mirrors [`std::process::Command`]. The program is searched on `PATH`
/// (via `posix_spawnp`). The child inherits the parent's environment.
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() -> std::io::Result<()> {
/// let child = ringline::process::Command::new("grep")
///     .arg("-r")
///     .arg("pattern")
///     .arg("src/")
///     .spawn()?
///     .await?;
/// let status = child.wait()?.await?;
/// # Ok(())
/// # }
/// ```
pub struct Command {
    program: String,
    args: Vec<String>,
}

impl Command {
    /// Create a new command for the given program.
    pub fn new(program: impl Into<String>) -> Self {
        Command {
            program: program.into(),
            args: Vec::new(),
        }
    }

    /// Add an argument.
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Add multiple arguments.
    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args.extend(args.into_iter().map(|a| a.into()));
        self
    }

    /// Spawn the command.
    ///
    /// Returns a future that resolves to a [`Child`] once the spawner pool
    /// has created the process and obtained its pidfd.
    pub fn spawn(self) -> io::Result<SpawnFuture> {
        let program =
            CString::new(self.program).map_err(|_| io::Error::other("invalid program name"))?;
        let args: Vec<CString> = self
            .args
            .iter()
            .map(|a| CString::new(a.as_str()).map_err(|_| io::Error::other("invalid argument")))
            .collect::<io::Result<_>>()?;

        try_with_state(|driver, executor| {
            let spawner = driver
                .spawner
                .as_ref()
                .ok_or_else(|| io::Error::other("spawner pool not configured"))?;
            let spawn_tx = driver
                .spawn_tx
                .as_ref()
                .ok_or_else(|| io::Error::other("spawner pool not configured"))?;

            let request_id = executor.next_spawn_id;
            executor.next_spawn_id += 1;

            let task_id = CURRENT_TASK_ID.with(|c| c.get());
            executor.pending_spawns.insert(request_id, (task_id, None));

            spawner
                .request_tx
                .send(crate::spawner::SpawnRequest {
                    program,
                    args,
                    request_id,
                    response_tx: spawn_tx.clone(),
                    wake_handle: driver.wake_handle,
                })
                .map_err(|_| io::Error::other("spawner pool shut down"))?;

            Ok(SpawnFuture { request_id })
        })
        .unwrap_or_else(|| Err(io::Error::other("called outside executor")))
    }
}

/// Future returned by [`Command::spawn()`]. Resolves to a [`Child`].
pub struct SpawnFuture {
    request_id: u64,
}

impl Drop for SpawnFuture {
    fn drop(&mut self) {
        // A SpawnFuture abandoned before completion (select/timeout loser)
        // must not leak its map entry — and, worse, the spawner's response
        // (containing an OPEN PIDFD) would be stored into the entry and
        // never taken. Mark the entry as abandoned by removing it; the
        // deliver path closes the pidfd when no entry is waiting.
        let _ = try_with_state(|_driver, executor| {
            // Response already arrived but was never consumed — close the
            // pidfd so it doesn't leak.
            if let Some((_, Some(Ok(r)))) = executor.pending_spawns.remove(&self.request_id) {
                unsafe {
                    libc::close(r.pidfd);
                }
            }
        });
    }
}

impl Future for SpawnFuture {
    type Output = io::Result<Child>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_state(|_driver, executor| {
            if let Some((_, slot)) = executor.pending_spawns.get_mut(&self.request_id)
                && let Some(result) = slot.take()
            {
                executor.pending_spawns.remove(&self.request_id);
                return Poll::Ready(result.map(|r| Child {
                    pid: r.pid,
                    pidfd: r.pidfd,
                }));
            }
            Poll::Pending
        })
    }
}
