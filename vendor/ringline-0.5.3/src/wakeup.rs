//! Cross-platform worker wakeup mechanism.
//!
//! With io_uring: uses `eventfd(2)` — a single fd that the event loop polls via
//! `IORING_OP_READ`. Writing 8 bytes wakes the worker.
//!
//! Without io_uring (mio backend): uses a `pipe(2)` pair. The read end is
//! registered with `mio::Poll`; writing 1 byte wakes the worker.

use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;

/// Internal fd carrier used on hot paths inside the runtime.
///
/// Cheap to copy; does not own the fd. The fd is owned by the
/// [`Arc<WakeFdInner>`] held inside [`WakeHandle`]. The runtime preserves the
/// invariant that every `WakeFd` is dropped before the last `WakeHandle`
/// clone — workers join before [`crate::ShutdownHandle`] drops.
#[derive(Clone, Copy)]
pub(crate) struct WakeFd {
    fd: RawFd,
}

impl WakeFd {
    /// Wrap a raw file descriptor as a non-owning fd carrier. Only valid
    /// for fds that accept writes (io_uring's eventfd is bidirectional; the
    /// mio backend's pipe read end is NOT — its driver receives the write
    /// end explicitly).
    #[cfg(has_io_uring)]
    pub(crate) fn from_raw_fd(fd: RawFd) -> Self {
        WakeFd { fd }
    }

    /// Return the underlying file descriptor.
    #[allow(dead_code)]
    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.fd
    }

    /// Wake the worker by writing to the underlying fd.
    pub(crate) fn wake(&self) {
        wake_fd(self.fd);
    }
}

/// Owns the wake fd; closes it on drop.
struct WakeFdInner {
    fd: RawFd,
}

impl Drop for WakeFdInner {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

/// Refcounted handle for waking a worker thread from any thread.
///
/// Returned by [`crate::ShutdownHandle::worker_wake_handle`]. Cloning is cheap
/// (an atomic refcount bump) and the underlying fd stays open until the last
/// clone is dropped, so it is safe to keep clones around past
/// [`crate::ShutdownHandle`] drop — the writes simply land in an fd nobody is
/// reading anymore.
///
/// Typical use is to deliver a response on a crossbeam channel and then call
/// [`wake`](Self::wake) so the target worker observes the channel without
/// waiting on its idle timeout.
#[derive(Clone)]
pub struct WakeHandle {
    inner: Arc<WakeFdInner>,
}

impl WakeHandle {
    /// Wake the associated worker.
    ///
    /// Non-blocking, never errors — a failed write means the worker is
    /// already gone, which is fine.
    pub fn wake(&self) {
        wake_fd(self.inner.fd);
    }

    /// Extract a non-owning [`WakeFd`] carrier for use on hot paths.
    pub(crate) fn as_wake_fd(&self) -> WakeFd {
        WakeFd { fd: self.inner.fd }
    }
}

fn wake_fd(fd: RawFd) {
    #[cfg(has_io_uring)]
    {
        // eventfd expects exactly 8 bytes (a u64).
        let val: u64 = 1;
        unsafe {
            libc::write(fd, &val as *const u64 as *const libc::c_void, 8);
        }
    }
    #[cfg(not(has_io_uring))]
    {
        // pipe expects any non-zero write; 1 byte suffices.
        let val: u8 = 1;
        unsafe {
            libc::write(fd, &val as *const u8 as *const libc::c_void, 1);
        }
    }
}

/// Create a per-worker wake fd.
///
/// With io_uring: creates an `eventfd(2)`.
/// Without io_uring: creates a `pipe(2)` and returns `(read_fd, WakeHandle)`
/// where `WakeHandle` wraps the write end.
#[cfg(has_io_uring)]
pub(crate) fn create_wake_fd() -> io::Result<(RawFd, WakeHandle)> {
    let efd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if efd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((
        efd,
        WakeHandle {
            inner: Arc::new(WakeFdInner { fd: efd }),
        },
    ))
}

/// Create a per-worker wake fd pair (pipe).
///
/// Returns `(read_fd, WakeHandle)` where `read_fd` is registered with the
/// poller and `WakeHandle` wraps the write end for cross-thread waking.
#[cfg(not(has_io_uring))]
pub(crate) fn create_wake_fd() -> io::Result<(RawFd, WakeHandle)> {
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let read_fd = fds[0];
    let write_fd = fds[1];

    // Set both ends non-blocking and close-on-exec.
    for fd in &fds {
        unsafe {
            let flags = libc::fcntl(*fd, libc::F_GETFL);
            libc::fcntl(*fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
            let fd_flags = libc::fcntl(*fd, libc::F_GETFD);
            libc::fcntl(*fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC);
        }
    }

    Ok((
        read_fd,
        WakeHandle {
            inner: Arc::new(WakeFdInner { fd: write_fd }),
        },
    ))
}
