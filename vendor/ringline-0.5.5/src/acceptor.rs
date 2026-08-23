use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crossbeam_channel::Sender;

/// Configuration for the centralized acceptor thread.
pub struct AcceptorConfig {
    /// The listening socket fd.
    pub listen_fd: RawFd,
    /// Per-worker channels to send accepted (fd, peer_addr) pairs.
    pub worker_channels: Vec<Sender<(RawFd, SocketAddr)>>,
    /// Per-worker wake handles to wake the event loop after sending a connection.
    pub worker_wake_handles: Vec<crate::wakeup::WakeFd>,
    /// Shared flag set by ShutdownHandle to signal the acceptor to stop.
    #[allow(dead_code)] // stored for future use; acceptor currently uses channel disconnect
    pub shutdown_flag: Arc<AtomicBool>,
    /// Whether to set TCP_NODELAY on accepted connections.
    pub tcp_nodelay: bool,
    /// Connections to assign to each worker before moving to the next.
    /// 1 = round-robin. See [`ConfigBuilder::conn_chunk_size`](crate::ConfigBuilder::conn_chunk_size).
    pub conn_chunk_size: usize,
    /// Whether to set SO_TIMESTAMPING on accepted connections.
    #[cfg(feature = "timestamps")]
    pub timestamps: bool,
}

/// Run the acceptor loop. Terminates when all channels disconnect.
///
/// Accepts connections via blocking `accept4` and distributes raw fds
/// to workers round-robin, waking each worker via eventfd.
pub fn run_acceptor(config: AcceptorConfig) {
    let num_workers = config.worker_channels.len();
    if num_workers == 0 {
        return;
    }

    let chunk_size = config.conn_chunk_size.max(1);
    let mut conn_count = 0usize; // successfully dispatched connections
    let mut addr_storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut alive = vec![true; num_workers];
    let mut alive_count = num_workers;

    loop {
        let mut addr_len: libc::socklen_t =
            std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

        let fd = accept_nonblock(config.listen_fd, &mut addr_storage, &mut addr_len);

        if fd < 0 {
            let err = std::io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EMFILE) | Some(libc::ENFILE) => {
                    // Too many open files — back off briefly.
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Some(libc::ECONNABORTED) | Some(libc::ECONNRESET) | Some(libc::EPERM) => {
                    // Connection reset before accept completed, or blocked by
                    // firewall — retry immediately.
                    continue;
                }
                _ => {
                    // Fatal accept error or listen fd closed.
                    return;
                }
            }
        }

        // Set TCP_NODELAY if configured (skip for Unix domain sockets).
        if config.tcp_nodelay && addr_storage.ss_family != libc::AF_UNIX as libc::sa_family_t {
            let optval: libc::c_int = 1;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Set SO_TIMESTAMPING for kernel-level RX timestamps (Linux only).
        #[cfg(all(target_os = "linux", feature = "timestamps"))]
        if config.timestamps {
            let flags: libc::c_int = (libc::SOF_TIMESTAMPING_SOFTWARE
                | libc::SOF_TIMESTAMPING_RX_SOFTWARE)
                as libc::c_int;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_TIMESTAMPING,
                    &flags as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Parse peer address from the sockaddr_storage filled by accept4.
        let peer_addr = sockaddr_to_socket_addr(&addr_storage)
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));

        // Pick a target worker based on chunk assignment, then fall back to
        // adjacent workers if that worker's channel is full or it has exited.
        // `try_send` lets us distinguish a full queue (skip) from a
        // disconnected channel (mark dead). Closing the fd when all workers
        // are full or dead lets the kernel deliver a clean connection-refused
        // to the peer instead of growing an unbounded backlog in the channel.
        let primary = (conn_count / chunk_size) % num_workers;
        let mut sent = false;
        for i in 0..num_workers {
            let worker_idx = (primary + i) % num_workers;

            if !alive[worker_idx] {
                continue;
            }

            match config.worker_channels[worker_idx].try_send((fd, peer_addr)) {
                Ok(()) => {
                    config.worker_wake_handles[worker_idx].wake();
                    conn_count = conn_count.wrapping_add(1);
                    sent = true;
                    break;
                }
                Err(crossbeam_channel::TrySendError::Full(_)) => {
                    // Worker is backlogged — try the next one.
                    continue;
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    // Worker has exited — mark dead.
                    alive[worker_idx] = false;
                    alive_count -= 1;
                    if alive_count == 0 {
                        unsafe {
                            libc::close(fd);
                        }
                        return;
                    }
                    continue;
                }
            }
        }

        if !sent {
            // Every live worker is backlogged. Drop the connection rather
            // than block the acceptor, and keep accepting — the backlog is
            // transient. (The all-workers-dead case returns above.)
            unsafe {
                libc::close(fd);
            }
            continue;
        }
    }
}

/// Accept a connection and set the **returned** fd to non-blocking +
/// close-on-exec. The call itself blocks on the listen socket until a
/// connection arrives — only the resulting accepted fd is non-blocking.
///
/// On Linux, uses `accept4(SOCK_NONBLOCK | SOCK_CLOEXEC)` for a single
/// syscall. On other platforms, falls back to `accept()` + `fcntl()`.
///
/// Because this blocks, the acceptor thread is unblocked at shutdown by
/// `shutdown(fd, SHUT_RD)` on the listen socket, which makes a blocked
/// (or subsequent) `accept4` fail with `EINVAL` on Linux. (Closing the fd
/// alone does NOT wake a blocked accept — the in-progress syscall holds a
/// file reference.) On non-Linux platforms the wake is best-effort; a
/// quiet listener's acceptor thread may persist until a peer connects.
fn accept_nonblock(
    listen_fd: libc::c_int,
    addr: &mut libc::sockaddr_storage,
    addr_len: &mut libc::socklen_t,
) -> libc::c_int {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            libc::accept4(
                listen_fd,
                addr as *mut _ as *mut libc::sockaddr,
                addr_len,
                libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            )
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let fd =
            unsafe { libc::accept(listen_fd, addr as *mut _ as *mut libc::sockaddr, addr_len) };
        if fd >= 0 {
            unsafe {
                let flags = libc::fcntl(fd, libc::F_GETFL);
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
                let fd_flags = libc::fcntl(fd, libc::F_GETFD);
                libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC);
            }
        }
        fd
    }
}

/// Convert a `sockaddr_storage` (from accept) to a Rust `SocketAddr`.
fn sockaddr_to_socket_addr(storage: &libc::sockaddr_storage) -> Option<SocketAddr> {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            let sa = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            let ip = std::net::Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr));
            let port = u16::from_be(sa.sin_port);
            Some(SocketAddr::from((ip, port)))
        }
        libc::AF_INET6 => {
            let sa = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(sa.sin6_addr.s6_addr);
            let port = u16::from_be(sa.sin6_port);
            Some(SocketAddr::from((ip, port)))
        }
        _ => None,
    }
}
