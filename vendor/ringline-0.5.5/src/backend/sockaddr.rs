//! Portable sockaddr conversion helpers shared across backends.

use std::net::SocketAddr;

/// Convert a libc sockaddr_storage to a std SocketAddr.
pub(crate) fn sockaddr_to_socket_addr(
    addr: &libc::sockaddr_storage,
    len: u32,
) -> Option<SocketAddr> {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
    match addr.ss_family as libc::c_int {
        libc::AF_INET if len >= std::mem::size_of::<libc::sockaddr_in>() as u32 => {
            let sa = unsafe { &*(addr as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr));
            let port = u16::from_be(sa.sin_port);
            Some(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 if len >= std::mem::size_of::<libc::sockaddr_in6>() as u32 => {
            let sa = unsafe { &*(addr as *const _ as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(sa.sin6_addr.s6_addr);
            let port = u16::from_be(sa.sin6_port);
            Some(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sa.sin6_flowinfo,
                sa.sin6_scope_id,
            )))
        }
        _ => None,
    }
}

/// Convert a libc sockaddr_storage to a `PeerAddr` (TCP or Unix).
#[allow(dead_code)]
pub(crate) fn sockaddr_to_peer_addr(
    addr: &libc::sockaddr_storage,
    len: u32,
) -> Option<crate::connection::PeerAddr> {
    match addr.ss_family as libc::c_int {
        libc::AF_INET | libc::AF_INET6 => {
            sockaddr_to_socket_addr(addr, len).map(crate::connection::PeerAddr::Tcp)
        }
        libc::AF_UNIX => {
            let sa = unsafe { &*(addr as *const _ as *const libc::sockaddr_un) };
            let path_offset = memoffset(sa);
            let path_len = (len as usize).saturating_sub(path_offset);
            if path_len == 0 {
                Some(crate::connection::PeerAddr::Unix(std::path::PathBuf::new()))
            } else {
                #[allow(clippy::unnecessary_cast)]
                let bytes: &[u8] = unsafe {
                    std::slice::from_raw_parts(sa.sun_path.as_ptr() as *const u8, path_len)
                };
                let end = bytes.iter().position(|&b| b == 0).unwrap_or(path_len);
                let path = std::str::from_utf8(&bytes[..end]).unwrap_or("");
                Some(crate::connection::PeerAddr::Unix(std::path::PathBuf::from(
                    path,
                )))
            }
        }
        _ => None,
    }
}

/// Offset of `sun_path` within `sockaddr_un`.
fn memoffset(sa: &libc::sockaddr_un) -> usize {
    let base = sa as *const _ as usize;
    let path = sa.sun_path.as_ptr() as usize;
    path - base
}

/// Write a Unix socket path into a sockaddr_storage, return the address length.
pub(crate) fn unix_path_to_sockaddr(
    path: &std::path::Path,
    storage: &mut libc::sockaddr_storage,
) -> u32 {
    unsafe {
        std::ptr::write_bytes(
            storage as *mut _ as *mut u8,
            0,
            std::mem::size_of::<libc::sockaddr_storage>(),
        );
    }
    let sa = storage as *mut _ as *mut libc::sockaddr_un;
    unsafe {
        (*sa).sun_family = libc::AF_UNIX as libc::sa_family_t;
    }
    let path_bytes = path.as_os_str().as_encoded_bytes();
    let max_len = std::mem::size_of_val(unsafe { &(*sa).sun_path }) - 1;
    let copy_len = path_bytes.len().min(max_len);
    #[allow(clippy::unnecessary_cast)]
    unsafe {
        std::ptr::copy_nonoverlapping(
            path_bytes.as_ptr(),
            (*sa).sun_path.as_mut_ptr() as *mut u8,
            copy_len,
        );
    }
    let sa_ref = unsafe { &*sa };
    let offset = memoffset(sa_ref);
    (offset + copy_len + 1) as u32
}

/// Write a SocketAddr into a sockaddr_storage, return the address length.
pub(crate) fn socket_addr_to_sockaddr(
    addr: SocketAddr,
    storage: &mut libc::sockaddr_storage,
) -> u32 {
    unsafe {
        std::ptr::write_bytes(
            storage as *mut _ as *mut u8,
            0,
            std::mem::size_of::<libc::sockaddr_storage>(),
        );
    }
    match addr {
        SocketAddr::V4(v4) => {
            let sa = storage as *mut _ as *mut libc::sockaddr_in;
            unsafe {
                (*sa).sin_family = libc::AF_INET as libc::sa_family_t;
                (*sa).sin_port = v4.port().to_be();
                (*sa).sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            }
            std::mem::size_of::<libc::sockaddr_in>() as u32
        }
        SocketAddr::V6(v6) => {
            let sa = storage as *mut _ as *mut libc::sockaddr_in6;
            unsafe {
                (*sa).sin6_family = libc::AF_INET6 as libc::sa_family_t;
                (*sa).sin6_port = v6.port().to_be();
                (*sa).sin6_flowinfo = v6.flowinfo();
                (*sa).sin6_addr.s6_addr = v6.ip().octets();
                (*sa).sin6_scope_id = v6.scope_id();
            }
            std::mem::size_of::<libc::sockaddr_in6>() as u32
        }
    }
}
