//! Shared UDP GRO (Generic Receive Offload) helpers used by both backends.
//!
//! With `setsockopt(SOL_UDP, UDP_GRO)` the kernel coalesces consecutive
//! same-flow datagrams into one `recvmsg` delivery and attaches a control
//! message carrying the per-segment size, which the receiver uses to split
//! the coalesced payload back into individual datagrams.

/// `setsockopt` option name for UDP GRO. Not always re-exported by `libc`,
/// so we pin the kernel value (`include/uapi/linux/udp.h`).
pub(crate) const UDP_GRO: libc::c_int = 104;

/// Control-region length to reserve on a recvmsg buffer when GRO is enabled.
/// The kernel writes a single `UDP_GRO` cmsg (one `cmsghdr` + an `int`);
/// 32 bytes is comfortable headroom over `CMSG_SPACE(sizeof(int))`.
pub(crate) const UDP_GRO_CMSG_LEN: usize = 32;

/// Parse the `UDP_GRO` segment size out of a recvmsg control region.
///
/// Returns the per-segment size used to split a coalesced payload, or `None`
/// when no GRO cmsg is present (the kernel delivered a single datagram).
pub(crate) fn parse_segment_size(control: &[u8]) -> Option<u32> {
    let hdr_size = std::mem::size_of::<libc::cmsghdr>();
    let align = std::mem::align_of::<libc::cmsghdr>();
    let mut offset = 0usize;
    while offset + hdr_size <= control.len() {
        // `read_unaligned`: the control region has no alignment guarantee.
        let hdr =
            unsafe { std::ptr::read_unaligned(control[offset..].as_ptr() as *const libc::cmsghdr) };
        if hdr.cmsg_len < hdr_size {
            break;
        }
        if hdr.cmsg_level == libc::IPPROTO_UDP && hdr.cmsg_type == UDP_GRO {
            // The kernel writes the gso size as a C `int`.
            let data_off = offset + hdr_size;
            if data_off + std::mem::size_of::<libc::c_int>() <= control.len() {
                let v = unsafe {
                    std::ptr::read_unaligned(control[data_off..].as_ptr() as *const libc::c_int)
                };
                if v > 0 {
                    return Some(v as u32);
                }
            }
            return None;
        }
        let next = offset + ((hdr.cmsg_len + align - 1) & !(align - 1));
        if next <= offset {
            break;
        }
        offset = next;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a control buffer holding one cmsg with the given level/type and
    /// a 4-byte `int` payload, laid out exactly as the kernel would.
    fn make_cmsg(level: libc::c_int, ty: libc::c_int, val: libc::c_int) -> Vec<u8> {
        let hdr_size = std::mem::size_of::<libc::cmsghdr>();
        let data_len = std::mem::size_of::<libc::c_int>();
        let mut buf = vec![0u8; hdr_size + data_len];
        let mut hdr: libc::cmsghdr = unsafe { std::mem::zeroed() };
        hdr.cmsg_len = (hdr_size + data_len) as _;
        hdr.cmsg_level = level;
        hdr.cmsg_type = ty;
        unsafe {
            std::ptr::write_unaligned(buf.as_mut_ptr() as *mut libc::cmsghdr, hdr);
            std::ptr::write_unaligned(buf[hdr_size..].as_mut_ptr() as *mut libc::c_int, val);
        }
        buf
    }

    #[test]
    fn parses_udp_gro_segment_size() {
        let buf = make_cmsg(libc::IPPROTO_UDP, UDP_GRO, 1400);
        assert_eq!(parse_segment_size(&buf), Some(1400));
    }

    #[test]
    fn ignores_other_cmsg() {
        let buf = make_cmsg(libc::SOL_SOCKET, libc::SCM_RIGHTS, 7);
        assert_eq!(parse_segment_size(&buf), None);
    }

    #[test]
    fn empty_control_is_none() {
        assert_eq!(parse_segment_size(&[]), None);
        assert_eq!(parse_segment_size(&[0u8; 4]), None);
    }

    #[test]
    fn zero_or_negative_segment_is_none() {
        let buf = make_cmsg(libc::IPPROTO_UDP, UDP_GRO, 0);
        assert_eq!(parse_segment_size(&buf), None);
    }
}
