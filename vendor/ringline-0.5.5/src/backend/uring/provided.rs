use std::io;
use std::ptr;
use std::sync::atomic::{self, AtomicU16};

/// A ring-mapped provided buffer ring for multishot recv operations.
///
/// The kernel picks a buffer from this ring at completion time.
/// We replenish buffers after processing to keep the ring full.
pub struct ProvidedBufRing {
    /// Pointer to the mmap'd ring (shared with kernel).
    ring_ptr: *mut u8,
    /// Size of the mmap'd ring region.
    ring_mmap_len: usize,
    /// Backing memory for all buffers.
    buf_backing: Vec<u8>,
    /// Buffer group ID.
    bgid: u16,
    /// Number of buffers (must be power of 2).
    ring_size: u16,
    /// Size of each buffer.
    buf_size: u32,
    /// Current tail index (we write, kernel reads).
    tail: u16,
    /// Mask for ring index wrapping.
    mask: u16,
    /// Buffers handed out to a completion (kernel-selected) and not yet
    /// replenished. `ring_size - outstanding` buffers are free in the ring for
    /// the kernel to pick. Maintained by `on_handout` (at recv completion) and
    /// `replenish_batch` (on return). Backpressure decisions read `free()`.
    outstanding: u32,
}

/// An io_uring buf_ring entry (matches kernel struct io_uring_buf).
#[repr(C)]
struct BufRingEntry {
    addr: u64,
    len: u32,
    bid: u16,
    resv: u16,
}

impl ProvidedBufRing {
    /// Size of a single ring entry.
    const ENTRY_SIZE: usize = std::mem::size_of::<BufRingEntry>();

    /// Create a new provided buffer ring.
    ///
    /// `ring_size` must be a power of 2.
    /// The ring memory is mmap'd so the kernel can access it directly.
    pub fn new(bgid: u16, ring_size: u16, buf_size: u32) -> io::Result<Self> {
        assert!(ring_size.is_power_of_two(), "ring_size must be power of 2");

        let ring_mmap_len = ring_size as usize * Self::ENTRY_SIZE;
        let buf_backing = vec![0u8; ring_size as usize * buf_size as usize];

        // mmap anonymous memory for the ring (page-aligned, shared with kernel)
        let ring_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                ring_mmap_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1,
                0,
            )
        };
        if ring_ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let mut ring = ProvidedBufRing {
            ring_ptr: ring_ptr as *mut u8,
            ring_mmap_len,
            buf_backing,
            bgid,
            ring_size,
            buf_size,
            tail: 0,
            mask: ring_size - 1,
            outstanding: 0,
        };

        // Pre-fill the ring with all buffers
        for i in 0..ring_size {
            ring.push_entry(i);
        }
        // Make entries visible to kernel
        ring.commit_tail();

        Ok(ring)
    }

    /// Get the ring pointer for `register_buf_ring()`.
    pub fn ring_addr(&self) -> u64 {
        self.ring_ptr as u64
    }

    /// Get the buffer group ID.
    pub fn bgid(&self) -> u16 {
        self.bgid
    }

    /// Get the ring size (number of entries).
    pub fn ring_entries(&self) -> u32 {
        self.ring_size as u32
    }

    /// Record that the kernel handed one buffer to a completion (consumed one
    /// from the ring). Call exactly once per recv completion that selected a
    /// buffer, before it is processed.
    ///
    /// Wired at every main-ring `buffer_select` in the recv completion handlers;
    /// balanced against `replenish_batch`. The `outstanding <= ring_size`
    /// assertion is exercised end-to-end by the recv test suite.
    #[inline]
    pub fn on_handout(&mut self) {
        self.outstanding += 1;
        debug_assert!(
            self.outstanding <= self.ring_size as u32,
            "provided-ring handout exceeds ring size ({} > {})",
            self.outstanding,
            self.ring_size
        );
    }

    /// Buffers currently available in the ring for the kernel to select.
    ///
    /// Consumed by the segmented-recv low-water reserve (the hold branch in
    /// `handle_recv_multi` via `recv::occupancy::delivery_decision`).
    #[inline]
    pub fn free(&self) -> u32 {
        self.ring_entries().saturating_sub(self.outstanding)
    }

    /// Get a pointer and length for a buffer by its ID.
    pub fn get_buffer(&self, bid: u16) -> (*const u8, u32) {
        let offset = bid as usize * self.buf_size as usize;
        let ptr = unsafe { self.buf_backing.as_ptr().add(offset) };
        (ptr, self.buf_size)
    }

    /// Batch replenish multiple buffers. Returns them to the ring and accounts
    /// them against `outstanding` (the sole replenish accounting point).
    pub fn replenish_batch(&mut self, bids: &[u16]) {
        for &bid in bids {
            self.push_entry(bid);
        }
        if !bids.is_empty() {
            // Tripwire: replenishing more buffers than are outstanding means a bid
            // was returned to the ring more than once (a double-replenish) — the
            // recurring failure mode for the zero-copy hold/segment/forward paths.
            // `saturating_sub` keeps `free()` sane in release; this catches the bug
            // in debug/tests before it can silently corrupt ring accounting.
            debug_assert!(
                self.outstanding >= bids.len() as u32,
                "double-replenish: returning {} buffers but only {} outstanding",
                bids.len(),
                self.outstanding,
            );
            self.outstanding = self.outstanding.saturating_sub(bids.len() as u32);
            self.commit_tail();
        }
    }

    fn push_entry(&mut self, bid: u16) {
        let ring_idx = (self.tail & self.mask) as usize;
        let entry_ptr = unsafe {
            self.ring_ptr
                .add(ring_idx * Self::ENTRY_SIZE)
                .cast::<BufRingEntry>()
        };
        let buf_offset = bid as usize * self.buf_size as usize;
        let buf_addr = unsafe { self.buf_backing.as_ptr().add(buf_offset) };
        unsafe {
            ptr::write(
                entry_ptr,
                BufRingEntry {
                    addr: buf_addr as u64,
                    len: self.buf_size,
                    bid,
                    resv: 0,
                },
            );
        }
        self.tail = self.tail.wrapping_add(1);
    }

    fn commit_tail(&self) {
        // The tail is at offset 14 within the ring header. The kernel overlays
        // the header with bufs[0]: struct io_uring_buf_ring { union {
        //   struct { u64 resv1; u32 resv2; u16 resv3; u16 tail; };
        //   struct io_uring_buf bufs[0]; }; };
        // io_uring_buf: { u64 addr(0); u32 len(8); u16 bid(12); u16 resv(14); }
        // So tail = bufs[0].resv = offset 14.
        let tail_ptr = unsafe { self.ring_ptr.add(14).cast::<AtomicU16>() };
        unsafe {
            (*tail_ptr).store(self.tail, atomic::Ordering::Release);
        }
    }
}

impl Drop for ProvidedBufRing {
    fn drop(&mut self) {
        if !self.ring_ptr.is_null() {
            unsafe {
                libc::munmap(self.ring_ptr as *mut _, self.ring_mmap_len);
            }
        }
    }
}

// Safety: The ring is only accessed from a single worker thread.
unsafe impl Send for ProvidedBufRing {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn occupancy_tracks_handout_and_replenish() {
        let mut ring = ProvidedBufRing::new(0, 8, 4096).expect("ring");
        // All 8 buffers start free (pre-filled), none outstanding.
        assert_eq!(ring.ring_entries(), 8);
        assert_eq!(ring.free(), 8);

        // Hand out 3.
        for _ in 0..3 {
            ring.on_handout();
        }
        assert_eq!(ring.free(), 5);

        // Replenish 2 (bids are arbitrary here; accounting is by count).
        ring.replenish_batch(&[0, 1]);
        assert_eq!(ring.free(), 7);

        // Replenish the last outstanding one.
        ring.replenish_batch(&[2]);
        assert_eq!(ring.free(), 8);
    }

    /// Replenishing more than are outstanding is a double-replenish; the debug
    /// tripwire fires in debug builds (where tests normally run).
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "double-replenish")]
    fn double_replenish_trips_debug_assert() {
        let mut ring = ProvidedBufRing::new(0, 4, 4096).expect("ring");
        // Nothing outstanding → returning 3 buffers is a double-replenish.
        ring.replenish_batch(&[0, 1, 2]);
    }

    /// In release builds (tripwire compiled out) the `saturating_sub` still keeps
    /// `free()` from underflowing.
    #[test]
    #[cfg(not(debug_assertions))]
    fn replenish_saturates_never_underflows() {
        let mut ring = ProvidedBufRing::new(0, 4, 4096).expect("ring");
        ring.replenish_batch(&[0, 1, 2]);
        assert_eq!(ring.free(), 4);
        assert_eq!(ring.ring_entries(), 4);
    }

    #[test]
    fn free_reaches_zero_when_fully_drained() {
        let mut ring = ProvidedBufRing::new(0, 4, 4096).expect("ring");
        for _ in 0..4 {
            ring.on_handout();
        }
        assert_eq!(ring.free(), 0);
    }
}
