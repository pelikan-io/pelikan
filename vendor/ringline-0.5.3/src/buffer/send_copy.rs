/// Pool of library-owned buffers for copying send data (soundness fix).
///
/// When `send()` is called, the data is copied into a pool slot so the SQE
/// points to memory owned by the library. The slot is released on the Send CQE.
pub struct SendCopyPool {
    backing: Vec<u8>,
    slot_size: u32,
    count: u16,
    free_list: Vec<u16>,
    slot_offset: Vec<u32>, // current byte offset within slot (advances on partial send)
    slot_remaining: Vec<u32>, // bytes remaining to send
    in_use: Vec<bool>,     // double-free protection
    // Whether this slot holds the final chunk of its logical send. A send
    // larger than one slot is split across several slots; only the last one
    // is marked, so the send waiter is woken once per logical send rather
    // than once per chunk. Independent single-slot sends are always final.
    slot_end_of_send: Vec<bool>,
}

impl SendCopyPool {
    /// Create a new pool with `count` slots, each `slot_size` bytes.
    pub fn new(count: u16, slot_size: u32) -> Self {
        let total = count as usize * slot_size as usize;
        let backing = vec![0u8; total];
        let free_list: Vec<u16> = (0..count).rev().collect();
        let n = count as usize;
        SendCopyPool {
            backing,
            slot_size,
            count,
            free_list,
            slot_offset: vec![0u32; n],
            slot_remaining: vec![0u32; n],
            in_use: vec![false; n],
            slot_end_of_send: vec![true; n],
        }
    }

    /// Allocate a slot, copy `data` into it, and return (slot_index, ptr, len).
    /// Returns `None` if no slots are free or data exceeds slot size.
    pub fn copy_in(&mut self, data: &[u8]) -> Option<(u16, *const u8, u32)> {
        if data.len() > self.slot_size as usize {
            return None;
        }
        let idx = match self.free_list.pop() {
            Some(i) => i,
            None => {
                crate::metrics::POOL.increment(crate::metrics::pool::SEND_EXHAUSTED);
                return None;
            }
        };
        let offset = idx as usize * self.slot_size as usize;
        self.backing[offset..offset + data.len()].copy_from_slice(data);
        let ptr = self.backing.as_ptr().wrapping_add(offset);
        self.slot_offset[idx as usize] = 0;
        self.slot_remaining[idx as usize] = data.len() as u32;
        self.in_use[idx as usize] = true;
        self.slot_end_of_send[idx as usize] = true;
        Some((idx, ptr, data.len() as u32))
    }

    /// Allocate a slot and copy multiple contiguous segments into it sequentially.
    /// Returns `None` if no slots are free or `total_len` exceeds slot size.
    ///
    /// # Safety
    /// Each `(ptr, len)` pair must point to valid readable memory.
    pub unsafe fn copy_in_gather(
        &mut self,
        parts: &[(*const u8, usize)],
        total_len: usize,
    ) -> Option<(u16, *const u8, u32)> {
        if total_len > self.slot_size as usize {
            return None;
        }
        let idx = match self.free_list.pop() {
            Some(i) => i,
            None => {
                crate::metrics::POOL.increment(crate::metrics::pool::SEND_EXHAUSTED);
                return None;
            }
        };
        let base = idx as usize * self.slot_size as usize;
        let mut dest_offset = 0;
        for &(ptr, len) in parts {
            let src = unsafe { std::slice::from_raw_parts(ptr, len) };
            self.backing[base + dest_offset..base + dest_offset + len].copy_from_slice(src);
            dest_offset += len;
        }
        let out_ptr = self.backing.as_ptr().wrapping_add(base);
        self.slot_offset[idx as usize] = 0;
        self.slot_remaining[idx as usize] = total_len as u32;
        self.in_use[idx as usize] = true;
        self.slot_end_of_send[idx as usize] = true;
        Some((idx, out_ptr, total_len as u32))
    }

    /// Allocate an empty slot for incremental filling (e.g. TLS ciphertext
    /// written directly by `write_tls` via `PoolWriter`). Returns
    /// `(slot, base_ptr, capacity)`. The caller fills bytes front-to-back
    /// and must call [`set_filled`](Self::set_filled) with the final length
    /// before the slot is used in an SQE (until then remaining == 0).
    pub fn alloc_raw(&mut self) -> Option<(u16, *mut u8, u32)> {
        let idx = match self.free_list.pop() {
            Some(i) => i,
            None => {
                crate::metrics::POOL.increment(crate::metrics::pool::SEND_EXHAUSTED);
                return None;
            }
        };
        let offset = idx as usize * self.slot_size as usize;
        self.slot_offset[idx as usize] = 0;
        self.slot_remaining[idx as usize] = 0;
        self.in_use[idx as usize] = true;
        self.slot_end_of_send[idx as usize] = true;
        let ptr = self.backing.as_mut_ptr().wrapping_add(offset);
        Some((idx, ptr, self.slot_size))
    }

    /// Record how many bytes were filled into a slot from [`alloc_raw`](Self::alloc_raw).
    pub fn set_filled(&mut self, slot: u16, len: u32) {
        let i = slot as usize;
        debug_assert!(self.in_use[i]);
        debug_assert!(len <= self.slot_size);
        self.slot_offset[i] = 0;
        self.slot_remaining[i] = len;
    }

    /// Release a slot back to the free list (called on Send CQE).
    pub fn release(&mut self, idx: u16) {
        debug_assert!((idx as usize) < self.count as usize);
        if !self.in_use[idx as usize] {
            return; // already released — prevent double-free
        }
        self.in_use[idx as usize] = false;
        self.slot_offset[idx as usize] = 0;
        self.slot_remaining[idx as usize] = 0;
        self.free_list.push(idx);
    }

    /// Try to advance a partial send. If `bytes_sent < remaining`, updates
    /// offset/remaining and returns `Some((new_ptr, new_remaining))` for
    /// resubmission. Returns `None` if fully sent.
    pub fn try_advance(&mut self, slot: u16, bytes_sent: u32) -> Option<(*const u8, u32)> {
        let i = slot as usize;
        debug_assert!(self.in_use[i]);
        debug_assert!(bytes_sent <= self.slot_remaining[i]);
        let new_remaining = self.slot_remaining[i] - bytes_sent;
        if new_remaining == 0 {
            return None;
        }
        self.slot_offset[i] += bytes_sent;
        self.slot_remaining[i] = new_remaining;
        let base = i * self.slot_size as usize;
        let ptr = self
            .backing
            .as_ptr()
            .wrapping_add(base + self.slot_offset[i] as usize);
        Some((ptr, new_remaining))
    }

    /// Whether a slot is currently in use (allocated but not yet released).
    pub fn in_use(&self, slot: u16) -> bool {
        self.in_use.get(slot as usize).copied().unwrap_or(false)
    }

    /// Get the current pointer and remaining length for a slot (for resubmission retries).
    pub fn current_ptr_remaining(&self, slot: u16) -> (*const u8, u32) {
        let i = slot as usize;
        let base = i * self.slot_size as usize;
        let ptr = self
            .backing
            .as_ptr()
            .wrapping_add(base + self.slot_offset[i] as usize);
        (ptr, self.slot_remaining[i])
    }

    /// Get the original total length for a slot: offset + remaining.
    /// Valid between `copy_in` and `release`.
    pub fn original_len(&self, slot: u16) -> u32 {
        let i = slot as usize;
        debug_assert!(self.in_use[i]);
        self.slot_offset[i] + self.slot_remaining[i]
    }

    /// Mark whether this slot holds the final chunk of its logical send.
    /// Defaults to `true` on allocation; `DriverCtx::send` clears it on every
    /// chunk but the last when a send is split across slots.
    pub fn set_end_of_send(&mut self, slot: u16, end_of_send: bool) {
        self.slot_end_of_send[slot as usize] = end_of_send;
    }

    /// Whether this slot holds the final chunk of its logical send.
    pub fn is_end_of_send(&self, slot: u16) -> bool {
        self.slot_end_of_send[slot as usize]
    }

    /// Bytes per slot.
    pub fn slot_size(&self) -> u32 {
        self.slot_size
    }

    /// Number of free slots.
    #[cfg(test)]
    pub fn free_count(&self) -> usize {
        self.free_list.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_in_and_release() {
        let mut pool = SendCopyPool::new(4, 128);
        assert_eq!(pool.free_count(), 4);

        let (idx, ptr, len) = pool.copy_in(b"hello").unwrap();
        assert_eq!(len, 5);
        assert_eq!(pool.free_count(), 3);

        // Verify data was copied
        let slice = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
        assert_eq!(slice, b"hello");

        pool.release(idx);
        assert_eq!(pool.free_count(), 4);
    }

    #[test]
    fn exhaust_pool() {
        let mut pool = SendCopyPool::new(2, 64);
        let _ = pool.copy_in(b"a").unwrap();
        let _ = pool.copy_in(b"b").unwrap();
        assert!(pool.copy_in(b"c").is_none());
    }

    #[test]
    fn exhausted_pool_returns_none_copy_in() {
        // A 1-slot pool: first alloc succeeds, second returns None (SEND_EXHAUSTED
        // metric is incremented inside; we assert the observable None behavior).
        let mut pool = SendCopyPool::new(1, 64);
        let _ = pool.copy_in(b"first").unwrap();
        assert_eq!(pool.free_count(), 0);
        assert!(pool.copy_in(b"second").is_none());
    }

    #[test]
    fn exhausted_pool_returns_none_copy_in_gather() {
        // Same as above but exercising the gather variant.
        let mut pool = SendCopyPool::new(1, 64);
        let data = b"first";
        let parts: &[(*const u8, usize)] = &[(data.as_ptr(), data.len())];
        let _ = unsafe { pool.copy_in_gather(parts, data.len()) }.unwrap();
        assert_eq!(pool.free_count(), 0);
        let data2 = b"second";
        let parts2: &[(*const u8, usize)] = &[(data2.as_ptr(), data2.len())];
        assert!(unsafe { pool.copy_in_gather(parts2, data2.len()) }.is_none());
    }

    #[test]
    fn data_too_large() {
        let mut pool = SendCopyPool::new(4, 4);
        assert!(pool.copy_in(b"toolarge").is_none());
    }

    #[test]
    fn try_advance_partial() {
        let mut pool = SendCopyPool::new(4, 128);
        let (idx, _ptr, len) = pool.copy_in(b"hello world").unwrap();
        assert_eq!(len, 11);
        assert_eq!(pool.original_len(idx), 11);

        // Partial send: 5 of 11 bytes sent.
        let result = pool.try_advance(idx, 5);
        assert!(result.is_some());
        let (new_ptr, new_remaining) = result.unwrap();
        assert_eq!(new_remaining, 6);
        assert_eq!(pool.original_len(idx), 11);

        // Verify the pointer points to the remaining data.
        let slice = unsafe { std::slice::from_raw_parts(new_ptr, new_remaining as usize) };
        assert_eq!(slice, b" world");

        // Second partial: 4 of 6 remaining.
        let result = pool.try_advance(idx, 4);
        assert!(result.is_some());
        let (new_ptr2, new_remaining2) = result.unwrap();
        assert_eq!(new_remaining2, 2);
        assert_eq!(pool.original_len(idx), 11);

        let slice2 = unsafe { std::slice::from_raw_parts(new_ptr2, new_remaining2 as usize) };
        assert_eq!(slice2, b"ld");

        // Final send: all remaining bytes sent.
        let result = pool.try_advance(idx, 2);
        assert!(result.is_none());

        pool.release(idx);
        assert_eq!(pool.free_count(), 4);
    }

    #[test]
    fn try_advance_full_send() {
        let mut pool = SendCopyPool::new(4, 128);
        let (idx, _ptr, len) = pool.copy_in(b"hello").unwrap();
        assert_eq!(len, 5);

        // Full send on first attempt — returns None.
        let result = pool.try_advance(idx, 5);
        assert!(result.is_none());
        assert_eq!(pool.original_len(idx), 5);

        pool.release(idx);
    }

    #[test]
    fn release_clears_tracking() {
        let mut pool = SendCopyPool::new(4, 128);
        let (idx, _ptr, _len) = pool.copy_in(b"test data").unwrap();

        // Partial send.
        pool.try_advance(idx, 4);
        assert_eq!(pool.original_len(idx), 9);

        // Release clears tracking.
        pool.release(idx);

        // Re-allocate — LIFO free list returns the just-released slot.
        let (idx2, _ptr2, len2) = pool.copy_in(b"new").unwrap();
        assert_eq!(idx2, idx);
        assert_eq!(len2, 3);
        assert_eq!(pool.original_len(idx2), 3);
    }
}
