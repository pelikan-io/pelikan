/// Per-connection byte accumulator for contiguous recv data.
///
/// Handlers always see a contiguous `&[u8]` and return the number of bytes consumed.
/// Unconsumed bytes are retained via O(1) `advance()` instead of shifting.
use bytes::{Bytes, BytesMut};

pub struct RecvAccumulator {
    buf: BytesMut,
    /// Unconsumed remainder of a previously frozen (`take_frozen`) view,
    /// held as a refcounted slice. Serving subsequent parses from this is
    /// O(1); it is merged (one copy) into `buf` only when NEW data arrives
    /// while a remainder is held. The old representation copied the whole
    /// remainder back into `buf` on every put-back — O(B²) over a
    /// pipelined batch of B responses and O(N·K) for an N-byte value
    /// arriving in K chunks.
    ///
    /// Invariant: `frozen` is `None` or non-empty.
    frozen: Option<Bytes>,
    /// Upper bound on total buffered bytes (`frozen` remainder + `buf`)
    /// after an `append`. `append` reports overflow rather than growing
    /// past this.
    max_size: usize,
    /// High-water total-size target announced via [`reserve`](Self::reserve)
    /// (a length-prefixed parser that has seen its header). Consulted by
    /// every growth site — `append` and the `unfreeze` merge — so a
    /// multi-chunk message allocates once at full size instead of running
    /// the doubling-regrowth cascade. Cleared when the contents drain.
    reserve_target: usize,
}

impl RecvAccumulator {
    /// Create a new accumulator with the given initial capacity and an
    /// unlimited size cap. For runtime use, prefer
    /// [`new_with_max`](Self::new_with_max).
    #[allow(dead_code)]
    pub fn new(capacity: usize) -> Self {
        Self::new_with_max(capacity, usize::MAX)
    }

    /// Create a new accumulator with an initial capacity and an upper-bound
    /// `max_size`. `append` rejects data that would push `buf.len()` past
    /// `max_size`, leaving the existing contents intact so the caller can
    /// fail the connection rather than OOM.
    pub fn new_with_max(capacity: usize, max_size: usize) -> Self {
        RecvAccumulator {
            buf: BytesMut::with_capacity(capacity),
            frozen: None,
            max_size,
            reserve_target: 0,
        }
    }

    /// Append received bytes. Returns `false` if the append would push the
    /// accumulator past its `max_size` — in that case the existing contents
    /// are preserved and the caller should close the connection (or accept
    /// that intermediate-flush data is dropped, depending on context).
    ///
    /// Not marked `#[must_use]`: not every caller is in a position to fail
    /// the connection (e.g. intermediate buffer-shuffling paths inside
    /// `WithDataFuture::poll`). The authoritative cap-enforcement sites are
    /// the kernel-recv handlers in `backend/uring/event_loop.rs`.
    pub fn append(&mut self, data: &[u8]) -> bool {
        let total = self.total_len();
        if total.saturating_add(data.len()) > self.max_size {
            return false;
        }
        // When a growth is imminent and a reserve target was announced,
        // size the allocation for the whole remaining message up front.
        if self.buf.capacity() - self.buf.len() < data.len() {
            let want = self.reserve_target.saturating_sub(total).max(data.len());
            self.buf.reserve(want);
        }
        self.buf.extend_from_slice(data);
        true
    }

    /// Total buffered bytes: held frozen remainder + mutable tail.
    fn total_len(&self) -> usize {
        self.frozen.as_ref().map_or(0, |f| f.len()) + self.buf.len()
    }

    /// Whether the accumulator holds no bytes at all — O(1), and unlike
    /// `data().is_empty()` it never merges a held frozen remainder into
    /// `buf`. Hot paths that only need emptiness (e.g. the zero-copy recv
    /// fast-path check) must use this: a `data()` call while a remainder is
    /// held and new bytes have arrived forces a full merge copy per call —
    /// O(N·K) over an N-byte value arriving in K chunks.
    pub fn is_empty(&self) -> bool {
        self.total_len() == 0
    }

    /// Merge a held frozen remainder into `buf` so the contents are one
    /// contiguous mutable region again. One copy of the remainder; only
    /// needed when new data arrived while a remainder was held, or when a
    /// borrowed-slice consumer needs a single `&[u8]` view.
    fn unfreeze(&mut self) {
        let Some(rem) = self.frozen.take() else {
            return;
        };
        // If the parser kept no slices alive (streaming NeedMore cycles),
        // the remainder is uniquely referenced and `try_into_mut` recovers
        // the original allocation with the data already in place — the
        // merge then costs only the newly appended bytes, not the whole
        // remainder. With live parser slices (pipelined batches) this
        // falls back to one full copy, which only happens when new data
        // arrives mid-batch.
        match rem.try_into_mut() {
            Ok(mut recovered) => {
                if !self.buf.is_empty() {
                    // Honor a pending reserve target so the streaming
                    // NeedAtLeast cycle merges into a full-size buffer once
                    // instead of regrowing per appended chunk.
                    let want = self
                        .reserve_target
                        .saturating_sub(recovered.len() + self.buf.len())
                        .saturating_add(self.buf.len());
                    if recovered.capacity() - recovered.len() < want {
                        recovered.reserve(want);
                    }
                    recovered.extend_from_slice(&self.buf);
                }
                self.buf = recovered;
            }
            Err(rem) => {
                if self.buf.is_empty() {
                    self.buf.extend_from_slice(&rem);
                } else {
                    let merged_len = rem.len() + self.buf.len();
                    let mut merged = BytesMut::with_capacity(merged_len.max(self.reserve_target));
                    merged.extend_from_slice(&rem);
                    merged.extend_from_slice(&self.buf);
                    self.buf = merged;
                }
            }
        }
    }

    /// Get a reference to the accumulated data.
    ///
    /// Merges a held frozen remainder first so the view is contiguous —
    /// borrowed-slice consumers (`with_data`, streams, TLS drains) pay the
    /// merge only when they interleave with `with_bytes` on one connection.
    pub fn data(&mut self) -> &[u8] {
        if self.frozen.is_some() && !self.buf.is_empty() {
            self.unfreeze();
        }
        if let Some(ref f) = self.frozen {
            &f[..]
        } else {
            &self.buf[..]
        }
    }

    /// Consume `n` bytes from the front — O(1).
    pub fn consume(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        // After a `data()` view, at most one of `frozen` / `buf` is
        // non-empty (data() merges when both are).
        if let Some(ref mut f) = self.frozen {
            debug_assert!(
                n <= f.len(),
                "consume({n}) exceeds frozen remainder length {}",
                f.len()
            );
            let n = n.min(f.len());
            f.advance(n);
            if f.is_empty() {
                self.frozen = None;
            }
            if self.total_len() == 0 {
                self.reserve_target = 0;
            }
            return;
        }
        debug_assert!(
            n <= self.buf.len(),
            "consume({n}) exceeds buffer length {}",
            self.buf.len()
        );
        let n = n.min(self.buf.len());
        self.buf.advance(n);
        if self.total_len() == 0 {
            self.reserve_target = 0;
        }
    }

    /// Announce that at least `additional` more bytes are expected beyond
    /// the current contents (a length-prefixed parser that has seen its
    /// header). Records a high-water total-size target — clamped to
    /// `max_size` — that `append` and the `unfreeze` merge use to allocate
    /// once at full size instead of running the doubling-regrowth cascade
    /// (~2× the payload in extra memcpy for a multi-MB message arriving
    /// in chunks). The target clears when the contents drain.
    pub fn reserve(&mut self, additional: usize) {
        let target = self
            .total_len()
            .saturating_add(additional)
            .min(self.max_size);
        self.reserve_target = self.reserve_target.max(target);
    }

    /// Reset the accumulator (discard all data).
    pub fn reset(&mut self) {
        self.frozen = None;
        self.buf.clear();
        self.reserve_target = 0;
    }

    /// Return the current backing-buffer capacity. Test-only.
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.buf.capacity()
    }
}

use bytes::Buf;

/// Parallel `Vec<RecvAccumulator>` indexed by connection index.
/// Stored as a separate field in EventLoop for borrow splitting.
pub struct AccumulatorTable {
    accumulators: Vec<RecvAccumulator>,
}

impl AccumulatorTable {
    /// Create a table with `count` accumulators, each with the given initial
    /// capacity and no upper-bound size.
    #[allow(dead_code)]
    pub fn new(count: u32, capacity: usize) -> Self {
        Self::new_with_max(count, capacity, usize::MAX)
    }

    /// Create a table with `count` accumulators, each with the given initial
    /// capacity and upper-bound `max_size`.
    pub fn new_with_max(count: u32, capacity: usize, max_size: usize) -> Self {
        let mut accumulators = Vec::with_capacity(count as usize);
        for _ in 0..count {
            accumulators.push(RecvAccumulator::new_with_max(capacity, max_size));
        }
        AccumulatorTable { accumulators }
    }

    /// Append data to the accumulator at the given index. Returns `false`
    /// if the append would exceed the accumulator's `max_size`; in that
    /// case the existing buffer is unchanged. The authoritative
    /// cap-enforcement sites are the kernel-recv handlers; intermediate
    /// flush callers may ignore the return value.
    pub fn append(&mut self, index: u32, data: &[u8]) -> bool {
        self.accumulators[index as usize].append(data)
    }

    /// Get accumulated data at the given index.
    ///
    /// `&mut` because a held frozen remainder may need a one-time merge to
    /// present a contiguous view (see [`RecvAccumulator::data`]).
    pub fn data(&mut self, index: u32) -> &[u8] {
        self.accumulators[index as usize].data()
    }

    /// Whether the accumulator at the given index is empty — O(1) and
    /// non-merging, unlike `data(index).is_empty()` (see
    /// [`RecvAccumulator::is_empty`]).
    pub fn is_empty(&self, index: u32) -> bool {
        self.accumulators[index as usize].is_empty()
    }

    /// Consume `n` bytes from the accumulator at the given index.
    pub fn consume(&mut self, index: u32, n: usize) {
        self.accumulators[index as usize].consume(n);
    }

    /// Reserve capacity for `additional` more bytes at the given index
    /// (clamped to the accumulator's `max_size`).
    pub fn reserve(&mut self, index: u32, additional: usize) {
        self.accumulators[index as usize].reserve(additional);
    }

    /// Reset the accumulator at the given index.
    pub fn reset(&mut self, index: u32) {
        self.accumulators[index as usize].reset();
    }

    /// Return the current backing-buffer capacity for the accumulator at
    /// the given index. Test-only.
    #[cfg(test)]
    pub(crate) fn capacity(&self, index: u32) -> usize {
        self.accumulators[index as usize].capacity()
    }

    /// Detach the accumulator's buffer as a frozen `Bytes` (O(1)).
    ///
    /// The accumulator is left empty but retains the tail capacity of the
    /// same allocation via `split_to`. Any subsequent `prepend()` of an
    /// unconsumed remainder (the hot pipelined-parse path) reuses that
    /// capacity instead of heap-allocating a fresh `BytesMut`.
    ///
    /// Note: when the returned `Bytes` (or sub-slices the parser keeps, as in
    /// `with_bytes`) outlive the next `append()`, the shared allocation cannot
    /// reclaim its front, so the tail capacity shrinks across cycles and a
    /// later `append()` may still reallocate. The win is the avoided per-parse
    /// remainder allocation, not elimination of all reallocation.
    pub fn take_frozen(&mut self, index: u32) -> Bytes {
        let acc = &mut self.accumulators[index as usize];
        // Serving a held remainder with no new data is the pipelined hot
        // path: a batch of B responses is frozen once and each recv()
        // cycle takes/puts-back the same refcounted Bytes — O(1) per
        // cycle, no copies (previously the remainder was memcpy'd back
        // into the buffer every cycle: O(B²) over the batch).
        if let Some(rem) = acc.frozen.take() {
            if acc.buf.is_empty() {
                return rem;
            }
            // New data arrived while a remainder was held — merge once.
            acc.frozen = Some(rem);
            acc.unfreeze();
        }
        let len = acc.buf.len();
        // Fast path: empty accumulator — `split_to(0)` would unnecessarily
        // upgrade `BytesMut` to shared mode, blocking later front-reclaim.
        if len == 0 {
            return Bytes::new();
        }
        // `split_to(len)` hands back the filled front as `other` and leaves
        // `acc.buf` empty but still owning the tail capacity of the same
        // allocation — O(1), no copy. The `freeze()` on the front is also
        // O(1). Both operations are non-allocating.
        acc.buf.split_to(len).freeze()
    }

    /// Put back the unconsumed remainder of a `take_frozen()` view — O(1).
    ///
    /// Called from the `with_bytes` path when the parser didn't consume
    /// everything (pipelined remainder or incomplete next message). The
    /// remainder is stored as a refcounted slice; it is copied at most once
    /// more (by `unfreeze`) and only if new data arrives before the next
    /// take.
    pub fn put_back(&mut self, index: u32, data: Bytes) {
        if data.is_empty() {
            return;
        }
        let acc = &mut self.accumulators[index as usize];
        debug_assert!(
            acc.frozen.is_none(),
            "put_back: a frozen remainder is already held (take/put must alternate)"
        );
        // `put_back` follows `take_frozen` within one poll, so `buf` holds
        // only data appended after the take (normally nothing).
        //
        // Drop `buf`'s handle when it is empty: `take_frozen`'s `split_to`
        // left it sharing the remainder's allocation, and that extra
        // reference makes `try_into_mut` in `unfreeze` fail — forcing a
        // full-remainder copy on EVERY merge instead of an in-place append
        // of just the new bytes. For an N-byte response streamed in K
        // chunks that is O(N·K): measured 69 GB memcpy'd to receive 4.8 GB
        // of 64 MiB values. Nothing is lost by dropping the handle — once
        // the remainder is uniquely referenced, `try_into_mut` recovers the
        // full original allocation (including this tail capacity).
        if acc.buf.is_empty() {
            acc.buf = BytesMut::new();
        }
        acc.frozen = Some(data);
    }

    /// Put unconsumed data back into the accumulator.
    ///
    /// Called after `take_frozen()` when the parser didn't consume
    /// everything (e.g. pipelined remainder or incomplete next message).
    pub fn prepend(&mut self, index: u32, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let acc = &mut self.accumulators[index as usize];
        // A held frozen remainder belongs BEHIND the prepended data
        // (prepend callers put back OLDER bytes). Merge it into `buf`
        // first so the ordering below is correct.
        acc.unfreeze();
        // NOTE: `buf` may legitimately be non-empty here — either data
        // appended after a `take_frozen`, or a frozen remainder merged by
        // the `unfreeze()` above. The prepended bytes are the OLDEST, so
        // they go in front either way.
        if acc.buf.is_empty() {
            // Fast path (invariant): buffer is empty, tail capacity is
            // retained from `take_frozen`'s `split_to` — no allocation.
            acc.buf.extend_from_slice(data);
        } else {
            // Slow path: new data already present (cannot happen in the
            // single-threaded runtime). Prepend by building a new buffer
            // with remainder first.
            let mut new_buf = BytesMut::with_capacity(data.len() + acc.buf.len());
            new_buf.extend_from_slice(data);
            new_buf.extend_from_slice(&acc.buf);
            acc.buf = new_buf;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_sizes_append_growth_once() {
        let mut acc = RecvAccumulator::new(64);
        acc.append(b"header");
        // Announce a 1MB remainder: the next growth allocates it all.
        acc.reserve(1024 * 1024);
        acc.append(&[0u8; 128]); // forces growth past the initial 64
        let cap_after_first_growth = acc.capacity();
        assert!(
            cap_after_first_growth >= 6 + 1024 * 1024,
            "growth ignored reserve target: cap {cap_after_first_growth}"
        );
        // Streaming the remainder never reallocates again.
        for _ in 0..15 {
            acc.append(&[0u8; 64 * 1024]);
        }
        assert_eq!(acc.capacity(), cap_after_first_growth);
    }

    #[test]
    fn reserve_survives_freeze_merge_cycle() {
        // The with_bytes cycle: take_frozen → NeedAtLeast → put_back →
        // kernel appends → next take_frozen merges. The merge must land in
        // a full-size buffer, not regrow per chunk.
        let mut table = AccumulatorTable::new(1, 64);
        table.append(0, b"$16\r\npartial");
        let frozen = table.take_frozen(0);
        table.put_back(0, frozen);
        table.reserve(0, 1024 * 1024);
        table.append(0, &[0u8; 1024]); // new data while remainder held
        let _ = table.data(0); // forces the merge
        let cap = table.capacity(0);
        assert!(
            cap >= 1024 * 1024,
            "merge ignored reserve target: cap {cap}"
        );
    }

    #[test]
    fn reserve_clamps_to_max_size() {
        let mut acc = RecvAccumulator::new_with_max(16, 1024);
        acc.append(b"x");
        acc.reserve(usize::MAX);
        acc.append(&[0u8; 64]);
        assert!(
            acc.capacity() <= 4096,
            "reserve target must clamp at max_size, cap {}",
            acc.capacity()
        );
    }

    #[test]
    fn reserve_target_clears_when_drained() {
        let mut acc = RecvAccumulator::new(16);
        acc.append(b"abc");
        acc.reserve(1024 * 1024);
        acc.consume(3);
        assert_eq!(acc.reserve_target, 0, "drain must clear the target");
        // Post-drain appends grow normally, not to the stale megabyte.
        acc.append(&[0u8; 64]);
        assert!(acc.capacity() < 1024 * 1024);
    }

    #[test]
    fn append_and_consume() {
        let mut acc = RecvAccumulator::new(64);
        assert!(acc.append(b"hello "));
        assert!(acc.append(b"world"));
        assert_eq!(acc.data(), b"hello world");
        acc.consume(6);
        assert_eq!(acc.data(), b"world");
        acc.consume(5);
        assert_eq!(acc.data(), b"");
    }

    #[test]
    fn grow_on_overflow() {
        let mut acc = RecvAccumulator::new(4);
        assert!(acc.append(b"abcdef")); // exceeds initial capacity but not max
        assert_eq!(acc.data(), b"abcdef");
    }

    #[test]
    fn reset_clears() {
        let mut acc = RecvAccumulator::new(16);
        assert!(acc.append(b"data"));
        acc.reset();
        assert_eq!(acc.data(), b"");
    }

    #[test]
    fn append_past_max_returns_false_and_preserves_contents() {
        let mut acc = RecvAccumulator::new_with_max(8, 8);
        assert!(acc.append(b"abcdef"));
        // Would push to 9 bytes, exceeding max=8.
        assert!(!acc.append(b"xyz"));
        // Existing contents intact.
        assert_eq!(acc.data(), b"abcdef");
    }

    #[test]
    fn table_operations() {
        let mut table = AccumulatorTable::new(4, 64);
        assert!(table.append(2, b"hello"));
        assert_eq!(table.data(2), b"hello");
        table.consume(2, 3);
        assert_eq!(table.data(2), b"lo");
        table.reset(2);
        assert_eq!(table.data(2), b"");
    }

    #[test]
    fn table_append_past_max_returns_false() {
        let mut table = AccumulatorTable::new_with_max(1, 4, 4);
        assert!(table.append(0, b"abcd"));
        assert!(!table.append(0, b"e"));
        assert_eq!(table.data(0), b"abcd");
    }

    #[test]
    fn take_frozen_and_prepend() {
        let mut table = AccumulatorTable::new(2, 64);
        assert!(table.append(0, b"$5\r\nhello\r\n$3\r\nbar\r\n"));

        let frozen = table.take_frozen(0);
        assert_eq!(&frozen[..], b"$5\r\nhello\r\n$3\r\nbar\r\n");
        // Accumulator is now empty.
        assert_eq!(table.data(0), b"");

        // Put back the unconsumed remainder.
        table.prepend(0, &frozen[11..]);
        assert_eq!(table.data(0), b"$3\r\nbar\r\n");
    }

    /// After `take_frozen` + `prepend(remainder)`, the accumulator must NOT
    /// have allocated a new backing buffer — the tail capacity from the
    /// split must still be in place (`capacity() > 0` immediately after
    /// `take_frozen`, and the `prepend` does not reallocate).
    #[test]
    fn take_frozen_preserves_tail_capacity() {
        let mut table = AccumulatorTable::new(1, 256);
        // Fill with data that will have a remainder after the first parse.
        assert!(table.append(0, b"$5\r\nhello\r\n$3\r\nbar\r\n"));

        let frozen = table.take_frozen(0);
        // Capacity must be non-zero — the tail allocation is retained.
        assert!(
            table.capacity(0) > 0,
            "take_frozen must retain tail capacity, got 0"
        );

        // Prepend the unconsumed remainder — must not reallocate.
        let cap_after_take = table.capacity(0);
        let remainder = &frozen[11..];
        table.prepend(0, remainder);
        assert_eq!(
            table.capacity(0),
            cap_after_take,
            "prepend(remainder) must reuse the retained capacity, not reallocate"
        );
        assert_eq!(table.data(0), b"$3\r\nbar\r\n");

        // Multiple cycles must each preserve capacity (no per-cycle realloc).
        for _ in 0..3 {
            assert!(table.append(0, b" extra"));
            let f = table.take_frozen(0);
            assert!(
                table.capacity(0) > 0,
                "cycle: take_frozen must retain capacity"
            );
            let cap_before = table.capacity(0);
            table.prepend(0, &f[..3]);
            assert_eq!(
                table.capacity(0),
                cap_before,
                "cycle: prepend must reuse retained capacity"
            );
            drop(f);
        }
    }

    /// Streaming NeedMore cycles (take → put_back whole → new data arrives →
    /// merge) must recover the remainder's allocation in place and append
    /// only the new bytes — NOT re-copy the whole remainder each cycle.
    /// This is the O(N·K) large-response path: the accumulator's empty `buf`
    /// keeping a `split_to` handle to the remainder's allocation used to
    /// defeat `try_into_mut` on every merge.
    #[test]
    fn streaming_merge_recovers_allocation_in_place() {
        let mut table = AccumulatorTable::new(1, 1024);
        assert!(table.append(0, b"0123456789"));
        let frozen = table.take_frozen(0);
        let alloc_ptr = frozen.as_ptr();
        // Parser saw an incomplete message: put the whole view back.
        table.put_back(0, frozen);
        // Next chunk arrives; the merge must take the in-place path.
        assert!(table.append(0, b"ABCDEF"));
        let merged = table.take_frozen(0);
        assert_eq!(&merged[..], b"0123456789ABCDEF");
        assert_eq!(
            merged.as_ptr(),
            alloc_ptr,
            "merge must recover the original allocation (try_into_mut), not copy"
        );
        // And again — every cycle of the stream, not just the first.
        table.put_back(0, merged);
        assert!(table.append(0, b"GHIJ"));
        let merged2 = table.take_frozen(0);
        assert_eq!(&merged2[..], b"0123456789ABCDEFGHIJ");
        assert_eq!(
            merged2.as_ptr(),
            alloc_ptr,
            "second cycle must also be in place"
        );
    }

    #[test]
    fn is_empty_is_non_merging() {
        let mut table = AccumulatorTable::new(1, 64);
        assert!(table.is_empty(0));
        assert!(table.append(0, b"abcd"));
        assert!(!table.is_empty(0));
        let frozen = table.take_frozen(0);
        assert!(table.is_empty(0));
        table.put_back(0, frozen);
        assert!(!table.is_empty(0));
        // New data while a remainder is held: emptiness must be answerable
        // without merging (can't observe the non-merge directly here, but
        // the streaming test above fails if a merge sneaks in and copies).
        assert!(table.append(0, b"ef"));
        assert!(!table.is_empty(0));
        assert_eq!(table.data(0), b"abcdef");
    }

    #[test]
    fn put_back_then_take_is_same_bytes_no_copy() {
        let mut table = AccumulatorTable::new(1, 64);
        assert!(table.append(0, b"aaaabbbbcccc"));
        let frozen = table.take_frozen(0);
        // Consume one "response", put the rest back.
        let rem = frozen.slice(4..);
        let rem_ptr = rem.as_ptr();
        table.put_back(0, rem);
        // Re-take must hand back the same allocation (O(1), no copy).
        let again = table.take_frozen(0);
        assert_eq!(&again[..], b"bbbbcccc");
        assert_eq!(again.as_ptr(), rem_ptr, "re-take must not copy");
    }

    #[test]
    fn append_while_frozen_held_merges_in_order() {
        let mut table = AccumulatorTable::new(1, 64);
        assert!(table.append(0, b"old-"));
        let frozen = table.take_frozen(0);
        table.put_back(0, frozen);
        // New data arrives while the remainder is held.
        assert!(table.append(0, b"new"));
        assert_eq!(table.data(0), b"old-new");
        let merged = table.take_frozen(0);
        assert_eq!(&merged[..], b"old-new");
    }

    #[test]
    fn cap_accounts_for_frozen_remainder() {
        let mut table = AccumulatorTable::new_with_max(1, 8, 8);
        assert!(table.append(0, b"abcdef"));
        let frozen = table.take_frozen(0);
        table.put_back(0, frozen);
        // 6 held + 3 new = 9 > 8: must be rejected.
        assert!(!table.append(0, b"xyz"));
        // 6 + 2 = 8: fits.
        assert!(table.append(0, b"gh"));
        assert_eq!(table.data(0), b"abcdefgh");
    }

    #[test]
    fn consume_from_frozen_remainder() {
        let mut table = AccumulatorTable::new(1, 64);
        assert!(table.append(0, b"0123456789"));
        let frozen = table.take_frozen(0);
        table.put_back(0, frozen);
        // Borrowed-slice consumer path (with_data) over a held remainder.
        assert_eq!(table.data(0), b"0123456789");
        table.consume(0, 4);
        assert_eq!(table.data(0), b"456789");
        table.consume(0, 6);
        assert_eq!(table.data(0), b"");
        // Accumulator is fully reusable afterwards.
        assert!(table.append(0, b"next"));
        assert_eq!(table.data(0), b"next");
    }

    #[test]
    fn prepend_orders_ahead_of_frozen_remainder() {
        let mut table = AccumulatorTable::new(1, 64);
        assert!(table.append(0, b"MID"));
        let frozen = table.take_frozen(0);
        table.put_back(0, frozen);
        assert!(table.append(0, b"TAIL"));
        // Oldest bytes prepended in front of remainder + new data.
        table.prepend(0, b"HEAD");
        assert_eq!(table.data(0), b"HEADMIDTAIL");
    }

    #[test]
    fn reset_clears_frozen_remainder() {
        let mut table = AccumulatorTable::new(1, 64);
        assert!(table.append(0, b"data"));
        let frozen = table.take_frozen(0);
        table.put_back(0, frozen);
        table.reset(0);
        assert_eq!(table.data(0), b"");
        assert!(table.append(0, b"fresh"));
        assert_eq!(table.data(0), b"fresh");
    }

    #[test]
    fn take_frozen_empty() {
        let mut table = AccumulatorTable::new(1, 16);
        let frozen = table.take_frozen(0);
        assert!(frozen.is_empty());
    }

    #[test]
    fn prepend_to_empty() {
        let mut table = AccumulatorTable::new(1, 16);
        table.prepend(0, b"leftover");
        assert_eq!(table.data(0), b"leftover");
    }

    #[test]
    fn prepend_empty_is_noop() {
        let mut table = AccumulatorTable::new(1, 16);
        assert!(table.append(0, b"existing"));
        table.prepend(0, b"");
        assert_eq!(table.data(0), b"existing");
    }
}
