use std::collections::VecDeque;
use std::task::{RawWaker, RawWakerVTable, Waker};

thread_local! {
    /// Thread-local queue of connection indices whose tasks are ready to poll.
    /// Wakers push to this queue; the executor drains it after each CQE batch.
    pub(crate) static READY_QUEUE: std::cell::RefCell<VecDeque<u32>> =
        const { std::cell::RefCell::new(VecDeque::new()) };
}

/// Bit flag that distinguishes standalone tasks from connection tasks
/// in the ready queue. Connection indices use bits 0..23 (max 16M),
/// so bit 31 is always free for connection tasks.
pub(crate) const STANDALONE_BIT: u32 = 1 << 31;

/// Create a [`Waker`] for the given connection index.
///
/// When woken, the waker pushes `conn_index` onto the thread-local
/// `READY_QUEUE`. Zero allocation — the conn_index is encoded as a
/// raw pointer (usize cast).
///
/// # Safety
///
/// Must only be used on the same thread where the executor runs
/// (single-threaded, thread-per-core model).
pub(crate) fn conn_waker(conn_index: u32) -> Waker {
    debug_assert!(
        conn_index & STANDALONE_BIT == 0,
        "conn_index has standalone bit set"
    );
    let data = conn_index as usize as *const ();
    // SAFETY: The vtable functions below follow the RawWaker contract.
    // The "data" is just a usize (conn_index) cast to a pointer — no
    // heap allocation, no lifetime concerns.
    unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) }
}

/// Create a [`Waker`] for a standalone task (not bound to a connection).
///
/// The `task_idx` is OR'd with `STANDALONE_BIT` so the executor can
/// distinguish it from connection wakeups.
pub(crate) fn standalone_waker(task_idx: u32) -> Waker {
    debug_assert!(
        task_idx & STANDALONE_BIT == 0,
        "task_idx already has standalone bit"
    );
    let data = (task_idx | STANDALONE_BIT) as usize as *const ();
    unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) }
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, wake_fn, wake_by_ref_fn, drop_fn);

unsafe fn clone_fn(data: *const ()) -> RawWaker {
    RawWaker::new(data, &VTABLE)
}

unsafe fn wake_fn(data: *const ()) {
    // SAFETY: wake_by_ref_fn is safe to call with data from our vtable.
    unsafe { wake_by_ref_fn(data) };
}

unsafe fn wake_by_ref_fn(data: *const ()) {
    let conn_index = data as usize as u32;
    READY_QUEUE.with(|q| {
        q.borrow_mut().push_back(conn_index);
    });
}

unsafe fn drop_fn(_data: *const ()) {
    // No resources to free — data is just a usize.
}

/// Drain the thread-local ready queue into the provided buffer.
pub(crate) fn drain_ready_queue(buf: &mut VecDeque<u32>) {
    READY_QUEUE.with(|q| {
        buf.append(&mut q.borrow_mut());
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waker_pushes_to_ready_queue() {
        // Clear any leftover state.
        READY_QUEUE.with(|q| q.borrow_mut().clear());

        let waker = conn_waker(42);
        waker.wake_by_ref();
        waker.wake_by_ref();

        let mut buf = VecDeque::new();
        drain_ready_queue(&mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], 42);
        assert_eq!(buf[1], 42);
    }

    #[test]
    fn waker_clone_works() {
        READY_QUEUE.with(|q| q.borrow_mut().clear());

        let waker = conn_waker(7);
        let cloned = waker.clone();

        waker.wake_by_ref();
        cloned.wake();

        let mut buf = VecDeque::new();
        drain_ready_queue(&mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], 7);
        assert_eq!(buf[1], 7);
    }

    #[test]
    fn drain_empty_queue() {
        READY_QUEUE.with(|q| q.borrow_mut().clear());

        let mut buf = VecDeque::new();
        drain_ready_queue(&mut buf);
        assert!(buf.is_empty());
    }
}
