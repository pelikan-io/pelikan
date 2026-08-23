//! Async channels for intra-worker communication.
//!
//! Both [`oneshot`] and [`mpsc`] channels are designed for ringline's
//! single-threaded, thread-per-core executor. They use `Rc<RefCell<...>>`
//! internally and are `!Send`.
//!
//! # Executor requirement
//!
//! Sending and receiving must happen within the ringline executor (connection
//! tasks or standalone tasks). The wakeup mechanism uses
//! [`Executor::wake_task`] via the thread-local driver state.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use super::CURRENT_TASK_ID;
use super::io::try_with_state;

/// Wake a task if a waiter is registered. Handles the case where we're
/// called outside the executor (e.g. in a unit test or during drop after
/// shutdown) by silently doing nothing.
fn wake_waiter(waiter: Option<u32>) {
    if let Some(id) = waiter {
        try_with_state(|_driver, executor| {
            executor.wake_task(id);
        });
    }
}

// ── Error types ─────────────────────────────────────────────────────

/// Error returned by [`oneshot::Receiver`] when the sender is dropped
/// without sending a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("channel closed")
    }
}

impl std::error::Error for RecvError {}

/// Error returned by [`mpsc::Sender::send`] and [`mpsc::Sender::try_send`]
/// when the receiver has been dropped. Contains the value that could not be
/// sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("channel closed")
    }
}

impl<T: fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned by [`mpsc::Receiver::try_recv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is empty but senders are still alive.
    Empty,
    /// All senders have been dropped and the channel is empty.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("channel empty"),
            TryRecvError::Disconnected => f.write_str("channel disconnected"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// Error returned by [`mpsc::Sender::try_send`] when the channel is full.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is at capacity. Contains the unsent value.
    Full(T),
    /// The receiver has been dropped. Contains the unsent value.
    Disconnected(T),
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("channel full"),
            TrySendError::Disconnected(_) => f.write_str("channel disconnected"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for TrySendError<T> {}

// ═══════════════════════════════════════════════════════════════════════
// oneshot
// ═══════════════════════════════════════════════════════════════════════

/// A single-use channel for sending exactly one value between tasks.
pub mod oneshot {
    use super::*;

    struct State<T> {
        value: Option<T>,
        recv_waiter: Option<u32>,
        closed: bool,
    }

    /// Create a new oneshot channel, returning the sender and receiver halves.
    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let state = Rc::new(RefCell::new(State {
            value: None,
            recv_waiter: None,
            closed: false,
        }));
        (
            Sender {
                state: Rc::clone(&state),
                sent: false,
            },
            Receiver { state },
        )
    }

    /// Sending half of a [`oneshot`] channel.
    ///
    /// Consumed on [`send`](Sender::send). If dropped without sending,
    /// the receiver sees [`RecvError`].
    pub struct Sender<T> {
        state: Rc<RefCell<State<T>>>,
        sent: bool,
    }

    impl<T> Sender<T> {
        /// Send a value to the receiver.
        ///
        /// Consumes the sender. Returns `Err(value)` if the receiver was
        /// already dropped.
        pub fn send(mut self, value: T) -> Result<(), T> {
            // Check if receiver is still alive.
            if Rc::strong_count(&self.state) == 1 {
                return Err(value);
            }
            let mut s = self.state.borrow_mut();
            s.value = Some(value);
            self.sent = true;
            let waiter = s.recv_waiter.take();
            drop(s);
            wake_waiter(waiter);
            Ok(())
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            if !self.sent {
                let mut s = self.state.borrow_mut();
                s.closed = true;
                let waiter = s.recv_waiter.take();
                drop(s);
                wake_waiter(waiter);
            }
        }
    }

    /// Receiving half of a [`oneshot`] channel.
    ///
    /// Implements [`Future`] — await it to get the value.
    pub struct Receiver<T> {
        state: Rc<RefCell<State<T>>>,
    }

    impl<T> Receiver<T> {
        /// Non-blocking attempt to receive.
        ///
        /// Returns `Ok(value)` if available, `Err(TryRecvError::Empty)` if
        /// not yet sent, or `Err(TryRecvError::Disconnected)` if the sender
        /// was dropped.
        pub fn try_recv(&self) -> Result<T, TryRecvError> {
            let mut s = self.state.borrow_mut();
            if let Some(value) = s.value.take() {
                return Ok(value);
            }
            if s.closed {
                return Err(TryRecvError::Disconnected);
            }
            Err(TryRecvError::Empty)
        }
    }

    impl<T> Future for Receiver<T> {
        type Output = Result<T, RecvError>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut s = self.state.borrow_mut();
            if let Some(value) = s.value.take() {
                return Poll::Ready(Ok(value));
            }
            if s.closed {
                return Poll::Ready(Err(RecvError));
            }
            s.recv_waiter = Some(CURRENT_TASK_ID.with(|c| c.get()));
            Poll::Pending
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// mpsc
// ═══════════════════════════════════════════════════════════════════════

/// A bounded multi-producer, single-consumer channel.
pub mod mpsc {
    use super::*;

    struct State<T> {
        queue: VecDeque<T>,
        capacity: usize,
        recv_waiter: Option<u32>,
        /// Tasks blocked in `send().await` on a full queue, in arrival
        /// order. A single slot here loses wakes with multiple producers:
        /// the second blocked sender overwrote the first, which then hung
        /// forever (this executor never re-polls spuriously).
        send_waiters: VecDeque<u32>,
        sender_count: usize,
    }

    /// Create a bounded mpsc channel with the given capacity.
    ///
    /// The channel can buffer up to `capacity` messages. Sends to a full
    /// channel return [`TrySendError::Full`].
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
        assert!(capacity > 0, "mpsc channel capacity must be > 0");
        let state = Rc::new(RefCell::new(State {
            queue: VecDeque::with_capacity(capacity),
            capacity,
            recv_waiter: None,
            send_waiters: VecDeque::new(),
            sender_count: 1,
        }));
        (
            Sender {
                state: Rc::clone(&state),
            },
            Receiver { state },
        )
    }

    /// Sending half of an [`mpsc`] channel.
    ///
    /// Can be cloned to create multiple producers.
    pub struct Sender<T> {
        state: Rc<RefCell<State<T>>>,
    }

    impl<T> Sender<T> {
        /// Try to send a value without blocking.
        ///
        /// Returns `Ok(())` if the value was queued. Returns
        /// [`TrySendError::Full`] if the channel is at capacity, or
        /// [`TrySendError::Disconnected`] if the receiver was dropped.
        pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
            let mut s = self.state.borrow_mut();
            // Receiver dropped? sender_count is tracked separately, so check
            // if the Receiver's Rc is still alive: total strong_count minus
            // sender_count should be 1 (the receiver's Rc).
            if Rc::strong_count(&self.state) <= s.sender_count {
                return Err(TrySendError::Disconnected(value));
            }
            if s.queue.len() >= s.capacity {
                return Err(TrySendError::Full(value));
            }
            s.queue.push_back(value);
            let waiter = s.recv_waiter.take();
            drop(s);
            wake_waiter(waiter);
            Ok(())
        }

        /// Send a value, returning a future that resolves when the value is
        /// queued.
        ///
        /// If the channel has capacity, the future resolves immediately.
        /// If the channel is full, the future parks until the receiver drains
        /// a slot.
        pub fn send(&self, value: T) -> SendFuture<'_, T> {
            SendFuture {
                state: &self.state,
                value: Some(value),
                registered: None,
            }
        }
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.state.borrow_mut().sender_count += 1;
            Sender {
                state: Rc::clone(&self.state),
            }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let mut s = self.state.borrow_mut();
            s.sender_count -= 1;
            if s.sender_count == 0 {
                let waiter = s.recv_waiter.take();
                drop(s);
                wake_waiter(waiter);
            }
        }
    }

    /// Future returned by [`Sender::send`].
    pub struct SendFuture<'a, T> {
        state: &'a Rc<RefCell<State<T>>>,
        value: Option<T>,
        /// Task id this future registered in `send_waiters`, if any.
        registered: Option<u32>,
    }

    // SAFETY: SendFuture has no self-referential data; it's safe to unpin.
    impl<T> Unpin for SendFuture<'_, T> {}

    impl<T> Future for SendFuture<'_, T> {
        type Output = Result<(), SendError<T>>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let mut s = this.state.borrow_mut();
            // Receiver dropped?
            if Rc::strong_count(this.state) <= s.sender_count {
                let value = this.value.take().unwrap();
                return Poll::Ready(Err(SendError(value)));
            }
            if s.queue.len() < s.capacity {
                let value = this.value.take().unwrap();
                s.queue.push_back(value);
                // This future's registration (if any) was consumed by the
                // wake that got us here.
                this.registered = None;
                let waiter = s.recv_waiter.take();
                drop(s);
                wake_waiter(waiter);
                return Poll::Ready(Ok(()));
            }
            // Full — park in FIFO order behind other blocked senders.
            let id = CURRENT_TASK_ID.with(|c| c.get());
            if this.registered != Some(id) {
                s.send_waiters.push_back(id);
                this.registered = Some(id);
            }
            Poll::Pending
        }
    }

    impl<T> Drop for SendFuture<'_, T> {
        fn drop(&mut self) {
            let Some(id) = self.registered else { return };
            let mut s = self.state.borrow_mut();
            if let Some(pos) = s.send_waiters.iter().position(|&w| w == id) {
                // Still registered — the wake was not consumed; just leave.
                s.send_waiters.remove(pos);
            } else {
                // Our registration was already popped: a capacity wake was
                // delivered to a future that will never act on it. Pass it
                // to the next blocked sender so the free slot isn't lost.
                let next = s.send_waiters.pop_front();
                drop(s);
                wake_waiter(next);
            }
        }
    }

    /// Receiving half of an [`mpsc`] channel.
    pub struct Receiver<T> {
        state: Rc<RefCell<State<T>>>,
    }

    impl<T> Receiver<T> {
        /// Non-blocking attempt to receive.
        pub fn try_recv(&self) -> Result<T, TryRecvError> {
            let mut s = self.state.borrow_mut();
            if let Some(value) = s.queue.pop_front() {
                // Wake the longest-blocked sender if there is one.
                let waiter = s.send_waiters.pop_front();
                drop(s);
                wake_waiter(waiter);
                return Ok(value);
            }
            if s.sender_count == 0 {
                return Err(TryRecvError::Disconnected);
            }
            Err(TryRecvError::Empty)
        }

        /// Receive a value, returning a future that resolves when one is
        /// available.
        ///
        /// Returns `None` when all senders have been dropped and the channel
        /// is empty.
        pub fn recv(&self) -> RecvFuture<'_, T> {
            RecvFuture { state: &self.state }
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            // Wake every blocked sender so it re-polls and observes
            // Disconnected instead of parking forever.
            let waiters = std::mem::take(&mut self.state.borrow_mut().send_waiters);
            for id in waiters {
                wake_waiter(Some(id));
            }
        }
    }

    /// Future returned by [`Receiver::recv`].
    pub struct RecvFuture<'a, T> {
        state: &'a Rc<RefCell<State<T>>>,
    }

    impl<T> Future for RecvFuture<'_, T> {
        type Output = Option<T>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut s = self.state.borrow_mut();
            if let Some(value) = s.queue.pop_front() {
                let waiter = s.send_waiters.pop_front();
                drop(s);
                wake_waiter(waiter);
                return Poll::Ready(Some(value));
            }
            if s.sender_count == 0 {
                return Poll::Ready(None);
            }
            s.recv_waiter = Some(CURRENT_TASK_ID.with(|c| c.get()));
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oneshot_send_before_recv() {
        let (tx, rx) = oneshot::channel::<i32>();
        assert!(tx.send(42).is_ok());
        assert_eq!(rx.try_recv(), Ok(42));
    }

    #[test]
    fn oneshot_sender_dropped() {
        let (tx, rx) = oneshot::channel::<i32>();
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn oneshot_receiver_dropped() {
        let (tx, _) = oneshot::channel::<i32>();
        assert_eq!(tx.send(42), Err(42));
    }

    #[test]
    fn mpsc_send_and_recv() {
        let (tx, rx) = mpsc::channel::<i32>(4);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        tx.try_send(3).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn mpsc_full_channel() {
        let (tx, _rx) = mpsc::channel::<i32>(2);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        match tx.try_send(3) {
            Err(TrySendError::Full(3)) => {}
            other => panic!("expected Full(3), got {other:?}"),
        }
    }

    #[test]
    fn mpsc_sender_clone_and_drop() {
        let (tx1, rx) = mpsc::channel::<i32>(4);
        let tx2 = tx1.clone();
        tx1.try_send(1).unwrap();
        tx2.try_send(2).unwrap();
        drop(tx1);
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        // One sender still alive.
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx2);
        // All senders gone.
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn mpsc_receiver_dropped() {
        let (tx, rx) = mpsc::channel::<i32>(4);
        drop(rx);
        match tx.try_send(1) {
            Err(TrySendError::Disconnected(1)) => {}
            other => panic!("expected Disconnected(1), got {other:?}"),
        }
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn mpsc_zero_capacity_panics() {
        let _ = mpsc::channel::<i32>(0);
    }
}
