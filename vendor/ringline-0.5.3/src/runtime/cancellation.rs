//! Structured cancellation via [`CancellationToken`].
//!
//! Tokens can be shared across tasks on the same worker thread (they are
//! `Clone` but `!Send`). A parent token can create child tokens that are
//! automatically cancelled when the parent is cancelled.
//!
//! # Example
//!
//! ```rust,no_run
//! use ringline::CancellationToken;
//!
//! # async fn example(conn: ringline::ConnCtx) {
//! let token = CancellationToken::new();
//! let child = token.child_token();
//!
//! ringline::spawn(async move {
//!     // This resolves when the token is cancelled.
//!     child.cancelled().await;
//! }).unwrap();
//!
//! // Cancel — wakes all tasks awaiting this token or any child.
//! token.cancel();
//! # }
//! ```

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};

use super::CURRENT_TASK_ID;
use super::io::try_with_state;

/// Shared state for a cancellation token.
struct State {
    cancelled: bool,
    /// Task IDs waiting for cancellation. Supports multiple waiters
    /// (e.g. several tasks each calling `cancelled()` on clones of the
    /// same token).
    waiters: Vec<u32>,
    /// Child tokens to cancel when this token is cancelled. Held weakly:
    /// a per-request child token must not be kept alive (and accumulated)
    /// by a process-lifetime parent after the request ends.
    children: Vec<Weak<RefCell<State>>>,
}

/// A token for cooperative cancellation of async tasks.
///
/// Clone the token to share it across tasks on the same worker.
/// Call [`cancel()`](Self::cancel) to signal cancellation and wake all
/// tasks awaiting [`cancelled()`](Self::cancelled).
///
/// Tokens form a tree: [`child_token()`](Self::child_token) creates a
/// child that is automatically cancelled when the parent is.
pub struct CancellationToken {
    state: Rc<RefCell<State>>,
}

impl CancellationToken {
    /// Create a new cancellation token.
    pub fn new() -> Self {
        CancellationToken {
            state: Rc::new(RefCell::new(State {
                cancelled: false,
                waiters: Vec::new(),
                children: Vec::new(),
            })),
        }
    }

    /// Cancel the token and all child tokens.
    ///
    /// All tasks awaiting [`cancelled()`](Self::cancelled) on this token
    /// or any descendant are woken.
    pub fn cancel(&self) {
        cancel_state(&self.state);
    }

    /// Returns `true` if the token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.state.borrow().cancelled
    }

    /// Returns a future that completes when the token is cancelled.
    ///
    /// If the token is already cancelled, the future resolves immediately.
    pub fn cancelled(&self) -> CancelledFuture {
        CancelledFuture {
            state: Rc::clone(&self.state),
            registered: None,
        }
    }

    /// Create a child token that is cancelled when this token is cancelled.
    ///
    /// The child can also be cancelled independently without affecting the
    /// parent.
    pub fn child_token(&self) -> CancellationToken {
        let child = CancellationToken::new();
        let mut s = self.state.borrow_mut();
        if s.cancelled {
            // Parent already cancelled — cancel the child immediately.
            drop(s);
            child.cancel();
        } else {
            // Opportunistically prune children that have been dropped so a
            // long-lived parent's list doesn't grow without bound.
            s.children.retain(|c| c.strong_count() > 0);
            s.children.push(Rc::downgrade(&child.state));
        }
        child
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CancellationToken {
    fn clone(&self) -> Self {
        CancellationToken {
            state: Rc::clone(&self.state),
        }
    }
}

/// Recursively cancel a state and all its children, waking all waiters.
fn cancel_state(state: &Rc<RefCell<State>>) {
    let (waiters, children) = {
        let mut s = state.borrow_mut();
        if s.cancelled {
            return;
        }
        s.cancelled = true;
        let waiters = std::mem::take(&mut s.waiters);
        let children = std::mem::take(&mut s.children);
        (waiters, children)
    };

    // Wake all waiters. Drop the borrow before calling try_with_state.
    for waiter_id in waiters {
        try_with_state(|_driver, executor| {
            executor.wake_task(waiter_id);
        });
    }

    // Recursively cancel children that are still alive.
    for child in children {
        if let Some(child) = child.upgrade() {
            cancel_state(&child);
        }
    }
}

/// Future returned by [`CancellationToken::cancelled`].
///
/// Resolves when the associated token is cancelled.
pub struct CancelledFuture {
    state: Rc<RefCell<State>>,
    /// The task id this future registered as a waiter, if any. Pushed at
    /// most once (avoids an O(n) `Vec::contains` scan on every re-poll)
    /// and removed again on drop.
    registered: Option<u32>,
}

impl Future for CancelledFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        // Check cancellation first.
        {
            let s = self.state.borrow();
            if s.cancelled {
                return Poll::Ready(());
            }
        }
        // Register as waiter exactly once.
        if self.registered.is_none() {
            let task_id = CURRENT_TASK_ID.with(|c| c.get());
            self.state.borrow_mut().waiters.push(task_id);
            self.registered = Some(task_id);
        }
        Poll::Pending
    }
}

impl Drop for CancelledFuture {
    fn drop(&mut self) {
        // The canonical select!-loop pattern creates a fresh CancelledFuture
        // per iteration. Deregister on drop so a long-lived token's waiter
        // list doesn't grow by one stale task id per iteration (each of
        // which would spuriously wake whatever task reuses that id when the
        // token finally cancels).
        let Some(task_id) = self.registered else {
            return;
        };
        let mut s = self.state.borrow_mut();
        if s.cancelled {
            // cancel_state already took the waiter list.
            return;
        }
        if let Some(pos) = s.waiters.iter().position(|&w| w == task_id) {
            s.waiters.swap_remove(pos);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_token_is_not_cancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn cancel_sets_flag() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancel_is_idempotent() {
        let token = CancellationToken::new();
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn clone_shares_state() {
        let token = CancellationToken::new();
        let clone = token.clone();
        token.cancel();
        assert!(clone.is_cancelled());
    }

    #[test]
    fn child_cancelled_with_parent() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        assert!(!child.is_cancelled());
        parent.cancel();
        assert!(child.is_cancelled());
    }

    #[test]
    fn child_cancelled_independently() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        child.cancel();
        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());
    }

    #[test]
    fn child_of_cancelled_parent_is_cancelled() {
        let parent = CancellationToken::new();
        parent.cancel();
        let child = parent.child_token();
        assert!(child.is_cancelled());
    }

    #[test]
    fn grandchild_cancelled_with_grandparent() {
        let gp = CancellationToken::new();
        let parent = gp.child_token();
        let child = parent.child_token();
        gp.cancel();
        assert!(parent.is_cancelled());
        assert!(child.is_cancelled());
    }

    #[test]
    fn default_is_new() {
        let token = CancellationToken::default();
        assert!(!token.is_cancelled());
    }

    /// Polling a CancelledFuture N times before cancel must add the waiter
    /// exactly once (no O(n) vec growth per poll).
    #[test]
    fn repeated_polls_do_not_grow_waiters() {
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

        // Minimal no-op waker for driving polls outside the executor.
        unsafe fn noop_clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &NOOP_VTABLE)
        }
        unsafe fn noop(_: *const ()) {}
        static NOOP_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_VTABLE)) };
        let mut cx = Context::from_waker(&waker);

        let token = CancellationToken::new();
        let mut fut = std::pin::pin!(token.cancelled());

        // Poll many times — should stay Pending, waiters vec stays length 1.
        for _ in 0..10 {
            assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
            assert_eq!(
                token.state.borrow().waiters.len(),
                1,
                "waiters vec must not grow on repeated polls"
            );
        }

        // After cancel the future must resolve.
        token.cancel();
        assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Ready(())));
    }

    /// Multiple futures awaiting the same token must all be woken.
    #[test]
    fn multiple_futures_all_registered() {
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

        unsafe fn noop_clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &NOOP_VTABLE)
        }
        unsafe fn noop(_: *const ()) {}
        static NOOP_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_VTABLE)) };
        let mut cx = Context::from_waker(&waker);

        let token = CancellationToken::new();

        // Three separate futures for three separate clones of the same token.
        let t1 = token.clone();
        let t2 = token.clone();
        let t3 = token.clone();
        let mut f1 = std::pin::pin!(t1.cancelled());
        let mut f2 = std::pin::pin!(t2.cancelled());
        let mut f3 = std::pin::pin!(t3.cancelled());

        // Poll each future once to register them.
        assert!(matches!(f1.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(f2.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(f3.as_mut().poll(&mut cx), Poll::Pending));

        // All three task IDs must be in the waiters list (3 entries).
        assert_eq!(token.state.borrow().waiters.len(), 3);

        // Cancel — wakers drain the list (cancel_state takes the vec).
        token.cancel();

        // Each future should now be Ready.
        assert!(matches!(f1.as_mut().poll(&mut cx), Poll::Ready(())));
        assert!(matches!(f2.as_mut().poll(&mut cx), Poll::Ready(())));
        assert!(matches!(f3.as_mut().poll(&mut cx), Poll::Ready(())));
    }
}
