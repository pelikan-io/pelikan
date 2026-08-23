use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project_lite::pin_project! {
    /// Tracks whether a sub-future has completed, storing its output.
    #[project = MaybeDoneProj]
    pub enum MaybeDone<F: Future> {
        /// The future is still pending.
        Pending { #[pin] future: F },
        /// The future completed; output is stored here.
        Done { output: F::Output },
        /// The output was already taken.
        Gone,
    }
}

impl<F: Future> MaybeDone<F> {
    fn take_output(self: Pin<&mut Self>) -> Option<F::Output> {
        // Safety: we only move out of Done, which is !Unpin-safe since
        // the future field is never touched in Done/Gone states.
        unsafe {
            let this = self.get_unchecked_mut();
            match this {
                MaybeDone::Done { .. } => {
                    let MaybeDone::Done { output } = std::mem::replace(this, MaybeDone::Gone)
                    else {
                        unreachable!()
                    };
                    Some(output)
                }
                _ => None,
            }
        }
    }
}

impl<F: Future> Future for MaybeDone<F> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Poll the future if pending, collecting the output.
        let output = match self.as_mut().project() {
            MaybeDoneProj::Pending { future } => match future.poll(cx) {
                Poll::Ready(o) => o,
                Poll::Pending => return Poll::Pending,
            },
            MaybeDoneProj::Done { .. } | MaybeDoneProj::Gone { .. } => return Poll::Ready(()),
        };
        // The projected Pin<&mut F> from the match above is now dropped,
        // so we can safely overwrite the enum variant.
        // Safety: we are replacing Pending{future} with Done{output},
        // and the future is consumed by poll returning Ready.
        unsafe {
            let slot = Pin::into_inner_unchecked(self);
            *slot = MaybeDone::Done { output };
        }
        Poll::Ready(())
    }
}

pin_project_lite::pin_project! {
    /// Future that polls two sub-futures and returns both outputs when complete.
    pub struct Join<A: Future, B: Future> {
        #[pin] a: MaybeDone<A>,
        #[pin] b: MaybeDone<B>,
    }
}

impl<A: Future, B: Future> Future for Join<A, B> {
    type Output = (A::Output, B::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let a_done = this.a.as_mut().poll(cx).is_ready();
        let b_done = this.b.as_mut().poll(cx).is_ready();
        if a_done && b_done {
            let a_out = this.a.take_output().unwrap();
            let b_out = this.b.take_output().unwrap();
            Poll::Ready((a_out, b_out))
        } else {
            Poll::Pending
        }
    }
}

/// Poll two futures concurrently, returning both outputs when they complete.
///
/// Unlike [`select()`](crate::select) which returns whichever finishes first,
/// `join` waits for **both** futures. Useful for fan-out patterns like parallel
/// backend requests.
///
/// The futures are polled in order (a, then b) on each iteration.
pub fn join<A: Future, B: Future>(a: A, b: B) -> Join<A, B> {
    Join {
        a: MaybeDone::Pending { future: a },
        b: MaybeDone::Pending { future: b },
    }
}

pin_project_lite::pin_project! {
    /// Future that polls three sub-futures and returns all outputs when complete.
    pub struct Join3<A: Future, B: Future, C: Future> {
        #[pin] a: MaybeDone<A>,
        #[pin] b: MaybeDone<B>,
        #[pin] c: MaybeDone<C>,
    }
}

impl<A: Future, B: Future, C: Future> Future for Join3<A, B, C> {
    type Output = (A::Output, B::Output, C::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let a_done = this.a.as_mut().poll(cx).is_ready();
        let b_done = this.b.as_mut().poll(cx).is_ready();
        let c_done = this.c.as_mut().poll(cx).is_ready();
        if a_done && b_done && c_done {
            let a_out = this.a.take_output().unwrap();
            let b_out = this.b.take_output().unwrap();
            let c_out = this.c.take_output().unwrap();
            Poll::Ready((a_out, b_out, c_out))
        } else {
            Poll::Pending
        }
    }
}

/// Poll three futures concurrently, returning all outputs when they complete.
///
/// Waits for **all three** futures. Useful for fan-out patterns with three
/// concurrent operations (e.g., three parallel backend requests).
pub fn join3<A: Future, B: Future, C: Future>(a: A, b: B, c: C) -> Join3<A, B, C> {
    Join3 {
        a: MaybeDone::Pending { future: a },
        b: MaybeDone::Pending { future: b },
        c: MaybeDone::Pending { future: c },
    }
}
