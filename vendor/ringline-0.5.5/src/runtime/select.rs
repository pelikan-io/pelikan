use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Result of [`select()`] — indicates which branch completed first.
pub enum Either<A, B> {
    /// The first (left) future completed.
    Left(A),
    /// The second (right) future completed.
    Right(B),
}

impl<T> Either<T, T> {
    /// Extract the value regardless of which branch completed.
    pub fn into_inner(self) -> T {
        match self {
            Either::Left(v) | Either::Right(v) => v,
        }
    }
}

pin_project_lite::pin_project! {
    /// Future that polls two sub-futures and returns whichever completes first.
    /// **Biased**: always polls `a` before `b`.
    pub struct Select<A, B> {
        #[pin] a: A,
        #[pin] b: B,
    }
}

impl<A: Future, B: Future> Future for Select<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(val) = this.a.poll(cx) {
            return Poll::Ready(Either::Left(val));
        }
        if let Poll::Ready(val) = this.b.poll(cx) {
            return Poll::Ready(Either::Right(val));
        }
        Poll::Pending
    }
}

/// Poll two futures concurrently, returning whichever completes first.
///
/// **Biased**: always polls `a` before `b`. When both are ready simultaneously,
/// `a` wins. The losing future is dropped.
///
/// Safe with ringline I/O futures — data buffered for a dropped `WithDataFuture`
/// remains in the accumulator and is consumed on the next `with_data()` call.
/// Dropped `SleepFuture`s correctly cancel their io_uring timeout SQE.
pub fn select<A: Future, B: Future>(a: A, b: B) -> Select<A, B> {
    Select { a, b }
}

/// Result of [`select3()`] — indicates which of three branches completed first.
pub enum Either3<A, B, C> {
    /// The first future completed.
    First(A),
    /// The second future completed.
    Second(B),
    /// The third future completed.
    Third(C),
}

impl<T> Either3<T, T, T> {
    /// Extract the value regardless of which branch completed.
    pub fn into_inner(self) -> T {
        match self {
            Either3::First(v) | Either3::Second(v) | Either3::Third(v) => v,
        }
    }
}

pin_project_lite::pin_project! {
    /// Future that polls three sub-futures and returns whichever completes first.
    /// **Biased**: polls `a`, then `b`, then `c`.
    pub struct Select3<A, B, C> {
        #[pin] a: A,
        #[pin] b: B,
        #[pin] c: C,
    }
}

impl<A: Future, B: Future, C: Future> Future for Select3<A, B, C> {
    type Output = Either3<A::Output, B::Output, C::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(val) = this.a.poll(cx) {
            return Poll::Ready(Either3::First(val));
        }
        if let Poll::Ready(val) = this.b.poll(cx) {
            return Poll::Ready(Either3::Second(val));
        }
        if let Poll::Ready(val) = this.c.poll(cx) {
            return Poll::Ready(Either3::Third(val));
        }
        Poll::Pending
    }
}

/// Poll three futures concurrently, returning whichever completes first.
///
/// **Biased**: polls `a`, then `b`, then `c`. When multiple are ready
/// simultaneously, the earliest in order wins. The losing futures are dropped.
///
/// Useful for client + backend + timeout patterns.
pub fn select3<A: Future, B: Future, C: Future>(a: A, b: B, c: C) -> Select3<A, B, C> {
    Select3 { a, b, c }
}
