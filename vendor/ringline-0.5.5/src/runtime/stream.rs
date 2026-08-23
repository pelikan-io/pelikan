//! `ConnStream` — `futures-io` trait adapter for [`ConnCtx`].
//!
//! Wraps a connection in the standard `AsyncRead + AsyncWrite + AsyncBufRead`
//! interface for ecosystem compatibility (codecs, tower, hyper, etc.).
//!
//! The callback-based [`ConnCtx::with_data`] / [`ConnCtx::with_bytes`] API
//! remains the zero-copy hot path for protocol implementations. `ConnStream`
//! is the streaming alternative when trait compatibility matters more than
//! minimising copies on the recv side.
//!
//! # Executor requirement
//!
//! All `poll_*` methods must be called from within the ringline executor
//! (connection task or standalone task). They access the thread-local driver
//! via [`with_state`] and will panic if called from an external runtime.
//!
//! # Waker note
//!
//! ringline does not use [`std::task::Waker`]. The `Context` argument to each
//! `poll_*` method is ignored. Wakeups are driven by the internal waiter-flag
//! system (`recv_waiters` / `send_waiters`), which is set inside `poll_read` /
//! `poll_write` before returning `Poll::Pending`.

use bytes::{Buf, Bytes};
use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::io::{ConnCtx, with_state};
use crate::connection::RecvMode;
use crate::runtime::CURRENT_TASK_ID;

/// Wraps a [`ConnCtx`] and implements [`AsyncRead`], [`AsyncWrite`], and
/// [`AsyncBufRead`].
///
/// Created via [`ConnStream::new`]. The inner [`ConnCtx`] is still accessible
/// via [`conn_ctx`](Self::conn_ctx) for operations that have no trait
/// equivalent (e.g. `connect`, `shutdown_write`).
///
/// # Copy profile
///
/// | Trait method | Copies (recv) | Mechanism |
/// |---|---|---|
/// | [`AsyncRead::poll_read`] | 1 | accumulator → caller buf |
/// | [`AsyncBufRead::poll_fill_buf`] | 0 | refcounted `Bytes` slice |
/// | [`AsyncWrite::poll_write`] | 1 | caller buf → send pool |
pub struct ConnStream {
    ctx: ConnCtx,
    write_closed: bool,
    /// Cached buffer detached from the accumulator for `AsyncBufRead`.
    /// Held until fully consumed, then dropped so new data flows from
    /// the accumulator on the next `poll_fill_buf`.
    fill_buf: Option<Bytes>,
}

impl ConnStream {
    /// Wrap a [`ConnCtx`] in a streaming adapter.
    pub fn new(ctx: ConnCtx) -> Self {
        ConnStream {
            ctx,
            write_closed: false,
            fill_buf: None,
        }
    }

    /// Borrow the inner [`ConnCtx`].
    ///
    /// Useful for operations without a trait equivalent, such as
    /// [`ConnCtx::connect`] or [`ConnCtx::peer_addr`].
    pub fn conn_ctx(&self) -> ConnCtx {
        self.ctx
    }

    /// Check whether the connection's recv side is closed.
    fn is_recv_closed(driver: &mut crate::backend::Driver, conn_index: u32) -> bool {
        driver
            .connections
            .get(conn_index)
            .map(|c| matches!(c.recv_mode, RecvMode::Closed))
            .unwrap_or(true)
    }

    /// Flush any pending zero-copy recv buffer to the accumulator so
    /// `ConnStream` can read it. The zero-copy path holds kernel buffers
    /// in `pending_recv_bufs` for `with_data()`/`with_bytes()` callers;
    /// `ConnStream` must flush these since it reads from the accumulator.
    fn flush_pending_recv(driver: &mut crate::backend::Driver, conn_index: u32) {
        #[cfg(has_io_uring)]
        if let Some(pending) = driver.pending_recv_bufs[conn_index as usize].take() {
            let data = unsafe { std::slice::from_raw_parts(pending.ptr, pending.len as usize) };
            driver.accumulators.append(conn_index, data);
            driver.pending_replenish.push(pending.bid);
        }
        #[cfg(not(has_io_uring))]
        let _ = (driver, conn_index);
    }
}

impl AsyncRead for ConnStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // Drain any leftover from a prior poll_fill_buf call first.
        if let Some(ref mut cached) = self.fill_buf {
            if !cached.is_empty() {
                let n = cached.len().min(buf.len());
                buf[..n].copy_from_slice(&cached[..n]);
                cached.advance(n);
                if cached.is_empty() {
                    self.fill_buf = None;
                }
                return Poll::Ready(Ok(n));
            }
            self.fill_buf = None;
        }

        let conn_index = self.ctx.conn_index;
        let generation = self.ctx.generation;

        with_state(|driver, executor| {
            // Stale stream: the connection this stream was created for is
            // gone and the slot may belong to a new connection. Reading the
            // accumulator or registering waiters here would leak another
            // connection's bytes and hijack its recv wakeups. Report EOF,
            // mirroring WithDataFuture's generation check.
            if driver.connections.generation(conn_index) != generation {
                return Poll::Ready(Ok(0));
            }
            // Flush zero-copy recv buffer to accumulator if present.
            Self::flush_pending_recv(driver, conn_index);

            let data = driver.accumulators.data(conn_index);
            if !data.is_empty() {
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                driver.accumulators.consume(conn_index, n);
                return Poll::Ready(Ok(n));
            }

            if Self::is_recv_closed(driver, conn_index) {
                return Poll::Ready(Ok(0));
            }

            executor.owner_task[conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.recv_waiters[conn_index as usize] = true;
            Poll::Pending
        })
    }
}

impl AsyncBufRead for ConnStream {
    fn poll_fill_buf(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();

        // Return cached buffer if present and non-empty.
        if matches!(this.fill_buf.as_ref(), Some(b) if !b.is_empty()) {
            return Poll::Ready(Ok(this.fill_buf.as_ref().unwrap().as_ref()));
        }

        // Empty or absent — fetch from accumulator.
        this.fill_buf = None;
        let conn_index = this.ctx.conn_index;
        let generation = this.ctx.generation;

        // Try to detach the accumulator as a refcounted Bytes (O(1)).
        let frozen = with_state(|driver, executor| {
            // Stale stream (slot reused) — report EOF; see poll_read.
            if driver.connections.generation(conn_index) != generation {
                return Ok(None);
            }
            // Flush zero-copy recv buffer to accumulator if present.
            Self::flush_pending_recv(driver, conn_index);

            let data = driver.accumulators.data(conn_index);
            if !data.is_empty() {
                return Ok(Some(driver.accumulators.take_frozen(conn_index)));
            }

            if Self::is_recv_closed(driver, conn_index) {
                return Ok(None); // EOF
            }

            executor.owner_task[conn_index as usize] = Some(CURRENT_TASK_ID.with(|c| c.get()));
            executor.recv_waiters[conn_index as usize] = true;
            Err(()) // Pending
        });

        match frozen {
            Err(()) => Poll::Pending,
            Ok(None) => {
                // EOF — return empty slice.
                Poll::Ready(Ok(&[]))
            }
            Ok(Some(bytes)) => {
                this.fill_buf = Some(bytes);
                Poll::Ready(Ok(this.fill_buf.as_ref().unwrap().as_ref()))
            }
        }
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        if let Some(ref mut b) = self.fill_buf {
            let advance = amt.min(b.len());
            b.advance(advance);
            if b.is_empty() {
                self.fill_buf = None;
            }
        }
    }
}

impl AsyncWrite for ConnStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let ctx = self.ctx;

        // Cap at the send pool's slot size so oversized writes become
        // partial writes (valid per the AsyncWrite contract).
        let max_len = with_state(|driver, _| driver.send_copy_pool.slot_size() as usize);
        let write_len = buf.len().min(max_len);

        match ctx.send_nowait(&buf[..write_len]) {
            Ok(()) => Poll::Ready(Ok(write_len)),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // io_uring submits SQEs directly; no userspace buffer to flush.
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.write_closed {
            self.write_closed = true;
            // Half-close: send TCP FIN so the peer sees EOF.
            // The full close happens when the connection task returns and the
            // executor drops the connection slot.
            self.ctx.shutdown_write();
        }
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_stream_size() {
        // ConnStream should be small — ConnCtx (8 bytes) + bool + Option<Bytes>.
        assert!(std::mem::size_of::<ConnStream>() <= 48);
    }
}
