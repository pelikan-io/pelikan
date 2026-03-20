// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A waker that wraps `mio::Waker` with an atomic pending counter to coalesce
//! redundant wake calls into a single syscall.

use core::sync::atomic::{AtomicU64, Ordering};

pub struct Waker {
    inner: mio::Waker,
    pending: AtomicU64,
}

impl From<mio::Waker> for Waker {
    fn from(inner: mio::Waker) -> Self {
        Self {
            inner,
            pending: AtomicU64::new(0),
        }
    }
}

impl Waker {
    /// Wake the associated event loop. Only issues the actual wake syscall if
    /// there are no other pending wakes, avoiding redundant notifications.
    pub fn wake(&self) -> std::io::Result<()> {
        if self.pending.fetch_add(1, Ordering::Relaxed) == 0 {
            self.inner.wake()
        } else {
            Ok(())
        }
    }

    /// Reset the pending counter. Should be called by the event loop after
    /// processing a wake event.
    pub fn reset(&self) {
        self.pending.store(0, Ordering::Relaxed);
    }
}
