//! Cross-thread control plane for dynamic fixed-buffer region registration.
//!
//! Each worker owns a [`RegionControlRx`] that the event loop drains once per
//! tick. [`ShutdownHandle::register_region`](crate::ShutdownHandle::register_region)
//! and [`ShutdownHandle::unregister_region`](crate::ShutdownHandle::unregister_region)
//! send a [`RegionControlMsg`] to every worker, wake them via their
//! [`WakeHandle`](crate::WakeHandle), and block until every worker acks.
//!
//! Slot indices are coherent across workers: the registrar picks a slot, then
//! every worker writes the same iovec into the same `RegionId`.

use std::io;
use std::sync::Mutex;

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};

use crate::buffer::fixed::{MemoryRegion, RegionId};
use crate::wakeup::WakeHandle;

/// A control message dispatched to every worker for a region operation.
pub(crate) enum RegionControlMsg {
    Register {
        slot: u16,
        region: MemoryRegion,
        ack: Sender<io::Result<()>>,
    },
    Unregister {
        slot: u16,
        ack: Sender<io::Result<()>>,
    },
}

/// Per-worker receiver. The event loop owns this and drains it each tick.
pub(crate) type RegionControlRx = Receiver<RegionControlMsg>;

/// Shared registrar held by [`ShutdownHandle`]. Owns the slot allocator and
/// the per-worker senders.
pub(crate) struct RegionRegistrar {
    inner: Mutex<RegistrarInner>,
    workers: Vec<Sender<RegionControlMsg>>,
    wake_handles: Vec<WakeHandle>,
}

struct RegistrarInner {
    /// Stack of free slot indices, popped on `register`, pushed on `unregister`.
    free_slots: Vec<u16>,
}

impl RegionRegistrar {
    /// Build a registrar with `max_slots` total capacity, where slots
    /// `0..reserved` are pre-occupied by the initial `Config::registered_regions`.
    pub(crate) fn new(
        max_slots: u16,
        reserved: u16,
        workers: Vec<Sender<RegionControlMsg>>,
        wake_handles: Vec<WakeHandle>,
    ) -> Self {
        debug_assert!(reserved <= max_slots);
        debug_assert_eq!(workers.len(), wake_handles.len());
        // Free list ordered so smaller slots come out first.
        let free_slots: Vec<u16> = (reserved..max_slots).rev().collect();
        RegionRegistrar {
            inner: Mutex::new(RegistrarInner { free_slots }),
            workers,
            wake_handles,
        }
    }

    /// Allocate a slot, broadcast a `Register` message to every worker, and
    /// block until all workers ack. On any worker error the slot is returned
    /// to the free list and the first error surfaces.
    pub(crate) fn register(&self, region: MemoryRegion) -> io::Result<RegionId> {
        let slot = {
            let mut inner = self.inner.lock().expect("registrar mutex poisoned");
            inner
                .free_slots
                .pop()
                .ok_or_else(|| io::Error::other("registered-region table is full"))?
        };

        match self.broadcast(|ack| RegionControlMsg::Register {
            slot,
            region: region.clone(),
            ack,
        }) {
            Ok(()) => Ok(RegionId(slot)),
            Err(e) => {
                self.return_slot(slot);
                Err(e)
            }
        }
    }

    /// Broadcast an `Unregister` message and block until every worker acks.
    /// On success the slot returns to the free list.
    pub(crate) fn unregister(&self, id: RegionId) -> io::Result<()> {
        if id == RegionId::UNREGISTERED {
            return Err(io::Error::other("cannot unregister UNREGISTERED sentinel"));
        }
        let slot = id.0;
        self.broadcast(|ack| RegionControlMsg::Unregister { slot, ack })?;
        self.return_slot(slot);
        Ok(())
    }

    fn broadcast(
        &self,
        mut build: impl FnMut(Sender<io::Result<()>>) -> RegionControlMsg,
    ) -> io::Result<()> {
        let n = self.workers.len();
        let (ack_tx, ack_rx) = bounded(n);

        for (tx, wake) in self.workers.iter().zip(&self.wake_handles) {
            // Each message carries its own ack sender so workers can reply
            // independently without coordinating with each other.
            let msg = build(ack_tx.clone());
            tx.send(msg)
                .map_err(|_| io::Error::other("worker control channel disconnected"))?;
            wake.wake();
        }
        drop(ack_tx);

        // Collect every ack; surface the first error encountered while still
        // draining the rest so no worker is left blocked on a send.
        let mut first_err: Option<io::Error> = None;
        for _ in 0..n {
            match ack_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) if first_err.is_none() => first_err = Some(e),
                Ok(Err(_)) => {}
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(io::Error::other(
                            "worker terminated before acknowledging region update",
                        ));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn return_slot(&self, slot: u16) {
        let mut inner = self.inner.lock().expect("registrar mutex poisoned");
        inner.free_slots.push(slot);
    }
}

/// Build per-worker sender/receiver pairs for the region control channel.
pub(crate) fn build_worker_channels(
    num_workers: usize,
) -> (Vec<Sender<RegionControlMsg>>, Vec<RegionControlRx>) {
    let mut txs = Vec::with_capacity(num_workers);
    let mut rxs = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let (tx, rx) = unbounded();
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, rxs)
}
