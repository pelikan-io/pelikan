//! Same-thread completion delivery for Ringline worker tasks.
//!
//! Storage threads must not call [CompletionTable::complete] directly. They
//! enqueue raw responses and signal the owning Ringline [super::WakeHandle];
//! that worker's on_notify callback then calls complete. This keeps task
//! wakers on the worker thread where Ringline created them.

use slab::Slab;
use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// Identifies one generation of a completion-table slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletionId {
    pub slot: u32,
    pub generation: u32,
}

/// Returned by a [Completion] whose table entry was canceled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletionCanceled;

impl fmt::Display for CompletionCanceled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("completion canceled")
    }
}

impl std::error::Error for CompletionCanceled {}

struct Entry<T> {
    generation: u32,
    retired: bool,
    value: Option<T>,
    waker: Option<Waker>,
}

/// Worker-local registry for outstanding asynchronous operations.
///
/// This type intentionally uses [Rc] and [RefCell], making it non-Send and
/// ensuring access remains local to the Ringline worker that owns its wakers.
pub struct CompletionTable<T> {
    entries: Rc<RefCell<Slab<Entry<T>>>>,
    generations: Rc<RefCell<Vec<u32>>>,
}

impl<T> CompletionTable<T> {
    pub fn new() -> Self {
        Self {
            entries: Rc::new(RefCell::new(Slab::new())),
            generations: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Allocates a table slot and its future.
    pub fn insert(&self) -> (CompletionId, Completion<T>) {
        let mut entries = self.entries.borrow_mut();
        let mut generations = self.generations.borrow_mut();
        let id = loop {
            let vacant = entries.vacant_entry();
            let key = vacant.key();
            let slot = u32::try_from(key).expect("completion table exhausted u32 slot space");

            let generation = match generations.get_mut(key) {
                Some(generation) => match generation.checked_add(1) {
                    Some(next) => {
                        *generation = next;
                        next
                    }
                    None => {
                        vacant.insert(Entry {
                            generation: *generation,
                            retired: true,
                            value: None,
                            waker: None,
                        });
                        continue;
                    }
                },
                None => {
                    generations.resize(key + 1, 0);
                    0
                }
            };

            let id = CompletionId { slot, generation };
            vacant.insert(Entry {
                generation,
                retired: false,
                value: None,
                waker: None,
            });
            break id;
        };

        (
            id,
            Completion {
                entries: Rc::clone(&self.entries),
                id,
                pending: true,
            },
        )
    }

    /// Delivers a value and wakes the pending worker task, if any.
    ///
    /// Returns the value unchanged when the slot is absent, stale, or was
    /// already completed.
    pub fn complete(&self, id: CompletionId, value: T) -> Result<(), T> {
        let waker = {
            let mut entries = self.entries.borrow_mut();
            let Some(entry) = entries.get_mut(id.slot as usize) else {
                return Err(value);
            };
            if entry.retired || entry.generation != id.generation || entry.value.is_some() {
                return Err(value);
            }

            entry.value = Some(value);
            entry.waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Cancels a matching slot and wakes its pending worker task.
    pub fn cancel(&self, id: CompletionId) -> bool {
        let waker = {
            let mut entries = self.entries.borrow_mut();
            if !matches!(
                entries.get(id.slot as usize),
                Some(entry)
                    if !entry.retired
                        && entry.generation == id.generation
                        && entry.value.is_none()
            ) {
                return false;
            }
            entries.remove(id.slot as usize).waker
        };

        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }
}

impl<T> Default for CompletionTable<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for CompletionTable<T> {
    fn clone(&self) -> Self {
        Self {
            entries: Rc::clone(&self.entries),
            generations: Rc::clone(&self.generations),
        }
    }
}

/// Future resolved by its worker-local [CompletionTable].
#[must_use = "futures do nothing unless polled"]
pub struct Completion<T> {
    entries: Rc<RefCell<Slab<Entry<T>>>>,
    id: CompletionId,
    pending: bool,
}

impl<T> Unpin for Completion<T> {}

impl<T> Future for Completion<T> {
    type Output = Result<T, CompletionCanceled>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let completion = self.get_mut();
        assert!(completion.pending, "polled Completion after it resolved");

        let mut entries = completion.entries.borrow_mut();
        let Some(entry) = entries.get_mut(completion.id.slot as usize) else {
            completion.pending = false;
            return Poll::Ready(Err(CompletionCanceled));
        };
        if entry.retired || entry.generation != completion.id.generation {
            completion.pending = false;
            return Poll::Ready(Err(CompletionCanceled));
        }

        if let Some(value) = entry.value.take() {
            entries.remove(completion.id.slot as usize);
            completion.pending = false;
            Poll::Ready(Ok(value))
        } else {
            if !matches!(&entry.waker, Some(waker) if waker.will_wake(context.waker())) {
                entry.waker = Some(context.waker().clone());
            }
            Poll::Pending
        }
    }
}

impl<T> Drop for Completion<T> {
    fn drop(&mut self) {
        if !self.pending {
            return;
        }

        let mut entries = self.entries.borrow_mut();
        if matches!(entries.get(self.id.slot as usize), Some(entry) if !entry.retired && entry.generation == self.id.generation)
        {
            entries.remove(self.id.slot as usize);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CompletionCanceled, CompletionTable};
    use std::future::Future;
    use std::mem::ManuallyDrop;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    #[derive(Default)]
    struct WakerCounts {
        clones: AtomicUsize,
        drops: AtomicUsize,
        wakes: AtomicUsize,
    }

    unsafe fn clone_counting_waker(data: *const ()) -> RawWaker {
        let counts = ManuallyDrop::new(unsafe { Arc::<WakerCounts>::from_raw(data.cast()) });
        counts.clones.fetch_add(1, Ordering::Relaxed);
        let cloned = Arc::clone(&counts);
        RawWaker::new(Arc::into_raw(cloned).cast(), &COUNTING_WAKER_VTABLE)
    }

    unsafe fn wake_counting_waker(data: *const ()) {
        let counts = unsafe { Arc::<WakerCounts>::from_raw(data.cast()) };
        counts.wakes.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn wake_counting_waker_by_ref(data: *const ()) {
        let counts = ManuallyDrop::new(unsafe { Arc::<WakerCounts>::from_raw(data.cast()) });
        counts.wakes.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn drop_counting_waker(data: *const ()) {
        let counts = unsafe { Arc::<WakerCounts>::from_raw(data.cast()) };
        counts.drops.fetch_add(1, Ordering::Relaxed);
    }

    static COUNTING_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_counting_waker,
        wake_counting_waker,
        wake_counting_waker_by_ref,
        drop_counting_waker,
    );

    fn counting_waker() -> (Waker, Arc<WakerCounts>) {
        let counts = Arc::new(WakerCounts::default());
        let raw = RawWaker::new(
            Arc::into_raw(Arc::clone(&counts)).cast(),
            &COUNTING_WAKER_VTABLE,
        );
        // SAFETY: the raw waker owns an Arc, and every vtable operation preserves
        // the Arc strong-count contract.
        let waker = unsafe { Waker::from_raw(raw) };
        (waker, counts)
    }

    fn poll_with<T>(
        future: &mut super::Completion<T>,
        waker: &Waker,
    ) -> Poll<Result<T, CompletionCanceled>> {
        Future::poll(Pin::new(future), &mut Context::from_waker(waker))
    }

    #[test]
    fn pending_completion_is_woken_exactly_once_on_same_thread() {
        let table = CompletionTable::new();
        let (id, mut future) = table.insert();
        let (waker, counts) = counting_waker();

        assert!(poll_with(&mut future, &waker).is_pending());
        table.complete(id, 42).unwrap();

        assert_eq!(counts.wakes.load(Ordering::Relaxed), 1);
        assert_eq!(poll_with(&mut future, &waker), Poll::Ready(Ok(42)));
    }

    #[test]
    fn repeated_pending_poll_keeps_equivalent_waker() {
        let table = CompletionTable::<u8>::new();
        let (_, mut future) = table.insert();
        let (waker, counts) = counting_waker();

        assert!(poll_with(&mut future, &waker).is_pending());
        assert!(poll_with(&mut future, &waker).is_pending());

        assert_eq!(counts.clones.load(Ordering::Relaxed), 1);
        assert_eq!(counts.drops.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn repeated_pending_poll_replaces_different_waker() {
        let table = CompletionTable::new();
        let (id, mut future) = table.insert();
        let (first_waker, first) = counting_waker();
        let (second_waker, second) = counting_waker();

        assert!(poll_with(&mut future, &first_waker).is_pending());
        assert!(poll_with(&mut future, &second_waker).is_pending());
        table.complete(id, 7).unwrap();

        assert_eq!(first.clones.load(Ordering::Relaxed), 1);
        assert_eq!(first.drops.load(Ordering::Relaxed), 1);
        assert_eq!(first.wakes.load(Ordering::Relaxed), 0);
        assert_eq!(second.clones.load(Ordering::Relaxed), 1);
        assert_eq!(second.wakes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn explicit_cancel_wakes_future_as_canceled() {
        let table = CompletionTable::<u8>::new();
        let (id, mut future) = table.insert();
        let (waker, counts) = counting_waker();

        assert!(poll_with(&mut future, &waker).is_pending());
        assert!(table.cancel(id));
        assert!(!table.cancel(id));
        assert_eq!(table.complete(id, 7), Err(7));
        assert_eq!(counts.wakes.load(Ordering::Relaxed), 1);
        assert_eq!(
            poll_with(&mut future, &waker),
            Poll::Ready(Err(CompletionCanceled))
        );
    }

    #[test]
    fn completion_before_first_poll_is_ready_without_waking() {
        let table = CompletionTable::new();
        let (id, mut future) = table.insert();
        let (waker, counts) = counting_waker();

        table.complete(id, 42).unwrap();

        assert_eq!(counts.wakes.load(Ordering::Relaxed), 0);
        assert_eq!(poll_with(&mut future, &waker), Poll::Ready(Ok(42)));
    }

    #[test]
    fn double_completion_preserves_first_value() {
        let table = CompletionTable::new();
        let (id, mut future) = table.insert();
        let (waker, _) = counting_waker();

        assert_eq!(table.complete(id, 1), Ok(()));
        assert_eq!(table.complete(id, 2), Err(2));
        assert_eq!(poll_with(&mut future, &waker), Poll::Ready(Ok(1)));
    }

    #[test]
    fn cancel_after_completion_preserves_ready_value() {
        let table = CompletionTable::new();
        let (id, mut future) = table.insert();
        let (waker, counts) = counting_waker();
        assert!(poll_with(&mut future, &waker).is_pending());

        table.complete(id, 42).unwrap();
        assert!(!table.cancel(id));

        assert_eq!(counts.wakes.load(Ordering::Relaxed), 1);
        assert_eq!(poll_with(&mut future, &waker), Poll::Ready(Ok(42)));
    }

    #[test]
    fn dropping_future_cancels_slot() {
        let table = CompletionTable::<u8>::new();
        let (id, future) = table.insert();

        drop(future);

        assert_eq!(table.complete(id, 7), Err(7));
        assert!(!table.cancel(id));
    }

    #[test]
    fn stale_generation_cannot_complete_reused_slot() {
        let table = CompletionTable::new();
        let (old_id, old_future) = table.insert();
        drop(old_future);
        let (new_id, mut new_future) = table.insert();
        let (waker, _) = counting_waker();

        assert_eq!(old_id.slot, new_id.slot);
        assert_ne!(old_id.generation, new_id.generation);
        assert_eq!(table.complete(old_id, 1), Err(1));
        table.complete(new_id, 2).unwrap();
        assert_eq!(poll_with(&mut new_future, &waker), Poll::Ready(Ok(2)));
    }

    #[test]
    fn exhausted_generation_retires_slot_instead_of_reusing_stale_id() {
        let table = CompletionTable::new();
        let (old_id, old_future) = table.insert();
        drop(old_future);
        table.generations.borrow_mut()[old_id.slot as usize] = u32::MAX;
        let stale_id = super::CompletionId {
            slot: old_id.slot,
            generation: u32::MAX,
        };

        let (new_id, mut new_future) = table.insert();
        let (waker, _) = counting_waker();

        assert_ne!(stale_id.slot, new_id.slot);
        assert_eq!(table.complete(stale_id, 1), Err(1));
        table.complete(new_id, 2).unwrap();
        assert_eq!(poll_with(&mut new_future, &waker), Poll::Ready(Ok(2)));
    }

    #[test]
    fn dropping_polled_ready_future_does_not_cancel_reused_slot() {
        let table = CompletionTable::new();
        let (ready_id, mut ready_future) = table.insert();
        let (waker, _) = counting_waker();
        table.complete(ready_id, 1).unwrap();
        assert_eq!(poll_with(&mut ready_future, &waker), Poll::Ready(Ok(1)));

        let (next_id, mut next_future) = table.insert();
        assert_eq!(ready_id.slot, next_id.slot);
        drop(ready_future);

        table.complete(next_id, 2).unwrap();
        assert_eq!(poll_with(&mut next_future, &waker), Poll::Ready(Ok(2)));
    }

    #[test]
    fn completion_types_are_not_send() {
        trait AmbiguousIfSend<A> {
            fn marker() {}
        }
        impl<T: ?Sized> AmbiguousIfSend<()> for T {}
        impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}

        let _ = <CompletionTable<u8> as AmbiguousIfSend<_>>::marker;
        let _ = <super::Completion<u8> as AmbiguousIfSend<_>>::marker;
    }
}
