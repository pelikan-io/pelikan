use std::mem::{self, MaybeUninit};

use crate::buffer::fixed::RegionId;

/// Trait for user-provided zero-copy send guards.
///
/// The guard keeps registered memory alive until the kernel ZC notification
/// arrives. Implement this for your cache/storage types (e.g., ValueRef).
pub trait SendGuard: Send + 'static {
    /// Pointer and length of the data to send.
    fn as_ptr_len(&self) -> (*const u8, u32);
    /// The registered memory region this data belongs to.
    fn region(&self) -> RegionId;
}

/// VTable for type-erased GuardBox.
struct GuardVTable {
    drop_fn: unsafe fn(*mut u8),
    as_ptr_len_fn: unsafe fn(*const u8) -> (*const u8, u32),
    region_fn: unsafe fn(*const u8) -> RegionId,
}

/// Inline type-erased storage for a `SendGuard` (64 bytes max, 8-byte aligned).
/// Avoids heap allocation for typical guards (~48 bytes).
pub struct GuardBox {
    storage: [MaybeUninit<u64>; 8], // 64 bytes, 8-byte aligned
    vtable: &'static GuardVTable,
}

// Safety: GuardBox only stores `SendGuard: Send` types.
unsafe impl Send for GuardBox {}

impl GuardBox {
    /// Create a new `GuardBox` from a concrete `SendGuard`.
    ///
    /// # Panics
    /// Panics if `size_of::<G>() > 64` or `align_of::<G>() > 8`.
    pub fn new<G: SendGuard>(guard: G) -> Self {
        assert!(
            mem::size_of::<G>() <= 64,
            "SendGuard type {} is {} bytes, max 64",
            std::any::type_name::<G>(),
            mem::size_of::<G>(),
        );
        assert!(
            mem::align_of::<G>() <= 8,
            "SendGuard type {} has alignment {}, max 8",
            std::any::type_name::<G>(),
            mem::align_of::<G>(),
        );

        let mut storage: [MaybeUninit<u64>; 8] = [MaybeUninit::uninit(); 8];

        // Safety: We checked size <= 64 and alignment <= 8 above.
        // [MaybeUninit<u64>; 8] is 64 bytes with 8-byte alignment.
        unsafe {
            let ptr = storage.as_mut_ptr() as *mut G;
            ptr.write(guard);
        }

        GuardBox {
            storage,
            vtable: vtable_for::<G>(),
        }
    }

    /// Get the pointer and length of the guarded data.
    pub fn as_ptr_len(&self) -> (*const u8, u32) {
        unsafe { (self.vtable.as_ptr_len_fn)(self.storage.as_ptr() as *const u8) }
    }

    /// Get the region ID of the guarded data.
    pub fn region(&self) -> RegionId {
        unsafe { (self.vtable.region_fn)(self.storage.as_ptr() as *const u8) }
    }
}

impl Drop for GuardBox {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop_fn)(self.storage.as_mut_ptr() as *mut u8) }
    }
}

unsafe fn guard_drop<G: SendGuard>(ptr: *mut u8) {
    unsafe { std::ptr::drop_in_place(ptr as *mut G) };
}

unsafe fn guard_as_ptr_len<G: SendGuard>(ptr: *const u8) -> (*const u8, u32) {
    let guard = unsafe { &*(ptr as *const G) };
    guard.as_ptr_len()
}

unsafe fn guard_region<G: SendGuard>(ptr: *const u8) -> RegionId {
    let guard = unsafe { &*(ptr as *const G) };
    guard.region()
}

fn vtable_for<G: SendGuard>() -> &'static GuardVTable {
    trait HasVTable {
        const VTABLE: GuardVTable;
    }
    impl<G: SendGuard> HasVTable for G {
        const VTABLE: GuardVTable = GuardVTable {
            drop_fn: guard_drop::<G>,
            as_ptr_len_fn: guard_as_ptr_len::<G>,
            region_fn: guard_region::<G>,
        };
    }
    &<G as HasVTable>::VTABLE
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestGuard {
        ptr: *const u8,
        len: u32,
        region: RegionId,
        dropped: Arc<AtomicBool>,
    }

    unsafe impl Send for TestGuard {}

    impl SendGuard for TestGuard {
        fn as_ptr_len(&self) -> (*const u8, u32) {
            (self.ptr, self.len)
        }
        fn region(&self) -> RegionId {
            self.region
        }
    }

    impl Drop for TestGuard {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn round_trip_ptr_len_region() {
        let data = [1u8, 2, 3, 4, 5];
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = TestGuard {
            ptr: data.as_ptr(),
            len: 5,
            region: RegionId(42),
            dropped: dropped.clone(),
        };

        let boxed = GuardBox::new(guard);
        let (ptr, len) = boxed.as_ptr_len();
        assert_eq!(ptr, data.as_ptr());
        assert_eq!(len, 5);
        assert_eq!(boxed.region(), RegionId(42));
        assert!(!dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn drop_runs() {
        let dropped = Arc::new(AtomicBool::new(false));
        let data = [0u8; 10];
        {
            let guard = TestGuard {
                ptr: data.as_ptr(),
                len: 10,
                region: RegionId(0),
                dropped: dropped.clone(),
            };
            let _boxed = GuardBox::new(guard);
            assert!(!dropped.load(Ordering::SeqCst));
        }
        assert!(dropped.load(Ordering::SeqCst));
    }

    /// Zero-size guard (no data stored, just region tracking).
    struct ZeroSizeGuard;

    impl SendGuard for ZeroSizeGuard {
        fn as_ptr_len(&self) -> (*const u8, u32) {
            (std::ptr::null(), 0)
        }
        fn region(&self) -> RegionId {
            RegionId(99)
        }
    }

    #[test]
    fn zero_size_guard() {
        assert_eq!(std::mem::size_of::<ZeroSizeGuard>(), 0);
        let boxed = GuardBox::new(ZeroSizeGuard);
        let (ptr, len) = boxed.as_ptr_len();
        assert!(ptr.is_null());
        assert_eq!(len, 0);
        assert_eq!(boxed.region(), RegionId(99));
    }

    /// Guard that uses the full 64 bytes.
    #[repr(C)]
    struct MaxSizeGuard {
        _data: [u8; 64],
    }

    impl SendGuard for MaxSizeGuard {
        fn as_ptr_len(&self) -> (*const u8, u32) {
            (self._data.as_ptr(), 64)
        }
        fn region(&self) -> RegionId {
            RegionId(1)
        }
    }

    #[test]
    fn max_size_guard() {
        let guard = MaxSizeGuard { _data: [0xAB; 64] };
        let boxed = GuardBox::new(guard);
        let (ptr, len) = boxed.as_ptr_len();
        assert_eq!(len, 64);
        let slice = unsafe { std::slice::from_raw_parts(ptr, 64) };
        assert!(slice.iter().all(|&b| b == 0xAB));
    }
}
