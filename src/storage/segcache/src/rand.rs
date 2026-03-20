// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Random number generator initialization

pub use inner::*;

use ::rand::TryRng;
use core::cell::UnsafeCell;
use core::convert::Infallible;
use std::rc::Rc;

pub struct ThreadRng {
    // Rc is explicitly !Send and !Sync
    rng: Rc<UnsafeCell<Random>>,
}

thread_local!(
    // We require Rc<..> to avoid premature freeing when thread_rng is used
    // within thread-local destructors. See #968.
    static THREAD_RNG_KEY: Rc<UnsafeCell<Random>> = {
        let rng = rng();
        Rc::new(UnsafeCell::new(rng))
    }
);

pub fn thread_rng() -> ThreadRng {
    let rng = THREAD_RNG_KEY.with(|t| t.clone());
    ThreadRng { rng }
}

impl TryRng for ThreadRng {
    type Error = Infallible;

    #[inline(always)]
    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        let rng = unsafe { &mut *self.rng.get() };
        rng.try_next_u32()
    }

    #[inline(always)]
    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        let rng = unsafe { &mut *self.rng.get() };
        rng.try_next_u64()
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Self::Error> {
        let rng = unsafe { &mut *self.rng.get() };
        rng.try_fill_bytes(dest)
    }
}

#[cfg(test)]
mod inner {
    use ::rand::SeedableRng;

    pub type Random = rand_xoshiro::Xoshiro256PlusPlus;

    // A very fast PRNG which is appropriate for testing
    pub fn rng() -> Random {
        rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(0)
    }
}

#[cfg(not(test))]
mod inner {
    use ::rand::SeedableRng;

    pub type Random = rand_xoshiro::Xoshiro256PlusPlus;

    // A fast PRNG appropriate for cache eviction sampling.
    pub fn rng() -> Random {
        rand_xoshiro::Xoshiro256PlusPlus::from_rng(&mut ::rand::rng())
    }
}
