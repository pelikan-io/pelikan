// Copyright 2021 Twitter, Inc.
// Copyright 2023 Pelikan Cache contributors
// Licensed under the MIT and Apache-2.0 licenses

//! Random number generator initialization

pub use inner::*;

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
