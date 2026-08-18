//! A ghost queue that stores fingerprints (hashes) of recently evicted items.
//! Used by the S3-FIFO eviction policy to decide whether a newly inserted
//! item should go directly into the main segment pool.

use ahash::AHashSet;
use std::collections::VecDeque;

pub(crate) struct GhostQueue {
    queue: VecDeque<u64>,
    set: AHashSet<u64>,
    capacity: usize,
}

impl GhostQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            set: AHashSet::with_capacity(capacity),
            capacity,
        }
    }

    #[inline]
    pub fn contains(&self, hash: u64) -> bool {
        self.set.contains(&hash)
    }

    pub fn insert(&mut self, hash: u64) {
        if self.set.contains(&hash) {
            return;
        }
        while self.set.len() >= self.capacity {
            if let Some(old) = self.queue.pop_front() {
                self.set.remove(&old);
            } else {
                break;
            }
        }
        self.queue.push_back(hash);
        self.set.insert(hash);
    }

    pub fn remove(&mut self, hash: u64) {
        self.set.remove(&hash);
    }
}
