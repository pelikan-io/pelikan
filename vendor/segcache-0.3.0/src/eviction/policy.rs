//! Eviction policies for segment selection.

/// Strategy for choosing which segment to reclaim under memory pressure.
///
/// Every policy operates at segment granularity — individual items are
/// never evicted in isolation (except during merge pruning). Segments
/// that are still the active write target for a TTL bucket are always
/// excluded from eviction.
#[derive(Copy, Clone, Debug)]
pub enum Policy {
    /// Disable eviction entirely. Insertions return an error once every
    /// segment is occupied. Segments are only reclaimed via TTL expiry.
    None,

    /// Choose an evictable segment uniformly at random. Simple and fast,
    /// but blind to item value — equivalent to random slab eviction.
    Random,

    /// Select a random *occupied* segment, find the TTL bucket it
    /// belongs to, and evict that bucket's head (oldest) segment. This
    /// weights eviction toward TTL ranges that consume the most memory
    /// while preserving the overall TTL distribution of the cache.
    RandomFifo,

    /// Evict the oldest segment across all TTL buckets, measured by the
    /// later of its creation and last-merge timestamps. Because segments
    /// are append-only, this behaves like LRU at segment granularity.
    Fifo,

    /// Evict the segment whose items will expire soonest
    /// (`create_at + ttl`). Effectively brings forward an expiration
    /// that would have happened shortly anyway, minimising wasted work.
    Cte,

    /// Evict the segment with the lowest `live_bytes`. Segments
    /// accumulate dead space when items are overwritten or deleted, so
    /// this policy targets the most fragmented segments first —
    /// reclaiming the most capacity with the least data loss.
    Util,

    /// Merge-based eviction from the segcache NSDI paper. Walks a chain
    /// of adjacent segments, scores each item by its approximate
    /// frequency, and copies high-value items into a compacted target
    /// segment while dropping the rest.
    ///
    /// Two sub-modes:
    /// - **Eviction merge**: prunes low-frequency items to free space.
    /// - **Compaction merge**: copies without pruning, triggered when a
    ///   segment's occupancy drops below `1/compact`. Useful for
    ///   workloads with heavy overwrites or deletes.
    Merge {
        /// Upper bound on segments consumed in a single merge pass.
        /// Limits tail-latency impact of eviction.
        max: usize,
        /// Number of segments to consider during an eviction merge.
        /// Higher values give the frequency estimator more data;
        /// lower values evict fewer items per pass.
        merge: usize,
        /// Number of segments to combine during compaction. Compaction
        /// fires when a segment falls below `1/compact` live-byte
        /// occupancy. Set to 0 to disable compaction entirely.
        compact: usize,
    },

    /// S3-FIFO (S3-Segcache): a two-pool design with a ghost filter.
    ///
    /// Fresh items enter the *admission* pool. When an admission segment
    /// is evicted, items whose frequency counter is non-zero are promoted
    /// to the *main* pool; zero-frequency items are discarded and their
    /// key hashes recorded in a ghost queue. On a subsequent insert, if
    /// the key's hash appears in the ghost queue, the item skips
    /// admission and goes directly to main — a second-chance mechanism
    /// that avoids re-admitting one-hit wonders. The main pool evicts
    /// using a CLOCK sweep that gives each item one additional chance
    /// based on its frequency.
    S3Fifo {
        /// Fraction of total segments allocated to the admission pool
        /// (0.0–1.0). The remainder forms the main pool. Typical range
        /// is 0.05–0.20.
        admission_ratio: f64,
    },
}
