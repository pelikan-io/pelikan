use segcache::*;
use std::time::Duration;

// Segment size chosen so items fill segments the same way as integration_basic.rs.
const SEGMENT_SIZE: i32 = 264;

fn small_cache(segments: usize, policy: Policy) -> Segcache {
    Segcache::builder()
        .segment_size(SEGMENT_SIZE)
        .heap_size(segments * SEGMENT_SIZE as usize)
        .hash_power(16)
        .eviction(policy)
        .build()
        .expect("failed to create cache")
}

// ── Bug: can_evict() blocks segments with short TTL ───────────────────────────
//
// can_evict() used to require (create_at + ttl) >= (now + 20s), which is
// equivalent to "remaining TTL >= 20s". Any segment whose items have a TTL
// shorter than 20 s could never be explicitly evicted — can_evict() returned
// false for them unconditionally — causing NoFreeSegments when the cache was
// full of such items.

#[test]
fn random_evicts_short_ttl_segment_when_full() {
    let mut cache = small_cache(2, Policy::Random);
    let ttl = Duration::from_secs(10); // < former SEG_MATURE_TIME (20s)

    // Fill both segments (mirroring integration_basic sizes).
    let _ = cache.insert(
        b"a",
        b"What's in a name? A rose by any other name would smell as sweet.",
        None,
        ttl,
    );
    let _ = cache.insert(b"b", b"All that glitters is not gold.", None, ttl);
    let _ = cache.insert(
        b"c",
        b"Cry 'havoc' and let slip the dogs of war.",
        None,
        ttl,
    );
    // segment 1 is now full

    let _ = cache.insert(
        b"d",
        b"There are more things in heaven and earth, Horatio, than are dreamt of in your philosophy.",
        None,
        ttl,
    );
    let _ = cache.insert(
        b"e",
        b"Uneasy lies the head that wears the crown.",
        None,
        ttl,
    );
    let _ = cache.insert(b"f", b"Brevity is the soul of wit.", None, ttl);
    #[cfg(not(feature = "integrity"))]
    let _ = cache.insert(
        b"g",
        b"But, for my own part, it was Greek to me.",
        None,
        ttl,
    );
    #[cfg(feature = "integrity")]
    let _ = cache.insert(b"g", b"Et tu, Brute?", None, ttl);
    // segment 2 is now full

    // This insert needs a free segment. It must evict segment 1 (which holds
    // short-TTL items). Before the fix can_evict() always returned false for
    // these segments, so this returned Err(NoFreeSegments).
    let result = cache.insert(
        b"h",
        b"There is nothing either good or bad, but thinking makes it so.",
        None,
        ttl,
    );

    assert!(
        result.is_ok(),
        "inserting into a full cache of short-TTL items should succeed via eviction"
    );
    assert!(
        cache.get(b"h").is_some(),
        "newly inserted item must be readable"
    );
}

// ── Bug: compare_fifo sorts by NEWEST first (LIFO) instead of OLDEST (FIFO) ──
//
// compare_fifo called lhs_age.cmp(&rhs_age).reverse(). sort_by is ascending,
// so .reverse() placed the segment with the LARGEST timestamp (most recently
// created/merged) at index 0, which is the slot evicted first. This is LIFO.
// Removing .reverse() makes the oldest segment sort first — correct FIFO.
//
// clocksource::coarse::Instant has 1-second resolution (stored as whole
// seconds), so we need at least a 1-second sleep to produce timestamps that
// the comparator can distinguish.

#[test]
fn fifo_evicts_oldest_segment_first() {
    // 3 segments: seg1 (old items) → seg2 (new items) → seg3 (tail/current write target).
    // Eviction must choose between seg1 and seg2; seg3 is never eligible because
    // it has no next_seg.  Correct FIFO picks seg1 (oldest).
    let mut cache = small_cache(3, Policy::Fifo);
    let ttl = Duration::ZERO;

    // Fill segment 1 with "old" items.
    let _ = cache.insert(
        b"old_a",
        b"What's in a name? A rose by any other name would smell as sweet.",
        None,
        ttl,
    );
    let _ = cache.insert(b"old_b", b"All that glitters is not gold.", None, ttl);
    let _ = cache.insert(
        b"old_c",
        b"Cry 'havoc' and let slip the dogs of war.",
        None,
        ttl,
    );
    // segment 1 is sealed; segment 2 becomes the new tail.

    // Sleep long enough for clocksource::coarse (1-second resolution) to tick.
    std::thread::sleep(Duration::from_millis(1100));

    // Fill segment 2 with "new" items.
    let _ = cache.insert(
        b"new_d",
        b"There are more things in heaven and earth, Horatio, than are dreamt of in your philosophy.",
        None,
        ttl,
    );
    let _ = cache.insert(
        b"new_e",
        b"Uneasy lies the head that wears the crown.",
        None,
        ttl,
    );
    let _ = cache.insert(b"new_f", b"Brevity is the soul of wit.", None, ttl);
    #[cfg(not(feature = "integrity"))]
    let _ = cache.insert(
        b"new_g",
        b"But, for my own part, it was Greek to me.",
        None,
        ttl,
    );
    #[cfg(feature = "integrity")]
    let _ = cache.insert(b"new_g", b"Et tu, Brute?", None, ttl);
    // segment 2 is sealed; segment 3 becomes the new tail.

    // Fill segment 3 so that the next insert must evict.
    // (Without this, "trigger" would fit in the still-empty tail segment 3
    // and no eviction would occur.)
    let _ = cache.insert(
        b"seg3_a",
        b"What's in a name? A rose by any other name would smell as sweet.",
        None,
        ttl,
    );
    let _ = cache.insert(b"seg3_b", b"All that glitters is not gold.", None, ttl);
    let _ = cache.insert(
        b"seg3_c",
        b"Cry 'havoc' and let slip the dogs of war.",
        None,
        ttl,
    );
    // segment 3 is now sealed; segment 4 would be the new tail — but we only
    // have 3 total, so the next insert triggers eviction.

    // Trigger eviction: insert something that requires a free segment.
    let _ = cache.insert(
        b"trigger",
        b"There is nothing either good or bad, but thinking makes it so.",
        None,
        ttl,
    );

    // Correct FIFO: oldest segment (seg1, "old_*" items) is evicted.
    assert!(
        cache.get(b"old_a").is_none(),
        "FIFO must evict the oldest segment: old_a should be gone"
    );
    assert!(
        cache.get(b"old_b").is_none(),
        "FIFO must evict the oldest segment: old_b should be gone"
    );
    assert!(
        cache.get(b"old_c").is_none(),
        "FIFO must evict the oldest segment: old_c should be gone"
    );

    // The newer segment (seg2, "new_*" items) must still be present.
    assert!(
        cache.get(b"new_d").is_some(),
        "FIFO must not evict the newer segment: new_d should be present"
    );
    assert!(
        cache.get(b"new_e").is_some(),
        "FIFO must not evict the newer segment: new_e should be present"
    );
}
