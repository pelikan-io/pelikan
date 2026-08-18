//! Tests for TTL bucket index mapping.

use crate::*;

#[test]
fn bucket_index() {
    let ttl_buckets = TtlBuckets::new();

    // Zero TTL and max duration both map to the last bucket.
    assert_eq!(ttl_buckets.get_bucket_index(Duration::from_secs(0)), 1023);
    assert_eq!(
        ttl_buckets.get_bucket_index(Duration::from_secs(u32::MAX)),
        1023
    );

    // First bucket covers TTLs 1–7s (bucket 0).
    assert_eq!(ttl_buckets.get_bucket_index(Duration::from_secs(1)), 0);
    assert_eq!(ttl_buckets.get_bucket_index(Duration::from_secs(7)), 0);

    // Tier 1: 8s–2048s, buckets 1–255, each 8s wide.
    for bucket in 1..256 {
        let start = Duration::from_secs(8 * bucket);
        let end = Duration::from_secs(8 * bucket + 7);
        assert_eq!(
            ttl_buckets.get_bucket_index(start) as u32,
            bucket,
            "ttl: {start:?}"
        );
        assert_eq!(
            ttl_buckets.get_bucket_index(end) as u32,
            bucket,
            "ttl: {end:?}"
        );
    }

    // Tier 2: 2048s–32768s, buckets 256–511, each 128s wide.
    for bucket in 16..256 {
        let start = Duration::from_secs(128 * bucket);
        let end = Duration::from_secs(128 * bucket + 127);
        assert_eq!(
            ttl_buckets.get_bucket_index(start) as u32,
            bucket + 256,
            "ttl: {start:?}"
        );
        assert_eq!(
            ttl_buckets.get_bucket_index(end) as u32,
            bucket + 256,
            "ttl: {end:?}"
        );
    }

    // Tier 3: 32768s–524288s, buckets 512–767, each 2048s wide.
    for bucket in 16..256 {
        let start = Duration::from_secs(2048 * bucket);
        let end = Duration::from_secs(2048 * bucket + 2047);
        assert_eq!(
            ttl_buckets.get_bucket_index(start) as u32,
            bucket + 512,
            "ttl: {start:?}"
        );
        assert_eq!(
            ttl_buckets.get_bucket_index(end) as u32,
            bucket + 512,
            "ttl: {end:?}"
        );
    }

    // Tier 4: 524288s–8388608s, buckets 768–1023, each 32768s wide.
    for bucket in 16..256 {
        let start = Duration::from_secs(32_768 * bucket);
        let end = Duration::from_secs(32_768 * bucket + 32_767);
        assert_eq!(
            ttl_buckets.get_bucket_index(start) as u32,
            bucket + 768,
            "ttl: {start:?}"
        );
        assert_eq!(
            ttl_buckets.get_bucket_index(end) as u32,
            bucket + 768,
            "ttl: {end:?}"
        );
    }

    // Beyond ~97 days maps to the max bucket.
    assert_eq!(
        ttl_buckets.get_bucket_index(Duration::from_secs(8_388_608)) as u32,
        1023
    );
}
