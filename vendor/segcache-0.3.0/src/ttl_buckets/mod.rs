//! TTL buckets group segments by expiration time for eager expiration.
//!
//! Each [`TtlBucket`] contains a doubly-linked chain of segments whose
//! items share a similar TTL. The [`TtlBuckets`] collection maps the
//! full TTL range across 1024 buckets with logarithmic widths.

mod error;
mod ttl_bucket;
#[allow(clippy::module_inception)]
mod ttl_buckets;

#[cfg(test)]
mod tests;

pub use error::TtlBucketsError;
pub use ttl_bucket::TtlBucket;
pub use ttl_buckets::TtlBuckets;
