//! Error types for TTL bucket operations.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TtlBucketsError {
    #[error("item oversized ({size} bytes)")]
    ItemOversized { size: usize },
    #[error("no free segments available for TTL bucket expansion")]
    NoFreeSegments,
}
