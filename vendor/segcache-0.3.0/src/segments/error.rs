//! Error types for segment operations.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SegmentsError {
    #[error("invalid segment id")]
    BadSegmentId,
    #[error("item relink failure during compaction")]
    RelinkFailure,
    #[error("no segments available for eviction")]
    NoEvictableSegments,
    #[error("eviction failed")]
    EvictFailure,
    #[error("segment size must be greater than item header overhead")]
    SegmentTooSmall,
    #[error(
        "heap size ({heap_size}) must be a non-zero multiple of segment size ({segment_size})"
    )]
    InvalidHeapSize {
        heap_size: usize,
        segment_size: usize,
    },
    #[error("mmap allocation failed")]
    Mmap(#[from] std::io::Error),
}
