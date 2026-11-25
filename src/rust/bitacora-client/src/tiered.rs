//! Reading a sealed segment from the object store.
//!
//! # Why this file exists
//!
//! Some time after a segment is sealed, a replica uploads it to the object store, and later the replicas
//! drop their copies (design, section 10, "Tiering"). From then on the object is the only place the
//! segment's records can be read from. A reader must not notice, apart from latency (design, section 6,
//! "Reading tiered segments"). This file is the part of the read path that fetches records from that
//! object instead of from a replica.
//!
//! # What it contains
//!
//! - [`TieredSegment`]: an opened tiered object, ready to serve records from any sequence number.
//! - [`TieredReadError`]: why the object could not be opened or read.
//!
//! # How it works
//!
//! Opening an object costs two range requests: one for the footer, whose position follows from the object
//! size recorded in the metadata store, and one for the block index the footer points at. The footer must
//! describe exactly the segment the metadata store says the object holds; anything else is refused, so a
//! wrong key or a replaced object cannot feed a reader records of another segment.
//!
//! Reading costs one range request per call: the block that holds the requested sequence number and as
//! many following blocks as fit in the caller's byte limit. Every batch in the fetched bytes is verified
//! against its checksum before a record is handed out, exactly as for bytes that come from a replica.
//!
//! Every request to the object store has a timeout.
//!
//! # Invariants
//!
//! - A record is returned only from a batch that passed its checksum for this segment.
//! - A [`TieredSegment`] never changes after it is opened: tiered objects are immutable.
//!
//! # See also
//!
//! - `bitacora-segment/src/tiered.rs`: the layout of the object.
//! - `reader.rs`: the cursor that decides when to read from the object store.

use std::ops::Range;
use std::time::Duration;

use bitacora_artifact::{ArtifactError, ArtifactStore};
use bitacora_meta::TieredRecord;
use bitacora_segment::format::{FramesError, decode_frames};
use bitacora_segment::tiered::{TIERED_FOOTER_BYTES, TieredError, TieredFooter, TieredIndex};
use bitacora_types::{LogRecord, SegmentAddress, SequenceNumber};
use bytes::Bytes;
use thiserror::Error;
use tokio::time::timeout;

/// Why a tiered segment could not be opened or read.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum TieredReadError {
    /// The object store failed the request.
    #[error("object store failed to read {object_key:?}: {reason}")]
    StoreFailed {
        /// The object that was being read.
        object_key: String,
        /// What the object store reported.
        reason: ArtifactError,
    },
    /// The object store did not answer in time.
    #[error("object store did not answer for {object_key:?} within {waited:?}")]
    TimedOut {
        /// The object that was being read.
        object_key: String,
        /// How long the reader waited.
        waited: Duration,
    },
    /// The object's footer or block index is damaged.
    #[error("object {object_key:?} is damaged: {reason}")]
    Damaged {
        /// The object that was read.
        object_key: String,
        /// What is wrong with it.
        reason: TieredError,
    },
    /// The object is intact but is not the segment the metadata store says it is.
    #[error("object {object_key:?} is not segment {expected_segment} sealed at {expected_sealed_end}")]
    WrongSegment {
        /// The object that was read.
        object_key: String,
        /// The segment the metadata store says the object holds.
        expected_segment: SegmentAddress,
        /// Where the metadata store says that segment ends.
        expected_sealed_end: SequenceNumber,
    },
    /// A block of the object holds a batch that fails its checksum or is out of order.
    #[error("object {object_key:?} holds invalid batches: {reason}")]
    InvalidBatches {
        /// The object that was read.
        object_key: String,
        /// What is wrong with the batches.
        reason: FramesError,
    },
}

/// A tiered segment object that was opened and found to be the expected segment.
#[derive(Clone, Debug)]
pub(crate) struct TieredSegment {
    /// The key of the object in the object store.
    object_key: String,
    /// The segment the object holds. Batch checksums are verified against it.
    address: SegmentAddress,
    /// Where each block of the object starts.
    index: TieredIndex,
    /// How long one request to the object store may take.
    request_timeout: Duration,
}

impl TieredSegment {
    /// Opens the tiered object of a segment: fetches and verifies its footer and its block index.
    ///
    /// Makes two range requests to the object store.
    ///
    /// # Arguments
    ///
    /// * `artifact_store` - The object store that holds the object.
    /// * `address` - The segment the metadata store says the object holds.
    /// * `sealed_end` - Where the metadata store says that segment ends.
    /// * `tiered` - The object's key and size, as recorded in the metadata store.
    /// * `request_timeout` - How long each request to the object store may take, now and in later reads.
    ///
    /// # Errors
    ///
    /// * [`TieredReadError::StoreFailed`] or [`TieredReadError::TimedOut`] if the object store fails or is
    ///   too slow. Trying again later may work.
    /// * [`TieredReadError::Damaged`] if the footer or the index is damaged.
    /// * [`TieredReadError::WrongSegment`] if the object is another segment, ends elsewhere, or has another
    ///   size than recorded.
    pub(crate) async fn open(
        artifact_store: &dyn ArtifactStore,
        address: SegmentAddress,
        sealed_end: SequenceNumber,
        tiered: &TieredRecord,
        request_timeout: Duration,
    ) -> Result<Self, TieredReadError> {
        let object_key = tiered.object_key.clone();
        let wrong_segment = || TieredReadError::WrongSegment {
            object_key: object_key.clone(),
            expected_segment: address,
            expected_sealed_end: sealed_end,
        };
        let damaged = |reason| TieredReadError::Damaged {
            object_key: object_key.clone(),
            reason,
        };

        let Some(footer_start) = tiered.size_bytes.checked_sub(TIERED_FOOTER_BYTES as u64) else {
            return Err(wrong_segment());
        };
        let footer_bytes = fetch(
            artifact_store,
            &object_key,
            footer_start..tiered.size_bytes,
            request_timeout,
        )
        .await?;
        let footer = TieredFooter::decode(&footer_bytes).map_err(damaged)?;
        let is_the_expected_segment = footer.segment.address == address
            && footer.sealed_end == sealed_end
            && footer.object_bytes() == tiered.size_bytes;
        if !is_the_expected_segment {
            return Err(wrong_segment());
        }

        let index_bytes = fetch(artifact_store, &object_key, footer.index_range(), request_timeout).await?;
        let index = TieredIndex::decode(&footer, &index_bytes).map_err(damaged)?;
        Ok(Self {
            object_key,
            address,
            index,
            request_timeout,
        })
    }

    /// Reads records of the segment starting at `from_sequence`.
    ///
    /// Makes one range request for the block that holds `from_sequence` and as many of the following
    /// blocks as fit in `max_bytes`, and verifies every batch in them.
    ///
    /// # Arguments
    ///
    /// * `artifact_store` - The object store that holds the object.
    /// * `from_sequence` - The first sequence number wanted.
    /// * `max_bytes` - About how many bytes to fetch. At least one whole block is always fetched.
    ///
    /// # Returns
    ///
    /// The records from `from_sequence` to the end of the fetched blocks, in order. Empty if the segment
    /// holds nothing at or after `from_sequence`.
    ///
    /// # Errors
    ///
    /// * [`TieredReadError::StoreFailed`] or [`TieredReadError::TimedOut`] if the object store fails or is
    ///   too slow.
    /// * [`TieredReadError::InvalidBatches`] if a fetched batch fails its checksum.
    pub(crate) async fn read_from(
        &self,
        artifact_store: &dyn ArtifactStore,
        from_sequence: SequenceNumber,
        max_bytes: usize,
    ) -> Result<Vec<LogRecord>, TieredReadError> {
        let blocks_range = self.index.blocks_from(from_sequence, max_bytes as u64);
        if blocks_range.is_empty() {
            return Ok(Vec::new());
        }
        let blocks = fetch(artifact_store, &self.object_key, blocks_range, self.request_timeout).await?;
        let batches = match decode_frames(self.address, &blocks) {
            Ok(batches) => batches,
            Err(reason) => {
                return Err(TieredReadError::InvalidBatches {
                    object_key: self.object_key.clone(),
                    reason,
                });
            }
        };
        let mut records = Vec::new();
        for batch in &batches {
            for record in batch.records() {
                if record.sequence >= from_sequence {
                    records.push(record);
                }
            }
        }
        Ok(records)
    }
}

/// Fetches one byte range of an object, waiting no longer than `request_timeout`.
async fn fetch(
    artifact_store: &dyn ArtifactStore,
    object_key: &str,
    byte_range: Range<u64>,
    request_timeout: Duration,
) -> Result<Bytes, TieredReadError> {
    let fetched = timeout(request_timeout, artifact_store.get_range(object_key, byte_range)).await;
    match fetched {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(reason)) => Err(TieredReadError::StoreFailed {
            object_key: object_key.to_owned(),
            reason,
        }),
        Err(_elapsed) => Err(TieredReadError::TimedOut {
            object_key: object_key.to_owned(),
            waited: request_timeout,
        }),
    }
}

#[cfg(test)]
#[path = "tiered_test.rs"]
mod tests;
