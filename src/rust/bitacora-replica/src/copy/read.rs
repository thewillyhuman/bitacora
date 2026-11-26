//! Reads: what a copy serves, and to whom.
//!
//! # Why this file exists
//!
//! A replica holds records before they are committed: a writer sends a batch to every replica and only
//! counts it as committed when enough of them have acknowledged. A reader must never see a record that
//! may still be lost, so a copy does not serve what it holds, it serves what the writer has declared
//! durable (design, section 6). The one reader that needs more is a writer taking over the stream, which
//! has to see the uncommitted tail to decide where the segment ends, and it may only look after it has
//! fenced the copy (design, section 9, step 6). This file is where a copy decides how far a read may go.
//!
//! # What it contains
//!
//! - `SegmentCopy::read`: reads batches for a reader or for a recovering writer.
//!
//! # How it works
//!
//! The limit of a read depends on the copy and on who asks:
//!
//! | Copy | Asked by | Readable up to |
//! | --- | --- | --- |
//! | sealed | anyone | the sealed end |
//! | open | a reader | the durable sequence last heard from the writer |
//! | open | a recovering writer at the fenced epoch | the copy's end |
//!
//! In every case the limit is capped at what the copy holds. The records file then returns whole batch
//! frames from the wanted sequence number up to the limit, as many as fit in the request's size, each one
//! verified against its checksum again on the way out.
//!
//! # Invariants
//!
//! - An open copy never serves a record above the durable sequence to anyone who has not fenced it.
//! - A recovering writer reads the tail only at exactly the epoch the copy is fenced at: below it, it has
//!   itself been superseded; above it, it has not fenced this copy yet.
//! - A frame that fails its checksum on the way out damages the copy and is not served.
//! - Reads run for every reader request. A read copies each returned frame once, in the records file, and
//!   allocates nothing else here.
//!
//! # See also
//!
//! - `../copy.rs`: the copy, and the durable sequence it remembers.
//! - `append.rs`: where the durable sequence is learned.
//! - `fence_seal.rs`: the fence a recovering writer needs before it reads the tail.
//! - `../actor.rs`: how a read waits for records to become readable.

use bitacora_segment::SegmentFile;
use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::{ReadLimit, ReadRequest, ReadResponse, ReplicaError};

use super::{SegmentCopy, invalid_request};

impl SegmentCopy {
    /// Reads batches for a reader or for a recovering writer.
    ///
    /// Reads the records file from the disk and verifies every frame it returns. See the file header for
    /// how far a read may go.
    ///
    /// # Arguments
    ///
    /// * `request` - The first sequence number wanted, the most bytes to return, and whose limit applies.
    ///   The request's wait is not looked at here: the replica waits before it asks the copy.
    ///
    /// # Returns
    ///
    /// Whole batch frames starting with the batch that holds the wanted sequence number, possibly none;
    /// the last sequence number that was readable; and the sealed end if the copy knows one.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Storage`] if the copy is damaged, or if a frame fails its checksum while it is
    ///   read, which damages the copy.
    /// * [`ReplicaError::SegmentNotFound`] if the replica has no records of the segment.
    /// * [`ReplicaError::Fenced`] if a recovering writer asks at an epoch below the copy's fence.
    /// * [`ReplicaError::InvalidRequest`] if a recovering writer asks at an epoch it has not fenced the
    ///   copy at.
    pub(crate) async fn read(&mut self, request: &ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.fail_if_unsound()?;
        let Some(file) = &self.file else {
            return Err(ReplicaError::SegmentNotFound { segment: self.address });
        };
        let readable_end = self.readable_end_for(file, request.limit)?;
        let read = file
            .read_frames(request.from_sequence, readable_end, request.max_bytes)
            .await;
        let batch_frames = match read {
            Ok(batch_frames) => batch_frames,
            Err(segment_error) => return Err(self.damaged_by(&segment_error)),
        };
        Ok(ReadResponse {
            batch_frames,
            readable_end,
            sealed_end: self.state.sealed_end,
        })
    }

    /// Decides the last sequence number a read with `limit` may be served from `file`, the copy's records
    /// file. See the table in the file header.
    fn readable_end_for(&self, file: &SegmentFile, limit: ReadLimit) -> Result<SequenceNumber, ReplicaError> {
        let local_end = file.local_end();
        if let Some(sealed_end) = self.state.sealed_end {
            return Ok(local_end.min(sealed_end));
        }
        match limit {
            ReadLimit::DurableSequence => Ok(local_end.min(self.known_durable)),
            ReadLimit::LocalEnd { recovery_epoch } => {
                if recovery_epoch < self.state.fenced_epoch {
                    return Err(ReplicaError::Fenced {
                        request_epoch: recovery_epoch,
                        fenced_epoch: self.state.fenced_epoch,
                    });
                }
                if recovery_epoch > self.state.fenced_epoch {
                    return Err(invalid_request(
                        "the copy must be fenced at the recovery epoch before its tail is read",
                    ));
                }
                Ok(local_end)
            }
        }
    }
}

#[cfg(test)]
#[path = "read_test.rs"]
mod tests;
