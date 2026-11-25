//! What can go wrong for a user of the client library.

use bitacora_meta::MetaError;
use bitacora_segment::format::EncodeError;
use bitacora_types::{Epoch, SegmentAddress, SequenceNumber, Timestamp};
use thiserror::Error;

/// Why an append did not report success (design, section 5, "What an append
/// result means").
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum AppendError {
    /// Another writer has taken the stream. This writer must stop. The
    /// record may or may not have been committed.
    #[error("another writer has taken the stream; the record may or may not be committed")]
    Fenced,
    /// The writer lost contact before learning the outcome. The record may
    /// or may not have been committed; the log decides, and the engine reads
    /// it back after recovery.
    #[error("the outcome of the append is unknown: {reason}")]
    Indeterminate {
        /// What interrupted the append.
        reason: String,
    },
    /// The writer was closed before the append was accepted. The record was
    /// not appended.
    #[error("the writer is closed; the record was not appended")]
    Closed,
    /// The record breaks a size limit. It was not appended.
    #[error("the record was not appended: {0}")]
    Rejected(#[from] EncodeError),
}

/// Why a writer could not be opened.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum OpenError {
    /// The metadata store refused or could not be reached.
    #[error(transparent)]
    Metadata(#[from] MetaError),
    /// Another candidate claimed the stream first. Whoever claims last wins,
    /// so the caller may simply try again if it still should be the writer.
    #[error("another writer claimed the stream; it is now at epoch {current_epoch}")]
    TakenByAnotherWriter {
        /// The stream's epoch after the other claim.
        current_epoch: Epoch,
    },
    /// A healthy writer still holds the lease and the caller chose to wait
    /// for it to lapse.
    #[error("writer {writer_identity:?} holds the stream until {expires_at}")]
    LeaseHeld {
        /// The writer that holds the lease.
        writer_identity: String,
        /// When its lease lapses unless renewed.
        expires_at: Timestamp,
    },
    /// Too few replicas of the previous writer's segment answered to
    /// guarantee that every committed record was found.
    #[error("recovery of segment {segment} reached {reached} of the {needed} replicas it needs")]
    RecoveryQuorumUnavailable {
        /// The segment being recovered.
        segment: SegmentAddress,
        /// How many replicas recovery must hear from.
        needed: usize,
        /// How many answered in time.
        reached: usize,
    },
    /// The controller has not placed the stream's next segment in time.
    #[error("stream {stream:?} has no standby segment; is the controller running?")]
    NoStandbySegment {
        /// The stream that cannot open a segment.
        stream: String,
    },
}

/// Why a read failed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ReadError {
    /// The metadata store refused or could not be reached.
    #[error(transparent)]
    Metadata(#[from] MetaError),
    /// The requested records were deleted. Reading can resume at the
    /// low-water mark.
    #[error("records below {low_water_mark} have been truncated")]
    Truncated {
        /// The first sequence number that can still be read.
        low_water_mark: SequenceNumber,
    },
    /// No replica holding the records could be reached.
    #[error("no replica of segment {segment} could serve the read: {reason}")]
    Unavailable {
        /// The segment that holds the requested records.
        segment: SegmentAddress,
        /// What the last replica tried answered.
        reason: String,
    },
}
