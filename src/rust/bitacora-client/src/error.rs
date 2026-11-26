//! What can go wrong for a user of the client library.
//!
//! # Why this file exists
//!
//! A database that puts its only copy of committed data into the log must know exactly what a failed call means. "It
//! failed" is not enough: after a failed append the record may be in the log or not, and the engine has to act
//! differently in each case. The design names the outcomes (section 5, "What an append result means"). This file makes
//! each of them a variant, so that a caller cannot treat an unknown outcome as a failure by accident.
//!
//! # What it contains
//!
//! - [`AppendError`]: why an append did not report success, and whether the record may be in the log anyway.
//! - [`OpenError`]: why a writer could not take a stream.
//! - [`ReadError`]: why a read failed, and where reading can resume.
//!
//! # How it works
//!
//! Every variant carries what a caller needs to act on it, in named fields: the epoch the stream is at now, the
//! low-water mark, the segment that could not be reached. Errors from the metadata store are passed through unchanged
//! in a `Metadata` variant, because the caller may want to tell a missing stream from a store that is down.
//!
//! # Invariants
//!
//! - An error that leaves the outcome unknown says so: [`AppendError::Fenced`] and [`AppendError::Indeterminate`]. The
//!   others guarantee that the record was not appended.
//! - The types are `Clone` and comparable, because one failure is reported to every append that was waiting.
//!
//! # See also
//!
//! - `writer.rs` and `writer/takeover.rs`: where [`AppendError`] and [`OpenError`] arise.
//! - `reader.rs`: where [`ReadError`] arises.
//! - `docs/protocol/replication.md`: the rules these outcomes come from.

use bitacora_meta::MetaError;
use bitacora_segment::format::EncodeError;
use bitacora_types::{Epoch, SegmentAddress, SequenceNumber, Timestamp};
use thiserror::Error;

/// Why an append did not report success (design, section 5, "What an append result means").
///
/// # Examples
///
/// What an engine does with each outcome:
///
/// ```
/// use bitacora_client::AppendError;
///
/// fn may_be_in_the_log(append_error: &AppendError) -> bool {
///     match append_error {
///         AppendError::Fenced | AppendError::Indeterminate { .. } => true,
///         AppendError::Closed | AppendError::Rejected(_) => false,
///     }
/// }
///
/// assert!(may_be_in_the_log(&AppendError::Fenced));
/// assert!(!may_be_in_the_log(&AppendError::Closed));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum AppendError {
    /// Another writer has taken the stream. This writer must stop. The record may or may not have been committed.
    #[error("another writer has taken the stream; the record may or may not be committed")]
    Fenced,
    /// The writer lost contact before learning the outcome. The record may or may not have been committed; the log
    /// decides, and the engine reads it back after recovery.
    #[error("the outcome of the append is unknown: {reason}")]
    Indeterminate {
        /// What interrupted the append.
        reason: String,
    },
    /// The writer was closed before the append was accepted. The record was not appended.
    #[error("the writer is closed; the record was not appended")]
    Closed,
    /// The record breaks a size limit. It was not appended.
    #[error("the record was not appended: {0}")]
    Rejected(#[from] EncodeError),
}

/// Why a writer could not be opened.
///
/// # Examples
///
/// ```
/// use bitacora_client::OpenError;
/// use bitacora_types::Epoch;
///
/// let lost_the_race = OpenError::TakenByAnotherWriter {
///     current_epoch: Epoch::new(8),
/// };
///
/// assert_eq!(
///     lost_the_race.to_string(),
///     "another writer claimed the stream; it is now at epoch 8"
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum OpenError {
    /// The metadata store refused or could not be reached.
    #[error(transparent)]
    Metadata(#[from] MetaError),
    /// Another candidate claimed the stream first. Whoever claims last wins, so the caller may simply try again if it
    /// still should be the writer.
    #[error("another writer claimed the stream; it is now at epoch {current_epoch}")]
    TakenByAnotherWriter {
        /// The stream's epoch after the other claim.
        current_epoch: Epoch,
    },
    /// A healthy writer still holds the lease and the caller chose to wait for it to lapse.
    #[error("writer {writer_identity:?} holds the stream until {expires_at}")]
    LeaseHeld {
        /// The writer that holds the lease.
        writer_identity: String,
        /// When its lease lapses unless renewed.
        expires_at: Timestamp,
    },
    /// Too few replicas of the previous writer's segment answered to guarantee that every committed record was found.
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
///
/// # Examples
///
/// ```
/// use bitacora_client::ReadError;
/// use bitacora_types::SequenceNumber;
///
/// let truncated = ReadError::Truncated {
///     low_water_mark: SequenceNumber::new(4_000),
/// };
///
/// // A reader that fell behind retention resumes from the mark.
/// let ReadError::Truncated { low_water_mark } = truncated else {
///     unreachable!("the error was built as a truncation");
/// };
/// assert_eq!(low_water_mark, SequenceNumber::new(4_000));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ReadError {
    /// The metadata store refused or could not be reached.
    #[error(transparent)]
    Metadata(#[from] MetaError),
    /// The requested records were deleted. Reading can resume at the low-water mark.
    #[error("records below {low_water_mark} have been truncated")]
    Truncated {
        /// The first sequence number that can still be read.
        low_water_mark: SequenceNumber,
    },
    /// Neither a replica nor the object store could serve the records.
    #[error("no replica of segment {segment} could serve the read: {reason}")]
    Unavailable {
        /// The segment that holds the requested records.
        segment: SegmentAddress,
        /// What the last source tried answered.
        reason: String,
    },
}
