//! The status codes of the C interface and how the client's errors become them.
//!
//! # Why this file exists
//!
//! A C caller cannot match on a Rust error. It gets one integer per call and has to decide from it what
//! to do next: stop writing, read the log to find out what happened, try again later, or fix its own
//! arguments. The Rust client has four error types with some twenty cases between them. This file is the
//! one place where each case is assigned the status a database needs to tell apart, so that the mapping
//! can be read, reviewed and tested as a whole.
//!
//! # What it contains
//!
//! - [`BitacoraStatus`]: the status codes. Their numbers are part of the ABI and never change.
//! - [`Failure`]: a status with the message that explains it, and the low-water mark when records were
//!   truncated.
//! - `From` implementations that turn [`AppendError`], [`OpenError`], [`ReadError`], [`ConnectError`] and
//!   [`MetaError`] into a [`Failure`].
//!
//! # How it works
//!
//! Every conversion is a `match` without a wildcard arm, and the lint that forbids one is an error in this
//! file. A variant added to any of the client's error types therefore stops this crate from compiling
//! until somebody decides which status it is.
//!
//! The message of a failure is the client's own error text. It is for people and logs; a program decides
//! on the status alone.
//!
//! ```
//! use bitacora::AppendError;
//! use bitacora_c::BitacoraStatus;
//!
//! // The numbers are what a C program sees, and what it may have stored or logged.
//! assert_eq!(BitacoraStatus::Ok as u32, 0);
//! assert_eq!(BitacoraStatus::Fenced as u32, 6);
//! # let _error_type_is_in_scope: Option<AppendError> = None;
//! ```
//!
//! # Invariants
//!
//! - A status keeps its number for ever. New statuses get new numbers at the end.
//! - Every case of every client error has exactly one status, decided here and nowhere else.
//! - [`BitacoraStatus::Ok`] is never the status of a [`Failure`].
//!
//! # See also
//!
//! - `guard.rs`: turns a [`Failure`] into the returned status and the thread's last error message.
//! - `docs/clients/c.md`: what a caller should do on each status.

#![deny(clippy::wildcard_enum_match_arm)]

use bitacora::{AppendError, ConnectError, OpenError, ReadError, SequenceNumber};
use bitacora_meta::MetaError;

/// What a call of the C interface resulted in.
///
/// Every function that can fail returns one of these. Anything other than `BITACORA_STATUS_OK` leaves a
/// message for people in `bitacora_last_error_message`. The numbers are stable: a status keeps its number
/// in every later version, and new ones are added at the end.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitacoraStatus {
    /// The call did what it was asked.
    Ok = 0,
    /// An argument cannot be used: a null pointer where one is required, text that is not UTF-8, a range
    /// that ends before it starts, a configuration that can never work, a name that is not allowed, a
    /// record that is too large. Nothing was done. Calling again with the same arguments fails again.
    InvalidArgument = 1,
    /// The stream, or the materializer, does not exist.
    NotFound = 2,
    /// A stream with this name exists already.
    AlreadyExists = 3,
    /// The cluster could not do it right now: the metadata store, the replicas or the object store could
    /// not be reached, or too few of them answered. For an append this status is never returned; an append
    /// whose outcome could not be learned is `BITACORA_STATUS_INDETERMINATE`. For every other call, nothing
    /// was changed or the change is safe to repeat: try again later.
    Unavailable = 4,
    /// The wait the caller allowed has passed. For an append this means the outcome is not known YET: the
    /// record keeps its place and may still be committed. Treat it like `BITACORA_STATUS_INDETERMINATE`.
    TimedOut = 5,
    /// Another writer has taken the stream. This writer must stop: every later append fails the same way.
    /// A record whose append returned this may or may not have been committed.
    Fenced = 6,
    /// The outcome of the append is unknown: the record may or may not have been committed. The log
    /// decides. Open a new writer, which recovers the stream, and read the log to find out.
    Indeterminate = 7,
    /// The records asked for were deleted. The call reports the low-water mark, the first sequence number
    /// that can still be read.
    Truncated = 8,
    /// Not an error: no record arrived within the wait the caller allowed. Call again to keep following.
    NothingNew = 9,
    /// The handle was closed, or the follower has ended after reporting why. Free it.
    Closed = 10,
    /// A healthy writer still holds the stream's lease and the caller chose to wait for it to lapse. Try
    /// again later, or take the stream by force if the old writer is known to be gone.
    LeaseHeld = 11,
    /// A defect in bitacora itself: a panic that was caught at the boundary, or a stored record that
    /// cannot be decoded. The handle the call was made on should be freed.
    Internal = 12,
}

/// Why a call did not return [`BitacoraStatus::Ok`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    /// The status the call returns. Never [`BitacoraStatus::Ok`].
    pub status: BitacoraStatus,
    /// What happened, for people. It becomes the thread's last error message.
    pub message: String,
    /// The first sequence number that can still be read, when the status is
    /// [`BitacoraStatus::Truncated`]. `None` for every other status.
    pub low_water_mark: Option<SequenceNumber>,
}

impl Failure {
    /// Builds a failure with `status` and the message that explains it.
    ///
    /// # Arguments
    ///
    /// * `status` - The status to return. Must not be [`BitacoraStatus::Ok`].
    /// * `message` - What happened, for people.
    ///
    /// # Returns
    ///
    /// The failure, without a low-water mark.
    pub(crate) fn new(status: BitacoraStatus, message: impl Into<String>) -> Self {
        debug_assert!(status != BitacoraStatus::Ok, "success is not a failure");
        Self {
            status,
            message: message.into(),
            low_water_mark: None,
        }
    }

    /// Builds the failure for an argument the caller got wrong.
    ///
    /// # Arguments
    ///
    /// * `message` - Which argument, and what is wrong with it.
    ///
    /// # Returns
    ///
    /// A failure with [`BitacoraStatus::InvalidArgument`].
    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(BitacoraStatus::InvalidArgument, message)
    }
}

impl From<AppendError> for Failure {
    /// Assigns each way an append can end without success its status.
    fn from(append_error: AppendError) -> Self {
        let status = match &append_error {
            AppendError::Fenced => BitacoraStatus::Fenced,
            AppendError::Indeterminate { reason: _ } => BitacoraStatus::Indeterminate,
            AppendError::Closed => BitacoraStatus::Closed,
            // The record breaks a size limit. It was not appended, and never will be as it is.
            AppendError::Rejected(_size_limit) => BitacoraStatus::InvalidArgument,
        };
        Self::new(status, append_error.to_string())
    }
}

impl From<OpenError> for Failure {
    /// Assigns each way opening a writer can fail its status.
    fn from(open_error: OpenError) -> Self {
        let message = open_error.to_string();
        let status = match open_error {
            OpenError::Metadata(meta_error) => return Self::from(meta_error),
            // Another candidate is the writer now, which is what "fenced" tells a writer.
            OpenError::TakenByAnotherWriter { current_epoch: _ } => BitacoraStatus::Fenced,
            OpenError::LeaseHeld {
                writer_identity: _,
                expires_at: _,
            } => BitacoraStatus::LeaseHeld,
            OpenError::RecoveryQuorumUnavailable {
                segment: _,
                needed: _,
                reached: _,
            }
            | OpenError::NoStandbySegment { stream: _ } => BitacoraStatus::Unavailable,
        };
        Self::new(status, message)
    }
}

impl From<ReadError> for Failure {
    /// Assigns each way a read can fail its status, keeping the low-water mark of a truncation.
    fn from(read_error: ReadError) -> Self {
        let message = read_error.to_string();
        match read_error {
            ReadError::Metadata(meta_error) => Self::from(meta_error),
            ReadError::Truncated { low_water_mark } => Self {
                status: BitacoraStatus::Truncated,
                message,
                low_water_mark: Some(low_water_mark),
            },
            ReadError::Unavailable { segment: _, reason: _ } => Self::new(BitacoraStatus::Unavailable, message),
        }
    }
}

impl From<ConnectError> for Failure {
    /// Assigns each reason a cluster configuration cannot be used its status.
    fn from(connect_error: ConnectError) -> Self {
        let status = match &connect_error {
            ConnectError::InvalidMetadataStore(_) | ConnectError::InvalidObjectStore(_) => {
                BitacoraStatus::InvalidArgument
            }
            // A directory that cannot be opened now may be there later: a disk that is not mounted yet.
            ConnectError::ObjectStore(_) => BitacoraStatus::Unavailable,
        };
        Self::new(status, connect_error.to_string())
    }
}

impl From<MetaError> for Failure {
    /// Assigns each thing the metadata store can refuse its status.
    fn from(meta_error: MetaError) -> Self {
        let status = match &meta_error {
            MetaError::Unavailable(_)
            | MetaError::NoStandbySegment { name: _ }
            | MetaError::TooMuchContention { key: _, attempts: _ } => BitacoraStatus::Unavailable,
            MetaError::StreamNotFound { name: _ }
            | MetaError::MaterializerNotRegistered {
                stream_id: _,
                materializer: _,
            } => BitacoraStatus::NotFound,
            MetaError::StreamAlreadyExists { name: _ } => BitacoraStatus::AlreadyExists,
            MetaError::InvalidName { name: _ } | MetaError::InvalidState { reason: _ } => {
                BitacoraStatus::InvalidArgument
            }
            MetaError::Fenced {
                held_epoch: _,
                current_epoch: _,
            }
            | MetaError::MaterializerFenced {
                held_token: _,
                current_token: _,
            } => BitacoraStatus::Fenced,
            MetaError::CorruptRecord { key: _, reason: _ } => BitacoraStatus::Internal,
        };
        Self::new(status, meta_error.to_string())
    }
}

#[cfg(test)]
#[path = "status_test.rs"]
mod tests;
