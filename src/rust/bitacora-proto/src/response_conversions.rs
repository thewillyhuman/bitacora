//! The answer of every call, to and from its message.
//!
//! # Why this file exists
//!
//! A replica answers every call with either a result or a `ReplicaError`,
//! and both must reach the caller unchanged. In the protocol each response
//! message therefore has an `outcome` that is either `success` or `error`.
//! This file writes a replica's answer into that shape, on the server, and
//! reads it back out, on the client.
//!
//! # What it contains
//!
//! Two functions per call, named after it:
//!
//! - `<call>_response_to_wire`: the replica's `Result` as a message.
//! - `<call>_response_from_wire`: the message as the `Result` the caller
//!   of `ReplicaApi` expects.
//!
//! And the rule both readers of an answer share:
//!
//! - `unreadable_answer`: the error for an answer that cannot be read.
//! - `replica_error_from_wire`: the error half of an outcome, read under
//!   that rule.
//!
//! # How it works
//!
//! Writing cannot fail. Reading can meet three things:
//!
//! - `success`: the fields are converted to the domain response.
//! - `error`: read by `replica_error_from_wire`.
//! - Something unreadable: no outcome at all, a field that fails a check,
//!   or an error of a kind this version does not know. The protocol says a
//!   client treats that as unavailability, the one error that promises
//!   nothing about what the replica did. So reading returns
//!   `ReplicaError::Unavailable` through `unreadable_answer`. This is why
//!   reading returns the caller's `Result` directly and has no error type
//!   of its own.
//!
//! The functions are written out one by one. They are alike, but each has
//! its own message types, and a reader can check any of them without
//! knowing the others.
//!
//! # Invariants
//!
//! - `result -> message -> result` is the identity, for a success and for
//!   every error.
//! - An unreadable answer is never read as a success, and never as an
//!   error that tells the caller the request was not applied.
//!
//! # See also
//!
//! - `error_conversions.rs` for the strict conversion of `ReplicaError`.
//! - `request_conversions.rs` for the other half of every call.

use bitacora_types::SequenceNumber;
use bitacora_types::replica_api as domain;
use bitacora_types::replica_api::ReplicaError;

use crate::conversions::{MalformedMessage, optional_sequence, replica_id_from_wire};
use crate::v1 as wire;
use crate::v1::append_response::{Outcome as AppendOutcome, Success as AppendSuccess};
use crate::v1::delete_segment_response::{Outcome as DeleteSegmentOutcome, Success as DeleteSegmentSuccess};
use crate::v1::drain_response::{Outcome as DrainOutcome, Success as DrainSuccess};
use crate::v1::fence_response::{Outcome as FenceOutcome, Success as FenceSuccess};
use crate::v1::list_segments_response::{Outcome as ListSegmentsOutcome, Success as ListSegmentsSuccess};
use crate::v1::read_response::{Outcome as ReadOutcome, Success as ReadSuccess};
use crate::v1::repair_segment_response::{Outcome as RepairSegmentOutcome, Success as RepairSegmentSuccess};
use crate::v1::seal_response::{Outcome as SealOutcome, Success as SealSuccess};
use crate::v1::status_response::{Outcome as StatusOutcome, Success as StatusSuccess};
use crate::v1::upload_segment_response::{Outcome as UploadSegmentOutcome, Success as UploadSegmentSuccess};

/// The error a client reports for an answer it cannot read.
///
/// The replica did answer, so it may well have applied the request; the
/// client just cannot tell. `Unavailable` is the only error that says so,
/// and the only one a caller answers by asking again.
///
/// # Arguments
///
/// * `malformed_message` - Why the answer could not be read.
///
/// # Returns
///
/// `ReplicaError::Unavailable`, with a reason that names the field at
/// fault.
fn unreadable_answer(malformed_message: &MalformedMessage) -> ReplicaError {
    ReplicaError::Unavailable {
        reason: format!("the replica's answer cannot be read: {malformed_message}"),
    }
}

/// The error for a response message that arrived without an outcome.
///
/// # Arguments
///
/// * `field` - The outcome field, as `Message.outcome`.
fn missing_outcome(field: &'static str) -> ReplicaError {
    unreadable_answer(&MalformedMessage::MissingField { field })
}

/// Reads the error in a replica's answer the way a client must: an error
/// that cannot be read is unavailability.
///
/// # Arguments
///
/// * `error` - The error message found in a response.
///
/// # Returns
///
/// The replica's error, exactly as it returned it. If the message is
/// malformed or of a kind this version does not know,
/// `ReplicaError::Unavailable` with the reason it could not be read.
fn replica_error_from_wire(error: wire::ReplicaError) -> ReplicaError {
    match ReplicaError::try_from(error) {
        Ok(replica_error) => replica_error,
        Err(malformed_message) => unreadable_answer(&malformed_message),
    }
}

/// Writes a replica's answer to an append as its message.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::append` returned.
pub(crate) fn append_response_to_wire(result: Result<domain::AppendResponse, ReplicaError>) -> wire::AppendResponse {
    let outcome = match result {
        Ok(response) => AppendOutcome::Success(AppendSuccess {
            local_end: response.local_end.get(),
        }),
        Err(error) => AppendOutcome::Error(error.into()),
    };
    wire::AppendResponse { outcome: Some(outcome) }
}

/// Reads a replica's answer to an append.
///
/// # Errors
///
/// Returns the replica's own error if it refused the append, and
/// `ReplicaError::Unavailable` if the answer cannot be read.
pub(crate) fn append_response_from_wire(
    response: wire::AppendResponse,
) -> Result<domain::AppendResponse, ReplicaError> {
    match response.outcome {
        Some(AppendOutcome::Success(success)) => Ok(domain::AppendResponse {
            local_end: SequenceNumber::new(success.local_end),
        }),
        Some(AppendOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("AppendResponse.outcome")),
    }
}

/// Writes a replica's answer to a read as its message. The batch frames
/// are shared with the domain response, not copied.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::read` returned.
pub(crate) fn read_response_to_wire(result: Result<domain::ReadResponse, ReplicaError>) -> wire::ReadResponse {
    let outcome = match result {
        Ok(response) => ReadOutcome::Success(ReadSuccess {
            batch_frames: response.batch_frames,
            readable_end: response.readable_end.get(),
            sealed_end: response.sealed_end.map(SequenceNumber::get),
        }),
        Err(error) => ReadOutcome::Error(error.into()),
    };
    wire::ReadResponse { outcome: Some(outcome) }
}

/// Reads a replica's answer to a read. The batch frames are not looked at:
/// the reader verifies them.
///
/// # Errors
///
/// Returns the replica's own error if it refused the read, and
/// `ReplicaError::Unavailable` if the answer cannot be read.
pub(crate) fn read_response_from_wire(response: wire::ReadResponse) -> Result<domain::ReadResponse, ReplicaError> {
    match response.outcome {
        Some(ReadOutcome::Success(success)) => Ok(domain::ReadResponse {
            batch_frames: success.batch_frames,
            readable_end: SequenceNumber::new(success.readable_end),
            sealed_end: optional_sequence(success.sealed_end),
        }),
        Some(ReadOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("ReadResponse.outcome")),
    }
}

/// Writes a replica's answer to a fence as its message.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::fence` returned.
pub(crate) fn fence_response_to_wire(result: Result<domain::FenceResponse, ReplicaError>) -> wire::FenceResponse {
    let outcome = match result {
        Ok(response) => FenceOutcome::Success(FenceSuccess {
            local_end: response.local_end.map(SequenceNumber::get),
            sealed_end: response.sealed_end.map(SequenceNumber::get),
        }),
        Err(error) => FenceOutcome::Error(error.into()),
    };
    wire::FenceResponse { outcome: Some(outcome) }
}

/// Reads a replica's answer to a fence.
///
/// # Errors
///
/// Returns the replica's own error if it refused the fence, and
/// `ReplicaError::Unavailable` if the answer cannot be read.
pub(crate) fn fence_response_from_wire(response: wire::FenceResponse) -> Result<domain::FenceResponse, ReplicaError> {
    match response.outcome {
        Some(FenceOutcome::Success(success)) => Ok(domain::FenceResponse {
            local_end: optional_sequence(success.local_end),
            sealed_end: optional_sequence(success.sealed_end),
        }),
        Some(FenceOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("FenceResponse.outcome")),
    }
}

/// Writes a replica's answer to a seal as its message.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::seal` returned.
pub(crate) fn seal_response_to_wire(result: Result<domain::SealResponse, ReplicaError>) -> wire::SealResponse {
    let outcome = match result {
        Ok(response) => SealOutcome::Success(SealSuccess {
            local_end: response.local_end.map(SequenceNumber::get),
        }),
        Err(error) => SealOutcome::Error(error.into()),
    };
    wire::SealResponse { outcome: Some(outcome) }
}

/// Reads a replica's answer to a seal.
///
/// # Errors
///
/// Returns the replica's own error if it refused the seal, and
/// `ReplicaError::Unavailable` if the answer cannot be read.
pub(crate) fn seal_response_from_wire(response: wire::SealResponse) -> Result<domain::SealResponse, ReplicaError> {
    match response.outcome {
        Some(SealOutcome::Success(success)) => Ok(domain::SealResponse {
            local_end: optional_sequence(success.local_end),
        }),
        Some(SealOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("SealResponse.outcome")),
    }
}

/// Writes a replica's answer to a repair as its message.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::repair_segment` returned.
pub(crate) fn repair_segment_response_to_wire(
    result: Result<domain::RepairResponse, ReplicaError>,
) -> wire::RepairSegmentResponse {
    let outcome = match result {
        Ok(response) => RepairSegmentOutcome::Success(RepairSegmentSuccess {
            local_end: response.local_end.get(),
        }),
        Err(error) => RepairSegmentOutcome::Error(error.into()),
    };
    wire::RepairSegmentResponse { outcome: Some(outcome) }
}

/// Reads a replica's answer to a repair.
///
/// # Errors
///
/// Returns the replica's own error if the repair failed, and
/// `ReplicaError::Unavailable` if the answer cannot be read.
pub(crate) fn repair_segment_response_from_wire(
    response: wire::RepairSegmentResponse,
) -> Result<domain::RepairResponse, ReplicaError> {
    match response.outcome {
        Some(RepairSegmentOutcome::Success(success)) => Ok(domain::RepairResponse {
            local_end: SequenceNumber::new(success.local_end),
        }),
        Some(RepairSegmentOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("RepairSegmentResponse.outcome")),
    }
}

/// Writes a replica's answer to an upload as its message.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::upload_segment` returned.
pub(crate) fn upload_segment_response_to_wire(
    result: Result<domain::UploadResponse, ReplicaError>,
) -> wire::UploadSegmentResponse {
    let outcome = match result {
        Ok(response) => UploadSegmentOutcome::Success(UploadSegmentSuccess {
            size_bytes: response.size_bytes,
        }),
        Err(error) => UploadSegmentOutcome::Error(error.into()),
    };
    wire::UploadSegmentResponse { outcome: Some(outcome) }
}

/// Reads a replica's answer to an upload.
///
/// # Errors
///
/// Returns the replica's own error if the upload failed, and
/// `ReplicaError::Unavailable` if the answer cannot be read.
pub(crate) fn upload_segment_response_from_wire(
    response: wire::UploadSegmentResponse,
) -> Result<domain::UploadResponse, ReplicaError> {
    match response.outcome {
        Some(UploadSegmentOutcome::Success(success)) => Ok(domain::UploadResponse {
            size_bytes: success.size_bytes,
        }),
        Some(UploadSegmentOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("UploadSegmentResponse.outcome")),
    }
}

/// Writes a replica's report about itself as its message.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::status` returned.
pub(crate) fn status_response_to_wire(result: Result<domain::ReplicaStatus, ReplicaError>) -> wire::StatusResponse {
    let outcome = match result {
        Ok(status) => StatusOutcome::Success(StatusSuccess {
            replica_id: status.replica_id.into(),
            capacity_bytes: status.capacity_bytes,
            free_bytes: status.free_bytes,
            is_draining: status.is_draining,
            segment_count: status.segment_count,
        }),
        Err(error) => StatusOutcome::Error(error.into()),
    };
    wire::StatusResponse { outcome: Some(outcome) }
}

/// Reads a replica's report about itself.
///
/// # Errors
///
/// Returns the replica's own error if it could not report, and
/// `ReplicaError::Unavailable` if the answer cannot be read, which includes
/// a report under something that is not a replica identifier.
pub(crate) fn status_response_from_wire(response: wire::StatusResponse) -> Result<domain::ReplicaStatus, ReplicaError> {
    match response.outcome {
        Some(StatusOutcome::Success(success)) => {
            replica_status_from_wire(success).map_err(|malformed| unreadable_answer(&malformed))
        }
        Some(StatusOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("StatusResponse.outcome")),
    }
}

/// Reads the fields of a replica's report about itself.
///
/// # Errors
///
/// Returns [`MalformedMessage::InvalidReplicaId`] if the report's identity
/// is not a replica identifier.
fn replica_status_from_wire(success: StatusSuccess) -> Result<domain::ReplicaStatus, MalformedMessage> {
    let replica_id = replica_id_from_wire(success.replica_id, "StatusResponse.Success.replica_id")?;
    Ok(domain::ReplicaStatus {
        replica_id,
        capacity_bytes: success.capacity_bytes,
        free_bytes: success.free_bytes,
        is_draining: success.is_draining,
        segment_count: success.segment_count,
    })
}

/// Writes a replica's answer to a drain as its message.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::drain` returned.
pub(crate) fn drain_response_to_wire(result: Result<(), ReplicaError>) -> wire::DrainResponse {
    let outcome = match result {
        Ok(()) => DrainOutcome::Success(DrainSuccess {}),
        Err(error) => DrainOutcome::Error(error.into()),
    };
    wire::DrainResponse { outcome: Some(outcome) }
}

/// Reads a replica's answer to a drain.
///
/// # Errors
///
/// Returns the replica's own error if it could not start draining, and
/// `ReplicaError::Unavailable` if the answer cannot be read.
pub(crate) fn drain_response_from_wire(response: wire::DrainResponse) -> Result<(), ReplicaError> {
    match response.outcome {
        Some(DrainOutcome::Success(DrainSuccess {})) => Ok(()),
        Some(DrainOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("DrainResponse.outcome")),
    }
}

/// Writes a replica's list of segment copies as its message.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::list_segments` returned.
pub(crate) fn list_segments_response_to_wire(
    result: Result<Vec<domain::SegmentStatus>, ReplicaError>,
) -> wire::ListSegmentsResponse {
    let outcome = match result {
        Ok(statuses) => ListSegmentsOutcome::Success(ListSegmentsSuccess {
            segments: statuses.into_iter().map(Into::into).collect(),
        }),
        Err(error) => ListSegmentsOutcome::Error(error.into()),
    };
    wire::ListSegmentsResponse { outcome: Some(outcome) }
}

/// Reads a replica's list of segment copies, in the order it sent them.
///
/// # Errors
///
/// Returns the replica's own error if it could not list its copies, and
/// `ReplicaError::Unavailable` if the answer cannot be read. One unreadable
/// entry makes the whole list unreadable: a list with a hole would look
/// like a replica that lost a copy.
pub(crate) fn list_segments_response_from_wire(
    response: wire::ListSegmentsResponse,
) -> Result<Vec<domain::SegmentStatus>, ReplicaError> {
    match response.outcome {
        Some(ListSegmentsOutcome::Success(success)) => {
            segment_statuses_from_wire(success.segments).map_err(|malformed| unreadable_answer(&malformed))
        }
        Some(ListSegmentsOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("ListSegmentsResponse.outcome")),
    }
}

/// Reads every entry of a list of segment copies, keeping their order.
///
/// # Errors
///
/// Returns the [`MalformedMessage`] of the first entry that cannot be read.
fn segment_statuses_from_wire(
    segments: Vec<wire::SegmentStatus>,
) -> Result<Vec<domain::SegmentStatus>, MalformedMessage> {
    let mut statuses = Vec::with_capacity(segments.len());
    for segment in segments {
        statuses.push(domain::SegmentStatus::try_from(segment)?);
    }
    Ok(statuses)
}

/// Writes a replica's answer to the deletion of a segment copy as its
/// message.
///
/// # Arguments
///
/// * `result` - What `ReplicaApi::delete_segment` returned.
pub(crate) fn delete_segment_response_to_wire(result: Result<(), ReplicaError>) -> wire::DeleteSegmentResponse {
    let outcome = match result {
        Ok(()) => DeleteSegmentOutcome::Success(DeleteSegmentSuccess {}),
        Err(error) => DeleteSegmentOutcome::Error(error.into()),
    };
    wire::DeleteSegmentResponse { outcome: Some(outcome) }
}

/// Reads a replica's answer to the deletion of a segment copy.
///
/// # Errors
///
/// Returns the replica's own error if it could not delete the copy, and
/// `ReplicaError::Unavailable` if the answer cannot be read.
pub(crate) fn delete_segment_response_from_wire(response: wire::DeleteSegmentResponse) -> Result<(), ReplicaError> {
    match response.outcome {
        Some(DeleteSegmentOutcome::Success(DeleteSegmentSuccess {})) => Ok(()),
        Some(DeleteSegmentOutcome::Error(error)) => Err(replica_error_from_wire(error)),
        None => Err(missing_outcome("DeleteSegmentResponse.outcome")),
    }
}

#[cfg(test)]
#[path = "response_conversions_test.rs"]
mod tests;
