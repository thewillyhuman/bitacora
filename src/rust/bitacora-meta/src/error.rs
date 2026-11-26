//! Why an operation on the metadata store failed.
//!
//! # Why this file exists
//!
//! Callers of the metadata store do different things for different failures, and some of those differences are what
//! keeps the log safe: a writer that is told it was fenced must stop for good, while one that is told the store was
//! unavailable must try again. The failures therefore need one precise vocabulary, shared by every operation, that a
//! caller can match on. This file is that vocabulary.
//!
//! # What it contains
//!
//! - [`MetaError`]: every way an operation of [`crate::MetadataStore`] fails.
//!
//! # How it works
//!
//! Each variant is a situation a caller reacts to differently, and carries the values needed to act on it or to explain
//! it in a log line. Three kinds are worth telling apart:
//!
//! - **Stop.** [`MetaError::Fenced`] and [`MetaError::MaterializerFenced`]: somebody else has taken over. Retrying
//!   cannot help and must not be attempted.
//! - **Try again.** [`MetaError::Unavailable`] and [`MetaError::TooMuchContention`]: the store could not do it right
//!   now. Every operation is safe to repeat.
//! - **Wrong request.** Everything else: what was asked for contradicts what the store holds, and the same request will
//!   fail the same way until something else changes.
//!
//! # Invariants
//!
//! - After [`MetaError::Unavailable`] the operation may or may not have been applied.
//! - After every other error the operation was not applied.
//!
//! # See also
//!
//! - `key_value.rs`: [`StoreUnavailable`], the one failure of a backend.
//! - `store.rs` and the files under `store/`: which operation returns which error.

use bitacora_types::{Epoch, StreamId};
use thiserror::Error;

use crate::key_value::StoreUnavailable;
use crate::keys::MAX_NAME_LENGTH_BYTES;

/// Why a metadata operation failed.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use std::sync::Arc;
///
/// use bitacora_meta::{MemoryKeyValueStore, MetaError, MetadataStore};
///
/// let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
///
/// let missing = metadata.get_stream("orders").await;
///
/// assert_eq!(
///     missing,
///     Err(MetaError::StreamNotFound {
///         name: "orders".to_owned()
///     })
/// );
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum MetaError {
    /// The store could not be reached. The operation may or may not have been applied; every operation is safe to
    /// repeat.
    #[error(transparent)]
    Unavailable(#[from] StoreUnavailable),
    /// No stream has this name.
    #[error("stream {name:?} does not exist")]
    StreamNotFound {
        /// The name that was looked up.
        name: String,
    },
    /// A stream with this name exists already.
    #[error("stream {name:?} already exists")]
    StreamAlreadyExists {
        /// The name that is taken.
        name: String,
    },
    /// A stream or materializer name cannot be used in a key.
    #[error("invalid name {name:?}: use 1 to {MAX_NAME_LENGTH_BYTES} ASCII letters, digits, '.', '_' or '-'")]
    InvalidName {
        /// The rejected name.
        name: String,
    },
    /// Another writer has taken the stream. The caller must stop.
    #[error("epoch {held_epoch} was superseded by epoch {current_epoch}")]
    Fenced {
        /// The epoch the caller holds.
        held_epoch: Epoch,
        /// The stream's current epoch.
        current_epoch: Epoch,
    },
    /// The controller has not pre-allocated a replica set for the next segment yet.
    #[error("stream {name:?} has no standby segment to roll into")]
    NoStandbySegment {
        /// The stream that cannot roll.
        name: String,
    },
    /// The request contradicts what the store holds.
    #[error("invalid metadata operation: {reason}")]
    InvalidState {
        /// What the contradiction is.
        reason: String,
    },
    /// The materializer is not registered on the stream.
    #[error("materializer {materializer:?} is not registered on stream {stream_id}")]
    MaterializerNotRegistered {
        /// The stream that was looked at.
        stream_id: StreamId,
        /// The materializer that was looked up.
        materializer: String,
    },
    /// Another runner has taken over the materializer. The caller must stop.
    #[error("materializer fencing token {held_token} was superseded by {current_token}")]
    MaterializerFenced {
        /// The token the caller holds.
        held_token: u64,
        /// The materializer's current token.
        current_token: u64,
    },
    /// A stored record cannot be decoded.
    #[error("record at {key:?} cannot be decoded: {reason}")]
    CorruptRecord {
        /// The key of the record.
        key: String,
        /// Why decoding failed.
        reason: String,
    },
    /// Other writers changed the same key on every attempt.
    #[error("gave up updating {key:?} after {attempts} conflicting writes")]
    TooMuchContention {
        /// The contended key.
        key: String,
        /// How many attempts were made.
        attempts: u32,
    },
}
