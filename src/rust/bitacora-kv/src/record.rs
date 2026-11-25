//! The log records of bitacora-kv.
//!
//! # Why this file exists
//!
//! Every change to the key-value store is one record of its stream (design, section 12). The bytes of
//! that record are a format that outlives any one process: the materializer of next year must read the
//! records of today, and the simulator must be able to say which payload a put produces. This file is
//! the one place that defines them.
//!
//! # What it contains
//!
//! - [`KvOperation`]: one put or one delete.
//! - [`KvRecord`]: the operations of one log record, applied together or not at all.
//! - [`KvRecordError`]: why a payload is not a record.
//!
//! # How it works
//!
//! All integers are little-endian.
//!
//! ```text
//! record     format version u8 | operation count u32 | operation*
//! operation  kind u8 | key length u32 | key | value length u32 | value
//! ```
//!
//! The kind is 1 for a put and 2 for a delete; a delete has no value length and no value. A record with
//! no operation is valid and changes nothing: it is the barrier a linearizable read appends (see
//! `store.rs`). There is no checksum, because the log already checksums every batch it stores.
//!
//! # Invariants
//!
//! - Encoding is deterministic: equal records encode to equal bytes.
//! - Decoding accepts exactly what encoding produces. A payload with a format version this code does not
//!   know is refused, never guessed at.
//!
//! # See also
//!
//! - `pages.rs`: how a record changes the bucket pages.
//! - `store.rs`: where records are appended.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// The record format this code reads and writes.
const RECORD_FORMAT_VERSION: u8 = 1;

/// The stored kind of a put.
const KIND_PUT: u8 = 1;

/// The stored kind of a delete.
const KIND_DELETE: u8 = 2;

/// Why a log record's payload is not a bitacora-kv record.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum KvRecordError {
    /// The payload was written by a format this code does not know.
    #[error("record format version {found} is not supported")]
    UnsupportedVersion {
        /// The version found in the payload.
        found: u8,
    },
    /// The payload ends before the record does, holds an unknown kind of operation, or goes on after the
    /// record ended.
    #[error("record is malformed: {reason}")]
    Malformed {
        /// What is wrong with it.
        reason: &'static str,
    },
}

/// One change to one key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvOperation {
    /// Sets the value of a key, whether or not it had one.
    Put {
        /// The key.
        key: Bytes,
        /// Its new value. An empty value is a value: the key exists afterwards.
        value: Bytes,
    },
    /// Removes a key. Removing a key that does not exist changes nothing.
    Delete {
        /// The key.
        key: Bytes,
    },
}

impl KvOperation {
    /// The key the operation changes.
    #[must_use]
    pub const fn key(&self) -> &Bytes {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// The operations of one log record. They are applied in order, all at the same sequence number.
///
/// # Examples
///
/// ```
/// use bitacora_kv::{KvOperation, KvRecord};
/// use bytes::Bytes;
///
/// let record = KvRecord {
///     operations: vec![
///         KvOperation::Put { key: Bytes::from_static(b"colour"), value: Bytes::from_static(b"red") },
///         KvOperation::Delete { key: Bytes::from_static(b"shape") },
///     ],
/// };
/// let payload = record.encode();
/// assert_eq!(KvRecord::decode(&payload), Ok(record));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvRecord {
    /// The changes, in the order they are applied.
    pub operations: Vec<KvOperation>,
}

impl KvRecord {
    /// A record that changes nothing. A linearizable read appends one to learn a position in the log
    /// that is current.
    #[must_use]
    pub const fn barrier() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// A record that sets one key.
    #[must_use]
    pub fn put(key: Bytes, value: Bytes) -> Self {
        Self {
            operations: vec![KvOperation::Put { key, value }],
        }
    }

    /// A record that removes one key.
    #[must_use]
    pub fn delete(key: Bytes) -> Self {
        Self {
            operations: vec![KvOperation::Delete { key }],
        }
    }

    /// The payload of the log record that carries these operations.
    ///
    /// # Panics
    ///
    /// Panics if the record has 2^32 operations or more, or a key or value of 4 GiB or more. The log
    /// refuses records far smaller than that.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut payload = BytesMut::new();
        payload.put_u8(RECORD_FORMAT_VERSION);
        payload.put_u32_le(length_as_u32(self.operations.len()));
        for operation in &self.operations {
            match operation {
                KvOperation::Put { key, value } => {
                    payload.put_u8(KIND_PUT);
                    payload.put_u32_le(length_as_u32(key.len()));
                    payload.put_slice(key);
                    payload.put_u32_le(length_as_u32(value.len()));
                    payload.put_slice(value);
                }
                KvOperation::Delete { key } => {
                    payload.put_u8(KIND_DELETE);
                    payload.put_u32_le(length_as_u32(key.len()));
                    payload.put_slice(key);
                }
            }
        }
        payload.freeze()
    }

    /// Reads the operations out of a log record's payload.
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload of a record of a bitacora-kv stream. Keys and values of the result
    ///   share its memory.
    ///
    /// # Errors
    ///
    /// Returns [`KvRecordError`] if the payload was not produced by [`KvRecord::encode`] of this or an
    /// older format version.
    pub fn decode(payload: &Bytes) -> Result<Self, KvRecordError> {
        let mut unread = payload.clone();
        let version = unread
            .try_get_u8()
            .map_err(|_cut_short| malformed("it is empty"))?;
        if version != RECORD_FORMAT_VERSION {
            return Err(KvRecordError::UnsupportedVersion { found: version });
        }
        let operation_count = unread
            .try_get_u32_le()
            .map_err(|_cut_short| malformed("it is cut short"))?;
        let mut operations = Vec::new();
        for _operation in 0..operation_count {
            let kind = unread
                .try_get_u8()
                .map_err(|_cut_short| malformed("it is cut short"))?;
            let key = read_length_prefixed(&mut unread)?;
            let operation = match kind {
                KIND_PUT => KvOperation::Put {
                    key,
                    value: read_length_prefixed(&mut unread)?,
                },
                KIND_DELETE => KvOperation::Delete { key },
                _unknown => return Err(malformed("it holds an unknown kind of operation")),
            };
            operations.push(operation);
        }
        if unread.has_remaining() {
            return Err(malformed("it goes on after its last operation"));
        }
        Ok(Self { operations })
    }
}

/// The error for a payload that is not a well-formed record.
const fn malformed(reason: &'static str) -> KvRecordError {
    KvRecordError::Malformed { reason }
}

/// A length as the four bytes it is stored in.
fn length_as_u32(length: usize) -> u32 {
    u32::try_from(length).expect("a part of a record is smaller than 4 GiB")
}

/// Reads a length and then that many bytes, or fails if the payload ends first.
fn read_length_prefixed(unread: &mut Bytes) -> Result<Bytes, KvRecordError> {
    let length = unread
        .try_get_u32_le()
        .map_err(|_cut_short| malformed("it is cut short"))? as usize;
    if unread.remaining() < length {
        return Err(malformed("it is cut short"));
    }
    Ok(unread.split_to(length))
}

#[cfg(test)]
#[path = "record_test.rs"]
mod tests;
