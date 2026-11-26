//! How an artifact catalog is kept in the details of a checkpoint manifest.
//!
//! # Why this file exists
//!
//! The catalog is the only record of which artifact covers which part of the log, and the log below a checkpoint
//! may be gone. The next instance of the materializer restores from the stored catalog, and so does every process
//! that only queries the artifacts and never runs the materializer. A stored catalog that is read wrongly, or that
//! claims a range no artifact covers, loses data silently. This file defines the bytes and refuses everything else.
//!
//! # What it contains
//!
//! - [`StoredCatalog`]: a catalog as a checkpoint stored it, with the saved pending records if there are any.
//! - [`encode_catalog`] and [`decode_catalog`]: the stored form and its reverse, with every check.
//!
//! # How it works
//!
//! All integers are little-endian.
//!
//! ```text
//! details  format version u32 | entry count u32 | entry* | pending flag u8 | entry if the flag is 1
//! entry    first sequence u64 | last sequence u64 | size u64 | key length u32 | key
//!          | summary length u32 | summary
//! ```
//!
//! The entries are the sealed artifacts in log order. The entry after the flag, if there is one, is the artifact
//! that holds the records the builder had not sealed at the checkpoint.
//!
//! The manifest that carries the details is checksummed as a whole, so decoding does not have to find flipped bits.
//! It refuses what this version of the code did not write, and it checks the one property everything else relies
//! on: the entries, and the pending records after them, follow each other without gap or overlap.
//!
//! # Invariants
//!
//! - Pure functions over bytes: no I/O.
//! - Decoding never panics, whatever the bytes, and accepts only what encoding produces.
//! - A decoded catalog is gapless: each entry starts right after the one before it, and saved pending records start
//!   right after the last entry.
//!
//! # See also
//!
//! - `docs/protocol/checkpoint-manifest.md`: the same layout, for readers in other languages. Change both together.
//! - `artifact_catalog.rs`: the adapter that writes a catalog at every checkpoint and restores from it.
//! - `committed.rs`: how a process that is not the runner gets the manifest whose details these are.

use bitacora_types::{SequenceNumber, SequenceRange};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{ArtifactDescriptor, CatalogEntry};
use crate::contract::MaterializerError;

/// The format of the details an artifact catalog puts in its checkpoint manifest.
const DETAILS_FORMAT_VERSION: u32 = 3;

/// The flag of a stored catalog that holds no saved pending records.
const NO_PENDING_RECORDS: u8 = 0;

/// The flag of a stored catalog in which one more entry follows: the saved pending records.
const PENDING_RECORDS_FOLLOW: u8 = 1;

/// What is wrong with a catalog that ends before its lengths say.
const CUT_SHORT: &str = "it is cut short";

/// A catalog as a checkpoint stored it.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::StoredCatalog;
///
/// let never_checkpointed = StoredCatalog::default();
/// assert!(never_checkpointed.entries.is_empty());
/// assert_eq!(never_checkpointed.pending, None);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoredCatalog {
    /// The sealed artifacts, in log order: gapless and without overlap.
    pub entries: Vec<CatalogEntry>,
    /// The artifact that holds the records the builder had not sealed at the checkpoint, and those
    /// records, if the builder saves its pending records and held any. They follow the last sealed
    /// artifact and end at the checkpoint. Only the builder's own readers understand the artifact.
    pub pending: Option<CatalogEntry>,
}

/// Serializes a catalog, and the saved pending records if there are any, as the details of a checkpoint
/// manifest.
///
/// # Arguments
///
/// * `entries` - The sealed artifacts, in log order.
/// * `pending` - The artifact the builder's pending records were saved to, if it saved any.
///
/// # Returns
///
/// The details. [`decode_catalog`] turns them back.
///
/// # Panics
///
/// Panics if the catalog has 2^32 entries or more, or an artifact key or a summary of 4 GiB or more.
pub(super) fn encode_catalog(entries: &[CatalogEntry], pending: Option<&CatalogEntry>) -> Bytes {
    let mut details = BytesMut::new();
    details.put_u32_le(DETAILS_FORMAT_VERSION);
    details.put_u32_le(u32::try_from(entries.len()).expect("far fewer than 2^32 artifacts"));
    for entry in entries {
        put_entry(&mut details, entry);
    }
    match pending {
        Some(pending) => {
            details.put_u8(PENDING_RECORDS_FOLLOW);
            put_entry(&mut details, pending);
        }
        None => details.put_u8(NO_PENDING_RECORDS),
    }
    details.freeze()
}

/// Appends one entry to the stored form of a catalog.
fn put_entry(details: &mut BytesMut, entry: &CatalogEntry) {
    details.put_u64_le(entry.covered_range.first().get());
    details.put_u64_le(entry.covered_range.last().get());
    details.put_u64_le(entry.artifact.size_bytes);
    details.put_u32_le(u32::try_from(entry.artifact.artifact_key.len()).expect("an artifact key is short"));
    details.put_slice(entry.artifact.artifact_key.as_bytes());
    details.put_u32_le(u32::try_from(entry.artifact.summary.len()).expect("a summary is small"));
    details.put_slice(&entry.artifact.summary);
}

/// Reads the catalog out of the details of a checkpoint manifest written by an `ArtifactCatalog`.
///
/// A process that only queries the artifacts does not run the materializer. It reads the committed
/// manifest (see [`read_committed_checkpoint`](crate::read_committed_checkpoint)) and passes its details here.
///
/// # Arguments
///
/// * `details` - `CheckpointManifest::details` of a manifest an artifact catalog wrote.
///
/// # Returns
///
/// The catalog as of that checkpoint, and the builder's saved pending records if it saved any.
///
/// # Errors
///
/// Returns [`MaterializerError::InvalidManifest`] if the bytes were not written by this version of the
/// catalog, or describe artifacts that leave a gap or overlap.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::decode_catalog;
/// use bytes::Bytes;
///
/// let not_a_catalog = Bytes::from_static(b"something else");
/// assert!(decode_catalog(&not_a_catalog).is_err());
/// ```
pub fn decode_catalog(details: &Bytes) -> Result<StoredCatalog, MaterializerError> {
    match read_catalog(details.clone()) {
        Ok(stored) => Ok(stored),
        Err(reason) => Err(MaterializerError::InvalidManifest {
            reason: format!("the artifact catalog is not valid: {reason}"),
        }),
    }
}

/// Reads a whole catalog out of `unread` and checks that it is gapless, or says what is wrong with it.
fn read_catalog(mut unread: Bytes) -> Result<StoredCatalog, &'static str> {
    if take_u32(&mut unread)? != DETAILS_FORMAT_VERSION {
        return Err("it was written by another format");
    }
    let entry_count = take_u32(&mut unread)?;
    let mut entries = Vec::new();
    for _entry in 0..entry_count {
        entries.push(take_entry(&mut unread)?);
    }
    let pending = take_pending(&mut unread)?;
    if unread.has_remaining() {
        return Err("it holds more than its lengths say");
    }
    if !is_gapless(&entries) {
        return Err("its artifacts leave a gap or overlap");
    }
    if let (Some(last_entry), Some(pending)) = (entries.last(), &pending)
        && pending.covered_range.first() != last_entry.covered_range.last().next()
    {
        return Err("its saved records do not follow its last artifact");
    }
    Ok(StoredCatalog { entries, pending })
}

/// Reads the flag that says whether pending records were saved, and their entry if they were.
fn take_pending(unread: &mut Bytes) -> Result<Option<CatalogEntry>, &'static str> {
    let Ok(pending_flag) = unread.try_get_u8() else {
        return Err(CUT_SHORT);
    };
    match pending_flag {
        NO_PENDING_RECORDS => Ok(None),
        PENDING_RECORDS_FOLLOW => Ok(Some(take_entry(unread)?)),
        _other => Err("it does not say whether pending records were saved"),
    }
}

/// Reads one entry of a stored catalog and moves `unread` past it.
fn take_entry(unread: &mut Bytes) -> Result<CatalogEntry, &'static str> {
    let first = SequenceNumber::new(take_u64(unread)?);
    let last = SequenceNumber::new(take_u64(unread)?);
    let size_bytes = take_u64(unread)?;
    let Ok(artifact_key) = String::from_utf8(take_length_prefixed(unread)?.to_vec()) else {
        return Err("an artifact key is not text");
    };
    let summary = take_length_prefixed(unread)?;
    let Ok(covered_range) = SequenceRange::new(first, last) else {
        return Err("an artifact covers no record");
    };
    Ok(CatalogEntry {
        covered_range,
        artifact: ArtifactDescriptor {
            artifact_key,
            size_bytes,
            summary,
        },
    })
}

/// Reads a length and then that many bytes, or says that the catalog ends first.
fn take_length_prefixed(unread: &mut Bytes) -> Result<Bytes, &'static str> {
    let length = take_u32(unread)? as usize;
    if unread.remaining() < length {
        return Err(CUT_SHORT);
    }
    Ok(unread.split_to(length))
}

/// Reads the next four bytes as an integer, or says that the catalog ends first.
fn take_u32(unread: &mut Bytes) -> Result<u32, &'static str> {
    match unread.try_get_u32_le() {
        Ok(value) => Ok(value),
        Err(_cut_short) => Err(CUT_SHORT),
    }
}

/// Reads the next eight bytes as an integer, or says that the catalog ends first.
fn take_u64(unread: &mut Bytes) -> Result<u64, &'static str> {
    match unread.try_get_u64_le() {
        Ok(value) => Ok(value),
        Err(_cut_short) => Err(CUT_SHORT),
    }
}

/// Whether every entry starts right after the previous one ends.
///
/// # Arguments
///
/// * `entries` - Catalog entries, in the order of the catalog.
///
/// # Returns
///
/// `true` if no record between the first and the last entry is left out or covered twice. An empty catalog and a
/// catalog of one entry are gapless.
pub(super) fn is_gapless(entries: &[CatalogEntry]) -> bool {
    for pair in entries.windows(2) {
        if pair[1].covered_range.first() != pair[0].covered_range.last().next() {
            return false;
        }
    }
    true
}

#[cfg(test)]
#[path = "stored_test.rs"]
mod tests;
