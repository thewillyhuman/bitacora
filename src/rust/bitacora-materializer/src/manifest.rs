//! How a checkpoint manifest is stored in the artifact store.
//!
//! # Why this file exists
//!
//! The metadata store holds one small record per materializer: the checkpoint sequence and a pointer to
//! the manifest (design, section 8). The manifest itself, which can name thousands of artifacts, is an
//! artifact like any other. This file defines its bytes, so that a manifest written by one instance is
//! read by the next, and so that a damaged manifest is noticed instead of restoring the wrong state.
//!
//! # What it contains
//!
//! - [`encode_manifest`]: the bytes of a manifest and the checkpoint sequence it belongs to.
//! - [`decode_manifest`]: the reverse, with every check.
//!
//! # How it works
//!
//! All integers are little-endian.
//!
//! ```text
//! stored manifest  magic 8 | format version u32 | checkpoint u64 | artifact count u32
//!                  | (key length u32 | key)* | details length u32 | details | crc32c u32
//! ```
//!
//! The checksum covers everything before it. The checkpoint sequence is stored so that the runner can
//! tell a pointer that leads to the manifest of another checkpoint from one that leads to the right one.
//!
//! # Invariants
//!
//! - Pure functions over bytes: no I/O.
//! - Decoding never panics on any input and accepts only what encoding produces.
//!
//! # See also
//!
//! - `contract.rs`: [`CheckpointManifest`], the value being stored.
//! - `runner.rs`: where manifests are written and read.

use bitacora_types::SequenceNumber;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::contract::{CheckpointManifest, MaterializerError};

/// The first bytes of every stored manifest.
const MANIFEST_MAGIC: [u8; 8] = *b"BITAMANI";

/// The manifest format this code reads and writes.
const MANIFEST_FORMAT_VERSION: u32 = 1;

/// The size of the checksum that ends a stored manifest.
const CHECKSUM_BYTES: usize = 4;

/// Serializes a manifest for storage.
///
/// # Arguments
///
/// * `checkpoint` - The last sequence number the checkpoint covers.
/// * `manifest` - What the materializer said about the checkpoint.
///
/// # Returns
///
/// The bytes to store as an artifact. [`decode_manifest`] turns them back.
///
/// # Panics
///
/// Panics if the manifest names more than `u32::MAX` artifacts or holds a key or details of more than
/// `u32::MAX` bytes, which no artifact store could hold either.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::{CheckpointManifest, decode_manifest, encode_manifest};
/// use bitacora_types::SequenceNumber;
///
/// let manifest = CheckpointManifest::default();
/// let stored = encode_manifest(SequenceNumber::new(42), &manifest);
/// assert_eq!(decode_manifest(&stored), Ok((SequenceNumber::new(42), manifest)));
/// ```
#[must_use]
pub fn encode_manifest(checkpoint: SequenceNumber, manifest: &CheckpointManifest) -> Bytes {
    let mut stored = BytesMut::new();
    stored.put_slice(&MANIFEST_MAGIC);
    stored.put_u32_le(MANIFEST_FORMAT_VERSION);
    stored.put_u64_le(checkpoint.get());
    stored.put_u32_le(length_as_u32(manifest.artifact_keys.len()));
    for artifact_key in &manifest.artifact_keys {
        stored.put_u32_le(length_as_u32(artifact_key.len()));
        stored.put_slice(artifact_key.as_bytes());
    }
    stored.put_u32_le(length_as_u32(manifest.details.len()));
    stored.put_slice(&manifest.details);
    let checksum = crc32c::crc32c(&stored);
    stored.put_u32_le(checksum);
    stored.freeze()
}

/// Parses and verifies a stored manifest.
///
/// # Arguments
///
/// * `stored` - The whole artifact, as written by [`encode_manifest`].
///
/// # Returns
///
/// The checkpoint sequence the manifest belongs to, and the manifest.
///
/// # Errors
///
/// Returns [`MaterializerError::InvalidManifest`] if the bytes are not a manifest, come from a newer
/// format, fail their checksum, or do not end where their lengths say.
pub fn decode_manifest(
    stored: &Bytes,
) -> Result<(SequenceNumber, CheckpointManifest), MaterializerError> {
    let Some(checksummed_bytes) = stored.len().checked_sub(CHECKSUM_BYTES) else {
        return Err(invalid("it is shorter than its checksum"));
    };
    let mut stored_checksum = &stored[checksummed_bytes..];
    if stored_checksum.get_u32_le() != crc32c::crc32c(&stored[..checksummed_bytes]) {
        return Err(invalid("it fails its checksum"));
    }

    let mut unread = stored.slice(..checksummed_bytes);
    if unread.remaining() < MANIFEST_MAGIC.len()
        || unread.split_to(MANIFEST_MAGIC.len()) != MANIFEST_MAGIC[..]
    {
        return Err(invalid("it does not start like a manifest"));
    }
    let version = read_u32(&mut unread)?;
    if version != MANIFEST_FORMAT_VERSION {
        return Err(invalid("it was written by a newer format"));
    }
    let checkpoint = SequenceNumber::new(read_u64(&mut unread)?);
    let artifact_count = read_u32(&mut unread)?;
    let mut artifact_keys = Vec::new();
    for _artifact in 0..artifact_count {
        let key_bytes = read_length_prefixed(&mut unread)?;
        let Ok(artifact_key) = String::from_utf8(key_bytes.to_vec()) else {
            return Err(invalid("an artifact key is not text"));
        };
        artifact_keys.push(artifact_key);
    }
    let details = read_length_prefixed(&mut unread)?;
    if unread.has_remaining() {
        return Err(invalid("it holds more than its lengths say"));
    }
    Ok((
        checkpoint,
        CheckpointManifest {
            artifact_keys,
            details,
        },
    ))
}

/// The error for a stored manifest that cannot be used.
fn invalid(reason: &str) -> MaterializerError {
    MaterializerError::InvalidManifest {
        reason: reason.to_owned(),
    }
}

/// A length as the four bytes it is stored in.
fn length_as_u32(length: usize) -> u32 {
    u32::try_from(length).expect("a manifest part is far smaller than 4 GiB")
}

/// Reads the next four bytes as an integer, or fails if the manifest ends first.
fn read_u32(unread: &mut Bytes) -> Result<u32, MaterializerError> {
    unread
        .try_get_u32_le()
        .map_err(|_cut_short| invalid("it is cut short"))
}

/// Reads the next eight bytes as an integer, or fails if the manifest ends first.
fn read_u64(unread: &mut Bytes) -> Result<u64, MaterializerError> {
    unread
        .try_get_u64_le()
        .map_err(|_cut_short| invalid("it is cut short"))
}

/// Reads a length and then that many bytes, or fails if the manifest ends first.
fn read_length_prefixed(unread: &mut Bytes) -> Result<Bytes, MaterializerError> {
    let length = read_u32(unread)? as usize;
    if unread.remaining() < length {
        return Err(invalid("it is cut short"));
    }
    Ok(unread.split_to(length))
}

#[cfg(test)]
#[path = "manifest_test.rs"]
mod tests;
