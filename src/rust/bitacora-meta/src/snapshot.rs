//! Snapshots of the whole metadata store, as one object.
//!
//! # Why this file exists
//!
//! The metadata store is small and holds what nothing else does: which segments make up a stream, where they end, who
//! may write. If it is lost, the records on the replicas are still there but cannot be found. The design therefore
//! copies the metadata store to the object store every few minutes (section 14, "Routine procedures"), so that a lost
//! etcd can be restored from the same place tiered segments live. This file defines what such a copy looks like and how
//! it is named.
//!
//! # What it contains
//!
//! - [`MetadataSnapshot`]: every key and value of the store at one moment, with `encode` and `decode`.
//! - [`SnapshotEntry`]: one key and its value.
//! - [`SnapshotError`]: why bytes are not a snapshot.
//! - [`snapshot_object_key`], [`snapshot_time_of`], [`SNAPSHOT_KEY_PREFIX`]: how snapshot objects are named, so that
//!   the newest is the last one in a listing.
//!
//! # How it works
//!
//! All integers are little-endian.
//!
//! ```text
//! snapshot  magic 8 | format version u32 | taken at, unix milliseconds u64 | entry count u64
//!           | entry* | crc32c u32
//! entry     key length u32 | key, UTF-8 | value length u32 | value
//! ```
//!
//! The checksum covers every byte before it. Values are stored as the bytes the metadata store held, which are JSON
//! records, so a snapshot can be restored by any version that can read those records: the snapshot format knows nothing
//! about streams or replicas.
//!
//! A snapshot object is named `metadata-snapshots/<unix milliseconds, 20 digits>`. The padding makes names sort by
//! time, so listing the prefix gives the snapshots oldest first.
//!
//! # Invariants
//!
//! - Pure: encoding and decoding do no I/O and read no clock.
//! - Decoding accepts exactly what encoding produces. A changed, missing or added byte is an error, never a snapshot
//!   with other content.
//! - Entries keep the order they were given in, which is the store's key order.
//!
//! # See also
//!
//! - `store/recovery.rs`: `MetadataStore::snapshot`, which reads the entries in one consistent read, and
//!   `MetadataStore::restore`, which writes them back.
//! - `bitacora-controller/src/controller/snapshot.rs`: who writes snapshots, and when.

use bitacora_types::Timestamp;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// The first bytes of every metadata snapshot.
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"BITAMETA";

/// The snapshot format this code reads and writes.
pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// The key prefix of every snapshot object in the object store.
pub const SNAPSHOT_KEY_PREFIX: &str = "metadata-snapshots/";

/// The bytes before the first entry: the magic, the format version (`u32`), the time the snapshot was taken at (`u64`)
/// and the entry count (`u64`).
const HEADER_BYTES: usize = SNAPSHOT_MAGIC.len() + size_of::<u32>() + size_of::<u64>() + size_of::<u64>();

/// The bytes of the checksum at the end, a `u32`.
const CHECKSUM_BYTES: usize = size_of::<u32>();

/// The bytes of a length field in front of a key or a value, a `u32`.
const LENGTH_BYTES: usize = size_of::<u32>();

/// How many digits the time takes in the key of a snapshot object: enough for the largest `u64`, so that every key has
/// the same length and keys sort by time.
const OBJECT_KEY_TIME_DIGITS: usize = 20;

/// Why bytes are not a metadata snapshot.
///
/// # Examples
///
/// ```
/// use bitacora_meta::{MetadataSnapshot, SnapshotError};
///
/// let another_object = b"an object that something else stored under the key of a snapshot";
///
/// let decoded = MetadataSnapshot::decode(another_object);
///
/// assert_eq!(decoded, Err(SnapshotError::BadMagic));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum SnapshotError {
    /// Fewer bytes than the smallest snapshot were given.
    #[error("a snapshot is at least {minimum_bytes} bytes but only {available_bytes} were read")]
    TooShort {
        /// How many bytes were given.
        available_bytes: usize,
        /// The size of a snapshot without entries.
        minimum_bytes: usize,
    },
    /// The bytes do not start like a snapshot.
    #[error("object is not a metadata snapshot")]
    BadMagic,
    /// The snapshot was written by a newer version of the format.
    #[error("snapshot format version {found} is not supported")]
    UnsupportedVersion {
        /// The version found in the snapshot.
        found: u32,
    },
    /// The snapshot fails its checksum.
    #[error("snapshot checksum is {computed:#010x} but {stored:#010x} is stored")]
    ChecksumMismatch {
        /// The checksum stored in the snapshot.
        stored: u32,
        /// The checksum of the snapshot as read.
        computed: u32,
    },
    /// The checksum holds but the content does not parse, which only a faulty writer produces.
    #[error("snapshot is malformed: {reason}")]
    Malformed {
        /// What is wrong with it.
        reason: &'static str,
    },
}

/// One key of the metadata store and the bytes stored under it.
///
/// The value is kept as bytes and never decoded, so a snapshot holds whatever the store held, whichever version wrote
/// it.
///
/// # Examples
///
/// ```
/// use bitacora_meta::SnapshotEntry;
/// use bytes::Bytes;
///
/// let entry = SnapshotEntry {
///     key: "counters/stream-id".to_owned(),
///     value: Bytes::from_static(b"7"),
/// };
///
/// assert_eq!(entry.value.len(), 1);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEntry {
    /// The key, without the prefix a backend may put in front of it.
    pub key: String,
    /// The stored bytes.
    pub value: Bytes,
}

/// Every key and value of the metadata store at one moment.
///
/// # Examples
///
/// ```
/// use bitacora_meta::{MetadataSnapshot, SnapshotEntry};
/// use bitacora_types::Timestamp;
/// use bytes::Bytes;
///
/// let snapshot = MetadataSnapshot {
///     taken_at: Timestamp::from_unix_millis(1_798_761_599_000),
///     entries: vec![SnapshotEntry {
///         key: "streams/orders".to_owned(),
///         value: Bytes::from_static(b"{}"),
///     }],
/// };
/// let stored_object = snapshot.encode();
/// assert_eq!(MetadataSnapshot::decode(&stored_object), Ok(snapshot));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataSnapshot {
    /// When the snapshot was read from the store.
    pub taken_at: Timestamp,
    /// The store's content, sorted by key.
    pub entries: Vec<SnapshotEntry>,
}

impl MetadataSnapshot {
    /// Serializes the snapshot as one object, checksum included.
    ///
    /// The header comes first, then every entry in the order it has in the snapshot, then a checksum of everything
    /// before it. The object is built in memory, in a buffer sized for it in advance.
    ///
    /// # Returns
    ///
    /// The bytes to store. [`MetadataSnapshot::decode`] reads them back.
    ///
    /// # Panics
    ///
    /// Panics if a key or a value is 4 GiB or larger, which no metadata store holds.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_meta::{MetadataSnapshot, SNAPSHOT_MAGIC};
    /// use bitacora_types::Timestamp;
    ///
    /// let snapshot_of_an_empty_store = MetadataSnapshot {
    ///     taken_at: Timestamp::from_unix_millis(1_798_761_599_000),
    ///     entries: Vec::new(),
    /// };
    ///
    /// let stored_object = snapshot_of_an_empty_store.encode();
    ///
    /// assert!(stored_object.starts_with(&SNAPSHOT_MAGIC));
    /// ```
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut content_bytes = 0;
        for entry in &self.entries {
            content_bytes += 2 * LENGTH_BYTES + entry.key.len() + entry.value.len();
        }
        let mut encoded = BytesMut::with_capacity(HEADER_BYTES + content_bytes + CHECKSUM_BYTES);
        encoded.put_slice(&SNAPSHOT_MAGIC);
        encoded.put_u32_le(SNAPSHOT_FORMAT_VERSION);
        encoded.put_u64_le(self.taken_at.unix_millis());
        encoded.put_u64_le(self.entries.len() as u64);
        for entry in &self.entries {
            put_length_prefixed(&mut encoded, entry.key.as_bytes());
            put_length_prefixed(&mut encoded, &entry.value);
        }
        let checksum = crc32c::crc32c(&encoded);
        encoded.put_u32_le(checksum);
        encoded.freeze()
    }

    /// Parses and verifies a snapshot.
    ///
    /// The frame is checked first: the length, the magic, the format version and the checksum over everything before
    /// it. Only then are the entries read, so a damaged object is reported as damaged and never as a snapshot with
    /// other content. Every value is copied out of `encoded`.
    ///
    /// # Arguments
    ///
    /// * `encoded` - The whole object, as [`MetadataSnapshot::encode`] produced it.
    ///
    /// # Returns
    ///
    /// The snapshot that was encoded: the same time and the same entries in the same order.
    ///
    /// # Errors
    ///
    /// * [`SnapshotError::TooShort`] if the bytes cannot hold even an empty snapshot.
    /// * [`SnapshotError::BadMagic`] if they are not a snapshot at all.
    /// * [`SnapshotError::UnsupportedVersion`] if a newer version of the format wrote them.
    /// * [`SnapshotError::ChecksumMismatch`] if any byte changed since they were written.
    /// * [`SnapshotError::Malformed`] if the checksum holds but the content does not parse.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_meta::{MetadataSnapshot, SnapshotError};
    /// use bitacora_types::Timestamp;
    ///
    /// let snapshot = MetadataSnapshot {
    ///     taken_at: Timestamp::from_unix_millis(1_798_761_599_000),
    ///     entries: Vec::new(),
    /// };
    /// let mut damaged_object = snapshot.encode().to_vec();
    /// let last_byte = damaged_object.len() - 1;
    /// damaged_object[last_byte] ^= 0x01;
    ///
    /// let decoded = MetadataSnapshot::decode(&damaged_object);
    ///
    /// assert!(matches!(decoded, Err(SnapshotError::ChecksumMismatch { .. })));
    /// ```
    pub fn decode(encoded: &[u8]) -> Result<Self, SnapshotError> {
        let mut unread = verified_body(encoded)?;
        let taken_at = Timestamp::from_unix_millis(unread.get_u64_le());
        let entry_count = unread.get_u64_le();
        let mut entries = Vec::new();
        for _entry in 0..entry_count {
            entries.push(take_entry(&mut unread)?);
        }
        if unread.has_remaining() {
            return Err(SnapshotError::Malformed {
                reason: "bytes follow the last entry",
            });
        }
        Ok(Self { taken_at, entries })
    }
}

/// Checks the frame of an encoded snapshot: that it is long enough, starts with the magic, has the format version this
/// code reads, and passes its checksum.
///
/// # Returns
///
/// The bytes between the format version and the checksum: the time the snapshot was taken at, the entry count and the
/// entries. The time and the count are always there, because the length was checked.
///
/// # Errors
///
/// Fails with [`SnapshotError::TooShort`], [`SnapshotError::BadMagic`], [`SnapshotError::UnsupportedVersion`] or
/// [`SnapshotError::ChecksumMismatch`], whichever of the four checks fails first, in that order. The version is judged
/// before the checksum, so a snapshot of a newer format is reported as such whatever its checksum is.
fn verified_body(encoded: &[u8]) -> Result<&[u8], SnapshotError> {
    let minimum_bytes = HEADER_BYTES + CHECKSUM_BYTES;
    if encoded.len() < minimum_bytes {
        return Err(SnapshotError::TooShort {
            available_bytes: encoded.len(),
            minimum_bytes,
        });
    }
    let (content, mut stored_checksum) = encoded.split_at(encoded.len() - CHECKSUM_BYTES);
    let mut unread = content;
    let mut magic = [0_u8; SNAPSHOT_MAGIC.len()];
    unread.copy_to_slice(&mut magic);
    if magic != SNAPSHOT_MAGIC {
        return Err(SnapshotError::BadMagic);
    }
    let version = unread.get_u32_le();
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(SnapshotError::UnsupportedVersion { found: version });
    }
    let stored = stored_checksum.get_u32_le();
    let computed = crc32c::crc32c(content);
    if stored != computed {
        return Err(SnapshotError::ChecksumMismatch { stored, computed });
    }
    Ok(unread)
}

/// Takes one entry, a key and then a value, off the front of `unread`.
///
/// # Errors
///
/// Fails with [`SnapshotError::Malformed`] if the entry is cut off or its key is not UTF-8 text.
fn take_entry(unread: &mut &[u8]) -> Result<SnapshotEntry, SnapshotError> {
    let key_bytes = take_length_prefixed(unread)?;
    let value_bytes = take_length_prefixed(unread)?;
    let Ok(key) = std::str::from_utf8(key_bytes) else {
        return Err(SnapshotError::Malformed {
            reason: "a key is not UTF-8 text",
        });
    };
    Ok(SnapshotEntry {
        key: key.to_owned(),
        value: Bytes::copy_from_slice(value_bytes),
    })
}

/// Appends `bytes` to `encoded` with its length in front.
fn put_length_prefixed(encoded: &mut BytesMut, bytes: &[u8]) {
    let length = u32::try_from(bytes.len()).expect("metadata keys and values are far below 4 GiB");
    encoded.put_u32_le(length);
    encoded.put_slice(bytes);
}

/// Takes one length-prefixed run of bytes off the front of `unread`.
fn take_length_prefixed<'encoded>(unread: &mut &'encoded [u8]) -> Result<&'encoded [u8], SnapshotError> {
    if unread.len() < LENGTH_BYTES {
        return Err(SnapshotError::Malformed {
            reason: "an entry is cut off before a length",
        });
    }
    let length = unread.get_u32_le() as usize;
    if unread.len() < length {
        return Err(SnapshotError::Malformed {
            reason: "an entry is cut off before its end",
        });
    }
    let (bytes, rest) = unread.split_at(length);
    *unread = rest;
    Ok(bytes)
}

/// The object key of the snapshot taken at `taken_at`.
///
/// # Arguments
///
/// * `taken_at` - The moment the snapshot was read from the store, as recorded in it.
///
/// # Returns
///
/// `metadata-snapshots/` followed by the time in unix milliseconds, padded to 20 digits so that keys sort by time.
///
/// # Examples
///
/// ```
/// use bitacora_meta::snapshot_object_key;
/// use bitacora_types::Timestamp;
///
/// let object_key = snapshot_object_key(Timestamp::from_unix_millis(1_798_761_599_000));
/// assert_eq!(object_key, "metadata-snapshots/00000001798761599000");
/// ```
#[must_use]
pub fn snapshot_object_key(taken_at: Timestamp) -> String {
    format!(
        "{SNAPSHOT_KEY_PREFIX}{:0width$}",
        taken_at.unix_millis(),
        width = OBJECT_KEY_TIME_DIGITS
    )
}

/// The time a snapshot object was taken at, read from its key.
///
/// The key alone answers, so a caller that lists the snapshot objects knows when each one was taken without reading any
/// of them.
///
/// # Arguments
///
/// * `object_key` - The key of an object in the object store, as a listing returns it.
///
/// # Returns
///
/// The time the key names. `None` if `object_key` is not the key of a snapshot object: it lacks the prefix, or what
/// follows the prefix is not a number of exactly 20 digits.
///
/// # Examples
///
/// ```
/// use bitacora_meta::snapshot_time_of;
/// use bitacora_types::Timestamp;
///
/// assert_eq!(
///     snapshot_time_of("metadata-snapshots/00000001798761599000"),
///     Some(Timestamp::from_unix_millis(1_798_761_599_000))
/// );
/// assert_eq!(snapshot_time_of("streams/7/segments/00000000000000000003"), None);
/// ```
#[must_use]
pub fn snapshot_time_of(object_key: &str) -> Option<Timestamp> {
    let digits = object_key.strip_prefix(SNAPSHOT_KEY_PREFIX)?;
    let is_a_padded_number =
        digits.len() == OBJECT_KEY_TIME_DIGITS && digits.bytes().all(|digit| digit.is_ascii_digit());
    if !is_a_padded_number {
        return None;
    }
    let unix_millis = digits.parse::<u64>().ok()?;
    Some(Timestamp::from_unix_millis(unix_millis))
}

#[cfg(test)]
#[path = "snapshot_test.rs"]
mod tests;
