//! The identifiers and positions of the data model.
//!
//! # Why this file exists
//!
//! Every part of bitacora talks about the same few things: which stream, which segment of it, which writer
//! generation, which record, which replica (design, section 4). All but the last are numbers. If they
//! travelled as bare `u64` values, a segment identifier could be passed where a sequence number was meant
//! and the compiler would say nothing; in a system whose safety rests on comparing epochs and positions,
//! that mistake corrupts data quietly. This file gives each of them a type of its own, so that mixing them
//! up does not compile, and puts the few rules about them (where numbering starts, what "next" means, what
//! a replica identifier may contain) in one place.
//!
//! # What it contains
//!
//! - [`StreamId`]: identifies a stream for its whole life.
//! - [`SegmentId`]: the position of a segment within its stream.
//! - [`SegmentAddress`]: a stream and a segment together, which names one segment in the whole cluster.
//! - [`Epoch`]: a writer generation. All fencing compares epochs.
//! - [`SequenceNumber`]: the position of a record in its stream.
//! - [`ReplicaId`]: identifies one replica's data directory, with [`InvalidReplicaId`] for text that
//!   cannot be one.
//!
//! # How it works
//!
//! Each numeric identifier wraps a `u64`. The wrappers are `Copy`, ordered by the number they hold, and
//! written by `serde` and by `Display` as that plain number, so a stored record or a log line shows `41`
//! and not a structure. Arithmetic exists only where the data model has it: the segment after this one,
//! the epoch after this one, positions a number of records apart. Running out of numbers panics instead
//! of wrapping around, because a wrapped position would silently name an old record; at one step per
//! nanosecond that takes five centuries.
//!
//! A [`ReplicaId`] is text, because operators choose it and read it. It is checked when it is built and
//! again when it is deserialized, so a value of the type is always usable as part of a metadata key.
//!
//! # Invariants
//!
//! - [`SequenceNumber::ZERO`] never names a record. It is the position before the first one.
//! - Numbering of segments starts at [`SegmentId::FIRST`] and of records at [`SequenceNumber::FIRST`].
//! - A [`ReplicaId`] that exists is valid: non-empty, at most [`ReplicaId::MAX_LENGTH_BYTES`] long, and
//!   made of ASCII letters, digits, `.`, `_` and `-` only.
//! - The serialized form of every type here is part of the metadata schema and of the wire protocol. It
//!   does not change without a new schema version.
//!
//! # See also
//!
//! - `records.rs`: records and ranges of records, which are built from [`SequenceNumber`].
//! - `replica_api.rs`: the requests and answers that carry these identifiers to a replica.
//! - `docs/protocol/README.md`: the same vocabulary in prose, for clients in other languages.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The punctuation a replica identifier may contain besides ASCII letters and digits. Each of these is
/// safe in a metadata key, a file name, a host name and a metric label, which is where identifiers end up.
const REPLICA_ID_PUNCTUATION: &str = "._-";

/// Identifies a stream for its whole life.
///
/// A stream also has a name, which people use. Names can be reused after a stream is deleted; identifiers
/// cannot, so anything stored under an identifier (segment files, tiered objects, checkpoints) can never be
/// taken for part of a later stream with the same name. The metadata store hands identifiers out.
///
/// # Examples
///
/// ```
/// use bitacora_types::StreamId;
///
/// let stream_id = StreamId::new(7);
///
/// assert_eq!(stream_id.get(), 7);
/// assert_eq!(stream_id.to_string(), "7");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamId(u64);

impl StreamId {
    /// Wraps a raw stream identifier.
    ///
    /// # Arguments
    ///
    /// * `raw_identifier` - The number the metadata store assigned to the stream. Any value is accepted.
    ///
    /// # Returns
    ///
    /// The identifier, as its own type.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::StreamId;
    ///
    /// let stream_id = StreamId::new(7);
    /// assert_eq!(stream_id, StreamId::new(7));
    /// ```
    #[must_use]
    pub const fn new(raw_identifier: u64) -> Self {
        Self(raw_identifier)
    }

    /// Returns the raw identifier, for writing it to a file header, a key or a message.
    ///
    /// # Returns
    ///
    /// The number given to [`StreamId::new`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::StreamId;
    ///
    /// assert_eq!(StreamId::new(7).get(), 7);
    /// ```
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for StreamId {
    /// Writes the identifier as a plain number, as it appears in paths, keys and log lines.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// The position of a segment within its stream.
///
/// Segments are numbered from [`SegmentId::FIRST`] in the order they were opened. A number is never used
/// twice within a stream, not even after the segment it named was deleted, because replicas may still
/// hold copies of old segments and a new segment must not be taken for one of them.
///
/// # Examples
///
/// ```
/// use bitacora_types::SegmentId;
///
/// let second_segment = SegmentId::FIRST.next();
///
/// assert_eq!(second_segment, SegmentId::new(2));
/// assert!(second_segment > SegmentId::FIRST);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SegmentId(u64);

impl SegmentId {
    /// The first segment of every stream.
    pub const FIRST: Self = Self(1);

    /// Wraps a raw segment identifier.
    ///
    /// # Arguments
    ///
    /// * `raw_identifier` - The segment's number within its stream. Any value is accepted.
    ///
    /// # Returns
    ///
    /// The identifier, as its own type.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::SegmentId;
    ///
    /// assert_eq!(SegmentId::new(1), SegmentId::FIRST);
    /// ```
    #[must_use]
    pub const fn new(raw_identifier: u64) -> Self {
        Self(raw_identifier)
    }

    /// Returns the raw identifier, for writing it to a file header, a key or a message.
    ///
    /// # Returns
    ///
    /// The number given to [`SegmentId::new`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::SegmentId;
    ///
    /// assert_eq!(SegmentId::new(3).get(), 3);
    /// ```
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the identifier of the segment opened after this one.
    ///
    /// # Returns
    ///
    /// The identifier one above this one.
    ///
    /// # Panics
    ///
    /// Panics if the identifier space is exhausted, which a stream rolling once per nanosecond would take
    /// five centuries to reach.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::SegmentId;
    ///
    /// assert_eq!(SegmentId::new(41).next(), SegmentId::new(42));
    /// ```
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.checked_add(1).expect("segment identifiers exhausted"))
    }
}

impl fmt::Display for SegmentId {
    /// Writes the identifier as a plain number, as it appears in paths, keys and log lines.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Identifies one segment across the whole cluster: a stream and the segment's position within it.
///
/// This is what a replica is asked about. A [`SegmentId`] alone means nothing to it, because every stream
/// numbers its segments from one. Batch checksums are also computed over the address, so that a batch
/// written to the wrong segment fails its checksum there.
///
/// # Examples
///
/// ```
/// use bitacora_types::{SegmentAddress, SegmentId, StreamId};
///
/// let address = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
///
/// assert_eq!(address.stream_id, StreamId::new(7));
/// assert_eq!(address.to_string(), "7/3");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentAddress {
    /// The stream the segment belongs to.
    pub stream_id: StreamId,
    /// The segment's position within that stream.
    pub segment_id: SegmentId,
}

impl SegmentAddress {
    /// Addresses one segment of one stream.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream the segment belongs to.
    /// * `segment_id` - The segment's position within that stream.
    ///
    /// # Returns
    ///
    /// The address of that segment.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::{SegmentAddress, SegmentId, StreamId};
    ///
    /// let address = SegmentAddress::new(StreamId::new(7), SegmentId::FIRST);
    /// assert_eq!(address.segment_id, SegmentId::FIRST);
    /// ```
    #[must_use]
    pub const fn new(stream_id: StreamId, segment_id: SegmentId) -> Self {
        Self { stream_id, segment_id }
    }
}

impl fmt::Display for SegmentAddress {
    /// Writes the address as `<stream id>/<segment id>`, the form used in log lines and error messages.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.stream_id, self.segment_id)
    }
}

/// A writer generation.
///
/// A stream has one writer at a time. The metadata store raises the stream's epoch by one every time a
/// writer takes over, and everything that keeps an old writer from doing harm (fencing on replicas,
/// conditional updates of the metadata) is a comparison of epochs. A segment belongs to the one epoch
/// that wrote it.
///
/// # Examples
///
/// ```
/// use bitacora_types::Epoch;
///
/// let previous_writer = Epoch::new(4);
/// let new_writer = previous_writer.next();
///
/// assert!(new_writer > previous_writer);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Epoch(u64);

impl Epoch {
    /// The epoch of a stream that no writer has opened yet. The first writer claims epoch 1.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw epoch.
    ///
    /// # Arguments
    ///
    /// * `raw_epoch` - The generation number. Any value is accepted.
    ///
    /// # Returns
    ///
    /// The epoch, as its own type.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::Epoch;
    ///
    /// assert_eq!(Epoch::new(0), Epoch::ZERO);
    /// ```
    #[must_use]
    pub const fn new(raw_epoch: u64) -> Self {
        Self(raw_epoch)
    }

    /// Returns the raw epoch, for writing it to a file header, a record or a message.
    ///
    /// # Returns
    ///
    /// The number given to [`Epoch::new`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::Epoch;
    ///
    /// assert_eq!(Epoch::new(5).get(), 5);
    /// ```
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the epoch a new writer claims when it takes over from this one.
    ///
    /// # Returns
    ///
    /// The epoch one above this one.
    ///
    /// # Panics
    ///
    /// Panics if the epoch space is exhausted, which cannot happen in practice: every increment is a
    /// consensus write to the metadata store.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::Epoch;
    ///
    /// assert_eq!(Epoch::ZERO.next(), Epoch::new(1));
    /// ```
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.checked_add(1).expect("epochs exhausted"))
    }
}

impl fmt::Display for Epoch {
    /// Writes the epoch as a plain number, as it appears in log lines and error messages.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A position in a stream.
///
/// Records are numbered densely from [`SequenceNumber::FIRST`]. [`SequenceNumber::ZERO`] is the position
/// before the first record: it is a valid value for an end or a durable sequence of an empty stream, and it
/// is never assigned to a record. That convention is why "the last record of an empty copy" needs no
/// `Option`: it is the position before the copy's first record.
///
/// # Examples
///
/// ```
/// use bitacora_types::SequenceNumber;
///
/// let first_of_a_batch = SequenceNumber::new(100);
/// let last_of_a_batch = first_of_a_batch.advanced_by(4);
///
/// assert_eq!(last_of_a_batch, SequenceNumber::new(104));
/// assert_eq!(last_of_a_batch.records_since(first_of_a_batch.preceding()), 5);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    /// The position before the first record of a stream. It never names a record.
    pub const ZERO: Self = Self(0);

    /// The sequence number of the first record of a stream.
    pub const FIRST: Self = Self(1);

    /// Wraps a raw sequence number.
    ///
    /// # Arguments
    ///
    /// * `raw_sequence` - The position. Zero is accepted and means "before the first record".
    ///
    /// # Returns
    ///
    /// The position, as its own type.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::SequenceNumber;
    ///
    /// assert_eq!(SequenceNumber::new(1), SequenceNumber::FIRST);
    /// ```
    #[must_use]
    pub const fn new(raw_sequence: u64) -> Self {
        Self(raw_sequence)
    }

    /// Returns the raw sequence number, for writing it to a frame, a record or a message.
    ///
    /// # Returns
    ///
    /// The number given to [`SequenceNumber::new`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::SequenceNumber;
    ///
    /// assert_eq!(SequenceNumber::new(41).get(), 41);
    /// ```
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the position right after this one.
    ///
    /// # Returns
    ///
    /// The position one record later.
    ///
    /// # Panics
    ///
    /// Panics if the sequence space is exhausted, which a stream appending one record per nanosecond would
    /// take five centuries to reach.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::SequenceNumber;
    ///
    /// assert_eq!(SequenceNumber::ZERO.next(), SequenceNumber::FIRST);
    /// ```
    #[must_use]
    pub const fn next(self) -> Self {
        self.advanced_by(1)
    }

    /// Returns the position a number of records after this one.
    ///
    /// # Arguments
    ///
    /// * `record_count` - How many records to move forward. Zero returns this position.
    ///
    /// # Returns
    ///
    /// The position `record_count` records later.
    ///
    /// # Panics
    ///
    /// Panics if the sequence space is exhausted; see [`SequenceNumber::next`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::SequenceNumber;
    ///
    /// assert_eq!(SequenceNumber::new(10).advanced_by(5), SequenceNumber::new(15));
    /// ```
    #[must_use]
    pub const fn advanced_by(self, record_count: u64) -> Self {
        Self(self.0.checked_add(record_count).expect("sequence numbers exhausted"))
    }

    /// Returns the position right before this one.
    ///
    /// The position before [`SequenceNumber::ZERO`] is `ZERO` itself, so the end of what precedes the
    /// first segment is the empty position.
    ///
    /// # Returns
    ///
    /// The position one record earlier, or `ZERO` if this is `ZERO`.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::SequenceNumber;
    ///
    /// assert_eq!(SequenceNumber::FIRST.preceding(), SequenceNumber::ZERO);
    /// assert_eq!(SequenceNumber::ZERO.preceding(), SequenceNumber::ZERO);
    /// ```
    #[must_use]
    pub const fn preceding(self) -> Self {
        Self(self.0.saturating_sub(1))
    }

    /// Returns how many records lie after an earlier position, up to and including this one.
    ///
    /// # Arguments
    ///
    /// * `earlier` - The position to count from. The record at `earlier` itself is not counted.
    ///
    /// # Returns
    ///
    /// The number of records in between, or zero if `earlier` is not before this position.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::SequenceNumber;
    ///
    /// let durable = SequenceNumber::new(12);
    /// let acknowledged_by_a_slow_replica = SequenceNumber::new(9);
    ///
    /// assert_eq!(durable.records_since(acknowledged_by_a_slow_replica), 3);
    /// assert_eq!(acknowledged_by_a_slow_replica.records_since(durable), 0);
    /// ```
    #[must_use]
    pub const fn records_since(self, earlier: Self) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl fmt::Display for SequenceNumber {
    /// Writes the position as a plain number, as it appears in log lines and error messages.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Why a string cannot be used as a [`ReplicaId`].
///
/// # Examples
///
/// ```
/// use bitacora_types::{InvalidReplicaId, ReplicaId};
///
/// let refused = ReplicaId::new("zone/a");
///
/// assert_eq!(
///     refused,
///     Err(InvalidReplicaId::ForbiddenCharacter { character: '/' })
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum InvalidReplicaId {
    /// The identifier is empty.
    #[error("replica identifier is empty")]
    Empty,
    /// The identifier is longer than [`ReplicaId::MAX_LENGTH_BYTES`].
    #[error("replica identifier is {length_bytes} bytes long, the limit is {limit_bytes}")]
    TooLong {
        /// The length of the rejected identifier, in bytes.
        length_bytes: usize,
        /// The limit it exceeded, in bytes.
        limit_bytes: usize,
    },
    /// The identifier contains a character that is unsafe in a metadata key.
    #[error("replica identifier contains {character:?}; use ASCII letters, digits, '.', '_' or '-'")]
    ForbiddenCharacter {
        /// The first rejected character.
        character: char,
    },
}

/// Identifies one replica's data directory for its whole life.
///
/// The identifier belongs to the data directory, not to the host: a replica whose disk is wiped comes back
/// under a new identifier, so it can never claim to hold a segment copy it has lost. The text is restricted
/// to characters that are safe wherever an identifier ends up: a metadata key, a file, a request header, a
/// metric label.
///
/// # Examples
///
/// ```
/// use bitacora_types::ReplicaId;
///
/// let replica_id = ReplicaId::new("replica-0.zone_a")?;
///
/// assert_eq!(replica_id.as_str(), "replica-0.zone_a");
/// # Ok::<(), bitacora_types::InvalidReplicaId>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ReplicaId(String);

impl ReplicaId {
    /// The longest identifier accepted, in bytes. Long enough for a fully qualified host name with a
    /// suffix, short enough to stay readable in a key.
    pub const MAX_LENGTH_BYTES: usize = 128;

    /// Validates and wraps a replica identifier.
    ///
    /// # Arguments
    ///
    /// * `identifier` - The text to use, for example a host name. It is taken as given: nothing is trimmed
    ///   or changed.
    ///
    /// # Returns
    ///
    /// The identifier, which from then on is known to be valid.
    ///
    /// # Errors
    ///
    /// * [`InvalidReplicaId::Empty`] if the text is empty.
    /// * [`InvalidReplicaId::TooLong`] if it is longer than [`ReplicaId::MAX_LENGTH_BYTES`].
    /// * [`InvalidReplicaId::ForbiddenCharacter`] if it contains anything but ASCII letters, digits, `.`,
    ///   `_` and `-`.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::{InvalidReplicaId, ReplicaId};
    ///
    /// assert!(ReplicaId::new("replica-a").is_ok());
    /// assert_eq!(ReplicaId::new(""), Err(InvalidReplicaId::Empty));
    /// ```
    pub fn new(identifier: impl Into<String>) -> Result<Self, InvalidReplicaId> {
        let identifier = identifier.into();
        if identifier.is_empty() {
            return Err(InvalidReplicaId::Empty);
        }
        if identifier.len() > Self::MAX_LENGTH_BYTES {
            return Err(InvalidReplicaId::TooLong {
                length_bytes: identifier.len(),
                limit_bytes: Self::MAX_LENGTH_BYTES,
            });
        }
        for character in identifier.chars() {
            if !is_allowed_in_a_replica_id(character) {
                return Err(InvalidReplicaId::ForbiddenCharacter { character });
            }
        }
        Ok(Self(identifier))
    }

    /// Returns the identifier as text.
    ///
    /// # Returns
    ///
    /// The text given to [`ReplicaId::new`], unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::ReplicaId;
    ///
    /// let replica_id = ReplicaId::new("replica-a")?;
    /// assert_eq!(replica_id.as_str(), "replica-a");
    /// # Ok::<(), bitacora_types::InvalidReplicaId>(())
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Whether `character` may appear in a replica identifier: an ASCII letter, a digit, or one of
/// [`REPLICA_ID_PUNCTUATION`].
fn is_allowed_in_a_replica_id(character: char) -> bool {
    character.is_ascii_alphanumeric() || REPLICA_ID_PUNCTUATION.contains(character)
}

impl fmt::Display for ReplicaId {
    /// Writes the identifier as its text, without quotes.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<String> for ReplicaId {
    type Error = InvalidReplicaId;

    /// Validates text read from a record or a message. This is what `serde` calls, so a stored identifier
    /// that is not valid is refused when it is read, not when it is first used.
    fn try_from(identifier: String) -> Result<Self, Self::Error> {
        Self::new(identifier)
    }
}

impl From<ReplicaId> for String {
    /// Gives the identifier's text back, for writing it to a record or a message.
    fn from(replica_id: ReplicaId) -> Self {
        replica_id.0
    }
}

#[cfg(test)]
#[path = "identifiers_test.rs"]
mod tests;
