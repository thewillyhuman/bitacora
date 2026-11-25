//! The identifiers and positions of the data model (design, section 4).

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Identifies a stream for its whole life. Names can be reused after a
/// stream is deleted; identifiers cannot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamId(u64);

impl StreamId {
    /// Wraps a raw stream identifier.
    #[must_use]
    pub const fn new(raw_identifier: u64) -> Self {
        Self(raw_identifier)
    }

    /// Returns the raw identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// The position of a segment within its stream. Segments are numbered from
/// [`SegmentId::FIRST`] without gaps, in the order they were opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SegmentId(u64);

impl SegmentId {
    /// The first segment of every stream.
    pub const FIRST: Self = Self(1);

    /// Wraps a raw segment identifier.
    #[must_use]
    pub const fn new(raw_identifier: u64) -> Self {
        Self(raw_identifier)
    }

    /// Returns the raw identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the identifier of the segment opened after this one.
    ///
    /// # Panics
    ///
    /// Panics if the identifier space is exhausted, which a stream rolling
    /// once per nanosecond would take five centuries to reach.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(
            self.0
                .checked_add(1)
                .expect("segment identifiers exhausted"),
        )
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A writer generation. The metadata store increments it on every writer
/// change, and all fencing rests on comparing epochs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Epoch(u64);

impl Epoch {
    /// The epoch of a stream that no writer has opened yet.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw epoch.
    #[must_use]
    pub const fn new(raw_epoch: u64) -> Self {
        Self(raw_epoch)
    }

    /// Returns the raw epoch.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the epoch a new writer claims when it takes over from this one.
    ///
    /// # Panics
    ///
    /// Panics if the epoch space is exhausted, which cannot happen in
    /// practice: every increment is a consensus write.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.checked_add(1).expect("epochs exhausted"))
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A position in a stream.
///
/// Records are numbered densely from [`SequenceNumber::FIRST`].
/// [`SequenceNumber::ZERO`] is the position before the first record: it is a
/// valid value for an end or a durable sequence of an empty stream, and it is
/// never assigned to a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    /// The position before the first record of a stream.
    pub const ZERO: Self = Self(0);

    /// The sequence number of the first record of a stream.
    pub const FIRST: Self = Self(1);

    /// Wraps a raw sequence number.
    #[must_use]
    pub const fn new(raw_sequence: u64) -> Self {
        Self(raw_sequence)
    }

    /// Returns the raw sequence number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the position right after this one.
    ///
    /// # Panics
    ///
    /// Panics if the sequence space is exhausted, which a stream appending
    /// one record per nanosecond would take five centuries to reach.
    #[must_use]
    pub const fn next(self) -> Self {
        self.advanced_by(1)
    }

    /// Returns the position `record_count` records after this one.
    ///
    /// # Panics
    ///
    /// Panics if the sequence space is exhausted; see [`SequenceNumber::next`].
    #[must_use]
    pub const fn advanced_by(self, record_count: u64) -> Self {
        Self(
            self.0
                .checked_add(record_count)
                .expect("sequence numbers exhausted"),
        )
    }

    /// Returns the position right before this one.
    ///
    /// The position before [`SequenceNumber::ZERO`] is `ZERO` itself, so the
    /// end of the segment preceding the first one is the empty position.
    #[must_use]
    pub const fn preceding(self) -> Self {
        Self(self.0.saturating_sub(1))
    }

    /// Returns how many records lie after `earlier` up to and including this
    /// position, or zero if `earlier` is not before it.
    #[must_use]
    pub const fn records_since(self, earlier: Self) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Why a string cannot be used as a [`ReplicaId`].
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum InvalidReplicaId {
    /// The identifier is empty.
    #[error("replica identifier is empty")]
    Empty,
    /// The identifier is longer than [`ReplicaId::MAX_LENGTH_BYTES`].
    #[error("replica identifier is {length_bytes} bytes long, the limit is {limit_bytes}")]
    TooLong {
        /// Length of the rejected identifier.
        length_bytes: usize,
        /// The limit it exceeded.
        limit_bytes: usize,
    },
    /// The identifier contains a character that is unsafe in a metadata key.
    #[error(
        "replica identifier contains {character:?}; use ASCII letters, digits, '.', '_' or '-'"
    )]
    ForbiddenCharacter {
        /// The first rejected character.
        character: char,
    },
}

/// Identifies one replica's disk for its whole life.
///
/// The identifier belongs to the data directory, not to the host: a replica
/// whose disk is wiped comes back under a new identifier, so it can never
/// claim to hold a segment copy it has lost.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ReplicaId(String);

impl ReplicaId {
    /// The longest identifier accepted.
    pub const MAX_LENGTH_BYTES: usize = 128;

    /// Validates and wraps a replica identifier.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidReplicaId`] if the identifier is empty, too long, or
    /// contains anything but ASCII letters, digits, `.`, `_` and `-`.
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
        let is_allowed =
            |character: char| character.is_ascii_alphanumeric() || "._-".contains(character);
        if let Some(character) = identifier.chars().find(|character| !is_allowed(*character)) {
            return Err(InvalidReplicaId::ForbiddenCharacter { character });
        }
        Ok(Self(identifier))
    }

    /// Returns the identifier as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ReplicaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<String> for ReplicaId {
    type Error = InvalidReplicaId;

    fn try_from(identifier: String) -> Result<Self, Self::Error> {
        Self::new(identifier)
    }
}

impl From<ReplicaId> for String {
    fn from(replica_id: ReplicaId) -> Self {
        replica_id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_record_follows_the_empty_position() {
        assert_eq!(SequenceNumber::ZERO.next(), SequenceNumber::FIRST);
    }

    #[test]
    fn advancing_moves_forward_by_the_record_count() {
        assert_eq!(
            SequenceNumber::new(10).advanced_by(5),
            SequenceNumber::new(15)
        );
    }

    #[test]
    fn position_before_the_first_record_is_the_empty_position() {
        assert_eq!(SequenceNumber::FIRST.preceding(), SequenceNumber::ZERO);
    }

    #[test]
    fn position_before_the_empty_position_stays_empty() {
        assert_eq!(SequenceNumber::ZERO.preceding(), SequenceNumber::ZERO);
    }

    #[test]
    fn records_since_counts_the_records_in_between() {
        let later = SequenceNumber::new(12);
        assert_eq!(later.records_since(SequenceNumber::new(9)), 3);
    }

    #[test]
    fn records_since_a_later_position_is_zero() {
        let earlier = SequenceNumber::new(9);
        assert_eq!(earlier.records_since(SequenceNumber::new(12)), 0);
    }

    #[test]
    #[should_panic(expected = "sequence numbers exhausted")]
    fn advancing_past_the_end_of_the_sequence_space_fails_fast() {
        let _ = SequenceNumber::new(u64::MAX).next();
    }

    #[test]
    fn sequence_numbers_order_by_position() {
        assert!(SequenceNumber::new(2) > SequenceNumber::new(1));
    }

    #[test]
    fn new_writer_claims_the_following_epoch() {
        assert_eq!(Epoch::ZERO.next(), Epoch::new(1));
    }

    #[test]
    fn segment_after_the_first_is_the_second() {
        assert_eq!(SegmentId::FIRST.next(), SegmentId::new(2));
    }

    #[test]
    fn identifiers_display_as_plain_numbers() {
        let rendered = format!(
            "{} {} {} {}",
            StreamId::new(7),
            SegmentId::new(3),
            Epoch::new(2),
            SequenceNumber::new(41)
        );
        assert_eq!(rendered, "7 3 2 41");
    }

    #[test]
    fn positions_serialize_as_plain_numbers() {
        let serialized = serde_json::to_string(&SequenceNumber::new(41)).expect("serializable");
        assert_eq!(serialized, "41");
    }

    #[test]
    fn replica_id_accepts_host_like_names() {
        let replica_id = ReplicaId::new("replica-0.zone_a").expect("valid identifier");
        assert_eq!(replica_id.as_str(), "replica-0.zone_a");
    }

    #[test]
    fn replica_id_rejects_empty_text() {
        assert_eq!(ReplicaId::new(""), Err(InvalidReplicaId::Empty));
    }

    #[test]
    fn replica_id_rejects_path_separators() {
        assert_eq!(
            ReplicaId::new("zone/a"),
            Err(InvalidReplicaId::ForbiddenCharacter { character: '/' })
        );
    }

    #[test]
    fn replica_id_rejects_overlong_text() {
        let overlong = "r".repeat(ReplicaId::MAX_LENGTH_BYTES + 1);
        assert_eq!(
            ReplicaId::new(overlong),
            Err(InvalidReplicaId::TooLong {
                length_bytes: ReplicaId::MAX_LENGTH_BYTES + 1,
                limit_bytes: ReplicaId::MAX_LENGTH_BYTES,
            })
        );
    }

    #[test]
    fn replica_id_is_validated_when_deserialized() {
        let deserialized = serde_json::from_str::<ReplicaId>("\"zone/a\"");
        assert!(deserialized.is_err());
    }
}
