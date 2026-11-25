//! Records and ranges of records (design, section 4).

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::SequenceNumber;

/// The largest payload a single record may carry.
pub const MAX_RECORD_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

/// One committed record of a stream.
///
/// bitacora never interprets the payload; materializers do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    /// The record's position in its stream.
    pub sequence: SequenceNumber,
    /// The bytes the writer appended.
    pub payload: Bytes,
}

/// Why two positions do not form a [`SequenceRange`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum InvalidSequenceRange {
    /// The range would start at the empty position, which holds no record.
    #[error("a range cannot start at the empty position")]
    StartsAtEmptyPosition,
    /// The range would end before it starts.
    #[error("range ends at {last}, before it starts at {first}")]
    EndsBeforeStart {
        /// The requested first sequence number.
        first: SequenceNumber,
        /// The requested last sequence number.
        last: SequenceNumber,
    },
}

/// A non-empty, contiguous run of records, with both ends included.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "UncheckedSequenceRange", into = "UncheckedSequenceRange")]
pub struct SequenceRange {
    first: SequenceNumber,
    last: SequenceNumber,
}

impl SequenceRange {
    /// Builds the range that covers `first` to `last`, both included.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidSequenceRange`] if `first` is the empty position or
    /// lies after `last`.
    pub fn new(first: SequenceNumber, last: SequenceNumber) -> Result<Self, InvalidSequenceRange> {
        if first == SequenceNumber::ZERO {
            return Err(InvalidSequenceRange::StartsAtEmptyPosition);
        }
        if last < first {
            return Err(InvalidSequenceRange::EndsBeforeStart { first, last });
        }
        Ok(Self { first, last })
    }

    /// The first sequence number in the range.
    #[must_use]
    pub const fn first(self) -> SequenceNumber {
        self.first
    }

    /// The last sequence number in the range.
    #[must_use]
    pub const fn last(self) -> SequenceNumber {
        self.last
    }

    /// How many records the range covers. Never zero.
    #[must_use]
    pub const fn record_count(self) -> u64 {
        self.last.records_since(self.first) + 1
    }

    /// Whether `sequence` lies inside the range.
    #[must_use]
    pub fn contains(self, sequence: SequenceNumber) -> bool {
        self.first <= sequence && sequence <= self.last
    }
}

impl fmt::Display for SequenceRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}..={}", self.first, self.last)
    }
}

/// The serialized form of a [`SequenceRange`], before validation.
#[derive(Serialize, Deserialize)]
struct UncheckedSequenceRange {
    first: SequenceNumber,
    last: SequenceNumber,
}

impl TryFrom<UncheckedSequenceRange> for SequenceRange {
    type Error = InvalidSequenceRange;

    fn try_from(unchecked: UncheckedSequenceRange) -> Result<Self, Self::Error> {
        Self::new(unchecked.first, unchecked.last)
    }
}

impl From<SequenceRange> for UncheckedSequenceRange {
    fn from(range: SequenceRange) -> Self {
        Self {
            first: range.first,
            last: range.last,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(first: u64, last: u64) -> SequenceRange {
        SequenceRange::new(SequenceNumber::new(first), SequenceNumber::new(last))
            .expect("valid range")
    }

    #[test]
    fn range_of_one_record_counts_one() {
        assert_eq!(range(5, 5).record_count(), 1);
    }

    #[test]
    fn range_counts_both_ends() {
        assert_eq!(range(5, 9).record_count(), 5);
    }

    #[test]
    fn range_contains_its_ends() {
        let covered = range(5, 9);
        assert!(
            covered.contains(SequenceNumber::new(5)) && covered.contains(SequenceNumber::new(9))
        );
    }

    #[test]
    fn range_excludes_its_neighbours() {
        let covered = range(5, 9);
        assert!(
            !covered.contains(SequenceNumber::new(4)) && !covered.contains(SequenceNumber::new(10))
        );
    }

    #[test]
    fn range_cannot_start_at_the_empty_position() {
        assert_eq!(
            SequenceRange::new(SequenceNumber::ZERO, SequenceNumber::new(3)),
            Err(InvalidSequenceRange::StartsAtEmptyPosition)
        );
    }

    #[test]
    fn range_cannot_end_before_it_starts() {
        let first = SequenceNumber::new(9);
        let last = SequenceNumber::new(5);
        assert_eq!(
            SequenceRange::new(first, last),
            Err(InvalidSequenceRange::EndsBeforeStart { first, last })
        );
    }

    #[test]
    fn range_displays_as_an_inclusive_interval() {
        assert_eq!(range(5, 9).to_string(), "5..=9");
    }

    #[test]
    fn range_is_validated_when_deserialized() {
        let deserialized = serde_json::from_str::<SequenceRange>(r#"{"first":9,"last":5}"#);
        assert!(deserialized.is_err());
    }

    #[test]
    fn range_survives_a_serialization_round_trip() {
        let serialized = serde_json::to_string(&range(5, 9)).expect("serializable");
        let deserialized: SequenceRange = serde_json::from_str(&serialized).expect("valid");
        assert_eq!(deserialized, range(5, 9));
    }
}
