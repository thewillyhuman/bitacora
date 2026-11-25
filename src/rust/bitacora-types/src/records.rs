//! Records and ranges of records.
//!
//! # Why this file exists
//!
//! A stream is a sequence of records, and what a reader gets back is records (design, section 4). The type
//! of a record, the limit on its size and the notion of "records from here to there" are needed by the
//! segment format, the replica, the client and every materializer. They are defined once, here, so that a
//! range that is valid for one of them is valid for all, and so that the size limit is one constant and
//! not four that drift apart.
//!
//! # What it contains
//!
//! - [`LogRecord`]: one committed record, a position and a payload.
//! - [`MAX_RECORD_PAYLOAD_BYTES`]: the largest payload a record may carry.
//! - [`SequenceRange`]: a non-empty, contiguous run of positions with both ends included, and
//!   [`InvalidSequenceRange`] for two positions that do not form one.
//!
//! # How it works
//!
//! A [`LogRecord`] holds its payload as `Bytes`, a reference-counted view. A record decoded from a batch
//! frame therefore shares the frame's buffer instead of copying the payload out of it, and cloning a record
//! is cheap.
//!
//! A [`SequenceRange`] can only be built through [`SequenceRange::new`], also when it is deserialized, so
//! a value of the type always starts at a real record and never ends before it starts. Code that receives
//! one does not check again.
//!
//! # Invariants
//!
//! - A [`SequenceRange`] holds at least one record, and its first position is never
//!   [`SequenceNumber::ZERO`].
//! - bitacora never interprets a payload. What the bytes mean is the materializer's business.
//!
//! # See also
//!
//! - `identifiers.rs`: [`SequenceNumber`], which ranges and records are built from.
//! - `bitacora-segment/src/format.rs`: how records are grouped into checksummed batches.

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::SequenceNumber;

/// The largest payload a single record may carry, in bytes: 4 MiB.
///
/// A record travels in one message and is checksummed in one batch, so its size bounds the memory a
/// replica and a reader need to handle one. An application with larger values splits them or stores them
/// elsewhere and logs a reference.
pub const MAX_RECORD_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

/// One committed record of a stream.
///
/// bitacora never interprets the payload; materializers do. The payload shares the buffer it was decoded
/// from, so cloning a record does not copy it.
///
/// # Examples
///
/// ```
/// use bitacora_types::{LogRecord, SequenceNumber};
/// use bytes::Bytes;
///
/// let record = LogRecord {
///     sequence: SequenceNumber::new(41),
///     payload: Bytes::from_static(b"set balance=10"),
/// };
///
/// assert_eq!(record.payload.len(), 14);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    /// The record's position in its stream.
    pub sequence: SequenceNumber,
    /// The bytes the writer appended, unchanged.
    pub payload: Bytes,
}

/// Why two positions do not form a [`SequenceRange`].
///
/// # Examples
///
/// ```
/// use bitacora_types::{InvalidSequenceRange, SequenceNumber, SequenceRange};
///
/// let refused = SequenceRange::new(SequenceNumber::ZERO, SequenceNumber::new(3));
///
/// assert_eq!(refused, Err(InvalidSequenceRange::StartsAtEmptyPosition));
/// ```
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
///
/// Both ends are included because that is how the rest of the system speaks: "sealed at 9500" means record
/// 9500 is in the segment. A range can only be built through [`SequenceRange::new`], so every value is
/// valid.
///
/// # Examples
///
/// ```
/// use bitacora_types::{SequenceNumber, SequenceRange};
///
/// let range = SequenceRange::new(SequenceNumber::new(5), SequenceNumber::new(9))?;
///
/// assert_eq!(range.record_count(), 5);
/// assert!(range.contains(SequenceNumber::new(9)));
/// assert_eq!(range.to_string(), "5..=9");
/// # Ok::<(), bitacora_types::InvalidSequenceRange>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "UncheckedSequenceRange", into = "UncheckedSequenceRange")]
pub struct SequenceRange {
    /// The first position in the range. Never the empty position.
    first: SequenceNumber,
    /// The last position in the range. Never before `first`.
    last: SequenceNumber,
}

impl SequenceRange {
    /// Builds the range that covers `first` to `last`, both included.
    ///
    /// # Arguments
    ///
    /// * `first` - The first sequence number of the range. Must name a record, so not
    ///   [`SequenceNumber::ZERO`].
    /// * `last` - The last sequence number of the range. Equal to `first` for a range of one record.
    ///
    /// # Returns
    ///
    /// The range.
    ///
    /// # Errors
    ///
    /// * [`InvalidSequenceRange::StartsAtEmptyPosition`] if `first` is the empty position.
    /// * [`InvalidSequenceRange::EndsBeforeStart`] if `last` lies before `first`.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::{SequenceNumber, SequenceRange};
    ///
    /// let one_record = SequenceRange::new(SequenceNumber::new(5), SequenceNumber::new(5));
    /// let backwards = SequenceRange::new(SequenceNumber::new(9), SequenceNumber::new(5));
    ///
    /// assert!(one_record.is_ok());
    /// assert!(backwards.is_err());
    /// ```
    pub fn new(first: SequenceNumber, last: SequenceNumber) -> Result<Self, InvalidSequenceRange> {
        if first == SequenceNumber::ZERO {
            return Err(InvalidSequenceRange::StartsAtEmptyPosition);
        }
        if last < first {
            return Err(InvalidSequenceRange::EndsBeforeStart { first, last });
        }
        Ok(Self { first, last })
    }

    /// Returns the first sequence number in the range.
    ///
    /// # Returns
    ///
    /// The position of the range's first record.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::{SequenceNumber, SequenceRange};
    ///
    /// let range = SequenceRange::new(SequenceNumber::new(5), SequenceNumber::new(9))?;
    /// assert_eq!(range.first(), SequenceNumber::new(5));
    /// # Ok::<(), bitacora_types::InvalidSequenceRange>(())
    /// ```
    #[must_use]
    pub const fn first(self) -> SequenceNumber {
        self.first
    }

    /// Returns the last sequence number in the range.
    ///
    /// # Returns
    ///
    /// The position of the range's last record, which is part of the range.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::{SequenceNumber, SequenceRange};
    ///
    /// let range = SequenceRange::new(SequenceNumber::new(5), SequenceNumber::new(9))?;
    /// assert_eq!(range.last(), SequenceNumber::new(9));
    /// # Ok::<(), bitacora_types::InvalidSequenceRange>(())
    /// ```
    #[must_use]
    pub const fn last(self) -> SequenceNumber {
        self.last
    }

    /// Returns how many records the range covers.
    ///
    /// # Returns
    ///
    /// The number of records from the first to the last, both counted. Never zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::{SequenceNumber, SequenceRange};
    ///
    /// let range = SequenceRange::new(SequenceNumber::new(5), SequenceNumber::new(9))?;
    /// assert_eq!(range.record_count(), 5);
    /// # Ok::<(), bitacora_types::InvalidSequenceRange>(())
    /// ```
    #[must_use]
    pub const fn record_count(self) -> u64 {
        self.last.records_since(self.first) + 1
    }

    /// Tells whether a position lies inside the range.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The position to look for.
    ///
    /// # Returns
    ///
    /// `true` if `sequence` is the first position, the last one, or anything in between.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::{SequenceNumber, SequenceRange};
    ///
    /// let range = SequenceRange::new(SequenceNumber::new(5), SequenceNumber::new(9))?;
    ///
    /// assert!(range.contains(SequenceNumber::new(5)));
    /// assert!(!range.contains(SequenceNumber::new(10)));
    /// # Ok::<(), bitacora_types::InvalidSequenceRange>(())
    /// ```
    #[must_use]
    pub fn contains(self, sequence: SequenceNumber) -> bool {
        self.first <= sequence && sequence <= self.last
    }
}

impl fmt::Display for SequenceRange {
    /// Writes the range as `<first>..=<last>`, the way Rust writes a range that includes its end.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}..={}", self.first, self.last)
    }
}

/// The serialized form of a [`SequenceRange`], before validation.
///
/// `serde` reads into this type and then converts, which is what makes a deserialized range go through the
/// same checks as one built in code.
#[derive(Serialize, Deserialize)]
struct UncheckedSequenceRange {
    /// The first position, as stored.
    first: SequenceNumber,
    /// The last position, as stored.
    last: SequenceNumber,
}

impl TryFrom<UncheckedSequenceRange> for SequenceRange {
    type Error = InvalidSequenceRange;

    /// Validates a range read from a record or a message.
    fn try_from(unchecked: UncheckedSequenceRange) -> Result<Self, Self::Error> {
        Self::new(unchecked.first, unchecked.last)
    }
}

impl From<SequenceRange> for UncheckedSequenceRange {
    /// Gives the two ends of a range, for writing them to a record or a message.
    fn from(range: SequenceRange) -> Self {
        Self {
            first: range.first,
            last: range.last,
        }
    }
}

#[cfg(test)]
#[path = "records_test.rs"]
mod tests;
