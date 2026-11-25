//! Replica counts and quorums.
//!
//! # Why this file exists
//!
//! The safety of the log rests on one piece of arithmetic (design, section 4, "Quorum arithmetic"): a
//! record is committed once W of its segment's N replicas have flushed it, and a writer that takes over
//! must hear from N − W + 1 replicas, because any such set shares a member with every set of W. The writer,
//! the recovery, the controller and placement all need N, W and that third number. If each computed it for
//! itself, one off-by-one would be enough to lose a committed record. This file holds the three numbers and
//! the rule that ties them together.
//!
//! # What it contains
//!
//! - [`ReplicationSettings`]: N and W for a stream, with the recovery quorum derived from them.
//! - [`InvalidReplicationSettings`]: why a pair of numbers cannot be used.
//!
//! # How it works
//!
//! The settings are fixed when a stream is created and stored in its metadata record. They can only be
//! built through [`ReplicationSettings::new`], also when they are deserialized, so every value satisfies
//! `1 <= W <= N`. With that, `N − W + 1` cannot underflow and is at least one.
//!
//! # Invariants
//!
//! - `1 <= write_quorum <= replica_count`.
//! - `recovery_quorum + write_quorum > replica_count`: a recovery quorum and a write quorum always share a
//!   replica. A test checks it for every valid pair up to nine replicas.
//! - The serialized form, `{"replica_count": N, "write_quorum": W}`, is part of the metadata schema.
//!
//! # See also
//!
//! - `docs/protocol/replication.md`: the rules that use these numbers.
//! - `bitacora-model`: the executable model that checks the rules for N = 3, W = 2.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Why a pair of numbers cannot be used as [`ReplicationSettings`].
///
/// # Examples
///
/// ```
/// use bitacora_types::{InvalidReplicationSettings, ReplicationSettings};
///
/// let refused = ReplicationSettings::new(3, 0);
///
/// assert_eq!(refused, Err(InvalidReplicationSettings::ZeroWriteQuorum));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum InvalidReplicationSettings {
    /// A record must be stored on at least one replica to be committed.
    #[error("write quorum must be at least 1")]
    ZeroWriteQuorum,
    /// A quorum cannot ask for more acknowledgements than there are replicas.
    #[error("write quorum {write_quorum} exceeds the replica count {replica_count}")]
    QuorumExceedsReplicas {
        /// The requested number of replicas per segment.
        replica_count: u8,
        /// The requested number of acknowledgements per commit.
        write_quorum: u8,
    },
}

/// How many replicas hold each segment of a stream, and how many of them must acknowledge a record before
/// it is committed.
///
/// A higher write quorum survives the loss of more replicas without losing a committed record; a lower one
/// keeps appends going while more replicas are slow or down. The recovery quorum follows from the two.
///
/// # Examples
///
/// ```
/// use bitacora_types::ReplicationSettings;
///
/// let settings = ReplicationSettings::new(3, 2)?;
///
/// assert_eq!(settings, ReplicationSettings::DEFAULT);
/// assert_eq!(settings.recovery_quorum(), 2);
/// # Ok::<(), bitacora_types::InvalidReplicationSettings>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "UncheckedReplicationSettings", into = "UncheckedReplicationSettings")]
pub struct ReplicationSettings {
    /// How many replicas hold each segment (N). At least `write_quorum`.
    replica_count: u8,
    /// How many acknowledgements commit a record (W). At least one.
    write_quorum: u8,
}

impl ReplicationSettings {
    /// Three replicas, two acknowledgements: one replica can be lost without losing a committed record,
    /// and one can be down without stopping appends.
    pub const DEFAULT: Self = Self {
        replica_count: 3,
        write_quorum: 2,
    };

    /// Builds settings for N replicas that commit at W acknowledgements.
    ///
    /// # Arguments
    ///
    /// * `replica_count` - How many replicas hold each segment (N).
    /// * `write_quorum` - How many of them must acknowledge a record before it is committed (W).
    ///
    /// # Returns
    ///
    /// The settings, which from then on are known to satisfy `1 <= W <= N`.
    ///
    /// # Errors
    ///
    /// * [`InvalidReplicationSettings::ZeroWriteQuorum`] if `write_quorum` is zero.
    /// * [`InvalidReplicationSettings::QuorumExceedsReplicas`] if `write_quorum` is above `replica_count`.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::ReplicationSettings;
    ///
    /// let six_replicas_four_acknowledgements = ReplicationSettings::new(6, 4);
    /// let more_acknowledgements_than_replicas = ReplicationSettings::new(3, 4);
    ///
    /// assert!(six_replicas_four_acknowledgements.is_ok());
    /// assert!(more_acknowledgements_than_replicas.is_err());
    /// ```
    pub const fn new(replica_count: u8, write_quorum: u8) -> Result<Self, InvalidReplicationSettings> {
        if write_quorum == 0 {
            return Err(InvalidReplicationSettings::ZeroWriteQuorum);
        }
        if write_quorum > replica_count {
            return Err(InvalidReplicationSettings::QuorumExceedsReplicas {
                replica_count,
                write_quorum,
            });
        }
        Ok(Self {
            replica_count,
            write_quorum,
        })
    }

    /// Returns the number of replicas that hold each segment (N).
    ///
    /// # Returns
    ///
    /// The replica count, as a `usize` because it is compared with lengths of replica sets.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::ReplicationSettings;
    ///
    /// assert_eq!(ReplicationSettings::DEFAULT.replica_count(), 3);
    /// ```
    #[must_use]
    pub const fn replica_count(self) -> usize {
        self.replica_count as usize
    }

    /// Returns the number of acknowledgements that commit a record (W).
    ///
    /// # Returns
    ///
    /// The write quorum, as a `usize` because it is compared with counts of acknowledgements.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::ReplicationSettings;
    ///
    /// assert_eq!(ReplicationSettings::DEFAULT.write_quorum(), 2);
    /// ```
    #[must_use]
    pub const fn write_quorum(self) -> usize {
        self.write_quorum as usize
    }

    /// Returns the number of replicas a recovering writer must hear from (N − W + 1).
    ///
    /// Any set of this size shares at least one replica with every write quorum, so at least one responder
    /// holds every committed record.
    ///
    /// # Returns
    ///
    /// The recovery quorum. At least one, and at most the replica count.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::ReplicationSettings;
    ///
    /// let six_replicas_four_acknowledgements = ReplicationSettings::new(6, 4)?;
    ///
    /// assert_eq!(six_replicas_four_acknowledgements.recovery_quorum(), 3);
    /// # Ok::<(), bitacora_types::InvalidReplicationSettings>(())
    /// ```
    #[must_use]
    pub const fn recovery_quorum(self) -> usize {
        self.replica_count() - self.write_quorum() + 1
    }
}

impl Default for ReplicationSettings {
    /// Returns [`ReplicationSettings::DEFAULT`].
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The serialized form of [`ReplicationSettings`], before validation.
///
/// `serde` reads into this type and then converts, which is what makes deserialized settings go through
/// the same checks as settings built in code.
#[derive(Serialize, Deserialize)]
struct UncheckedReplicationSettings {
    /// The replica count, as stored.
    replica_count: u8,
    /// The write quorum, as stored.
    write_quorum: u8,
}

impl TryFrom<UncheckedReplicationSettings> for ReplicationSettings {
    type Error = InvalidReplicationSettings;

    /// Validates settings read from a metadata record.
    fn try_from(unchecked: UncheckedReplicationSettings) -> Result<Self, Self::Error> {
        Self::new(unchecked.replica_count, unchecked.write_quorum)
    }
}

impl From<ReplicationSettings> for UncheckedReplicationSettings {
    /// Gives the two numbers of the settings, for writing them to a metadata record.
    fn from(settings: ReplicationSettings) -> Self {
        Self {
            replica_count: settings.replica_count,
            write_quorum: settings.write_quorum,
        }
    }
}

#[cfg(test)]
#[path = "replication_test.rs"]
mod tests;
