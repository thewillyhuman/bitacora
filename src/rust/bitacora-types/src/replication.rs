//! Replica counts and quorums (design, section 4, "Quorum arithmetic").

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Why a pair of numbers cannot be used as [`ReplicationSettings`].
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

/// How many replicas hold each segment of a stream, and how many of them
/// must acknowledge a record before it is committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "UncheckedReplicationSettings", into = "UncheckedReplicationSettings")]
pub struct ReplicationSettings {
    replica_count: u8,
    write_quorum: u8,
}

impl ReplicationSettings {
    /// Three replicas, two acknowledgements: survives the loss of one
    /// failure domain for both durability and availability.
    pub const DEFAULT: Self = Self {
        replica_count: 3,
        write_quorum: 2,
    };

    /// Builds settings for `replica_count` replicas (N) that commit at
    /// `write_quorum` acknowledgements (W).
    ///
    /// # Errors
    ///
    /// Returns [`InvalidReplicationSettings`] unless `1 <= W <= N`.
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

    /// The number of replicas that hold each segment (N).
    #[must_use]
    pub const fn replica_count(self) -> usize {
        self.replica_count as usize
    }

    /// The number of acknowledgements that commit a record (W).
    #[must_use]
    pub const fn write_quorum(self) -> usize {
        self.write_quorum as usize
    }

    /// The number of replicas a recovering writer must hear from (N − W + 1).
    ///
    /// Any set of this size shares at least one replica with every write
    /// quorum, so at least one responder holds every committed record.
    #[must_use]
    pub const fn recovery_quorum(self) -> usize {
        self.replica_count() - self.write_quorum() + 1
    }
}

impl Default for ReplicationSettings {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The serialized form of [`ReplicationSettings`], before validation.
#[derive(Serialize, Deserialize)]
struct UncheckedReplicationSettings {
    replica_count: u8,
    write_quorum: u8,
}

impl TryFrom<UncheckedReplicationSettings> for ReplicationSettings {
    type Error = InvalidReplicationSettings;

    fn try_from(unchecked: UncheckedReplicationSettings) -> Result<Self, Self::Error> {
        Self::new(unchecked.replica_count, unchecked.write_quorum)
    }
}

impl From<ReplicationSettings> for UncheckedReplicationSettings {
    fn from(settings: ReplicationSettings) -> Self {
        Self {
            replica_count: settings.replica_count,
            write_quorum: settings.write_quorum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_recovery_hears_from_two_of_three_replicas() {
        assert_eq!(ReplicationSettings::DEFAULT.recovery_quorum(), 2);
    }

    #[test]
    fn aurora_style_recovery_hears_from_three_of_six_replicas() {
        let aurora_style = ReplicationSettings::new(6, 4).expect("valid settings");
        assert_eq!(aurora_style.recovery_quorum(), 3);
    }

    #[test]
    fn recovery_quorum_always_overlaps_the_write_quorum() {
        for replica_count in 1..=9_u8 {
            for write_quorum in 1..=replica_count {
                let settings = ReplicationSettings::new(replica_count, write_quorum).expect("valid settings");
                assert!(settings.recovery_quorum() + settings.write_quorum() > settings.replica_count());
            }
        }
    }

    #[test]
    fn zero_write_quorum_is_rejected() {
        assert_eq!(
            ReplicationSettings::new(3, 0),
            Err(InvalidReplicationSettings::ZeroWriteQuorum)
        );
    }

    #[test]
    fn write_quorum_above_the_replica_count_is_rejected() {
        assert_eq!(
            ReplicationSettings::new(3, 4),
            Err(InvalidReplicationSettings::QuorumExceedsReplicas {
                replica_count: 3,
                write_quorum: 4,
            })
        );
    }

    #[test]
    fn settings_are_validated_when_deserialized() {
        let deserialized = serde_json::from_str::<ReplicationSettings>(r#"{"replica_count":2,"write_quorum":3}"#);
        assert!(deserialized.is_err());
    }
}
