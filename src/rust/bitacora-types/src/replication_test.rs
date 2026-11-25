//! Tests of the replication settings.
//!
//! # What is covered
//!
//! - The recovery quorum for the default settings and for six replicas with four acknowledgements.
//! - The property everything else rests on: a recovery quorum and a write quorum always share a replica.
//! - The two ways a pair of numbers is refused, in code and when deserialized, and the serialized form.
//!
//! # How the fixtures work
//!
//! There are none. The overlap test walks every valid pair up to [`LARGEST_REPLICA_COUNT_CHECKED`] replicas.

use super::*;

/// The largest replica count the overlap property is checked for. Far more than any deployment uses.
const LARGEST_REPLICA_COUNT_CHECKED: u8 = 9;

#[test]
fn default_settings_are_three_replicas() {
    assert_eq!(ReplicationSettings::DEFAULT.replica_count(), 3);
}

#[test]
fn default_settings_commit_at_two_acknowledgements() {
    assert_eq!(ReplicationSettings::DEFAULT.write_quorum(), 2);
}

#[test]
fn default_recovery_hears_from_two_of_three_replicas() {
    assert_eq!(ReplicationSettings::DEFAULT.recovery_quorum(), 2);
}

#[test]
fn recovery_with_four_of_six_acknowledgements_hears_from_three_replicas() {
    let six_replicas_four_acknowledgements = ReplicationSettings::new(6, 4).expect("four is at most six");

    assert_eq!(six_replicas_four_acknowledgements.recovery_quorum(), 3);
}

#[test]
fn recovery_quorum_always_overlaps_the_write_quorum() {
    for replica_count in 1..=LARGEST_REPLICA_COUNT_CHECKED {
        for write_quorum in 1..=replica_count {
            let settings = ReplicationSettings::new(replica_count, write_quorum)
                .expect("the quorum is between one and the replica count");

            assert!(
                settings.recovery_quorum() + settings.write_quorum() > settings.replica_count(),
                "{settings:?}"
            );
        }
    }
}

#[test]
fn zero_write_quorum_is_rejected() {
    let refused = ReplicationSettings::new(3, 0);

    assert_eq!(refused, Err(InvalidReplicationSettings::ZeroWriteQuorum));
}

#[test]
fn write_quorum_above_the_replica_count_is_rejected() {
    let refused = ReplicationSettings::new(3, 4);

    assert_eq!(
        refused,
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

#[test]
fn settings_serialize_as_their_two_numbers() {
    let serialized = serde_json::to_string(&ReplicationSettings::DEFAULT).expect("two numbers serialize");

    assert_eq!(serialized, r#"{"replica_count":3,"write_quorum":2}"#);
}

#[test]
fn settings_without_a_choice_are_the_default_ones() {
    assert_eq!(ReplicationSettings::default(), ReplicationSettings::DEFAULT);
}
