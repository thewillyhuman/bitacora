//! Tests of the controller's settings.
//!
//! # What is covered
//!
//! The defaults that the design names (when a segment is uploaded, how long its local copies stay, how often the
//! metadata store is copied), and the two relations between defaults that the controller's behaviour relies on.
//!
//! # How the fixtures work
//!
//! There are none: every test reads [`ControllerSettings::default`].

use super::*;

#[test]
fn segment_is_uploaded_fifteen_minutes_after_its_seal_by_default() {
    assert_eq!(ControllerSettings::default().tiering_delay, Duration::from_mins(15));
}

#[test]
fn local_copies_stay_for_a_day_after_the_upload_by_default() {
    assert_eq!(ControllerSettings::default().hot_read_window, Duration::from_hours(24));
}

#[test]
fn metadata_store_is_copied_every_five_minutes_by_default() {
    assert_eq!(
        ControllerSettings::default().metadata_snapshot_interval,
        Duration::from_mins(5)
    );
}

#[test]
fn a_day_of_metadata_snapshots_is_kept_by_default() {
    let settings = ControllerSettings::default();
    let snapshots_per_day = Duration::from_hours(24).as_secs() / settings.metadata_snapshot_interval.as_secs();

    assert_eq!(settings.metadata_snapshots_kept as u64, snapshots_per_day);
}

/// A restart or an upgrade of a node must not trigger re-replication: a replica stops receiving new segments long
/// before its copies are given up.
#[test]
fn replica_is_given_up_much_later_than_it_stops_receiving_segments() {
    let settings = ControllerSettings::default();

    assert!(settings.replica_lost_after >= settings.placement.liveness_window * 10);
}

/// A leader that misses a few passes must not lose the leadership for it.
#[test]
fn leadership_outlasts_several_passes() {
    let settings = ControllerSettings::default();

    assert!(settings.leadership_duration >= settings.pass_interval * 5);
}
