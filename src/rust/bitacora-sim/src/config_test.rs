//! Tests of the configuration of a run.
//!
//! # What is covered
//!
//! - The standard run is what the documentation says it is.
//! - A mix enables exactly the kinds whose switches are on, in the order of its fields.
//!
//! # How the fixtures work
//!
//! The configuration is plain data, so the tests need no runtime and no cluster.

use super::*;

#[test]
fn fault_mix_without_faults_enables_nothing() {
    assert_eq!(FaultMix::NONE.enabled_kinds(), Vec::new());
}

/// The nemesis draws a kind by its position in this list, so the order is pinned along with the content.
#[test]
fn full_fault_mix_enables_every_kind_in_the_order_of_its_fields() {
    assert_eq!(
        FaultMix::ALL.enabled_kinds(),
        [
            FaultKind::NodeCrash,
            FaultKind::TornCrash,
            FaultKind::Partition,
            FaultKind::SlowLink,
            FaultKind::LostResponses,
            FaultKind::FlushFailure,
            FaultKind::MetadataOutage,
            FaultKind::WriterCrash,
            FaultKind::WriterSwitch,
            FaultKind::SilentCorruption,
            FaultKind::DiskLoss,
            FaultKind::ObjectStoreOutage,
        ]
    );
}

#[test]
fn full_fault_mix_enables_every_kind() {
    assert_eq!(FaultMix::ALL.enabled_kinds().len(), 12);
}

#[test]
fn mix_with_one_switch_on_enables_that_kind_only() {
    let only_disk_loss = FaultMix {
        disk_loss: true,
        ..FaultMix::NONE
    };

    assert_eq!(only_disk_loss.enabled_kinds(), [FaultKind::DiskLoss]);
}

#[test]
fn enabled_kinds_keep_the_order_of_the_fields_when_some_are_off() {
    let two_kinds = FaultMix {
        object_store_outages: true,
        partitions: true,
        ..FaultMix::NONE
    };

    assert_eq!(
        two_kinds.enabled_kinds(),
        [FaultKind::Partition, FaultKind::ObjectStoreOutage]
    );
}

#[test]
fn standard_run_injects_every_fault() {
    assert_eq!(SimulationConfig::with_seed(1).faults, FaultMix::ALL);
}

#[test]
fn standard_run_has_no_injected_bug() {
    assert_eq!(SimulationConfig::with_seed(1).injected_bug, InjectedBug::None);
}

#[test]
fn standard_run_keeps_the_seed_it_was_given() {
    assert_eq!(SimulationConfig::with_seed(179).seed, 179);
}

/// The regression seeds were found with these numbers. Changing one changes what every seed does.
#[test]
fn standard_run_has_the_size_and_length_the_regression_seeds_were_found_with() {
    let standard = SimulationConfig::with_seed(0);

    assert_eq!(
        standard,
        SimulationConfig {
            seed: 0,
            node_count: 5,
            compute_node_count: 2,
            reader_count: 2,
            chaos_duration: Duration::from_secs(60),
            settle_timeout: Duration::from_secs(300),
            faults: FaultMix::ALL,
            mean_time_between_faults: Duration::from_millis(700),
            injected_bug: InjectedBug::None,
        }
    );
}
