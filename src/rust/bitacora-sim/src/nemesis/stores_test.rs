//! Tests of the outages of the metadata store and of the object store.
//!
//! # What is covered
//!
//! - An outage makes its store unreachable, and only its store.
//! - The store is reachable again after the planned time.
//! - Each outage is recorded with its duration.
//!
//! # How the fixtures work
//!
//! Each test starts a small world of the test support and calls an outage function as the nemesis loop would. Whether a
//! store is reachable is found out by asking it something harmless: the metadata store for the registry, the object
//! store for a listing.

use bitacora_artifact::ArtifactStore as _;

use super::*;
use crate::test_support::{fault_descriptions, start_world};

/// How long the outages of these tests last.
const LASTS_FOR: Duration = Duration::from_secs(3);

/// A moment just after a fault of these tests is undone.
const JUST_AFTER_THE_END: Duration = Duration::from_millis(3_001);

/// Says whether the metadata store of `world` answers.
async fn is_metadata_store_reachable(world: &World) -> bool {
    world.cluster.metadata().list_replicas().await.is_ok()
}

/// Says whether the object store of `world` answers.
async fn is_object_store_reachable(world: &World) -> bool {
    world.cluster.artifact_store().list_prefix("").await.is_ok()
}

#[tokio::test(start_paused = true)]
async fn metadata_outage_makes_the_metadata_store_unreachable() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();

    cut_off_metadata_store(&world, LASTS_FOR, &mut repairs);

    assert!(!is_metadata_store_reachable(&world).await);
}

#[tokio::test(start_paused = true)]
async fn metadata_outage_leaves_the_object_store_reachable() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();

    cut_off_metadata_store(&world, LASTS_FOR, &mut repairs);

    assert!(is_object_store_reachable(&world).await);
}

#[tokio::test(start_paused = true)]
async fn metadata_store_is_reachable_again_after_the_planned_time() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();
    cut_off_metadata_store(&world, LASTS_FOR, &mut repairs);

    sleep(JUST_AFTER_THE_END).await;

    assert!(is_metadata_store_reachable(&world).await);
}

#[tokio::test(start_paused = true)]
async fn metadata_outage_is_recorded_with_its_duration() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();

    cut_off_metadata_store(&world, LASTS_FOR, &mut repairs);

    assert_eq!(
        fault_descriptions(&world.history),
        ["metadata store is unreachable for 3s"]
    );
}

#[tokio::test(start_paused = true)]
async fn object_store_outage_makes_the_object_store_unreachable() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();

    cut_off_object_store(&world, LASTS_FOR, &mut repairs);

    assert!(!is_object_store_reachable(&world).await);
}

#[tokio::test(start_paused = true)]
async fn object_store_outage_leaves_the_metadata_store_reachable() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();

    cut_off_object_store(&world, LASTS_FOR, &mut repairs);

    assert!(is_metadata_store_reachable(&world).await);
}

#[tokio::test(start_paused = true)]
async fn object_store_is_reachable_again_after_the_planned_time() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();
    cut_off_object_store(&world, LASTS_FOR, &mut repairs);

    sleep(JUST_AFTER_THE_END).await;

    assert!(is_object_store_reachable(&world).await);
}

#[tokio::test(start_paused = true)]
async fn object_store_outage_is_recorded_with_its_duration() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();

    cut_off_object_store(&world, LASTS_FOR, &mut repairs);

    assert_eq!(
        fault_descriptions(&world.history),
        ["object store is unreachable for 3s"]
    );
}
