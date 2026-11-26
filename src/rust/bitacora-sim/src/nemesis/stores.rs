//! Outages of the two stores every part of the cluster depends on: the metadata store and the object store.
//!
//! # Why this file exists
//!
//! The metadata store holds epochs, leases, segment lists and the registry; the object store holds tiered segments and
//! the key-value store's layers. Both are single services in front of the log, and both go away now and then. A log
//! that loses data, or stops for good, when one of them is unreachable for a few seconds would be useless, so the
//! nemesis takes each away for a while: appends in flight must end as committed or unknown, never wrongly, and
//! everything must carry on when the store is back.
//!
//! # What it contains
//!
//! - [`cut_off_metadata_store`]: makes the metadata store unreachable and schedules its return.
//! - [`cut_off_object_store`]: the same for the object store.
//!
//! # How it works
//!
//! Each store has one switch, and an outage flips it. The return task flips it back after the planned duration,
//! whatever state it is in by then, so overlapping outages of one store end when the first of them does.
//!
//! # Invariants
//!
//! - An outage draws no random number of its own.
//! - The history entries are `metadata store is unreachable for D` and `object store is unreachable for D`, and are
//!   part of the fingerprint of a run.
//!
//! # See also
//!
//! - `nemesis.rs`: the loop that calls these.
//! - `bitacora-meta`: `MemoryKeyValueStore::set_unavailable`.
//! - `bitacora-artifact`: `MemoryArtifactStore::set_unavailable`.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::world::World;

/// Makes the metadata store unreachable and schedules its return.
///
/// Records the outage, makes every operation of the metadata store fail, and spawns a task that makes the store
/// reachable again after `lasts_for`.
///
/// # Arguments
///
/// * `world` - The run the fault is injected into.
/// * `lasts_for` - How long the store stays unreachable.
/// * `repairs` - The set that owns the task that brings the store back.
pub(super) fn cut_off_metadata_store(world: &Arc<World>, lasts_for: Duration, repairs: &mut JoinSet<()>) {
    world
        .history
        .fault(format!("metadata store is unreachable for {lasts_for:?}"));
    world.cluster.key_value_store().set_unavailable(true);
    let world = Arc::clone(world);
    repairs.spawn(async move {
        sleep(lasts_for).await;
        world.cluster.key_value_store().set_unavailable(false);
    });
}

/// Makes the object store unreachable and schedules its return.
///
/// Records the outage, makes every operation of the object store fail, and spawns a task that makes the store reachable
/// again after `lasts_for`.
///
/// # Arguments
///
/// * `world` - The run the fault is injected into.
/// * `lasts_for` - How long the store stays unreachable.
/// * `repairs` - The set that owns the task that brings the store back.
pub(super) fn cut_off_object_store(world: &Arc<World>, lasts_for: Duration, repairs: &mut JoinSet<()>) {
    world
        .history
        .fault(format!("object store is unreachable for {lasts_for:?}"));
    world.cluster.artifact_store().set_unavailable(true);
    let world = Arc::clone(world);
    repairs.spawn(async move {
        sleep(lasts_for).await;
        world.cluster.artifact_store().set_unavailable(false);
    });
}

#[cfg(test)]
#[path = "stores_test.rs"]
mod tests;
