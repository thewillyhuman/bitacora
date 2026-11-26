//! What a run says when its healed cluster does not recover.
//!
//! # Why this file exists
//!
//! A run that breaks an invariant has a history to show for it. A run whose cluster simply never settles has nothing:
//! the audit waits for something that does not happen, time runs out, and all the history says is that the last event
//! was long ago. Finding out why would mean replaying the seed with tracing and reading thousands of lines. Most of the
//! time one look at the state of the cluster is enough: which segment still lacks a copy, which replica holds what,
//! where the low-water mark is. This file takes that look and puts it in one line of the report.
//!
//! # What it contains
//!
//! - [`describe_stuck_cluster`]: the line. It names the step the audit was at, the stream record as the metadata store
//!   has it, and what every node holds on its disk.
//!
//! # How it works
//!
//! The description is built from three sources, in this order: the audit's own note of what it was waiting for; the
//! stream record, if the metadata store answers; and, for every node, the list of segment copies its replica reports,
//! asked directly and not through the network.
//!
//! # Invariants
//!
//! - The description is taken after the audit's time ran out. It changes nothing in the cluster and nothing in the
//!   history.
//! - It never fails: what cannot be read is left out or shown as the error it gave.
//!
//! # See also
//!
//! - `audit.rs`: the steps a cluster can be stuck at.
//! - `report.rs`: `SimulationReport::stuck`, where the description ends up.

use std::fmt::Write as _;
use std::time::Duration;

use bitacora_meta::{SegmentRecord, StreamRecord};
use bitacora_testkit::Node;
use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::{ReplicaApi, ReplicaError, SegmentStatus};

use crate::world::{STREAM, World};

/// Says what the healed cluster was still waiting for when time ran out, and what state it was in.
///
/// Reads the stream record from the metadata store and asks every running replica for its segment copies.
///
/// # Arguments
///
/// * `world` - The run whose audit did not finish.
/// * `waited` - How long the audit was given.
///
/// # Returns
///
/// One line: the step the audit was at; then, if the metadata store answers, the stream's epoch, low-water mark,
/// standby replicas and segments with the state of each copy; then, per node, the copies its replica holds, or `down`.
pub(crate) async fn describe_stuck_cluster(world: &World, waited: Duration) -> String {
    let step = world.audit_step().description();
    let mut description = format!("the healed cluster did not settle within {waited:?}; it was {step}");
    if let Ok(stream) = world.cluster.metadata().get_stream(STREAM).await {
        describe_stream(&stream, &mut description);
    }
    for node in world.cluster.nodes() {
        let held = describe_holdings(node).await;
        let _ = write!(description, "; {} holds {held:?}", node.replica_id());
    }
    description
}

/// Adds the stream record to `description`: its epoch, low-water mark, standby replicas and every segment.
fn describe_stream(stream: &StreamRecord, description: &mut String) {
    let _ = write!(
        description,
        "; epoch {}, low-water mark {}, standby {:?}, segments:",
        stream.epoch, stream.low_water_mark, stream.standby_replicas
    );
    for segment in &stream.segments {
        let _ = write!(
            description,
            " [{} from {} sealed {:?}: {}]",
            segment.segment_id,
            segment.first_sequence,
            segment.sealed_end(),
            describe_copies(segment)
        );
    }
}

/// Lists the copies of `segment` as the metadata store records them: `replica=State`, separated by spaces.
fn describe_copies(segment: &SegmentRecord) -> String {
    let mut copies = Vec::new();
    for copy in &segment.copies {
        copies.push(format!("{}={:?}", copy.replica_id, copy.state));
    }
    copies.join(" ")
}

/// Lists what the replica of `node` holds on its disk, asking the replica directly.
///
/// # Returns
///
/// One entry per segment copy, separated by spaces; `down` for a node whose replica is not running; the replica's error
/// if it cannot list its copies.
async fn describe_holdings(node: &Node) -> Result<String, ReplicaError> {
    let Some(replica) = node.replica() else {
        return Ok("down".to_owned());
    };
    let statuses = replica.list_segments().await?;
    let mut holdings = Vec::new();
    for status in &statuses {
        holdings.push(describe_copy_on_disk(status));
    }
    Ok(holdings.join(" "))
}

/// Describes one segment copy as its replica reports it: where it ends, the epoch it is fenced at, where it is sealed,
/// and whether it failed a checksum.
fn describe_copy_on_disk(status: &SegmentStatus) -> String {
    format!(
        "{}:end={:?},fenced={},sealed={:?}{}",
        status.address.segment_id,
        status.local_end.map(SequenceNumber::get),
        status.fenced_epoch,
        status.sealed_end.map(SequenceNumber::get),
        if status.is_damaged { ",DAMAGED" } else { "" }
    )
}

#[cfg(test)]
#[path = "diagnosis_test.rs"]
mod tests;
