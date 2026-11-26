//! Choosing the replicas that hold a segment.
//!
//! # Why this file exists
//!
//! Every segment lives on a fixed set of replicas, and that set decides what a failure can cost: three copies on one
//! host are one copy (design, sections 8 and 14). Three parties choose replica sets: the controller, which keeps a
//! standby set ready for every stream; the administration client, which places the first segment of a new stream; and
//! repair, which replaces a lost copy. They must all decide the same way, or the spread of a segment would depend on
//! who happened to place it. So the decision is one pure function of the replica registry, and it lives here.
//!
//! # What it contains
//!
//! - [`choose_replica_set`]: the replicas for a new segment.
//! - [`choose_additional_replica`]: one more replica for a segment that keeps some of its copies.
//! - [`PlacementPolicy`]: which replicas may receive new segments at all.
//! - [`PlacementError`]: why no choice could be made.
//!
//! # How it works
//!
//! A replica is eligible if it renewed its registration recently, is not being drained, has room, and is not excluded
//! by the caller. Copies are then placed one at a time. Each time, the eligible replica that shares the fewest failure
//! domains with the copies already placed wins, comparing the widest domain first: a replica in an unused zone beats
//! one in an unused rack of a used zone. Among equally spread replicas the one with the most free space wins, then the
//! lowest identifier, so that the same registry always gives the same answer.
//!
//! A replica's failure domains are its labels from widest to narrowest, for example zone, rack, host. The whole list
//! names its narrowest domain. Spreading over the wider levels is best effort; not sharing the narrowest is a rule: a
//! set in which two copies have the same full list of labels is refused. A replica without labels counts as a domain of
//! its own, which is what a test cluster wants and what a deployment must not rely on.
//!
//! # Invariants
//!
//! - Pure: no I/O, no clock (the caller passes the time), no randomness.
//! - Two copies of one segment never share their narrowest failure domain.
//! - The same registry, time, policy and exclusions always give the same replicas, in the same order.
//!
//! # See also
//!
//! - `records.rs`: [`ReplicaRecord`], what a replica registers about itself.
//! - `bitacora-controller/src/controller/standby.rs` and `repair.rs`: the steps of a pass that call this.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Duration;

use bitacora_types::{ReplicaId, Timestamp};
use thiserror::Error;

use crate::records::ReplicaRecord;

/// How old a replica's last heartbeat may be, by default, before placement presumes it dead. Long enough for a replica
/// to miss a renewal or two without losing its place.
const DEFAULT_LIVENESS_WINDOW: Duration = Duration::from_secs(15);

/// How full a replica's disk may be, by default, before it receives no new segments, in percent. The same threshold at
/// which a replica itself starts refusing new segments (design, section 7).
const DEFAULT_REFUSE_AT_USED_PERCENT: u8 = 85;

/// Which replicas may receive new segments.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_meta::PlacementPolicy;
///
/// let patient_policy = PlacementPolicy {
///     liveness_window: Duration::from_secs(60),
///     ..PlacementPolicy::default()
/// };
///
/// assert_eq!(patient_policy.refuse_at_used_percent, 85);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacementPolicy {
    /// A replica whose last heartbeat is older than this is presumed dead.
    pub liveness_window: Duration,
    /// A replica whose disk is at least this full, in percent, receives no new segments.
    pub refuse_at_used_percent: u8,
}

impl Default for PlacementPolicy {
    /// The policy of the design: dead after fifteen seconds of silence, full at 85 percent.
    fn default() -> Self {
        Self {
            liveness_window: DEFAULT_LIVENESS_WINDOW,
            refuse_at_used_percent: DEFAULT_REFUSE_AT_USED_PERCENT,
        }
    }
}

/// Why no replica set could be chosen.
///
/// # Examples
///
/// ```
/// use std::collections::BTreeSet;
///
/// use bitacora_meta::{PlacementError, PlacementPolicy, choose_replica_set};
/// use bitacora_types::Timestamp;
///
/// let empty_registry = [];
/// let now = Timestamp::from_unix_millis(100_000);
///
/// let chosen = choose_replica_set(&empty_registry, 3, now, PlacementPolicy::default(), &BTreeSet::new());
///
/// assert_eq!(
///     chosen,
///     Err(PlacementError::NotEnoughReplicas {
///         needed: 3,
///         eligible: 0
///     })
/// );
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum PlacementError {
    /// Fewer replicas are alive, undrained and roomy than a segment needs.
    #[error("a segment needs {needed} replicas, only {eligible} are eligible")]
    NotEnoughReplicas {
        /// How many replicas were asked for.
        needed: usize,
        /// How many replicas may receive new segments right now.
        eligible: usize,
    },
    /// Enough replicas are eligible, but only by putting two copies of the segment in the same narrowest failure
    /// domain, such as one host.
    #[error("a segment needs {needed} separate failure domains, only {available} are available")]
    NotEnoughFailureDomains {
        /// How many separate domains the segment needs: one per copy, kept copies included.
        needed: usize,
        /// How many separate domains the chosen and kept replicas span.
        available: usize,
    },
}

/// Chooses the replicas for a new segment.
///
/// Copies are spread as widely as the registry allows: across zones first, then racks, then hosts, following each
/// replica's `failure_domains` from widest to narrowest. Two copies never share the narrowest domain. Among equally
/// spread candidates the one with the most free space wins, then the lowest identifier, so the result is deterministic.
///
/// # Arguments
///
/// * `replicas` - The replica registry, as read from the metadata store.
/// * `replica_count` - How many copies the segment has.
/// * `now` - The current time, to judge which replicas are alive.
/// * `policy` - Which replicas may receive new segments.
/// * `excluded` - Replicas that must not be chosen whatever their state, for example the ones a writer is rolling away
///   from.
///
/// # Returns
///
/// The chosen replicas, in the order they were chosen.
///
/// # Errors
///
/// * [`PlacementError::NotEnoughReplicas`] if fewer than `replica_count` replicas are alive, undrained, roomy and not
///   excluded.
/// * [`PlacementError::NotEnoughFailureDomains`] if they are enough but two of the chosen ones would share their
///   narrowest failure domain.
///
/// # Examples
///
/// ```
/// use std::collections::BTreeSet;
///
/// use bitacora_meta::{PlacementPolicy, RECORD_SCHEMA_VERSION, ReplicaRecord, choose_replica_set};
/// use bitacora_types::{ReplicaId, Timestamp};
///
/// let now = Timestamp::from_unix_millis(100_000);
/// let replica_on = |name: &str, zone: &str| ReplicaRecord {
///     schema_version: RECORD_SCHEMA_VERSION,
///     replica_id: ReplicaId::new(name).expect("a valid identifier"),
///     address: format!("{name}:7000"),
///     failure_domains: vec![zone.to_owned(), format!("host-{name}")],
///     capacity_bytes: 1_000,
///     free_bytes: 900,
///     is_draining: false,
///     heartbeat_at: now,
/// };
/// let registry = [
///     replica_on("a1", "zone-a"),
///     replica_on("a2", "zone-a"),
///     replica_on("b1", "zone-b"),
/// ];
///
/// let chosen = choose_replica_set(&registry, 2, now, PlacementPolicy::default(), &BTreeSet::new())?;
///
/// let chosen_names: Vec<String> = chosen.iter().map(ToString::to_string).collect();
/// assert_eq!(chosen_names, ["a1", "b1"]);
/// # Ok::<(), bitacora_meta::PlacementError>(())
/// ```
pub fn choose_replica_set(
    replicas: &[ReplicaRecord],
    replica_count: usize,
    now: Timestamp,
    policy: PlacementPolicy,
    excluded: &BTreeSet<ReplicaId>,
) -> Result<Vec<ReplicaId>, PlacementError> {
    place(replicas, &[], replica_count, now, policy, excluded)
}

/// Chooses one more replica for a segment that keeps its copies on `kept_replicas`, as when repair replaces a lost
/// copy.
///
/// The new replica is the one that spreads the segment widest together with the copies it keeps, and never shares their
/// narrowest failure domain. A kept replica that is no longer in the registry cannot be taken into account and is
/// ignored.
///
/// # Arguments
///
/// * `replicas` - The replica registry, as read from the metadata store.
/// * `kept_replicas` - The replicas that keep their copy of the segment. None of them is chosen.
/// * `now` - The current time, to judge which replicas are alive.
/// * `policy` - Which replicas may receive new segments.
///
/// # Returns
///
/// The replica that should hold the new copy.
///
/// # Errors
///
/// * [`PlacementError::NotEnoughReplicas`] if no replica outside `kept_replicas` is alive, undrained and roomy.
/// * [`PlacementError::NotEnoughFailureDomains`] if the best one shares its narrowest failure domain with a kept copy.
///
/// # Panics
///
/// Never in practice: placing one copy successfully yields one replica.
///
/// # Examples
///
/// ```
/// use bitacora_meta::{PlacementPolicy, RECORD_SCHEMA_VERSION, ReplicaRecord, choose_additional_replica};
/// use bitacora_types::{ReplicaId, Timestamp};
///
/// let now = Timestamp::from_unix_millis(100_000);
/// let replica_named = |name: &str| ReplicaRecord {
///     schema_version: RECORD_SCHEMA_VERSION,
///     replica_id: ReplicaId::new(name).expect("a valid identifier"),
///     address: format!("{name}:7000"),
///     failure_domains: vec![format!("host-{name}")],
///     capacity_bytes: 1_000,
///     free_bytes: 900,
///     is_draining: false,
///     heartbeat_at: now,
/// };
/// let registry = [replica_named("r1"), replica_named("r2"), replica_named("r3")];
/// let kept_replicas = [
///     ReplicaId::new("r1").expect("a valid identifier"),
///     ReplicaId::new("r2").expect("a valid identifier"),
/// ];
///
/// let replacement = choose_additional_replica(&registry, &kept_replicas, now, PlacementPolicy::default())?;
///
/// assert_eq!(replacement.to_string(), "r3");
/// # Ok::<(), bitacora_meta::PlacementError>(())
/// ```
pub fn choose_additional_replica(
    replicas: &[ReplicaRecord],
    kept_replicas: &[ReplicaId],
    now: Timestamp,
    policy: PlacementPolicy,
) -> Result<ReplicaId, PlacementError> {
    let excluded: BTreeSet<ReplicaId> = kept_replicas.iter().cloned().collect();
    let mut already_placed = Vec::new();
    for replica in replicas {
        if excluded.contains(&replica.replica_id) {
            already_placed.push(replica);
        }
    }
    let mut chosen = place(replicas, &already_placed, 1, now, policy, &excluded)?;
    Ok(chosen.pop().expect("placing one replica yields one replica"))
}

/// Places `replica_count` copies next to the ones `already_placed`.
///
/// # Returns
///
/// The newly placed replicas only, in the order they were chosen.
///
/// # Errors
///
/// As [`choose_replica_set`]. The domain check counts the already placed copies too.
fn place(
    replicas: &[ReplicaRecord],
    already_placed: &[&ReplicaRecord],
    replica_count: usize,
    now: Timestamp,
    policy: PlacementPolicy,
    excluded: &BTreeSet<ReplicaId>,
) -> Result<Vec<ReplicaId>, PlacementError> {
    let mut candidates = eligible_replicas(replicas, now, policy, excluded);
    if candidates.len() < replica_count {
        return Err(PlacementError::NotEnoughReplicas {
            needed: replica_count,
            eligible: candidates.len(),
        });
    }

    let mut placed: Vec<&ReplicaRecord> = already_placed.to_vec();
    for _copy in 0..replica_count {
        let best_position = position_of_the_best_candidate(&candidates, &placed);
        placed.push(candidates.swap_remove(best_position));
    }
    require_separate_narrowest_domains(&placed)?;

    let mut newly_placed = Vec::with_capacity(replica_count);
    for replica in &placed[already_placed.len()..] {
        newly_placed.push(replica.replica_id.clone());
    }
    Ok(newly_placed)
}

/// Returns the replicas that may receive a new segment: alive, not draining, with room, and not excluded.
fn eligible_replicas<'registry>(
    replicas: &'registry [ReplicaRecord],
    now: Timestamp,
    policy: PlacementPolicy,
    excluded: &BTreeSet<ReplicaId>,
) -> Vec<&'registry ReplicaRecord> {
    let mut eligible = Vec::new();
    for replica in replicas {
        let is_alive = replica.is_alive(now, policy.liveness_window);
        let has_room = replica.used_percent() < policy.refuse_at_used_percent;
        let is_excluded = excluded.contains(&replica.replica_id);
        if is_alive && !replica.is_draining && has_room && !is_excluded {
            eligible.push(replica);
        }
    }
    eligible
}

/// Returns the position, in `candidates`, of the replica that should hold the next copy.
///
/// # Panics
///
/// Panics if there is no candidate. The caller checked that there are at least as many candidates as copies left to
/// place.
fn position_of_the_best_candidate(candidates: &[&ReplicaRecord], placed: &[&ReplicaRecord]) -> usize {
    let mut best_position = None;
    for (position, candidate) in candidates.iter().enumerate() {
        let is_better = match best_position {
            None => true,
            Some(best) => compare_candidates(candidate, candidates[best], placed) == Ordering::Less,
        };
        if is_better {
            best_position = Some(position);
        }
    }
    best_position.expect("there are at least as many candidates as copies left to place")
}

/// Orders two candidates for the next copy: `Less` means `left` is the better choice.
///
/// The one that shares fewer failure domains with the placed copies wins, widest domain first; then the one with more
/// free space; then the lower identifier.
fn compare_candidates(left: &ReplicaRecord, right: &ReplicaRecord, placed: &[&ReplicaRecord]) -> Ordering {
    let by_spread = shared_domains(left, placed).cmp(&shared_domains(right, placed));
    if by_spread != Ordering::Equal {
        return by_spread;
    }
    let by_room = right.free_bytes.cmp(&left.free_bytes);
    if by_room != Ordering::Equal {
        return by_room;
    }
    left.replica_id.cmp(&right.replica_id)
}

/// Counts, for each level from widest to narrowest, how many already chosen replicas share `candidate`'s failure domain
/// at that level. Comparing two such lists prefers the candidate that shares the fewest wide domains.
fn shared_domains(candidate: &ReplicaRecord, chosen: &[&ReplicaRecord]) -> Vec<usize> {
    let mut shared_per_level = Vec::with_capacity(candidate.failure_domains.len());
    for depth in 1..=candidate.failure_domains.len() {
        let candidate_domain = &candidate.failure_domains[..depth];
        let mut sharing_replicas = 0;
        for replica in chosen {
            if replica.failure_domains.get(..depth) == Some(candidate_domain) {
                sharing_replicas += 1;
            }
        }
        shared_per_level.push(sharing_replicas);
    }
    shared_per_level
}

/// Checks that no two of the placed replicas share their narrowest failure domain.
///
/// A replica's full list of labels names its narrowest domain. A replica without labels is a domain of its own.
///
/// # Errors
///
/// Returns [`PlacementError::NotEnoughFailureDomains`] if two placed replicas have the same full list of labels.
fn require_separate_narrowest_domains(placed: &[&ReplicaRecord]) -> Result<(), PlacementError> {
    let mut separate_domains: BTreeSet<&[String]> = BTreeSet::new();
    let mut unlabelled_replicas = 0;
    for replica in placed {
        if replica.failure_domains.is_empty() {
            unlabelled_replicas += 1;
        } else {
            separate_domains.insert(replica.failure_domains.as_slice());
        }
    }
    let available = separate_domains.len() + unlabelled_replicas;
    if available < placed.len() {
        return Err(PlacementError::NotEnoughFailureDomains {
            needed: placed.len(),
            available,
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "placement_test.rs"]
mod tests;
