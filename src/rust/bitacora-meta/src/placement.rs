//! Choosing the replica set of a segment (design, sections 8 and 14).
//!
//! Placement is a pure function of the replica registry, so that the
//! controller, which keeps a standby segment ready for every stream, and the
//! admin client, which places the first segment of a new stream, decide the
//! same way.

use std::collections::BTreeSet;
use std::time::Duration;

use bitacora_types::{ReplicaId, Timestamp};
use thiserror::Error;

use crate::records::ReplicaRecord;

/// Which replicas may receive new segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacementPolicy {
    /// A replica whose last heartbeat is older than this is presumed dead.
    pub liveness_window: Duration,
    /// A replica whose disk is at least this full receives no new segments.
    pub refuse_at_used_percent: u8,
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        Self {
            liveness_window: Duration::from_secs(15),
            refuse_at_used_percent: 85,
        }
    }
}

/// Why no replica set could be chosen.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum PlacementError {
    /// Fewer replicas are alive, undrained and roomy than a segment needs.
    #[error("a segment needs {needed} replicas, only {eligible} are eligible")]
    NotEnoughReplicas {
        /// How many replicas a segment needs.
        needed: usize,
        /// How many replicas may receive new segments right now.
        eligible: usize,
    },
    /// Enough replicas are eligible, but only by putting two copies of the
    /// segment in the same narrowest failure domain, such as one host.
    #[error("a segment needs {needed} separate failure domains, only {available} are available")]
    NotEnoughFailureDomains {
        /// How many separate domains a segment needs.
        needed: usize,
        /// How many separate domains the eligible replicas span.
        available: usize,
    },
}

/// Chooses `replica_count` replicas for a new segment.
///
/// Copies are spread as widely as the registry allows: across zones first,
/// then racks, then hosts, following each replica's `failure_domains` from
/// widest to narrowest. Two copies never share the narrowest domain. Among
/// equally spread candidates the one with the most free space wins, then the
/// lowest identifier, so the result is deterministic.
///
/// # Errors
///
/// Returns [`PlacementError`] if too few replicas are alive, undrained,
/// roomy and not in `excluded`, or if they do not span enough domains.
pub fn choose_replica_set(
    replicas: &[ReplicaRecord],
    replica_count: usize,
    now: Timestamp,
    policy: PlacementPolicy,
    excluded: &BTreeSet<ReplicaId>,
) -> Result<Vec<ReplicaId>, PlacementError> {
    place(replicas, &[], replica_count, now, policy, excluded)
}

/// Chooses one more replica for a segment that keeps its copies on
/// `kept_replicas`, as when repair replaces a lost copy.
///
/// The new replica is the one that spreads the segment widest together
/// with the copies it keeps, and never shares their narrowest domain.
///
/// # Errors
///
/// Returns [`PlacementError`] if no replica outside `kept_replicas` is
/// alive, undrained and roomy, or none is in a domain of its own.
pub fn choose_additional_replica(
    replicas: &[ReplicaRecord],
    kept_replicas: &[ReplicaId],
    now: Timestamp,
    policy: PlacementPolicy,
) -> Result<ReplicaId, PlacementError> {
    let excluded: BTreeSet<ReplicaId> = kept_replicas.iter().cloned().collect();
    let already_placed: Vec<&ReplicaRecord> = replicas
        .iter()
        .filter(|replica| excluded.contains(&replica.replica_id))
        .collect();
    let chosen = place(replicas, &already_placed, 1, now, policy, &excluded)?;
    Ok(chosen
        .into_iter()
        .next()
        .expect("placing one replica yields one replica"))
}

/// Places `replica_count` copies next to the ones `already_placed`.
fn place(
    replicas: &[ReplicaRecord],
    already_placed: &[&ReplicaRecord],
    replica_count: usize,
    now: Timestamp,
    policy: PlacementPolicy,
    excluded: &BTreeSet<ReplicaId>,
) -> Result<Vec<ReplicaId>, PlacementError> {
    let mut candidates: Vec<&ReplicaRecord> = replicas
        .iter()
        .filter(|replica| {
            replica.is_alive(now, policy.liveness_window)
                && !replica.is_draining
                && replica.used_percent() < policy.refuse_at_used_percent
                && !excluded.contains(&replica.replica_id)
        })
        .collect();
    if candidates.len() < replica_count {
        return Err(PlacementError::NotEnoughReplicas {
            needed: replica_count,
            eligible: candidates.len(),
        });
    }

    let mut placed: Vec<&ReplicaRecord> = already_placed.to_vec();
    while placed.len() < already_placed.len() + replica_count {
        let best_position = candidates
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                shared_domains(left, &placed)
                    .cmp(&shared_domains(right, &placed))
                    .then(right.free_bytes.cmp(&left.free_bytes))
                    .then(left.replica_id.cmp(&right.replica_id))
            })
            .map(|(position, _candidate)| position)
            .expect("there are at least as many candidates as copies left to place");
        placed.push(candidates.swap_remove(best_position));
    }

    let separate_domains: BTreeSet<&[String]> = placed
        .iter()
        .filter(|replica| !replica.failure_domains.is_empty())
        .map(|replica| replica.failure_domains.as_slice())
        .collect();
    let unlabelled_count = placed
        .iter()
        .filter(|replica| replica.failure_domains.is_empty())
        .count();
    let available = separate_domains.len() + unlabelled_count;
    if available < placed.len() {
        return Err(PlacementError::NotEnoughFailureDomains {
            needed: placed.len(),
            available,
        });
    }
    Ok(placed[already_placed.len()..]
        .iter()
        .map(|replica| replica.replica_id.clone())
        .collect())
}

/// Counts, for each level from widest to narrowest, how many already chosen
/// replicas share `candidate`'s failure domain at that level. Comparing two
/// such lists prefers the candidate that shares the fewest wide domains.
fn shared_domains(candidate: &ReplicaRecord, chosen: &[&ReplicaRecord]) -> Vec<usize> {
    (1..=candidate.failure_domains.len())
        .map(|depth| {
            let candidate_domain = &candidate.failure_domains[..depth];
            chosen
                .iter()
                .filter(|replica| replica.failure_domains.get(..depth) == Some(candidate_domain))
                .count()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::records::RECORD_SCHEMA_VERSION;

    use super::*;

    const NOW: Timestamp = Timestamp::from_unix_millis(100_000);

    fn replica(name: &str, failure_domains: &[&str]) -> ReplicaRecord {
        ReplicaRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            replica_id: ReplicaId::new(name).expect("valid identifier"),
            address: format!("{name}:7000"),
            failure_domains: failure_domains.iter().map(|domain| (*domain).to_owned()).collect(),
            capacity_bytes: 1_000,
            free_bytes: 900,
            is_draining: false,
            heartbeat_at: NOW,
        }
    }

    fn choose(replicas: &[ReplicaRecord], replica_count: usize) -> Result<Vec<String>, PlacementError> {
        choose_replica_set(
            replicas,
            replica_count,
            NOW,
            PlacementPolicy::default(),
            &BTreeSet::new(),
        )
        .map(|chosen| {
            let mut names: Vec<String> = chosen.iter().map(ToString::to_string).collect();
            names.sort();
            names
        })
    }

    #[test]
    fn three_copies_land_in_three_zones() {
        let replicas = [
            replica("a1", &["zone-a", "host-a1"]),
            replica("a2", &["zone-a", "host-a2"]),
            replica("b1", &["zone-b", "host-b1"]),
            replica("c1", &["zone-c", "host-c1"]),
        ];
        assert_eq!(choose(&replicas, 3), Ok(vec!["a1".into(), "b1".into(), "c1".into()]));
    }

    #[test]
    fn six_copies_spread_evenly_over_three_zones() {
        let mut replicas = Vec::new();
        for zone in ["a", "b", "c"] {
            for host in 1..=3 {
                replicas.push(replica(
                    &format!("{zone}{host}"),
                    &[&format!("zone-{zone}"), &format!("host-{zone}{host}")],
                ));
            }
        }
        let chosen = choose(&replicas, 6).expect("placement succeeds");
        let copies_per_zone: Vec<usize> = ["a", "b", "c"]
            .iter()
            .map(|zone| chosen.iter().filter(|name| name.starts_with(zone)).count())
            .collect();
        assert_eq!(copies_per_zone, [2, 2, 2]);
    }

    #[test]
    fn one_zone_falls_back_to_separate_racks() {
        let replicas = [
            replica("r1", &["zone-a", "rack-1", "host-1"]),
            replica("r2", &["zone-a", "rack-1", "host-2"]),
            replica("r3", &["zone-a", "rack-2", "host-3"]),
            replica("r4", &["zone-a", "rack-3", "host-4"]),
        ];
        let chosen = choose(&replicas, 3).expect("placement succeeds");
        assert!(
            chosen.contains(&"r3".to_owned()) && chosen.contains(&"r4".to_owned()),
            "{chosen:?}"
        );
    }

    #[test]
    fn two_copies_never_share_a_host() {
        let replicas = [
            replica("disk-1", &["zone-a", "host-1"]),
            replica("disk-2", &["zone-a", "host-1"]),
            replica("disk-3", &["zone-a", "host-2"]),
        ];
        assert_eq!(
            choose(&replicas, 3),
            Err(PlacementError::NotEnoughFailureDomains {
                needed: 3,
                available: 2,
            })
        );
    }

    #[test]
    fn copies_avoid_a_shared_host_when_another_host_exists() {
        let replicas = [
            replica("disk-1", &["zone-a", "host-1"]),
            replica("disk-2", &["zone-a", "host-1"]),
            replica("disk-3", &["zone-a", "host-2"]),
        ];
        let chosen = choose(&replicas, 2).expect("placement succeeds");
        assert!(chosen.contains(&"disk-3".to_owned()), "{chosen:?}");
    }

    #[test]
    fn unlabelled_replicas_each_count_as_their_own_domain() {
        let replicas = [replica("r1", &[]), replica("r2", &[]), replica("r3", &[])];
        assert_eq!(choose(&replicas, 3), Ok(vec!["r1".into(), "r2".into(), "r3".into()]));
    }

    #[test]
    fn roomier_replica_wins_among_equally_spread_ones() {
        let mut roomy = replica("r2", &["zone-a", "host-2"]);
        roomy.free_bytes = 950;
        let replicas = [replica("r1", &["zone-a", "host-1"]), roomy];
        assert_eq!(choose(&replicas, 1), Ok(vec!["r2".into()]));
    }

    #[test]
    fn lowest_identifier_breaks_remaining_ties() {
        let replicas = [
            replica("r2", &["zone-a", "host-2"]),
            replica("r1", &["zone-a", "host-1"]),
        ];
        assert_eq!(choose(&replicas, 1), Ok(vec!["r1".into()]));
    }

    #[test]
    fn dead_replicas_are_not_chosen() {
        let mut dead = replica("r1", &[]);
        dead.heartbeat_at = Timestamp::from_unix_millis(1_000);
        let replicas = [dead, replica("r2", &[])];
        assert_eq!(
            choose(&replicas, 2),
            Err(PlacementError::NotEnoughReplicas { needed: 2, eligible: 1 })
        );
    }

    #[test]
    fn draining_replicas_are_not_chosen() {
        let mut draining = replica("r1", &[]);
        draining.is_draining = true;
        let replicas = [draining, replica("r2", &[])];
        assert_eq!(choose(&replicas, 1), Ok(vec!["r2".into()]));
    }

    #[test]
    fn nearly_full_replicas_are_not_chosen() {
        let mut nearly_full = replica("r1", &[]);
        nearly_full.free_bytes = 150;
        let replicas = [nearly_full, replica("r2", &[])];
        assert_eq!(choose(&replicas, 1), Ok(vec!["r2".into()]));
    }

    #[test]
    fn excluded_replicas_are_not_chosen() {
        let replicas = [replica("r1", &[]), replica("r2", &[])];
        let excluded = BTreeSet::from([ReplicaId::new("r1").expect("valid identifier")]);
        let chosen = choose_replica_set(&replicas, 1, NOW, PlacementPolicy::default(), &excluded);
        assert_eq!(chosen, Ok(vec![ReplicaId::new("r2").expect("valid identifier")]));
    }

    fn additional(replicas: &[ReplicaRecord], kept: &[&str]) -> Result<String, PlacementError> {
        let kept_replicas: Vec<ReplicaId> = kept
            .iter()
            .map(|name| ReplicaId::new(*name).expect("valid identifier"))
            .collect();
        choose_additional_replica(replicas, &kept_replicas, NOW, PlacementPolicy::default())
            .map(|chosen| chosen.to_string())
    }

    #[test]
    fn replacement_goes_to_the_zone_that_lost_its_copy() {
        let replicas = [
            replica("a1", &["zone-a", "host-a1"]),
            replica("a2", &["zone-a", "host-a2"]),
            replica("b1", &["zone-b", "host-b1"]),
            replica("b2", &["zone-b", "host-b2"]),
            replica("c1", &["zone-c", "host-c1"]),
        ];
        assert_eq!(additional(&replicas, &["a1", "c1"]), Ok("b1".to_owned()));
    }

    #[test]
    fn replacement_is_never_a_replica_that_keeps_a_copy() {
        let replicas = [replica("r1", &[]), replica("r2", &[]), replica("r3", &[])];
        assert_eq!(additional(&replicas, &["r1", "r2"]), Ok("r3".to_owned()));
    }

    #[test]
    fn replacement_never_shares_a_host_with_a_kept_copy() {
        let replicas = [
            replica("disk-1", &["zone-a", "host-1"]),
            replica("disk-2", &["zone-a", "host-1"]),
            replica("disk-3", &["zone-a", "host-2"]),
        ];
        assert_eq!(
            additional(&replicas, &["disk-1", "disk-3"]),
            Err(PlacementError::NotEnoughFailureDomains {
                needed: 3,
                available: 2,
            })
        );
    }

    #[test]
    fn replacement_ignores_kept_replicas_that_left_the_registry() {
        let replicas = [replica("r2", &[]), replica("r3", &[])];
        assert_eq!(additional(&replicas, &["r1", "r2"]), Ok("r3".to_owned()));
    }

    #[test]
    fn no_replacement_exists_when_every_other_replica_is_dead() {
        let mut dead = replica("r3", &[]);
        dead.heartbeat_at = Timestamp::from_unix_millis(1_000);
        let replicas = [replica("r1", &[]), replica("r2", &[]), dead];
        assert_eq!(
            additional(&replicas, &["r1", "r2"]),
            Err(PlacementError::NotEnoughReplicas { needed: 1, eligible: 0 })
        );
    }

    #[test]
    fn empty_registry_cannot_place_anything() {
        assert_eq!(
            choose(&[], 3),
            Err(PlacementError::NotEnoughReplicas { needed: 3, eligible: 0 })
        );
    }
}
