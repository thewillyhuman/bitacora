//! Tests of choosing the replicas that hold a segment.
//!
//! # What is covered
//!
//! - Spread: copies go to separate zones, then separate racks, and never share a host.
//! - Ties: more free space wins, then the lower identifier.
//! - Eligibility: dead, draining, nearly full and excluded replicas are not chosen.
//! - Replacing one copy: the replacement restores the spread, is never a replica that keeps a copy, and never shares a
//!   host with one.
//! - The errors, with the numbers they report.
//!
//! # How the fixtures work
//!
//! `replica` builds a live, roomy replica from a name and its failure-domain labels, widest first. `choose` and
//! `additional` call the two public functions with the default policy at [`NOW`] and return names, sorted for `choose`,
//! so that an expectation reads as a list of names.

use super::*;
use crate::records::RECORD_SCHEMA_VERSION;

/// The moment every test places at. The replicas of the fixtures renewed their registration then.
const NOW: Timestamp = Timestamp::from_unix_millis(100_000);

/// A moment so long before [`NOW`] that a replica last heard from then is presumed dead.
const LONG_AGO: Timestamp = Timestamp::from_unix_millis(1_000);

/// The replica identifier `name`.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// A live replica called `name` with a disk that is one tenth full, in the given failure domains.
fn replica(name: &str, failure_domains: &[&str]) -> ReplicaRecord {
    let mut owned_domains = Vec::new();
    for domain in failure_domains {
        owned_domains.push((*domain).to_owned());
    }
    ReplicaRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        replica_id: replica_id(name),
        address: format!("{name}:7000"),
        failure_domains: owned_domains,
        capacity_bytes: 1_000,
        free_bytes: 900,
        is_draining: false,
        heartbeat_at: NOW,
    }
}

/// Places `replica_count` copies with the default policy and nothing excluded, and returns the chosen names in
/// alphabetical order.
fn choose(replicas: &[ReplicaRecord], replica_count: usize) -> Result<Vec<String>, PlacementError> {
    let chosen = choose_replica_set(
        replicas,
        replica_count,
        NOW,
        PlacementPolicy::default(),
        &BTreeSet::new(),
    )?;
    let mut names = Vec::new();
    for chosen_replica in &chosen {
        names.push(chosen_replica.to_string());
    }
    names.sort();
    Ok(names)
}

/// Chooses a replacement for a segment that keeps its copies on the replicas named in `kept`, with the default policy,
/// and returns its name.
fn additional(replicas: &[ReplicaRecord], kept: &[&str]) -> Result<String, PlacementError> {
    let mut kept_replicas = Vec::new();
    for name in kept {
        kept_replicas.push(replica_id(name));
    }
    let chosen = choose_additional_replica(replicas, &kept_replicas, NOW, PlacementPolicy::default())?;
    Ok(chosen.to_string())
}

/// Three zones with three hosts each: `a1` to `c3`.
fn three_zones_of_three_hosts() -> Vec<ReplicaRecord> {
    let mut replicas = Vec::new();
    for zone in ["a", "b", "c"] {
        for host in 1..=3 {
            replicas.push(replica(
                &format!("{zone}{host}"),
                &[&format!("zone-{zone}"), &format!("host-{zone}{host}")],
            ));
        }
    }
    replicas
}

/// How many of the `chosen` names start with `zone`.
fn copies_in_zone(chosen: &[String], zone: &str) -> usize {
    let mut copies = 0;
    for name in chosen {
        if name.starts_with(zone) {
            copies += 1;
        }
    }
    copies
}

#[test]
fn three_copies_land_in_three_zones() {
    let replicas = [
        replica("a1", &["zone-a", "host-a1"]),
        replica("a2", &["zone-a", "host-a2"]),
        replica("b1", &["zone-b", "host-b1"]),
        replica("c1", &["zone-c", "host-c1"]),
    ];

    let chosen = choose(&replicas, 3);

    assert_eq!(chosen, Ok(vec!["a1".to_owned(), "b1".to_owned(), "c1".to_owned()]));
}

#[test]
fn six_copies_spread_evenly_over_three_zones() {
    let replicas = three_zones_of_three_hosts();

    let chosen = choose(&replicas, 6).expect("nine replicas are enough for six copies");

    assert_eq!(copies_in_zone(&chosen, "a"), 2);
    assert_eq!(copies_in_zone(&chosen, "b"), 2);
    assert_eq!(copies_in_zone(&chosen, "c"), 2);
}

#[test]
fn one_zone_falls_back_to_separate_racks() {
    let replicas = [
        replica("r1", &["zone-a", "rack-1", "host-1"]),
        replica("r2", &["zone-a", "rack-1", "host-2"]),
        replica("r3", &["zone-a", "rack-2", "host-3"]),
        replica("r4", &["zone-a", "rack-3", "host-4"]),
    ];

    let chosen = choose(&replicas, 3).expect("four replicas are enough for three copies");

    assert!(chosen.contains(&"r3".to_owned()), "{chosen:?}");
    assert!(chosen.contains(&"r4".to_owned()), "{chosen:?}");
}

#[test]
fn two_copies_never_share_a_host() {
    let replicas = [
        replica("disk-1", &["zone-a", "host-1"]),
        replica("disk-2", &["zone-a", "host-1"]),
        replica("disk-3", &["zone-a", "host-2"]),
    ];

    let chosen = choose(&replicas, 3);

    assert_eq!(
        chosen,
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

    let chosen = choose(&replicas, 2).expect("two hosts are enough for two copies");

    assert!(chosen.contains(&"disk-3".to_owned()), "{chosen:?}");
}

#[test]
fn unlabelled_replicas_each_count_as_their_own_domain() {
    let replicas = [replica("r1", &[]), replica("r2", &[]), replica("r3", &[])];

    let chosen = choose(&replicas, 3);

    assert_eq!(chosen, Ok(vec!["r1".to_owned(), "r2".to_owned(), "r3".to_owned()]));
}

#[test]
fn roomier_replica_wins_among_equally_spread_ones() {
    let mut roomy = replica("r2", &["zone-a", "host-2"]);
    roomy.free_bytes = 950;
    let replicas = [replica("r1", &["zone-a", "host-1"]), roomy];

    let chosen = choose(&replicas, 1);

    assert_eq!(chosen, Ok(vec!["r2".to_owned()]));
}

#[test]
fn lowest_identifier_breaks_remaining_ties() {
    let replicas = [
        replica("r2", &["zone-a", "host-2"]),
        replica("r1", &["zone-a", "host-1"]),
    ];

    let chosen = choose(&replicas, 1);

    assert_eq!(chosen, Ok(vec!["r1".to_owned()]));
}

#[test]
fn replicas_are_returned_in_the_order_they_were_chosen() {
    let mut roomy = replica("r2", &[]);
    roomy.free_bytes = 950;
    let replicas = [replica("r1", &[]), roomy];

    let chosen = choose_replica_set(&replicas, 2, NOW, PlacementPolicy::default(), &BTreeSet::new());

    assert_eq!(chosen, Ok(vec![replica_id("r2"), replica_id("r1")]));
}

#[test]
fn dead_replicas_are_not_chosen() {
    let mut dead = replica("r1", &[]);
    dead.heartbeat_at = LONG_AGO;
    let replicas = [dead, replica("r2", &[])];

    let chosen = choose(&replicas, 2);

    assert_eq!(
        chosen,
        Err(PlacementError::NotEnoughReplicas { needed: 2, eligible: 1 })
    );
}

#[test]
fn draining_replicas_are_not_chosen() {
    let mut draining = replica("r1", &[]);
    draining.is_draining = true;
    let replicas = [draining, replica("r2", &[])];

    let chosen = choose(&replicas, 1);

    assert_eq!(chosen, Ok(vec!["r2".to_owned()]));
}

#[test]
fn nearly_full_replicas_are_not_chosen() {
    let mut nearly_full = replica("r1", &[]);
    nearly_full.free_bytes = 150;
    let replicas = [nearly_full, replica("r2", &[])];

    let chosen = choose(&replicas, 1);

    assert_eq!(chosen, Ok(vec!["r2".to_owned()]));
}

#[test]
fn excluded_replicas_are_not_chosen() {
    let replicas = [replica("r1", &[]), replica("r2", &[])];
    let excluded = BTreeSet::from([replica_id("r1")]);

    let chosen = choose_replica_set(&replicas, 1, NOW, PlacementPolicy::default(), &excluded);

    assert_eq!(chosen, Ok(vec![replica_id("r2")]));
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

    let replacement = additional(&replicas, &["a1", "c1"]);

    assert_eq!(replacement, Ok("b1".to_owned()));
}

#[test]
fn replacement_is_never_a_replica_that_keeps_a_copy() {
    let replicas = [replica("r1", &[]), replica("r2", &[]), replica("r3", &[])];

    let replacement = additional(&replicas, &["r1", "r2"]);

    assert_eq!(replacement, Ok("r3".to_owned()));
}

#[test]
fn replacement_never_shares_a_host_with_a_kept_copy() {
    let replicas = [
        replica("disk-1", &["zone-a", "host-1"]),
        replica("disk-2", &["zone-a", "host-1"]),
        replica("disk-3", &["zone-a", "host-2"]),
    ];

    let replacement = additional(&replicas, &["disk-1", "disk-3"]);

    assert_eq!(
        replacement,
        Err(PlacementError::NotEnoughFailureDomains {
            needed: 3,
            available: 2,
        })
    );
}

#[test]
fn replacement_ignores_kept_replicas_that_left_the_registry() {
    let replicas = [replica("r2", &[]), replica("r3", &[])];

    let replacement = additional(&replicas, &["r1", "r2"]);

    assert_eq!(replacement, Ok("r3".to_owned()));
}

#[test]
fn no_replacement_exists_when_every_other_replica_is_dead() {
    let mut dead = replica("r3", &[]);
    dead.heartbeat_at = LONG_AGO;
    let replicas = [replica("r1", &[]), replica("r2", &[]), dead];

    let replacement = additional(&replicas, &["r1", "r2"]);

    assert_eq!(
        replacement,
        Err(PlacementError::NotEnoughReplicas { needed: 1, eligible: 0 })
    );
}

#[test]
fn empty_registry_cannot_place_anything() {
    let chosen = choose(&[], 3);

    assert_eq!(
        chosen,
        Err(PlacementError::NotEnoughReplicas { needed: 3, eligible: 0 })
    );
}

#[test]
fn default_policy_is_the_one_of_the_design() {
    let policy = PlacementPolicy::default();

    assert_eq!(policy.liveness_window, Duration::from_secs(15));
    assert_eq!(policy.refuse_at_used_percent, 85);
}
