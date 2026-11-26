//! Tests of choosing the replicas that hold a segment.
//!
//! # What is covered
//!
//! - Spread: copies go to separate zones, then separate racks, and never share a host.
//! - Room: a replica that is a tenth of its disk emptier wins; one that is a little emptier does not.
//! - Equals: replicas that are equally spread and equally full share the placements by the placement key, about
//!   evenly, and one key always gives the same replicas.
//! - The placement keys and the score of a replica, held to fixed values: the hash is part of what placement decides.
//! - Eligibility: dead, draining, nearly full and excluded replicas are not chosen.
//! - Replacing one copy: the replacement restores the spread, is never a replica that keeps a copy, and never shares a
//!   host with one.
//! - The errors, with the numbers they report.
//!
//! # How the fixtures work
//!
//! `replica` builds a live, roomy replica from a name and its failure-domain labels, widest first. `choose` and
//! `additional` call the two public functions with the default policy at [`NOW`] and the placement key [`KEY`], and
//! return names, sorted for `choose`, so that an expectation reads as a list of names. `choose_for` takes the key, for
//! the tests that are about it.
//!
//! The tests that count how keys spread over equal replicas compare against bounds, not exact counts, so that they say
//! what matters. They cannot fail by chance: the hash is fixed, so every count is the same on every run.

use bitacora_types::{SegmentId, StreamId};

use super::*;
use crate::records::RECORD_SCHEMA_VERSION;

/// The moment every test places at. The replicas of the fixtures renewed their registration then.
const NOW: Timestamp = Timestamp::from_unix_millis(100_000);

/// A moment so long before [`NOW`] that a replica last heard from then is presumed dead.
const LONG_AGO: Timestamp = Timestamp::from_unix_millis(1_000);

/// The placement key of the tests that are not about the key.
const KEY: u64 = 7;

/// How many keys the tests about equals place with.
const KEY_COUNT: u64 = 1_000;

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

/// Places `replica_count` copies for [`KEY`] with the default policy and nothing excluded, and returns the chosen
/// names in alphabetical order.
fn choose(replicas: &[ReplicaRecord], replica_count: usize) -> Result<Vec<String>, PlacementError> {
    choose_for(replicas, replica_count, KEY)
}

/// Places `replica_count` copies for `placement_key` with the default policy and nothing excluded, and returns the
/// chosen names in alphabetical order.
fn choose_for(
    replicas: &[ReplicaRecord],
    replica_count: usize,
    placement_key: u64,
) -> Result<Vec<String>, PlacementError> {
    let chosen = choose_replica_set(
        replicas,
        replica_count,
        NOW,
        PlacementPolicy::default(),
        &BTreeSet::new(),
        placement_key,
    )?;
    let mut names = Vec::new();
    for chosen_replica in &chosen {
        names.push(chosen_replica.to_string());
    }
    names.sort();
    Ok(names)
}

/// How many of the keys `0..KEY_COUNT` place their one copy on the replica called `name`.
fn keys_placed_on(replicas: &[ReplicaRecord], name: &str) -> u64 {
    let mut keys = 0;
    for placement_key in 0..KEY_COUNT {
        let chosen = choose_for(replicas, 1, placement_key).expect("one replica is enough for one copy");
        if chosen == [name] {
            keys += 1;
        }
    }
    keys
}

/// Chooses a replacement for a segment that keeps its copies on the replicas named in `kept`, with the default policy,
/// and returns its name.
fn additional(replicas: &[ReplicaRecord], kept: &[&str]) -> Result<String, PlacementError> {
    let mut kept_replicas = Vec::new();
    for name in kept {
        kept_replicas.push(replica_id(name));
    }
    let chosen = choose_additional_replica(replicas, &kept_replicas, NOW, PlacementPolicy::default(), KEY)?;
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

    let chosen = choose(&replicas, 3).expect("four replicas are enough for three copies");

    assert_eq!(copies_in_zone(&chosen, "a"), 1);
    assert_eq!(copies_in_zone(&chosen, "b"), 1);
    assert_eq!(copies_in_zone(&chosen, "c"), 1);
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

/// A replica whose disk is a tenth emptier is clearly emptier: it gets every placement, whatever the key.
#[test]
fn replica_a_tenth_emptier_wins_among_equally_spread_ones_for_every_key() {
    let mut roomy = replica("r2", &["zone-a", "host-2"]);
    roomy.free_bytes = 950;
    let replicas = [replica("r1", &["zone-a", "host-1"]), roomy];

    let keys_on_the_roomy_one = keys_placed_on(&replicas, "r2");

    assert_eq!(keys_on_the_roomy_one, KEY_COUNT);
}

/// What the old rule got wrong on a real cluster: one replica of a zone was a few megabytes emptier than its
/// neighbour and got the first segment of every stream. A difference inside a tenth of the disk no longer counts.
#[test]
fn replica_a_little_emptier_gets_about_half_of_the_keys() {
    let mut a_little_fuller = replica("r2", &["zone-a", "host-2"]);
    a_little_fuller.free_bytes = 899;
    let replicas = [replica("r1", &["zone-a", "host-1"]), a_little_fuller];

    let keys_on_the_emptier_one = keys_placed_on(&replicas, "r1");

    assert!(
        (400..=600).contains(&keys_on_the_emptier_one),
        "{keys_on_the_emptier_one}"
    );
}

#[test]
fn two_equal_replicas_get_about_half_of_the_keys_each() {
    let replicas = [
        replica("r2", &["zone-a", "host-2"]),
        replica("r1", &["zone-a", "host-1"]),
    ];

    let keys_on_the_first = keys_placed_on(&replicas, "r1");

    assert!((400..=600).contains(&keys_on_the_first), "{keys_on_the_first}");
}

/// Identifiers that differ in their last character only, which is what the replicas of a test cluster and of many
/// real ones are called. A hash that ranked those unevenly would pile streams up again.
#[test]
fn five_equal_replicas_get_about_a_fifth_of_the_keys_each() {
    let replicas = [
        replica("r1", &[]),
        replica("r2", &[]),
        replica("r3", &[]),
        replica("r4", &[]),
        replica("r5", &[]),
    ];

    for name in ["r1", "r2", "r3", "r4", "r5"] {
        let keys_on_this_one = keys_placed_on(&replicas, name);

        assert!((150..=250).contains(&keys_on_this_one), "{name}: {keys_on_this_one}");
    }
}

#[test]
fn one_key_always_gives_the_same_replicas() {
    let replicas = three_zones_of_three_hosts();

    let first_answer = choose_replica_set(&replicas, 3, NOW, PlacementPolicy::default(), &BTreeSet::new(), KEY);
    let second_answer = choose_replica_set(&replicas, 3, NOW, PlacementPolicy::default(), &BTreeSet::new(), KEY);

    assert_eq!(first_answer, second_answer);
}

#[test]
fn order_of_the_registry_does_not_change_the_choice() {
    let replicas = three_zones_of_three_hosts();
    let mut reversed_replicas = replicas.clone();
    reversed_replicas.reverse();

    let from_the_registry = choose_replica_set(&replicas, 3, NOW, PlacementPolicy::default(), &BTreeSet::new(), KEY);
    let from_the_reversed_one = choose_replica_set(
        &reversed_replicas,
        3,
        NOW,
        PlacementPolicy::default(),
        &BTreeSet::new(),
        KEY,
    );

    assert_eq!(from_the_registry, from_the_reversed_one);
}

/// A stream's next segment has another key than its last one, so a stream does not stay on one replica of a zone for
/// its whole life.
#[test]
fn segments_of_one_stream_use_every_replica_of_a_zone() {
    let replicas = three_zones_of_three_hosts();
    let stream_id = StreamId::new(7);
    let mut replicas_used_in_zone_a = BTreeSet::new();

    for raw_segment_id in 1..=16 {
        let placement_key = placement_key_of_segment(stream_id, SegmentId::new(raw_segment_id));
        let chosen = choose_for(&replicas, 3, placement_key).expect("nine replicas are enough for three copies");
        for name in chosen {
            if name.starts_with('a') {
                replicas_used_in_zone_a.insert(name);
            }
        }
    }

    assert_eq!(replicas_used_in_zone_a.len(), 3, "{replicas_used_in_zone_a:?}");
}

/// The cluster the old rule was caught on: five replicas in three zones of one, two and two, and 128 streams created
/// together. Every stream got the same three replicas, and the two others stayed idle.
#[test]
fn streams_created_together_use_both_replicas_of_a_zone() {
    let replicas = [
        replica("a1", &["zone-a", "host-a1"]),
        replica("b1", &["zone-b", "host-b1"]),
        replica("b2", &["zone-b", "host-b2"]),
        replica("c1", &["zone-c", "host-c1"]),
        replica("c2", &["zone-c", "host-c2"]),
    ];
    let mut streams_on_b1 = 0;

    for stream_number in 0..128 {
        let placement_key = placement_key_of_new_stream(&format!("stream-{stream_number}"));
        let chosen = choose_for(&replicas, 3, placement_key).expect("five replicas are enough for three copies");
        if chosen.contains(&"b1".to_owned()) {
            streams_on_b1 += 1;
        }
    }

    assert!((44..=84).contains(&streams_on_b1), "{streams_on_b1}");
}

#[test]
fn key_of_a_segment_is_the_fixed_hash_of_its_two_identifiers() {
    let placement_key = placement_key_of_segment(StreamId::new(7), SegmentId::new(1));

    assert_eq!(placement_key, 0xba20_a56a_32af_65c3);
}

#[test]
fn segments_of_one_stream_have_different_keys() {
    let stream_id = StreamId::new(7);

    let first_key = placement_key_of_segment(stream_id, SegmentId::new(1));
    let second_key = placement_key_of_segment(stream_id, SegmentId::new(2));

    assert_ne!(first_key, second_key);
}

#[test]
fn key_of_a_new_stream_is_the_fixed_hash_of_its_name() {
    let placement_key = placement_key_of_new_stream("orders");

    assert_eq!(placement_key, 0x0012_5d92_50be_8b4c);
}

#[test]
fn score_of_a_replica_is_the_fixed_hash_of_the_key_and_its_identifier() {
    let score = rendezvous_score(KEY, &replica_id("replica-a"));

    assert_eq!(score, 0x817c_4c74_d62e_26a2);
}

#[test]
fn replicas_are_returned_in_the_order_they_were_chosen() {
    let mut roomy = replica("r2", &[]);
    roomy.free_bytes = 950;
    let replicas = [replica("r1", &[]), roomy];

    let chosen = choose_replica_set(&replicas, 2, NOW, PlacementPolicy::default(), &BTreeSet::new(), KEY);

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

    let chosen = choose_replica_set(&replicas, 1, NOW, PlacementPolicy::default(), &excluded, KEY);

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

    let replacement = additional(&replicas, &["a1", "c1"]).expect("zone b has two replicas to choose from");

    assert!(replacement.starts_with('b'), "{replacement}");
}

/// A lost replica's copies are rebuilt by key too, so that they do not all land on one neighbour.
#[test]
fn replacements_for_different_keys_use_both_replicas_of_the_zone() {
    let replicas = [
        replica("a1", &["zone-a", "host-a1"]),
        replica("b1", &["zone-b", "host-b1"]),
        replica("b2", &["zone-b", "host-b2"]),
        replica("c1", &["zone-c", "host-c1"]),
    ];
    let kept_replicas = [replica_id("a1"), replica_id("c1")];
    let mut replacements_on_b1 = 0;

    for placement_key in 0..KEY_COUNT {
        let replacement = choose_additional_replica(
            &replicas,
            &kept_replicas,
            NOW,
            PlacementPolicy::default(),
            placement_key,
        );
        if replacement == Ok(replica_id("b1")) {
            replacements_on_b1 += 1;
        }
    }

    assert!((400..=600).contains(&replacements_on_b1), "{replacements_on_b1}");
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

/// The spread of the replicas with the given names and failure domains.
fn spread(replicas: &[ReplicaRecord]) -> Vec<usize> {
    let mut borrowed = Vec::new();
    for replica in replicas {
        borrowed.push(replica);
    }
    spread_of(&borrowed)
}

#[test]
fn spread_counts_the_domains_of_each_level_widest_first() {
    let replicas = [
        replica("a1", &["zone-a", "host-a1"]),
        replica("a2", &["zone-a", "host-a2"]),
        replica("c1", &["zone-c", "host-c1"]),
    ];

    assert_eq!(spread(&replicas), [2, 3]);
}

/// The comparison a controller makes when a zone has come back: the set placed without it is the narrower one.
#[test]
fn set_in_three_zones_is_spread_wider_than_one_in_two() {
    let in_two_zones = [
        replica("a1", &["zone-a", "host-a1"]),
        replica("a2", &["zone-a", "host-a2"]),
        replica("c1", &["zone-c", "host-c1"]),
    ];
    let in_three_zones = [
        replica("a1", &["zone-a", "host-a1"]),
        replica("b1", &["zone-b", "host-b1"]),
        replica("c1", &["zone-c", "host-c1"]),
    ];

    assert!(spread(&in_three_zones) > spread(&in_two_zones));
}

#[test]
fn sets_in_the_same_zones_on_different_hosts_are_spread_equally() {
    let on_the_first_hosts = [
        replica("a1", &["zone-a", "host-a1"]),
        replica("b1", &["zone-b", "host-b1"]),
        replica("c1", &["zone-c", "host-c1"]),
    ];
    let on_the_second_hosts = [
        replica("a2", &["zone-a", "host-a2"]),
        replica("b2", &["zone-b", "host-b2"]),
        replica("c2", &["zone-c", "host-c2"]),
    ];

    assert_eq!(spread(&on_the_first_hosts), spread(&on_the_second_hosts));
}

#[test]
fn unlabelled_replicas_are_spread_as_one_domain_each() {
    let replicas = [replica("one", &[]), replica("two", &[]), replica("three", &[])];

    assert_eq!(spread(&replicas), [3]);
}

#[test]
fn no_replicas_have_no_spread() {
    assert_eq!(spread(&[]), Vec::<usize>::new());
}
