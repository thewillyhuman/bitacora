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
//! - [`placement_key_of_segment`] and [`placement_key_of_new_stream`]: the number that says what is being placed,
//!   which decides between replicas that are equally good.
//! - [`spread_of`]: how widely a replica set is spread, to tell whether a set chosen earlier is still as good as one
//!   chosen now.
//! - [`PlacementPolicy`]: which replicas may receive new segments at all.
//! - [`PlacementError`]: why no choice could be made.
//!
//! # How it works
//!
//! A replica is eligible if it renewed its registration recently, is not being drained, has room, and is not excluded
//! by the caller. Copies are then placed one at a time. Each time the best eligible replica wins, by these criteria in
//! this order:
//!
//! 1. **Spread.** The replica that shares the fewest failure domains with the copies already placed, comparing the
//!    widest domain first: a replica in an unused zone beats one in an unused rack of a used zone.
//! 2. **Room, coarsely.** The replica whose disk is emptier by tenths: one that is 5% full beats one that is 15%
//!    full, and two that are 11% and 19% full are equals.
//! 3. **The placement key.** Among equals, the replica with the highest score for the key of what is being placed.
//!    The score is a hash of the key and the replica's identifier, so each key ranks the equals in an order of its
//!    own: rendezvous hashing.
//! 4. **The identifier**, lowest first, which only decides if two scores are the same number.
//!
//! Room is compared coarsely on purpose. Free space differs a little between any two replicas and changes slowly, so
//! a rule that prefers the emptier replica down to the byte gives every placement to the same one for as long as it
//! leads. On a real cluster of five replicas, 128 streams created together all got the same three replicas while the
//! two others stayed idle (decision record 0019). Tenths still fill the emptier disk first when the difference is
//! worth acting on, and leave the rest to the key, which spreads streams, and the successive segments of one stream,
//! over the replicas that are as good as each other.
//!
//! The score is fixed by this file and not by the standard library's hasher, whose results may change from one Rust
//! version to the next: FNV-1a over the eight bytes of the key, least significant first, followed by the bytes of the
//! identifier, and then the final mixing step of the Murmur 3 hash. FNV-1a alone will not do: it changes its high bits
//! little for a change in the last byte, and ranked five identifiers that differ in their last character so that one
//! of them won half of all keys.
//!
//! A replica's failure domains are its labels from widest to narrowest, for example zone, rack, host. The whole list
//! names its narrowest domain. Spreading over the wider levels is best effort; not sharing the narrowest is a rule: a
//! set in which two copies have the same full list of labels is refused. A replica without labels counts as a domain of
//! its own, which is what a test cluster wants and what a deployment must not rely on.
//!
//! # Invariants
//!
//! - Pure: no I/O, no clock (the caller passes the time), no randomness, no state. The key is an argument.
//! - Two copies of one segment never share their narrowest failure domain.
//! - The same registry, time, policy, exclusions and key always give the same replicas, in the same order, whatever
//!   the order of the registry and whichever program asks. Tests hold the keys and the score to fixed values.
//! - The key never costs spread or a tenth of room: it only orders replicas that the first two criteria rank equal.
//!
//! # See also
//!
//! - `records.rs`: [`ReplicaRecord`], what a replica registers about itself.
//! - `bitacora-controller/src/controller/standby.rs` and `repair.rs`: the steps of a pass that call this.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Duration;

use bitacora_types::{ReplicaId, SegmentId, StreamId, Timestamp};
use thiserror::Error;

use crate::records::ReplicaRecord;

/// How old a replica's last heartbeat may be, by default, before placement presumes it dead. Long enough for a replica
/// to miss a renewal or two without losing its place.
const DEFAULT_LIVENESS_WINDOW: Duration = Duration::from_secs(15);

/// How full a replica's disk may be, by default, before it receives no new segments, in percent. The same threshold at
/// which a replica itself starts refusing new segments (design, section 7).
const DEFAULT_REFUSE_AT_USED_PERCENT: u8 = 85;

/// How much of a disk counts as one step when two replicas' room is compared, in percent: a tenth.
const ROOM_STEP_PERCENT: u8 = 10;

/// The starting value of the 64-bit FNV-1a hash.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// The number the 64-bit FNV-1a hash multiplies by after each byte.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

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
/// let nothing_excluded = BTreeSet::new();
/// let placement_key = 7;
///
/// let chosen = choose_replica_set(
///     &empty_registry,
///     3,
///     now,
///     PlacementPolicy::default(),
///     &nothing_excluded,
///     placement_key,
/// );
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
/// spread candidates the one whose disk is emptier by tenths wins; among those, `placement_key` decides, so that
/// different segments go to different replicas and the same segment always to the same ones. See the file header.
///
/// # Arguments
///
/// * `replicas` - The replica registry, as read from the metadata store.
/// * `replica_count` - How many copies the segment has.
/// * `now` - The current time, to judge which replicas are alive.
/// * `policy` - Which replicas may receive new segments.
/// * `excluded` - Replicas that must not be chosen whatever their state, for example the ones a writer is rolling away
///   from.
/// * `placement_key` - What is being placed: [`placement_key_of_segment`] for a segment of a stream that exists,
///   [`placement_key_of_new_stream`] for the first segment of one that does not yet.
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
/// use bitacora_meta::{
///     PlacementPolicy, RECORD_SCHEMA_VERSION, ReplicaRecord, choose_replica_set, placement_key_of_segment,
/// };
/// use bitacora_types::{ReplicaId, SegmentId, StreamId, Timestamp};
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
/// let nothing_excluded = BTreeSet::new();
/// let placement_key = placement_key_of_segment(StreamId::new(7), SegmentId::FIRST);
///
/// let chosen = choose_replica_set(&registry, 2, now, PlacementPolicy::default(), &nothing_excluded, placement_key)?;
///
/// // One copy in each zone: the only replica of zone b holds one, and a replica of zone a the other. Which replica
/// // of zone a depends on the key.
/// let only_replica_of_zone_b = ReplicaId::new("b1").expect("a valid identifier");
/// assert_eq!(chosen.len(), 2);
/// assert!(chosen.contains(&only_replica_of_zone_b));
/// # Ok::<(), bitacora_meta::PlacementError>(())
/// ```
pub fn choose_replica_set(
    replicas: &[ReplicaRecord],
    replica_count: usize,
    now: Timestamp,
    policy: PlacementPolicy,
    excluded: &BTreeSet<ReplicaId>,
    placement_key: u64,
) -> Result<Vec<ReplicaId>, PlacementError> {
    let request = PlacementRequest {
        now,
        policy,
        excluded,
        placement_key,
    };
    place(replicas, &[], replica_count, &request)
}

/// Chooses one more replica for a segment that keeps its copies on `kept_replicas`, as when repair replaces a lost
/// copy.
///
/// The new replica is the one that spreads the segment widest together with the copies it keeps, and never shares their
/// narrowest failure domain. A kept replica that is no longer in the registry cannot be taken into account and is
/// ignored. Among replicas that would spread the segment equally, room by tenths and then `placement_key` decide, as
/// for a new segment, so that the copies of a lost replica are not all rebuilt on one neighbour.
///
/// # Arguments
///
/// * `replicas` - The replica registry, as read from the metadata store.
/// * `kept_replicas` - The replicas that keep their copy of the segment. None of them is chosen.
/// * `now` - The current time, to judge which replicas are alive.
/// * `policy` - Which replicas may receive new segments.
/// * `placement_key` - The key of the segment that gets the copy: [`placement_key_of_segment`].
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
/// use bitacora_meta::{
///     PlacementPolicy, RECORD_SCHEMA_VERSION, ReplicaRecord, choose_additional_replica, placement_key_of_segment,
/// };
/// use bitacora_types::{ReplicaId, SegmentId, StreamId, Timestamp};
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
/// let placement_key = placement_key_of_segment(StreamId::new(7), SegmentId::FIRST);
///
/// let replacement =
///     choose_additional_replica(&registry, &kept_replicas, now, PlacementPolicy::default(), placement_key)?;
///
/// assert_eq!(replacement.to_string(), "r3");
/// # Ok::<(), bitacora_meta::PlacementError>(())
/// ```
pub fn choose_additional_replica(
    replicas: &[ReplicaRecord],
    kept_replicas: &[ReplicaId],
    now: Timestamp,
    policy: PlacementPolicy,
    placement_key: u64,
) -> Result<ReplicaId, PlacementError> {
    let excluded: BTreeSet<ReplicaId> = kept_replicas.iter().cloned().collect();
    let mut already_placed = Vec::new();
    for replica in replicas {
        if excluded.contains(&replica.replica_id) {
            already_placed.push(replica);
        }
    }
    let request = PlacementRequest {
        now,
        policy,
        excluded: &excluded,
        placement_key,
    };
    let mut chosen = place(replicas, &already_placed, 1, &request)?;
    Ok(chosen.pop().expect("placing one replica yields one replica"))
}

/// The placement key of one segment of a stream that exists.
///
/// Every segment of every stream has a key of its own, so the successive segments of one stream are spread over the
/// replicas that are equally good, as are the segments of different streams.
///
/// # Arguments
///
/// * `stream_id` - The stream the segment belongs to.
/// * `segment_id` - The segment: the one being placed, or the one that gets a new copy.
///
/// # Returns
///
/// The 64-bit FNV-1a hash of sixteen bytes: the stream's identifier and then the segment's, each as eight bytes,
/// least significant first. The value is fixed: it must not change with the program or the Rust version, because two
/// programs that place the same segment must agree.
///
/// # Examples
///
/// ```
/// use bitacora_meta::placement_key_of_segment;
/// use bitacora_types::{SegmentId, StreamId};
///
/// let stream_id = StreamId::new(7);
///
/// let first_key = placement_key_of_segment(stream_id, SegmentId::new(1));
/// let second_key = placement_key_of_segment(stream_id, SegmentId::new(2));
///
/// assert_ne!(first_key, second_key);
/// ```
#[must_use]
pub fn placement_key_of_segment(stream_id: StreamId, segment_id: SegmentId) -> u64 {
    let hashed_stream_id = fnv1a_hash(FNV_OFFSET_BASIS, &stream_id.get().to_le_bytes());
    fnv1a_hash(hashed_stream_id, &segment_id.get().to_le_bytes())
}

/// The placement key of the first segment of a stream that is about to be created.
///
/// A stream gets its identifier when its record is written, which is after its first segment was placed. Until then
/// its name is what tells it from other streams.
///
/// # Arguments
///
/// * `name` - The name the stream will have.
///
/// # Returns
///
/// The 64-bit FNV-1a hash of the name's UTF-8 bytes. The value is fixed, like [`placement_key_of_segment`].
///
/// # Examples
///
/// ```
/// use bitacora_meta::placement_key_of_new_stream;
///
/// assert_ne!(placement_key_of_new_stream("orders"), placement_key_of_new_stream("payments"));
/// ```
#[must_use]
pub fn placement_key_of_new_stream(name: &str) -> u64 {
    fnv1a_hash(FNV_OFFSET_BASIS, name.as_bytes())
}

/// What one placement is given besides the registry: when it happens, which replicas may be chosen, and what is being
/// placed.
struct PlacementRequest<'request> {
    /// The current time, to judge which replicas are alive.
    now: Timestamp,
    /// Which replicas may receive new segments.
    policy: PlacementPolicy,
    /// Replicas that must not be chosen whatever their state.
    excluded: &'request BTreeSet<ReplicaId>,
    /// What is being placed, which orders replicas that are equally spread and equally full.
    placement_key: u64,
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
    request: &PlacementRequest<'_>,
) -> Result<Vec<ReplicaId>, PlacementError> {
    let mut candidates = eligible_replicas(replicas, request.now, request.policy, request.excluded);
    if candidates.len() < replica_count {
        return Err(PlacementError::NotEnoughReplicas {
            needed: replica_count,
            eligible: candidates.len(),
        });
    }

    let mut placed: Vec<&ReplicaRecord> = already_placed.to_vec();
    for _copy in 0..replica_count {
        let best_position = position_of_the_best_candidate(&candidates, &placed, request.placement_key);
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
fn position_of_the_best_candidate(
    candidates: &[&ReplicaRecord],
    placed: &[&ReplicaRecord],
    placement_key: u64,
) -> usize {
    let mut best_position = None;
    for (position, candidate) in candidates.iter().enumerate() {
        let is_better = match best_position {
            None => true,
            Some(best) => compare_candidates(candidate, candidates[best], placed, placement_key) == Ordering::Less,
        };
        if is_better {
            best_position = Some(position);
        }
    }
    best_position.expect("there are at least as many candidates as copies left to place")
}

/// Orders two candidates for the next copy: `Less` means `left` is the better choice.
///
/// The one that shares fewer failure domains with the placed copies wins, widest domain first; then the one whose disk
/// is emptier by tenths; then the one with the higher score for `placement_key`; then the lower identifier.
fn compare_candidates(
    left: &ReplicaRecord,
    right: &ReplicaRecord,
    placed: &[&ReplicaRecord],
    placement_key: u64,
) -> Ordering {
    let by_spread = shared_domains(left, placed).cmp(&shared_domains(right, placed));
    if by_spread != Ordering::Equal {
        return by_spread;
    }
    let by_room = used_tenths(left).cmp(&used_tenths(right));
    if by_room != Ordering::Equal {
        return by_room;
    }
    let left_score = rendezvous_score(placement_key, &left.replica_id);
    let right_score = rendezvous_score(placement_key, &right.replica_id);
    let by_score = right_score.cmp(&left_score);
    if by_score != Ordering::Equal {
        return by_score;
    }
    left.replica_id.cmp(&right.replica_id)
}

/// How full a replica's disk is, in tenths, rounded down: 0 for a disk that is less than 10% full.
///
/// Two replicas with the same number are equally roomy as far as placement is concerned.
fn used_tenths(replica: &ReplicaRecord) -> u8 {
    replica.used_percent() / ROOM_STEP_PERCENT
}

/// The score of a replica for one placement key. Among equally good replicas the highest score wins.
///
/// The hash is FNV-1a over the eight bytes of the key, least significant first, and then the UTF-8 bytes of the
/// replica's identifier, put through the final mixing step of the Murmur 3 hash so that every bit of the input changes
/// every bit of the score. See the file header for why both steps are needed and why the result is fixed.
fn rendezvous_score(placement_key: u64, replica_id: &ReplicaId) -> u64 {
    let hashed_key = fnv1a_hash(FNV_OFFSET_BASIS, &placement_key.to_le_bytes());
    let hashed_key_and_replica = fnv1a_hash(hashed_key, replica_id.as_str().as_bytes());
    mix_all_bits(hashed_key_and_replica)
}

/// Continues a 64-bit FNV-1a hash over `bytes`: for each byte, exclusive-or it into the hash, then multiply by the
/// FNV prime. Start from [`FNV_OFFSET_BASIS`].
fn fnv1a_hash(hash_so_far: u64, bytes: &[u8]) -> u64 {
    let mut hash = hash_so_far;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// The final mixing step of the Murmur 3 hash for 64 bits: two rounds of folding the high half into the low half and
/// multiplying, and a last fold. A change in any bit of `hash` changes about half the bits of the result.
const fn mix_all_bits(hash: u64) -> u64 {
    /// The first multiplier of the step.
    const FIRST_MULTIPLIER: u64 = 0xff51_afd7_ed55_8ccd;
    /// The second multiplier of the step.
    const SECOND_MULTIPLIER: u64 = 0xc4ce_b9fe_1a85_ec53;
    /// How far the high bits are shifted down to be folded into the low ones.
    const FOLD_SHIFT: u32 = 33;

    let mut mixed = hash;
    mixed ^= mixed >> FOLD_SHIFT;
    mixed = mixed.wrapping_mul(FIRST_MULTIPLIER);
    mixed ^= mixed >> FOLD_SHIFT;
    mixed = mixed.wrapping_mul(SECOND_MULTIPLIER);
    mixed ^= mixed >> FOLD_SHIFT;
    mixed
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

/// Says how widely a replica set is spread over failure domains, so that two sets can be compared.
///
/// Counts, for each level of failure domain, how many different domains the replicas are in. A replica with fewer
/// labels than a level is counted as a domain of its own at that level, like a replica without labels everywhere.
///
/// # Arguments
///
/// * `replicas` - The registrations of the replicas of the set.
///
/// # Returns
///
/// One number per level, widest first: with zones and hosts, how many zones and how many hosts. A list that compares
/// greater is a wider spread, the widest level deciding first: three zones beat two, and with as many zones, three
/// hosts beat two. Empty for a set without replicas.
///
/// # Examples
///
/// ```
/// use bitacora_meta::{RECORD_SCHEMA_VERSION, ReplicaRecord, spread_of};
/// use bitacora_types::{ReplicaId, Timestamp};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let replica_in = |name: &str, zone: &str| -> Result<ReplicaRecord, Box<dyn std::error::Error>> {
///     Ok(ReplicaRecord {
///         schema_version: RECORD_SCHEMA_VERSION,
///         replica_id: ReplicaId::new(name)?,
///         address: format!("{name}:7000"),
///         failure_domains: vec![zone.to_owned(), format!("host-{name}")],
///         capacity_bytes: 1_000,
///         free_bytes: 900,
///         is_draining: false,
///         heartbeat_at: Timestamp::from_unix_millis(0),
///     })
/// };
/// let first = replica_in("first", "zone-a")?;
/// let second = replica_in("second", "zone-a")?;
/// let third = replica_in("third", "zone-c")?;
///
/// // Two zones, three hosts.
/// assert_eq!(spread_of(&[&first, &second, &third]), [2, 3]);
/// # Ok(())
/// # }
/// ```
#[must_use]
pub fn spread_of(replicas: &[&ReplicaRecord]) -> Vec<usize> {
    let mut level_count = 0;
    for replica in replicas {
        level_count = level_count.max(replica.failure_domains.len());
    }
    // A set without labels still has one level: every replica is a domain of its own.
    if !replicas.is_empty() {
        level_count = level_count.max(1);
    }
    let mut domains_per_level = Vec::with_capacity(level_count);
    for depth in 1..=level_count {
        let mut labelled_domains: BTreeSet<&[String]> = BTreeSet::new();
        let mut replicas_of_their_own = 0;
        for replica in replicas {
            match replica.failure_domains.get(..depth) {
                Some(domain) => {
                    labelled_domains.insert(domain);
                }
                None => replicas_of_their_own += 1,
            }
        }
        domains_per_level.push(labelled_domains.len() + replicas_of_their_own);
    }
    domains_per_level
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
