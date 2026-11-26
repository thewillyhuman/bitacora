//! The `bita metadata rebuild` command: stream records from what replicas and the object store hold.
//!
//! # Why this file exists
//!
//! The metadata store says which segments make up a stream and where each ends. If it is lost and no
//! snapshot is recent enough, the records are still on the replicas and in the object store, but nothing
//! says how they fit together. The design names a last resort for that (section 14): reconstruct the
//! segment lists by scanning segment headers on replicas and in the object store. This file is that
//! reconstruction.
//!
//! # What it contains
//!
//! - [`rebuild`]: asks the replicas and the object store, plans, and writes the plan if told to.
//! - [`plan_rebuild`]: the plan itself, as a pure function of what was found.
//! - [`RebuildInputs`], [`ReplicaInventory`], [`TieredObject`]: what was found.
//! - [`PlannedStream`]: the record planned for one stream, with every assumption and problem.
//! - [`RebuildReport`] and its parts: what the operator is shown.
//! - Private steps, in the order the file reads: `collect_evidence`, `plan_stream` and `plan_record` with one
//!   function per rule below, the report's rendering, then the gathering (`replicas_to_ask`,
//!   `gather_inventories`, `find_tiered_objects`, `gather_what_is_known`) and `apply_plan`.
//!
//! # How it works
//!
//! **Gathering.** Every registered replica, and every replica named on the command line, is asked for its
//! segment copies. Each copy with a file reports its header: the segment's epoch and first sequence
//! number. Every object under `streams/` in the object store has its footer read, which says the same and
//! the sealed end.
//!
//! **Planning**, per stream identifier found:
//!
//! 1. A segment is *located* if a file or an object says where it starts. Segments known only from
//!    fencing or sealing state hold no records and are left out, but their identifiers and epochs count.
//! 2. A located segment's sealed end is what a replica or the object recorded. If nothing recorded it and
//!    a later located segment exists, the end is one below that segment's start: a segment is only ever
//!    opened after its predecessor was sealed, and it starts right after it. The last located segment
//!    without a recorded seal is left open, for the next writer's recovery to seal.
//! 3. Consecutive segments must connect. If two do not, records in between are lost; the stream is then
//!    rebuilt from the segment after the last such gap, and the gap is reported as a problem.
//! 4. Every replica that reported a segment becomes a copy in state `unverified`, longest copy first, so
//!    that the controller verifies each one against the sealed end. If fewer than the replication's
//!    number were found, the rest are recorded as copies on replicas that do not exist, in state `bad`,
//!    which is what makes the controller rebuild them elsewhere. A segment with an object and no file on
//!    any replica is recorded as left to the object store.
//! 5. The epoch is set one above every epoch found anywhere, so that the next writer fences everything.
//!
//! **What cannot be recovered** is taken from the stream's record if the metadata store still has one (for
//! example after a restore), and otherwise assumed and reported: the name (must be given with `--name`),
//! the replication (assumed three replicas, write quorum two), the retention policy (none), the
//! registered materializers (none: nothing is truncated until they are registered again), the low-water
//! mark (the first sequence number still found), and when each segment was sealed (now).
//!
//! **Applying.** Without `--apply` the plan is only shown. With it, every planned record is written with
//! `MetadataStore::adopt_stream`. A plan with problems is not applied unless forced.
//!
//! Assumptions and problems are sentences for the operator. The planning functions append them to the
//! two lists of the stream they plan, in the order of the rules above, so the report reads in that order.
//!
//! # Invariants
//!
//! - Planning is pure: the same inventory gives the same plan, and nothing is asked or written.
//! - Nothing is written without `--apply`, and nothing with problems without `--force`.
//! - A stream recorded as deleted is never rebuilt: its records are being removed on purpose.
//! - No replica is told to change anything. Rebuilding only writes to the metadata store.
//! - [`rebuild`] asks in a fixed order: the registered replicas, each replica's copies, the object store,
//!   the deleted streams, the existing streams. Nothing is written before all of it was read.
//!
//! # See also
//!
//! - `commands/metadata.rs`: restoring a snapshot, which comes first when there is one.
//! - `bitacora-meta/src/store/recovery.rs`: `adopt_stream`.
//! - `bitacora-segment/src/tiered.rs`: the footer of a tiered object.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use bitacora_artifact::{ArtifactStore, ObjectInfo};
use bitacora_meta::{
    CopyRecord, CopyState, RECORD_SCHEMA_VERSION, RetentionPolicy, SealRecord, SegmentRecord, StreamRecord,
    TieredRecord,
};
use bitacora_segment::tiered::{TIERED_FOOTER_BYTES, TieredFooter};
use bitacora_types::replica_api::SegmentStatus;
use bitacora_types::{
    Epoch, ReplicaId, ReplicationSettings, SegmentAddress, SegmentId, SequenceNumber, StreamId, Timestamp,
};
use serde::Serialize;
use tokio::time::timeout;

use super::CommandError;
use super::segment::{SegmentRow, segment_row, segment_table};
use crate::cluster::Cluster;
use crate::output::Table;

/// The key prefix of every tiered segment object.
const SEGMENT_OBJECTS_PREFIX: &str = "streams/";

/// What the identifier of a copy that was not found starts with. No replica has such an identifier, so
/// the controller takes the copy for lost and rebuilds it on a real replica.
const MISSING_COPY_PREFIX: &str = "rebuild-missing-";

/// What one replica answered when asked for its segment copies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaInventory {
    /// The replica that answered.
    pub(crate) replica_id: ReplicaId,
    /// Every segment copy it holds, files and bare fencing or sealing state alike.
    pub(crate) copies: Vec<SegmentStatus>,
}

/// One tiered segment object and what its footer says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TieredObject {
    /// The key of the object.
    pub(crate) object_key: String,
    /// The size of the object.
    pub(crate) size_bytes: u64,
    /// The object's verified footer: the segment's identity, start and sealed end.
    pub(crate) footer: TieredFooter,
}

/// Everything a rebuild is planned from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RebuildInputs {
    /// The stream records the metadata store still holds, if any.
    pub(crate) existing_streams: Vec<StreamRecord>,
    /// The identifiers of streams the metadata store records as deleted.
    pub(crate) deleted_stream_ids: BTreeSet<StreamId>,
    /// The names the operator gave for streams, by stream identifier.
    pub(crate) names_by_stream_id: BTreeMap<u64, String>,
    /// What each replica that answered holds.
    pub(crate) inventories: Vec<ReplicaInventory>,
    /// The tiered segment objects found in the object store.
    pub(crate) tiered_objects: Vec<TieredObject>,
    /// The time of the rebuild, recorded where a time is needed and none is known.
    pub(crate) now: Timestamp,
}

/// The record planned for one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedStream {
    /// The stream's identifier, as found on replicas or in the object store.
    pub(crate) stream_id: StreamId,
    /// The stream's name, if the metadata store or the operator knows it.
    pub(crate) name: Option<String>,
    /// The record to write. `None` if none could be put together; `problems` says why.
    pub(crate) record: Option<StreamRecord>,
    /// What was assumed because it cannot be recovered. Not a reason not to apply.
    pub(crate) assumptions: Vec<String>,
    /// What is wrong or lost. A plan with problems is applied only if forced.
    pub(crate) problems: Vec<String>,
}

/// What one replica reported about a segment.
#[derive(Clone, Debug)]
struct Holder {
    /// The replica that reported the copy.
    replica_id: ReplicaId,
    /// What it reported: a file with its header, or bare fencing or sealing state.
    status: SegmentStatus,
}

/// Everything found about one segment.
#[derive(Clone, Debug, Default)]
struct SegmentEvidence {
    /// The replicas that reported the segment, each with what it reported.
    holders: Vec<Holder>,
    /// The segment's object in the object store, if there is one.
    tiered: Option<TieredObject>,
}

/// Everything found about every segment, by stream and then by segment.
type EvidenceByStream = BTreeMap<StreamId, BTreeMap<SegmentId, SegmentEvidence>>;

/// Where a segment starts, as one file header or one object footer states it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SegmentStart {
    /// The epoch of the writer that authored the segment.
    epoch: Epoch,
    /// The sequence number of the segment's first record.
    first_sequence: SequenceNumber,
}

/// A segment whose start is known, from a file header or an object footer.
#[derive(Clone, Debug)]
struct LocatedSegment {
    /// The segment's identifier within its stream.
    segment_id: SegmentId,
    /// The epoch of the writer that authored it.
    epoch: Epoch,
    /// The sequence number of its first record.
    first_sequence: SequenceNumber,
    /// Its sealed end, if a replica or the object recorded one.
    recorded_sealed_end: Option<SequenceNumber>,
    /// Everything found about it.
    evidence: SegmentEvidence,
}

/// What looking at every segment of one stream found.
struct SegmentScan {
    /// The segments whose start is known, oldest first.
    located_segments: Vec<LocatedSegment>,
    /// The highest epoch that any copy, any fencing state, any object or the existing record mentions.
    highest_epoch: Epoch,
    /// The highest segment identifier seen, located or not.
    highest_segment_id: SegmentId,
}

/// Plans the stream records to write, one per stream identifier found.
///
/// See the file header for the rules. This function asks nothing and writes nothing.
///
/// # Arguments
///
/// * `inputs` - What the replicas and the object store hold, and what the metadata store and the operator
///   still know.
///
/// # Returns
///
/// One plan per stream identifier that a replica or an object mentions, sorted by identifier. Streams the
/// metadata store knows but nothing was found of are not planned and stay as they are.
pub(crate) fn plan_rebuild(inputs: &RebuildInputs) -> Vec<PlannedStream> {
    let evidence_by_stream = collect_evidence(inputs);
    let mut planned_streams = Vec::with_capacity(evidence_by_stream.len());
    for (stream_id, evidence_by_segment) in &evidence_by_stream {
        planned_streams.push(plan_stream(*stream_id, evidence_by_segment, inputs));
    }
    planned_streams
}

/// Sorts what the replicas reported and what the object store holds by the segment it is about.
fn collect_evidence(inputs: &RebuildInputs) -> EvidenceByStream {
    let mut evidence_by_stream = EvidenceByStream::new();
    for inventory in &inputs.inventories {
        for copy in &inventory.copies {
            evidence_about(&mut evidence_by_stream, copy.address)
                .holders
                .push(Holder {
                    replica_id: inventory.replica_id.clone(),
                    status: *copy,
                });
        }
    }
    for tiered_object in &inputs.tiered_objects {
        let address = tiered_object.footer.segment.address;
        evidence_about(&mut evidence_by_stream, address).tiered = Some(tiered_object.clone());
    }
    evidence_by_stream
}

/// The evidence about the segment at `address`, which is empty the first time the segment is mentioned.
fn evidence_about(evidence_by_stream: &mut EvidenceByStream, address: SegmentAddress) -> &mut SegmentEvidence {
    let evidence_by_segment = evidence_by_stream.entry(address.stream_id).or_default();
    evidence_by_segment.entry(address.segment_id).or_default()
}

/// Plans the record of one stream from everything found about its segments.
///
/// A stream recorded as deleted, and one of which no file and no object was found, gets no record and a
/// problem that says so.
fn plan_stream(
    stream_id: StreamId,
    evidence_by_segment: &BTreeMap<SegmentId, SegmentEvidence>,
    inputs: &RebuildInputs,
) -> PlannedStream {
    let existing = existing_record_of(stream_id, &inputs.existing_streams);
    let name = match existing {
        Some(known_stream) => Some(known_stream.name.clone()),
        None => inputs.names_by_stream_id.get(&stream_id.get()).cloned(),
    };
    let mut planned = PlannedStream {
        stream_id,
        name,
        record: None,
        assumptions: Vec::new(),
        problems: Vec::new(),
    };
    if inputs.deleted_stream_ids.contains(&stream_id) {
        planned
            .problems
            .push("the stream is recorded as deleted; its records are being removed and it is not rebuilt".to_owned());
        return planned;
    }
    let scan = scan_segments(evidence_by_segment, existing, &mut planned.problems);
    if scan.located_segments.is_empty() {
        planned
            .problems
            .push("no replica that answered holds a file of the stream, and the object store holds none".to_owned());
        return planned;
    }
    planned.record = plan_record(&scan, existing, inputs.now, &mut planned);
    planned
}

/// The record the metadata store still holds for `stream_id`, if it holds one.
fn existing_record_of(stream_id: StreamId, existing_streams: &[StreamRecord]) -> Option<&StreamRecord> {
    let mut existing = None;
    for known_stream in existing_streams {
        if known_stream.stream_id == stream_id {
            existing = Some(known_stream);
        }
    }
    existing
}

/// Looks at everything found about each segment of a stream: locates the segments that hold records, and
/// keeps the highest epoch and the highest segment identifier seen anywhere.
///
/// The epoch of `existing`, the record the metadata store still has, counts too. Copies that disagree
/// about a segment are added to `problems`.
fn scan_segments(
    evidence_by_segment: &BTreeMap<SegmentId, SegmentEvidence>,
    existing: Option<&StreamRecord>,
    problems: &mut Vec<String>,
) -> SegmentScan {
    let mut scan = SegmentScan {
        located_segments: Vec::new(),
        highest_epoch: existing.map_or(Epoch::ZERO, |known_stream| known_stream.epoch),
        highest_segment_id: SegmentId::new(0),
    };
    for (segment_id, evidence) in evidence_by_segment {
        scan.highest_segment_id = scan.highest_segment_id.max(*segment_id);
        scan.highest_epoch = scan.highest_epoch.max(highest_epoch_in(evidence));
        if let Some(located) = locate_segment(*segment_id, evidence, problems) {
            scan.located_segments.push(located);
        }
    }
    scan
}

/// Plans the record of a stream that has at least one located segment: rules 2 to 5 of the file header.
///
/// Every assumption and problem is added to `planned`, in the order of the rules.
///
/// # Returns
///
/// The record, or `None` if no name is known for the stream, which is added to the problems.
fn plan_record(
    scan: &SegmentScan,
    existing: Option<&StreamRecord>,
    now: Timestamp,
    planned: &mut PlannedStream,
) -> Option<StreamRecord> {
    let located_segments = &scan.located_segments;
    let sealed_ends = sealed_ends_of(located_segments, &mut planned.assumptions);
    let first_kept = first_segment_after_the_last_gap(located_segments, &sealed_ends, &mut planned.problems);
    let replication = replication_of(existing, &mut planned.assumptions);
    let mut segments = Vec::new();
    for index in first_kept..located_segments.len() {
        let located = &located_segments[index];
        segments.push(segment_record(located, sealed_ends[index], replication, now, planned));
    }
    note_what_replicas_do_not_store(existing, &mut planned.assumptions);
    let Some(name) = planned.name.clone() else {
        planned.problems.push(format!(
            "no name is known for the stream: pass --name {}=<name>",
            planned.stream_id
        ));
        return None;
    };
    let findings = StreamFindings {
        highest_epoch: scan.highest_epoch,
        highest_segment_id: scan.highest_segment_id,
        first_found_sequence: located_segments[first_kept].first_sequence,
    };
    Some(stream_record(
        planned.stream_id,
        name,
        replication,
        segments,
        &findings,
        existing,
    ))
}

/// The stream's replication: what `existing` records, or the default with an assumption that says so.
fn replication_of(existing: Option<&StreamRecord>, assumptions: &mut Vec<String>) -> ReplicationSettings {
    if let Some(known_stream) = existing {
        return known_stream.replication;
    }
    assumptions.push("replication is not stored on replicas: assumed 3 replicas with write quorum 2".to_owned());
    ReplicationSettings::DEFAULT
}

/// Adds the assumptions about what no replica stores: when segments were sealed and, unless `existing`
/// says, the retention policy and the materializers.
fn note_what_replicas_do_not_store(existing: Option<&StreamRecord>, assumptions: &mut Vec<String>) {
    assumptions.push(
        "seal times are not stored on replicas: sealed segments count as sealed now, which only \
         delays their tiering and retention"
            .to_owned(),
    );
    if existing.is_none() {
        assumptions.push(
            "retention policy and materializers are not stored on replicas: none is set, and \
             nothing is truncated until materializers are registered again"
                .to_owned(),
        );
    }
}

/// What the scan of a stream's segments found, beyond the segments themselves.
struct StreamFindings {
    /// The highest epoch any copy, any fencing state or any object mentions.
    highest_epoch: Epoch,
    /// The highest segment identifier seen, located or not.
    highest_segment_id: SegmentId,
    /// The first sequence number of the first segment the rebuilt stream keeps.
    first_found_sequence: SequenceNumber,
}

/// Puts the record of a rebuilt stream together.
///
/// What replicas do not store is taken from `existing`, the record the metadata store still has, if there
/// is one, and is otherwise the most conservative value: no retention, no materializers, no truncation
/// request. What `existing` knows is never moved back: neither the low-water mark nor the next segment
/// identifier.
fn stream_record(
    stream_id: StreamId,
    name: String,
    replication: ReplicationSettings,
    segments: Vec<SegmentRecord>,
    findings: &StreamFindings,
    existing: Option<&StreamRecord>,
) -> StreamRecord {
    let mut record = StreamRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        stream_id,
        name,
        replication,
        retention: RetentionPolicy::default(),
        // One above everything found, so that the next writer fences every replica.
        epoch: findings.highest_epoch.next(),
        writer: None,
        segments,
        next_segment_id: findings.highest_segment_id.next(),
        standby_replicas: None,
        truncate_before: None,
        low_water_mark: findings.first_found_sequence,
        materializers: BTreeSet::new(),
    };
    if let Some(known_stream) = existing {
        record.retention = known_stream.retention;
        record.truncate_before = known_stream.truncate_before;
        record.materializers.clone_from(&known_stream.materializers);
        record.low_water_mark = record.low_water_mark.max(known_stream.low_water_mark);
        record.next_segment_id = record.next_segment_id.max(known_stream.next_segment_id);
    }
    record
}

/// The highest epoch anything found about a segment mentions.
fn highest_epoch_in(evidence: &SegmentEvidence) -> Epoch {
    let mut highest_epoch = Epoch::ZERO;
    for holder in &evidence.holders {
        highest_epoch = highest_epoch.max(holder.status.fenced_epoch);
        if let Some(descriptor) = holder.status.descriptor {
            highest_epoch = highest_epoch.max(descriptor.epoch);
        }
    }
    if let Some(tiered_object) = &evidence.tiered {
        highest_epoch = highest_epoch.max(tiered_object.footer.segment.epoch);
    }
    highest_epoch
}

/// Works out where a segment starts and, if anything recorded it, where it was sealed.
///
/// # Returns
///
/// `None` if no file and no object says where the segment starts: it holds no records worth listing.
/// Disagreements between what was found are added to `problems`; the first statement found is used, and
/// the object's footer comes before the replicas' files.
fn locate_segment(
    segment_id: SegmentId,
    evidence: &SegmentEvidence,
    problems: &mut Vec<String>,
) -> Option<LocatedSegment> {
    let stated_starts = stated_starts_of(evidence);
    let recorded_seals = recorded_seals_of(evidence);
    let start = *stated_starts.first()?;
    if !all_agree(&stated_starts) {
        problems.push(format!(
            "segment {segment_id}: its copies disagree about its epoch or first sequence number"
        ));
    }
    if !all_agree(&recorded_seals) {
        problems.push(format!(
            "segment {segment_id}: its copies disagree about where it was sealed"
        ));
    }
    Some(LocatedSegment {
        segment_id,
        epoch: start.epoch,
        first_sequence: start.first_sequence,
        recorded_sealed_end: recorded_seals.first().copied(),
        evidence: evidence.clone(),
    })
}

/// Every statement of where a segment starts: the object's footer first, then each file's header.
fn stated_starts_of(evidence: &SegmentEvidence) -> Vec<SegmentStart> {
    let mut stated_starts = Vec::new();
    if let Some(tiered_object) = &evidence.tiered {
        stated_starts.push(SegmentStart {
            epoch: tiered_object.footer.segment.epoch,
            first_sequence: tiered_object.footer.segment.first_sequence,
        });
    }
    for holder in &evidence.holders {
        if let Some(descriptor) = holder.status.descriptor {
            stated_starts.push(SegmentStart {
                epoch: descriptor.epoch,
                first_sequence: descriptor.first_sequence,
            });
        }
    }
    stated_starts
}

/// Every sealed end that was recorded for a segment: the object's footer first, then each replica's.
fn recorded_seals_of(evidence: &SegmentEvidence) -> Vec<SequenceNumber> {
    let mut recorded_seals = Vec::new();
    if let Some(tiered_object) = &evidence.tiered {
        recorded_seals.push(tiered_object.footer.sealed_end);
    }
    for holder in &evidence.holders {
        if let Some(sealed_end) = holder.status.sealed_end {
            recorded_seals.push(sealed_end);
        }
    }
    recorded_seals
}

/// Whether every statement says the same as the first one. An empty list agrees.
fn all_agree<Statement: PartialEq>(statements: &[Statement]) -> bool {
    for statement in statements {
        if Some(statement) != statements.first() {
            return false;
        }
    }
    true
}

/// The sealed end of every located segment, in the same order; `None` for a segment left open.
///
/// A segment without a recorded seal is sealed one below the start of the next located segment, because
/// a segment is only opened after its predecessor was sealed and starts right after it. Only the last
/// located segment can be left open.
fn sealed_ends_of(located_segments: &[LocatedSegment], assumptions: &mut Vec<String>) -> Vec<Option<SequenceNumber>> {
    let mut sealed_ends = Vec::with_capacity(located_segments.len());
    for (index, located) in located_segments.iter().enumerate() {
        if located.recorded_sealed_end.is_some() {
            sealed_ends.push(located.recorded_sealed_end);
            continue;
        }
        let Some(next_located) = located_segments.get(index + 1) else {
            assumptions.push(format!(
                "segment {}: nothing recorded a seal; it is left open for the next writer to recover",
                located.segment_id
            ));
            sealed_ends.push(None);
            continue;
        };
        let inferred_end = next_located.first_sequence.preceding();
        assumptions.push(format!(
            "segment {}: nothing recorded its seal; sealed at {inferred_end} because segment {} \
             starts right after",
            located.segment_id, next_located.segment_id
        ));
        sealed_ends.push(Some(inferred_end));
    }
    sealed_ends
}

/// The index of the first segment the rebuilt stream keeps: the one after the last place where two
/// consecutive segments do not connect, or the first segment if all connect. Every such place is added
/// to `problems`.
fn first_segment_after_the_last_gap(
    located_segments: &[LocatedSegment],
    sealed_ends: &[Option<SequenceNumber>],
    problems: &mut Vec<String>,
) -> usize {
    let mut first_kept = 0;
    for index in 1..located_segments.len() {
        let previous = &located_segments[index - 1];
        let current = &located_segments[index];
        let connects = sealed_ends[index - 1].is_some_and(|previous_end| previous_end.next() == current.first_sequence);
        if connects {
            continue;
        }
        let previous_end = match sealed_ends[index - 1] {
            Some(end) => end.to_string(),
            None => "an unknown position".to_owned(),
        };
        problems.push(format!(
            "segment {} ends at {previous_end} and segment {} starts at {}: the records in between \
             are lost, and the stream is rebuilt from sequence {} on",
            previous.segment_id, current.segment_id, current.first_sequence, current.first_sequence
        ));
        first_kept = index;
    }
    first_kept
}

/// Builds the record of one located segment: rule 4 of the file header.
///
/// Copies that were not found are added to the assumptions of `planned`, and records that may be lost to
/// its problems.
fn segment_record(
    located: &LocatedSegment,
    sealed_end: Option<SequenceNumber>,
    replication: ReplicationSettings,
    now: Timestamp,
    planned: &mut PlannedStream,
) -> SegmentRecord {
    let segment_id = located.segment_id;
    let tiered_object = located.evidence.tiered.as_ref();
    let holders = holders_longest_first(&located.evidence);
    let survey = survey_copies(&holders, tiered_object.is_some(), sealed_end);
    let is_released = sealed_end.is_some() && tiered_object.is_some() && survey.file_holder_count == 0;
    let replica_count = replication.replica_count();
    let copies = if is_released {
        Vec::new()
    } else {
        copy_records(segment_id, &holders, replica_count, &mut planned.assumptions)
    };
    if let Some(problem) = lost_records_problem(segment_id, sealed_end, &survey, holders.len(), replication) {
        planned.problems.push(problem);
    }
    SegmentRecord {
        segment_id,
        epoch: located.epoch,
        first_sequence: located.first_sequence,
        sealed: sealed_end.map(|end| SealRecord { end, at: now }),
        copies,
        tiered: tiered_object.map(|tiered_object| tiered_record(tiered_object, now, is_released)),
    }
}

/// The record of where a segment lives in the object store.
///
/// The time of the upload is not known and is recorded as `now`. `is_released` says that no replica
/// holds a file of the segment any more, so the object is the only copy.
fn tiered_record(tiered_object: &TieredObject, now: Timestamp, is_released: bool) -> TieredRecord {
    TieredRecord {
        object_key: tiered_object.object_key.clone(),
        size_bytes: tiered_object.size_bytes,
        uploaded_at: now,
        are_local_copies_dropped: is_released,
    }
}

/// The replicas that reported a segment, the one with the longest copy first and those with bare fencing
/// or sealing state last. Replicas with copies of the same length are sorted by identifier, so that the
/// plan does not depend on the order in which the replicas answered.
fn holders_longest_first(evidence: &SegmentEvidence) -> Vec<Holder> {
    let mut holders = evidence.holders.clone();
    holders.sort_by(|left, right| {
        let longest_first = right.status.local_end.cmp(&left.status.local_end);
        longest_first.then_with(|| left.replica_id.cmp(&right.replica_id))
    });
    holders
}

/// What the copies of one segment add up to.
struct CopySurvey {
    /// How many replicas hold a file of the segment, and not only fencing or sealing state.
    file_holder_count: usize,
    /// Whether the object store, or an undamaged file that reaches the sealed end, holds every record.
    has_complete_copy: bool,
}

/// Counts the files among `holders` and looks for a copy that holds every record up to `sealed_end`.
///
/// An object in the object store is such a copy: it was verified before it was recorded. A file is one if
/// it reaches the sealed end and is not damaged. An open segment has no complete copy on a replica.
fn survey_copies(holders: &[Holder], is_tiered: bool, sealed_end: Option<SequenceNumber>) -> CopySurvey {
    let mut survey = CopySurvey {
        file_holder_count: 0,
        has_complete_copy: is_tiered,
    };
    for holder in holders {
        if holder.status.local_end.is_some() {
            survey.file_holder_count += 1;
        }
        let reaches_the_end = match (holder.status.local_end, sealed_end) {
            (Some(local_end), Some(sealed_end)) => local_end >= sealed_end,
            _open_or_without_file => false,
        };
        if reaches_the_end && !holder.status.is_damaged {
            survey.has_complete_copy = true;
        }
    }
    survey
}

/// The copies to record for a segment that still has, or should have, copies on replicas.
///
/// The first `replica_count` holders become copies in state `unverified`. If fewer were found, the rest
/// are copies in state `bad` on replicas that do not exist, and an assumption says so.
fn copy_records(
    segment_id: SegmentId,
    holders: &[Holder],
    replica_count: usize,
    assumptions: &mut Vec<String>,
) -> Vec<CopyRecord> {
    let mut copies = Vec::new();
    for holder in holders.iter().take(replica_count) {
        copies.push(CopyRecord {
            replica_id: holder.replica_id.clone(),
            state: CopyState::Unverified,
        });
    }
    let found_count = copies.len();
    for missing_number in found_count..replica_count {
        copies.push(CopyRecord {
            replica_id: ReplicaId::new(format!("{MISSING_COPY_PREFIX}{}", missing_number + 1))
                .expect("the prefix and a number make a valid identifier"),
            state: CopyState::Bad,
        });
    }
    if found_count < replica_count {
        assumptions.push(format!(
            "segment {segment_id}: {found_count} of {replica_count} copies were found; the \
             others are recorded as missing, and the controller rebuilds them"
        ));
    }
    copies
}

/// The problem to report if records of a segment may be lost, if there is one.
///
/// A sealed segment needs a complete copy, which `survey` says whether it has. An open one needs a
/// recovery quorum of the replicas that hold it to have answered, or the next writer cannot recover it;
/// `holder_count` is how many answered.
fn lost_records_problem(
    segment_id: SegmentId,
    sealed_end: Option<SequenceNumber>,
    survey: &CopySurvey,
    holder_count: usize,
    replication: ReplicationSettings,
) -> Option<String> {
    match sealed_end {
        Some(sealed_end) if !survey.has_complete_copy => Some(format!(
            "segment {segment_id}: no undamaged copy that reaches its sealed end {sealed_end} was \
             found; records are lost unless a replica that did not answer holds one"
        )),
        None if holder_count < replication.recovery_quorum() => Some(format!(
            "segment {segment_id}: only {holder_count} of its replicas answered, and a writer needs {} to \
             recover the open segment",
            replication.recovery_quorum()
        )),
        _nothing_wrong => None,
    }
}

/// One replica that was asked for its segment copies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AskedReplica {
    /// The replica's identifier.
    pub(crate) replica_id: String,
    /// The address it was asked at.
    pub(crate) address: String,
    /// Whether it answered.
    pub(crate) has_answered: bool,
    /// How many segment copies it reported.
    pub(crate) copies: usize,
}

/// One stream of the plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RebuiltStream {
    /// The stream's identifier.
    pub(crate) stream_id: u64,
    /// The stream's name, if known.
    pub(crate) name: Option<String>,
    /// The epoch of the planned record, if a record could be planned.
    pub(crate) epoch: Option<u64>,
    /// The low-water mark of the planned record.
    pub(crate) low_water_mark: Option<u64>,
    /// The next segment identifier of the planned record.
    pub(crate) next_segment_id: Option<u64>,
    /// The segments of the planned record, oldest first.
    pub(crate) segments: Vec<SegmentRow>,
    /// What was assumed because it cannot be recovered.
    pub(crate) assumptions: Vec<String>,
    /// What is wrong or lost.
    pub(crate) problems: Vec<String>,
    /// Whether the record was written to the metadata store.
    pub(crate) is_written: bool,
}

/// What a rebuild found, planned and did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RebuildReport {
    /// Whether the plan was written. `false` for a dry run.
    pub(crate) is_applied: bool,
    /// The replicas that were asked.
    pub(crate) replicas: Vec<AskedReplica>,
    /// How many tiered segment objects were found in the object store.
    pub(crate) tiered_objects: usize,
    /// The plan, one entry per stream identifier found.
    pub(crate) streams: Vec<RebuiltStream>,
    /// What is wrong beyond a single stream: replicas that did not answer, objects that cannot be read.
    pub(crate) problems: Vec<String>,
}

impl RebuiltStream {
    /// Appends the stream to `rendered`: a title line, the planned record with its segments if there is
    /// one, then the assumptions and the problems.
    fn render_into(&self, rendered: &mut String) {
        let name = match &self.name {
            Some(name) => format!("{name:?}"),
            None => "(no name)".to_owned(),
        };
        let written = if self.is_written { "written" } else { "not written" };
        let _ = writeln!(rendered, "\nstream {} {name}: {written}", self.stream_id);
        if let (Some(epoch), Some(low_water_mark), Some(next_segment_id)) =
            (self.epoch, self.low_water_mark, self.next_segment_id)
        {
            let _ = writeln!(
                rendered,
                "epoch {epoch}, low-water mark {low_water_mark}, next segment id {next_segment_id}"
            );
            let mut table = segment_table();
            for segment in &self.segments {
                table.push_row(segment.table_row());
            }
            rendered.push_str(&table.render());
        }
        render_list(rendered, "assumptions", &self.assumptions);
        render_list(rendered, "problems", &self.problems);
    }
}

impl RebuildReport {
    /// A report of a rebuild that has asked nothing yet.
    fn empty() -> Self {
        Self {
            is_applied: false,
            replicas: Vec::new(),
            tiered_objects: 0,
            streams: Vec::new(),
            problems: Vec::new(),
        }
    }

    /// How many problems the report holds, in the streams and beyond.
    fn problem_count(&self) -> usize {
        let mut count = self.problems.len();
        for stream in &self.streams {
            count += stream.problems.len();
        }
        count
    }

    /// Renders the report: what was asked, then every stream with its segments, assumptions and
    /// problems, then what to do next.
    pub(crate) fn render_text(&self) -> String {
        let mut rendered = self.render_what_was_asked();
        for stream in &self.streams {
            stream.render_into(&mut rendered);
        }
        rendered.push('\n');
        rendered.push_str(self.next_step());
        rendered
    }

    /// Renders the replicas that were asked as a table, the number of objects found, and the problems
    /// that are not about one stream.
    fn render_what_was_asked(&self) -> String {
        let mut replica_table = Table::new(&["REPLICA", "ADDRESS", "ANSWERED", "COPIES"]);
        for replica in &self.replicas {
            replica_table.push_row(vec![
                replica.replica_id.clone(),
                replica.address.clone(),
                if replica.has_answered { "yes" } else { "no" }.to_owned(),
                replica.copies.to_string(),
            ]);
        }
        let mut rendered = replica_table.render();
        let _ = writeln!(rendered, "tiered segment objects found: {}", self.tiered_objects);
        render_list(&mut rendered, "problems", &self.problems);
        rendered
    }

    /// The last line of the report: what happened and what the operator does next.
    fn next_step(&self) -> &'static str {
        if self.is_applied {
            "applied. Start the controllers: they verify every copy and rebuild missing ones.\n"
        } else if self.problem_count() == 0 {
            "dry run: nothing was written. Pass --apply to write this plan.\n"
        } else {
            "dry run: nothing was written. The plan has problems; fix them, or pass --apply \
             --force to write what could be rebuilt.\n"
        }
    }
}

/// Appends a titled list of sentences to `rendered`, or nothing if the list is empty.
fn render_list(rendered: &mut String, title: &str, sentences: &[String]) {
    if sentences.is_empty() {
        return;
    }
    let _ = writeln!(rendered, "{title}:");
    for sentence in sentences {
        let _ = writeln!(rendered, "  - {sentence}");
    }
}

/// Rebuilds stream records from what the replicas and the object store hold.
///
/// See the file header. The replicas are only asked what they hold; nothing is changed on them.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `names` - The names of streams the metadata store does not know, as (stream identifier, name).
/// * `extra_replicas` - Replicas to ask that are not registered, as (identifier, address). An entry for a
///   registered replica replaces its address.
/// * `is_applied` - Write the planned records. Without it, the plan is only returned.
/// * `is_forced` - Write what could be planned although the plan has problems.
///
/// # Returns
///
/// What was found, the plan, and whether each record was written.
///
/// # Errors
///
/// * [`CommandError::NothingToDo`] if no replica is registered and none was named.
/// * [`CommandError::Refused`] if the plan has problems and is to be applied without force. Nothing is
///   written then.
/// * [`CommandError::Metadata`] if the metadata store is unavailable or refuses a record.
pub(crate) async fn rebuild(
    cluster: &Cluster,
    names: &[(u64, String)],
    extra_replicas: &[(ReplicaId, String)],
    is_applied: bool,
    is_forced: bool,
) -> Result<RebuildReport, CommandError> {
    let addresses_by_replica = replicas_to_ask(cluster, extra_replicas).await?;
    let mut report = RebuildReport::empty();
    let inventories = gather_inventories(cluster, &addresses_by_replica, &mut report).await;
    let tiered_objects = find_tiered_objects(cluster, &mut report.problems).await;
    report.tiered_objects = tiered_objects.len();

    let inputs = gather_what_is_known(cluster, names, inventories, tiered_objects).await?;
    let planned_streams = plan_rebuild(&inputs);
    for planned in &planned_streams {
        report.streams.push(rebuilt_stream(planned));
    }
    if is_applied {
        apply_plan(cluster, &planned_streams, is_forced, &mut report).await?;
    }
    Ok(report)
}

/// The replicas to ask and the address of each: every registered replica, and every replica the operator
/// named, whose address replaces a registered one.
///
/// # Errors
///
/// * [`CommandError::NothingToDo`] if no replica is registered and none was named.
/// * [`CommandError::Metadata`] if the registry cannot be read.
async fn replicas_to_ask(
    cluster: &Cluster,
    extra_replicas: &[(ReplicaId, String)],
) -> Result<BTreeMap<ReplicaId, String>, CommandError> {
    let mut addresses_by_replica = BTreeMap::new();
    for registered in cluster.metadata.list_replicas().await? {
        addresses_by_replica.insert(registered.replica_id, registered.address);
    }
    for (replica_id, address) in extra_replicas {
        addresses_by_replica.insert(replica_id.clone(), address.clone());
    }
    if addresses_by_replica.is_empty() {
        return Err(CommandError::NothingToDo {
            reason: "no replica is registered and none was named: start the replicas so that they \
                     register, or pass --replica <id>=<address> for each"
                .to_owned(),
        });
    }
    Ok(addresses_by_replica)
}

/// Puts together what the plan is made from: what was gathered, and what the metadata store and the
/// operator still know.
///
/// # Errors
///
/// Returns [`CommandError::Metadata`] if the deleted streams or the existing streams cannot be read.
async fn gather_what_is_known(
    cluster: &Cluster,
    names: &[(u64, String)],
    inventories: Vec<ReplicaInventory>,
    tiered_objects: Vec<TieredObject>,
) -> Result<RebuildInputs, CommandError> {
    let mut deleted_stream_ids = BTreeSet::new();
    for deleted in cluster.metadata.list_deleted_streams().await? {
        deleted_stream_ids.insert(deleted.stream_id);
    }
    let mut names_by_stream_id = BTreeMap::new();
    for (stream_id, name) in names {
        names_by_stream_id.insert(*stream_id, name.clone());
    }
    Ok(RebuildInputs {
        existing_streams: cluster.metadata.list_streams().await?,
        deleted_stream_ids,
        names_by_stream_id,
        inventories,
        tiered_objects,
        now: cluster.clock.now(),
    })
}

/// Writes every planned record to the metadata store and marks it as written in `report`.
///
/// # Errors
///
/// * [`CommandError::Refused`] if the report holds problems and `is_forced` is not set. Nothing is
///   written then.
/// * [`CommandError::Metadata`] if the metadata store refuses a record. Records written before it stay.
async fn apply_plan(
    cluster: &Cluster,
    planned_streams: &[PlannedStream],
    is_forced: bool,
    report: &mut RebuildReport,
) -> Result<(), CommandError> {
    let problem_count = report.problem_count();
    if problem_count > 0 && !is_forced {
        return Err(CommandError::Refused {
            reason: format!(
                "the plan has {problem_count} problems and was not applied; run without --apply to \
                 read it, then fix them or pass --force to write what could be rebuilt"
            ),
        });
    }
    for (planned, reported) in planned_streams.iter().zip(&mut report.streams) {
        if let Some(record) = &planned.record {
            cluster.metadata.adopt_stream(record).await?;
            reported.is_written = true;
        }
    }
    report.is_applied = true;
    Ok(())
}

/// Asks every replica for its segment copies.
///
/// Each replica asked is added to the report, and one that does not answer also to the report's
/// problems: its copies are missing from the plan.
///
/// # Returns
///
/// What the replicas that answered hold.
async fn gather_inventories(
    cluster: &Cluster,
    addresses_by_replica: &BTreeMap<ReplicaId, String>,
    report: &mut RebuildReport,
) -> Vec<ReplicaInventory> {
    let mut inventories = Vec::new();
    for (replica_id, address) in addresses_by_replica {
        let answer = ask_for_copies(cluster, replica_id, address).await;
        report.replicas.push(AskedReplica {
            replica_id: replica_id.to_string(),
            address: address.clone(),
            has_answered: answer.is_ok(),
            copies: answer.as_ref().map_or(0, Vec::len),
        });
        match answer {
            Ok(copies) => inventories.push(ReplicaInventory {
                replica_id: replica_id.clone(),
                copies,
            }),
            Err(problem) => report.problems.push(problem),
        }
    }
    inventories
}

/// Asks one replica for its segment copies, waiting no longer than the request timeout.
///
/// # Errors
///
/// Returns the sentence for the report's problems if the replica refused or did not answer in time.
async fn ask_for_copies(
    cluster: &Cluster,
    replica_id: &ReplicaId,
    address: &str,
) -> Result<Vec<SegmentStatus>, String> {
    let client = cluster.connector.connect(replica_id, address);
    match timeout(cluster.request_timeout, client.list_segments()).await {
        Ok(Ok(copies)) => Ok(copies),
        Ok(Err(replica_error)) => Err(format!(
            "replica {replica_id} did not answer ({replica_error}): its copies are not in \
             the plan"
        )),
        Err(_elapsed) => Err(format!(
            "replica {replica_id} did not answer in time: its copies are not in the plan"
        )),
    }
}

/// Describes one planned stream for the report.
fn rebuilt_stream(planned: &PlannedStream) -> RebuiltStream {
    let mut segments = Vec::new();
    if let Some(record) = &planned.record {
        for segment in &record.segments {
            segments.push(segment_row(record, segment));
        }
    }
    RebuiltStream {
        stream_id: planned.stream_id.get(),
        name: planned.name.clone(),
        epoch: planned.record.as_ref().map(|record| record.epoch.get()),
        low_water_mark: planned.record.as_ref().map(|record| record.low_water_mark.get()),
        next_segment_id: planned.record.as_ref().map(|record| record.next_segment_id.get()),
        segments,
        assumptions: planned.assumptions.clone(),
        problems: planned.problems.clone(),
        is_written: false,
    }
}

/// Lists the tiered segment objects and reads the footer of each.
///
/// If the configuration names no object store, or the store cannot be listed, that is added to `problems`
/// and nothing is found: the segments that were left to the object store are then missing from the plan.
async fn find_tiered_objects(cluster: &Cluster, problems: &mut Vec<String>) -> Vec<TieredObject> {
    let Some(artifact_store) = &cluster.artifact_store else {
        problems.push(
            "the configuration names no object store: segments that were left to the object \
             store cannot be found"
                .to_owned(),
        );
        return Vec::new();
    };
    let listing = timeout(
        cluster.request_timeout,
        artifact_store.list_prefix(SEGMENT_OBJECTS_PREFIX),
    )
    .await;
    let Ok(Ok(objects)) = listing else {
        problems
            .push("the object store could not be listed: segments that were left to it are not in the plan".to_owned());
        return Vec::new();
    };
    read_tiered_objects(cluster, artifact_store.as_ref(), objects, problems).await
}

/// Reads the footer of every object in `objects` whose key names a segment.
///
/// An object whose key is shaped differently is somebody else's and is skipped without a word. One that
/// cannot be read as the segment its key names is added to `problems` and left out.
async fn read_tiered_objects(
    cluster: &Cluster,
    artifact_store: &dyn ArtifactStore,
    objects: Vec<ObjectInfo>,
    problems: &mut Vec<String>,
) -> Vec<TieredObject> {
    let mut tiered_objects = Vec::new();
    for object in objects {
        let Some(address) = segment_address_of(&object.object_key) else {
            continue;
        };
        match read_footer_of(cluster, artifact_store, &object, address).await {
            Some(footer) => tiered_objects.push(TieredObject {
                object_key: object.object_key,
                size_bytes: object.size_bytes,
                footer,
            }),
            None => problems.push(format!(
                "object {:?} cannot be read as the segment its key names: it is not in the plan",
                object.object_key
            )),
        }
    }
    tiered_objects
}

/// Reads the footer of `object` and checks that it describes the whole segment at `address`.
///
/// # Returns
///
/// The footer, or `None` if the object is shorter than a footer, cannot be fetched in time, does not end
/// in an intact footer, or its footer names another segment or another object size.
async fn read_footer_of(
    cluster: &Cluster,
    artifact_store: &dyn ArtifactStore,
    object: &ObjectInfo,
    address: SegmentAddress,
) -> Option<TieredFooter> {
    let footer_bytes = TIERED_FOOTER_BYTES as u64;
    if object.size_bytes < footer_bytes {
        return None;
    }
    let footer_range = object.size_bytes - footer_bytes..object.size_bytes;
    let fetched = timeout(
        cluster.request_timeout,
        artifact_store.get_range(&object.object_key, footer_range),
    )
    .await;
    let Ok(Ok(encoded_footer)) = fetched else {
        return None;
    };
    let footer = TieredFooter::decode(&encoded_footer).ok()?;
    let is_the_named_segment = footer.segment.address == address && footer.object_bytes() == object.size_bytes;
    if is_the_named_segment { Some(footer) } else { None }
}

/// The segment that a tiered object key names, as `streams/<stream id>/segments/<segment id>`.
///
/// # Returns
///
/// `None` if the key is not shaped like that.
fn segment_address_of(object_key: &str) -> Option<SegmentAddress> {
    let rest = object_key.strip_prefix(SEGMENT_OBJECTS_PREFIX)?;
    let (stream_id, rest) = rest.split_once('/')?;
    let segment_id = rest.strip_prefix("segments/")?;
    Some(SegmentAddress::new(
        StreamId::new(stream_id.parse().ok()?),
        SegmentId::new(segment_id.parse().ok()?),
    ))
}

#[cfg(test)]
#[path = "rebuild_test.rs"]
mod tests;
