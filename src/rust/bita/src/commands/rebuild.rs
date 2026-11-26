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
//! # Invariants
//!
//! - Planning is pure: the same inventory gives the same plan, and nothing is asked or written.
//! - Nothing is written without `--apply`, and nothing with problems without `--force`.
//! - A stream recorded as deleted is never rebuilt: its records are being removed on purpose.
//! - No replica is told to change anything. Rebuilding only writes to the metadata store.
//!
//! # See also
//!
//! - `commands/metadata.rs`: restoring a snapshot, which comes first when there is one.
//! - `bitacora-meta/src/store/recovery.rs`: `adopt_stream`.
//! - `bitacora-segment/src/tiered.rs`: the footer of a tiered object.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use bitacora_artifact::ArtifactStore;
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

/// Everything found about one segment.
#[derive(Clone, Debug, Default)]
struct SegmentEvidence {
    /// The replicas that reported the segment, each with what it reported.
    holders: Vec<(ReplicaId, SegmentStatus)>,
    /// The segment's object in the object store, if there is one.
    tiered: Option<TieredObject>,
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
    let mut evidence_by_stream: BTreeMap<StreamId, BTreeMap<SegmentId, SegmentEvidence>> = BTreeMap::new();
    for inventory in &inputs.inventories {
        for copy in &inventory.copies {
            evidence_by_stream
                .entry(copy.address.stream_id)
                .or_default()
                .entry(copy.address.segment_id)
                .or_default()
                .holders
                .push((inventory.replica_id.clone(), *copy));
        }
    }
    for tiered_object in &inputs.tiered_objects {
        let address = tiered_object.footer.segment.address;
        evidence_by_stream
            .entry(address.stream_id)
            .or_default()
            .entry(address.segment_id)
            .or_default()
            .tiered = Some(tiered_object.clone());
    }
    let mut planned_streams = Vec::with_capacity(evidence_by_stream.len());
    for (stream_id, evidence_by_segment) in &evidence_by_stream {
        planned_streams.push(plan_stream(*stream_id, evidence_by_segment, inputs));
    }
    planned_streams
}

/// Plans the record of one stream from everything found about its segments.
fn plan_stream(
    stream_id: StreamId,
    evidence_by_segment: &BTreeMap<SegmentId, SegmentEvidence>,
    inputs: &RebuildInputs,
) -> PlannedStream {
    let mut existing = None;
    for known_stream in &inputs.existing_streams {
        if known_stream.stream_id == stream_id {
            existing = Some(known_stream);
        }
    }
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

    let mut located_segments = Vec::new();
    let mut highest_epoch = existing.map_or(Epoch::ZERO, |known_stream| known_stream.epoch);
    let mut highest_segment_id = SegmentId::new(0);
    for (segment_id, evidence) in evidence_by_segment {
        highest_segment_id = highest_segment_id.max(*segment_id);
        highest_epoch = highest_epoch.max(highest_epoch_in(evidence));
        if let Some(located) = locate_segment(*segment_id, evidence, &mut planned.problems) {
            located_segments.push(located);
        }
    }
    if located_segments.is_empty() {
        planned
            .problems
            .push("no replica that answered holds a file of the stream, and the object store holds none".to_owned());
        return planned;
    }

    let sealed_ends = sealed_ends_of(&located_segments, &mut planned.assumptions);
    let first_kept = first_segment_after_the_last_gap(&located_segments, &sealed_ends, &mut planned.problems);
    let replication = if let Some(known_stream) = existing {
        known_stream.replication
    } else {
        planned
            .assumptions
            .push("replication is not stored on replicas: assumed 3 replicas with write quorum 2".to_owned());
        ReplicationSettings::DEFAULT
    };
    let mut segments = Vec::new();
    for index in first_kept..located_segments.len() {
        segments.push(segment_record(
            &located_segments[index],
            sealed_ends[index],
            replication,
            inputs.now,
            &mut planned,
        ));
    }
    planned.assumptions.push(
        "seal times are not stored on replicas: sealed segments count as sealed now, which only \
         delays their tiering and retention"
            .to_owned(),
    );
    if existing.is_none() {
        planned.assumptions.push(
            "retention policy and materializers are not stored on replicas: none is set, and \
             nothing is truncated until materializers are registered again"
                .to_owned(),
        );
    }
    let Some(name) = planned.name.clone() else {
        planned.problems.push(format!(
            "no name is known for the stream: pass --name {stream_id}=<name>"
        ));
        return planned;
    };
    let findings = StreamFindings {
        highest_epoch,
        highest_segment_id,
        first_found_sequence: located_segments[first_kept].first_sequence,
    };
    planned.record = Some(stream_record(
        stream_id,
        name,
        replication,
        segments,
        &findings,
        existing,
    ));
    planned
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
    for (_replica_id, status) in &evidence.holders {
        highest_epoch = highest_epoch.max(status.fenced_epoch);
        if let Some(descriptor) = status.descriptor {
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
/// Disagreements between what was found are added to `problems`; the first statement found is used.
fn locate_segment(
    segment_id: SegmentId,
    evidence: &SegmentEvidence,
    problems: &mut Vec<String>,
) -> Option<LocatedSegment> {
    let mut starts = Vec::new();
    let mut recorded_seals = Vec::new();
    if let Some(tiered_object) = &evidence.tiered {
        let footer = tiered_object.footer;
        starts.push((footer.segment.epoch, footer.segment.first_sequence));
        recorded_seals.push(footer.sealed_end);
    }
    for (_replica_id, status) in &evidence.holders {
        if let Some(descriptor) = status.descriptor {
            starts.push((descriptor.epoch, descriptor.first_sequence));
        }
        if let Some(sealed_end) = status.sealed_end {
            recorded_seals.push(sealed_end);
        }
    }
    let (epoch, first_sequence) = *starts.first()?;
    for other_start in &starts {
        if *other_start != (epoch, first_sequence) {
            problems.push(format!(
                "segment {segment_id}: its copies disagree about its epoch or first sequence number"
            ));
            break;
        }
    }
    let recorded_sealed_end = recorded_seals.first().copied();
    for other_seal in &recorded_seals {
        if Some(*other_seal) != recorded_sealed_end {
            problems.push(format!(
                "segment {segment_id}: its copies disagree about where it was sealed"
            ));
            break;
        }
    }
    Some(LocatedSegment {
        segment_id,
        epoch,
        first_sequence,
        recorded_sealed_end,
        evidence: evidence.clone(),
    })
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

/// Builds the record of one located segment.
fn segment_record(
    located: &LocatedSegment,
    sealed_end: Option<SequenceNumber>,
    replication: ReplicationSettings,
    now: Timestamp,
    planned: &mut PlannedStream,
) -> SegmentRecord {
    let segment_id = located.segment_id;
    let mut holders = located.evidence.holders.clone();
    // Longest copy first; a replica with bare state last.
    holders.sort_by(|(left_id, left), (right_id, right)| {
        right.local_end.cmp(&left.local_end).then_with(|| left_id.cmp(right_id))
    });
    let mut file_holder_count = 0;
    let mut has_complete_copy = located.evidence.tiered.is_some();
    for (_replica_id, status) in &holders {
        if status.local_end.is_some() {
            file_holder_count += 1;
        }
        let reaches_the_end = match (status.local_end, sealed_end) {
            (Some(local_end), Some(sealed_end)) => local_end >= sealed_end,
            _open_or_without_file => false,
        };
        if reaches_the_end && !status.is_damaged {
            has_complete_copy = true;
        }
    }

    let replica_count = replication.replica_count();
    let is_released = sealed_end.is_some() && located.evidence.tiered.is_some() && file_holder_count == 0;
    let mut copies = Vec::new();
    if !is_released {
        for (replica_id, _status) in holders.iter().take(replica_count) {
            copies.push(CopyRecord {
                replica_id: replica_id.clone(),
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
            planned.assumptions.push(format!(
                "segment {segment_id}: {found_count} of {replica_count} copies were found; the \
                 others are recorded as missing, and the controller rebuilds them"
            ));
        }
    }

    match sealed_end {
        Some(sealed_end) if !has_complete_copy => planned.problems.push(format!(
            "segment {segment_id}: no undamaged copy that reaches its sealed end {sealed_end} was \
             found; records are lost unless a replica that did not answer holds one"
        )),
        None if holders.len() < replication.recovery_quorum() => planned.problems.push(format!(
            "segment {segment_id}: only {} of its replicas answered, and a writer needs {} to \
             recover the open segment",
            holders.len(),
            replication.recovery_quorum()
        )),
        _nothing_wrong => {}
    }

    SegmentRecord {
        segment_id,
        epoch: located.epoch,
        first_sequence: located.first_sequence,
        sealed: sealed_end.map(|end| SealRecord { end, at: now }),
        copies,
        tiered: located.evidence.tiered.as_ref().map(|tiered_object| TieredRecord {
            object_key: tiered_object.object_key.clone(),
            size_bytes: tiered_object.size_bytes,
            uploaded_at: now,
            are_local_copies_dropped: is_released,
        }),
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

impl RebuildReport {
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
        let mut rendered = String::new();
        let mut replica_table = Table::new(&["REPLICA", "ADDRESS", "ANSWERED", "COPIES"]);
        for replica in &self.replicas {
            replica_table.push_row(vec![
                replica.replica_id.clone(),
                replica.address.clone(),
                if replica.has_answered { "yes" } else { "no" }.to_owned(),
                replica.copies.to_string(),
            ]);
        }
        rendered.push_str(&replica_table.render());
        let _ = writeln!(rendered, "tiered segment objects found: {}", self.tiered_objects);
        render_list(&mut rendered, "problems", &self.problems);

        for stream in &self.streams {
            let name = match &stream.name {
                Some(name) => format!("{name:?}"),
                None => "(no name)".to_owned(),
            };
            let written = if stream.is_written { "written" } else { "not written" };
            let _ = writeln!(rendered, "\nstream {} {name}: {written}", stream.stream_id);
            if let (Some(epoch), Some(low_water_mark), Some(next_segment_id)) =
                (stream.epoch, stream.low_water_mark, stream.next_segment_id)
            {
                let _ = writeln!(
                    rendered,
                    "epoch {epoch}, low-water mark {low_water_mark}, next segment id {next_segment_id}"
                );
                let mut table = segment_table();
                for segment in &stream.segments {
                    table.push_row(segment.table_row());
                }
                rendered.push_str(&table.render());
            }
            render_list(&mut rendered, "assumptions", &stream.assumptions);
            render_list(&mut rendered, "problems", &stream.problems);
        }

        rendered.push('\n');
        if self.is_applied {
            rendered.push_str("applied. Start the controllers: they verify every copy and rebuild missing ones.\n");
        } else if self.problem_count() == 0 {
            rendered.push_str("dry run: nothing was written. Pass --apply to write this plan.\n");
        } else {
            rendered.push_str(
                "dry run: nothing was written. The plan has problems; fix them, or pass --apply \
                 --force to write what could be rebuilt.\n",
            );
        }
        rendered
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

    let mut report = RebuildReport {
        is_applied: false,
        replicas: Vec::new(),
        tiered_objects: 0,
        streams: Vec::new(),
        problems: Vec::new(),
    };
    let inventories = gather_inventories(cluster, &addresses_by_replica, &mut report).await;

    let tiered_objects = if let Some(artifact_store) = &cluster.artifact_store {
        find_tiered_objects(cluster, artifact_store.as_ref(), &mut report.problems).await
    } else {
        report.problems.push(
            "the configuration names no object store: segments that were left to the object \
             store cannot be found"
                .to_owned(),
        );
        Vec::new()
    };
    report.tiered_objects = tiered_objects.len();

    let mut deleted_stream_ids = BTreeSet::new();
    for deleted in cluster.metadata.list_deleted_streams().await? {
        deleted_stream_ids.insert(deleted.stream_id);
    }
    let mut names_by_stream_id = BTreeMap::new();
    for (stream_id, name) in names {
        names_by_stream_id.insert(*stream_id, name.clone());
    }
    let planned_streams = plan_rebuild(&RebuildInputs {
        existing_streams: cluster.metadata.list_streams().await?,
        deleted_stream_ids,
        names_by_stream_id,
        inventories,
        tiered_objects,
        now: cluster.clock.now(),
    });
    for planned in &planned_streams {
        report.streams.push(rebuilt_stream(planned));
    }

    if !is_applied {
        return Ok(report);
    }
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
    Ok(report)
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
        let client = cluster.connector.connect(replica_id, address);
        let answer = timeout(cluster.request_timeout, client.list_segments()).await;
        let copies = match answer {
            Ok(Ok(copies)) => Some(copies),
            Ok(Err(replica_error)) => {
                report.problems.push(format!(
                    "replica {replica_id} did not answer ({replica_error}): its copies are not in \
                     the plan"
                ));
                None
            }
            Err(_elapsed) => {
                report.problems.push(format!(
                    "replica {replica_id} did not answer in time: its copies are not in the plan"
                ));
                None
            }
        };
        report.replicas.push(AskedReplica {
            replica_id: replica_id.to_string(),
            address: address.clone(),
            has_answered: copies.is_some(),
            copies: copies.as_ref().map_or(0, Vec::len),
        });
        if let Some(copies) = copies {
            inventories.push(ReplicaInventory {
                replica_id: replica_id.clone(),
                copies,
            });
        }
    }
    inventories
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
/// An object that cannot be read, or whose footer does not describe the segment its key names, is added
/// to `problems` and left out.
async fn find_tiered_objects(
    cluster: &Cluster,
    artifact_store: &dyn ArtifactStore,
    problems: &mut Vec<String>,
) -> Vec<TieredObject> {
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
    let mut tiered_objects = Vec::new();
    for object in objects {
        let Some(address) = segment_address_of(&object.object_key) else {
            continue;
        };
        let footer_bytes = TIERED_FOOTER_BYTES as u64;
        let footer = if object.size_bytes < footer_bytes {
            None
        } else {
            let footer_range = object.size_bytes - footer_bytes..object.size_bytes;
            let fetched = timeout(
                cluster.request_timeout,
                artifact_store.get_range(&object.object_key, footer_range),
            )
            .await;
            match fetched {
                Ok(Ok(encoded_footer)) => TieredFooter::decode(&encoded_footer).ok(),
                _failed_or_timed_out => None,
            }
        };
        let is_the_named_segment = footer
            .is_some_and(|footer| footer.segment.address == address && footer.object_bytes() == object.size_bytes);
        match footer {
            Some(footer) if is_the_named_segment => tiered_objects.push(TieredObject {
                object_key: object.object_key,
                size_bytes: object.size_bytes,
                footer,
            }),
            _unreadable_or_another_segment => problems.push(format!(
                "object {:?} cannot be read as the segment its key names: it is not in the plan",
                object.object_key
            )),
        }
    }
    tiered_objects
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
