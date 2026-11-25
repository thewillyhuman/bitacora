//! The records bitacora keeps in the metadata store (design, section 8).
//!
//! Records are stored as JSON so that an operator can read them with the
//! store's own tools. Each carries a schema version; a build reads every
//! older version and writes the newest, so records are migrated lazily.

use std::collections::BTreeSet;
use std::time::Duration;

use bitacora_types::replica_api::SegmentDescriptor;
use bitacora_types::{
    Epoch, ReplicaId, ReplicationSettings, SegmentAddress, SegmentId, SequenceNumber, StreamId,
    Timestamp,
};
use serde::{Deserialize, Serialize};

/// The schema version this build writes.
pub const RECORD_SCHEMA_VERSION: u32 = 1;

/// How long a stream keeps records that nothing needs any more.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Keep every sealed segment at least this long, in milliseconds, for
    /// point-in-time recovery. `None` deletes as soon as truncation allows.
    pub keep_sealed_for_millis: Option<u64>,
}

impl RetentionPolicy {
    /// A policy that keeps sealed segments for at least `duration`.
    #[must_use]
    pub fn keep_for(duration: Duration) -> Self {
        Self {
            keep_sealed_for_millis: Some(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
        }
    }

    /// How long sealed segments are kept, if the policy sets a minimum.
    #[must_use]
    pub fn keep_sealed_for(self) -> Option<Duration> {
        self.keep_sealed_for_millis.map(Duration::from_millis)
    }
}

/// Who holds a stream's writer role, and until when without renewal.
///
/// The lease is a courtesy that prevents accidental takeover of a healthy
/// writer. Correctness depends only on the epoch (design, section 9).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterLease {
    /// A name the writer chose for itself, for operators.
    pub writer_identity: String,
    /// When the lease lapses unless renewed.
    pub expires_at: Timestamp,
}

/// Whether a replica's copy of a sealed segment is known to be complete.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyState {
    /// Nobody has checked the copy since the segment was sealed.
    Unverified,
    /// The copy holds the whole segment and passes its checksums.
    Good,
    /// The copy is short, damaged or lost, and must be replaced.
    Bad,
}

/// One replica's copy of a segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyRecord {
    /// The replica that holds the copy.
    pub replica_id: ReplicaId,
    /// What is known about the copy.
    pub state: CopyState,
}

/// When and where a segment ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealRecord {
    /// The segment's final sequence number. One below the first sequence
    /// number if the segment was sealed empty.
    pub end: SequenceNumber,
    /// When the seal was written.
    pub at: Timestamp,
}

/// Where a sealed segment lives in the object store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieredRecord {
    /// The key of the segment object.
    pub object_key: String,
    /// The size of the object, which locates its footer.
    pub size_bytes: u64,
    /// When the upload was verified.
    pub uploaded_at: Timestamp,
    /// Whether the replicas have dropped their copies after the hot-read
    /// window, leaving the object store as the only place to read from.
    pub are_local_copies_dropped: bool,
}

/// One segment of a stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRecord {
    /// The segment's position within the stream.
    pub segment_id: SegmentId,
    /// The epoch of the only writer that authors records in it.
    pub epoch: Epoch,
    /// The sequence number of its first record.
    pub first_sequence: SequenceNumber,
    /// Set once the segment is sealed.
    pub sealed: Option<SealRecord>,
    /// The replica set, fixed while the segment is open.
    pub copies: Vec<CopyRecord>,
    /// Set once the segment is uploaded to the object store.
    pub tiered: Option<TieredRecord>,
}

impl SegmentRecord {
    /// The segment's cluster-wide address.
    #[must_use]
    pub const fn address(&self, stream_id: StreamId) -> SegmentAddress {
        SegmentAddress::new(stream_id, self.segment_id)
    }

    /// What a replica needs to create its copy of the segment.
    #[must_use]
    pub const fn descriptor(&self, stream_id: StreamId) -> SegmentDescriptor {
        SegmentDescriptor {
            address: self.address(stream_id),
            epoch: self.epoch,
            first_sequence: self.first_sequence,
        }
    }

    /// The segment's final sequence number, if it is sealed.
    #[must_use]
    pub fn sealed_end(&self) -> Option<SequenceNumber> {
        self.sealed.map(|seal| seal.end)
    }

    /// Whether the segment still accepts appends.
    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.sealed.is_none()
    }

    /// Whether `sequence` belongs to this segment. An open segment holds
    /// everything from its first sequence on.
    #[must_use]
    pub fn contains(&self, sequence: SequenceNumber) -> bool {
        sequence >= self.first_sequence
            && self
                .sealed_end()
                .is_none_or(|sealed_end| sequence <= sealed_end)
    }
}

/// Everything the metadata store knows about one stream, except its
/// materializers' checkpoints, which have keys of their own so that
/// checkpointing never contends with the writer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecord {
    /// The schema version the record was written with.
    pub schema_version: u32,
    /// The stream's permanent identifier.
    pub stream_id: StreamId,
    /// The stream's name.
    pub name: String,
    /// How many replicas hold each segment and how many commit a record.
    pub replication: ReplicationSettings,
    /// How long records nothing needs are kept.
    pub retention: RetentionPolicy,
    /// The current writer generation. All fencing rests on it.
    pub epoch: Epoch,
    /// The current writer's lease, if a writer holds the stream.
    pub writer: Option<WriterLease>,
    /// The stream's segments, oldest first. The last one is the open
    /// segment unless it is sealed.
    pub segments: Vec<SegmentRecord>,
    /// The replica set the controller pre-allocated for the next segment,
    /// so that a roll never waits on placement.
    pub standby_replicas: Option<Vec<ReplicaId>>,
    /// The position below which the owner asked for records to be deleted.
    pub truncate_before: Option<SequenceNumber>,
    /// The first sequence number readers may still ask for. Everything below
    /// it is deleted or about to be.
    pub low_water_mark: SequenceNumber,
    /// The materializers whose checkpoints hold the low-water mark back.
    pub materializers: BTreeSet<String>,
}

impl StreamRecord {
    /// The most recent segment, open or sealed.
    #[must_use]
    pub fn last_segment(&self) -> Option<&SegmentRecord> {
        self.segments.last()
    }

    /// The segment the writer is appending to, if there is one.
    #[must_use]
    pub fn open_segment(&self) -> Option<&SegmentRecord> {
        self.last_segment().filter(|segment| segment.is_open())
    }

    /// The segment that holds `sequence`, if it has not been deleted.
    #[must_use]
    pub fn segment_containing(&self, sequence: SequenceNumber) -> Option<&SegmentRecord> {
        self.segments
            .iter()
            .find(|segment| segment.contains(sequence))
    }

    /// The sequence number the next segment will start at.
    #[must_use]
    pub fn next_segment_first_sequence(&self) -> SequenceNumber {
        match self.last_segment() {
            Some(last_segment) => last_segment
                .sealed_end()
                .map_or(last_segment.first_sequence, SequenceNumber::next),
            None => self.low_water_mark,
        }
    }
}

/// A materializer's durable statement: "everything up to `checkpoint` is now
/// in my artifacts".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    /// The schema version the record was written with.
    pub schema_version: u32,
    /// The materializer's name, unique within its stream.
    pub materializer: String,
    /// The last sequence number covered by the materializer's artifacts.
    pub checkpoint: SequenceNumber,
    /// Incremented every time a runner takes over. Only the holder of the
    /// current token can commit a checkpoint.
    pub fencing_token: u64,
    /// Where the manifest of the checkpoint's artifacts is stored.
    pub manifest: Option<String>,
}

/// What a replica registers about itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaRecord {
    /// The schema version the record was written with.
    pub schema_version: u32,
    /// The identity of the replica's data directory.
    pub replica_id: ReplicaId,
    /// Where writers and readers reach the replica.
    pub address: String,
    /// The replica's failure domains, widest first: zone, rack, host.
    pub failure_domains: Vec<String>,
    /// The total size of the replica's disk.
    pub capacity_bytes: u64,
    /// How much of the disk is unused.
    pub free_bytes: u64,
    /// Whether the replica is being drained for maintenance.
    pub is_draining: bool,
    /// When the replica last renewed its registration.
    pub heartbeat_at: Timestamp,
}

impl ReplicaRecord {
    /// The share of the replica's disk in use, in percent, rounded down.
    #[must_use]
    pub fn used_percent(&self) -> u8 {
        if self.capacity_bytes == 0 {
            return 100;
        }
        let used_bytes = u128::from(self.capacity_bytes.saturating_sub(self.free_bytes));
        let percent = used_bytes * 100 / u128::from(self.capacity_bytes);
        u8::try_from(percent).expect("a share of the whole is at most 100")
    }

    /// Whether the replica renewed its registration within `liveness_window`
    /// before `now`.
    #[must_use]
    pub fn is_alive(&self, now: Timestamp, liveness_window: Duration) -> bool {
        now.duration_since(self.heartbeat_at) <= liveness_window
    }
}

/// A stream that was deleted and whose segments still have to be removed
/// from replicas and the object store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedStreamRecord {
    /// The identifier the stream had. It is never reused.
    pub stream_id: StreamId,
    /// The name the stream had, for operators. It may be in use again.
    pub name: String,
}

/// Which controller instance is active, and until when without renewal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderRecord {
    /// The name of the active controller instance.
    pub holder: String,
    /// When its leadership lapses unless renewed.
    pub expires_at: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(segment_id: u64, first_sequence: u64, sealed_end: Option<u64>) -> SegmentRecord {
        SegmentRecord {
            segment_id: SegmentId::new(segment_id),
            epoch: Epoch::new(1),
            first_sequence: SequenceNumber::new(first_sequence),
            sealed: sealed_end.map(|end| SealRecord {
                end: SequenceNumber::new(end),
                at: Timestamp::UNIX_EPOCH,
            }),
            copies: Vec::new(),
            tiered: None,
        }
    }

    fn stream(segments: Vec<SegmentRecord>) -> StreamRecord {
        StreamRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            stream_id: StreamId::new(7),
            name: "orders".to_owned(),
            replication: ReplicationSettings::DEFAULT,
            retention: RetentionPolicy::default(),
            epoch: Epoch::new(1),
            writer: None,
            segments,
            standby_replicas: None,
            truncate_before: None,
            low_water_mark: SequenceNumber::FIRST,
            materializers: BTreeSet::new(),
        }
    }

    #[test]
    fn sealed_segment_contains_exactly_its_range() {
        let sealed = segment(1, 10, Some(19));
        let contained: Vec<bool> = [9, 10, 19, 20]
            .iter()
            .map(|sequence| sealed.contains(SequenceNumber::new(*sequence)))
            .collect();
        assert_eq!(contained, [false, true, true, false]);
    }

    #[test]
    fn open_segment_contains_everything_from_its_start() {
        let open = segment(1, 10, None);
        assert!(
            !open.contains(SequenceNumber::new(9)) && open.contains(SequenceNumber::new(1_000_000))
        );
    }

    #[test]
    fn segment_sealed_empty_contains_nothing() {
        let sealed_empty = segment(1, 10, Some(9));
        assert!(!sealed_empty.contains(SequenceNumber::new(10)));
    }

    #[test]
    fn stream_finds_the_segment_holding_a_sequence() {
        let orders = stream(vec![segment(1, 1, Some(9)), segment(2, 10, None)]);
        let found = orders.segment_containing(SequenceNumber::new(9));
        assert_eq!(
            found.map(|segment| segment.segment_id),
            Some(SegmentId::new(1))
        );
    }

    #[test]
    fn stream_skips_an_empty_sealed_segment_when_locating_a_sequence() {
        let orders = stream(vec![segment(1, 1, Some(0)), segment(2, 1, None)]);
        let found = orders.segment_containing(SequenceNumber::new(1));
        assert_eq!(
            found.map(|segment| segment.segment_id),
            Some(SegmentId::new(2))
        );
    }

    #[test]
    fn open_segment_is_the_last_one_only_while_unsealed() {
        let writing = stream(vec![segment(1, 1, Some(9)), segment(2, 10, None)]);
        let closed = stream(vec![segment(1, 1, Some(9))]);
        assert!(writing.open_segment().is_some() && closed.open_segment().is_none());
    }

    #[test]
    fn next_segment_of_a_fresh_stream_starts_at_the_first_record() {
        assert_eq!(
            stream(Vec::new()).next_segment_first_sequence(),
            SequenceNumber::FIRST
        );
    }

    #[test]
    fn next_segment_starts_right_after_the_last_sealed_end() {
        let orders = stream(vec![segment(1, 1, Some(9))]);
        assert_eq!(
            orders.next_segment_first_sequence(),
            SequenceNumber::new(10)
        );
    }

    #[test]
    fn next_segment_after_an_empty_sealed_one_reuses_its_start() {
        let orders = stream(vec![segment(1, 1, Some(9)), segment(2, 10, Some(9))]);
        assert_eq!(
            orders.next_segment_first_sequence(),
            SequenceNumber::new(10)
        );
    }

    #[test]
    fn retention_duration_survives_the_millisecond_encoding() {
        let policy = RetentionPolicy::keep_for(Duration::from_hours(7 * 24));
        assert_eq!(policy.keep_sealed_for(), Some(Duration::from_hours(7 * 24)));
    }

    #[test]
    fn replica_is_alive_within_its_liveness_window_only() {
        let replica = ReplicaRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            replica_id: ReplicaId::new("replica-a").expect("valid identifier"),
            address: "replica-a:7000".to_owned(),
            failure_domains: Vec::new(),
            capacity_bytes: 1_000,
            free_bytes: 250,
            is_draining: false,
            heartbeat_at: Timestamp::from_unix_millis(10_000),
        };
        let window = Duration::from_secs(5);
        assert!(
            replica.is_alive(Timestamp::from_unix_millis(15_000), window)
                && !replica.is_alive(Timestamp::from_unix_millis(15_001), window)
                && replica.used_percent() == 75
        );
    }

    #[test]
    fn stream_record_survives_a_json_round_trip() {
        let mut orders = stream(vec![segment(1, 1, Some(9)), segment(2, 10, None)]);
        orders.materializers.insert("pages".to_owned());
        orders.writer = Some(WriterLease {
            writer_identity: "compute-1".to_owned(),
            expires_at: Timestamp::from_unix_millis(5_000),
        });
        let serialized = serde_json::to_vec(&orders).expect("serializable");
        let deserialized: StreamRecord = serde_json::from_slice(&serialized).expect("valid");
        assert_eq!(deserialized, orders);
    }

    #[test]
    fn copy_states_are_stored_as_readable_words() {
        let serialized = serde_json::to_string(&CopyState::Unverified).expect("serializable");
        assert_eq!(serialized, "\"unverified\"");
    }
}
