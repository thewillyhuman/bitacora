//! The records bitacora keeps in the metadata store.
//!
//! # Why this file exists
//!
//! The metadata store holds what nothing else knows: which segments make up a stream and where they end, who may write,
//! how far each materializer got, which replicas exist and who leads the controllers (design, section 8). Writers,
//! readers, the controller, materializers and operator tools all read and write these records, and they are upgraded
//! one at a time, so the records are an interface between programs and versions. This file is where that interface is
//! defined, in one place, as plain data.
//!
//! # What it contains
//!
//! - [`StreamRecord`], with [`SegmentRecord`], [`CopyRecord`], [`CopyState`], [`SealRecord`], [`TieredRecord`],
//!   [`WriterLease`] and [`RetentionPolicy`]: everything about one stream.
//! - [`CheckpointRecord`]: how far one materializer got, and who may say so.
//! - [`ReplicaRecord`]: what a replica registers about itself.
//! - [`LeaderRecord`]: which controller instance is active.
//! - [`DeletedStreamRecord`]: a note that a deleted stream still has data to remove.
//! - [`RECORD_SCHEMA_VERSION`]: the version this build writes.
//!
//! # How it works
//!
//! Records are stored as JSON, so that an operator can read them with the store's own tools. The field names and the
//! shape of every record are fixed by `docs/protocol/metadata.md` and held to it by golden texts in
//! `tests/records_golden_test.rs`: renaming a field here fails that test.
//!
//! The larger records carry a schema version. A build reads every older version and writes the newest, so records are
//! migrated lazily, the next time they are written. A field added later is optional when read, so that a record written
//! by an older program still decodes.
//!
//! A stream's checkpoints are not part of its record. They have keys of their own, so that a materializer committing a
//! checkpoint never contends with the writer rolling a segment.
//!
//! The records have a few methods, all of them pure questions about the data: which segment holds a sequence number,
//! where the next segment starts, whether a replica is alive. What may change, and on which condition, is not decided
//! here but in the files under `store/`.
//!
//! # Invariants
//!
//! - A stream's segments are listed oldest first, and at most the last one is open.
//! - Segment identifiers within a stream only grow: `next_segment_id` is above every identifier that was ever used.
//! - A stream's epoch, its low-water mark and a materializer's checkpoint never decrease.
//! - Nothing here does I/O or reads a clock.
//!
//! # See also
//!
//! - `store.rs` and the files under `store/`: the operations that read and write these records.
//! - `tests/records_golden_test.rs`: the JSON of every record, as schema version 1 wrote it.
//! - `docs/protocol/metadata.md`: the same records, for a reader who does not read Rust.

use std::collections::BTreeSet;
use std::time::Duration;

use bitacora_types::replica_api::SegmentDescriptor;
use bitacora_types::{
    Epoch, ReplicaId, ReplicationSettings, SegmentAddress, SegmentId, SequenceNumber, StreamId, Timestamp,
};
use serde::{Deserialize, Serialize};

/// The schema version this build writes into every record that carries one.
pub const RECORD_SCHEMA_VERSION: u32 = 1;

/// The share of a disk, in percent, that is in use when all of it is.
const FULL_PERCENT: u8 = 100;

/// How long a stream keeps records that nothing needs any more.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_meta::RetentionPolicy;
///
/// let keep_a_week = RetentionPolicy::keep_for(Duration::from_secs(7 * 24 * 60 * 60));
/// let delete_as_soon_as_allowed = RetentionPolicy::default();
///
/// assert!(keep_a_week.keep_sealed_for().is_some());
/// assert_eq!(delete_as_soon_as_allowed.keep_sealed_for(), None);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Keep every sealed segment at least this long, in milliseconds, for point-in-time recovery. `None` deletes as
    /// soon as truncation allows.
    pub keep_sealed_for_millis: Option<u64>,
}

impl RetentionPolicy {
    /// Builds a policy that keeps sealed segments for at least `duration`.
    ///
    /// # Arguments
    ///
    /// * `duration` - How long a segment stays after it was sealed, whatever the materializers' checkpoints say. It is
    ///   stored in whole milliseconds; a duration too long to count in 64 bits of milliseconds is stored as the longest
    ///   one that fits.
    ///
    /// # Returns
    ///
    /// The policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use bitacora_meta::RetentionPolicy;
    ///
    /// let policy = RetentionPolicy::keep_for(Duration::from_secs(60));
    ///
    /// assert_eq!(policy.keep_sealed_for_millis, Some(60_000));
    /// ```
    #[must_use]
    pub fn keep_for(duration: Duration) -> Self {
        Self {
            keep_sealed_for_millis: Some(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
        }
    }

    /// Returns how long sealed segments are kept.
    ///
    /// # Returns
    ///
    /// The minimum time a sealed segment stays, or `None` if the policy sets none and segments are deleted as soon as
    /// truncation allows.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use bitacora_meta::RetentionPolicy;
    ///
    /// let policy = RetentionPolicy::keep_for(Duration::from_secs(60));
    ///
    /// assert_eq!(policy.keep_sealed_for(), Some(Duration::from_secs(60)));
    /// ```
    #[must_use]
    pub fn keep_sealed_for(self) -> Option<Duration> {
        self.keep_sealed_for_millis.map(Duration::from_millis)
    }
}

/// Who holds a stream's writer role, and until when without renewal.
///
/// The lease is a courtesy that prevents accidental takeover of a healthy writer. Correctness depends only on the epoch
/// (design, section 9).
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// # use std::sync::Arc;
/// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
/// # use bitacora_types::{Epoch, ReplicationSettings, Timestamp};
/// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// # metadata
/// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
/// #     .await?;
/// let lease_expires_at = Timestamp::from_unix_millis(10_000);
///
/// let claimed = metadata
///     .claim_epoch("orders", Epoch::ZERO, "compute-1", lease_expires_at)
///     .await?;
///
/// let lease = claimed.writer.expect("a claim records the writer's lease");
/// assert_eq!(lease.writer_identity, "compute-1");
/// assert_eq!(lease.expires_at, lease_expires_at);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterLease {
    /// A name the writer chose for itself, for operators. Nothing compares it.
    pub writer_identity: String,
    /// When the lease lapses unless the writer renews it.
    pub expires_at: Timestamp,
}

/// Whether a replica's copy of a sealed segment is known to be complete.
///
/// The state means something only once the segment is sealed: while it is open, the writer is the one who knows what
/// each replica holds.
///
/// # Examples
///
/// A state is stored as a lower-case word:
///
/// ```
/// use bitacora_meta::CopyState;
///
/// let stored_state = serde_json::to_string(&CopyState::Unverified).expect("a copy state is plain data");
///
/// assert_eq!(stored_state, r#""unverified""#);
/// ```
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
///
/// # Examples
///
/// The copies of a segment that was just opened are the standby replicas, none of them verified yet:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// # use std::sync::Arc;
/// # use bitacora_meta::{CopyState, MemoryKeyValueStore, MetadataStore, RetentionPolicy};
/// # use bitacora_types::{Epoch, ReplicaId, ReplicationSettings, Timestamp};
/// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// # let lease_expires_at = Timestamp::from_unix_millis(10_000);
/// let standby_replica = ReplicaId::new("replica-a").expect("a valid identifier");
/// metadata
///     .create_stream(
///         "orders",
///         ReplicationSettings::DEFAULT,
///         RetentionPolicy::default(),
///         Some(vec![standby_replica.clone()]),
///     )
///     .await?;
/// metadata.claim_epoch("orders", Epoch::ZERO, "compute-1", lease_expires_at).await?;
///
/// let (_stream, opened_segment) = metadata.open_segment("orders", Epoch::new(1)).await?;
///
/// let copy = &opened_segment.copies[0];
/// assert_eq!(copy.replica_id, standby_replica);
/// assert_eq!(copy.state, CopyState::Unverified);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyRecord {
    /// The replica that holds the copy.
    pub replica_id: ReplicaId,
    /// What is known about the copy.
    pub state: CopyState,
}

/// When and where a segment ended.
///
/// # Examples
///
/// A segment that starts at sequence number 10 and is sealed without a record ends one below its start:
///
/// ```
/// use bitacora_meta::SealRecord;
/// use bitacora_types::{SequenceNumber, Timestamp};
///
/// let first_sequence = SequenceNumber::new(10);
///
/// let empty_seal = SealRecord {
///     end: first_sequence.preceding(),
///     at: Timestamp::from_unix_millis(5_000),
/// };
///
/// assert_eq!(empty_seal.end, SequenceNumber::new(9));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealRecord {
    /// The segment's final sequence number. One below the first sequence number if the segment was sealed empty.
    pub end: SequenceNumber,
    /// When the seal was written. Retention and tiering count from here.
    pub at: Timestamp,
}

/// Where a sealed segment lives in the object store.
///
/// # Examples
///
/// ```
/// use bitacora_meta::TieredRecord;
/// use bitacora_types::Timestamp;
///
/// let just_uploaded = TieredRecord {
///     object_key: "streams/7/segments/00000000000000000003".to_owned(),
///     size_bytes: 67_108_864,
///     uploaded_at: Timestamp::from_unix_millis(5_000),
///     are_local_copies_dropped: false,
/// };
///
/// let stored_record = serde_json::to_value(&just_uploaded).expect("a record is plain data");
///
/// assert_eq!(stored_record["size_bytes"], 67_108_864_u64);
/// assert_eq!(stored_record["uploaded_at"], 5_000);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieredRecord {
    /// The key of the segment object.
    pub object_key: String,
    /// The size of the object in bytes, which locates its footer.
    pub size_bytes: u64,
    /// When the upload was verified. The hot-read window counts from here.
    pub uploaded_at: Timestamp,
    /// Whether the replicas have dropped their copies after the hot-read window, leaving the object store as the only
    /// place to read from.
    pub are_local_copies_dropped: bool,
}

/// One segment of a stream.
///
/// # Examples
///
/// ```
/// use bitacora_meta::{SealRecord, SegmentRecord};
/// use bitacora_types::{Epoch, SegmentId, SequenceNumber, Timestamp};
///
/// let sealed_segment = SegmentRecord {
///     segment_id: SegmentId::new(3),
///     epoch: Epoch::new(2),
///     first_sequence: SequenceNumber::new(10),
///     sealed: Some(SealRecord {
///         end: SequenceNumber::new(19),
///         at: Timestamp::from_unix_millis(5_000),
///     }),
///     copies: Vec::new(),
///     tiered: None,
/// };
///
/// assert!(sealed_segment.contains(SequenceNumber::new(19)));
/// assert!(!sealed_segment.contains(SequenceNumber::new(20)));
/// ```
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
    /// The replica set, fixed while the segment is open. Empty once the segment was left to the object store alone.
    pub copies: Vec<CopyRecord>,
    /// Set once the segment is uploaded to the object store.
    pub tiered: Option<TieredRecord>,
}

impl SegmentRecord {
    /// Returns the segment's cluster-wide address.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream this segment belongs to. The record does not hold it; its stream record does.
    ///
    /// # Returns
    ///
    /// The stream identifier together with the segment identifier, which is how replicas name a segment.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bitacora_meta::SegmentRecord;
    /// # use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// # let segment = SegmentRecord {
    /// #     segment_id: SegmentId::new(3),
    /// #     epoch: Epoch::new(2),
    /// #     first_sequence: SequenceNumber::new(10),
    /// #     sealed: None,
    /// #     copies: Vec::new(),
    /// #     tiered: None,
    /// # };
    /// let address = segment.address(StreamId::new(7));
    ///
    /// assert_eq!(address, SegmentAddress::new(StreamId::new(7), SegmentId::new(3)));
    /// ```
    #[must_use]
    pub const fn address(&self, stream_id: StreamId) -> SegmentAddress {
        SegmentAddress::new(stream_id, self.segment_id)
    }

    /// Returns what a replica needs to create its copy of the segment.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream this segment belongs to.
    ///
    /// # Returns
    ///
    /// The segment's address, the epoch that authors it and its first sequence number: what a replica writes into the
    /// header of the segment file.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bitacora_meta::SegmentRecord;
    /// # use bitacora_types::{Epoch, SegmentId, SequenceNumber, StreamId};
    /// # let segment = SegmentRecord {
    /// #     segment_id: SegmentId::new(3),
    /// #     epoch: Epoch::new(2),
    /// #     first_sequence: SequenceNumber::new(10),
    /// #     sealed: None,
    /// #     copies: Vec::new(),
    /// #     tiered: None,
    /// # };
    /// let descriptor = segment.descriptor(StreamId::new(7));
    ///
    /// assert_eq!(descriptor.epoch, Epoch::new(2));
    /// assert_eq!(descriptor.first_sequence, SequenceNumber::new(10));
    /// ```
    #[must_use]
    pub const fn descriptor(&self, stream_id: StreamId) -> SegmentDescriptor {
        SegmentDescriptor {
            address: self.address(stream_id),
            epoch: self.epoch,
            first_sequence: self.first_sequence,
        }
    }

    /// Returns the segment's final sequence number, if it is sealed.
    ///
    /// # Returns
    ///
    /// The sealed end, or `None` while the segment is open. For a segment that was sealed empty, the end is one below
    /// its first sequence number.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bitacora_meta::SegmentRecord;
    /// # use bitacora_types::{Epoch, SegmentId, SequenceNumber};
    /// # let open_segment = SegmentRecord {
    /// #     segment_id: SegmentId::new(3),
    /// #     epoch: Epoch::new(2),
    /// #     first_sequence: SequenceNumber::new(10),
    /// #     sealed: None,
    /// #     copies: Vec::new(),
    /// #     tiered: None,
    /// # };
    /// assert_eq!(open_segment.sealed_end(), None);
    /// ```
    #[must_use]
    pub fn sealed_end(&self) -> Option<SequenceNumber> {
        self.sealed.map(|seal| seal.end)
    }

    /// Says whether the segment still accepts appends.
    ///
    /// # Returns
    ///
    /// `true` until the segment is sealed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bitacora_meta::SegmentRecord;
    /// # use bitacora_types::{Epoch, SegmentId, SequenceNumber};
    /// # let open_segment = SegmentRecord {
    /// #     segment_id: SegmentId::new(3),
    /// #     epoch: Epoch::new(2),
    /// #     first_sequence: SequenceNumber::new(10),
    /// #     sealed: None,
    /// #     copies: Vec::new(),
    /// #     tiered: None,
    /// # };
    /// assert!(open_segment.is_open());
    /// ```
    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.sealed.is_none()
    }

    /// Says whether `sequence` belongs to this segment.
    ///
    /// A sealed segment holds its first sequence number up to its sealed end. An open segment holds everything from its
    /// first sequence number on, because nobody knows yet where it will end. A segment that was sealed empty holds
    /// nothing.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The sequence number to look for.
    ///
    /// # Returns
    ///
    /// `true` if a record with this sequence number is, or would be, in this segment.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bitacora_meta::SegmentRecord;
    /// # use bitacora_types::{Epoch, SegmentId, SequenceNumber};
    /// # let open_segment = SegmentRecord {
    /// #     segment_id: SegmentId::new(3),
    /// #     epoch: Epoch::new(2),
    /// #     first_sequence: SequenceNumber::new(10),
    /// #     sealed: None,
    /// #     copies: Vec::new(),
    /// #     tiered: None,
    /// # };
    /// assert!(!open_segment.contains(SequenceNumber::new(9)));
    /// assert!(open_segment.contains(SequenceNumber::new(1_000_000)));
    /// ```
    #[must_use]
    pub fn contains(&self, sequence: SequenceNumber) -> bool {
        if sequence < self.first_sequence {
            return false;
        }
        match self.sealed_end() {
            Some(sealed_end) => sequence <= sealed_end,
            None => true,
        }
    }
}

/// Everything the metadata store knows about one stream.
///
/// The checkpoints of the stream's materializers are not in it. They have keys of their own, so that checkpointing
/// never contends with the writer.
///
/// # Examples
///
/// A stream record is obtained from the metadata store, not built by hand:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// use std::sync::Arc;
///
/// use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
/// use bitacora_types::{ReplicationSettings, SequenceNumber};
///
/// let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// let stream = metadata
///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
///     .await?;
///
/// assert!(stream.segments.is_empty());
/// assert_eq!(stream.next_segment_first_sequence(), SequenceNumber::FIRST);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecord {
    /// The schema version the record was written with.
    pub schema_version: u32,
    /// The stream's permanent identifier. Never reused, also after the stream is deleted.
    pub stream_id: StreamId,
    /// The stream's name, which is also what its key is made from.
    pub name: String,
    /// How many replicas hold each segment and how many commit a record.
    pub replication: ReplicationSettings,
    /// How long records that nothing needs are kept.
    pub retention: RetentionPolicy,
    /// The current writer generation. All fencing rests on it.
    pub epoch: Epoch,
    /// The current writer's lease, if a writer holds the stream.
    pub writer: Option<WriterLease>,
    /// The stream's segments, oldest first. The last one is the open segment unless it is sealed.
    pub segments: Vec<SegmentRecord>,
    /// The identifier the next segment gets. Identifiers are never reused, not even after every segment was deleted:
    /// replicas may still hold copies of old segments, and a new segment must not be taken for one.
    pub next_segment_id: SegmentId,
    /// The replica set the controller pre-allocated for the next segment, so that a roll never waits on placement.
    pub standby_replicas: Option<Vec<ReplicaId>>,
    /// The position below which the owner asked for records to be deleted.
    pub truncate_before: Option<SequenceNumber>,
    /// The first sequence number readers may still ask for. Everything below it is deleted or about to be.
    pub low_water_mark: SequenceNumber,
    /// The materializers whose checkpoints hold the low-water mark back.
    pub materializers: BTreeSet<String>,
}

impl StreamRecord {
    /// Returns the most recent segment, open or sealed.
    ///
    /// # Returns
    ///
    /// The last segment of the list, or `None` if the stream has none: it was never written, or retention deleted
    /// everything.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::ReplicationSettings;
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let new_stream = metadata
    ///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await?;
    ///
    /// assert!(new_stream.last_segment().is_none());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn last_segment(&self) -> Option<&SegmentRecord> {
        self.segments.last()
    }

    /// Returns the segment the writer is appending to, if there is one.
    ///
    /// # Returns
    ///
    /// The last segment if it is not sealed. `None` if the stream has no segment or its last segment is sealed, as
    /// between a seal and the opening of the next segment.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{Epoch, ReplicaId, ReplicationSettings, Timestamp};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let standby_replicas = vec![ReplicaId::new("replica-a").expect("a valid identifier")];
    /// # metadata
    /// #     .create_stream(
    /// #         "orders",
    /// #         ReplicationSettings::DEFAULT,
    /// #         RetentionPolicy::default(),
    /// #         Some(standby_replicas),
    /// #     )
    /// #     .await?;
    /// # let lease_expires_at = Timestamp::from_unix_millis(10_000);
    /// metadata.claim_epoch("orders", Epoch::ZERO, "compute-1", lease_expires_at).await?;
    /// let (stream, opened_segment) = metadata.open_segment("orders", Epoch::new(1)).await?;
    ///
    /// assert_eq!(stream.open_segment(), Some(&opened_segment));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn open_segment(&self) -> Option<&SegmentRecord> {
        self.last_segment().filter(|segment| segment.is_open())
    }

    /// Returns the segment that holds `sequence`, if it has not been deleted.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The sequence number to locate.
    ///
    /// # Returns
    ///
    /// The first listed segment that contains the sequence number (see [`SegmentRecord::contains`]), or `None` if no
    /// listed segment does: the record was truncated, or lies beyond a sealed last segment. A segment still listed
    /// below the low-water mark is returned too; whether a position may be read is the caller's question to the mark.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{ReplicationSettings, SequenceNumber};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let new_stream = metadata
    ///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await?;
    ///
    /// assert!(new_stream.segment_containing(SequenceNumber::FIRST).is_none());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn segment_containing(&self, sequence: SequenceNumber) -> Option<&SegmentRecord> {
        self.segments.iter().find(|segment| segment.contains(sequence))
    }

    /// Returns the sequence number the next segment will start at.
    ///
    /// # Returns
    ///
    /// One past the sealed end of the last segment. If the last segment is still open, its own first sequence number,
    /// which is what an empty seal would leave. If the stream has no segment, the low-water mark: everything below it
    /// is gone, so the stream continues there.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{ReplicationSettings, SequenceNumber};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let new_stream = metadata
    ///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await?;
    ///
    /// assert_eq!(new_stream.next_segment_first_sequence(), SequenceNumber::FIRST);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn next_segment_first_sequence(&self) -> SequenceNumber {
        let Some(last_segment) = self.last_segment() else {
            return self.low_water_mark;
        };
        match last_segment.sealed_end() {
            Some(sealed_end) => sealed_end.next(),
            None => last_segment.first_sequence,
        }
    }
}

/// A materializer's durable statement: "everything up to `checkpoint` is now in my artifacts".
///
/// # Examples
///
/// The checkpoint of a materializer that was just registered: no runner has taken it, and it has no artifacts yet.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// # use std::sync::Arc;
/// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
/// # use bitacora_types::ReplicationSettings;
/// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// # metadata
/// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
/// #     .await?;
/// let checkpoint = metadata.register_materializer("orders", "pages").await?;
///
/// assert_eq!(checkpoint.materializer, "pages");
/// assert_eq!(checkpoint.fencing_token, 0);
/// assert_eq!(checkpoint.manifest, None);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    /// The schema version the record was written with.
    pub schema_version: u32,
    /// The materializer's name, unique within its stream.
    pub materializer: String,
    /// The last sequence number covered by the materializer's artifacts.
    pub checkpoint: SequenceNumber,
    /// Incremented every time a runner takes over. Only the holder of the current token can commit a checkpoint.
    pub fencing_token: u64,
    /// Where the manifest of the checkpoint's artifacts is stored. Opaque to the log.
    pub manifest: Option<String>,
}

/// What a replica registers about itself.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_meta::{RECORD_SCHEMA_VERSION, ReplicaRecord};
/// use bitacora_types::{ReplicaId, Timestamp};
///
/// let replica = ReplicaRecord {
///     schema_version: RECORD_SCHEMA_VERSION,
///     replica_id: ReplicaId::new("replica-a").expect("a valid identifier"),
///     address: "10.0.0.11:7000".to_owned(),
///     failure_domains: vec!["zone-1".to_owned(), "rack-4".to_owned(), "host-11".to_owned()],
///     capacity_bytes: 1_000,
///     free_bytes: 250,
///     is_draining: false,
///     heartbeat_at: Timestamp::from_unix_millis(10_000),
/// };
///
/// assert_eq!(replica.used_percent(), 75);
/// assert!(replica.is_alive(Timestamp::from_unix_millis(12_000), Duration::from_secs(5)));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaRecord {
    /// The schema version the record was written with.
    pub schema_version: u32,
    /// The identity of the replica's data directory.
    pub replica_id: ReplicaId,
    /// Where writers and readers reach the replica, as `host:port`.
    pub address: String,
    /// The replica's failure domains, widest first: zone, rack, host.
    pub failure_domains: Vec<String>,
    /// The total size of the replica's disk, in bytes.
    pub capacity_bytes: u64,
    /// How much of the disk is unused, in bytes.
    pub free_bytes: u64,
    /// Whether the replica is being drained for maintenance.
    pub is_draining: bool,
    /// When the replica last renewed its registration.
    pub heartbeat_at: Timestamp,
}

impl ReplicaRecord {
    /// Returns the share of the replica's disk that is in use.
    ///
    /// # Returns
    ///
    /// The used share in percent, rounded down, from 0 to 100. A replica that reports no capacity counts as full, so
    /// that nothing is placed on it.
    ///
    /// # Panics
    ///
    /// Never in practice: the share of a whole is at most 100, which fits the return type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bitacora_meta::{RECORD_SCHEMA_VERSION, ReplicaRecord};
    /// # use bitacora_types::{ReplicaId, Timestamp};
    /// # let mut replica = ReplicaRecord {
    /// #     schema_version: RECORD_SCHEMA_VERSION,
    /// #     replica_id: ReplicaId::new("replica-a").expect("a valid identifier"),
    /// #     address: "10.0.0.11:7000".to_owned(),
    /// #     failure_domains: Vec::new(),
    /// #     capacity_bytes: 0,
    /// #     free_bytes: 0,
    /// #     is_draining: false,
    /// #     heartbeat_at: Timestamp::from_unix_millis(10_000),
    /// # };
    /// replica.capacity_bytes = 0;
    ///
    /// assert_eq!(replica.used_percent(), 100);
    /// ```
    #[must_use]
    pub fn used_percent(&self) -> u8 {
        if self.capacity_bytes == 0 {
            return FULL_PERCENT;
        }
        let used_bytes = u128::from(self.capacity_bytes.saturating_sub(self.free_bytes));
        let percent = used_bytes * u128::from(FULL_PERCENT) / u128::from(self.capacity_bytes);
        u8::try_from(percent).expect("a share of the whole is at most 100")
    }

    /// Says whether the replica renewed its registration recently enough to count as alive.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time, from the caller's clock.
    /// * `liveness_window` - How old the last heartbeat may be. A heartbeat exactly this old still counts.
    ///
    /// # Returns
    ///
    /// `true` if the last heartbeat is at most `liveness_window` before `now`. A heartbeat that lies after `now`, as a
    /// clock that runs behind would see it, counts as alive.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use bitacora_meta::{RECORD_SCHEMA_VERSION, ReplicaRecord};
    /// # use bitacora_types::{ReplicaId, Timestamp};
    /// # let replica = ReplicaRecord {
    /// #     schema_version: RECORD_SCHEMA_VERSION,
    /// #     replica_id: ReplicaId::new("replica-a").expect("a valid identifier"),
    /// #     address: "10.0.0.11:7000".to_owned(),
    /// #     failure_domains: Vec::new(),
    /// #     capacity_bytes: 1_000,
    /// #     free_bytes: 250,
    /// #     is_draining: false,
    /// #     heartbeat_at: Timestamp::from_unix_millis(10_000),
    /// # };
    /// let long_after_the_heartbeat = Timestamp::from_unix_millis(60_000);
    ///
    /// assert!(!replica.is_alive(long_after_the_heartbeat, Duration::from_secs(5)));
    /// ```
    #[must_use]
    pub fn is_alive(&self, now: Timestamp, liveness_window: Duration) -> bool {
        now.duration_since(self.heartbeat_at) <= liveness_window
    }
}

/// A stream that was deleted and whose segments still have to be removed from replicas and the object store.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// # use std::sync::Arc;
/// # use bitacora_meta::{DeletedStreamRecord, MemoryKeyValueStore, MetadataStore, RetentionPolicy};
/// # use bitacora_types::ReplicationSettings;
/// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// # metadata
/// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
/// #     .await?;
/// let deleted_stream = metadata.delete_stream("orders").await?;
///
/// let notes = metadata.list_deleted_streams().await?;
///
/// let expected_note = DeletedStreamRecord {
///     stream_id: deleted_stream.stream_id,
///     name: "orders".to_owned(),
/// };
/// assert_eq!(notes, [expected_note]);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedStreamRecord {
    /// The identifier the stream had. It is never reused.
    pub stream_id: StreamId,
    /// The name the stream had, for operators. It may be in use again.
    pub name: String,
}

/// Which controller instance is active, and until when without renewal.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// # use std::sync::Arc;
/// # use bitacora_meta::{LeaderRecord, MemoryKeyValueStore, MetadataStore};
/// # use bitacora_types::Timestamp;
/// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// let now = Timestamp::from_unix_millis(1_000);
/// let in_five_seconds = Timestamp::from_unix_millis(6_000);
///
/// metadata.try_lead("controller-1", now, in_five_seconds).await?;
///
/// let expected_leader = LeaderRecord {
///     holder: "controller-1".to_owned(),
///     expires_at: in_five_seconds,
///     leading_since: Some(now),
/// };
/// assert_eq!(metadata.get_leader().await?, Some(expected_leader));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderRecord {
    /// The name of the active controller instance.
    pub holder: String,
    /// When its leadership lapses unless renewed.
    pub expires_at: Timestamp,
    /// Since when the holder has led without a lapse. Renewals keep it; taking over, also from one's own lapsed
    /// leadership, sets it anew. `None` only in a record written before this field existed.
    #[serde(default)]
    pub leading_since: Option<Timestamp>,
}

#[cfg(test)]
#[path = "records_test.rs"]
mod tests;
