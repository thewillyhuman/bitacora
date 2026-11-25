//! The metadata records, held to the JSON written by schema version 1.
//!
//! # What is covered
//!
//! For every record the metadata store holds: today's code writes exactly the JSON below, and reads that
//! JSON back to the value it was made from. The records are an interface between programs that are
//! upgraded one at a time and outlive every one of them, so a field renamed by accident must fail a test.
//! A deliberate change has to update these texts, the schema version and `docs/protocol/metadata.md`
//! together.
//!
//! # How the fixtures work
//!
//! Each golden text is the compact JSON that schema version 1 wrote for the record built next to it. The
//! stream is the one used as the example in the protocol document: a sealed segment, an open one, a
//! standby and one materializer.

use std::collections::BTreeSet;

use bitacora_meta::{
    CheckpointRecord, CopyRecord, CopyState, DeletedStreamRecord, LeaderRecord,
    RECORD_SCHEMA_VERSION, ReplicaRecord, RetentionPolicy, SealRecord, SegmentRecord, StreamRecord,
    TieredRecord, WriterLease,
};
use bitacora_types::{
    Epoch, ReplicaId, ReplicationSettings, SegmentId, SequenceNumber, StreamId, Timestamp,
};

/// The stream record of schema version 1.
const GOLDEN_STREAM: &str = concat!(
    r#"{"schema_version":1,"stream_id":7,"name":"orders","#,
    r#""replication":{"replica_count":3,"write_quorum":2},"#,
    r#""retention":{"keep_sealed_for_millis":null},"#,
    r#""epoch":12,"writer":{"writer_identity":"compute-2","expires_at":1795000000000},"#,
    r#""segments":[{"segment_id":41,"epoch":11,"first_sequence":9001,"#,
    r#""sealed":{"end":9500,"at":1794999990000},"#,
    r#""copies":[{"replica_id":"replica-a","state":"good"},"#,
    r#"{"replica_id":"replica-b","state":"bad"},"#,
    r#"{"replica_id":"replica-c","state":"unverified"}],"#,
    r#""tiered":{"object_key":"streams/7/segments/00000000000000000041","size_bytes":4096,"#,
    r#""uploaded_at":1794999995000,"are_local_copies_dropped":false}},"#,
    r#"{"segment_id":42,"epoch":12,"first_sequence":9501,"sealed":null,"#,
    r#""copies":[{"replica_id":"replica-a","state":"unverified"}],"tiered":null}],"#,
    r#""next_segment_id":43,"standby_replicas":["replica-b","replica-c"],"#,
    r#""truncate_before":8000,"low_water_mark":9001,"materializers":["pages"]}"#,
);

/// The checkpoint record of schema version 1.
const GOLDEN_CHECKPOINT: &str = concat!(
    r#"{"schema_version":1,"materializer":"pages","checkpoint":9000,"#,
    r#""fencing_token":4,"manifest":"manifests/9000"}"#,
);

/// The replica record of schema version 1.
const GOLDEN_REPLICA: &str = concat!(
    r#"{"schema_version":1,"replica_id":"replica-a","address":"10.0.0.11:7000","#,
    r#""failure_domains":["zone-1","rack-4","host-11"],"capacity_bytes":960000000000,"#,
    r#""free_bytes":412000000000,"is_draining":false,"heartbeat_at":1795000000000}"#,
);

/// The leader record of schema version 1.
const GOLDEN_LEADER: &str = concat!(
    r#"{"holder":"controller-1","expires_at":1795000010000,"#,
    r#""leading_since":1794999000000}"#,
);

/// A leader record as it was written before `leading_since` existed. Controllers are upgraded one at a
/// time, so a new one must read what an old one wrote.
const GOLDEN_LEADER_WITHOUT_ITS_START: &str =
    r#"{"holder":"controller-1","expires_at":1795000010000}"#;

/// The deleted-stream record of schema version 1.
const GOLDEN_DELETED_STREAM: &str = r#"{"stream_id":7,"name":"orders"}"#;

/// The replica identifier `name`.
fn replica(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// A copy on replica `name` in `state`.
fn copy(name: &str, state: CopyState) -> CopyRecord {
    CopyRecord {
        replica_id: replica(name),
        state,
    }
}

/// The stream the golden text describes.
fn stream() -> StreamRecord {
    let sealed_segment = SegmentRecord {
        segment_id: SegmentId::new(41),
        epoch: Epoch::new(11),
        first_sequence: SequenceNumber::new(9001),
        sealed: Some(SealRecord {
            end: SequenceNumber::new(9500),
            at: Timestamp::from_unix_millis(1_794_999_990_000),
        }),
        copies: vec![
            copy("replica-a", CopyState::Good),
            copy("replica-b", CopyState::Bad),
            copy("replica-c", CopyState::Unverified),
        ],
        tiered: Some(TieredRecord {
            object_key: "streams/7/segments/00000000000000000041".to_owned(),
            size_bytes: 4096,
            uploaded_at: Timestamp::from_unix_millis(1_794_999_995_000),
            are_local_copies_dropped: false,
        }),
    };
    let open_segment = SegmentRecord {
        segment_id: SegmentId::new(42),
        epoch: Epoch::new(12),
        first_sequence: SequenceNumber::new(9501),
        sealed: None,
        copies: vec![copy("replica-a", CopyState::Unverified)],
        tiered: None,
    };
    StreamRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        stream_id: StreamId::new(7),
        name: "orders".to_owned(),
        replication: ReplicationSettings::DEFAULT,
        retention: RetentionPolicy::default(),
        epoch: Epoch::new(12),
        writer: Some(WriterLease {
            writer_identity: "compute-2".to_owned(),
            expires_at: Timestamp::from_unix_millis(1_795_000_000_000),
        }),
        segments: vec![sealed_segment, open_segment],
        next_segment_id: SegmentId::new(43),
        standby_replicas: Some(vec![replica("replica-b"), replica("replica-c")]),
        truncate_before: Some(SequenceNumber::new(8000)),
        low_water_mark: SequenceNumber::new(9001),
        materializers: BTreeSet::from(["pages".to_owned()]),
    }
}

/// The checkpoint the golden text describes.
fn checkpoint() -> CheckpointRecord {
    CheckpointRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        materializer: "pages".to_owned(),
        checkpoint: SequenceNumber::new(9000),
        fencing_token: 4,
        manifest: Some("manifests/9000".to_owned()),
    }
}

/// The replica registration the golden text describes.
fn replica_registration() -> ReplicaRecord {
    ReplicaRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        replica_id: replica("replica-a"),
        address: "10.0.0.11:7000".to_owned(),
        failure_domains: vec![
            "zone-1".to_owned(),
            "rack-4".to_owned(),
            "host-11".to_owned(),
        ],
        capacity_bytes: 960_000_000_000,
        free_bytes: 412_000_000_000,
        is_draining: false,
        heartbeat_at: Timestamp::from_unix_millis(1_795_000_000_000),
    }
}

/// The leadership the golden text describes.
fn leader() -> LeaderRecord {
    LeaderRecord {
        holder: "controller-1".to_owned(),
        expires_at: Timestamp::from_unix_millis(1_795_000_010_000),
        leading_since: Some(Timestamp::from_unix_millis(1_794_999_000_000)),
    }
}

/// The deletion note the golden text describes.
fn deleted_stream() -> DeletedStreamRecord {
    DeletedStreamRecord {
        stream_id: StreamId::new(7),
        name: "orders".to_owned(),
    }
}

#[test]
fn stream_record_is_written_as_in_schema_version_1() {
    let written = serde_json::to_string(&stream()).expect("records serialize");

    assert_eq!(written, GOLDEN_STREAM);
}

#[test]
fn stream_record_of_schema_version_1_is_read() {
    let read: StreamRecord = serde_json::from_str(GOLDEN_STREAM).expect("the golden text is valid");

    assert_eq!(read, stream());
}

#[test]
fn checkpoint_record_is_written_as_in_schema_version_1() {
    let written = serde_json::to_string(&checkpoint()).expect("records serialize");

    assert_eq!(written, GOLDEN_CHECKPOINT);
}

#[test]
fn checkpoint_record_of_schema_version_1_is_read() {
    let read: CheckpointRecord =
        serde_json::from_str(GOLDEN_CHECKPOINT).expect("the golden text is valid");

    assert_eq!(read, checkpoint());
}

#[test]
fn replica_record_is_written_as_in_schema_version_1() {
    let written = serde_json::to_string(&replica_registration()).expect("records serialize");

    assert_eq!(written, GOLDEN_REPLICA);
}

#[test]
fn replica_record_of_schema_version_1_is_read() {
    let read: ReplicaRecord =
        serde_json::from_str(GOLDEN_REPLICA).expect("the golden text is valid");

    assert_eq!(read, replica_registration());
}

#[test]
fn leader_record_is_written_as_in_schema_version_1() {
    let written = serde_json::to_string(&leader()).expect("records serialize");

    assert_eq!(written, GOLDEN_LEADER);
}

#[test]
fn leader_record_of_schema_version_1_is_read() {
    let read: LeaderRecord = serde_json::from_str(GOLDEN_LEADER).expect("the golden text is valid");

    assert_eq!(read, leader());
}

#[test]
fn leader_record_written_before_its_start_was_recorded_is_read() {
    let read: LeaderRecord =
        serde_json::from_str(GOLDEN_LEADER_WITHOUT_ITS_START).expect("the golden text is valid");

    assert_eq!(
        read,
        LeaderRecord {
            leading_since: None,
            ..leader()
        }
    );
}

#[test]
fn deleted_stream_record_is_written_as_in_schema_version_1() {
    let written = serde_json::to_string(&deleted_stream()).expect("records serialize");

    assert_eq!(written, GOLDEN_DELETED_STREAM);
}

#[test]
fn deleted_stream_record_of_schema_version_1_is_read() {
    let read: DeletedStreamRecord =
        serde_json::from_str(GOLDEN_DELETED_STREAM).expect("the golden text is valid");

    assert_eq!(read, deleted_stream());
}
