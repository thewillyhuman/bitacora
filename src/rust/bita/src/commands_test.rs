//! Tests of running a parsed command and of rendering results.
//!
//! # What is covered
//!
//! - Dispatch: a parsed command reaches the right function, with its arguments, and `stream append`
//!   takes its records from standard input and from its arguments.
//! - Rendering: every kind of result as text, written out in full so that a change of layout is a
//!   visible change of this file; and the shape of the JSON.
//!
//! # How the fixtures work
//!
//! Dispatch tests use the in-process cluster of `testing.rs`. Rendering tests build results by hand:
//! renderers are pure functions and need no cluster.

use bitacora_types::ReplicaId;

use super::*;
use crate::commands::testing::{STREAM, TestCluster};

/// Runs a command that takes nothing from standard input.
async fn run_command(started: &TestCluster, command: Command) -> Result<CommandOutput, CommandError> {
    run(&command, &started.cluster, Vec::new()).await
}

/// A stream summary with every field set to a distinct value.
fn orders_summary() -> stream::StreamSummary {
    stream::StreamSummary {
        name: "orders".to_owned(),
        stream_id: 7,
        epoch: 4,
        writer: Some("compute-1".to_owned()),
        replicas: 3,
        write_quorum: 2,
        segments: 12,
        low_water_mark: 1_500,
    }
}

#[tokio::test(start_paused = true)]
async fn stream_create_command_creates_the_stream() {
    let started = TestCluster::start(3).await;
    let command = Command::Stream(StreamCommand::Create {
        name: STREAM.to_owned(),
        replicas: 3,
        write_quorum: 2,
        keep_sealed_for: None,
    });

    let output = run_command(&started, command).await.expect("runs");

    assert!(matches!(output, CommandOutput::StreamSummary(summary) if summary.name == STREAM));
}

#[tokio::test(start_paused = true)]
async fn stream_list_command_lists_the_streams() {
    let started = TestCluster::with_stream(3).await;

    let output = run_command(&started, Command::Stream(StreamCommand::List))
        .await
        .expect("runs");

    assert!(matches!(output, CommandOutput::StreamList(list) if list.streams.len() == 1));
}

#[tokio::test(start_paused = true)]
async fn stream_delete_command_passes_the_confirmation_on() {
    let started = TestCluster::with_stream(3).await;
    let command = Command::Stream(StreamCommand::Delete {
        name: STREAM.to_owned(),
        yes: false,
    });

    let refused = run_command(&started, command).await;

    assert!(matches!(refused, Err(CommandError::Refused { .. })));
}

#[tokio::test(start_paused = true)]
async fn stream_append_command_appends_standard_input_before_its_arguments() {
    let started = TestCluster::with_stream(3).await;
    let command = Command::Stream(StreamCommand::Append {
        name: STREAM.to_owned(),
        records: vec!["from-argument".to_owned()],
        force: true,
    });

    run(&command, &started.cluster, vec![Bytes::from("from-input")])
        .await
        .expect("runs");

    let found = stream::read(&started.cluster, STREAM, None, 10).await.expect("reads");
    assert_eq!(found.records[0].payload_text, "from-input");
    assert_eq!(found.records[1].payload_text, "from-argument");
}

#[tokio::test(start_paused = true)]
async fn stream_tail_is_not_run_as_a_command_with_a_result() {
    let started = TestCluster::with_stream(3).await;
    let command = Command::Stream(StreamCommand::Tail {
        name: STREAM.to_owned(),
        from: None,
        count: None,
    });

    let refused = run_command(&started, command).await;

    assert!(matches!(refused, Err(CommandError::NothingToDo { .. })));
}

#[tokio::test(start_paused = true)]
async fn replica_list_command_lists_the_registry() {
    let started = TestCluster::start(3).await;

    let output = run_command(&started, Command::Replica(ReplicaCommand::List))
        .await
        .expect("runs");

    assert!(matches!(output, CommandOutput::ReplicaList(list) if list.replicas.len() == 3));
}

#[tokio::test(start_paused = true)]
async fn replica_remove_command_passes_the_force_on() {
    let started = TestCluster::start(3).await;
    let command = Command::Replica(ReplicaCommand::Remove {
        replica_id: ReplicaId::new("replica-0").expect("a valid identifier"),
        force: false,
    });

    let refused = run_command(&started, command).await;

    assert!(matches!(refused, Err(CommandError::Refused { .. })));
}

#[tokio::test(start_paused = true)]
async fn segment_list_command_lists_the_segments() {
    let started = TestCluster::with_stream(3).await;
    let command = Command::Segment(SegmentCommand::List {
        stream: STREAM.to_owned(),
    });

    let output = run_command(&started, command).await.expect("runs");

    assert!(matches!(output, CommandOutput::SegmentList(list) if list.stream == STREAM));
}

#[tokio::test(start_paused = true)]
async fn controller_status_command_reads_the_leadership() {
    let started = TestCluster::start(3).await;

    let output = run_command(&started, Command::Controller(ControllerCommand::Status))
        .await
        .expect("runs");

    assert!(matches!(output, CommandOutput::ControllerStatus(status) if status.leader.is_none()));
}

#[test]
fn acknowledgement_is_one_line() {
    let output = CommandOutput::Acknowledgement(Acknowledgement {
        message: "stream orders deleted".to_owned(),
    });

    assert_eq!(output.render(OutputFormat::Table), "stream orders deleted\n");
}

#[test]
fn json_is_the_result_itself_without_a_wrapper() {
    let output = CommandOutput::Acknowledgement(Acknowledgement {
        message: "stream orders deleted".to_owned(),
    });

    assert_eq!(
        output.render(OutputFormat::Json),
        "{\n  \"message\": \"stream orders deleted\"\n}\n"
    );
}

#[test]
fn json_of_a_stream_uses_the_field_names_of_the_summary() {
    let output = CommandOutput::StreamSummary(orders_summary());

    let rendered: serde_json::Value = serde_json::from_str(&output.render(OutputFormat::Json)).expect("valid JSON");

    assert_eq!(
        rendered,
        serde_json::json!({
            "name": "orders",
            "stream_id": 7,
            "epoch": 4,
            "writer": "compute-1",
            "replicas": 3,
            "write_quorum": 2,
            "segments": 12,
            "low_water_mark": 1500
        })
    );
}

#[test]
fn streams_are_rendered_as_a_table() {
    let mut idle = orders_summary();
    idle.name = "audit".to_owned();
    idle.writer = None;
    let output = CommandOutput::StreamList(stream::StreamList {
        streams: vec![idle, orders_summary()],
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "NAME    ID  EPOCH  WRITER     REPLICATION  SEGMENTS  LOW-WATER\n\
         audit   7   4      -          3/2          12        1500\n\
         orders  7   4      compute-1  3/2          12        1500\n"
    );
}

#[test]
fn stream_details_are_rendered_as_named_values_and_a_table_of_materializers() {
    let output = CommandOutput::StreamDetails(stream::StreamDetails {
        name: "orders".to_owned(),
        stream_id: 7,
        epoch: 4,
        writer: Some(stream::WriterDetails {
            identity: "compute-1".to_owned(),
            lease_expires_at: "2026-11-25T22:03:44Z".to_owned(),
            is_lease_current: true,
        }),
        replicas: 3,
        write_quorum: 2,
        keep_sealed_for_seconds: Some(3_600),
        low_water_mark: 1_500,
        truncate_before: None,
        segments: 12,
        next_segment_id: 40,
        standby_replicas: Some(vec!["replica-a".to_owned(), "replica-b".to_owned()]),
        materializers: vec![stream::MaterializerDetails {
            name: "pages".to_owned(),
            checkpoint: 1_499,
            fencing_token: 2,
            manifest: None,
        }],
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "name:             orders\n\
         id:               7\n\
         epoch:            4\n\
         writer:           compute-1 (lease until 2026-11-25T22:03:44Z)\n\
         replication:      3 replicas, write quorum 2\n\
         retention:        sealed segments kept at least 3600s\n\
         low-water mark:   1500\n\
         truncate before:  -\n\
         segments:         12\n\
         next segment id:  40\n\
         standby replicas: replica-a replica-b\n\
         \n\
         MATERIALIZER  CHECKPOINT  TOKEN  MANIFEST\n\
         pages         1499        2      -\n"
    );
}

#[test]
fn appended_records_are_rendered_as_one_line() {
    let output = CommandOutput::AppendedRecords(stream::AppendedRecords {
        stream: "orders".to_owned(),
        epoch: 5,
        first_sequence: 10,
        last_sequence: 12,
        records: 3,
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "appended 3 records to orders at sequences 10 to 12 under epoch 5\n"
    );
}

#[test]
fn read_records_are_rendered_as_a_table() {
    let output = CommandOutput::ReadRecords(stream::ReadRecords {
        records: vec![stream::RecordLine {
            sequence: 10,
            payload_text: "first".to_owned(),
            payload_hex: "6669727374".to_owned(),
        }],
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "SEQUENCE  PAYLOAD\n10        first\n"
    );
}

#[test]
fn replicas_are_rendered_as_a_table() {
    let output = CommandOutput::ReplicaList(replica::ReplicaList {
        replicas: vec![replica::ReplicaSummary {
            replica_id: "replica-a".to_owned(),
            address: "10.0.0.1:7000".to_owned(),
            is_alive: true,
            last_heartbeat_at: "2026-11-25T22:03:44Z".to_owned(),
            heartbeat_age_seconds: 3,
            used_percent: 41,
            capacity_bytes: 2 * 1_024 * 1_024 * 1_024 * 1_024,
            free_bytes: 1_024 * 1_024 * 1_024 * 1_024,
            is_draining: false,
            failure_domains: vec!["zone-a".to_owned(), "rack-3".to_owned()],
        }],
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "REPLICA    ADDRESS        ALIVE  HEARTBEAT  USED  CAPACITY  DRAINING  DOMAINS\n\
         replica-a  10.0.0.1:7000  yes    3s ago     41%   2.0 TiB   no        zone-a/rack-3\n"
    );
}

#[test]
fn replica_report_is_rendered_as_named_values() {
    let output = CommandOutput::ReplicaReport(replica::ReplicaReport {
        replica_id: "replica-a".to_owned(),
        address: "10.0.0.1:7000".to_owned(),
        capacity_bytes: 2_048,
        free_bytes: 1_024,
        is_draining: true,
        segment_count: 17,
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "replica:        replica-a\n\
         address:        10.0.0.1:7000\n\
         capacity:       2.0 KiB\n\
         free:           1.0 KiB\n\
         draining:       yes\n\
         segment copies: 17\n"
    );
}

#[test]
fn finished_drain_is_rendered_as_one_line() {
    let output = CommandOutput::DrainReport(replica::DrainReport {
        replica_id: "replica-a".to_owned(),
        is_drained: true,
        open_segments: Vec::new(),
        standby_streams: Vec::new(),
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "replica replica-a is draining and no open segment names it: it can be stopped\n"
    );
}

#[test]
fn unfinished_drain_names_what_still_uses_the_replica() {
    let output = CommandOutput::DrainReport(replica::DrainReport {
        replica_id: "replica-a".to_owned(),
        is_drained: false,
        open_segments: vec![replica::OpenSegmentReference {
            stream: "orders".to_owned(),
            segment_id: 39,
        }],
        standby_streams: vec!["audit".to_owned()],
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "replica replica-a is draining; writers roll away from it as they append\n\
         \n\
         STREAM  OPEN SEGMENT\n\
         orders  39\n\
         \n\
         streams whose standby set still names it: audit\n"
    );
}

#[test]
fn segments_are_rendered_as_a_table() {
    let output = CommandOutput::SegmentList(segment::SegmentList {
        stream: "orders".to_owned(),
        low_water_mark: 20,
        segments: vec![
            segment::SegmentRow {
                segment_id: 2,
                epoch: 4,
                first_sequence: 10,
                sealed_end: Some(19),
                state: "released".to_owned(),
                awaits_deletion: true,
                copies: Vec::new(),
                object_key: Some("streams/7/segments/00000000000000000002".to_owned()),
                object_size_bytes: Some(2_048),
            },
            segment::SegmentRow {
                segment_id: 3,
                epoch: 5,
                first_sequence: 20,
                sealed_end: None,
                state: "open".to_owned(),
                awaits_deletion: false,
                copies: vec![segment::CopyCell {
                    replica_id: "replica-a".to_owned(),
                    state: "unverified".to_owned(),
                }],
                object_key: None,
                object_size_bytes: None,
            },
        ],
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "SEGMENT  EPOCH  FIRST  END  STATE                       COPIES                OBJECT\n\
         2        4      10     19   released (awaits deletion)                        \
         streams/7/segments/00000000000000000002 (2.0 KiB)\n\
         3        5      20     -    open                        replica-a=unverified  -\n"
    );
}

#[test]
fn cluster_without_a_leader_is_rendered_as_one_line() {
    let output = CommandOutput::ControllerStatus(controller::ControllerStatus { leader: None });

    assert_eq!(
        output.render(OutputFormat::Table),
        "no controller has ever led this cluster\n"
    );
}

#[test]
fn leader_is_rendered_as_named_values() {
    let output = CommandOutput::ControllerStatus(controller::ControllerStatus {
        leader: Some(controller::LeaderDetails {
            holder: "controller-1".to_owned(),
            is_current: true,
            leading_since: Some("2026-11-25T20:00:00Z".to_owned()),
            leading_for_seconds: Some(7_424),
            expires_at: "2026-11-25T22:03:54Z".to_owned(),
        }),
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "leader:      controller-1\n\
         state:       leading\n\
         since:       2026-11-25T20:00:00Z (2h)\n\
         valid until: 2026-11-25T22:03:54Z\n"
    );
}

#[test]
fn lapsed_leadership_is_rendered_as_lapsed() {
    let output = CommandOutput::ControllerStatus(controller::ControllerStatus {
        leader: Some(controller::LeaderDetails {
            holder: "controller-1".to_owned(),
            is_current: false,
            leading_since: Some("2026-11-25T20:00:00Z".to_owned()),
            leading_for_seconds: None,
            expires_at: "2026-11-25T22:03:54Z".to_owned(),
        }),
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "leader:      controller-1\n\
         state:       lapsed: no controller leads now\n\
         since:       2026-11-25T20:00:00Z\n\
         valid until: 2026-11-25T22:03:54Z\n"
    );
}

#[tokio::test(start_paused = true)]
async fn metadata_snapshot_list_command_lists_the_snapshots() {
    let started = TestCluster::with_stream(3).await;
    started.run_controller_pass().await;
    let command = Command::Metadata(MetadataCommand::Snapshot(SnapshotCommand::List));

    let output = run_command(&started, command).await.expect("runs");

    assert!(matches!(output, CommandOutput::SnapshotList(list) if list.snapshots.len() == 1));
}

#[tokio::test(start_paused = true)]
async fn metadata_restore_command_passes_the_dry_run_on() {
    let started = TestCluster::with_stream(3).await;
    started.run_controller_pass().await;
    let command = Command::Metadata(MetadataCommand::Restore {
        snapshot: "latest".to_owned(),
        force: false,
        dry_run: true,
    });

    let output = run_command(&started, command).await.expect("runs");

    assert!(matches!(output, CommandOutput::RestoreReport(report) if report.is_dry_run));
}

#[tokio::test(start_paused = true)]
async fn metadata_rebuild_command_plans_without_applying_by_default() {
    let started = TestCluster::with_stream(3).await;
    let command = Command::Metadata(MetadataCommand::Rebuild {
        names: Vec::new(),
        replicas: Vec::new(),
        apply: false,
        force: false,
    });

    let output = run_command(&started, command).await.expect("runs");

    assert!(matches!(output, CommandOutput::RebuildReport(report) if !report.is_applied));
}

#[test]
fn snapshots_are_rendered_as_a_table() {
    let output = CommandOutput::SnapshotList(metadata::SnapshotList {
        snapshots: vec![metadata::SnapshotRow {
            object_key: "metadata-snapshots/00000001798761599000".to_owned(),
            taken_at: "2026-12-31T23:59:59Z".to_owned(),
            age_seconds: 300,
            size_bytes: 1_300,
        }],
    });

    assert_eq!(
        output.render(OutputFormat::Table),
        "SNAPSHOT                                 TAKEN AT              AGE  SIZE\n\
         metadata-snapshots/00000001798761599000  2026-12-31T23:59:59Z  5m   1.2 KiB\n"
    );
}

/// A restore report with every count set to a distinct value.
fn restore_report(is_dry_run: bool, keys_in_store_before: usize) -> metadata::RestoreReport {
    metadata::RestoreReport {
        snapshot: "metadata-snapshots/00000001798761599000".to_owned(),
        taken_at: "2026-12-31T23:59:59Z".to_owned(),
        is_dry_run,
        keys_in_store_before,
        keys_written: 12,
        keys_deleted: 2,
    }
}

#[test]
fn restore_is_rendered_with_what_to_do_next() {
    let output = CommandOutput::RestoreReport(restore_report(false, 0));

    assert_eq!(
        output.render(OutputFormat::Table),
        "snapshot:                 metadata-snapshots/00000001798761599000\n\
         taken at:                 2026-12-31T23:59:59Z\n\
         outcome:                  restored\n\
         keys in the store before: 0\n\
         keys written:             12\n\
         keys deleted:             2\n\
         next: run `bita metadata rebuild` to pick up what changed after the snapshot\n"
    );
}

#[test]
fn dry_run_of_a_restore_into_a_used_store_says_that_it_needs_force() {
    let output = CommandOutput::RestoreReport(restore_report(true, 5));

    assert_eq!(
        output.render(OutputFormat::Table),
        "snapshot:                 metadata-snapshots/00000001798761599000\n\
         taken at:                 2026-12-31T23:59:59Z\n\
         outcome:                  dry run: nothing was written\n\
         keys in the store before: 5\n\
         keys written:             12\n\
         keys deleted:             2\n\
         the metadata store is not empty: the restore needs --force and replaces it\n"
    );
}

/// A rebuild report of one stream with one segment, one assumption and the given problems.
fn rebuild_report(is_applied: bool, problems: Vec<String>) -> rebuild::RebuildReport {
    rebuild::RebuildReport {
        is_applied,
        replicas: vec![rebuild::AskedReplica {
            replica_id: "replica-a".to_owned(),
            address: "10.0.0.1:7000".to_owned(),
            has_answered: true,
            copies: 3,
        }],
        tiered_objects: 1,
        streams: vec![rebuild::RebuiltStream {
            stream_id: 7,
            name: Some("orders".to_owned()),
            epoch: Some(12),
            low_water_mark: Some(1),
            next_segment_id: Some(3),
            segments: vec![segment::SegmentRow {
                segment_id: 2,
                epoch: 4,
                first_sequence: 1,
                sealed_end: Some(9),
                state: "sealed".to_owned(),
                awaits_deletion: false,
                copies: vec![segment::CopyCell {
                    replica_id: "replica-a".to_owned(),
                    state: "unverified".to_owned(),
                }],
                object_key: None,
                object_size_bytes: None,
            }],
            assumptions: vec!["replication is assumed".to_owned()],
            problems,
            is_written: is_applied,
        }],
        problems: Vec::new(),
    }
}

#[test]
fn rebuild_plan_without_problems_says_how_to_apply_it() {
    let output = CommandOutput::RebuildReport(rebuild_report(false, Vec::new()));

    assert_eq!(
        output.render(OutputFormat::Table),
        "REPLICA    ADDRESS        ANSWERED  COPIES\n\
         replica-a  10.0.0.1:7000  yes       3\n\
         tiered segment objects found: 1\n\
         \n\
         stream 7 \"orders\": not written\n\
         epoch 12, low-water mark 1, next segment id 3\n\
         SEGMENT  EPOCH  FIRST  END  STATE   COPIES                OBJECT\n\
         2        4      1      9    sealed  replica-a=unverified  -\n\
         assumptions:\n\
         \x20 - replication is assumed\n\
         \n\
         dry run: nothing was written. Pass --apply to write this plan.\n"
    );
}

#[test]
fn rebuild_plan_with_problems_lists_them_and_asks_for_force() {
    let output = CommandOutput::RebuildReport(rebuild_report(false, vec!["a segment is lost".to_owned()]));

    let rendered = output.render(OutputFormat::Table);

    assert!(rendered.contains("problems:\n  - a segment is lost\n"), "{rendered}");
    assert!(
        rendered.ends_with(
            "dry run: nothing was written. The plan has problems; fix them, or pass --apply \
             --force to write what could be rebuilt.\n"
        ),
        "{rendered}"
    );
}

#[test]
fn applied_rebuild_says_what_happens_next() {
    let output = CommandOutput::RebuildReport(rebuild_report(true, Vec::new()));

    let rendered = output.render(OutputFormat::Table);

    assert!(rendered.contains("stream 7 \"orders\": written\n"), "{rendered}");
    assert!(
        rendered.ends_with("applied. Start the controllers: they verify every copy and rebuild missing ones.\n"),
        "{rendered}"
    );
}
