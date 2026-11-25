//! Tests of the `bita stream` commands, against an in-process cluster.
//!
//! # What is covered
//!
//! - Administration: creating with and without valid settings, listing, describing, deleting with and
//!   without confirmation, truncation requests, materializer registration.
//! - Smoke tests: appending, reading with every combination of start and count, and following.
//!
//! # How the fixtures work
//!
//! See `testing.rs`. `TestCluster::append_numbered` appends records called `record-<n>` through the tool
//! itself, after a controller pass has given the stream a segment to roll into.

use std::sync::Arc;

use bitacora::ReadError;
use bitacora_meta::MetaError;

use super::*;
use crate::commands::testing::{STREAM, TestCluster};

/// Runs `tail` to its end and returns what it wrote, as text.
async fn tail_output(
    started: &TestCluster,
    from: Option<u64>,
    count: u64,
    format: OutputFormat,
) -> String {
    let mut written = Vec::new();
    tail(
        &started.cluster,
        STREAM,
        from,
        Some(count),
        format,
        &mut written,
    )
    .await
    .expect("following succeeds");
    String::from_utf8(written).expect("the tool writes text")
}

#[tokio::test(start_paused = true)]
async fn created_stream_reports_its_settings() {
    let started = TestCluster::start(3).await;

    let created = create(&started.cluster, STREAM, 3, 2, None)
        .await
        .expect("the stream is created");

    assert_eq!(
        created,
        StreamSummary {
            name: STREAM.to_owned(),
            stream_id: 1,
            epoch: 0,
            writer: None,
            replicas: 3,
            write_quorum: 2,
            segments: 0,
            low_water_mark: 1,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn write_quorum_above_the_replica_count_is_refused() {
    let started = TestCluster::start(3).await;

    let refused = create(&started.cluster, STREAM, 2, 3, None).await;

    assert!(matches!(refused, Err(CommandError::InvalidReplication(_))));
}

#[tokio::test(start_paused = true)]
async fn name_that_is_taken_is_refused() {
    let started = TestCluster::with_stream(3).await;

    let refused = create(&started.cluster, STREAM, 3, 2, None).await;

    assert_eq!(
        refused,
        Err(CommandError::Metadata(MetaError::StreamAlreadyExists {
            name: STREAM.to_owned()
        }))
    );
}

#[tokio::test(start_paused = true)]
async fn streams_are_listed_sorted_by_name() {
    let started = TestCluster::start(3).await;
    for name in ["payments", "audit", "orders"] {
        create(&started.cluster, name, 3, 2, None)
            .await
            .expect("the stream is created");
    }

    let listed = list(&started.cluster).await.expect("lists");

    let mut names = Vec::new();
    for stream in &listed.streams {
        names.push(stream.name.clone());
    }
    assert_eq!(names, ["audit", "orders", "payments"]);
}

#[tokio::test(start_paused = true)]
async fn cluster_without_streams_lists_none() {
    let started = TestCluster::start(3).await;

    let listed = list(&started.cluster).await.expect("lists");

    assert_eq!(listed.streams, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn description_shows_the_retention_that_was_asked_for() {
    let started = TestCluster::start(3).await;
    create(
        &started.cluster,
        STREAM,
        3,
        2,
        Some(Duration::from_hours(1)),
    )
    .await
    .expect("the stream is created");

    let described = describe(&started.cluster, STREAM).await.expect("describes");

    assert_eq!(described.keep_sealed_for_seconds, Some(3_600));
}

#[tokio::test(start_paused = true)]
async fn description_shows_the_replicas_set_aside_for_the_first_segment() {
    let started = TestCluster::with_stream(3).await;

    let described = describe(&started.cluster, STREAM).await.expect("describes");

    assert_eq!(
        described.standby_replicas,
        Some(vec![
            "replica-0".to_owned(),
            "replica-1".to_owned(),
            "replica-2".to_owned()
        ])
    );
}

#[tokio::test(start_paused = true)]
async fn describing_a_stream_that_does_not_exist_fails() {
    let started = TestCluster::start(3).await;

    let failed = describe(&started.cluster, "no-such-stream").await;

    assert_eq!(
        failed,
        Err(CommandError::Metadata(MetaError::StreamNotFound {
            name: "no-such-stream".to_owned()
        }))
    );
}

#[tokio::test(start_paused = true)]
async fn deletion_without_confirmation_is_refused() {
    let started = TestCluster::with_stream(3).await;

    let refused = delete(&started.cluster, STREAM, false).await;

    assert!(matches!(refused, Err(CommandError::Refused { .. })));
}

#[tokio::test(start_paused = true)]
async fn refused_deletion_keeps_the_stream() {
    let started = TestCluster::with_stream(3).await;

    let _refused = delete(&started.cluster, STREAM, false).await;

    assert!(describe(&started.cluster, STREAM).await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn confirmed_deletion_removes_the_stream() {
    let started = TestCluster::with_stream(3).await;

    delete(&started.cluster, STREAM, true)
        .await
        .expect("the stream is deleted");

    let listed = list(&started.cluster).await.expect("lists");
    assert_eq!(listed.streams, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn truncation_request_is_recorded() {
    let started = TestCluster::with_stream(3).await;

    truncate_before(&started.cluster, STREAM, 1_500)
        .await
        .expect("the request is recorded");

    let described = describe(&started.cluster, STREAM).await.expect("describes");
    assert_eq!(described.truncate_before, Some(1_500));
}

#[tokio::test(start_paused = true)]
async fn registered_materializer_starts_before_the_first_record() {
    let started = TestCluster::with_stream(3).await;

    let registered = register_materializer(&started.cluster, STREAM, "pages")
        .await
        .expect("the materializer is registered");

    assert_eq!(registered.name, "pages");
    assert_eq!(registered.checkpoint, 0);
}

#[tokio::test(start_paused = true)]
async fn registered_materializer_is_part_of_the_description() {
    let started = TestCluster::with_stream(3).await;
    register_materializer(&started.cluster, STREAM, "pages")
        .await
        .expect("the materializer is registered");

    let described = describe(&started.cluster, STREAM).await.expect("describes");

    assert_eq!(described.materializers.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn unregistered_materializer_leaves_the_description() {
    let started = TestCluster::with_stream(3).await;
    register_materializer(&started.cluster, STREAM, "pages")
        .await
        .expect("the materializer is registered");

    unregister_materializer(&started.cluster, STREAM, "pages")
        .await
        .expect("the materializer is unregistered");

    let described = describe(&started.cluster, STREAM).await.expect("describes");
    assert_eq!(described.materializers, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn append_commits_the_records_at_consecutive_sequences() {
    let started = TestCluster::with_stream(3).await;
    let payloads = vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")];

    let appended = append(&started.cluster, STREAM, payloads, TakeoverPolicy::Force)
        .await
        .expect("the records commit");

    assert_eq!(
        appended,
        AppendedRecords {
            stream: STREAM.to_owned(),
            epoch: 1,
            first_sequence: 1,
            last_sequence: 3,
            records: 3,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn append_without_records_does_nothing() {
    let started = TestCluster::with_stream(3).await;

    let refused = append(&started.cluster, STREAM, Vec::new(), TakeoverPolicy::Force).await;

    assert!(matches!(refused, Err(CommandError::NothingToDo { .. })));
}

#[tokio::test(start_paused = true)]
async fn append_steps_down_when_it_is_done() {
    let started = TestCluster::with_stream(3).await;

    started.append_numbered(1, 2).await;

    let described = describe(&started.cluster, STREAM).await.expect("describes");
    assert_eq!(described.writer, None);
}

#[tokio::test(start_paused = true)]
async fn second_append_continues_where_the_first_one_stopped() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 3).await;
    started.run_controller_pass().await;

    let appended = append(
        &started.cluster,
        STREAM,
        vec![Bytes::from("d")],
        TakeoverPolicy::Force,
    )
    .await
    .expect("the record commits");

    assert_eq!(appended.first_sequence, 4);
}

#[tokio::test(start_paused = true)]
async fn append_to_a_stream_that_does_not_exist_fails() {
    let started = TestCluster::start(3).await;

    let failed = append(
        &started.cluster,
        "no-such-stream",
        vec![Bytes::from("a")],
        TakeoverPolicy::Force,
    )
    .await;

    assert!(matches!(failed, Err(CommandError::Open(_))));
}

#[tokio::test(start_paused = true)]
async fn read_returns_each_record_as_text_and_as_hexadecimal() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 1).await;

    let found = read(&started.cluster, STREAM, None, 100)
        .await
        .expect("reads");

    assert_eq!(
        found.records,
        [RecordLine {
            sequence: 1,
            payload_text: "record-1".to_owned(),
            payload_hex: "7265636f72642d31".to_owned(),
        }]
    );
}

#[tokio::test(start_paused = true)]
async fn read_starts_at_the_sequence_it_is_given() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 5).await;

    let found = read(&started.cluster, STREAM, Some(4), 100)
        .await
        .expect("reads");

    assert_eq!(found.records.len(), 2);
    assert_eq!(found.records[0].sequence, 4);
}

#[tokio::test(start_paused = true)]
async fn read_stops_after_its_count() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 5).await;

    let found = read(&started.cluster, STREAM, None, 2)
        .await
        .expect("reads");

    assert_eq!(found.records.len(), 2);
    assert_eq!(found.records[1].sequence, 2);
}

#[tokio::test(start_paused = true)]
async fn read_of_zero_records_reads_nothing() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 5).await;

    let found = read(&started.cluster, STREAM, None, 0)
        .await
        .expect("reads");

    assert_eq!(found.records, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn read_of_an_empty_stream_finds_nothing() {
    let started = TestCluster::with_stream(3).await;

    let found = read(&started.cluster, STREAM, None, 100)
        .await
        .expect("reads");

    assert_eq!(found.records, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn read_of_a_stream_that_does_not_exist_fails() {
    let started = TestCluster::start(3).await;

    let failed = read(&started.cluster, "no-such-stream", None, 100).await;

    assert!(matches!(
        failed,
        Err(CommandError::Read(ReadError::Metadata(_)))
    ));
}

#[tokio::test(start_paused = true)]
async fn tail_writes_one_line_per_record() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 3).await;

    let written = tail_output(&started, None, 3, OutputFormat::Table).await;

    assert_eq!(written, "1  record-1\n2  record-2\n3  record-3\n");
}

#[tokio::test(start_paused = true)]
async fn tail_starts_at_the_sequence_it_is_given() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 3).await;

    let written = tail_output(&started, Some(3), 1, OutputFormat::Table).await;

    assert_eq!(written, "3  record-3\n");
}

#[tokio::test(start_paused = true)]
async fn tail_as_json_writes_one_object_per_line() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 1).await;

    let written = tail_output(&started, None, 1, OutputFormat::Json).await;

    assert_eq!(
        written,
        "{\"sequence\":1,\"payload_text\":\"record-1\",\"payload_hex\":\"7265636f72642d31\"}\n"
    );
}

#[tokio::test(start_paused = true)]
async fn tail_prints_records_that_commit_while_it_follows() {
    let started = Arc::new(TestCluster::with_stream(3).await);
    let following = {
        let started = Arc::clone(&started);
        tokio::spawn(async move { tail_output(&started, None, 2, OutputFormat::Table).await })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;

    started.append_numbered(1, 2).await;

    let written = following.await.expect("the follower does not panic");
    assert_eq!(written, "1  record-1\n2  record-2\n");
}

#[tokio::test(start_paused = true)]
async fn tail_of_a_stream_that_does_not_exist_fails() {
    let started = TestCluster::start(3).await;
    let mut written = Vec::new();

    let failed = tail(
        &started.cluster,
        "no-such-stream",
        None,
        Some(1),
        OutputFormat::Table,
        &mut written,
    )
    .await;

    assert!(matches!(failed, Err(CommandError::Read(_))));
}
