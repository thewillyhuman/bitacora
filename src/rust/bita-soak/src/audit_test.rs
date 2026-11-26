//! Tests of the audit of a stream.
//!
//! # What is covered
//!
//! - The audit of a stream that was written reads back every record, and the checker finds nothing.
//! - The audit ends the stream with a record of its own, appended by a new writer of the next epoch.
//! - The audit of an empty stream works: the stream then holds the final record only.
//! - A stream that cannot be read back in time is not reported as audited: the audit fails with the reason, and no
//!   half audit is left in the history.
//!
//! # How the fixtures work
//!
//! `written_stream` starts the in-process cluster of `test_support.rs`, creates a stream and commits a few records
//! to it through a writer that records into the stream's history, as the workload does.

use bitacora::StreamOptions;
use bitacora_sim::{Event, check_audit};
use bitacora_types::Epoch;

use std::sync::Arc;

use super::*;
use crate::report::HistoryCounts;
use crate::test_support::InProcessCluster;

/// The stream the tests audit.
const STREAM: &str = "soak-test-1-0";

/// How long the audit is given in tests that expect it to work.
const TIME_LIMIT: Duration = Duration::from_secs(60);

/// The in-process cluster and a stream on it with `record_count` committed records in its history.
async fn written_stream(record_count: u64) -> (InProcessCluster, SoakedStream) {
    let cluster = InProcessCluster::start().await;
    let client = &cluster.soaked.client;
    client
        .admin()
        .create_stream(STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    let stream = SoakedStream::new(STREAM.to_owned());
    let writer = client
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the stream can be written");
    for record_number in 0..record_count {
        let payload = Bytes::from(format!("{STREAM}/{record_number}"));
        let operation = stream.history.append_invoked(payload.clone());
        let sequence = writer.append(payload).await.expect("the record is committed");
        let epoch = writer.epoch();
        stream
            .history
            .append_finished(operation, AppendOutcome::Committed { sequence, epoch });
    }
    drop(writer);
    (cluster, stream)
}

#[tokio::test(start_paused = true)]
async fn audit_of_a_written_stream_finds_every_committed_record() {
    let (cluster, stream) = written_stream(20).await;

    let audit_session = audit_stream(&cluster.soaked.client, &stream, TIME_LIMIT)
        .await
        .expect("the stream is read back");

    assert_eq!(check_audit(&stream.history.events(), audit_session), []);
}

#[tokio::test(start_paused = true)]
async fn audit_reads_the_stream_from_its_first_record_to_its_own() {
    let (cluster, stream) = written_stream(20).await;

    audit_stream(&cluster.soaked.client, &stream, TIME_LIMIT)
        .await
        .expect("the stream is read back");

    let counts = HistoryCounts::of(&stream.history.events());
    assert_eq!(counts.appends_committed, 21);
    assert_eq!(counts.records_read, 21);
}

/// Taking the stream over is what settles the appends whose writer never learned how they ended.
#[tokio::test(start_paused = true)]
async fn final_record_is_appended_by_a_writer_of_the_next_epoch() {
    let (cluster, stream) = written_stream(3).await;

    audit_stream(&cluster.soaked.client, &stream, TIME_LIMIT)
        .await
        .expect("the stream is read back");

    let mut last_committed_epoch = None;
    for timed in stream.history.events() {
        if let Event::AppendFinished {
            outcome: AppendOutcome::Committed { epoch, .. },
            ..
        } = timed.event
        {
            last_committed_epoch = Some(epoch);
        }
    }
    assert_eq!(last_committed_epoch, Some(Epoch::new(2)));
}

#[tokio::test(start_paused = true)]
async fn audit_of_a_stream_nobody_wrote_reads_its_own_record() {
    let (cluster, stream) = written_stream(0).await;

    audit_stream(&cluster.soaked.client, &stream, TIME_LIMIT)
        .await
        .expect("the stream is read back");

    assert_eq!(HistoryCounts::of(&stream.history.events()).records_read, 1);
}

#[tokio::test(start_paused = true)]
async fn stream_that_cannot_be_written_in_time_is_not_audited() {
    let (cluster, stream) = written_stream(3).await;
    cluster.faults.set_metadata_store_unavailable(true);

    let audited = audit_stream(&cluster.soaked.client, &stream, Duration::from_secs(20)).await;

    let reason = audited.expect_err("no writer can take the stream");
    assert!(
        reason.starts_with("no final record could be committed to soak-test-1-0"),
        "{reason}"
    );
}

#[tokio::test(start_paused = true)]
async fn failed_audit_leaves_no_reading_session_in_the_history() {
    let (cluster, stream) = written_stream(3).await;
    cluster.faults.set_metadata_store_unavailable(true);

    let _not_audited = audit_stream(&cluster.soaked.client, &stream, Duration::from_secs(20)).await;

    assert_eq!(HistoryCounts::of(&stream.history.events()).records_read, 0);
}

#[tokio::test(start_paused = true)]
async fn audit_works_once_the_cluster_is_back() {
    let (cluster, stream) = written_stream(3).await;
    cluster.faults.set_metadata_store_unavailable(true);
    let faults = Arc::clone(&cluster.faults);
    tokio::spawn(async move {
        sleep(Duration::from_secs(10)).await;
        faults.set_metadata_store_unavailable(false);
    });

    let audited = audit_stream(&cluster.soaked.client, &stream, TIME_LIMIT).await;

    assert!(audited.is_ok(), "{audited:?}");
}
