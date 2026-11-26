//! Tests of what can happen to a storage node.
//!
//! # What is covered
//!
//! - A crashed node is down until it is restarted, and comes back with what it had flushed, or with what the crash mode
//!   left of it.
//! - A node that lost its disk comes back empty, under a new identity, at its old address; what it uploaded to the
//!   object store is still there.
//! - A node reports a refused flush until it is restarted, and takes writes again afterwards.
//! - Cutting a node off leaves the other faults of its link alone.
//! - The two ways to look at a node's copy: where it ends, and what a reader would be served.
//!
//! # How the fixtures work
//!
//! Each test starts from `cluster_with_three_records`: one node whose replica holds records 1 to 3 of `SEGMENT`,
//! flushed. The node is looked at through `Node::local_end`, which bypasses the link, so the tests can tell what the
//! replica holds whatever state the network is in.

use std::time::Duration;

use bitacora_artifact::ArtifactStore as _;
use bitacora_segment::format::SEGMENT_HEADER_BYTES;
use bitacora_types::replica_api::{ReplicaError, SealRequest, UploadRequest};

use super::*;
use crate::Cluster;
use crate::test_support::{SEGMENT, STORED_RECORD_COUNT, append_request, cluster_with_three_records, is_unavailable};

/// The end of the node's copy as the fixture leaves it.
const STORED_END: Option<SequenceNumber> = Some(SequenceNumber::new(STORED_RECORD_COUNT));

/// How many bytes of the only batch survive the crash that tears it: enough to leave a piece of the batch behind, too
/// few to leave a whole one.
const TORN_BATCH_BYTES: usize = 16;

/// The key the upload test stores the segment under.
const OBJECT_KEY: &str = "streams/1/segments/1";

#[tokio::test(start_paused = true)]
async fn started_node_is_running() {
    let cluster = cluster_with_three_records().await;

    assert!(cluster.nodes()[0].is_running());
}

#[tokio::test(start_paused = true)]
async fn crashed_node_is_not_running() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];

    node.crash();

    assert!(!node.is_running());
    assert!(node.replica().is_none());
}

#[tokio::test(start_paused = true)]
async fn crashed_node_is_down_until_restarted() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    node.crash();

    let while_down = node.link().append(append_request(4, 1)).await;
    node.restart().await;
    let after_restart = node.link().append(append_request(4, 1)).await;

    assert!(is_unavailable(&while_down), "{while_down:?}");
    assert!(after_restart.is_ok(), "{after_restart:?}");
}

#[tokio::test(start_paused = true)]
async fn restarted_node_still_holds_what_it_had_flushed() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];

    node.crash();
    node.restart().await;

    assert_eq!(node.local_end(SEGMENT).await, STORED_END);
}

#[tokio::test(start_paused = true)]
async fn crash_can_leave_whatever_the_caller_decides_on_disk() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    // Keep the segment header and the first bytes of the only batch.
    let surviving_bytes = SEGMENT_HEADER_BYTES + TORN_BATCH_BYTES;

    node.crash_with(CrashMode::KeepFirstBytes(surviving_bytes));
    node.restart().await;

    assert_eq!(node.local_end(SEGMENT).await, Some(SequenceNumber::ZERO));
}

#[tokio::test(start_paused = true)]
async fn restart_of_a_running_node_keeps_its_process() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    let process_before = node.replica().expect("the node is running");

    node.restart().await;

    let process_after = node.replica().expect("the node is running");
    assert!(Arc::ptr_eq(&process_before, &process_after));
}

#[tokio::test(start_paused = true)]
async fn node_that_lost_its_disk_is_down_until_restarted() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];

    node.lose_disk();

    assert!(!node.is_running());
}

#[tokio::test(start_paused = true)]
async fn node_that_lost_its_disk_returns_under_a_new_identity() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    let identity_before = node.replica_id();

    node.lose_disk();
    node.restart().await;

    let status = node.link().status().await.expect("a restarted node answers");
    assert_ne!(status.replica_id, identity_before);
    assert_eq!(status.replica_id, node.replica_id());
}

#[tokio::test(start_paused = true)]
async fn node_that_lost_its_disk_returns_empty() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];

    node.lose_disk();
    node.restart().await;

    let status = node.link().status().await.expect("a restarted node answers");
    assert_eq!(status.segment_count, 0);
}

#[tokio::test(start_paused = true)]
async fn node_that_lost_its_disk_keeps_its_address() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    let address_before = node.address().to_owned();

    node.lose_disk();
    node.restart().await;

    assert_eq!(node.address(), address_before);
}

#[tokio::test(start_paused = true)]
async fn every_lost_disk_gives_the_node_another_identity() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];

    node.lose_disk();
    let after_the_first_loss = node.replica_id().to_string();
    node.lose_disk();
    let after_the_second_loss = node.replica_id().to_string();

    assert_eq!(after_the_first_loss, "replica-0-disk-1");
    assert_eq!(after_the_second_loss, "replica-0-disk-2");
}

/// The object store is the cluster's backup: what a node uploaded must still be there after the node's disk is gone.
#[tokio::test(start_paused = true)]
async fn uploaded_segment_outlives_the_disk_of_the_node_that_uploaded_it() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    let sealed_end = SequenceNumber::new(STORED_RECORD_COUNT);
    node.link()
        .seal(SealRequest {
            segment: SEGMENT,
            sealed_end,
        })
        .await
        .expect("a copy that ends at the sealed end is sealed");
    node.link()
        .upload_segment(UploadRequest {
            segment: append_request(1, STORED_RECORD_COUNT).segment,
            sealed_end,
            object_key: OBJECT_KEY.to_owned(),
        })
        .await
        .expect("a sealed copy is uploaded");

    node.lose_disk();
    node.restart().await;

    let stored_objects = cluster
        .artifact_store()
        .list_prefix("streams/1/")
        .await
        .expect("the object store is up");
    assert_eq!(stored_objects.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn append_fails_when_the_disk_refuses_the_flush() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    node.disk().fail_flushes(true);

    let failed = node.link().append(append_request(4, 1)).await;

    assert!(matches!(failed, Err(ReplicaError::Storage { .. })), "{failed:?}");
}

#[tokio::test(start_paused = true)]
async fn node_reports_no_failed_write_while_its_disk_is_healthy() {
    let cluster = cluster_with_three_records().await;

    assert!(!cluster.nodes()[0].has_failed_write());
}

#[tokio::test(start_paused = true)]
async fn node_reports_a_failed_flush_until_it_is_restarted() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    node.disk().fail_flushes(true);
    let _ = node.link().append(append_request(4, 1)).await;

    let reported_before_the_restart = node.has_failed_write();
    node.crash();
    node.restart().await;

    assert!(reported_before_the_restart);
    assert!(!node.has_failed_write());
}

#[tokio::test(start_paused = true)]
async fn node_that_is_down_reports_no_failed_write() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    node.disk().fail_flushes(true);
    let _ = node.link().append(append_request(4, 1)).await;

    node.crash();

    assert!(!node.has_failed_write());
}

#[tokio::test(start_paused = true)]
async fn restarted_node_takes_appends_again_after_a_failed_flush() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    node.disk().fail_flushes(true);
    let _ = node.link().append(append_request(4, 1)).await;

    node.crash();
    node.restart().await;
    let answer = node.link().append(append_request(4, 1)).await;

    assert!(answer.is_ok(), "{answer:?}");
}

#[tokio::test(start_paused = true)]
async fn cutting_a_node_off_keeps_the_other_faults_of_its_link() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    let slow_and_lossy = LinkFault {
        is_cut: false,
        loses_responses: true,
        delay: Duration::from_millis(50),
    };
    node.link().set_fault(slow_and_lossy);

    node.set_reachable(false);

    assert_eq!(
        node.link().fault(),
        LinkFault {
            is_cut: true,
            ..slow_and_lossy
        }
    );
}

#[tokio::test(start_paused = true)]
async fn node_that_is_cut_off_is_still_running() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];

    node.set_reachable(false);

    assert!(node.is_running());
}

#[tokio::test(start_paused = true)]
async fn local_end_is_where_the_copy_ends() {
    let cluster = cluster_with_three_records().await;

    let local_end = cluster.nodes()[0].local_end(SEGMENT).await;

    assert_eq!(local_end, STORED_END);
}

#[tokio::test(start_paused = true)]
async fn local_end_is_unknown_while_the_node_is_down() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];

    node.crash();

    assert_eq!(node.local_end(SEGMENT).await, None);
}

#[tokio::test(start_paused = true)]
async fn local_end_is_unknown_for_a_segment_the_node_does_not_hold() {
    let cluster = Cluster::start(1).await;

    let local_end = cluster.nodes()[0].local_end(SEGMENT).await;

    assert_eq!(local_end, None);
}

#[tokio::test(start_paused = true)]
async fn readable_sequences_follow_the_durable_sequence() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];

    node.link()
        .append(append_request(4, 2))
        .await
        .expect("the next batch continues the copy");

    assert_eq!(node.readable_sequences(SEGMENT).await, [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn nothing_is_readable_while_the_node_is_down() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    node.link()
        .append(append_request(4, 2))
        .await
        .expect("the next batch continues the copy");

    node.crash();

    assert_eq!(node.readable_sequences(SEGMENT).await, Vec::<u64>::new());
}

#[tokio::test(start_paused = true)]
async fn nothing_is_readable_of_a_segment_the_node_does_not_hold() {
    let cluster = Cluster::start(1).await;

    let readable = cluster.nodes()[0].readable_sequences(SEGMENT).await;

    assert_eq!(readable, Vec::<u64>::new());
}
