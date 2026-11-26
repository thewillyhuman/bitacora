//! Tests of the faults of a link.
//!
//! # What is covered
//!
//! - A healthy link carries requests; a cut link fails them before they reach the replica; a restored link carries them
//!   again.
//! - A link that loses answers applies the request and fails the caller.
//! - A slow link holds a request for its delay, and a request that is still on its way when the link is cut or the node
//!   goes down never arrives.
//! - A link with no process behind it fails requests.
//! - Every operation of the replica interface goes through the link's faults.
//!
//! # How the fixtures work
//!
//! Each test starts from `cluster_with_three_records`: one node whose replica holds records 1 to 3 of `SEGMENT`. A test
//! sends the append of record 4 through the link and then asks the node, bypassing the link, where its copy ends: 3
//! means the request never reached the replica, 4 means it was applied. Time is paused, so a delay takes exactly as
//! long as it says.

use std::time::Duration;

use bitacora_types::SequenceNumber;
use tokio::time::Instant;

use super::*;
use crate::Cluster;
use crate::test_support::{
    EVERY_OPERATION, SEGMENT, append_request, cluster_with_three_records, is_unavailable,
    operations_failed_as_unavailable,
};

/// The delay of the slow link in these tests.
const LINK_DELAY: Duration = Duration::from_millis(300);

/// A moment while a request sent at the start of a test is held by [`LINK_DELAY`].
const WHILE_IN_FLIGHT: Duration = Duration::from_millis(100);

/// The end of the node's copy when the append of record 4 never reached it.
const END_BEFORE_THE_APPEND: Option<SequenceNumber> = Some(SequenceNumber::new(3));

/// The end of the node's copy when the append of record 4 was applied.
const END_AFTER_THE_APPEND: Option<SequenceNumber> = Some(SequenceNumber::new(4));

/// A link that only loses answers.
const LOSES_RESPONSES: LinkFault = LinkFault {
    is_cut: false,
    loses_responses: true,
    delay: Duration::ZERO,
};

/// A link that is only slow, by [`LINK_DELAY`].
const SLOW: LinkFault = LinkFault {
    is_cut: false,
    loses_responses: false,
    delay: LINK_DELAY,
};

/// Sends the append of record 4 through the link of the only node of `cluster`.
async fn append_record_4(cluster: &Cluster) -> Result<AppendResponse, ReplicaError> {
    cluster.nodes()[0].link().append(append_request(4, 1)).await
}

/// Reports where the copy of the only node of `cluster` ends, bypassing the link.
async fn local_end(cluster: &Cluster) -> Option<SequenceNumber> {
    cluster.nodes()[0].local_end(SEGMENT).await
}

/// Starts the append of record 4 on a slow link and returns while the request is still on its way.
async fn start_an_append_on_a_slow_link(
    cluster: &Arc<Cluster>,
) -> tokio::task::JoinHandle<Result<AppendResponse, ReplicaError>> {
    cluster.nodes()[0].link().set_fault(SLOW);
    let sender = {
        let cluster = Arc::clone(cluster);
        tokio::spawn(async move { append_record_4(&cluster).await })
    };
    tokio::time::sleep(WHILE_IN_FLIGHT).await;
    sender
}

#[tokio::test(start_paused = true)]
async fn healthy_link_carries_requests() {
    let cluster = cluster_with_three_records().await;

    let answer = append_record_4(&cluster).await;

    assert!(answer.is_ok(), "{answer:?}");
}

#[tokio::test(start_paused = true)]
async fn new_link_has_no_fault() {
    let cluster = cluster_with_three_records().await;

    let fault = cluster.nodes()[0].link().fault();

    assert_eq!(fault, LinkFault::default());
}

#[tokio::test(start_paused = true)]
async fn cut_link_fails_requests_without_touching_the_replica() {
    let cluster = cluster_with_three_records().await;
    cluster.nodes()[0].set_reachable(false);

    let refused = append_record_4(&cluster).await;

    assert!(is_unavailable(&refused), "{refused:?}");
    assert_eq!(local_end(&cluster).await, END_BEFORE_THE_APPEND);
}

#[tokio::test(start_paused = true)]
async fn restored_link_carries_requests_again() {
    let cluster = cluster_with_three_records().await;
    cluster.nodes()[0].set_reachable(false);
    cluster.nodes()[0].set_reachable(true);

    let answer = append_record_4(&cluster).await;

    assert!(answer.is_ok(), "{answer:?}");
}

#[tokio::test(start_paused = true)]
async fn link_that_loses_responses_still_applies_the_request() {
    let cluster = cluster_with_three_records().await;
    cluster.nodes()[0].link().set_fault(LOSES_RESPONSES);

    let answer = append_record_4(&cluster).await;

    assert!(is_unavailable(&answer), "{answer:?}");
    assert_eq!(local_end(&cluster).await, END_AFTER_THE_APPEND);
}

/// The fault is read when the answer is ready, not when the request left.
#[tokio::test(start_paused = true)]
async fn answer_is_lost_if_the_link_starts_losing_them_while_the_request_is_on_its_way() {
    let cluster = Arc::new(cluster_with_three_records().await);
    let sender = start_an_append_on_a_slow_link(&cluster).await;

    cluster.nodes()[0].link().set_fault(LinkFault {
        loses_responses: true,
        ..SLOW
    });
    let answer = sender.await.expect("the sending task finishes");

    assert!(is_unavailable(&answer), "{answer:?}");
    assert_eq!(local_end(&cluster).await, END_AFTER_THE_APPEND);
}

#[tokio::test(start_paused = true)]
async fn slow_link_holds_requests_for_its_delay() {
    let cluster = cluster_with_three_records().await;
    cluster.nodes()[0].link().set_fault(SLOW);
    let started_at = Instant::now();

    append_record_4(&cluster).await.expect("a slow link still delivers");

    assert_eq!(started_at.elapsed(), LINK_DELAY);
}

#[tokio::test(start_paused = true)]
async fn request_in_flight_when_the_link_is_cut_never_arrives() {
    let cluster = Arc::new(cluster_with_three_records().await);
    let sender = start_an_append_on_a_slow_link(&cluster).await;

    cluster.nodes()[0].set_reachable(false);
    let answer = sender.await.expect("the sending task finishes");

    assert!(is_unavailable(&answer), "{answer:?}");
    assert_eq!(local_end(&cluster).await, END_BEFORE_THE_APPEND);
}

#[tokio::test(start_paused = true)]
async fn request_in_flight_when_the_node_goes_down_fails() {
    let cluster = Arc::new(cluster_with_three_records().await);
    let sender = start_an_append_on_a_slow_link(&cluster).await;

    cluster.nodes()[0].crash();
    let answer = sender.await.expect("the sending task finishes");

    assert!(is_unavailable(&answer), "{answer:?}");
}

#[tokio::test(start_paused = true)]
async fn link_to_a_node_that_is_down_fails_requests() {
    let cluster = cluster_with_three_records().await;
    cluster.nodes()[0].crash();

    let answer = append_record_4(&cluster).await;

    assert!(is_unavailable(&answer), "{answer:?}");
}

#[tokio::test(start_paused = true)]
async fn stopping_the_process_twice_leaves_the_link_without_one() {
    let cluster = cluster_with_three_records().await;
    let link = cluster.nodes()[0].link();

    link.stop_process();
    link.stop_process();

    assert!(link.running_replica().is_none());
}

#[tokio::test(start_paused = true)]
async fn cut_link_fails_every_operation() {
    let cluster = cluster_with_three_records().await;
    let link = cluster.nodes()[0].link();
    link.set_fault(LinkFault {
        is_cut: true,
        ..LinkFault::default()
    });

    let failed_operations = operations_failed_as_unavailable(link.as_ref()).await;

    assert_eq!(failed_operations, EVERY_OPERATION);
}

#[tokio::test(start_paused = true)]
async fn link_that_loses_responses_loses_the_answer_of_every_operation() {
    let cluster = cluster_with_three_records().await;
    let link = cluster.nodes()[0].link();
    link.set_fault(LOSES_RESPONSES);

    let failed_operations = operations_failed_as_unavailable(link.as_ref()).await;

    assert_eq!(failed_operations, EVERY_OPERATION);
}
