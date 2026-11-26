//! Tests of the decisions inside the takeover protocol.
//!
//! # What is covered
//!
//! - Which lease a candidate waits for: a live one, and only when the policy respects leases.
//! - How many replicas recovery must hear from: N − W + 1, and never fewer than one.
//! - Where the segment is sealed: at the highest local end, unless a responder already knows the seal.
//! - Which responder the others are repaired from.
//! - What one replica's answer to the fence becomes: a responder, nothing, or the end of the takeover.
//! - How a failed metadata write is reported to the caller.
//!
//! The protocol as a whole, with replicas that crash and writers that race, is covered end to end in
//! `tests/writer_test.rs`, by the simulator in `bitacora-sim`, and exhaustively by `bitacora-model`.
//!
//! # How the fixtures work
//!
//! Every function under test is pure, so the tests build its arguments by hand. A responder needs a connected replica;
//! the tests give it a stand-in that fails every request, because none of these decisions sends one.

use std::time::Duration;

use bitacora_types::ReplicaId;
use bitacora_types::replica_api::UnreachableReplica;

use super::*;

/// The position before the first record of the segment the tests recover: what a replica without a copy counts as.
const EMPTY_END: SequenceNumber = SequenceNumber::new(100);

/// A connected replica named `name` that fails every request.
fn stand_in_replica(name: &str) -> ConnectedReplica {
    ConnectedReplica {
        replica_id: ReplicaId::new(name).expect("a valid identifier"),
        client: Arc::new(UnreachableReplica::new("the decisions under test send no request")),
    }
}

/// A responder named `name` whose copy ends at `local_end` and that knows no seal.
fn responder(name: &str, local_end: u64) -> Responder {
    Responder {
        replica: stand_in_replica(name),
        local_end: SequenceNumber::new(local_end),
        sealed_end: None,
    }
}

/// A responder named `name` whose copy ends at `local_end` and that knows the segment is sealed at `sealed_end`.
fn sealed_responder(name: &str, local_end: u64, sealed_end: u64) -> Responder {
    Responder {
        replica: stand_in_replica(name),
        local_end: SequenceNumber::new(local_end),
        sealed_end: Some(SequenceNumber::new(sealed_end)),
    }
}

/// A lease that lapses `unix_millis` milliseconds after the Unix epoch.
fn lease_expiring_at(unix_millis: u64) -> WriterLease {
    WriterLease {
        writer_identity: "primary-1".to_owned(),
        expires_at: Timestamp::from_unix_millis(unix_millis),
    }
}

/// What a request that outlived its timeout resolves to.
async fn elapsed_timeout() -> Elapsed {
    timeout(Duration::from_millis(1), std::future::pending::<()>())
        .await
        .expect_err("a future that never completes outlives any timeout")
}

#[test]
fn lease_that_has_not_lapsed_is_waited_for() {
    let lease = lease_expiring_at(5_000);

    let waited_for = live_lease(
        Some(&lease),
        TakeoverPolicy::WaitForLeaseExpiry,
        Timestamp::from_unix_millis(4_999),
    );

    assert_eq!(waited_for, Some(&lease));
}

#[test]
fn lease_that_lapses_now_is_not_waited_for() {
    let lease = lease_expiring_at(5_000);

    let waited_for = live_lease(
        Some(&lease),
        TakeoverPolicy::WaitForLeaseExpiry,
        Timestamp::from_unix_millis(5_000),
    );

    assert_eq!(waited_for, None);
}

#[test]
fn lease_that_lapsed_earlier_is_not_waited_for() {
    let lease = lease_expiring_at(5_000);

    let waited_for = live_lease(
        Some(&lease),
        TakeoverPolicy::WaitForLeaseExpiry,
        Timestamp::from_unix_millis(60_000),
    );

    assert_eq!(waited_for, None);
}

#[test]
fn forced_takeover_waits_for_no_lease() {
    let lease = lease_expiring_at(5_000);

    let waited_for = live_lease(Some(&lease), TakeoverPolicy::Force, Timestamp::from_unix_millis(0));

    assert_eq!(waited_for, None);
}

#[test]
fn stream_without_a_writer_has_no_lease_to_wait_for() {
    let waited_for = live_lease(None, TakeoverPolicy::WaitForLeaseExpiry, Timestamp::from_unix_millis(0));

    assert_eq!(waited_for, None);
}

#[test]
fn three_replicas_with_a_write_quorum_of_two_need_two_answers() {
    assert_eq!(recovery_quorum(3, 2), 2);
}

#[test]
fn five_replicas_with_a_write_quorum_of_three_need_three_answers() {
    assert_eq!(recovery_quorum(5, 3), 3);
}

#[test]
fn write_quorum_of_every_replica_needs_one_answer() {
    assert_eq!(recovery_quorum(3, 3), 1);
}

#[test]
fn write_quorum_larger_than_the_replica_set_still_needs_one_answer() {
    assert_eq!(recovery_quorum(1, 2), 1);
}

#[test]
fn recovery_end_is_the_highest_local_end() {
    let responders = [
        responder("replica-a", 104),
        responder("replica-b", 109),
        responder("replica-c", 107),
    ];

    let recovery_end = choose_recovery_end(&responders, EMPTY_END);

    assert_eq!(recovery_end, SequenceNumber::new(109));
}

#[test]
fn recovery_end_is_the_sealed_end_when_a_responder_knows_it() {
    let responders = [responder("replica-a", 109), sealed_responder("replica-b", 104, 106)];

    let recovery_end = choose_recovery_end(&responders, EMPTY_END);

    assert_eq!(recovery_end, SequenceNumber::new(106));
}

#[test]
fn sealed_end_is_taken_from_the_first_responder_that_knows_it() {
    let responders = [
        responder("replica-a", 109),
        sealed_responder("replica-b", 106, 106),
        sealed_responder("replica-c", 108, 108),
    ];

    let recovery_end = choose_recovery_end(&responders, EMPTY_END);

    assert_eq!(recovery_end, SequenceNumber::new(106));
}

#[test]
fn recovery_end_without_responders_is_the_empty_end() {
    let recovery_end = choose_recovery_end(&[], EMPTY_END);

    assert_eq!(recovery_end, EMPTY_END);
}

#[test]
fn responders_without_records_leave_the_segment_empty() {
    let responders = [
        responder("replica-a", EMPTY_END.get()),
        responder("replica-b", EMPTY_END.get()),
    ];

    let recovery_end = choose_recovery_end(&responders, EMPTY_END);

    assert_eq!(recovery_end, EMPTY_END);
}

#[test]
fn others_are_repaired_from_the_first_responder_that_reaches_the_recovery_end() {
    let responders = [
        responder("replica-a", 104),
        responder("replica-b", 109),
        responder("replica-c", 109),
    ];

    let longest = longest_responder(&responders, SequenceNumber::new(109)).expect("replica-b reaches the end");

    assert_eq!(longest.replica.replica_id.as_str(), "replica-b");
}

#[test]
fn nobody_is_repaired_from_when_no_responder_reaches_the_recovery_end() {
    let responders = [responder("replica-a", 104), sealed_responder("replica-b", 104, 109)];

    let longest = longest_responder(&responders, SequenceNumber::new(109));

    assert!(longest.is_none());
}

#[test]
fn replica_that_answers_the_fence_responds_with_its_local_end() {
    let fence_answer = FenceResponse {
        local_end: Some(SequenceNumber::new(104)),
        sealed_end: None,
    };

    let Ok(Some(responder)) = responder_from(stand_in_replica("replica-a"), Ok(Ok(fence_answer)), EMPTY_END) else {
        panic!("a replica that answers the fence is a responder");
    };

    assert_eq!(responder.local_end, SequenceNumber::new(104));
}

#[test]
fn replica_that_answers_the_fence_keeps_its_identity() {
    let fence_answer = FenceResponse {
        local_end: Some(SequenceNumber::new(104)),
        sealed_end: None,
    };

    let Ok(Some(responder)) = responder_from(stand_in_replica("replica-a"), Ok(Ok(fence_answer)), EMPTY_END) else {
        panic!("a replica that answers the fence is a responder");
    };

    assert_eq!(responder.replica.replica_id.as_str(), "replica-a");
}

#[test]
fn replica_without_a_copy_responds_with_the_empty_end() {
    let fence_answer = FenceResponse {
        local_end: None,
        sealed_end: None,
    };

    let Ok(Some(responder)) = responder_from(stand_in_replica("replica-a"), Ok(Ok(fence_answer)), EMPTY_END) else {
        panic!("a replica that answers the fence is a responder");
    };

    assert_eq!(responder.local_end, EMPTY_END);
}

#[test]
fn replica_that_knows_the_seal_responds_with_it() {
    let fence_answer = FenceResponse {
        local_end: Some(SequenceNumber::new(104)),
        sealed_end: Some(SequenceNumber::new(106)),
    };

    let Ok(Some(responder)) = responder_from(stand_in_replica("replica-a"), Ok(Ok(fence_answer)), EMPTY_END) else {
        panic!("a replica that answers the fence is a responder");
    };

    assert_eq!(responder.sealed_end, Some(SequenceNumber::new(106)));
}

#[test]
fn replica_fenced_at_a_higher_epoch_ends_the_takeover() {
    let fenced = ReplicaError::Fenced {
        request_epoch: Epoch::new(4),
        fenced_epoch: Epoch::new(5),
    };

    let Err(open_error) = responder_from(stand_in_replica("replica-a"), Ok(Err(fenced)), EMPTY_END) else {
        panic!("a higher fence means a newer writer is taking the stream");
    };

    assert_eq!(
        open_error,
        OpenError::TakenByAnotherWriter {
            current_epoch: Epoch::new(5)
        }
    );
}

#[test]
fn replica_that_fails_the_fence_does_not_respond() {
    let failed = ReplicaError::Storage {
        reason: "disk error".to_owned(),
    };

    let answer = responder_from(stand_in_replica("replica-a"), Ok(Err(failed)), EMPTY_END);

    assert!(matches!(answer, Ok(None)));
}

#[tokio::test(start_paused = true)]
async fn replica_that_does_not_answer_in_time_does_not_respond() {
    let timed_out = elapsed_timeout().await;

    let answer = responder_from(stand_in_replica("replica-a"), Err(timed_out), EMPTY_END);

    assert!(matches!(answer, Ok(None)));
}

#[test]
fn metadata_write_refused_for_the_epoch_means_another_writer_took_the_stream() {
    let fenced = MetaError::Fenced {
        held_epoch: Epoch::new(4),
        current_epoch: Epoch::new(6),
    };

    let open_error = lost_race_or_metadata(fenced);

    assert_eq!(
        open_error,
        OpenError::TakenByAnotherWriter {
            current_epoch: Epoch::new(6)
        }
    );
}

#[test]
fn other_metadata_failures_are_passed_on() {
    let not_found = MetaError::StreamNotFound {
        name: "orders".to_owned(),
    };

    let open_error = lost_race_or_metadata(not_found.clone());

    assert_eq!(open_error, OpenError::Metadata(not_found));
}
