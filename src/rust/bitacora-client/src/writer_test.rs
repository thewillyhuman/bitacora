//! Tests of the small decisions of the writer's handle.
//!
//! # What is covered
//!
//! - What a failed metadata write means for a writer that is closing: it lost the stream, or the outcome is unknown.
//! - What an append resolves to when its writer goes away.
//! - That the replicas' identities are handed to the state in the order of the segment's replica set, which is what the
//!   senders' indexes refer to.
//!
//! The handle as a whole (appends, flow control, rolls, close, drop) is covered end to end in `tests/writer_test.rs`,
//! and the bookkeeping in `writer/state_test.rs`.
//!
//! # How the fixtures work
//!
//! The functions under test are pure. A connected replica is needed only for its identity; the tests give it a stand-in
//! handle that fails every request, because none is sent.

use bitacora_types::replica_api::UnreachableReplica;

use super::*;

/// A connected replica named `name` that fails every request.
fn stand_in_replica(name: &str) -> ConnectedReplica {
    ConnectedReplica {
        replica_id: ReplicaId::new(name).expect("a valid identifier"),
        client: Arc::new(UnreachableReplica::new("the functions under test send no request")),
    }
}

#[test]
fn closing_writer_whose_epoch_was_superseded_reports_that_it_was_fenced() {
    let fenced = MetaError::Fenced {
        held_epoch: Epoch::new(4),
        current_epoch: Epoch::new(5),
    };

    let append_error = close_error(&fenced);

    assert_eq!(append_error, AppendError::Fenced);
}

#[test]
fn closing_writer_whose_seal_failed_otherwise_reports_an_unknown_outcome() {
    let not_found = MetaError::StreamNotFound {
        name: "orders".to_owned(),
    };

    let append_error = close_error(&not_found);

    assert_eq!(
        append_error,
        AppendError::Indeterminate {
            reason: "the segment could not be sealed: stream \"orders\" does not exist".to_owned()
        }
    );
}

#[test]
fn append_of_a_writer_that_went_away_has_an_unknown_outcome() {
    let append_error = writer_gone();

    assert_eq!(
        append_error,
        AppendError::Indeterminate {
            reason: "the writer was dropped before the outcome was known".to_owned()
        }
    );
}

#[test]
fn replica_identities_keep_the_order_of_the_replica_set() {
    let replicas = [
        stand_in_replica("replica-c"),
        stand_in_replica("replica-a"),
        stand_in_replica("replica-b"),
    ];

    let replica_ids = replica_ids_of(&replicas);

    let names: Vec<&str> = replica_ids.iter().map(ReplicaId::as_str).collect();
    assert_eq!(names, ["replica-c", "replica-a", "replica-b"]);
}

#[test]
fn no_replicas_have_no_identities() {
    let replica_ids = replica_ids_of(&[]);

    assert_eq!(replica_ids, Vec::<ReplicaId>::new());
}
