//! The typed metadata operations, run on etcd.
//!
//! # Why this file exists
//!
//! Nothing in bitacora calls the key-value interface directly: replicas,
//! writers and the controller go through `bitacora_meta::MetadataStore`,
//! which turns each operation into a read followed by a conditional
//! transaction. Those operations are tested in depth on the in-memory
//! backend. This file runs a representative set of them on etcd, one or
//! two per kind of record, to show that the layer everything else uses
//! behaves the same on the backend a real cluster has.
//!
//! # What it contains
//!
//! - Streams: creation, and creation of a name that is taken.
//! - Epochs: a claim that comes second, and two claims at the same moment.
//! - Segments: open, seal, and roll in one step.
//! - Checkpoints: a commit, and a commit with a superseded fencing token.
//! - Controller leadership: held, and taken over once lapsed.
//!
//! # How the fixtures work
//!
//! Each test builds a `MetadataStore` on a store under a fresh key prefix
//! of the shared etcd (see `etcd_process.rs`). The helpers mirror the ones
//! of the tests in `bitacora-meta/src/store.rs`, so a scenario reads the
//! same in both places. Time never passes here: leases and leadership take
//! the timestamps the tests hand them.

use std::sync::Arc;

use bitacora_meta::{MetaError, MetadataStore, RetentionPolicy};
use bitacora_types::{Epoch, ReplicaId, ReplicationSettings, SegmentId, SequenceNumber, Timestamp};

use crate::etcd_process::{EtcdProcess, shared_etcd};

/// The stream every test works on.
const STREAM: &str = "orders";

/// The materializer the checkpoint tests register.
const MATERIALIZER: &str = "pages";

/// When the writer lease of a claimed epoch runs out.
const LEASE_EXPIRY: Timestamp = Timestamp::from_unix_millis(10_000);

/// When a segment is recorded as sealed.
const SEALED_AT: Timestamp = Timestamp::from_unix_millis(5_000);

/// The moment the first controller asks to lead.
const NOW: Timestamp = Timestamp::from_unix_millis(1_000);

/// When the first controller's leadership lapses.
const LEADERSHIP_EXPIRY: Timestamp = Timestamp::from_unix_millis(6_000);

/// Returns the replicas the next segment of the stream would be placed on.
fn standby() -> Vec<ReplicaId> {
    let mut replica_ids = Vec::new();
    for name in ["replica-a", "replica-b", "replica-c"] {
        replica_ids.push(ReplicaId::new(name).expect("valid identifier"));
    }
    replica_ids
}

/// Returns an empty metadata store on `etcd`.
async fn empty_store(etcd: &EtcdProcess) -> MetadataStore {
    MetadataStore::new(Arc::new(etcd.store_in_fresh_namespace().await))
}

/// Returns a store with stream "orders", standby placed, no writer yet.
async fn store_with_stream(etcd: &EtcdProcess) -> MetadataStore {
    let store = empty_store(etcd).await;
    store
        .create_stream(
            STREAM,
            ReplicationSettings::DEFAULT,
            RetentionPolicy::default(),
            Some(standby()),
        )
        .await
        .expect("stream is created");
    store
}

/// Returns a store where a writer holds epoch 1 and segment 1 is open.
async fn store_with_open_segment(etcd: &EtcdProcess) -> MetadataStore {
    let store = store_with_stream(etcd).await;
    store
        .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
        .await
        .expect("epoch is claimed");
    store
        .open_segment(STREAM, Epoch::new(1))
        .await
        .expect("segment is opened");
    store
}

// ---- streams ----

#[tokio::test]
async fn created_stream_is_read_back() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = empty_store(&etcd).await;
    let created = store
        .create_stream(
            STREAM,
            ReplicationSettings::DEFAULT,
            RetentionPolicy::default(),
            Some(standby()),
        )
        .await
        .expect("stream is created");

    let read = store.get_stream(STREAM).await;

    assert_eq!(read, Ok(created));
}

#[tokio::test]
async fn creating_a_stream_that_exists_is_refused() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = store_with_stream(&etcd).await;

    let second_creation = store
        .create_stream(STREAM, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
        .await;

    assert_eq!(
        second_creation,
        Err(MetaError::StreamAlreadyExists {
            name: STREAM.to_owned()
        })
    );
}

/// Stream identifiers come from a counter kept in the store, read and
/// advanced with a conditional write.
#[tokio::test]
async fn two_streams_get_different_identifiers() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = store_with_stream(&etcd).await;
    let orders = store.get_stream(STREAM).await.expect("stream exists");

    let payments = store
        .create_stream(
            "payments",
            ReplicationSettings::DEFAULT,
            RetentionPolicy::default(),
            None,
        )
        .await
        .expect("stream is created");

    assert_ne!(payments.stream_id, orders.stream_id);
}

// ---- epochs ----

#[tokio::test]
async fn second_claim_from_the_same_epoch_loses() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = store_with_stream(&etcd).await;
    store
        .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
        .await
        .expect("epoch is claimed");

    let late_claim = store.claim_epoch(STREAM, Epoch::ZERO, "compute-2", LEASE_EXPIRY).await;

    assert_eq!(
        late_claim,
        Err(MetaError::Fenced {
            held_epoch: Epoch::ZERO,
            current_epoch: Epoch::new(1),
        })
    );
}

/// Two writers claim at the same moment, so both read the stream before
/// either writes. etcd's transaction must let one through and make the
/// other read again and find itself fenced.
#[tokio::test]
async fn only_one_of_two_racing_claims_wins() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = store_with_stream(&etcd).await;

    let (first_claim, second_claim) = tokio::join!(
        store.claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY),
        store.claim_epoch(STREAM, Epoch::ZERO, "compute-2", LEASE_EXPIRY),
    );

    let mut winner_count = 0;
    for claim in [first_claim, second_claim] {
        if claim.is_ok() {
            winner_count += 1;
        }
    }
    assert_eq!(winner_count, 1);
}

// ---- segments ----

#[tokio::test]
async fn opened_segment_is_the_first_of_the_stream() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = store_with_stream(&etcd).await;
    store
        .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
        .await
        .expect("epoch is claimed");

    let (_record, opened) = store
        .open_segment(STREAM, Epoch::new(1))
        .await
        .expect("segment is opened");

    assert_eq!(opened.segment_id, SegmentId::FIRST);
}

#[tokio::test]
async fn sealed_segment_keeps_its_end() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = store_with_open_segment(&etcd).await;

    let record = store
        .seal_segment(
            STREAM,
            Epoch::new(1),
            SegmentId::FIRST,
            SequenceNumber::new(50),
            SEALED_AT,
        )
        .await
        .expect("segment is sealed");

    assert_eq!(record.segments[0].sealed_end(), Some(SequenceNumber::new(50)));
}

#[tokio::test]
async fn roll_opens_the_next_segment_after_the_sealed_end() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = store_with_open_segment(&etcd).await;
    store
        .update_stream(STREAM, |record| {
            record.standby_replicas = Some(standby());
            Ok(())
        })
        .await
        .expect("standby is placed");

    let (_record, opened) = store
        .roll_segment(
            STREAM,
            Epoch::new(1),
            SegmentId::FIRST,
            SequenceNumber::new(50),
            SEALED_AT,
        )
        .await
        .expect("segment is rolled");

    assert_eq!(
        (opened.segment_id, opened.first_sequence),
        (SegmentId::new(2), SequenceNumber::new(51))
    );
}

// ---- checkpoints ----

#[tokio::test]
async fn committed_checkpoint_is_read_back() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = store_with_stream(&etcd).await;
    let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
    store
        .register_materializer(STREAM, MATERIALIZER)
        .await
        .expect("registered");
    let acquired = store
        .acquire_checkpoint(stream_id, MATERIALIZER)
        .await
        .expect("acquired");

    store
        .commit_checkpoint(
            stream_id,
            MATERIALIZER,
            acquired.fencing_token,
            SequenceNumber::new(40),
            None,
        )
        .await
        .expect("committed");

    let read = store
        .get_checkpoint(stream_id, MATERIALIZER)
        .await
        .expect("checkpoint exists");
    assert_eq!(read.checkpoint, SequenceNumber::new(40));
}

#[tokio::test]
async fn superseded_runner_cannot_commit() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = store_with_stream(&etcd).await;
    let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
    store
        .register_materializer(STREAM, MATERIALIZER)
        .await
        .expect("registered");
    let old_runner = store
        .acquire_checkpoint(stream_id, MATERIALIZER)
        .await
        .expect("acquired");
    let new_runner = store
        .acquire_checkpoint(stream_id, MATERIALIZER)
        .await
        .expect("acquired");

    let stale_commit = store
        .commit_checkpoint(
            stream_id,
            MATERIALIZER,
            old_runner.fencing_token,
            SequenceNumber::new(40),
            None,
        )
        .await;

    assert_eq!(
        stale_commit,
        Err(MetaError::MaterializerFenced {
            held_token: old_runner.fencing_token,
            current_token: new_runner.fencing_token,
        })
    );
}

// ---- controller leadership ----

#[tokio::test]
async fn held_leadership_is_refused_to_another_controller() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = empty_store(&etcd).await;
    store
        .try_lead("controller-1", NOW, LEADERSHIP_EXPIRY)
        .await
        .expect("store is available");

    let leads = store.try_lead("controller-2", NOW, LEADERSHIP_EXPIRY).await;

    assert_eq!(leads, Ok(false));
}

#[tokio::test]
async fn lapsed_leadership_can_be_taken_over() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = empty_store(&etcd).await;
    store
        .try_lead("controller-1", NOW, LEADERSHIP_EXPIRY)
        .await
        .expect("store is available");

    let leads = store
        .try_lead("controller-2", LEADERSHIP_EXPIRY, Timestamp::from_unix_millis(11_000))
        .await;

    assert_eq!(leads, Ok(true));
}
