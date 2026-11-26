//! Tests of the writer's operations: epochs, leases and segments.
//!
//! # What is covered
//!
//! - Epochs and leases: a claim raises the epoch by one, the second claim from the same epoch loses, a superseded
//!   writer cannot renew, and a released lease keeps the epoch.
//! - Opening: the first segment takes the standby, opening twice opens once, and opening is refused without a standby
//!   or over an unsealed segment of another epoch.
//! - Sealing: the end and the time are recorded, the same end can be sealed again, another end cannot, a segment can be
//!   sealed empty but not below that, and a superseded writer cannot seal.
//! - Rolling: seal and open in one step, all or nothing, and repeatable.
//! - That segment identifiers are never reused, and what a failover leaves behind.
//!
//! # How the fixtures work
//!
//! See `test_support.rs`. `store_with_open_segment` is the usual start: `compute-1` holds epoch 1 of the stream
//! `orders` and segment 1 is open on the standby replicas. `claim_as_second_writer` is the takeover that fences it.

use super::*;
use crate::store::test_support::{
    LEASE_EXPIRY, SEALED_AT, STREAM, place_standby, seal_first_segment_at, standby, store_with_open_segment,
    store_with_stream,
};

/// The epoch `compute-1` holds in [`store_with_open_segment`].
const FIRST_WRITER_EPOCH: Epoch = Epoch::new(1);

/// The epoch a second writer holds after it took the stream from the first.
const SECOND_WRITER_EPOCH: Epoch = Epoch::new(2);

/// Takes the stream from the writer of epoch 1, as `compute-2`.
async fn claim_as_second_writer(store: &MetadataStore) {
    store
        .claim_epoch(STREAM, FIRST_WRITER_EPOCH, "compute-2", LEASE_EXPIRY)
        .await
        .expect("the epoch is claimed");
}

/// Rolls away from segment 1 at `sealed_end`, as the writer of epoch 1.
async fn roll_first_segment_at(
    store: &MetadataStore,
    sealed_end: u64,
) -> Result<(StreamRecord, SegmentRecord), MetaError> {
    store
        .roll_segment(
            STREAM,
            FIRST_WRITER_EPOCH,
            SegmentId::FIRST,
            SequenceNumber::new(sealed_end),
            SEALED_AT,
        )
        .await
}

/// The stream's record as it is stored now.
async fn stored_stream(store: &MetadataStore) -> StreamRecord {
    store.get_stream(STREAM).await.expect("the stream exists")
}

#[tokio::test]
async fn claiming_raises_the_epoch_by_one() {
    let store = store_with_stream().await;

    let record = store
        .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
        .await
        .expect("the epoch is claimed");

    assert_eq!(record.epoch, Epoch::new(1));
}

#[tokio::test]
async fn claiming_records_the_writer_and_its_lease() {
    let store = store_with_stream().await;

    let record = store
        .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
        .await
        .expect("the epoch is claimed");

    assert_eq!(
        record.writer,
        Some(WriterLease {
            writer_identity: "compute-1".to_owned(),
            expires_at: LEASE_EXPIRY,
        })
    );
}

#[tokio::test]
async fn second_claim_from_the_same_epoch_loses() {
    let store = store_with_stream().await;
    store
        .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
        .await
        .expect("the epoch is claimed");

    let late_claim = store.claim_epoch(STREAM, Epoch::ZERO, "compute-2", LEASE_EXPIRY).await;

    assert_eq!(
        late_claim,
        Err(MetaError::Fenced {
            held_epoch: Epoch::ZERO,
            current_epoch: Epoch::new(1),
        })
    );
}

#[tokio::test]
async fn superseded_writer_cannot_renew_its_lease() {
    let store = store_with_open_segment().await;
    claim_as_second_writer(&store).await;

    let renewal = store
        .renew_lease(STREAM, FIRST_WRITER_EPOCH, Timestamp::from_unix_millis(99_000))
        .await;

    assert!(matches!(renewal, Err(MetaError::Fenced { .. })), "{renewal:?}");
}

#[tokio::test]
async fn renewal_extends_the_lease() {
    let store = store_with_open_segment().await;
    let extended_expiry = Timestamp::from_unix_millis(99_000);

    store
        .renew_lease(STREAM, FIRST_WRITER_EPOCH, extended_expiry)
        .await
        .expect("the lease is renewed");

    let lease = stored_stream(&store).await.writer.expect("the writer holds a lease");
    assert_eq!(lease.expires_at, extended_expiry);
}

#[tokio::test]
async fn renewal_of_a_released_lease_does_not_bring_it_back() {
    let store = store_with_open_segment().await;
    store
        .release_lease(STREAM, FIRST_WRITER_EPOCH)
        .await
        .expect("the lease is released");

    store
        .renew_lease(STREAM, FIRST_WRITER_EPOCH, Timestamp::from_unix_millis(99_000))
        .await
        .expect("renewing is not an error");

    assert_eq!(stored_stream(&store).await.writer, None);
}

#[tokio::test]
async fn released_lease_keeps_the_epoch() {
    let store = store_with_open_segment().await;

    store
        .release_lease(STREAM, FIRST_WRITER_EPOCH)
        .await
        .expect("the lease is released");

    let record = stored_stream(&store).await;
    assert_eq!(record.writer, None);
    assert_eq!(record.epoch, FIRST_WRITER_EPOCH);
}

#[tokio::test]
async fn superseded_writer_cannot_release_the_lease_of_its_successor() {
    let store = store_with_open_segment().await;
    claim_as_second_writer(&store).await;

    let release = store.release_lease(STREAM, FIRST_WRITER_EPOCH).await;

    assert!(matches!(release, Err(MetaError::Fenced { .. })), "{release:?}");
}

#[tokio::test]
async fn first_segment_starts_the_stream_under_the_writers_epoch() {
    let store = store_with_stream().await;
    store
        .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
        .await
        .expect("the epoch is claimed");

    let (_record, opened) = store
        .open_segment(STREAM, FIRST_WRITER_EPOCH)
        .await
        .expect("the segment is opened");

    assert_eq!(opened.segment_id, SegmentId::FIRST);
    assert_eq!(opened.first_sequence, SequenceNumber::FIRST);
    assert_eq!(opened.epoch, FIRST_WRITER_EPOCH);
}

#[tokio::test]
async fn first_segment_opens_on_the_standby_replicas() {
    let store = store_with_stream().await;
    store
        .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
        .await
        .expect("the epoch is claimed");

    let (_record, opened) = store
        .open_segment(STREAM, FIRST_WRITER_EPOCH)
        .await
        .expect("the segment is opened");

    let mut opened_on = Vec::new();
    for copy in &opened.copies {
        opened_on.push(copy.replica_id.clone());
    }
    assert_eq!(opened_on, standby());
}

#[tokio::test]
async fn copies_of_a_new_segment_are_unverified() {
    let store = store_with_open_segment().await;

    let record = stored_stream(&store).await;

    for copy in &record.segments[0].copies {
        assert_eq!(copy.state, CopyState::Unverified, "{copy:?}");
    }
}

#[tokio::test]
async fn opening_a_segment_uses_the_standby_up() {
    let store = store_with_open_segment().await;

    let record = stored_stream(&store).await;

    assert_eq!(record.standby_replicas, None);
}

#[tokio::test]
async fn opening_twice_under_one_epoch_opens_one_segment() {
    let store = store_with_open_segment().await;

    let (record, reopened) = store
        .open_segment(STREAM, FIRST_WRITER_EPOCH)
        .await
        .expect("the open segment is returned");

    assert_eq!(record.segments.len(), 1);
    assert_eq!(reopened.segment_id, SegmentId::FIRST);
}

#[tokio::test]
async fn opening_without_a_standby_fails() {
    let store = store_with_open_segment().await;
    seal_first_segment_at(&store, 50).await;

    let opened = store.open_segment(STREAM, FIRST_WRITER_EPOCH).await;

    assert_eq!(
        opened,
        Err(MetaError::NoStandbySegment {
            name: STREAM.to_owned()
        })
    );
}

#[tokio::test]
async fn new_writer_cannot_open_while_the_old_segment_is_unsealed() {
    let store = store_with_open_segment().await;
    claim_as_second_writer(&store).await;
    place_standby(&store).await;

    let opened = store.open_segment(STREAM, SECOND_WRITER_EPOCH).await;

    assert!(matches!(opened, Err(MetaError::InvalidState { .. })), "{opened:?}");
}

#[tokio::test]
async fn superseded_writer_cannot_open_a_segment() {
    let store = store_with_open_segment().await;
    claim_as_second_writer(&store).await;

    let opened = store.open_segment(STREAM, FIRST_WRITER_EPOCH).await;

    assert!(matches!(opened, Err(MetaError::Fenced { .. })), "{opened:?}");
}

#[tokio::test]
async fn sealing_records_the_end_and_the_time() {
    let store = store_with_open_segment().await;

    let record = seal_first_segment_at(&store, 50).await;

    assert_eq!(
        record.segments[0].sealed,
        Some(SealRecord {
            end: SequenceNumber::new(50),
            at: SEALED_AT,
        })
    );
}

#[tokio::test]
async fn sealing_again_at_the_same_end_succeeds() {
    let store = store_with_open_segment().await;
    seal_first_segment_at(&store, 50).await;

    let second_seal = store
        .seal_segment(
            STREAM,
            FIRST_WRITER_EPOCH,
            SegmentId::FIRST,
            SequenceNumber::new(50),
            SEALED_AT,
        )
        .await;

    assert!(second_seal.is_ok(), "{second_seal:?}");
}

#[tokio::test]
async fn sealing_again_at_another_end_fails() {
    let store = store_with_open_segment().await;
    seal_first_segment_at(&store, 50).await;

    let second_seal = store
        .seal_segment(
            STREAM,
            FIRST_WRITER_EPOCH,
            SegmentId::FIRST,
            SequenceNumber::new(60),
            SEALED_AT,
        )
        .await;

    assert!(
        matches!(second_seal, Err(MetaError::InvalidState { .. })),
        "{second_seal:?}"
    );
}

#[tokio::test]
async fn sealing_a_segment_that_is_not_the_last_fails() {
    let store = store_with_open_segment().await;

    let seal_of_another_segment = store
        .seal_segment(
            STREAM,
            FIRST_WRITER_EPOCH,
            SegmentId::new(2),
            SequenceNumber::new(50),
            SEALED_AT,
        )
        .await;

    assert!(
        matches!(seal_of_another_segment, Err(MetaError::InvalidState { .. })),
        "{seal_of_another_segment:?}"
    );
}

#[tokio::test]
async fn segment_can_be_sealed_empty() {
    let store = store_with_open_segment().await;

    let record = seal_first_segment_at(&store, 0).await;

    assert_eq!(record.next_segment_first_sequence(), SequenceNumber::FIRST);
}

#[tokio::test]
async fn sealing_below_the_segment_start_fails() {
    let store = store_with_open_segment().await;
    place_standby(&store).await;
    roll_first_segment_at(&store, 50).await.expect("the segment is rolled");

    let seal_below_start = store
        .seal_segment(
            STREAM,
            FIRST_WRITER_EPOCH,
            SegmentId::new(2),
            SequenceNumber::new(40),
            SEALED_AT,
        )
        .await;

    assert!(
        matches!(seal_below_start, Err(MetaError::InvalidState { .. })),
        "{seal_below_start:?}"
    );
}

#[tokio::test]
async fn superseded_writer_cannot_seal() {
    let store = store_with_open_segment().await;
    claim_as_second_writer(&store).await;

    let stale_seal = store
        .seal_segment(
            STREAM,
            FIRST_WRITER_EPOCH,
            SegmentId::FIRST,
            SequenceNumber::new(50),
            SEALED_AT,
        )
        .await;

    assert_eq!(
        stale_seal,
        Err(MetaError::Fenced {
            held_epoch: FIRST_WRITER_EPOCH,
            current_epoch: SECOND_WRITER_EPOCH,
        })
    );
}

#[tokio::test]
async fn roll_seals_the_old_segment() {
    let store = store_with_open_segment().await;
    place_standby(&store).await;

    let (record, _opened) = roll_first_segment_at(&store, 50).await.expect("the segment is rolled");

    assert_eq!(record.segments[0].sealed_end(), Some(SequenceNumber::new(50)));
}

#[tokio::test]
async fn roll_opens_the_next_segment_right_after_the_sealed_end() {
    let store = store_with_open_segment().await;
    place_standby(&store).await;

    let (_record, opened) = roll_first_segment_at(&store, 50).await.expect("the segment is rolled");

    assert_eq!(opened.segment_id, SegmentId::new(2));
    assert_eq!(opened.first_sequence, SequenceNumber::new(51));
}

#[tokio::test]
async fn roll_without_a_standby_is_refused() {
    let store = store_with_open_segment().await;

    let roll = roll_first_segment_at(&store, 50).await;

    assert!(matches!(roll, Err(MetaError::NoStandbySegment { .. })), "{roll:?}");
}

#[tokio::test]
async fn failed_roll_leaves_the_segment_open() {
    let store = store_with_open_segment().await;

    let _refused_roll = roll_first_segment_at(&store, 50).await;

    assert!(stored_stream(&store).await.segments[0].is_open());
}

#[tokio::test]
async fn repeated_roll_returns_the_segment_already_opened() {
    let store = store_with_open_segment().await;
    place_standby(&store).await;
    let (_record, first_opened) = roll_first_segment_at(&store, 50).await.expect("the segment is rolled");

    let (_record, opened_again) = roll_first_segment_at(&store, 50)
        .await
        .expect("the repeated roll succeeds");

    assert_eq!(opened_again, first_opened);
}

#[tokio::test]
async fn repeated_roll_opens_no_further_segment() {
    let store = store_with_open_segment().await;
    place_standby(&store).await;
    roll_first_segment_at(&store, 50).await.expect("the segment is rolled");

    roll_first_segment_at(&store, 50)
        .await
        .expect("the repeated roll succeeds");

    assert_eq!(stored_stream(&store).await.segments.len(), 2);
}

/// Replicas may still hold copies of segments that retention deleted. A new segment that reused one of their
/// identifiers would be taken for the old one.
#[tokio::test]
async fn segment_identifiers_are_not_reused_after_every_segment_is_deleted() {
    let store = store_with_open_segment().await;
    seal_first_segment_at(&store, 50).await;
    store
        .update_stream(STREAM, |record| {
            record.low_water_mark = SequenceNumber::new(51);
            record.segments.clear();
            Ok(())
        })
        .await
        .expect("retention deletes the only segment");
    place_standby(&store).await;

    let (_record, opened) = store
        .open_segment(STREAM, FIRST_WRITER_EPOCH)
        .await
        .expect("the segment is opened");

    assert_eq!(opened.segment_id, SegmentId::new(2));
}

#[tokio::test]
async fn segment_opened_after_every_segment_was_deleted_starts_at_the_low_water_mark() {
    let store = store_with_open_segment().await;
    seal_first_segment_at(&store, 50).await;
    store
        .update_stream(STREAM, |record| {
            record.low_water_mark = SequenceNumber::new(51);
            record.segments.clear();
            Ok(())
        })
        .await
        .expect("retention deletes the only segment");
    place_standby(&store).await;

    let (_record, opened) = store
        .open_segment(STREAM, FIRST_WRITER_EPOCH)
        .await
        .expect("the segment is opened");

    assert_eq!(opened.first_sequence, SequenceNumber::new(51));
}

#[tokio::test]
async fn failover_seals_the_old_segment_and_opens_under_the_new_epoch() {
    let store = store_with_open_segment().await;
    claim_as_second_writer(&store).await;
    place_standby(&store).await;
    store
        .seal_segment(
            STREAM,
            SECOND_WRITER_EPOCH,
            SegmentId::FIRST,
            SequenceNumber::new(77),
            SEALED_AT,
        )
        .await
        .expect("the new writer seals the old segment");

    let (_record, opened) = store
        .open_segment(STREAM, SECOND_WRITER_EPOCH)
        .await
        .expect("the new writer opens its segment");

    assert_eq!(opened.epoch, SECOND_WRITER_EPOCH);
    assert_eq!(opened.first_sequence, SequenceNumber::new(78));
}
