//! Tests of controller leadership.
//!
//! # What is covered
//!
//! - Taking it: the first candidate leads, a second one waits while the leadership is live, and a lapsed leadership can
//!   be taken over.
//! - Keeping it: the leader renews its own leadership, and the renewal extends it.
//! - Since when: a renewal keeps the moment the leadership began; a takeover, and a leader that let its own leadership
//!   lapse, start a new one.
//! - Reading it: nobody before the first claim, the record after it, and a record written before the start of a
//!   leadership was recorded.
//!
//! # How the fixtures work
//!
//! `store_led_by_the_first_controller` gives a store in which `controller-1` took the leadership at `NOW`, until
//! `LEADERSHIP_EXPIRY` (both in `test_support.rs`). `stored_leader` reads the record back.
//! `store_with_a_leader_record_without_a_start` writes the JSON of an older leader record straight into the backend.

use bytes::Bytes;

use super::*;
use crate::key_value::{KeyValueStore, Mutation};
use crate::store::test_support::{LEADERSHIP_EXPIRY, NOW, empty_store, empty_store_with_backend};

/// The instance that leads in [`store_led_by_the_first_controller`].
const FIRST_CONTROLLER: &str = "controller-1";

/// The instance that stands by.
const SECOND_CONTROLLER: &str = "controller-2";

/// A moment at which a leadership taken at [`NOW`] is still live.
const BEFORE_THE_LAPSE: Timestamp = Timestamp::from_unix_millis(4_000);

/// A moment at which a leadership taken at [`NOW`] has lapsed.
const AFTER_THE_LAPSE: Timestamp = Timestamp::from_unix_millis(20_000);

/// When a leadership taken or renewed late in these tests lapses.
const LATER_EXPIRY: Timestamp = Timestamp::from_unix_millis(25_000);

/// Returns a store in which [`FIRST_CONTROLLER`] leads from [`NOW`] until [`LEADERSHIP_EXPIRY`].
async fn store_led_by_the_first_controller() -> MetadataStore {
    let store = empty_store();
    store
        .try_lead(FIRST_CONTROLLER, NOW, LEADERSHIP_EXPIRY)
        .await
        .expect("the store is available");
    store
}

/// Returns a store whose leader record was written before the start of a leadership was recorded: it names
/// [`FIRST_CONTROLLER`] until [`LEADERSHIP_EXPIRY`] and has no `leading_since` field. Such records must still decode,
/// and a renewal of one has no earlier start to keep.
async fn store_with_a_leader_record_without_a_start() -> MetadataStore {
    let fixture = empty_store_with_backend();
    let record_without_a_start = Mutation::Put {
        key: CONTROLLER_LEADER_KEY.to_owned(),
        value: Bytes::from_static(br#"{"holder":"controller-1","expires_at":6000}"#),
    };
    fixture
        .key_value_store
        .transact(Vec::new(), vec![record_without_a_start])
        .await
        .expect("the store is available");
    fixture.store
}

/// The leader record as it is stored now.
async fn stored_leader(store: &MetadataStore) -> LeaderRecord {
    store
        .get_leader()
        .await
        .expect("the store is available")
        .expect("somebody leads")
}

#[tokio::test]
async fn first_candidate_becomes_the_leader() {
    let store = empty_store();

    let leads = store.try_lead(FIRST_CONTROLLER, NOW, LEADERSHIP_EXPIRY).await;

    assert_eq!(leads, Ok(true));
}

#[tokio::test]
async fn nobody_leads_a_store_that_was_never_led() {
    let store = empty_store();

    let leader = store.get_leader().await;

    assert_eq!(leader, Ok(None));
}

#[tokio::test]
async fn leader_can_be_read_back() {
    let store = store_led_by_the_first_controller().await;

    let leader = store.get_leader().await;

    assert_eq!(
        leader,
        Ok(Some(LeaderRecord {
            holder: FIRST_CONTROLLER.to_owned(),
            expires_at: LEADERSHIP_EXPIRY,
            leading_since: Some(NOW),
        }))
    );
}

#[tokio::test]
async fn second_candidate_waits_while_the_leader_is_live() {
    let store = store_led_by_the_first_controller().await;

    let leads = store.try_lead(SECOND_CONTROLLER, NOW, LEADERSHIP_EXPIRY).await;

    assert_eq!(leads, Ok(false));
}

#[tokio::test]
async fn refused_candidate_does_not_change_the_leader() {
    let store = store_led_by_the_first_controller().await;

    let _refused = store.try_lead(SECOND_CONTROLLER, NOW, LATER_EXPIRY).await;

    let leader = stored_leader(&store).await;
    assert_eq!(leader.holder, FIRST_CONTROLLER);
    assert_eq!(leader.expires_at, LEADERSHIP_EXPIRY);
}

#[tokio::test]
async fn leader_renews_its_own_leadership() {
    let store = store_led_by_the_first_controller().await;

    let leads = store
        .try_lead(FIRST_CONTROLLER, NOW, Timestamp::from_unix_millis(9_000))
        .await;

    assert_eq!(leads, Ok(true));
}

#[tokio::test]
async fn renewal_extends_the_leadership() {
    let store = store_led_by_the_first_controller().await;
    let extended_expiry = Timestamp::from_unix_millis(9_000);

    store
        .try_lead(FIRST_CONTROLLER, BEFORE_THE_LAPSE, extended_expiry)
        .await
        .expect("the store is available");

    assert_eq!(stored_leader(&store).await.expires_at, extended_expiry);
}

#[tokio::test]
async fn lapsed_leadership_can_be_taken_over() {
    let store = store_led_by_the_first_controller().await;

    let leads = store
        .try_lead(
            SECOND_CONTROLLER,
            LEADERSHIP_EXPIRY,
            Timestamp::from_unix_millis(11_000),
        )
        .await;

    assert_eq!(leads, Ok(true));
}

#[tokio::test]
async fn renewal_keeps_the_moment_the_leadership_began() {
    let store = store_led_by_the_first_controller().await;

    store
        .try_lead(FIRST_CONTROLLER, BEFORE_THE_LAPSE, Timestamp::from_unix_millis(9_000))
        .await
        .expect("the store is available");

    assert_eq!(stored_leader(&store).await.leading_since, Some(NOW));
}

#[tokio::test]
async fn takeover_starts_a_new_leadership() {
    let store = store_led_by_the_first_controller().await;

    store
        .try_lead(
            SECOND_CONTROLLER,
            LEADERSHIP_EXPIRY,
            Timestamp::from_unix_millis(11_000),
        )
        .await
        .expect("the store is available");

    let leader = stored_leader(&store).await;
    assert_eq!(leader.holder, SECOND_CONTROLLER);
    assert_eq!(leader.leading_since, Some(LEADERSHIP_EXPIRY));
}

#[tokio::test]
async fn leader_that_let_its_leadership_lapse_starts_a_new_one() {
    let store = store_led_by_the_first_controller().await;

    store
        .try_lead(FIRST_CONTROLLER, AFTER_THE_LAPSE, LATER_EXPIRY)
        .await
        .expect("the store is available");

    assert_eq!(stored_leader(&store).await.leading_since, Some(AFTER_THE_LAPSE));
}

#[tokio::test]
async fn leader_record_written_before_the_start_was_recorded_is_still_read() {
    let store = store_with_a_leader_record_without_a_start().await;

    let leader = store.get_leader().await;

    assert_eq!(
        leader,
        Ok(Some(LeaderRecord {
            holder: FIRST_CONTROLLER.to_owned(),
            expires_at: LEADERSHIP_EXPIRY,
            leading_since: None,
        }))
    );
}

#[tokio::test]
async fn renewal_of_a_record_without_a_start_records_the_renewal_as_the_start() {
    let store = store_with_a_leader_record_without_a_start().await;

    store
        .try_lead(FIRST_CONTROLLER, BEFORE_THE_LAPSE, Timestamp::from_unix_millis(9_000))
        .await
        .expect("the store is available");

    assert_eq!(stored_leader(&store).await.leading_since, Some(BEFORE_THE_LAPSE));
}

#[tokio::test]
async fn leadership_cannot_be_taken_while_the_store_is_unavailable() {
    let fixture = empty_store_with_backend();
    fixture.key_value_store.set_unavailable(true);

    let leads = fixture.store.try_lead(FIRST_CONTROLLER, NOW, LEADERSHIP_EXPIRY).await;

    assert!(matches!(leads, Err(MetaError::Unavailable(_))), "{leads:?}");
}
