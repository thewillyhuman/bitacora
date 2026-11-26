//! Tests of what a page store decides from its state alone.
//!
//! # What is covered
//!
//! - The read plan: the base image, the delta layers in reach, the records in memory, and the horizon.
//! - The delta layer a checkpoint flushes, and the image layer it may write.
//! - The layering policy: when an image is due, and which layers no read needs any more.
//! - The stored details: that they give the state back, and every way they are refused.
//!
//! # How the fixtures work
//!
//! `delta` and `image` build layer descriptors from the sequence numbers they cover; the artifact key says the same,
//! so a failed assertion names the layer. `state_with` wraps a list of layers in a state that has applied everything
//! up to the end of the last layer. `record` is a record with a one-byte payload, and `page` the one page the
//! tests of records in memory use. Nothing here touches an artifact store: the rules are pure.
//!
//! The same rules are tested again through the store, with real layers, in `page_store_test.rs`.

use bytes::BufMut;

use super::*;
use crate::layer::{LAYER_HEADER_BYTES, decode_header};

/// The encoded identifier of the page the tests of records in memory use.
fn page() -> Bytes {
    Bytes::from_static(b"\x07")
}

/// A delta layer that covers `first_sequence` to `last_sequence`.
fn delta(first_sequence: u64, last_sequence: u64) -> LayerDescriptor {
    LayerDescriptor {
        kind: LayerKind::Delta,
        first_sequence: SequenceNumber::new(first_sequence),
        last_sequence: SequenceNumber::new(last_sequence),
        artifact_key: format!("delta-{first_sequence}-{last_sequence}"),
    }
}

/// An image layer as of `sequence`.
fn image(sequence: u64) -> LayerDescriptor {
    LayerDescriptor {
        kind: LayerKind::Image,
        first_sequence: SequenceNumber::new(sequence),
        last_sequence: SequenceNumber::new(sequence),
        artifact_key: format!("image-{sequence}"),
    }
}

/// A state with `layers` and nothing in memory, that has applied everything its last layer covers.
fn state_with(layers: Vec<LayerDescriptor>) -> StoreState {
    let applied_sequence = match layers.last() {
        Some(last_layer) => last_layer.last_sequence,
        None => SequenceNumber::ZERO,
    };
    StoreState {
        layers,
        unflushed_by_page: BTreeMap::new(),
        applied_sequence,
        horizon: SequenceNumber::ZERO,
    }
}

/// The record at `sequence`, with a payload of one byte.
fn record(sequence: u64) -> LogRecord {
    LogRecord {
        sequence: SequenceNumber::new(sequence),
        payload: Bytes::from_static(b"x"),
    }
}

/// `state` with the records at `sequences` in memory for `page`, applied up to the last of them.
fn with_unflushed_records(mut state: StoreState, sequences: &[u64]) -> StoreState {
    let mut chain = Vec::new();
    for sequence in sequences {
        chain.push(record(*sequence));
        state.applied_sequence = SequenceNumber::new(*sequence);
    }
    state.unflushed_by_page.insert(page(), chain);
    state
}

/// The plan for reading `page` as of `as_of_sequence`.
fn plan(state: &StoreState, as_of_sequence: u64) -> ReadPlan {
    state
        .plan_read(&page(), SequenceNumber::new(as_of_sequence))
        .expect("the sequence is at or above the horizon")
}

/// The details of `state`, with the bytes from `cut_at` on removed.
fn details_cut_at(state: &StoreState, cut_at: usize) -> Bytes {
    encode_details(state).slice(..cut_at)
}

/// The reason `decode_details` gives for refusing `details`.
fn refusal_of(details: &Bytes) -> String {
    match decode_details(details) {
        Err(MaterializerError::InvalidManifest { reason }) => reason,
        other => panic!("expected the details to be refused as an invalid manifest, got {other:?}"),
    }
}

#[test]
fn empty_state_has_no_layers_and_is_at_the_empty_position() {
    let state = StoreState::empty();

    assert_eq!(state, state_with(Vec::new()));
}

#[test]
fn read_without_an_image_starts_from_nothing() {
    let state = state_with(vec![delta(1, 40), delta(41, 90)]);

    let planned = plan(&state, 90);

    assert_eq!(planned.base_image, None);
    assert_eq!(planned.base_sequence, SequenceNumber::ZERO);
}

#[test]
fn read_starts_from_the_newest_image_at_or_below_its_sequence() {
    let state = state_with(vec![delta(1, 40), image(40), delta(41, 90), image(90), delta(91, 130)]);

    let planned = plan(&state, 100);

    assert_eq!(planned.base_image, Some(image(90)));
    assert_eq!(planned.base_sequence, SequenceNumber::new(90));
}

#[test]
fn read_below_the_newest_image_starts_from_an_older_one() {
    let state = state_with(vec![delta(1, 40), image(40), delta(41, 90), image(90)]);

    let planned = plan(&state, 89);

    assert_eq!(planned.base_image, Some(image(40)));
}

#[test]
fn read_at_the_sequence_of_an_image_starts_from_that_image() {
    let state = state_with(vec![delta(1, 40), image(40), delta(41, 90)]);

    let planned = plan(&state, 40);

    assert_eq!(planned.base_image, Some(image(40)));
}

#[test]
fn read_needs_the_delta_layers_after_its_base_up_to_its_sequence() {
    let state = state_with(vec![
        delta(1, 40),
        image(40),
        delta(41, 90),
        delta(91, 130),
        delta(131, 150),
    ]);

    let planned = plan(&state, 100);

    assert_eq!(planned.delta_layers, vec![delta(41, 90), delta(91, 130)]);
}

#[test]
fn read_without_an_image_needs_every_delta_layer_up_to_its_sequence() {
    let state = state_with(vec![delta(1, 40), delta(41, 90), delta(91, 130)]);

    let planned = plan(&state, 41);

    assert_eq!(planned.delta_layers, vec![delta(1, 40), delta(41, 90)]);
}

#[test]
fn read_at_the_sequence_of_an_image_needs_no_delta_layer() {
    let state = state_with(vec![delta(1, 40), image(40), delta(41, 90)]);

    let planned = plan(&state, 40);

    assert_eq!(planned.delta_layers, Vec::new());
}

#[test]
fn read_takes_the_records_in_memory_up_to_its_sequence() {
    let state = with_unflushed_records(state_with(vec![delta(1, 40)]), &[41, 45, 50]);

    let planned = plan(&state, 45);

    assert_eq!(planned.unflushed_records, vec![record(41), record(45)]);
}

#[test]
fn read_of_a_page_without_records_in_memory_takes_none() {
    let state = with_unflushed_records(state_with(vec![delta(1, 40)]), &[41]);
    let another_page = Bytes::from_static(b"\x08");

    let planned = state
        .plan_read(&another_page, SequenceNumber::new(41))
        .expect("the sequence is at or above the horizon");

    assert_eq!(planned.unflushed_records, Vec::new());
}

#[test]
fn read_below_the_horizon_is_refused_with_the_horizon() {
    let mut state = state_with(vec![image(90), delta(91, 130)]);
    state.horizon = SequenceNumber::new(90);

    let planned = state.plan_read(&page(), SequenceNumber::new(89));

    assert_eq!(
        planned,
        Err(PageReadError::BelowHorizon {
            as_of_sequence: SequenceNumber::new(89),
            horizon: SequenceNumber::new(90),
        })
    );
}

#[test]
fn read_at_the_horizon_is_planned() {
    let mut state = state_with(vec![image(90), delta(91, 130)]);
    state.horizon = SequenceNumber::new(90);

    let planned = state.plan_read(&page(), SequenceNumber::new(90));

    assert!(planned.is_ok(), "{planned:?}");
}

#[test]
fn state_without_records_in_memory_flushes_no_delta_layer() {
    let state = state_with(vec![delta(1, 40)]);

    let flushed = state.delta_layer_of_unflushed_records();

    assert_eq!(flushed, None);
}

#[test]
fn flushed_delta_layer_covers_from_the_oldest_record_in_memory_to_the_last_applied() {
    let state = with_unflushed_records(state_with(vec![delta(1, 40)]), &[43, 45, 50]);

    let flushed = state
        .delta_layer_of_unflushed_records()
        .expect("there are records in memory");

    assert_eq!(flushed.kind, LayerKind::Delta);
    assert_eq!(flushed.first_sequence, SequenceNumber::new(43));
    assert_eq!(flushed.last_sequence, SequenceNumber::new(50));
}

#[test]
fn flushed_delta_layer_says_in_its_header_what_it_covers() {
    let state = with_unflushed_records(state_with(Vec::new()), &[1, 2, 3]);
    let flushed = state
        .delta_layer_of_unflushed_records()
        .expect("there are records in memory");

    let header = decode_header(&flushed.contents[..LAYER_HEADER_BYTES]).expect("a layer that was just built");

    assert_eq!(header.kind, LayerKind::Delta);
    assert_eq!(header.first_sequence, SequenceNumber::new(1));
    assert_eq!(header.last_sequence, SequenceNumber::new(3));
    assert_eq!(header.page_count, 1);
}

#[test]
fn image_layer_covers_the_one_sequence_it_is_at() {
    let mut pages_by_id = BTreeMap::new();
    pages_by_id.insert(page(), Bytes::from_static(b"the page"));

    let built = image_layer_of(&pages_by_id, SequenceNumber::new(90));

    assert_eq!(built.kind, LayerKind::Image);
    assert_eq!(built.first_sequence, SequenceNumber::new(90));
    assert_eq!(built.last_sequence, SequenceNumber::new(90));
}

#[test]
fn new_layer_is_listed_under_the_key_it_was_stored_as() {
    let built = image_layer_of(&BTreeMap::new(), SequenceNumber::new(90));

    let listed = built.stored_as("image-90".to_owned());

    assert_eq!(listed, image(90));
}

#[test]
fn image_is_due_once_as_many_delta_layers_follow_the_last_image_as_the_policy_says() {
    let policy = LayeringPolicy {
        image_after_delta_layers: 2,
        images_kept: 2,
    };

    let is_due = is_image_due(&[delta(1, 40), image(40), delta(41, 90), delta(91, 130)], policy);

    assert!(is_due);
}

#[test]
fn image_is_not_due_while_fewer_delta_layers_follow_the_last_image() {
    let policy = LayeringPolicy {
        image_after_delta_layers: 2,
        images_kept: 2,
    };

    let is_due = is_image_due(&[delta(1, 40), delta(41, 90), image(90), delta(91, 130)], policy);

    assert!(!is_due);
}

#[test]
fn delta_layers_count_towards_the_first_image_from_the_start() {
    let policy = LayeringPolicy {
        image_after_delta_layers: 2,
        images_kept: 2,
    };

    let is_due = is_image_due(&[delta(1, 40), delta(41, 90)], policy);

    assert!(is_due);
}

#[test]
fn policy_that_asks_for_an_image_after_no_delta_layer_gets_one_after_each() {
    let policy = LayeringPolicy {
        image_after_delta_layers: 0,
        images_kept: 2,
    };

    assert!(is_image_due(&[image(40), delta(41, 90)], policy));
    assert!(!is_image_due(&[delta(1, 40), image(40)], policy));
}

#[test]
fn newest_image_is_found_wherever_it_is_in_the_list() {
    let layers = [delta(1, 40), image(40), delta(41, 90), image(90), delta(91, 130)];

    let position = newest_image_position(&layers);

    assert_eq!(position, Some(3));
}

#[test]
fn list_without_an_image_has_no_newest_image() {
    let position = newest_image_position(&[delta(1, 40), delta(41, 90)]);

    assert_eq!(position, None);
}

#[test]
fn layers_older_than_the_oldest_image_kept_are_dropped() {
    let mut layers = vec![
        delta(1, 40),
        image(40),
        delta(41, 90),
        image(90),
        delta(91, 130),
        image(130),
    ];

    drop_layers_no_read_needs(&mut layers, 2);

    assert_eq!(layers, vec![image(90), delta(91, 130), image(130)]);
}

#[test]
fn dropping_layers_moves_the_horizon_to_the_oldest_image_kept() {
    let mut layers = vec![delta(1, 40), image(40), delta(41, 90), image(90)];

    let horizon = drop_layers_no_read_needs(&mut layers, 1);

    assert_eq!(horizon, Some(SequenceNumber::new(90)));
}

#[test]
fn nothing_is_dropped_while_fewer_images_exist_than_are_kept() {
    let mut layers = vec![delta(1, 40), image(40), delta(41, 90), image(90)];
    let layers_before = layers.clone();

    let horizon = drop_layers_no_read_needs(&mut layers, 3);

    assert_eq!(horizon, None);
    assert_eq!(layers, layers_before);
}

#[test]
fn nothing_is_dropped_when_the_oldest_image_kept_is_the_first_layer() {
    let mut layers = vec![image(40), delta(41, 90), image(90)];
    let layers_before = layers.clone();

    let horizon = drop_layers_no_read_needs(&mut layers, 2);

    assert_eq!(horizon, None);
    assert_eq!(layers, layers_before);
}

#[test]
fn policy_that_keeps_no_image_still_keeps_the_newest() {
    let mut layers = vec![delta(1, 40), image(40), delta(41, 90), image(90)];

    drop_layers_no_read_needs(&mut layers, 0);

    assert_eq!(layers, vec![image(90)]);
}

#[test]
fn details_give_back_the_layers_the_position_and_the_horizon() {
    let mut state = state_with(vec![image(90), delta(91, 130)]);
    state.horizon = SequenceNumber::new(90);

    let restored = decode_details(&encode_details(&state));

    assert_eq!(restored, Ok(state));
}

#[test]
fn details_do_not_carry_the_records_in_memory() {
    let state = with_unflushed_records(state_with(vec![delta(1, 40)]), &[41]);

    let restored = decode_details(&encode_details(&state)).expect("details that were just written");

    assert_eq!(restored.unflushed_by_page, BTreeMap::new());
}

#[test]
fn details_of_an_empty_state_give_back_an_empty_state() {
    let restored = decode_details(&encode_details(&StoreState::empty()));

    assert_eq!(restored, Ok(StoreState::empty()));
}

#[test]
fn details_of_another_format_are_refused() {
    let mut details = BytesMut::from(&encode_details(&state_with(vec![delta(1, 40)]))[..]);
    details[0] = 0xFF;

    let reason = refusal_of(&details.freeze());

    assert_eq!(
        reason,
        "the page store details are not valid: they were written by another format"
    );
}

#[test]
fn details_cut_inside_their_fixed_part_are_refused() {
    let details = details_cut_at(&state_with(vec![delta(1, 40)]), 10);

    let reason = refusal_of(&details);

    assert_eq!(reason, "the page store details are not valid: they are cut short");
}

#[test]
fn details_cut_inside_a_layer_are_refused() {
    let whole = encode_details(&state_with(vec![delta(1, 40)]));
    let details = whole.slice(..whole.len() - 1);

    let reason = refusal_of(&details);

    assert_eq!(reason, "the page store details are not valid: they are cut short");
}

#[test]
fn details_that_end_before_the_layers_they_count_are_refused() {
    let one_layer = state_with(vec![delta(1, 40)]);
    let fixed_part_bytes = encode_details(&StoreState::empty()).len();
    let details = details_cut_at(&one_layer, fixed_part_bytes);

    let reason = refusal_of(&details);

    assert_eq!(reason, "the page store details are not valid: they are cut short");
}

#[test]
fn details_with_a_layer_of_an_unknown_kind_are_refused() {
    let fixed_part_bytes = encode_details(&StoreState::empty()).len();
    let mut details = BytesMut::from(&encode_details(&state_with(vec![delta(1, 40)]))[..]);
    details[fixed_part_bytes] = 0x7F;

    let reason = refusal_of(&details.freeze());

    assert_eq!(
        reason,
        "the page store details are not valid: a layer is of an unknown kind"
    );
}

#[test]
fn details_with_an_artifact_key_that_is_not_text_are_refused() {
    let mut details = BytesMut::from(&encode_details(&state_with(vec![delta(1, 40)]))[..]);
    let last_byte = details.len() - 1;
    details[last_byte] = 0xFF;

    let reason = refusal_of(&details.freeze());

    assert_eq!(
        reason,
        "the page store details are not valid: an artifact key is not text"
    );
}

#[test]
fn details_with_bytes_after_the_last_layer_are_refused() {
    let mut details = BytesMut::from(&encode_details(&state_with(vec![delta(1, 40)]))[..]);
    details.put_u8(0);

    let reason = refusal_of(&details.freeze());

    assert_eq!(
        reason,
        "the page store details are not valid: they hold more than their lengths say"
    );
}
