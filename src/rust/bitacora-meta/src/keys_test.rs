//! Tests of the key layout and of the rule for names.
//!
//! # What is covered
//!
//! - Every key function, against the key written in `docs/protocol/metadata.md`.
//! - That the checkpoints of one stream are not listed with those of a stream whose identifier starts with the same
//!   digits.
//! - `validate_name`: what is accepted, and each way a name is refused.
//!
//! # How the fixtures work
//!
//! There are none: the functions are pure and take plain values.

use super::*;

#[test]
fn stream_record_is_keyed_by_the_stream_name() {
    assert_eq!(stream_key("orders"), "streams/orders");
}

#[test]
fn deleted_stream_note_is_keyed_by_the_stream_identifier() {
    assert_eq!(deleted_stream_key(StreamId::new(7)), "deleted-streams/7");
}

#[test]
fn checkpoint_is_keyed_by_stream_identifier_and_materializer() {
    assert_eq!(checkpoint_key(StreamId::new(7), "pages"), "checkpoints/7/pages");
}

#[test]
fn replica_registration_is_keyed_by_the_replica_identifier() {
    let replica_id = ReplicaId::new("replica-a").expect("a valid identifier");

    assert_eq!(replica_key(&replica_id), "replicas/replica-a");
}

#[test]
fn checkpoints_of_a_stream_are_not_listed_with_those_of_a_longer_identifier() {
    let checkpoint_of_stream_70 = checkpoint_key(StreamId::new(70), "pages");

    let prefix_of_stream_7 = checkpoints_key_prefix(StreamId::new(7));

    assert!(!checkpoint_of_stream_70.starts_with(&prefix_of_stream_7));
}

#[test]
fn checkpoint_key_starts_with_the_prefix_of_its_stream() {
    let checkpoint = checkpoint_key(StreamId::new(7), "pages");

    assert!(checkpoint.starts_with(&checkpoints_key_prefix(StreamId::new(7))));
}

#[test]
fn stream_keys_are_not_listed_with_deleted_stream_notes() {
    assert!(!deleted_stream_key(StreamId::new(7)).starts_with(STREAM_KEY_PREFIX));
}

#[test]
fn names_of_letters_digits_and_the_allowed_punctuation_are_accepted() {
    for name in ["orders", "Orders-2", "a.b_c-d", "7"] {
        let validated = validate_name(name);

        assert_eq!(validated, Ok(()), "{name:?}");
    }
}

#[test]
fn name_of_exactly_the_longest_length_is_accepted() {
    let longest_name = "n".repeat(MAX_NAME_LENGTH_BYTES);

    assert_eq!(validate_name(&longest_name), Ok(()));
}

#[test]
fn empty_name_is_refused() {
    assert_eq!(validate_name(""), Err(MetaError::InvalidName { name: String::new() }));
}

#[test]
fn name_longer_than_the_longest_length_is_refused() {
    let too_long_name = "n".repeat(MAX_NAME_LENGTH_BYTES + 1);

    let validated = validate_name(&too_long_name);

    assert!(matches!(validated, Err(MetaError::InvalidName { .. })));
}

#[test]
fn names_that_would_change_the_structure_of_a_key_are_refused() {
    for name in ["a/b", "with space", "accentué", "tab\tbed"] {
        let validated = validate_name(name);

        assert!(
            matches!(validated, Err(MetaError::InvalidName { .. })),
            "name {name:?} was accepted"
        );
    }
}
