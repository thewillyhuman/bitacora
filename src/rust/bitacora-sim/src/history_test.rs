//! Tests of the record of a simulated run.
//!
//! # What is covered
//!
//! - Events are kept in the order they were recorded, with the simulated time they were recorded at.
//! - Calls and reading sessions get identifiers of their own; appends and key-value calls share one numbering.
//! - Each recording method records the event it is named after, with what it was given.
//! - Clones record into the same history.
//!
//! # How the fixtures work
//!
//! The tests run under a paused runtime, so the time of an event is exactly what the test advanced the clock by.
//! `kind_of` reduces an event to the name of its variant, for tests that are about order.

use super::*;

/// Names the variant of `event`.
fn kind_of(event: &Event) -> &'static str {
    match event {
        Event::AppendInvoked { .. } => "append invoked",
        Event::AppendFinished { .. } => "append finished",
        Event::ReaderStarted { .. } => "reader started",
        Event::RecordRead { .. } => "record read",
        Event::TruncatedPastCheckpoint { .. } => "truncated past checkpoint",
        Event::FillerDeclared { .. } => "filler declared",
        Event::KvInvoked { .. } => "kv invoked",
        Event::KvReturned { .. } => "kv returned",
        Event::Fault { .. } => "fault",
    }
}

/// Lists the variant names of everything `history` recorded, in order.
fn kinds_recorded(history: &History) -> Vec<&'static str> {
    let mut kinds = Vec::new();
    for timed in history.events() {
        kinds.push(kind_of(&timed.event));
    }
    kinds
}

/// The only event of a history that recorded exactly one.
fn only_event(history: &History) -> Event {
    let mut events = history.events();
    assert_eq!(events.len(), 1, "the history holds {events:?}");
    events.remove(0).event
}

#[tokio::test(start_paused = true)]
async fn new_history_is_empty() {
    let history = History::new();

    assert_eq!(history.events(), Vec::new());
}

#[tokio::test(start_paused = true)]
async fn default_history_is_empty() {
    let history = History::default();

    assert_eq!(history.events(), Vec::new());
}

#[tokio::test(start_paused = true)]
async fn events_are_kept_in_the_order_they_were_recorded() {
    let history = History::new();

    let operation = history.append_invoked(Bytes::from("a"));
    history.append_finished(operation, AppendOutcome::Unknown);

    assert_eq!(kinds_recorded(&history), ["append invoked", "append finished"]);
}

#[tokio::test(start_paused = true)]
async fn appends_get_distinct_identifiers() {
    let history = History::new();

    let first = history.append_invoked(Bytes::from("a"));
    let second = history.append_invoked(Bytes::from("b"));

    assert_ne!(first, second);
}

#[tokio::test(start_paused = true)]
async fn sessions_get_distinct_identifiers() {
    let history = History::new();

    let first = history.reader_started(SequenceNumber::FIRST);
    let second = history.reader_started(SequenceNumber::FIRST);

    assert_ne!(first, second);
}

/// An append and a key-value call are both "operations" in the history; the linearizability check and the log checker
/// must never mistake one for the other.
#[tokio::test(start_paused = true)]
async fn appends_and_key_value_calls_share_one_numbering() {
    let history = History::new();

    let append = history.append_invoked(Bytes::from("a"));
    let call = history.kv_invoked(KvCall::Get { key: Bytes::from("k") });

    assert_ne!(append, call);
}

#[tokio::test(start_paused = true)]
async fn events_carry_the_simulated_time_they_happened_at() {
    let history = History::new();
    tokio::time::advance(Duration::from_secs(7)).await;

    history.fault("node 0 crashed");

    assert_eq!(history.events()[0].at, Duration::from_secs(7));
}

#[tokio::test(start_paused = true)]
async fn clones_record_into_the_same_history() {
    let history = History::new();

    history.clone().fault("recorded through a clone");

    assert_eq!(history.events().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn invoked_append_is_recorded_with_its_payload() {
    let history = History::new();

    let operation = history.append_invoked(Bytes::from("a"));

    assert_eq!(
        only_event(&history),
        Event::AppendInvoked {
            operation,
            payload: Bytes::from("a"),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn started_reader_is_recorded_with_where_it_starts() {
    let history = History::new();

    let session = history.reader_started(SequenceNumber::new(5));

    assert_eq!(
        only_event(&history),
        Event::ReaderStarted {
            session,
            from_sequence: SequenceNumber::new(5),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn read_record_is_recorded_with_its_session_and_content() {
    let history = History::new();

    history.record_read(3, SequenceNumber::new(5), Bytes::from("a"));

    assert_eq!(
        only_event(&history),
        Event::RecordRead {
            session: 3,
            sequence: SequenceNumber::new(5),
            payload: Bytes::from("a"),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn truncation_past_a_checkpoint_is_recorded_with_both_positions() {
    let history = History::new();

    history.truncated_past_checkpoint(SequenceNumber::new(10), SequenceNumber::new(15));

    assert_eq!(
        only_event(&history),
        Event::TruncatedPastCheckpoint {
            checkpoint: SequenceNumber::new(10),
            low_water_mark: SequenceNumber::new(15),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn declared_filler_is_recorded_with_its_payload() {
    let history = History::new();

    history.declare_filler(Bytes::from("barrier"));

    assert_eq!(
        only_event(&history),
        Event::FillerDeclared {
            payload: Bytes::from("barrier"),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn key_value_call_is_recorded_when_invoked_and_when_returned() {
    let history = History::new();

    let operation = history.kv_invoked(KvCall::Delete { key: Bytes::from("k") });
    history.kv_returned(operation, KvOutcome::Written);

    assert_eq!(kinds_recorded(&history), ["kv invoked", "kv returned"]);
}

#[tokio::test(start_paused = true)]
async fn fault_is_recorded_with_its_description() {
    let history = History::new();

    history.fault("node 0 crashed");

    assert_eq!(
        only_event(&history),
        Event::Fault {
            description: "node 0 crashed".to_owned(),
        }
    );
}

#[test]
fn call_reports_the_key_it_is_about() {
    let key = Bytes::from("k");
    let put = KvCall::Put {
        key: key.clone(),
        value: Bytes::from("v"),
    };
    let delete = KvCall::Delete { key: key.clone() };
    let get = KvCall::Get { key: key.clone() };

    assert_eq!([put.key(), delete.key(), get.key()], [&key, &key, &key]);
}

/// The fingerprint of a run is computed from this rendering. A change here changes every fingerprint that was ever
/// written down, so the rendering is pinned.
#[test]
fn debug_rendering_of_a_timed_event_is_pinned() {
    let timed = TimedEvent {
        at: Duration::from_millis(1_500),
        event: Event::AppendFinished {
            operation: 4,
            outcome: AppendOutcome::Committed {
                sequence: SequenceNumber::new(9),
                epoch: Epoch::new(2),
            },
        },
    };

    assert_eq!(
        format!("{timed:?}"),
        "TimedEvent { at: 1.5s, event: AppendFinished { operation: 4, outcome: Committed { sequence: \
         SequenceNumber(9), epoch: Epoch(2) } } }"
    );
}
